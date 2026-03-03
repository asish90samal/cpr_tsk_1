"""
feature_builder.py
──────────────────
Feature engineering for the AML ML scoring model.

Fixes applied vs original:
  1. Feature set expanded from 2 → 14 meaningful features
  2. DOB similarity scoring (year / month / day partial credit)
  3. Country risk score lookup
  4. Token overlap (Jaccard)
  5. Name length ratio
  6. Alias match flag
  7. All features documented with expected range
  8. Returns named dict AND flat list for compatibility with both sklearn and XGBoost
"""

from __future__ import annotations
import re
from typing import Any

import pandas as pd

from etl_layer.normalization import normalize_dob

# ── Country risk scores (FATF-based, 0.0 = low risk, 1.0 = maximum risk) ──
_COUNTRY_RISK: dict[str, float] = {
    "IRAN":         1.00,
    "NORTH KOREA":  1.00,
    "MYANMAR":      0.90,
    "SYRIA":        0.90,
    "RUSSIA":       0.85,
    "BELARUS":      0.80,
    "VENEZUELA":    0.75,
    "CUBA":         0.70,
    "SUDAN":        0.70,
    "SOMALIA":      0.70,
    "AFGHANISTAN":  0.65,
    "LIBYA":        0.65,
    "UNKNOWN":      0.50,
}
_DEFAULT_COUNTRY_RISK = 0.30   # for countries not in the list


def _country_risk(country: str | None) -> float:
    if not country:
        return _DEFAULT_COUNTRY_RISK
    return _COUNTRY_RISK.get(country.upper().strip(), _DEFAULT_COUNTRY_RISK)


def _token_jaccard(name_a: str, name_b: str) -> float:
    """Jaccard similarity of word token sets."""
    tokens_a = set(name_a.upper().split())
    tokens_b = set(name_b.upper().split())
    if not tokens_a and not tokens_b:
        return 1.0
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


def _name_length_ratio(name_a: str, name_b: str) -> float:
    """Ratio of shorter to longer name length (chars). Range: (0, 1]."""
    la, lb = len(name_a.replace(" ", "")), len(name_b.replace(" ", ""))
    if la == 0 or lb == 0:
        return 0.0
    return min(la, lb) / max(la, lb)


def _dob_similarity(dob_input: str | None, dob_candidate: str | None) -> dict[str, float]:
    """
    Compare two DOB strings at year / month / day granularity.
    Returns partial-credit scores: year_match, month_match, day_match, full_match.
    """
    d1 = normalize_dob(dob_input)
    d2 = normalize_dob(dob_candidate)

    year_match  = float(d1["year"]  == d2["year"])  if d1["year"]  and d2["year"]  else 0.0
    month_match = float(d1["month"] == d2["month"]) if d1["month"] and d2["month"] else 0.0
    day_match   = float(d1["day"]   == d2["day"])   if d1["day"]   and d2["day"]   else 0.0
    full_match  = float(year_match == 1 and month_match == 1 and day_match == 1)

    return {
        "dob_year_match":  year_match,
        "dob_month_match": month_match,
        "dob_day_match":   day_match,
        "dob_full_match":  full_match,
    }


def build_features(
    match_result: dict[str, Any],
    input_record: dict[str, Any],
    candidate_row: pd.Series,
) -> dict[str, float]:
    """
    Build a rich feature dictionary for a (input, candidate) pair.

    Parameters
    ----------
    match_result   : Output dict from matcher.match() / batch_match()
    input_record   : Dict with keys: name, dob, country (the query record)
    candidate_row  : Row from the sanctions DataFrame

    Returns
    -------
    Feature dict with 14 named features (values are all floats in [0, 1]).

    Features
    --------
    composite_score      - weighted fuzzy score (from matcher)
    score_token_sort     - token-sort-ratio score
    score_token_set      - token-set-ratio score
    score_partial        - partial-ratio score
    score_jaro_winkler   - Jaro-Winkler score
    matched_on_alias     - 1.0 if best match was on an alias (not primary name)
    token_jaccard        - Jaccard of name token sets
    name_length_ratio    - ratio of shorter/longer name (chars)
    risk_weight          - candidate's declared risk weight
    country_risk_score   - lookup risk score for candidate's country
    country_match        - 1.0 if input and candidate share same country
    dob_year_match       - 1.0 if birth years match
    dob_month_match      - 1.0 if birth months match
    dob_day_match        - 1.0 if birth days match
    dob_full_match       - 1.0 if full DOB matches
    """
    input_name   = str(input_record.get("name", "")).upper()
    cand_name    = str(candidate_row.get("primary_name", "")).upper()
    input_dob    = input_record.get("dob")
    cand_dob     = candidate_row.get("dob")
    input_country  = str(input_record.get("country", "")).upper()
    cand_country   = str(candidate_row.get("nationality", "")).upper()

    dob_scores = _dob_similarity(input_dob, cand_dob)

    features: dict[str, float] = {
        # ── Similarity scores from matcher ──────────────────────────────
        "composite_score":    float(match_result.get("composite_score", 0.0)),
        "score_token_sort":   float(match_result.get("score_token_sort", 0.0)),
        "score_token_set":    float(match_result.get("score_token_set", 0.0)),
        "score_partial":      float(match_result.get("score_partial", 0.0)),
        "score_jaro_winkler": float(match_result.get("score_jaro_winkler", 0.0)),
        "matched_on_alias":   float(match_result.get("matched_on_alias", False)),

        # ── Name structural features ────────────────────────────────────
        "token_jaccard":      _token_jaccard(input_name, cand_name),
        "name_length_ratio":  _name_length_ratio(input_name, cand_name),

        # ── Risk / entity context ────────────────────────────────────────
        "risk_weight":          float(candidate_row.get("risk_weight", 0.5)),
        "country_risk_score":   _country_risk(cand_country),
        "country_match":        float(input_country == cand_country) if input_country and cand_country else 0.0,

        # ── DOB features ─────────────────────────────────────────────────
        **dob_scores,
    }

    return features


def features_to_list(feature_dict: dict[str, float]) -> list[float]:
    """Return features as an ordered list (stable key order) for sklearn/XGBoost."""
    return list(feature_dict.values())


FEATURE_NAMES: list[str] = [
    "composite_score", "score_token_sort", "score_token_set",
    "score_partial", "score_jaro_winkler", "matched_on_alias",
    "token_jaccard", "name_length_ratio",
    "risk_weight", "country_risk_score", "country_match",
    "dob_year_match", "dob_month_match", "dob_day_match", "dob_full_match",
]
