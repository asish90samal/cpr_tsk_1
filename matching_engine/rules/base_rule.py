"""
matching_engine/rules/base_rule.py  -- Per-job matching rules for all 10 jobs.

Rule map
--------
SAN_IND   -> SanctionIndividualRule   (4-algo fuzzy + DOB boost + exact ID auto-ALERT)
SAN_ENT   -> SanctionEntityRule       (suffix strip + registration_number auto-ALERT)
SCION_*   -> ScionRule                (account/reference exact match + status boost)
PEP_IND   -> PEPIndividualRule        (tier/active/country boosts)
PEP_ENT   -> PEPEntityRule            (direct list + beneficial_owner_pep_id link)
NNS_* (articles)   -> NNSArticleRule  (fuzzy x recency x severity)
NNS_* (structured) -> NNSStructuredRule (fuzzy + case_id review flag)
CTRY_*    -> CtryRule                 (pure country lookup, no name scoring)
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any
import pandas as pd
from rapidfuzz import fuzz, distance


def _composite(a: str, b: str) -> float:
    a, b = a.upper().strip(), b.upper().strip()
    if not a or not b: return 0.0
    ts  = fuzz.token_sort_ratio(a, b) / 100.0
    tse = fuzz.token_set_ratio(a, b)  / 100.0
    pr  = fuzz.partial_ratio(a, b)    / 100.0
    jw  = distance.JaroWinkler.normalized_similarity(a, b)
    return round(0.30*ts + 0.30*tse + 0.20*pr + 0.20*jw, 4)


def _best_alias_score(input_name: str, row: pd.Series) -> tuple:
    names = [(str(row.get("primary_name", "")), False)]
    als = row.get("aliases", "")
    if als:
        for a in str(als).split("|"):
            a = a.strip()
            if a: names.append((a, True))
    best, bname, is_al = 0.0, str(row.get("primary_name", "")), False
    for name, alias_flag in names:
        if input_name.upper() == name.upper():
            return 1.0, name, alias_flag
        s = _composite(input_name, name)
        if s > best:
            best, bname, is_al = s, name, alias_flag
    return best, bname, is_al


_SUFFIXES = {"LTD","LIMITED","LLC","INC","CORP","CORPORATION","PLC","JSC","OJSC","PJSC",
             "GMBH","AG","SA","BV","NV","SRL","CO","COMPANY","GROUP","HOLDINGS","TRADING"}

def _strip_suffix(name: str) -> str:
    toks = name.upper().strip().split()
    return " ".join(toks[:-1]) if toks and toks[-1] in _SUFFIXES else name.upper().strip()


@dataclass
class RuleResult:
    entity_id:        Any
    primary_name:     str
    matched_name:     str
    dataset_code:     str
    job_code:         str
    rule_name:        str
    name_score:       float
    rule_score:       float
    auto_alert:       bool
    review_flag:      bool
    matched_on_alias: bool
    match_details:    dict = field(default_factory=dict)


class BaseMatchingRule(ABC):
    @property
    @abstractmethod
    def rule_name(self) -> str: ...
    @property
    @abstractmethod
    def job_codes(self) -> list: ...
    @property
    @abstractmethod
    def dataset_codes(self) -> list: ...
    @abstractmethod
    def score(self, inp: dict, row: pd.Series, exact_id_hit: bool = False) -> RuleResult: ...


class SanctionIndividualRule(BaseMatchingRule):
    rule_name    = "SAN_INDIVIDUAL"
    job_codes    = ["SAN_IND"]
    dataset_codes = ["OFAC_SDN","UN_CONSOLIDATED","EU_SANCTIONS","HM_TREASURY",
                     "INTERPOL_RED","WORLD_CHECK","DOW_JONES","COMPLY_ADVANTAGE","ACCUITY_FIRCO"]

    def score(self, inp: dict, row: pd.Series, exact_id_hit: bool = False) -> RuleResult:
        eid = row.get("entity_id",""); pname = str(row.get("primary_name","")); ds = str(row.get("dataset_type",""))
        if exact_id_hit:
            return RuleResult(entity_id=eid,primary_name=pname,matched_name=pname,dataset_code=ds,
                job_code="SAN_IND",rule_name=self.rule_name,name_score=1.0,rule_score=1.0,
                auto_alert=True,review_flag=False,matched_on_alias=False,match_details={"trigger":"exact_id_match"})
        ns, mn, ia = _best_alias_score(str(inp.get("name","")), row)
        dob_boost = 0.0
        id1, id2 = str(inp.get("dob","")), str(row.get("dob",""))
        if id1 and id2:
            if id1[:4] == id2[:4] and id1[:4].isdigit(): dob_boost = 0.05
            if id1[:7] == id2[:7]: dob_boost = 0.08
            if id1 == id2: dob_boost = 0.12
        rs = min(1.0, round(ns + dob_boost, 4))
        return RuleResult(entity_id=eid,primary_name=pname,matched_name=mn,dataset_code=ds,
            job_code="SAN_IND",rule_name=self.rule_name,name_score=ns,rule_score=rs,
            auto_alert=False,review_flag=False,matched_on_alias=ia,match_details={"dob_boost":dob_boost})


class SanctionEntityRule(BaseMatchingRule):
    rule_name    = "SAN_ENTITY"
    job_codes    = ["SAN_ENT"]
    dataset_codes = ["OFAC_SDN","UN_CONSOLIDATED","EU_SANCTIONS","HM_TREASURY",
                     "WORLD_CHECK","DOW_JONES","COMPLY_ADVANTAGE","ACCUITY_FIRCO"]

    def score(self, inp: dict, row: pd.Series, exact_id_hit: bool = False) -> RuleResult:
        eid = row.get("entity_id",""); pname = str(row.get("primary_name","")); ds = str(row.get("dataset_type",""))
        if exact_id_hit:
            return RuleResult(entity_id=eid,primary_name=pname,matched_name=pname,dataset_code=ds,
                job_code="SAN_ENT",rule_name=self.rule_name,name_score=1.0,rule_score=1.0,
                auto_alert=True,review_flag=False,matched_on_alias=False,match_details={"trigger":"exact_id_match"})
        inp_name = str(inp.get("name","")).upper()
        ns, mn, ia = _best_alias_score(inp_name, row)
        s_stripped = _composite(_strip_suffix(inp_name), _strip_suffix(pname))
        nws = str(row.get("name_without_suffix",""))
        s_nws = _composite(_strip_suffix(inp_name), nws) if nws else 0.0
        final = max(ns, s_stripped, s_nws)
        if s_stripped > ns: mn, ia = _strip_suffix(pname), True
        if s_nws > s_stripped and s_nws > ns: mn, ia = nws, True
        return RuleResult(entity_id=eid,primary_name=pname,matched_name=mn,dataset_code=ds,
            job_code="SAN_ENT",rule_name=self.rule_name,name_score=ns,rule_score=round(final,4),
            auto_alert=False,review_flag=False,matched_on_alias=ia,
            match_details={"score_stripped":s_stripped,"score_nws":s_nws})


class ScionRule(BaseMatchingRule):
    rule_name    = "SCION"
    job_codes    = ["SCION_IND","SCION_ENT"]
    dataset_codes = ["SCION"]

    def score(self, inp: dict, row: pd.Series, exact_id_hit: bool = False) -> RuleResult:
        eid = row.get("entity_id",""); pname = str(row.get("primary_name",""))
        etype = str(inp.get("entity_type","INDIVIDUAL")); jc = "SCION_IND" if etype=="INDIVIDUAL" else "SCION_ENT"
        src = str(row.get("scion_source",""))
        if exact_id_hit:
            return RuleResult(entity_id=eid,primary_name=pname,matched_name=pname,dataset_code="SCION",
                job_code=jc,rule_name=self.rule_name,name_score=1.0,rule_score=1.0,
                auto_alert=True,review_flag=False,matched_on_alias=False,
                match_details={"trigger":"account_reference_match","scion_source":src})
        ns, mn, ia = _best_alias_score(str(inp.get("name","")), row)
        status = str(row.get("watchlist_status","")).upper()
        boost = {"BLACKLIST":0.10,"DECLINED_ONBOARDING":0.07,"GREYLIST":0.03}.get(status, 0.0)
        rs = min(1.0, round(ns + boost, 4))
        return RuleResult(entity_id=eid,primary_name=pname,matched_name=mn,dataset_code="SCION",
            job_code=jc,rule_name=self.rule_name,name_score=ns,rule_score=rs,
            auto_alert=False,review_flag=False,matched_on_alias=ia,
            match_details={"scion_source":src,"watchlist_status":status,"status_boost":boost,
                           "flagging_reason":str(row.get("flagging_reason",""))})


class PEPIndividualRule(BaseMatchingRule):
    rule_name    = "PEP_INDIVIDUAL"
    job_codes    = ["PEP_IND"]
    dataset_codes = ["PEP_DATABASE"]

    def score(self, inp: dict, row: pd.Series, exact_id_hit: bool = False) -> RuleResult:
        eid = row.get("entity_id",""); pname = str(row.get("primary_name",""))
        if str(row.get("entity_type","")) == "ENTITY":
            return RuleResult(entity_id=eid,primary_name=pname,matched_name=pname,
                dataset_code="PEP_DATABASE",job_code="PEP_IND",rule_name=self.rule_name,
                name_score=0.0,rule_score=0.0,auto_alert=False,review_flag=False,
                matched_on_alias=False,match_details={"skip":"entity_row"})
        ns, mn, ia = _best_alias_score(str(inp.get("name","")), row)
        tier = int(row.get("pep_tier", 3))
        active = bool(row.get("is_active", False))
        tier_b = {1:0.08,2:0.04,3:0.00}[tier]
        if active: tier_b += 0.03
        ctry_b = 0.05 if (str(inp.get("country","")).upper() == str(row.get("country_of_office","")).upper()
                          and inp.get("country","")) else 0.0
        rs = min(1.0, round(ns + tier_b + ctry_b, 4))
        return RuleResult(entity_id=eid,primary_name=pname,matched_name=mn,
            dataset_code="PEP_DATABASE",job_code="PEP_IND",rule_name=self.rule_name,
            name_score=ns,rule_score=rs,auto_alert=False,review_flag=False,matched_on_alias=ia,
            match_details={"pep_tier":tier,"is_active":active,"tier_boost":tier_b,"country_boost":ctry_b,
                           "political_role":str(row.get("political_role",""))})


class PEPEntityRule(BaseMatchingRule):
    rule_name    = "PEP_ENTITY"
    job_codes    = ["PEP_ENT"]
    dataset_codes = ["PEP_DATABASE"]

    def score(self, inp: dict, row: pd.Series, exact_id_hit: bool = False) -> RuleResult:
        eid = row.get("entity_id",""); pname = str(row.get("primary_name",""))
        if str(row.get("entity_type","")) != "ENTITY":
            return RuleResult(entity_id=eid,primary_name=pname,matched_name=pname,
                dataset_code="PEP_DATABASE",job_code="PEP_ENT",rule_name=self.rule_name,
                name_score=0.0,rule_score=0.0,auto_alert=False,review_flag=False,
                matched_on_alias=False,match_details={"skip":"individual_row"})
        ns, mn, ia = _best_alias_score(str(inp.get("name","")), row)
        bop = str(row.get("beneficial_owner_pep_id",""))
        has_bop = bool(bop and bop.strip())
        boost = 0.05 if bool(row.get("is_active", True)) else 0.0
        rs = min(1.0, round(ns + boost, 4))
        return RuleResult(entity_id=eid,primary_name=pname,matched_name=mn,
            dataset_code="PEP_DATABASE",job_code="PEP_ENT",rule_name=self.rule_name,
            name_score=ns,rule_score=rs,auto_alert=False,review_flag=has_bop,matched_on_alias=ia,
            match_details={"beneficial_owner_pep_id":bop,"has_beneficial_owner_link":has_bop})


class NNSArticleRule(BaseMatchingRule):
    rule_name    = "NNS_ARTICLES"
    job_codes    = ["NNS_IND","NNS_ENT"]
    dataset_codes = ["NNS_ARTICLES"]

    def score(self, inp: dict, row: pd.Series, exact_id_hit: bool = False) -> RuleResult:
        eid = row.get("entity_id",""); pname = str(row.get("primary_name",""))
        ns, mn, ia = _best_alias_score(str(inp.get("name","")), row)
        rw = float(row.get("recency_weight", 0.5)); sev = float(row.get("category_severity", 0.5))
        rs = round(ns * rw * sev, 4)
        jc = "NNS_IND" if str(inp.get("entity_type","INDIVIDUAL")) == "INDIVIDUAL" else "NNS_ENT"
        return RuleResult(entity_id=eid,primary_name=pname,matched_name=mn,
            dataset_code="NNS_ARTICLES",job_code=jc,rule_name=self.rule_name,
            name_score=ns,rule_score=rs,auto_alert=False,review_flag=False,matched_on_alias=ia,
            match_details={"recency_weight":rw,"category_severity":sev,
                           "category":str(row.get("category","")),
                           "publication_date":str(row.get("publication_date","")),
                           "source":str(row.get("source_publication","")),
                           "headline":str(row.get("headline_snippet",""))})


class NNSStructuredRule(BaseMatchingRule):
    rule_name    = "NNS_STRUCTURED"
    job_codes    = ["NNS_IND","NNS_ENT"]
    dataset_codes = ["NNS_STRUCTURED"]

    def score(self, inp: dict, row: pd.Series, exact_id_hit: bool = False) -> RuleResult:
        eid = row.get("entity_id",""); pname = str(row.get("primary_name",""))
        ns, mn, ia = _best_alias_score(str(inp.get("name","")), row)
        sev = float(row.get("category_severity", 0.5))
        rs = round(ns * sev, 4)
        jc = "NNS_IND" if str(inp.get("entity_type","INDIVIDUAL")) == "INDIVIDUAL" else "NNS_ENT"
        return RuleResult(entity_id=eid,primary_name=pname,matched_name=mn,
            dataset_code="NNS_STRUCTURED",job_code=jc,rule_name=self.rule_name,
            name_score=ns,rule_score=rs,auto_alert=False,review_flag=exact_id_hit,matched_on_alias=ia,
            match_details={"case_id":str(row.get("case_id","")),"category":str(row.get("category","")),
                           "category_severity":sev,"country_of_subject":str(row.get("country_of_subject","")),
                           "linked_entity_ids":str(row.get("linked_entity_ids",""))})


@dataclass
class CtryRuleResult:
    job_code:             str
    entity_type:          str
    checked_fields:       dict
    field_scores:         dict
    highest_risk_score:   float
    highest_risk_country: str
    highest_risk_field:   str
    risk_tier:            str
    fatf_status:          str
    un_sanctions:         bool
    eu_sanctions:         bool
    ofac_sanctions:       bool
    auto_alert:           bool
    review_flag:          bool


class CtryRule:
    rule_name = "CTRY_LOOKUP"

    def screen(self, input_record: dict, entity_type: str = "INDIVIDUAL",
               alert_threshold: float = 0.75, review_band: float = 0.10) -> CtryRuleResult:
        from data_layer.generators.ctry import get_country_risk, CTRY_IND_FIELDS, CTRY_ENT_FIELDS
        fields = CTRY_IND_FIELDS if entity_type == "INDIVIDUAL" else CTRY_ENT_FIELDS
        jc = "CTRY_IND" if entity_type == "INDIVIDUAL" else "CTRY_ENT"
        checked, scores = {}, {}
        for f in fields:
            val = str(input_record.get(f, "")).strip()
            if val:
                rec = get_country_risk(val)
                checked[f] = rec["country"]; scores[f] = rec["risk_score"]
        if not scores:
            rec = get_country_risk("UNKNOWN"); checked["nationality"] = "UNKNOWN"; scores["nationality"] = rec["risk_score"]
        top_field = max(scores, key=lambda k: scores[k])
        top_score = scores[top_field]; top_country = checked[top_field]
        top_rec = get_country_risk(top_country)
        auto_alert  = top_score >= alert_threshold
        review_flag = (not auto_alert) and (top_score >= alert_threshold - review_band)
        return CtryRuleResult(job_code=jc,entity_type=entity_type,checked_fields=checked,
            field_scores=scores,highest_risk_score=top_score,highest_risk_country=top_country,
            highest_risk_field=top_field,risk_tier=top_rec["risk_tier"],fatf_status=top_rec["fatf_status"],
            un_sanctions=top_rec["un_sanctions"],eu_sanctions=top_rec["eu_sanctions"],
            ofac_sanctions=top_rec["ofac_sanctions"],auto_alert=auto_alert,review_flag=review_flag)


class MatchingRuleEngine:
    def __init__(self):
        self._rules = {}
        self.ctry_rule = CtryRule()
        self._register_defaults()

    def _register_defaults(self):
        for rule in [SanctionIndividualRule(),SanctionEntityRule(),ScionRule(),
                     PEPIndividualRule(),PEPEntityRule(),NNSArticleRule(),NNSStructuredRule()]:
            for jc in rule.job_codes:
                for ds in rule.dataset_codes:
                    self._rules[(jc, ds)] = rule

    def get_rule(self, job_code: str, dataset_code: str):
        key = (job_code.upper(), dataset_code.upper())
        if key in self._rules: return self._rules[key]
        for (jc, ds), rule in self._rules.items():
            if ds == dataset_code.upper(): return rule
        return SanctionIndividualRule()

    def apply(self, input_record: dict, candidate_row: pd.Series,
              job_code: str, dataset_code: str, exact_id_hit: bool = False) -> RuleResult:
        return self.get_rule(job_code, dataset_code).score(input_record, candidate_row, exact_id_hit=exact_id_hit)

    def list_rules(self):
        return [(jc, ds, r.rule_name) for (jc, ds), r in sorted(self._rules.items())]
