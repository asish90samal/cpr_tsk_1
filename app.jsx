import { useState } from "react";

const MONO = "'JetBrains Mono', 'Fira Code', monospace";
const DISPLAY = "'Bebas Neue', sans-serif";
const BODY = "'Nunito', sans-serif";

const PHASES = [
  {
    num: 1,
    label: "FOUNDATION",
    title: "Data Layer",
    subtitle: "Build the raw material first — nothing else can run without this",
    color: "#f97316",
    duration: "Start here",
    modules: [
      "utils/alias_engine.py",
      "utils/transliteration_engine.py",
      "generators/san_individual.py",
      "generators/san_entity.py",
      "generators/scion.py",
      "generators/pep.py",
      "generators/nns.py",
      "generators/ctry.py",
      "registry/dataset_registry.py",
    ],
    why: "Everything in the project depends on data existing. The matching engine needs records to compare against. The inverted index needs records to index. The ML model needs records to train on. If you build anything else first, you have nothing to test it with. You'd be building engines with no fuel.",
    what_you_can_test: [
      "registry.load_all() runs without error",
      "registry.summary() prints correct row counts for all 10 job partitions",
      "registry.get('OFAC_SDN', 'INDIVIDUAL') returns a non-empty DataFrame",
      "registry.get_for_job('SAN_IND') returns 9 datasets in priority order",
      "CTRY register has 32 rows with correct risk_scores",
      "aliases column is never empty — every record has variants",
    ],
    checkpoint: "You can call registry.summary() and see 17,000+ records across all partitions. Every dataset has the right columns.",
    depends_on: [],
    unlocks: ["Phase 2 — ETL", "Phase 3 — Blocking"],
    pitfalls: [
      "Build utils/ BEFORE generators/ — alias_engine is imported by generators",
      "ctry.py has no n= parameter — it always returns exactly 32 rows",
      "NNS has TWO generators (generate_articles + generate_structured) — both must be called",
      "Test risk_label exists on every row — ML needs it later",
    ],
  },
  {
    num: 2,
    label: "CLEANING",
    title: "ETL Layer",
    subtitle: "Normalise names before any comparison — dirty data = wrong results",
    color: "#eab308",
    duration: "Build second",
    modules: [
      "etl_layer/normalization.py",
    ],
    why: "Before you can compare 'Mohammed' with 'MOHAMMED' or 'Möhammed', they must all be cleaned to the same form. If you skip ETL and go straight to matching, 'Mohammed' and 'MOHAMMED' score 0% similarity — identical names, zero match. ETL is the invisible layer that makes everything else work correctly.",
    what_you_can_test: [
      "normalize_name('Möhammed Al-Hässan') → 'MOHAMMED AL HASSAN'",
      "normalize_name('VLADIMIR') → 'VLADIMIR' (no change needed)",
      "normalize_aliases('MOHD KHAN|M. KHAN') → ['MOHD KHAN', 'M KHAN']",
      "normalize_dob('1968-3-5') → {year:1968, month:3, day:5}",
      "normalize_country('IR') → 'IRAN'",
    ],
    checkpoint: "normalize_name() handles diacritics, case, punctuation. All ScreeningInputs normalise their name on creation via __post_init__.",
    depends_on: ["Phase 1 — Data Layer"],
    unlocks: ["Phase 3 — Blocking", "Phase 5 — Matching Rules"],
    pitfalls: [
      "Normalise BOTH the input AND the candidates — not just one side",
      "Do not over-strip: 'BIN' in 'OSAMA BIN LADEN' must not be removed",
      "Country codes (IR, RU) must expand BEFORE country risk lookup",
    ],
  },
  {
    num: 3,
    label: "SPEED",
    title: "Blocking Engine",
    subtitle: "Cut 17,000 records to ~200 candidates before expensive scoring",
    color: "#22c55e",
    duration: "Build third",
    modules: [
      "blocking_engine/inverted_index.py",
      "blocking_engine/blocking.py",
    ],
    why: "Fuzzy matching (Jaro-Winkler etc) costs ~0.1ms per comparison. Against 17,000 records = 1.7 seconds per query. After blocking cuts to 200 candidates = 20ms. That's 85x faster. Without blocking the system is too slow for batch screening. The inverted index also enables exact ID lookup in O(1) time — critical for auto-ALERT logic.",
    what_you_can_test: [
      "InvertedIndex builds successfully from a DataFrame",
      "query('MOHAMMED HUSSAIN') returns < 500 candidates from 5,000 records",
      "exact_id_lookup('P7654321') returns a single row in < 1ms",
      "MultiDatasetIndex.build_from_registry(registry) indexes all partitions",
      "query on 'UNKNOWN NAME XYZ' returns empty DataFrame — no false candidates",
    ],
    checkpoint: "MultiDatasetIndex is built from the full registry. Query 'MOHAMMED' returns ~200 candidates in under 2ms across all SAN datasets.",
    depends_on: ["Phase 1 — Data Layer", "Phase 2 — ETL"],
    unlocks: ["Phase 4 — Config", "Phase 5 — Matching Rules"],
    pitfalls: [
      "Index must be rebuilt if registry data changes — it's a snapshot at build time",
      "Phonetic index (Double Metaphone) must cover aliases, not just primary_name",
      "CTRY does NOT need an inverted index — it's a country lookup, skip it for CTRY",
    ],
  },
  {
    num: 4,
    label: "RULES",
    title: "Config + Policy Engine",
    subtitle: "Define the 10 jobs, their datasets, and their thresholds",
    color: "#3b82f6",
    duration: "Build fourth",
    modules: [
      "config/job_types.py",
      "policy_engine/thresholds.py",
      "routing_engine/router.py",
    ],
    why: "The matching engine and orchestrator both need to know: which datasets does SAN_IND screen against? What threshold triggers an ALERT for PEP_IND vs PEP_ENT? Which job does an INDIVIDUAL + SAN request map to? This configuration must exist before you build the orchestrator — otherwise the orchestrator has nowhere to look up its rules.",
    what_you_can_test: [
      "get_job('SAN_IND').threshold == 0.65",
      "get_job('PEP_IND').threshold == 0.60  ← strictest threshold",
      "get_job('SAN_IND').datasets has 9 entries, SAN_ENT has 8 (no INTERPOL)",
      "get_job('CTRY_IND').is_country_lookup == True",
      "route('SAN','INDIVIDUAL','IRAN').risk_tier == 'HIGH'",
      "apply_threshold(0.73, 'SAN_IND').decision == 'ALERT'  ← 0.73 >= 0.65",
      "apply_threshold(0.63, 'SAN_IND').decision == 'REVIEW' ← within 0.08 band",
      "apply_threshold(0.50, 'SAN_IND').decision == 'NO_ALERT'",
    ],
    checkpoint: "All 10 job configs load correctly. Thresholds return the right decisions for edge cases (just above, just below, in review band).",
    depends_on: ["Phase 1 — Data Layer"],
    unlocks: ["Phase 5 — Matching Rules", "Phase 6 — Orchestrator"],
    pitfalls: [
      "PEP_IND threshold is 0.60 not 0.65 — it is the STRICTEST job",
      "CTRY threshold is a country risk score (0–1), not a name fuzzy score",
      "Review band = ±0.08 of threshold — test boundary cases carefully",
      "INTERPOL_RED must only appear in SAN_IND — not SAN_ENT",
    ],
  },
  {
    num: 5,
    label: "CORE LOGIC",
    title: "Matching Rules + Consolidator",
    subtitle: "The brain — per-dataset rules that score every candidate",
    color: "#a855f7",
    duration: "Build fifth",
    modules: [
      "matching_engine/rules/base_rule.py",
      "matching_engine/matcher.py",
      "scoring/consolidator.py",
    ],
    why: "This is where actual screening happens. For every candidate the inverted index returns, a matching rule scores it. Different datasets need different rules: OFAC uses DOB boost, PEP uses tier boost, NNS uses recency × severity, CTRY skips name scoring entirely. The consolidator then merges all 30–50 rule results into one final score with multi-list multiplier applied.",
    what_you_can_test: [
      "SanctionIndividualRule: exact id_number match → rule_score=1.0, auto_alert=True",
      "SanctionIndividualRule: matching DOB → rule_score = name_score + 0.12",
      "PEPIndividualRule: Tier 1 active PEP → boost = +0.08 + 0.03 + 0.05 = +0.16",
      "NNSArticleRule: old article (recency=0.30) → score heavily penalised",
      "ScionRule: BLACKLIST status → +0.10 boost over base name score",
      "CtryRule: IRAN → auto_alert=True (score=1.0 >= threshold=0.75)",
      "consolidate(): entity in 3 datasets → score × 1.15² = score × 1.32",
      "consolidate(): any auto_alert=True → final decision always ALERT",
    ],
    checkpoint: "Screen 'MOHAMMED ALI HUSSAIN' with exact passport match against OFAC. Should return auto_alert=True, rule_score=1.0, decision=ALERT regardless of threshold.",
    depends_on: ["Phase 1", "Phase 2 — ETL", "Phase 3 — Blocking", "Phase 4 — Config"],
    unlocks: ["Phase 6 — Orchestrator"],
    pitfalls: [
      "Score each candidate individually — do NOT average across candidates",
      "NNS case_id match is review_flag=True, NOT auto_alert — it needs analyst review",
      "PEP_ENT beneficial_owner_pep_id link → review_flag=True, not auto_alert",
      "Multi-list multiplier: 1.15^(N-1) — if in 1 dataset, multiplier = 1.0 (no boost)",
      "Rule score is always capped at 1.0",
    ],
  },
  {
    num: 6,
    label: "CONDUCTOR",
    title: "Orchestrator",
    subtitle: "Ties everything together — one input in, one decision out",
    color: "#ec4899",
    duration: "Build sixth",
    modules: [
      "orchestration/orchestrator.py",
    ],
    why: "The orchestrator is the only module the outside world needs to call. It takes a ScreeningInput, routes it to the right job, queries the inverted index for each dataset, applies the correct matching rule, consolidates the results, logs the decision, and creates an alert if needed. You build it last (of the core pipeline) because it depends on ALL previous phases.",
    what_you_can_test: [
      "orchestrator.screen(SAN_IND input) runs without error end-to-end",
      "orchestrator.screen(CTRY_IND input) uses CtryRule, NOT inverted index",
      "orchestrator.screen(PEP_ENT input) checks both direct list AND beneficial_owner_pep_id",
      "orchestrator.screen_batch(50 inputs) completes in < 10 seconds",
      "batch_summary() returns correct alert_rate_pct and avg_latency_ms",
      "latency_ms is recorded on every ScreeningOutput",
    ],
    checkpoint: "Call orchestrator.screen() for all 10 job types — SAN_IND, SAN_ENT, SCION_IND, SCION_ENT, PEP_IND, PEP_ENT, NNS_IND, NNS_ENT, CTRY_IND, CTRY_ENT. All return a ScreeningOutput without error.",
    depends_on: ["All previous phases"],
    unlocks: ["Phase 7 — Governance", "Phase 8 — ML"],
    pitfalls: [
      "CTRY job must skip the inverted index entirely — different code path",
      "screen_batch uses ThreadPoolExecutor — test for thread-safety issues",
      "Always measure latency_ms — it reveals if blocking is working",
    ],
  },
  {
    num: 7,
    label: "COMPLIANCE",
    title: "Governance + Workflow",
    subtitle: "Every decision logged. Every alert created. Legal requirement.",
    color: "#14b8a6",
    duration: "Build seventh",
    modules: [
      "governance/audit.py",
      "workflow/alert.py",
    ],
    why: "FATF regulations require that every AML screening decision is logged with timestamp, score, reason, and analyst. Without an audit trail, your screening system is not compliant — even if the scores are perfect. Alerts create work items for compliance analysts. These modules are built after the orchestrator because they are called BY the orchestrator once a decision is made.",
    what_you_can_test: [
      "Every orchestrator.screen() call creates one AuditEntry",
      "AuditEntry has entity_id, input_name, score, decision, timestamp",
      "ALERT and REVIEW decisions create an Alert object — NO_ALERT does not",
      "Alert severity: score >= 0.92 → CRITICAL, >= 0.80 → HIGH, >= 0.65 → MEDIUM",
      "audit_log_to_df() returns all decisions as a DataFrame for export",
    ],
    checkpoint: "Run 50 batch screenings. audit_log_to_df() returns exactly 50 rows. All ALERTs have a corresponding Alert object in the alert queue.",
    depends_on: ["Phase 6 — Orchestrator"],
    unlocks: ["Phase 9 — Monitoring / KPIs"],
    pitfalls: [
      "Audit log must be thread-safe if using screen_batch with multiple workers",
      "Timestamp must be UTC — not local time",
      "Alert severity tiers must match your department's escalation policy",
    ],
  },
  {
    num: 8,
    label: "INTELLIGENCE",
    title: "ML Model + Feature Engine",
    subtitle: "Learn from historical decisions to improve future scores",
    color: "#f43f5e",
    duration: "Build eighth",
    modules: [
      "feature_engine/feature_builder.py",
      "ml_engine/model.py",
    ],
    why: "Rules-based scoring (phases 5–6) works well but cannot learn. The ML model trains on the rule results + features extracted from them (fuzzy scores, DOB match, country risk, alias match etc.) and learns which combinations actually predict true positives vs false alarms. It augments — not replaces — the rule-based system.",
    what_you_can_test: [
      "build_features(rule_results) returns a DataFrame with 15 columns",
      "risk_label column exists and has ~15% positive rate",
      "model.train(X, y) with scale_pos_weight = n_neg/n_pos (imbalance correction)",
      "model.evaluate() reports PR-AUC > 0.70 (not ROC-AUC — wrong metric for imbalanced data)",
      "model.predict_proba() returns calibrated probabilities (0.0–1.0)",
      "Confusion matrix: recall > 0.75 (catching criminals matters more than precision)",
    ],
    checkpoint: "Train on 80% of batch results. Evaluate on 20%. PR-AUC > 0.70. Recall > 0.75. scale_pos_weight must be set — without it the model predicts NO_ALERT for everything and gets 85% 'accuracy' but catches nothing.",
    depends_on: ["Phase 6 — Orchestrator", "Phase 7 — Governance"],
    unlocks: ["Phase 9 — Graph Engine", "Phase 10 — Monitoring"],
    pitfalls: [
      "scale_pos_weight = n_neg/n_pos — without this, model is useless on imbalanced data",
      "Use PR-AUC not ROC-AUC as primary metric",
      "Calibrate probabilities with CalibratedClassifierCV — raw XGBoost scores are not probabilities",
      "Features from CTRY job are different from name-match jobs — handle separately",
    ],
  },
  {
    num: 9,
    label: "NETWORK",
    title: "Graph Engine",
    subtitle: "Detect shell company networks and hidden connections",
    color: "#8b5cf6",
    duration: "Build ninth",
    modules: [
      "graph_engine/graph.py",
    ],
    why: "Sanctions evasion rarely happens in isolation. A sanctioned person creates a clean-looking company, uses family members, routes money through associates. Name matching catches the person but misses the network. The graph engine builds connections between entities (shared country, shared name tokens, shared ID prefixes) and propagates risk through the network — raising the score of connected entities even if they don't appear on any list.",
    what_you_can_test: [
      "Graph builds with 500 entities as nodes and edges connecting related ones",
      "propagate_risk(G, iterations=3) raises risk of entities near sanctioned nodes",
      "A clean company (risk=0.30) connected to 4 sanctioned entities → risk rises above 0.60",
      "get_connected_components(G) returns the largest cluster (highest-risk network)",
      "Graph analysis completes in < 30 seconds for 500 nodes",
    ],
    checkpoint: "Build graph from 500 batch results. Find the largest connected cluster. Verify that risk propagation raises scores for indirectly connected entities.",
    depends_on: ["Phase 6 — Orchestrator", "Phase 8 — ML"],
    unlocks: ["Phase 10 — Monitoring"],
    pitfalls: [
      "Graph edges are probabilistic connections, not confirmed relationships",
      "Risk propagation must be damped (damping=0.15) — without damping, all nodes converge to the same score",
      "Graph is a supplement to name-matching, not a replacement",
    ],
  },
  {
    num: 10,
    label: "VISIBILITY",
    title: "Monitoring + KPIs",
    subtitle: "Measure everything. Know if the system is working or drifting.",
    color: "#06b6d4",
    duration: "Build last",
    modules: [
      "monitoring/kpi.py",
    ],
    why: "A screening system that produces no metrics is a black box. You cannot tell if it's getting worse over time, generating too many false positives, or missing real criminals. KPIs measure: recall (are we catching the bad guys?), precision (are we wasting analyst time?), false discovery rate (how many alerts are wrong?), and score distribution drift (is the model going stale?).",
    what_you_can_test: [
      "recall = TP / (TP + FN) — correctly computed, not confused with precision",
      "FDR = FP / (TP + FP) — how many alerts are wrong? Target < 30%",
      "FPR = FP / (FP + TN) — how often do innocent people get flagged?",
      "KS drift test alerts if score distribution shifts by > threshold",
      "kpi_report() produces a summary DataFrame that can be exported",
    ],
    checkpoint: "Run full pipeline on 200 records. kpi_report() shows recall > 0.75, FDR < 0.40. Score distribution plot shows clear separation between positive and negative classes.",
    depends_on: ["All previous phases"],
    unlocks: ["Complete system — ready for notebook walkthrough"],
    pitfalls: [
      "Do NOT use ROC-AUC as your primary metric — it is misleading for imbalanced data",
      "FPR and FDR are DIFFERENT metrics — easy to confuse them",
      "Drift detection needs a baseline — store the score distribution from Phase 8 training",
    ],
  },
];

const DEPENDENCY_LINES = [
  { from:1, to:2 }, { from:1, to:3 }, { from:1, to:4 },
  { from:2, to:5 }, { from:3, to:5 }, { from:4, to:5 },
  { from:5, to:6 }, { from:6, to:7 }, { from:6, to:8 },
  { from:7, to:10 }, { from:8, to:9 }, { from:9, to:10 },
];

// ─── COMPONENT ─────────────────────────────────────────────────────────────
export default function BuildRoadmap() {
  const [active, setActive] = useState(1);
  const [tab, setTab] = useState("detail"); // detail | test | pitfalls

  const phase = PHASES[active - 1];

  return (
    <div style={{ fontFamily: BODY, background: "#08080c", minHeight: "100vh",
      color: "#e2e8f0", display: "flex", flexDirection: "column" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Nunito:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
        ::-webkit-scrollbar{width:4px;height:4px}
        ::-webkit-scrollbar-track{background:#0d0d12}
        ::-webkit-scrollbar-thumb{background:#2a2a35;border-radius:2px}
        .phase-btn{cursor:pointer;border:none;background:none;text-align:left;width:100%;transition:all 0.12s;}
        .phase-btn:hover{background:#12121a;}
        .tab-btn{cursor:pointer;border:none;background:none;font-family:'Nunito',sans-serif;
          font-size:12px;font-weight:600;padding:6px 14px;border-radius:4px;transition:all 0.12s;}
      `}</style>

      {/* ── Header ── */}
      <div style={{ padding:"16px 22px 12px", borderBottom:"1px solid #1a1a24",
        background:"#08080c", flexShrink:0 }}>
        <div style={{ fontFamily:MONO, fontSize:9, letterSpacing:"0.2em",
          color:"#3f3f52", marginBottom:4 }}>AML ENTERPRISE — BUILD ORDER</div>
        <div style={{ fontFamily:DISPLAY, fontSize:26, letterSpacing:"0.05em", color:"#f4f4f8" }}>
          WHAT TO BUILD FIRST
        </div>
        <div style={{ fontSize:12, color:"#52525e", marginTop:2 }}>
          10 phases · click any phase to see what it does, how to test it, and common mistakes
        </div>
      </div>

      <div style={{ display:"flex", flex:1, overflow:"hidden" }}>

        {/* ── Left sidebar: phase list ── */}
        <div style={{ width:200, borderRight:"1px solid #1a1a24",
          overflowY:"auto", flexShrink:0 }}>
          {PHASES.map(p => (
            <button key={p.num} className="phase-btn"
              onClick={() => setActive(p.num)}
              style={{ padding:"11px 14px", borderBottom:"1px solid #0d0d14",
                background: active === p.num ? p.color+"14" : "transparent" }}>
              <div style={{ display:"flex", gap:9, alignIt
