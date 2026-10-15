"""
Data Siege defense.

Strategy
--------
* Exactly one metered call per event (the only tool that can see that event),
  never a repeat: full single-pass coverage fits the budget, a second call
  never adds information.
* Score economics: a caught fault is worth 0.5/n_faulty, a false alarm costs
  0.3/n_clean. With ~25-30% faulty events one TP is worth ~4 FPs, so the
  alert threshold sits somewhat inside the published mean +/- 3 sigma bands
  rather than on them (K_SIGMA below), to reach subtle faults.
* ctx.baseline gives mean +/- 3 sigma bounds. Two-sided metrics give mean and
  sigma directly. One-sided metrics (only a max is published) get their
  centre from a running median of what this stream has shown so far (robust
  to the faults mixed into it), and sigma = (max - centre) / 3.
* Structural signals (contract violations, lineage edges / outputs) are
  binary: any mismatch alerts. The "normal" upstream set and downstream count
  are learned from the stream itself (modal value), because the payload's
  declared inputs are not the full upstream set.
"""
from collections import Counter

from api import Verdict

# How many implied sigmas from the centre before alerting. 3.0 == the bare
# published baseline; lower trades a few false alarms for subtle-fault recall.
K_SIGMA = 2.5
# Feature skew is |serve - train| / train_std, a half-normal quantity whose
# clean tail is heavy relative to its median, so stay closer to the baseline.
K_SIGMA_FEATURE = 2.8
# Running median needs a few points before it beats the published max alone;
# a median of 3 already shrugs off one faulty sample, and waiting longer leaves
# early subtle faults (and each new lineage job's first runs) uncovered.
MIN_HISTORY = 3
# std_amount has no published baseline: robust z (median / MAD) on history.
K_MAD = 4.0
MIN_HISTORY_MAD = 10


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


# ---------------------------------------------------------------- helpers

def _median(xs):
    # statistics.median is unusable here: the sandbox blocks its 'numbers' import.
    xs = sorted(xs)
    n = len(xs)
    mid = n // 2
    return xs[mid] if n % 2 else (xs[mid - 1] + xs[mid]) / 2.0


def _history(ctx, metric):
    return ctx.state.setdefault("hist", {}).setdefault(metric, [])


def _two_sided(value, lo, hi, k=None):
    """Published band [lo, hi] is mean +/- 3 sigma."""
    k = K_SIGMA if k is None else k
    mean = (lo + hi) / 2.0
    sigma = (hi - lo) / 6.0
    return sigma > 0 and abs(value - mean) > k * sigma


def _one_sided(ctx, metric, value, hi, k=None):
    """Only hi = mean + 3 sigma is published; centre from running median."""
    k = K_SIGMA if k is None else k
    hist = _history(ctx, metric)
    if value > hi:
        flagged = True
    elif len(hist) >= MIN_HISTORY:
        centre = _median(hist)
        sigma = (hi - centre) / 3.0
        flagged = sigma > 0 and value > centre + k * sigma
    else:
        flagged = False
    hist.append(value)
    return flagged


def _robust_outlier(ctx, metric, value, k=None):
    """No baseline at all: median / MAD z-score against this stream."""
    k = K_MAD if k is None else k
    hist = _history(ctx, metric)
    flagged = False
    if len(hist) >= MIN_HISTORY_MAD:
        med = _median(hist)
        mad = _median(abs(x - med) for x in hist)
        if mad > 0:
            flagged = abs(value - med) / (1.4826 * mad) > k
    hist.append(value)
    return flagged


def _modal(ctx, metric, value):
    """Most common value seen so far for metric (before adding value)."""
    counts = ctx.state.setdefault("modal", {}).setdefault(metric, Counter())
    mode = counts.most_common(1)[0][0] if counts else None
    counts[value] += 1
    return mode


def _bad(resp):
    return not isinstance(resp, dict) or "error" in resp


def _verdict(pillar, reasons):
    return Verdict(alert=bool(reasons), pillar=pillar, reason="; ".join(reasons),
                   confidence=1.0 if reasons else 0.0)


# ---------------------------------------------------------------- handlers

def check_data_batch(payload, ctx):
    p = ctx.tools.batch_profile(payload["batch_id"])
    if _bad(p):
        return Verdict(alert=False, pillar="checks", reason="profile unavailable")
    b = ctx.baseline
    reasons = []

    if _two_sided(p["row_count"], b["row_count_min"], b["row_count_max"]):
        reasons.append(f"volume row_count={p['row_count']}")
    if _two_sided(p["mean_amount"], b["mean_amount_min"], b["mean_amount_max"]):
        reasons.append(f"distribution mean_amount={p['mean_amount']}")
    null_rate = (p.get("null_rate") or {}).get("customer_id", 0.0)
    if _one_sided(ctx, "null_rate", null_rate, b["null_rate_max"]):
        reasons.append(f"null_rate={null_rate}")
    if _one_sided(ctx, "staleness", p["staleness_min"], b["staleness_min_max"]):
        reasons.append(f"staleness={p['staleness_min']}")
    if _robust_outlier(ctx, "std_amount", p["std_amount"]):
        reasons.append(f"distribution std_amount={p['std_amount']}")
    return _verdict("checks", reasons)


def check_contract_checkpoint(payload, ctx):
    d = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if _bad(d):
        return Verdict(alert=False, pillar="contracts", reason="diff unavailable")
    reasons = list(d.get("violations") or [])

    delay = d["freshness_delay_min"]
    sla = (payload.get("declared_sla") or {}).get("freshness_min")
    if sla is not None and delay > sla:
        reasons.append(f"sla freshness {delay} > {sla}")
    elif _one_sided(ctx, "freshness_delay", delay, ctx.baseline["freshness_delay_max_min"]):
        reasons.append(f"freshness_delay={delay}")
    return _verdict("contracts", reasons)


def check_lineage_run(payload, ctx):
    g = ctx.tools.lineage_graph_slice(payload["run_id"])
    if _bad(g):
        return Verdict(alert=False, pillar="lineage", reason="lineage unavailable")
    reasons = []
    job = payload.get("job", "")

    upstream = frozenset(g.get("actual_upstream") or [])
    declared = {i.get("name") for i in payload.get("inputs") or []}
    expected = _modal(ctx, f"upstream:{job}", upstream)
    missing = declared - upstream
    if expected is not None:
        missing |= expected - upstream
    if missing:
        reasons.append(f"missing_upstream={sorted(missing)}")

    downstream = g.get("actual_downstream_count")
    expected_down = _modal(ctx, f"downstream:{job}", downstream)
    if downstream == 0 or (expected_down is not None and downstream < expected_down):
        reasons.append(f"orphan_output downstream={downstream}")

    if _one_sided(ctx, f"duration:{job}", g["duration_ms"], ctx.baseline["lineage_duration_ms_max"]):
        reasons.append(f"runtime={g['duration_ms']}")
    return _verdict("lineage", reasons)


def check_feature_materialization(payload, ctx):
    f = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if _bad(f):
        return Verdict(alert=False, pillar="ai_infra", reason="drift unavailable")
    reasons = []
    if _one_sided(ctx, "feature_shift", f["mean_shift_sigma"],
                  ctx.baseline["feature_mean_shift_sigma_max"], k=K_SIGMA_FEATURE):
        reasons.append(f"feature_skew sigma={f['mean_shift_sigma']}")
    return _verdict("ai_infra", reasons)


def check_embedding_batch(payload, ctx):
    e = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if _bad(e):
        return Verdict(alert=False, pillar="ai_infra", reason="drift unavailable")
    b = ctx.baseline
    reasons = []
    if _one_sided(ctx, "centroid_shift", e["centroid_shift"], b["embedding_centroid_shift_max"]):
        reasons.append(f"embedding_drift={e['centroid_shift']}")
    if _one_sided(ctx, "doc_age", e["avg_doc_age_days"], b["corpus_avg_doc_age_days_max"]):
        reasons.append(f"corpus_staleness={e['avg_doc_age_days']}")
    return _verdict("ai_infra", reasons)
