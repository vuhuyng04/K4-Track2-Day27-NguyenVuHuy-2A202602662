# Reflection (≤1 page)

**Results.** Practice 49.66 (TPR 1.00, FPR 0.011). Public 47.69 (TPR 1.00, FPR 0.017, cost 240/220). **Private 31.28 (TPR 0.63, FPR 0.007, no overage).**

**Which fault types were hardest to catch, and why?**

- **Subtle-magnitude statistical faults:** volume, null-rate, freshness/staleness, distribution, feature skew, embedding drift and corpus age.
  - Practice made them look easy. Clean values sat well inside the published mean ± 3σ bands, and faults sat far outside them (row_count 800+ vs ~500, doc age 128+ days vs ~26).
  - On private I caught only ~63% of faults, at almost zero false alarms. The private faults therefore sit inside the region my thresholds treated as normal.
  - A per-metric threshold alone can't separate a 2σ fault from a 2σ clean tail. Only one sample of each event is visible, and the score caps how many false alarms you can afford.
- **One-sided metrics** (null rate, staleness, freshness delay, lineage duration, centroid shift, doc age):
  - The baseline publishes only a max (= mean + 3σ). I estimated the centre with a running median over the stream and derived σ = (max − centre)/3.
  - Early in the stream that estimate is unavailable. On public, alerting only above the bare max during the first 5 samples missed 2 faults. Dropping the warm-up to 3 samples (a median of 3 already tolerates one faulty point) recovered both.
- **Structural faults were easy and exact:**
  - Contract `violations` and lineage `actual_downstream_count == 0` are binary.
  - Missing upstream needed one insight: the payload's declared `inputs` list only `raw.orders`, while clean runs actually read `raw.orders` + `raw.customers`. So the expected upstream set has to be learned from the stream (modal set per job), not taken from the payload.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

- **Cost:** one metered call per event, never repeated, is the right shape.
  - A metered call is worth ≈ P(fault) × 0.5/n_faulty ≈ 0.24 score points.
  - One credit over budget costs only ≈ 0.06 points.
  - So I never skip an event to save budget. Public went 9% over and that was still the better trade.
- **Coverage:** I tuned too conservatively.
  - K = 2.5σ was the knee on practice/public: 2.25 added false alarms, 3.0 lost a true positive. Those streams contained almost no near-boundary faults, so the knee says little about private.
  - With a true positive worth ~4 false positives, the optimum on a subtle-heavy stream is lower, around 2σ. Next pass I'd choose K from that payoff ratio instead of from the knee observed on easy streams.
  - I would also combine signals per event, e.g. a Mahalanobis / χ² score over row_count, mean_amount, std_amount, null rate and staleness together, so two moderate deviations add up to an alert even when neither crosses its own line.
  - Per-metric CUSUM/EWMA over the stream would catch faults that persist across several events.
