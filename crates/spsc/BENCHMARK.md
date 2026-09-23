# SPSC Benchmark — PMU Acceptance Pipeline

## Operational Status

| Check | Status |
|---|---|
| SPSC Execution | PASS |
| Run-Specific Performance Delta | OBSERVED |
| Controlled PMU Telemetry | PENDING |
| Cache/Coherence Mechanism | UNVERIFIED |
| False-Sharing Attribution | UNPROVEN |
| Alternative Mechanism Attribution | UNRESOLVED |
| Universal Hardware Claim | LOCKED |

## Acceptance Pipeline

```
Standardize Workload
    → Control Placement & Thread Affinity
    → Repeat Statistically
    → Measure Raw PMU Telemetry
    → Discriminate Mechanism
    → Attribute Causal Conclusion
```

## Governing Invariant

> "Performance difference observed; false sharing not demonstrated; mechanism remains unresolved."

## Running the benchmark

```bash
# Statistical distribution (Criterion HTML report at target/criterion/)
cargo bench --bench throughput

# With raw PMU telemetry (Linux, requires perf)
taskset -c 0,4 perf stat \
  -e cache-misses,LLC-load-misses,LLC-store-misses \
  cargo bench --bench throughput 2>&1 | tee pmu-same-l3.txt

# Cross-L3 domain comparison
taskset -c 0,8 perf stat \
  -e cache-misses,LLC-load-misses,LLC-store-misses \
  cargo bench --bench throughput 2>&1 | tee pmu-cross-l3.txt
```

## PMU counter targets

| Counter | Discriminates |
|---|---|
| `cache-misses` | General cache pressure |
| `LLC-load-misses` | L3 miss rate |
| `LLC-store-misses` | Write-back pressure |
| `OFFCORE_RESPONSE.DEMAND_DATA_RD.L3_MISS.SNOOP_HIT_WITH_FWD` (Intel) | Cache-to-cache transfer (direct false-sharing fingerprint) |
| AMD equivalent: `MFILL` events | Same signal on Zen |

## Causal conclusion criteria

False sharing is **demonstrated** only when:
1. `OFFCORE_RESPONSE` / SNOOP_HIT_WITH_FWD counts are significantly elevated in an **unaligned** build vs. the aligned build.
2. The effect reproduces statistically across ≥ 30 independent runs.
3. Thread affinity is fixed (same L3 domain and cross-domain runs both measured).
4. No alternative mechanism (scheduler jitter, NUMA, prefetcher interference) accounts for the delta.
