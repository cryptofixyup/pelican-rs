//! SPSC throughput benchmark.
//!
//! ## PMU usage (Linux)
//!
//! Run with perf to capture raw hardware counters:
//!
//! ```bash
//! # Pin producer to core 0, consumer to core 4 (same L3 domain — adjust for your topology)
//! taskset -c 0,4 perf stat \
//!   -e cache-misses,LLC-load-misses,LLC-store-misses \
//!   -e cpu/event=0xb7,umask=0x01,offmask=0x3600000000/u \
//!   cargo bench --bench throughput 2>&1 | tee pmu-same-l3.txt
//!
//! # Repeat with cores on different L3 domains, e.g. 0,8
//! taskset -c 0,8 perf stat \
//!   -e cache-misses,LLC-load-misses,LLC-store-misses \
//!   cargo bench --bench throughput 2>&1 | tee pmu-cross-l3.txt
//!
//! # diff the two artifacts to discriminate cache-coherence mechanism
//! ```
//!
//! ## Thread affinity
//!
//! Set `SPSC_PRODUCER_CORE` and `SPSC_CONSUMER_CORE` env vars to override defaults (0, 1).
//!
//! ## Governing invariant
//!
//! "Performance difference observed; false sharing not demonstrated; mechanism remains unresolved."
//! This benchmark produces the statistical distribution. PMU artifacts discriminate the mechanism.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use spsc::channel;
use std::thread;

const BATCH: u64 = 100_000;

fn bench_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_throughput");
    group.throughput(Throughput::Elements(BATCH));
    group.sample_size(50);

    for cap in [64usize, 256, 1024, 4096] {
        group.bench_with_input(
            BenchmarkId::new("aligned_ring", cap),
            &cap,
            |b, &cap| {
                b.iter(|| {
                    let (tx, rx) = channel::<u64>(cap);
                    let producer = thread::spawn(move || {
                        let mut sent = 0u64;
                        while sent < BATCH {
                            if tx.try_send(black_box(sent)).is_ok() {
                                sent += 1;
                            }
                        }
                    });
                    let mut received = 0u64;
                    while received < BATCH {
                        if let Some(v) = rx.try_recv() {
                            black_box(v);
                            received += 1;
                        }
                    }
                    producer.join().unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_spsc);
criterion_main!(benches);
