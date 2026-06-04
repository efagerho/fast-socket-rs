//! Criterion benchmarks for the Poptrie crate.
//!
//! Run with `cargo bench -p poptrie`. The benchmark covers IPv4 longest-prefix
//! lookup latency, a linear-scan baseline for small/medium route tables, and
//! builder throughput.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use poptrie::Poptrie;

const LOOKUP_KEYS: usize = 1 << 16;
const REFERENCE_CHECK_KEYS: usize = 512;

/// Small, fast, dependency-free PRNG (xorshift64).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }
}

/// Generates pseudo-random prefixes biased toward the lengths common in IPv4
/// routing tables: many /24s, fewer short prefixes, plus a default route.
fn synthetic_prefixes(count: usize, seed: u64) -> Vec<(u32, u8, u32)> {
    let mut rng = Rng::new(seed);
    let mut prefixes = Vec::with_capacity(count + 1);
    prefixes.push((0, 0, 0));
    for value in 1..=count as u32 {
        let len = match rng.next_u32() % 100 {
            0..=4 => 8 + (rng.next_u32() % 8) as u8,
            5..=24 => 16 + (rng.next_u32() % 8) as u8,
            25..=89 => 24,
            _ => 25 + (rng.next_u32() % 8) as u8,
        };
        let mask = if len == 0 {
            0
        } else {
            u32::MAX << (32 - u32::from(len))
        };
        prefixes.push((rng.next_u32() & mask, len, value));
    }
    prefixes
}

fn random_keys(count: usize, seed: u64) -> Vec<u32> {
    let mut rng = Rng::new(seed);
    (0..count).map(|_| rng.next_u32()).collect()
}

/// Reference longest-prefix-match by linear scan.
#[inline]
fn linear_lookup(prefixes: &[(u32, u8, u32)], key: u32) -> Option<u32> {
    let mut best: Option<(u8, u32)> = None;
    for &(prefix, len, value) in prefixes {
        let mask = if len == 0 {
            0
        } else {
            u32::MAX << (32 - u32::from(len))
        };
        if (key & mask) == (prefix & mask) && best.is_none_or(|(best_len, _)| len >= best_len) {
            best = Some((len, value));
        }
    }
    best.map(|(_, value)| value)
}

fn build_table<const DIRECT_BITS: u32>(
    prefixes: &[(u32, u8, u32)],
) -> Poptrie<u32, u32, DIRECT_BITS> {
    let mut builder = Poptrie::<u32, u32, DIRECT_BITS>::builder();
    for &(prefix, len, value) in prefixes {
        builder.insert(prefix, len, value);
    }
    builder.build()
}

fn assert_matches_reference<const DIRECT_BITS: u32>(
    table: &Poptrie<u32, u32, DIRECT_BITS>,
    prefixes: &[(u32, u8, u32)],
    keys: &[u32],
) {
    for &key in keys.iter().take(REFERENCE_CHECK_KEYS) {
        assert_eq!(table.lookup(key).copied(), linear_lookup(prefixes, key));
    }
}

fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipv4_lookup");
    group.throughput(Throughput::Elements(1));

    for &count in &[8usize, 64, 512, 4_096, 50_000] {
        let prefixes = synthetic_prefixes(count, 0xA076_1D64_78BD_642F ^ count as u64);
        let keys = random_keys(LOOKUP_KEYS, 0xE703_7ED1_A0B4_28DB ^ count as u64);
        let table = build_table::<14>(&prefixes);
        assert_matches_reference(&table, &prefixes, &keys);

        group.bench_function(BenchmarkId::new("poptrie_direct14", count), |b| {
            let mut key_index = 0usize;
            b.iter(|| {
                let key = keys[key_index];
                key_index += 1;
                if key_index == keys.len() {
                    key_index = 0;
                }
                black_box(table.lookup(black_box(key)).copied().unwrap_or(0))
            });
        });
    }

    for &count in &[8usize, 64, 512, 4_096] {
        let prefixes = synthetic_prefixes(count, 0xA076_1D64_78BD_642F ^ count as u64);
        let keys = random_keys(LOOKUP_KEYS, 0xE703_7ED1_A0B4_28DB ^ count as u64);

        group.bench_function(BenchmarkId::new("linear_scan", count), |b| {
            let mut key_index = 0usize;
            b.iter(|| {
                let key = keys[key_index];
                key_index += 1;
                if key_index == keys.len() {
                    key_index = 0;
                }
                black_box(
                    linear_lookup(black_box(prefixes.as_slice()), black_box(key)).unwrap_or(0),
                )
            });
        });
    }

    group.finish();
}

fn bench_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipv4_build_direct14");

    for &count in &[8usize, 64, 512, 4_096, 50_000] {
        let prefixes = synthetic_prefixes(count, 0x8EBC_6AF0_9C88_C6E3 ^ count as u64);
        group.throughput(Throughput::Elements(prefixes.len() as u64));
        group.bench_function(BenchmarkId::from_parameter(count), |b| {
            b.iter(|| black_box(build_table::<14>(black_box(prefixes.as_slice()))));
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(20);
    targets = bench_lookup, bench_build
}
criterion_main!(benches);
