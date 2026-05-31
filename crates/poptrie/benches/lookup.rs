//! Lookup throughput benchmark for the Poptrie crate.
//!
//! Run with `cargo bench -p poptrie` (release). It compares Poptrie longest-
//! prefix-match against the naive linear scan it is meant to replace, across
//! table sizes, and reports per-lookup latency, throughput, build time, and
//! memory footprint. No external bench harness is used (manual timing, matching
//! the other benches in this workspace).

use std::hint::black_box;
use std::time::Instant;

use poptrie::Ipv4Poptrie;

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

/// Generates `count` pseudo-random prefixes biased toward the lengths seen in
/// real routing tables (lots of /24, fewer short prefixes), plus a default.
fn synthetic_prefixes(count: usize, rng: &mut Rng) -> Vec<(u32, u8, u32)> {
    let mut prefixes = Vec::with_capacity(count + 1);
    prefixes.push((0, 0, 0)); // default route, value 0
    for value in 1..=count as u32 {
        // Bias the length distribution toward /24.
        let len = match rng.next_u32() % 100 {
            0..=4 => 8 + (rng.next_u32() % 8) as u8,   // /8..=/15
            5..=24 => 16 + (rng.next_u32() % 8) as u8, // /16..=/23
            25..=89 => 24,                             // /24 (the bulk)
            _ => 25 + (rng.next_u32() % 8) as u8,      // /25..=/32
        };
        let mask = if len == 0 {
            0
        } else {
            u32::MAX << (32 - u32::from(len))
        };
        let prefix = rng.next_u32() & mask;
        prefixes.push((prefix, len, value));
    }
    prefixes
}

/// Reference longest-prefix-match by linear scan — exactly the per-packet cost
/// this structure removes.
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

fn build_table(prefixes: &[(u32, u8, u32)]) -> Ipv4Poptrie<u32> {
    let mut builder = Ipv4Poptrie::builder();
    for &(prefix, len, value) in prefixes {
        builder.insert(prefix, len, value);
    }
    builder.build()
}

fn random_keys(count: usize, rng: &mut Rng) -> Vec<u32> {
    (0..count).map(|_| rng.next_u32()).collect()
}

fn main() {
    println!("poptrie lookup benchmark (IPv4, 14-bit direct root)\n");

    // Head-to-head against linear scan at small/medium sizes — the regime the
    // fast-socket route table lives in, where the linear cost is per packet.
    println!("== Poptrie vs linear scan ==");
    println!(
        "{:>10} {:>14} {:>14} {:>10}",
        "prefixes", "poptrie ns/op", "linear ns/op", "speedup"
    );
    let mut rng = Rng::new(0xDEAD_BEEF_CAFE_F00D);
    let keys = random_keys(1 << 20, &mut rng);
    for &count in &[8usize, 64, 512, 4096] {
        let prefixes = synthetic_prefixes(count, &mut rng);
        let table = build_table(&prefixes);

        // Correctness sanity check before timing.
        for &key in keys.iter().take(2000) {
            assert_eq!(table.lookup(key).copied(), linear_lookup(&prefixes, key));
        }

        let start = Instant::now();
        let mut acc = 0u64;
        for &key in &keys {
            acc = acc.wrapping_add(
                black_box(table.lookup(black_box(key)))
                    .copied()
                    .unwrap_or(0) as u64,
            );
        }
        let poptrie_ns = start.elapsed().as_nanos() as f64 / keys.len() as f64;
        black_box(acc);

        let start = Instant::now();
        let mut acc = 0u64;
        for &key in &keys {
            acc = acc.wrapping_add(
                black_box(linear_lookup(&prefixes, black_box(key))).unwrap_or(0) as u64,
            );
        }
        let linear_ns = start.elapsed().as_nanos() as f64 / keys.len() as f64;
        black_box(acc);

        println!(
            "{count:>10} {poptrie_ns:>14.2} {linear_ns:>14.2} {:>9.1}x",
            linear_ns / poptrie_ns
        );
    }

    // Scaling: Poptrie throughput on large tables (linear scan is impractical
    // here). Reports build time and footprint so DIRECT_BITS can be tuned.
    println!("\n== Poptrie scaling (large tables) ==");
    println!(
        "{:>10} {:>12} {:>12} {:>12} {:>12}",
        "prefixes", "build ms", "ns/lookup", "Mlookups/s", "heap KiB"
    );
    let lookups = random_keys(8 << 20, &mut rng);
    for &count in &[50_000usize, 200_000, 800_000] {
        let prefixes = synthetic_prefixes(count, &mut rng);

        let start = Instant::now();
        let table = build_table(&prefixes);
        let build_ms = start.elapsed().as_secs_f64() * 1e3;

        let start = Instant::now();
        let mut acc = 0u64;
        for &key in &lookups {
            acc = acc.wrapping_add(
                black_box(table.lookup(black_box(key)))
                    .copied()
                    .unwrap_or(0) as u64,
            );
        }
        let elapsed = start.elapsed();
        black_box(acc);

        let ns = elapsed.as_nanos() as f64 / lookups.len() as f64;
        let mlps = lookups.len() as f64 / elapsed.as_secs_f64() / 1e6;
        println!(
            "{count:>10} {build_ms:>12.2} {ns:>12.2} {mlps:>12.1} {:>12}",
            table.heap_bytes() / 1024
        );
    }
}
