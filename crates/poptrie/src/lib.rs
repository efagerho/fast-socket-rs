//! Fast longest-prefix-match (LPM) IP route lookups using a **Poptrie**.
//!
//! A Poptrie is a multibit trie that compresses each node with a population
//! count (`popcnt`) over a 64-bit bitmap, giving very cache-efficient,
//! branch-light lookups. It is the data structure from Asai & Ohara,
//! *"Poptrie: A Compressed Trie with Population Count for Fast and Scalable
//! Software IP Routing Table Lookup"* (SIGCOMM 2015), and is one of the fastest
//! software LPM structures published.
//!
//! This crate is dependency-free and `#![forbid(unsafe_code)]`: the lookups are
//! plain safe indexing, and the speed comes from the algorithm, not from
//! skipping bounds checks.
//!
//! # The three optimizations that matter
//!
//! 1. **6-bit stride + `u64` bitmaps.** Each internal node fans out 64 ways. A
//!    node stores two 64-bit bitmaps and two base offsets; the child for a
//!    6-bit chunk `v` is found with one `popcnt` of the bitmap masked below
//!    `v`. Six bits is chosen so the bitmap is exactly one `u64` and the mask
//!    popcount maps to a single hardware instruction
//!    ([`u64::count_ones`]).
//! 2. **Direct pointing.** The top `DIRECT_BITS` of the address index a flat
//!    array, skipping the first few trie levels entirely. Each entry is either
//!    a route (for an address range with no longer prefixes) or a pointer to
//!    the trie node that resolves the rest of the address. This turns the
//!    common case into one array load plus a short walk.
//! 3. **Leaf compression.** Within a node, a run of adjacent 6-bit slots that
//!    resolve to the same route is stored as a *single* leaf entry, so the
//!    leaf array scales with the number of distinct route boundaries rather
//!    than with 64 × node count.
//!
//! # Layout and tuning
//!
//! The structure is built once (offline / on route change) and is immutable and
//! read-optimized afterwards. `DIRECT_BITS` is a tuning knob exposed as a const
//! generic: larger values make lookups shallower at the cost of a `4 *
//! 2^DIRECT_BITS`-byte array. The bits below the direct-pointing root must tile
//! into whole 6-bit strides, i.e. `(K::BITS - DIRECT_BITS) % 6 == 0`; this is
//! checked when the trie is built.
//!
//! The provided [`Ipv4Poptrie`] / [`Ipv6Poptrie`] aliases use a 14-bit direct
//! array (64 KiB), a good general default. For a large table where lookup
//! latency dominates, instantiate [`Poptrie`] directly with a larger
//! `DIRECT_BITS` (for IPv4: 20 → a 4 MiB array and at most two node hops).
//!
//! # Example
//!
//! ```
//! use std::net::Ipv4Addr;
//! use poptrie::Ipv4Poptrie;
//!
//! let mut builder = Ipv4Poptrie::builder();
//! builder
//!     .insert(u32::from(Ipv4Addr::new(0, 0, 0, 0)), 0, "default")
//!     .insert(u32::from(Ipv4Addr::new(10, 0, 0, 0)), 8, "ten")
//!     .insert(u32::from(Ipv4Addr::new(10, 1, 2, 0)), 24, "lan");
//! let table = builder.build();
//!
//! assert_eq!(table.lookup(u32::from(Ipv4Addr::new(10, 1, 2, 7))), Some(&"lan"));
//! assert_eq!(table.lookup(u32::from(Ipv4Addr::new(10, 9, 9, 9))), Some(&"ten"));
//! assert_eq!(table.lookup(u32::from(Ipv4Addr::new(8, 8, 8, 8))), Some(&"default"));
//! ```

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod build;

pub use build::PoptrieBuilder;

use std::marker::PhantomData;

/// Stride width in bits. Six bits exactly fills a `u64` bitmap and maps the
/// masked population count to a single hardware `popcnt`.
const STRIDE: u32 = 6;

/// Top bit of a direct-array / leaf encoding: set means "leaf" (a resolved
/// route), clear means "internal node index".
const LEAF_FLAG: u32 = 1 << 31;
/// The value-index payload below [`LEAF_FLAG`]. A payload of `0` means "no
/// route"; `n > 0` refers to value `n - 1`.
const VALUE_MASK: u32 = !LEAF_FLAG;

/// A fixed-width IP address usable as a Poptrie key.
///
/// Implemented for [`u32`] (IPv4) and [`u128`] (IPv6). All methods view the
/// address most-significant-bit first, matching CIDR prefix ordering.
pub trait IpKey: Copy + Eq + Ord + std::hash::Hash {
    /// Address width in bits (32 for IPv4, 128 for IPv6).
    const BITS: u32;

    /// Returns bit `i` counting from the most significant bit (`i == 0`).
    ///
    /// Used by the builder to walk a prefix; `i` is always `< BITS`.
    fn bit(self, i: u32) -> bool;

    /// Extracts the 6-bit stride chunk starting `offset` bits from the MSB,
    /// returned in the low 6 bits. The caller guarantees `offset + 6 <= BITS`.
    fn stride(self, offset: u32) -> u32;

    /// Returns the top `bits` bits as an array index (`bits <= BITS`).
    fn direct(self, bits: u32) -> usize;
}

impl IpKey for u32 {
    const BITS: u32 = 32;

    #[inline]
    fn bit(self, i: u32) -> bool {
        (self >> (Self::BITS - 1 - i)) & 1 == 1
    }

    #[inline]
    fn stride(self, offset: u32) -> u32 {
        (self >> (Self::BITS - offset - STRIDE)) & 0x3f
    }

    #[inline]
    fn direct(self, bits: u32) -> usize {
        (self >> (Self::BITS - bits)) as usize
    }
}

impl IpKey for u128 {
    const BITS: u32 = 128;

    #[inline]
    fn bit(self, i: u32) -> bool {
        (self >> (Self::BITS - 1 - i)) & 1 == 1
    }

    #[inline]
    fn stride(self, offset: u32) -> u32 {
        ((self >> (Self::BITS - offset - STRIDE)) as u32) & 0x3f
    }

    #[inline]
    fn direct(self, bits: u32) -> usize {
        (self >> (Self::BITS - bits)) as usize
    }
}

/// A Poptrie internal node: two 64-bit bitmaps and two base offsets.
///
/// Field order keeps the size at 24 bytes with no padding (`u64`, `u64`,
/// `u32`, `u32`) so a node never straddles more cache lines than necessary,
/// and the hot internal-descent reads `vector` (offset 0) then `base1`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Node {
    /// Bit `i` set ⇒ stride slot `i` is an internal child node.
    vector: u64,
    /// Bit `i` set ⇒ stride slot `i` begins a new leaf run.
    leafvec: u64,
    /// Index into `leaves` of this node's first leaf run.
    base0: u32,
    /// Index into `nodes` of this node's first child.
    base1: u32,
}

const _: () = assert!(std::mem::size_of::<Node>() == 24);

/// An immutable, read-optimized Poptrie over keys of type `K` holding values of
/// type `V`.
///
/// Construct one with [`Poptrie::builder`] (or the [`Ipv4Poptrie`] /
/// [`Ipv6Poptrie`] aliases). `DIRECT_BITS` is the direct-pointing root width;
/// see the [crate docs](crate) for tuning.
pub struct Poptrie<K: IpKey, V, const DIRECT_BITS: u32> {
    /// `2^DIRECT_BITS` entries. Top bit set ⇒ leaf (value payload below it);
    /// clear ⇒ index into `nodes`.
    direct: Box<[u32]>,
    /// Internal trie nodes; a node's children are contiguous from `base1`.
    nodes: Box<[Node]>,
    /// Per-leaf-run value encodings: `0` = no route, `n` = `values[n - 1]`.
    leaves: Box<[u32]>,
    /// Distinct route values, deduplicated at build time.
    values: Box<[V]>,
    _key: PhantomData<fn() -> K>,
}

impl<K: IpKey, V, const DIRECT_BITS: u32> Poptrie<K, V, DIRECT_BITS> {
    /// Returns a new builder for this Poptrie configuration.
    #[must_use]
    pub fn builder() -> PoptrieBuilder<K, V, DIRECT_BITS>
    where
        V: Clone + Eq + std::hash::Hash,
    {
        PoptrieBuilder::new()
    }

    /// Returns the value of the most specific prefix matching `key`, or `None`
    /// when no inserted prefix (including a default route) covers it.
    ///
    /// This is the hot path: one direct-array load, then at most
    /// `(K::BITS - DIRECT_BITS) / 6` node hops, each a `popcnt`.
    #[inline]
    #[must_use]
    pub fn lookup(&self, key: K) -> Option<&V> {
        let entry = self.direct[key.direct(DIRECT_BITS)];
        if entry & LEAF_FLAG != 0 {
            return self.value(entry & VALUE_MASK);
        }

        let mut node_index = entry as usize;
        let mut offset = DIRECT_BITS;
        loop {
            let node = &self.nodes[node_index];
            let v = key.stride(offset);
            offset += STRIDE;
            let bit = 1u64 << v;
            if node.vector & bit != 0 {
                // Internal child: count children below `v`.
                node_index = node.base1 as usize + (node.vector & (bit - 1)).count_ones() as usize;
            } else {
                // Leaf: count leaf runs at or below `v`. `bit | (bit - 1)` is the
                // inclusive low mask `0..=v` and avoids overflow when `v == 63`.
                let mask = bit | (bit - 1);
                let runs = (node.leafvec & mask).count_ones() as usize;
                debug_assert!(runs >= 1, "leaf slot must be covered by a leaf run");
                return self.value(self.leaves[node.base0 as usize + runs - 1]);
            }
        }
    }

    /// Resolves a value encoding (`0` = no route) to a borrowed value.
    #[inline]
    fn value(&self, encoding: u32) -> Option<&V> {
        if encoding == 0 {
            None
        } else {
            Some(&self.values[encoding as usize - 1])
        }
    }

    /// Number of distinct route values stored.
    #[must_use]
    pub fn value_count(&self) -> usize {
        self.values.len()
    }

    /// Number of internal trie nodes.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Approximate heap footprint in bytes (direct array + nodes + leaves +
    /// value slots), useful for tuning `DIRECT_BITS`.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        self.direct.len() * std::mem::size_of::<u32>()
            + self.nodes.len() * std::mem::size_of::<Node>()
            + self.leaves.len() * std::mem::size_of::<u32>()
            + self.values.len() * std::mem::size_of::<V>()
    }
}

impl<K: IpKey, V, const DIRECT_BITS: u32> std::fmt::Debug for Poptrie<K, V, DIRECT_BITS> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Poptrie")
            .field("direct_bits", &DIRECT_BITS)
            .field("direct_entries", &self.direct.len())
            .field("nodes", &self.nodes.len())
            .field("leaves", &self.leaves.len())
            .field("values", &self.values.len())
            .finish()
    }
}

/// IPv4 Poptrie with a 14-bit (64 KiB) direct-pointing root and three 6-bit
/// strides below it. See the [crate docs](crate) for picking a different
/// `DIRECT_BITS`.
pub type Ipv4Poptrie<V> = Poptrie<u32, V, 14>;

/// IPv6 Poptrie with a 14-bit (64 KiB) direct-pointing root. IPv6 tables with
/// many `/32`–`/48` prefixes benefit from a larger `DIRECT_BITS`
/// (e.g. `Poptrie<u128, V, 20>`).
pub type Ipv6Poptrie<V> = Poptrie<u128, V, 14>;

/// Builder for an [`Ipv4Poptrie`].
pub type Ipv4PoptrieBuilder<V> = PoptrieBuilder<u32, V, 14>;

/// Builder for an [`Ipv6Poptrie`].
pub type Ipv6PoptrieBuilder<V> = PoptrieBuilder<u128, V, 14>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn v4(a: u8, b: u8, c: u8, d: u8) -> u32 {
        u32::from(Ipv4Addr::new(a, b, c, d))
    }

    /// Reference longest-prefix-match by exhaustive scan, for cross-checking.
    fn reference_lpm(prefixes: &[(u32, u8, u32)], key: u32) -> Option<u32> {
        let mut best: Option<(u8, u32)> = None;
        for &(p, len, val) in prefixes {
            let mask = if len == 0 {
                0
            } else {
                u32::MAX << (32 - len as u32)
            };
            if (key & mask) == (p & mask) && best.is_none_or(|(bl, _)| len >= bl) {
                best = Some((len, val));
            }
        }
        best.map(|(_, v)| v)
    }

    /// Reference IPv6 longest-prefix-match by exhaustive scan.
    fn reference_lpm_v6(prefixes: &[(u128, u8, u32)], key: u128) -> Option<u32> {
        let mut best: Option<(u8, u32)> = None;
        for &(p, len, val) in prefixes {
            let mask = if len == 0 {
                0
            } else {
                u128::MAX << (128 - len as u32)
            };
            if (key & mask) == (p & mask) && best.is_none_or(|(bl, _)| len >= bl) {
                best = Some((len, val));
            }
        }
        best.map(|(_, v)| v)
    }

    #[test]
    fn empty_table_misses_everything() {
        let table = Ipv4Poptrie::<&str>::builder().build();
        assert_eq!(table.lookup(v4(1, 2, 3, 4)), None);
        assert_eq!(table.lookup(0), None);
        assert_eq!(table.lookup(u32::MAX), None);
    }

    #[test]
    fn default_route_covers_all() {
        let mut b = Ipv4Poptrie::builder();
        b.insert(0, 0, "default");
        let table = b.build();
        assert_eq!(table.lookup(v4(8, 8, 8, 8)), Some(&"default"));
        assert_eq!(table.lookup(0), Some(&"default"));
        assert_eq!(table.lookup(u32::MAX), Some(&"default"));
    }

    #[test]
    fn longest_prefix_wins_regardless_of_insert_order() {
        let mut b = Ipv4Poptrie::builder();
        b.insert(v4(10, 1, 2, 0), 24, "lan")
            .insert(0, 0, "default")
            .insert(v4(10, 0, 0, 0), 8, "ten");
        let table = b.build();
        assert_eq!(table.lookup(v4(10, 1, 2, 200)), Some(&"lan"));
        assert_eq!(table.lookup(v4(10, 1, 3, 1)), Some(&"ten"));
        assert_eq!(table.lookup(v4(11, 0, 0, 1)), Some(&"default"));
    }

    #[test]
    fn host_route_slash_32_matches_exactly() {
        let mut b = Ipv4Poptrie::builder();
        b.insert(v4(192, 168, 0, 0), 16, "net")
            .insert(v4(192, 168, 5, 5), 32, "host");
        let table = b.build();
        assert_eq!(table.lookup(v4(192, 168, 5, 5)), Some(&"host"));
        assert_eq!(table.lookup(v4(192, 168, 5, 6)), Some(&"net"));
    }

    #[test]
    fn boundary_stride_slot_63() {
        // 0xFC = 0b1111_1100 — exercises the v == 63 mask path at a deep stride.
        let mut b = Ipv4Poptrie::builder();
        b.insert(v4(255, 255, 255, 255), 32, "all-ones")
            .insert(v4(255, 255, 255, 252), 30, "tail");
        let table = b.build();
        assert_eq!(table.lookup(v4(255, 255, 255, 255)), Some(&"all-ones"));
        assert_eq!(table.lookup(v4(255, 255, 255, 254)), Some(&"tail"));
        assert_eq!(table.lookup(v4(255, 255, 255, 251)), None);
    }

    #[test]
    fn deduplicates_equal_values() {
        let mut b = Ipv4Poptrie::builder();
        for i in 0..16u8 {
            b.insert(v4(10, i, 0, 0), 16, "same");
        }
        let table = b.build();
        assert_eq!(
            table.value_count(),
            1,
            "equal values should be deduplicated"
        );
        assert_eq!(table.lookup(v4(10, 7, 1, 1)), Some(&"same"));
    }

    #[test]
    fn last_insert_wins_for_identical_prefix() {
        let mut b = Ipv4Poptrie::builder();
        b.insert(v4(10, 0, 0, 0), 8, "first")
            .insert(v4(10, 0, 0, 0), 8, "second");
        let table = b.build();
        assert_eq!(table.lookup(v4(10, 1, 1, 1)), Some(&"second"));
        assert_eq!(table.value_count(), 1);
    }

    #[test]
    fn overwritten_values_are_compacted_even_with_shared_live_values() {
        let mut b = Ipv4Poptrie::builder();
        b.insert(0, 0, "old-default")
            .insert(0, 0, "default")
            .insert(v4(10, 0, 0, 0), 8, "old-ten")
            .insert(v4(10, 0, 0, 0), 8, "ten")
            .insert(v4(10, 1, 0, 0), 16, "old-lan")
            .insert(v4(10, 1, 0, 0), 16, "lan")
            .insert(v4(192, 0, 2, 0), 24, "shared")
            .insert(v4(198, 51, 100, 0), 24, "shared");
        let table = b.build();

        assert_eq!(table.lookup(v4(8, 8, 8, 8)), Some(&"default"));
        assert_eq!(table.lookup(v4(10, 2, 3, 4)), Some(&"ten"));
        assert_eq!(table.lookup(v4(10, 1, 2, 3)), Some(&"lan"));
        assert_eq!(table.lookup(v4(192, 0, 2, 9)), Some(&"shared"));
        assert_eq!(table.lookup(v4(198, 51, 100, 9)), Some(&"shared"));
        assert_eq!(table.value_count(), 4);
    }

    #[test]
    fn prefixes_around_direct_root_boundary_match_longest_prefix() {
        let mut b = Ipv4Poptrie::builder();
        b.insert(v4(10, 0, 0, 0), 13, "slash13")
            .insert(v4(10, 4, 0, 0), 14, "slash14")
            .insert(v4(10, 6, 0, 0), 15, "slash15");
        let table = b.build();

        assert_eq!(table.lookup(v4(10, 1, 2, 3)), Some(&"slash13"));
        assert_eq!(table.lookup(v4(10, 5, 2, 3)), Some(&"slash14"));
        assert_eq!(table.lookup(v4(10, 6, 2, 3)), Some(&"slash15"));
        assert_eq!(table.lookup(v4(10, 8, 0, 1)), None);
    }

    #[test]
    fn alternate_ipv4_direct_bits_configurations_match() {
        let mut small = Poptrie::<u32, u32, 2>::builder();
        small
            .insert(0, 0, 1)
            .insert(v4(203, 0, 113, 0), 24, 2)
            .insert(v4(203, 0, 113, 42), 32, 3);
        let small = small.build();
        assert_eq!(small.lookup(v4(203, 0, 113, 42)), Some(&3));
        assert_eq!(small.lookup(v4(203, 0, 113, 43)), Some(&2));
        assert_eq!(small.lookup(v4(8, 8, 8, 8)), Some(&1));

        let mut mid = Poptrie::<u32, u32, 8>::builder();
        mid.insert(v4(198, 51, 100, 0), 24, 1)
            .insert(v4(198, 51, 100, 64), 30, 2)
            .insert(v4(198, 51, 100, 66), 32, 3);
        let mid = mid.build();
        assert_eq!(mid.lookup(v4(198, 51, 100, 66)), Some(&3));
        assert_eq!(mid.lookup(v4(198, 51, 100, 65)), Some(&2));
        assert_eq!(mid.lookup(v4(198, 51, 100, 99)), Some(&1));
        assert_eq!(mid.lookup(v4(198, 51, 101, 1)), None);
    }

    #[test]
    fn large_direct_bits_configuration_matches() {
        let mut b = Poptrie::<u32, u32, 20>::builder();
        b.insert(v4(172, 16, 0, 0), 12, 1)
            .insert(v4(172, 16, 5, 0), 24, 2);
        let table = b.build();
        assert_eq!(table.lookup(v4(172, 16, 5, 9)), Some(&2));
        assert_eq!(table.lookup(v4(172, 20, 0, 1)), Some(&1));
        assert_eq!(table.lookup(v4(10, 0, 0, 1)), None);
    }

    #[test]
    fn matches_reference_on_random_tables() {
        // Deterministic pseudo-random prefixes + keys, cross-checked against an
        // exhaustive longest-prefix-match reference.
        let mut rng = Lcg::new(0x9E37_79B9_7F4A_7C15);
        for trial in 0..40 {
            let count = 1 + (rng.next() % 400) as usize;
            let mut prefixes: Vec<(u32, u8, u32)> = Vec::with_capacity(count);
            let mut b = Ipv4Poptrie::builder();
            for value in 0..count as u32 {
                let len = (rng.next() % 33) as u8;
                let raw = rng.next() as u32;
                let mask = if len == 0 {
                    0
                } else {
                    u32::MAX << (32 - len as u32)
                };
                let prefix = raw & mask;
                prefixes.push((prefix, len, value));
                b.insert(prefix, len, value);
            }
            let table = b.build();
            for _ in 0..2_000 {
                let key = rng.next() as u32;
                let got = table.lookup(key).copied();
                let want = reference_lpm(&prefixes, key);
                assert_eq!(got, want, "trial {trial}, key {key:#010x}");
            }
        }
    }

    #[test]
    #[should_panic(expected = "prefix_len 33 exceeds key width 32")]
    fn ipv4_prefix_len_over_key_width_panics() {
        let mut b = Ipv4Poptrie::builder();
        b.insert(0, 33, 1u32);
    }

    #[test]
    #[should_panic(expected = "prefix_len 129 exceeds key width 128")]
    fn ipv6_prefix_len_over_key_width_panics() {
        let mut b = Ipv6Poptrie::builder();
        b.insert(0, 129, 1u32);
    }

    #[test]
    #[should_panic(expected = "DIRECT_BITS must be in 1..32")]
    fn zero_direct_bits_panics() {
        let _ = Poptrie::<u32, u32, 0>::builder().build();
    }

    #[test]
    #[should_panic(expected = "must tile into 6-bit strides")]
    fn misaligned_direct_bits_panics() {
        let _ = Poptrie::<u32, u32, 13>::builder().build();
    }

    #[test]
    fn ipv6_basic_longest_prefix() {
        use std::net::Ipv6Addr;
        let mut b = Ipv6Poptrie::builder();
        let net: u128 = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0).into();
        let sub: u128 = Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 0).into();
        b.insert(net, 32, "doc").insert(sub, 64, "subnet");
        let table = b.build();
        let host: u128 = Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 0x99).into();
        assert_eq!(table.lookup(host), Some(&"subnet"));
        let other: u128 = Ipv6Addr::new(0x2001, 0xdb8, 0, 2, 0, 0, 0, 1).into();
        assert_eq!(table.lookup(other), Some(&"doc"));
        let outside: u128 = Ipv6Addr::new(0x2001, 0xdb9, 0, 0, 0, 0, 0, 1).into();
        assert_eq!(table.lookup(outside), None);
    }

    #[test]
    fn ipv6_larger_direct_bits_configuration_matches() {
        use std::net::Ipv6Addr;

        let mut b = Poptrie::<u128, u32, 20>::builder();
        let net: u128 = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0).into();
        let sub: u128 = Ipv6Addr::new(0x2001, 0xdb8, 0, 0xaaaa, 0, 0, 0, 0).into();
        b.insert(net, 32, 1).insert(sub, 64, 2);
        let table = b.build();

        let host: u128 = Ipv6Addr::new(0x2001, 0xdb8, 0, 0xaaaa, 0, 0, 0, 1).into();
        assert_eq!(table.lookup(host), Some(&2));
        let sibling: u128 = Ipv6Addr::new(0x2001, 0xdb8, 0, 0xaaab, 0, 0, 0, 1).into();
        assert_eq!(table.lookup(sibling), Some(&1));
        let outside: u128 = Ipv6Addr::new(0x2001, 0xdb9, 0, 0, 0, 0, 0, 1).into();
        assert_eq!(table.lookup(outside), None);
    }

    #[test]
    fn ipv6_matches_reference_on_random_tables() {
        let mut rng = Lcg::new(0xD1B5_4A32_D192_ED03);
        for trial in 0..16 {
            let count = 1 + (rng.next() % 160) as usize;
            let mut prefixes: Vec<(u128, u8, u32)> = Vec::with_capacity(count);
            let mut b = Ipv6Poptrie::builder();
            for value in 0..count as u32 {
                let len = (rng.next() % 129) as u8;
                let raw = rng.next_u128();
                let mask = if len == 0 {
                    0
                } else {
                    u128::MAX << (128 - len as u32)
                };
                let prefix = raw & mask;
                prefixes.push((prefix, len, value));
                b.insert(prefix, len, value);
            }
            let table = b.build();
            for _ in 0..1_000 {
                let key = rng.next_u128();
                let got = table.lookup(key).copied();
                let want = reference_lpm_v6(&prefixes, key);
                assert_eq!(got, want, "trial {trial}, key {key:#034x}");
            }
        }
    }

    /// Small xorshift-style PRNG so the randomized test needs no dependencies.
    struct Lcg(u64);
    impl Lcg {
        fn new(seed: u64) -> Self {
            Self(seed | 1)
        }
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn next_u128(&mut self) -> u128 {
            (u128::from(self.next()) << 64) | u128::from(self.next())
        }
    }
}
