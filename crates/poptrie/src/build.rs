//! Construction of a [`Poptrie`] from a set of prefixes.
//!
//! Building is a three-stage offline process, none of it on the lookup hot
//! path:
//!
//! 1. Insert every `(prefix, len, value)` into a binary (1-bit) trie, interning
//!    values into a deduplicated table so identical routes share an index
//!    (which lets adjacent leaves merge later).
//! 2. Emit the direct-pointing array for the top `DIRECT_BITS` of the address,
//!    bulk-filling whole ranges that have no longer prefixes.
//! 3. Below each direct entry that still branches, emit 6-bit-stride Poptrie
//!    nodes, leaf-pushing inherited routes down so every slot resolves to its
//!    longest-prefix value, and compressing runs of equal leaves.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use crate::{IpKey, LEAF_FLAG, Node, Poptrie, STRIDE, VALUE_MASK};

/// A node in the intermediate binary trie. `value == 0` means no prefix ends
/// here; otherwise it is a 1-based index into the value table.
#[derive(Clone)]
struct BinNode {
    children: [Option<u32>; 2],
    value: u32,
}

impl BinNode {
    fn empty() -> Self {
        Self {
            children: [None, None],
            value: 0,
        }
    }
}

/// Incrementally collects prefixes, then produces an immutable [`Poptrie`].
///
/// Values are deduplicated, so `V` must be `Clone + Eq + Hash`. Inserting the
/// same prefix twice keeps the last value.
pub struct PoptrieBuilder<K: IpKey, V, const DIRECT_BITS: u32> {
    bin: Vec<BinNode>,
    values: Vec<V>,
    value_index: HashMap<V, u32>,
    _key: PhantomData<fn() -> K>,
}

impl<K, V, const DIRECT_BITS: u32> Default for PoptrieBuilder<K, V, DIRECT_BITS>
where
    K: IpKey,
    V: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, const DIRECT_BITS: u32> PoptrieBuilder<K, V, DIRECT_BITS>
where
    K: IpKey,
    V: Clone + Eq + Hash,
{
    /// Creates an empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            bin: vec![BinNode::empty()], // index 0 is the trie root
            values: Vec::new(),
            value_index: HashMap::new(),
            _key: PhantomData,
        }
    }

    /// Inserts a route: addresses whose top `prefix_len` bits equal `prefix`
    /// resolve to `value`, unless a longer inserted prefix also matches.
    ///
    /// `prefix_len == 0` installs a default route. Bits of `prefix` below
    /// `prefix_len` are ignored. Re-inserting the same `(prefix, prefix_len)`
    /// replaces the value.
    ///
    /// # Panics
    /// Panics if `prefix_len` exceeds the key width (`K::BITS`).
    pub fn insert(&mut self, prefix: K, prefix_len: u8, value: V) -> &mut Self {
        let prefix_len = u32::from(prefix_len);
        assert!(
            prefix_len <= K::BITS,
            "prefix_len {prefix_len} exceeds key width {}",
            K::BITS,
        );

        let value_index = self.intern(value);
        let mut node = 0usize;
        for i in 0..prefix_len {
            let branch = usize::from(prefix.bit(i));
            node = match self.bin[node].children[branch] {
                Some(child) => child as usize,
                None => {
                    let child = self.bin.len() as u32;
                    self.bin.push(BinNode::empty());
                    self.bin[node].children[branch] = Some(child);
                    child as usize
                }
            };
        }
        self.bin[node].value = value_index;
        self
    }

    /// Returns the 1-based table index for `value`, inserting it if new.
    fn intern(&mut self, value: V) -> u32 {
        if let Some(&index) = self.value_index.get(&value) {
            return index;
        }
        let index = self.values.len() as u32 + 1;
        self.values.push(value.clone());
        self.value_index.insert(value, index);
        index
    }

    /// Consumes the builder and produces the immutable, read-optimized trie.
    ///
    /// # Panics
    /// Panics if `DIRECT_BITS` is out of range or the bits below it do not tile
    /// into whole 6-bit strides (`(K::BITS - DIRECT_BITS) % 6 != 0`), or if the
    /// table is too large to address with 31-bit node/value indices.
    #[must_use]
    pub fn build(self) -> Poptrie<K, V, DIRECT_BITS> {
        assert!(
            DIRECT_BITS >= 1 && DIRECT_BITS < K::BITS,
            "DIRECT_BITS must be in 1..{}",
            K::BITS,
        );
        assert!(
            (K::BITS - DIRECT_BITS).is_multiple_of(STRIDE),
            "bits below DIRECT_BITS ({}) must tile into {STRIDE}-bit strides",
            K::BITS - DIRECT_BITS,
        );
        assert!(
            self.values.len() < VALUE_MASK as usize,
            "too many distinct route values for a Poptrie",
        );

        let mut emitter = Emitter {
            bin: &self.bin,
            nodes: Vec::new(),
            leaves: Vec::new(),
        };
        let mut direct = vec![0u32; 1usize << DIRECT_BITS];
        emitter.fill_direct(Some(0), 0, 0, 0, DIRECT_BITS, &mut direct);

        assert!(
            emitter.nodes.len() < LEAF_FLAG as usize,
            "too many Poptrie nodes to index with 31 bits",
        );

        Poptrie {
            direct: direct.into_boxed_slice(),
            nodes: emitter.nodes.into_boxed_slice(),
            leaves: emitter.leaves.into_boxed_slice(),
            values: self.values.into_boxed_slice(),
            _key: PhantomData,
        }
    }
}

/// What a stride slot resolves to during emission.
#[derive(Clone, Copy)]
enum Slot {
    /// A deeper node is needed; carry the binary-trie node and the value
    /// inherited into it for leaf-pushing.
    Internal(u32, u32),
    /// A resolved route (value encoding; `0` = no route).
    Leaf(u32),
}

/// Walks the binary trie and writes the Poptrie node / leaf / direct arrays.
struct Emitter<'a> {
    bin: &'a [BinNode],
    nodes: Vec<Node>,
    leaves: Vec<u32>,
}

impl Emitter<'_> {
    /// Effective (leaf-pushed) value at a trie node: its own prefix value if
    /// any, else the value inherited from the nearest ancestor prefix.
    #[inline]
    fn effective(&self, node: Option<u32>, inherited: u32) -> u32 {
        match node {
            Some(id) if self.bin[id as usize].value != 0 => self.bin[id as usize].value,
            _ => inherited,
        }
    }

    #[inline]
    fn has_children(&self, id: u32) -> bool {
        let node = &self.bin[id as usize];
        node.children[0].is_some() || node.children[1].is_some()
    }

    #[inline]
    fn children(&self, node: Option<u32>) -> (Option<u32>, Option<u32>) {
        match node {
            Some(id) => {
                let node = &self.bin[id as usize];
                (node.children[0], node.children[1])
            }
            None => (None, None),
        }
    }

    /// Fills the direct-pointing array for the top `direct_bits` of the address.
    ///
    /// A range with no longer prefixes is bulk-filled with a single leaf
    /// encoding; a range that still branches at `depth == direct_bits` becomes a
    /// node pointer to a freshly emitted subtree.
    fn fill_direct(
        &mut self,
        node: Option<u32>,
        inherited: u32,
        depth: u32,
        pattern: usize,
        direct_bits: u32,
        direct: &mut [u32],
    ) {
        let effective = self.effective(node, inherited);
        let branches = node.is_some_and(|id| self.has_children(id));

        if !branches {
            // Uniform region: every address under `pattern` resolves to one leaf.
            let shift = direct_bits - depth;
            let start = pattern << shift;
            let len = 1usize << shift;
            let encoding = LEAF_FLAG | effective;
            direct[start..start + len].fill(encoding);
            return;
        }

        if depth == direct_bits {
            // Still branching below the direct root: hand off to a node subtree.
            let root = self.build_subtree(node.expect("branches implies Some"), effective);
            direct[pattern] = root; // top bit clear ⇒ node index
            return;
        }

        let (zero, one) = self.children(node);
        self.fill_direct(
            zero,
            effective,
            depth + 1,
            pattern << 1,
            direct_bits,
            direct,
        );
        self.fill_direct(
            one,
            effective,
            depth + 1,
            (pattern << 1) | 1,
            direct_bits,
            direct,
        );
    }

    /// Emits a fresh node subtree rooted at binary-trie node `bin_id` and
    /// returns its node index.
    fn build_subtree(&mut self, bin_id: u32, inherited: u32) -> u32 {
        let slot = self.nodes.len() as u32;
        self.nodes.push(Node::default());
        self.fill(slot, bin_id, inherited);
        slot
    }

    /// Fills the already-reserved node at `slot` and recursively emits its
    /// children. Children are reserved contiguously *before* recursing so a
    /// node's direct children always occupy `[base1, base1 + n)`.
    fn fill(&mut self, slot: u32, bin_id: u32, inherited: u32) {
        let mut slots = [Slot::Leaf(0); 1 << STRIDE];
        self.resolve_stride(Some(bin_id), inherited, 0, 0, &mut slots);

        let base0 = self.leaves.len() as u32;
        let mut leafvec = 0u64;
        let mut vector = 0u64;
        let mut children: Vec<(u32, u32)> = Vec::new();
        let mut current_run: Option<u32> = None;

        for (i, slot) in slots.iter().enumerate() {
            match *slot {
                Slot::Internal(child, child_inherited) => {
                    vector |= 1u64 << i;
                    children.push((child, child_inherited));
                    current_run = None; // an internal slot ends any leaf run
                }
                Slot::Leaf(value) => {
                    if current_run != Some(value) {
                        leafvec |= 1u64 << i;
                        self.leaves.push(value);
                        current_run = Some(value);
                    }
                }
            }
        }

        let base1 = self.nodes.len() as u32;
        for _ in &children {
            self.nodes.push(Node::default());
        }
        self.nodes[slot as usize] = Node {
            vector,
            leafvec,
            base0,
            base1,
        };

        for (k, (child, child_inherited)) in children.into_iter().enumerate() {
            self.fill(base1 + k as u32, child, child_inherited);
        }
    }

    /// Resolves the 64 slots of one 6-bit stride below `node`, leaf-pushing
    /// `inherited` into ranges that fall off the binary trie.
    fn resolve_stride(
        &self,
        node: Option<u32>,
        inherited: u32,
        depth: u32,
        pattern: usize,
        out: &mut [Slot; 1 << STRIDE],
    ) {
        let effective = self.effective(node, inherited);
        let branches = node.is_some_and(|id| self.has_children(id));

        if !branches {
            // Uniform leaf over this sub-range of stride slots.
            let shift = STRIDE - depth;
            let start = pattern << shift;
            let len = 1usize << shift;
            out[start..start + len].fill(Slot::Leaf(effective));
            return;
        }

        if depth == STRIDE {
            out[pattern] = Slot::Internal(node.expect("branches implies Some"), effective);
            return;
        }

        let (zero, one) = self.children(node);
        self.resolve_stride(zero, effective, depth + 1, pattern << 1, out);
        self.resolve_stride(one, effective, depth + 1, (pattern << 1) | 1, out);
    }
}

#[cfg(test)]
mod tests {
    use crate::Ipv4Poptrie;
    use std::net::Ipv4Addr;

    fn v4(a: u8, b: u8, c: u8, d: u8) -> u32 {
        u32::from(Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn equal_values_dedup_and_resolve() {
        // Two distinct /20 prefixes (below the 14-bit direct root, so they land
        // in trie nodes) sharing one value: the value table deduplicates to one
        // entry and both ranges resolve to it, while the covering /8 fills the
        // gaps. Exercises leaf-run handling and value interning together.
        let mut b = Ipv4Poptrie::builder();
        b.insert(v4(10, 0, 0, 0), 8, "ten")
            .insert(v4(10, 10, 16, 0), 20, "same")
            .insert(v4(10, 10, 32, 0), 20, "same");
        let table = b.build();

        assert_eq!(table.lookup(v4(10, 10, 17, 5)), Some(&"same"));
        assert_eq!(table.lookup(v4(10, 10, 33, 5)), Some(&"same"));
        assert_eq!(table.lookup(v4(10, 10, 48, 5)), Some(&"ten"));
        assert_eq!(table.lookup(v4(11, 0, 0, 1)), None);
        assert_eq!(
            table.value_count(),
            2,
            "\"ten\" and \"same\" — equal values deduped"
        );
    }
}
