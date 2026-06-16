# Poptrie Route Tables

Route lookup is a longest-prefix-match problem. A table contains prefixes such
as `0.0.0.0/0`, `10.0.0.0/8`, and `10.1.2.0/24`; a lookup for a destination IP
must return the value attached to the most specific matching prefix. The
`poptrie` crate provides an immutable, read-optimized table for exactly that
operation.

The intended shape is:

- Build a table off the packet path from the current routing state.
- Store the application's route payload as the Poptrie value.
- Share or swap the finished immutable table.
- On each packet, perform one lookup and use the returned route value.

This keeps control-plane work such as allocation, value deduplication, and trie
construction away from packet processing. The hot path only sees fixed arrays and
population counts.

## What the Value Represents

`poptrie` does not prescribe a route value type. The value can be a route ID,
next-hop index, egress cache entry, or any compact application-specific handle
that implements the builder's `Clone + Eq + Hash` bounds. The lookup API returns
`Option<&V>`:

- `Some(route)` means a matching prefix was found.
- `None` means no prefix covers the destination.

A default route is just an inserted prefix with length `0`.

For example, an IPv4 table can store route IDs that index a side table of
next-hop information:

```rust,ignore
use std::net::Ipv4Addr;

use fast_socket_rs::RouteId;
use poptrie::Ipv4Poptrie;

let mut builder = Ipv4Poptrie::builder();

builder
    .insert(
        u32::from(Ipv4Addr::new(0, 0, 0, 0)),
        0,
        RouteId::new(1),
    )
    .insert(
        u32::from(Ipv4Addr::new(10, 0, 0, 0)),
        8,
        RouteId::new(2),
    )
    .insert(
        u32::from(Ipv4Addr::new(10, 1, 2, 0)),
        24,
        RouteId::new(3),
    );

let table = builder.build();

let dst = Ipv4Addr::new(10, 1, 2, 42);
let route_id = table.lookup(u32::from(dst)).copied().unwrap();

assert_eq!(route_id, RouteId::new(3));
```

Bits below `prefix_len` are ignored when inserting, so callers do not need to
pre-mask prefixes. Inserting the same prefix again replaces the previous value.
Equal values are deduplicated during construction, which lets adjacent leaf
ranges share the same stored route.

## Lookup Layout

The Poptrie layout is built for cache-efficient longest-prefix lookup. It has
four arrays:

- `direct`: a flat root table indexed by the top `DIRECT_BITS` address bits.
- `nodes`: internal 6-bit-stride nodes.
- `leaves`: compressed runs of resolved route values.
- `values`: the distinct route values inserted by the builder.

Each direct entry is either a leaf value or an index into `nodes`. If lookup
enters the node array, each node consumes six address bits. A node stores two
`u64` bitmaps:

- `vector` marks stride slots that continue to child nodes.
- `leafvec` marks the start of runs that resolve to route values.

For a 6-bit chunk `v`, lookup computes `1 << v`. If the bit is present in
`vector`, a population count over lower bits gives the child rank. Otherwise, a
population count over `leafvec` gives the leaf-run rank. This is the core
Poptrie trick: one 64-bit bitmap plus hardware `popcnt` turns a sparse 64-way
node into compact arrays with little branching.

For the default IPv4 alias, lookup performs:

- one direct-array load;
- at most three 6-bit node hops;
- one `popcnt` per node hop.

If the direct entry already resolves to a route, lookup returns after the first
array load.

## Building and Swapping Tables

Construction is intentionally offline. The builder first inserts routes into an
ordinary binary trie, then emits the direct array and compressed Poptrie nodes.
During this process it leaf-pushes inherited routes downward, so every final
slot resolves immediately to the longest prefix known for that address range.

That makes the recommended route-update pattern simple:

```rust,ignore
use std::sync::Arc;

use poptrie::Ipv4Poptrie;

type Routes = Ipv4Poptrie<u32>;

fn rebuild(prefixes: &[(u32, u8, u32)]) -> Arc<Routes> {
    let mut builder = Routes::builder();
    for &(prefix, len, route_id) in prefixes {
        builder.insert(prefix, len, route_id);
    }
    Arc::new(builder.build())
}
```

The packet path should use the current finished table. When the control plane
receives a route update, build a replacement separately and publish it as a
whole. The fastest shared-table option is usually the `arc-swap` crate
(`arc_swap` in code): readers load the current `Arc` with one cheap atomic
operation while writers swap in a complete replacement.

A tiled design can avoid synchronization entirely by keeping each Poptrie table
owned by the worker thread that uses it. Route updates are delivered to that
tile's owner, and packet processing reads ordinary thread-local state with no
atomic load on the lookup path.

The public API is safe. Internally, the lookup path uses unchecked indexing into
private arrays because the builder creates and validates the required invariants.
In debug and test builds, the builder checks that direct entries, node child
ranges, leaf ranges, and value encodings are all valid before the table is used.

## Choosing `DIRECT_BITS`

`DIRECT_BITS` controls the size of the flat root table. A larger direct table
uses more memory but skips more trie levels. The direct table alone costs:

```text
4 * 2^DIRECT_BITS bytes
```

The built-in aliases use `DIRECT_BITS = 14`, so the direct table is 64 KiB:

```rust,ignore
use poptrie::{Ipv4Poptrie, Ipv6Poptrie};

type V4Routes<V> = Ipv4Poptrie<V>;
type V6Routes<V> = Ipv6Poptrie<V>;
```

For IPv4, valid direct widths are the values where the remaining address bits
tile into 6-bit strides:

| Type | Direct bits | Direct table | Worst-case node hops |
| ---- | ----------- | ------------ | -------------------- |
| `Poptrie<u32, V, 2>` | 2 | 16 B | 5 |
| `Poptrie<u32, V, 8>` | 8 | 1 KiB | 4 |
| `Ipv4Poptrie<V>` | 14 | 64 KiB | 3 |
| `Poptrie<u32, V, 20>` | 20 | 4 MiB | 2 |
| `Poptrie<u32, V, 26>` | 26 | 256 MiB | 1 |

The default is a good general tradeoff. `20` can make sense for very large IPv4
tables when memory is less important than shaving a node hop. `26` is usually
too large unless the table is specialized and memory is abundant.

IPv6 follows the same rule, but the address is much wider. The default
`Ipv6Poptrie<V>` still uses a 64 KiB direct table, while tables dominated by
common `/32` to `/48` boundaries may benefit from a wider direct root.

## Operational Notes

The route values must implement `Clone + Eq + Hash` while building because the
builder deduplicates equal values. The finished table can then be used through
shared references.

Use a compact value when possible. A Poptrie lookup returns a borrowed value, so
large route records can increase cache pressure even when lookup itself is fast.
A small route ID, next-hop index, or precomputed egress handle is often better
than embedding a large control-plane object.

Run the benchmark with:

```sh
cargo bench -p poptrie
```

The benchmark uses synthetic IPv4 tables biased toward many `/24` routes, fewer
short prefixes, and a default route. It compares Poptrie lookup with a linear
scan for small and medium route tables and measures builder throughput.
