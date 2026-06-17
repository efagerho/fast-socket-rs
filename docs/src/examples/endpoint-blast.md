# endpoint-blast

`endpoint-blast` is an XDP-only packet generator for one remote UDP endpoint. It
prepares a socket-specific endpoint for the configured target, then sends
generated payload buffers through that endpoint as fast as the XDP workers
accept them.

This example demonstrates the fastest packet-generation path currently exposed
by the library. Recent single-worker runs on this host generate about 81M PPS.
When using an `Endpoint`, the XDP backend caches a contiguous L2+IPv4+UDP header
template for the remote peer. The hot path copies that cached header into each TX
frame, then patches the length-dependent IPv4 and UDP fields. The common
Ethernet+IPv4+UDP header is copied with a small fixed-size word-copy path, with a
generic memcpy fallback for other header sizes.

Note that AF_XDP sockets don't currently expose checksum offloads from the NIC,
so the `Endpoint` implementation has to calculate the checksums on the CPU.

```sh
cargo run -p fast-socket-examples --bin endpoint-blast -- \
  --device eth0 \
  --target 192.168.0.20:9000 \
  --threads 1 \
  --payload-len 64 \
  --batch-size 64 \
  --drain-every-batches 2 \
  --duration-ms 10000
```

Important flags:

- `--target <ipv4:port>` is the remote UDP endpoint.
- `--source-ip <ipv4>` overrides the interface IPv4 address used as the source.
- `--source-port <port>` overrides the generated dynamic source port.
- `--threads <n>` controls how many XDP worker plans the factory builds.
- `--drain-every-batches <n>` controls how often each socket drains TX
  completions while making progress.
- `--duration-ms <ms>` stops the generator after a fixed duration. Without it,
  the process runs until a shutdown signal or worker failure.

The target must be routable through the selected interface, and the route lookup
must find neighbor information for the target or next hop.

## Prepared Endpoints

Each worker opens its XDP aggregate through the normal factory path:

```rust,ignore
let mut aggregate = plan.open_udp_busy_poll(local)?;
```

Before entering the transmit loop, the worker prepares one endpoint per member
socket. The fixed `payload_len` lets the XDP backend cache the complete
L2+IPv4+UDP header shape for that packet size. Endpoints without a fixed payload
length still use the same cached header template, but patch the IPv4 total
length, IPv4 checksum, and UDP length for each returned payload length:

```rust,ignore
let mut endpoints = Vec::with_capacity(aggregate.len());

for socket in aggregate.members_mut() {
    let mut spec = UdpEndpointSpec::new(target);
    spec.payload_len = Some(payload_len);
    endpoints.push(socket.prepare_udp_endpoint(spec)?);
}
```

## Transmit Loop

The inner loop uses the `UdpSocket` endpoint batch builder. On XDP sockets the
builder writes the cached endpoint header and caller-provided payloads directly
into UMEM-backed TX frames, so the example does not allocate
`UdpEndpointTransmit` slots for each packet.

```rust,ignore
let accepted = socket
    .udp_endpoint_batch(endpoint, batch_size)
    .send(|_, payload| {
        let payload_len = payload_bytes.len();
        let payload = &mut payload[..payload_len];
        payload.copy_from_slice(payload_bytes);
        write_sequence(payload, *sequence);
        *sequence = (*sequence).wrapping_add(1);
        payload_len
    })?;
```

The callback runs once for each TX frame that the socket can reserve, up to the
requested batch size. Each slice is the endpoint's maximum UDP payload size, and
the returned length selects how many bytes become part of that packet. Backends
are not required to clear the slice before the callback runs, so the callback
must initialize every byte in `payload[..returned_len]`. The example copies a
reusable payload template and writes the sequence prefix with the shared
benchmark helper, which uses a direct big-endian word store for payloads of at
least eight bytes.

```rust,ignore
if accepted == 0 {
    socket.drain_tx_completions()?;
}
```

Workers drain TX completions after `--drain-every-batches` accepted batches and
also when the socket makes no progress. Progress counters and shutdown checks
are amortized across many batches so they do not dominate the hot packet loop.

The XDP backend refreshes the cached endpoint header if the route table changes,
so the hot path gets the prepared header copy while route updates still take
effect before later sends.
