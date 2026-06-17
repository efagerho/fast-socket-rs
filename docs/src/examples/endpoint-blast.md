# endpoint-blast

`endpoint-blast` is an XDP-only packet generator for one remote UDP endpoint. It
prepares a socket-specific endpoint for the configured target, then sends
generated payload buffers through that endpoint as fast as the XDP workers
accept them.

This example basically demonstrates the most efficient way to generate packets
using the library. Initial tests generated about 28M PPS on a single CPU core.
When using an `Endpoint`, we can precompute the L2+IP+UDP header only needing to
adjust checksums during header generation. The header generation then translates
mainly into a memcpy.

Note that AF_XDP sockets don't currently expose checksum offloads from the NIC,
so the `Endpoint` implementation has to calculate the checksums on the CPU.

It does not take `--backend`; it always uses busy-poll XDP sockets.

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
L2+IPv4+UDP header shape for that packet size:

```rust,ignore
let mut endpoints = Vec::with_capacity(aggregate.len());

for socket in aggregate.members_mut() {
    let mut spec = UdpEndpointSpec::new(target);
    spec.payload_len = Some(payload_len);
    endpoints.push(socket.prepare_udp_endpoint(spec)?);
}
```

## Transmit Loop

The inner loop still allocates normal TX buffers and writes the sequence payload
into each buffer. The difference is the batch item: `UdpEndpointTransmit` only
carries the packet buffer because the endpoint already owns the destination,
source selection, MTU, and cached header state.

```rust,ignore
while let Some(mut buffer) = tx_buffers.pop() {
    write_sequence(payload_bytes, *sequence);
    buffer.extend_from_slice(payload_bytes)?;
    batch.push(TxSlot::Ready(UdpEndpointTransmit::new(buffer.freeze())));
    *sequence = sequence.wrapping_add(1);
}
```

The completed batch is submitted through the prepared endpoint:

```rust,ignore
let accepted = socket.send_to_udp_endpoint(endpoint, batch.as_mut_slice())?;
```

The XDP backend refreshes the cached endpoint header if the route table changes,
so the hot path gets the prepared header copy while route updates still take
effect before later sends.
