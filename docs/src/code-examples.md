# Code Examples

This chapter collects small programs that exercise the public socket APIs.

The examples focus on backend-neutral contracts. Backend construction belongs
in `main`; packet loops should usually be generic over the core traits.

- The [Packet Blaster](code-examples/packet-blaster.md),
  [Reflection Server](code-examples/reflection-server.md), and
  [Custom Router](code-examples/custom-router.md) show generic packet loops over
  the core traits.
- The [XDP Factory](code-examples/xdp-factory.md) shows how to build AF_XDP
  aggregate sockets — one logical socket per worker thread spanning several NIC
  queues over a shared UMEM — with `XdpFactoryBuilder` and a `threads(T)` knob.
  The blaster, reflection server, and custom router each take a `--threads`
  argument in XDP mode and open their sockets this way.
