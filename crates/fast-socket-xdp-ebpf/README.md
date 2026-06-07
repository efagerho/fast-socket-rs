# fast-socket-xdp-ebpf

AF_XDP eBPF redirect program for `fast-socket-xdp-rs`. It redirects IPv4 and
IPv6 frames into the queue's AF_XDP socket while passing non-IP link traffic
such as ARP back to Linux.

The host-side library exposes prebuilt BPF object bytes. The `src/main.rs`
program is built only with the `ebpf` feature for `bpfel-unknown-none`, so
ordinary workspace builds do not require the BPF toolchain.

Rebuild the object manually after editing the program:

```sh
./build-ebpf.sh
```
