#!/usr/bin/env sh
set -eu
cargo +nightly build --release \
  --target-dir ./target \
  --target bpfel-unknown-none -Z build-std=core,compiler_builtins \
  -Z build-std-features=compiler-builtins-mem
cp target/bpfel-unknown-none/release/fast-socket-xdp-prog ./fast-socket-xdp-prog
