mqtt-broker
===========

A tokio-based MQTT v5 broker written in Rust.

# Dependencies
- cargo
- rustc

# Build

```
$ cargo build --release
```

# Run

```
$ cargo run --release
```

# Testing

```
$ cargo test
```

# Code Format

The formatting options currently use nightly-only options.

```
$ cargo +nightly fmt
```

# Code Fuzzing

Fuzzing requires a nightly toolchain. Fuzzing for this project is currently confirmed to work with:

```
rustc 1.42.0-nightly (6d3f4e0aa 2020-01-25)
```

## Running

```
cargo install cargo-fuzz
cargo +nightly fuzz run decoder_fuzzer
```
