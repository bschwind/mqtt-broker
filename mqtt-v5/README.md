mqtt-v5
=======

Rust types, encode/decode functions, and a tokio codec for MQTT V5.

# Dependencies
- cargo
- rustc

# Feature Flags

`codec`: Export an `MqttCodec` type under `mqtt_v5::codec::MqttCodec`. Enabled by default.

# Build

```
$ cargo build --release
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

# Code Linting

```
$ cargo clippy
```

# Code Fuzzing

Fuzzing requires a nightly toolchain. Fuzzing for this project is currently confirmed to work with:

```
rustc 1.42.0-nightly (6d3f4e0aa 2020-01-25)
```

## Running

Run this from the root project, not inside the `mqtt-v5` directory.

```
cargo install cargo-fuzz
cargo +nightly fuzz run decoder_fuzzer_v311
cargo +nightly fuzz run decoder_fuzzer_v500
cargo +nightly fuzz run topic_filter_fuzzer
cargo +nightly fuzz run topic_fuzzer
```
