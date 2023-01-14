# mqtt-broker

A tokio-based MQTT v5 broker written in Rust.

## Project Goals

The goals for this project are fairly straightforward:

* Adhere to the MQTT V5 [Spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
* Be easily deployable as a single binary
* Have reasonable performance and memory usage on a single node

I originally started this project as a simple, open-source broker for any IoT products I might make. If I were to sell such products, I would want to allow users to run their own broker in case I can no longer run one, and it should be as easy as possible to do so.

Extra features like a broker cluster or extra transport layers are _nice to have_ features, but won't be considered until the core V5 spec is implemented. The exception to this is the WebSocket transport, which is specifically mentioned in the spec and quite useful to have.

## Comparison to Other Brokers

Wikipedia has a fairly [comprehensive list](https://en.wikipedia.org/wiki/Comparison_of_MQTT_implementations) of brokers to choose from.

[rumqtt](https://github.com/bytebeamio/rumqtt) at the moment appears to be the most fully-featured broker. Take a look there first if you're looking for a more "ready-to-go" Rust broker.

[PubSubRT](https://github.com/alttch/psrt/) is another interesting Rust-based broker. It's an alternative to MQTT, not an implementation of it.

[NATS.rs](https://github.com/nats-io/nats.rs) seems really nice too, but I haven't looked further into it yet.

## Spec Compliance

This broker is currently _not_ compliant with the MQTT V5 spec. Visit the [spec compliance milestone](https://github.com/bschwind/mqtt-broker/milestone/1) to see the current progress.

## Dependencies
- cargo
- rustc (version 1.39 or later)

## Build

```
$ cargo build --release
```

## Run

```
$ cargo run --release
```

## Testing

```
$ cargo test
```

## Code Format

The formatting options currently use nightly-only options.

```
$ cargo +nightly fmt
```

## Code Linting

```
$ cargo clippy
```

## Code Fuzzing

Fuzzing requires a nightly toolchain. Fuzzing for this project is currently confirmed to work with:

```
rustc 1.42.0-nightly (6d3f4e0aa 2020-01-25)
```

### Running

```
cargo install cargo-fuzz
cargo +nightly fuzz run decoder_fuzzer_v311
cargo +nightly fuzz run decoder_fuzzer_v500
cargo +nightly fuzz run topic_filter_fuzzer
cargo +nightly fuzz run topic_fuzzer
```
