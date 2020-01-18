mqtt-broker
===========

A tokio-based MQTT broker written in Rust.

Dependencies
------------
- cargo
- rustc

Build
-----
    $ cargo build --release

Run
---
	$ cargo run --release

Testing
-------

	$ cargo test

Code Format
-----------
The formatting options currently use nightly-only options.

    $ cargo +nightly fmt
