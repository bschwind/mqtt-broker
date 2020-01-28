pub const TOPIC_SEPARATOR: char = '/';

pub const MULTI_LEVEL_WILDCARD: char = '#';
pub const MULTI_LEVEL_WILDCARD_STR: &str = "#";

pub const SINGLE_LEVEL_WILDCARD: char = '+';
pub const SINGLE_LEVEL_WILDCARD_STR: &str = "+";

pub const SHARED_SUBSCRIPTION_PREFIX: &str = "$share/";

pub const MAX_TOPIC_LEN_BYTES: usize = 65_535;

pub mod decoder;
pub mod encoder;
pub mod topic;
pub mod types;

#[cfg(feature = "codec")]
pub mod codec {
    use crate::{
        decoder, encoder,
        types::{DecodeError, Packet, ProtocolVersion},
    };
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    pub struct MqttCodec {
        version: ProtocolVersion,
    }

    impl MqttCodec {
        pub fn new() -> Self {
            MqttCodec { version: ProtocolVersion::V311 }
        }

        pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Packet>, DecodeError> {
            // TODO - Ideally we should keep a state machine to store the data we've read so far.
            let packet = decoder::decode_mqtt(buf, self.version);

            if let Ok(Some(Packet::Connect(packet))) = &packet {
                self.version = packet.protocol_version;
            }

            packet
        }

        pub fn encode(&mut self, packet: Packet, bytes: &mut BytesMut) -> Result<(), DecodeError> {
            encoder::encode_mqtt(&packet, bytes);
            Ok(())
        }
    }

    impl Decoder for MqttCodec {
        type Error = DecodeError;
        type Item = Packet;

        fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            // TODO - Ideally we should keep a state machine to store the data we've read so far.
            self.decode(buf)
        }
    }

    impl Encoder for MqttCodec {
        type Error = DecodeError;
        type Item = Packet;

        fn encode(&mut self, packet: Self::Item, bytes: &mut BytesMut) -> Result<(), DecodeError> {
            self.encode(packet, bytes)
        }
    }
}
