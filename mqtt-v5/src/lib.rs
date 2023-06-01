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
        types::{DecodeError, EncodeError, Packet, ProtocolVersion},
    };
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    pub struct MqttCodec {
        version: ProtocolVersion,
    }

    impl Default for MqttCodec {
        fn default() -> Self {
            MqttCodec::new()
        }
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

        pub fn encode(&mut self, packet: Packet, bytes: &mut BytesMut) -> Result<(), EncodeError> {
            encoder::encode_mqtt(&packet, bytes, self.version);
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

    impl Encoder<Packet> for MqttCodec {
        type Error = EncodeError;

        fn encode(&mut self, packet: Packet, bytes: &mut BytesMut) -> Result<(), Self::Error> {
            self.encode(packet, bytes)
        }
    }
}

#[cfg(feature = "websocket")]
pub mod websocket {
    use base64::{engine::general_purpose::STANDARD as STANDARD_BASE64_ENGINE, Engine};
    use bytes::BytesMut;
    use sha1::Digest;
    use tokio_util::codec::{Decoder, Encoder};

    pub use websocket_codec as codec;

    #[derive(Debug)]
    pub enum WsDecodeError {
        InvalidString,
        InvalidUpgradeRequest,
        InvalidHttpVersion,
        InvalidUpgradeHeaders,
        MissingWebSocketKey,
        Io(std::io::Error),
    }

    #[derive(Debug)]
    pub enum WsEncodeError {
        Io(std::io::Error),
    }

    impl From<std::io::Error> for WsDecodeError {
        fn from(err: std::io::Error) -> WsDecodeError {
            WsDecodeError::Io(err)
        }
    }

    impl From<std::io::Error> for WsEncodeError {
        fn from(err: std::io::Error) -> WsEncodeError {
            WsEncodeError::Io(err)
        }
    }

    #[derive(Debug, Default)]
    pub struct WsUpgraderCodec {}

    impl WsUpgraderCodec {
        pub fn new() -> Self {
            Self {}
        }

        fn validate_request_line(request_line: &str) -> Result<(), WsDecodeError> {
            let mut request_parts = request_line.split_whitespace();
            let method = request_parts.next();
            let uri = request_parts.next();
            let version = request_parts.next();

            match (method, uri, version) {
                (Some(method), Some(_uri), Some(version)) => {
                    let is_get = method.eq_ignore_ascii_case("get");
                    let http_version =
                        version.split('/').nth(1).ok_or(WsDecodeError::InvalidHttpVersion)?;

                    let mut versions = http_version.split('.');
                    let major_str = versions.next().ok_or(WsDecodeError::InvalidHttpVersion)?;
                    let minor_str = versions.next().ok_or(WsDecodeError::InvalidHttpVersion)?;

                    let major: u8 =
                        major_str.parse().map_err(|_| WsDecodeError::InvalidHttpVersion)?;
                    let minor: u8 =
                        minor_str.parse().map_err(|_| WsDecodeError::InvalidHttpVersion)?;

                    let version_is_ok = major > 1 || (major == 1 && minor >= 1);

                    if is_get && version_is_ok {
                        return Ok(());
                    }
                },
                _ => return Err(WsDecodeError::InvalidUpgradeRequest),
            }

            Ok(())
        }

        fn validate_headers<'a>(
            header_lines: impl Iterator<Item = &'a str>,
        ) -> Result<&'a str, WsDecodeError> {
            let mut websocket_key: Option<&'a str> = None;

            let mut header_lines = header_lines.peekable();

            while let Some(header_line) = header_lines.next() {
                let mut split_line = header_line.split(':');
                let header_name =
                    split_line.next().ok_or(WsDecodeError::InvalidUpgradeHeaders)?.trim();
                let header_val =
                    split_line.next().ok_or(WsDecodeError::InvalidUpgradeHeaders)?.trim();

                match header_name {
                    header if header.eq_ignore_ascii_case("Upgrade") => {
                        if header_val != "websocket" {
                            return Err(WsDecodeError::InvalidUpgradeHeaders);
                        }
                    },
                    header if header.eq_ignore_ascii_case("Connection") => {
                        if header_val != "Upgrade" {
                            return Err(WsDecodeError::InvalidUpgradeHeaders);
                        }
                    },
                    header if header.eq_ignore_ascii_case("Sec-WebSocket-Key") => {
                        websocket_key = Some(header_val);
                    },
                    header if header.eq_ignore_ascii_case("Sec-WebSocket-Version") => {
                        if header_val != "13" {
                            return Err(WsDecodeError::InvalidUpgradeHeaders);
                        }
                    },
                    header if header.eq_ignore_ascii_case("Sec-WebSocket-Protocol") => {
                        let mut versions = header_val.split(',');

                        if !versions.any(|proto| proto == "mqtt") {
                            return Err(WsDecodeError::InvalidUpgradeHeaders);
                        }
                    },
                    _ => {},
                }

                if header_lines.peek() == Some(&"") {
                    break;
                }
            }

            websocket_key.ok_or(WsDecodeError::MissingWebSocketKey)
        }
    }

    impl Decoder for WsUpgraderCodec {
        type Error = WsDecodeError;
        type Item = String;

        fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            match String::from_utf8(buf[..].into()) {
                Ok(s) => {
                    let mut lines = s.split("\r\n");

                    if let Some(request_line) = lines.next() {
                        Self::validate_request_line(request_line)?;

                        let websocket_key = Self::validate_headers(lines)?;

                        let mut hasher = sha1::Sha1::new();
                        hasher.update(websocket_key.as_bytes());
                        hasher.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
                        let sha1_bytes = hasher.finalize();
                        let sha1_str = STANDARD_BASE64_ENGINE.encode(sha1_bytes);

                        let _rest = buf.split_to(s.len());

                        Ok(Some(sha1_str))
                    } else {
                        Ok(None)
                    }
                },
                Err(_e) => Err(WsDecodeError::InvalidString),
            }
        }
    }

    impl Encoder<String> for WsUpgraderCodec {
        type Error = WsEncodeError;

        fn encode(
            &mut self,
            websocket_key: String,
            bytes: &mut BytesMut,
        ) -> Result<(), Self::Error> {
            let response = format!(
                "HTTP/1.1 101 Switching Protocols\r\n\
                 Upgrade: websocket\r\n\
                 Connection: Upgrade\r\n\
                 Sec-WebSocket-Protocol: mqtt\r\n\
                 Sec-WebSocket-Accept: {}\r\n\r\n",
                websocket_key
            );

            bytes.extend_from_slice(response.as_bytes());
            Ok(())
        }
    }
}
