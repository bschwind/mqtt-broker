use crate::types::{Packet, PacketType, ParseError};
use bytes::{Buf, BytesMut};
use std::convert::TryFrom;

fn encode_variable_int(value: u32, buf: &mut [u8]) -> usize {
    let mut x = value;
    let mut byte_counter = 0;

    loop {
        let mut encoded_byte: u8 = (x % 128) as u8;
        x = x / 128;

        if x > 0 {
            encoded_byte = encoded_byte | 128;
        }

        buf[byte_counter] = encoded_byte;

        byte_counter += 1;

        if x <= 0 {
            break;
        }
    }

    byte_counter
}

fn decode_variable_int(bytes: &mut BytesMut) -> Result<Option<u32>, ParseError> {
    if bytes.len() < 1 {
        Ok(None)
    } else {
        let mut multiplier = 1;
        let mut value: u32 = 0;

        loop {
            let encoded_byte;
            if bytes.has_remaining() {
                encoded_byte = bytes.get_u8();
            } else {
                return Ok(None);
            }

            value += ((encoded_byte & 0b01111111) as u32) * multiplier;
            multiplier *= 128;

            if multiplier > (128 * 128 * 128) {
                return Err(ParseError::InvalidRemainingLength);
            }

            if encoded_byte & 0b10000000 == 0b00000000 {
                break;
            }
        }

        Ok(Some(value))
    }
}

fn parse_string(bytes: &mut BytesMut) -> Result<Option<String>, ParseError> {
    if bytes.len() < 2 {
        return Ok(None);
    }

    let str_size_bytes = bytes.get_u16() as usize;

    if bytes.len() < str_size_bytes {
        return Ok(None);
    }

    let string_bytes = bytes.split_to(str_size_bytes);

    // TODO - Use Cow<str> and from_utf8_lossy later for less copying
    match String::from_utf8(string_bytes.to_vec()) {
        Ok(string) => Ok(Some(string)),
        Err(_) => {
            return Err(ParseError::InvalidUtf8);
        },
    }
}

pub fn parse_mqtt(bytes: &mut BytesMut) -> Result<Option<Packet>, ParseError> {
    if bytes.len() < 2 {
        return Ok(None);
    }

    let first_byte = bytes.get_u8();

    let first_byte_val = (first_byte & 0b11110000) >> 4;
    let packet_type = PacketType::try_from(first_byte_val)?;
    let remaining_length_opt = decode_variable_int(bytes)?;

    if remaining_length_opt.is_none() {
        return Ok(None);
    }

    let remaining_length = remaining_length_opt.unwrap();

    if bytes.len() < remaining_length as usize {
        // If we don't have the full payload, just bail
        return Ok(None);
    } else if bytes.len() > remaining_length as usize {
        // If more bytes were provided than was stated, something is up
        return Err(ParseError::PacketTooLarge);
    }

    let rest = bytes.split_to(remaining_length as usize);
    let packet = Packet::new(packet_type, &rest);

    Ok(Some(packet))
}
