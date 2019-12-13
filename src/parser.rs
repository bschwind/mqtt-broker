use crate::types::{ConnectVariableHeader, Packet, PacketType, ParseError};
use bytes::{Buf, BytesMut};
use std::{convert::TryFrom, io::Cursor};

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

fn decode_variable_int(bytes: &mut Cursor<&mut BytesMut>) -> Result<Option<u32>, ParseError> {
    if bytes.get_ref().len() < 1 {
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

fn parse_string(bytes: &mut Cursor<&mut BytesMut>) -> Result<Option<String>, ParseError> {
    if bytes.get_ref().len() < 2 {
        return Ok(None);
    }

    let str_size_bytes = bytes.get_u16() as usize;

    if bytes.get_ref().len() < str_size_bytes {
        return Ok(None);
    }

    let position = bytes.position() as usize;

    // TODO - Use Cow<str> and from_utf8_lossy later for less copying
    match String::from_utf8(bytes.get_ref()[position..(position + str_size_bytes)].into()) {
        Ok(string) => {
            bytes.advance(str_size_bytes);
            Ok(Some(string))
        },
        Err(_) => {
            return Err(ParseError::InvalidUtf8);
        },
    }
}

macro_rules! return_if_none {
    ($x: expr) => {{
        let string_opt = $x;
        if string_opt.is_none() {
            return Ok(None);
        }

        string_opt.unwrap()
    }};
}

fn parse_variable_header(
    packet_type: &PacketType,
    bytes: &mut Cursor<&mut BytesMut>,
) -> Result<Option<ConnectVariableHeader>, ParseError> {
    match packet_type {
        PacketType::Connect => {
            let protocol_name = return_if_none!(parse_string(bytes)?);
            let protocol_level = bytes.get_u8();
            let connect_flags = bytes.get_u8();
            let keep_alive = bytes.get_u16();

            let _property_length = return_if_none!(decode_variable_int(bytes)?);

            Ok(Some(ConnectVariableHeader {
                protocol_name,
                protocol_level,
                connect_flags,
                keep_alive,

                session_expiry_interval: None,
                receive_maximum: None,
                maximum_packet_size: None,
                topic_alias_maximum: None,
                request_response_information: None,
                request_problem_information: None,
                user_properties: vec![],
                authentication_method: None,
                authentication_data: None,
            }))
        },
        _ => Ok(None),
    }
}

pub fn parse_mqtt(bytes: &mut BytesMut) -> Result<Option<Packet>, ParseError> {
    let mut bytes_cursor = Cursor::new(bytes);

    if bytes_cursor.get_ref().len() < 2 {
        return Ok(None);
    }

    let first_byte = bytes_cursor.get_u8();

    let first_byte_val = (first_byte & 0b11110000) >> 4;
    let packet_type = PacketType::try_from(first_byte_val)?;
    let remaining_packet_length_opt = decode_variable_int(&mut bytes_cursor)?;

    if remaining_packet_length_opt.is_none() {
        return Ok(None);
    }

    let remaining_packet_length = remaining_packet_length_opt.unwrap();
    let cursor_pos = bytes_cursor.position() as usize;
    let remaining_buffer_amount = bytes_cursor.get_ref().len() - cursor_pos;

    if remaining_buffer_amount < remaining_packet_length as usize {
        // If we don't have the full payload, just bail
        return Ok(None);
    }

    let variable_header = parse_variable_header(&packet_type, &mut bytes_cursor)?;

    println!("variable_header: {:?}", variable_header);

    let bytes = bytes_cursor.into_inner();

    let split_pos = cursor_pos + remaining_packet_length as usize;
    let rest = bytes.split_to(split_pos);
    let packet = Packet::new(packet_type, &rest);

    Ok(Some(packet))
}
