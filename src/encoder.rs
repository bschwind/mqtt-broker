use crate::types::{ConnectAckPacket, DecodeError, Packet, SubscribeAckPacket};
use bytes::{BufMut, BytesMut};

fn encode_variable_int(value: u32, bytes: &mut BytesMut) -> usize {
    let mut x = value;
    let mut byte_counter = 0;

    loop {
        let mut encoded_byte: u8 = (x % 128) as u8;
        x /= 128;

        if x > 0 {
            encoded_byte |= 128;
        }

        bytes.put_u8(encoded_byte);

        byte_counter += 1;

        if x == 0 {
            break;
        }
    }

    byte_counter
}

fn encode_connect_ack(packet: &ConnectAckPacket, bytes: &mut BytesMut) -> Result<(), DecodeError> {
    // TODO - calculate remaining length
    let remaining_length = 3;

    encode_variable_int(remaining_length, bytes);

    let mut connect_ack_flags = 0b0000_0000;
    if packet.session_present {
        connect_ack_flags |= 0b0000_0001;
    }

    bytes.put_u8(connect_ack_flags);

    // TODO - replace with actual reason code
    let reason_code = 0;
    bytes.put_u8(reason_code);

    // TODO - replace with actual property length
    let property_length = 0;
    encode_variable_int(property_length, bytes);

    Ok(())
}

fn encode_subscribe_ack(
    packet: &SubscribeAckPacket,
    bytes: &mut BytesMut,
) -> Result<(), DecodeError> {
    // TODO - calculate remaining length
    let remaining_length = 3 + packet.reason_codes.len();

    encode_variable_int(remaining_length as u32, bytes);

    bytes.put_u16(packet.packet_id);

    // TODO - replace with actual property length
    let property_length = 0;
    encode_variable_int(property_length, bytes);

    for code in &packet.reason_codes {
        bytes.put_u8((*code) as u8);
    }

    Ok(())
}

pub fn encode_mqtt(packet: &Packet, bytes: &mut BytesMut) -> Result<(), DecodeError> {
    // TODO - reserve the exact amount
    // bytes.reserve(packet_size);
    let first_byte = packet.to_byte();
    let mut first_byte_val = (first_byte << 4) & 0b1111_0000;
    first_byte_val |= packet.fixed_header_flags();

    bytes.put_u8(first_byte_val);

    match packet {
        Packet::ConnectAck(p) => encode_connect_ack(p, bytes)?,
        Packet::SubscribeAck(p) => encode_subscribe_ack(p, bytes)?,
        _ => {},
    }

    Ok(())
}
