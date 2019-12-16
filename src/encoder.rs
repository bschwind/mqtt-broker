fn encode_variable_int(value: u32, buf: &mut [u8]) -> usize {
    let mut x = value;
    let mut byte_counter = 0;

    loop {
        let mut encoded_byte: u8 = (x % 128) as u8;
        x /= 128;

        if x > 0 {
            encoded_byte |= 128;
        }

        buf[byte_counter] = encoded_byte;

        byte_counter += 1;

        if x == 0 {
            break;
        }
    }

    byte_counter
}
