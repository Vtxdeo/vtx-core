pub fn check_json_limits(
    input: &str,
    max_len: usize,
    max_depth: usize,
) -> Result<(), &'static str> {
    if input.len() > max_len {
        return Err("payload too large");
    }

    let mut depth = 0usize;
    let mut in_string = false;
    let mut escape = false;

    for byte in input.bytes() {
        if in_string {
            if escape {
                escape = false;
                continue;
            }
            match byte {
                b'\\' => escape = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max_depth {
                    return Err("payload nested too deeply");
                }
            }
            b'}' | b']' => {
                if depth == 0 {
                    return Err("payload structure invalid");
                }
                depth -= 1;
            }
            _ => {}
        }
    }

    Ok(())
}
