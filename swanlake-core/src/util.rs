//! Shared utility helpers.

/// Wraps a SQL identifier in double quotes, escaping any embedded double quotes
/// by doubling them (standard SQL identifier quoting).
///
/// # Examples
///
/// ```
/// # use swanlake_core::util::quote_identifier;
/// assert_eq!(quote_identifier("my_table"), r#""my_table""#);
/// assert_eq!(quote_identifier(r#"has"quote"#), r#""has""quote""#);
/// ```
pub fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', r#""""#);
    format!(r#""{escaped}""#)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_identifier() {
        assert_eq!(quote_identifier("users"), r#""users""#);
    }

    #[test]
    fn identifier_with_embedded_quotes() {
        assert_eq!(quote_identifier(r#"my"table"#), r#""my""table""#);
    }

    #[test]
    fn empty_identifier() {
        assert_eq!(quote_identifier(""), r#""""#);
    }

    #[test]
    fn identifier_with_spaces_and_special_chars() {
        assert_eq!(
            quote_identifier("some table; DROP TABLE--"),
            r#""some table; DROP TABLE--""#
        );
    }
}
