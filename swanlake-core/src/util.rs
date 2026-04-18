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

/// Quotes a potentially dot-separated qualified name (e.g. `schema.table`)
/// by quoting each component independently so the dot is interpreted as a
/// schema/catalog separator rather than part of the identifier.
///
/// # Examples
///
/// ```
/// # use swanlake_core::util::quote_qualified_name;
/// assert_eq!(quote_qualified_name("swanlake.my_table"), r#""swanlake"."my_table""#);
/// assert_eq!(quote_qualified_name("plain"), r#""plain""#);
/// ```
pub fn quote_qualified_name(name: &str) -> String {
    name.split('.')
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(".")
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

    #[test]
    fn qualified_name_schema_table() {
        assert_eq!(
            quote_qualified_name("swanlake.my_table"),
            r#""swanlake"."my_table""#
        );
    }

    #[test]
    fn qualified_name_single_part() {
        assert_eq!(quote_qualified_name("users"), r#""users""#);
    }

    #[test]
    fn qualified_name_three_parts() {
        assert_eq!(
            quote_qualified_name("catalog.schema.table"),
            r#""catalog"."schema"."table""#
        );
    }
}
