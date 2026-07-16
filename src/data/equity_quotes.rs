use crate::data::types::EquityQuote;
use crate::domain::market::Ticker;
use chrono::DateTime;
use chrono::Utc;

pub fn parse_quote_messages(text: &str) -> Vec<EquityQuote> {
    let messages: Vec<serde_json::Value> = match serde_json::from_str(text) {
        Ok(messages) => messages,
        Err(_) => return Vec::new(),
    };

    messages
        .into_iter()
        .filter_map(|message| {
            if message.get("T").and_then(|t| t.as_str()) != Some("q") {
                return None;
            }

            let ticker = message
                .get("S")
                .and_then(|s| s.as_str())
                .and_then(Ticker::new)?;
            let bid_price = message.get("bp").and_then(|v| v.as_f64())?;
            let ask_price = message.get("ap").and_then(|v| v.as_f64())?;
            let bid_size_raw = message.get("bs").and_then(|v| v.as_i64())?;
            let ask_size_raw = message.get("as").and_then(|v| v.as_i64())?;
            if bid_size_raw < 0 || ask_size_raw < 0 {
                return None;
            }
            let bid_size = i32::try_from(bid_size_raw).ok()?;
            let ask_size = i32::try_from(ask_size_raw).ok()?;
            let timestamp_str = message.get("t").and_then(|v| v.as_str())?;
            let timestamp = timestamp_str.parse::<DateTime<Utc>>().ok()?;

            Some(EquityQuote::new(
                timestamp, ticker, bid_price, ask_price, bid_size, ask_size,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::parse_quote_messages;

    #[test]
    fn test_parse_quote_messages_returns_quotes() {
        let text = r#"[
            {"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z","bx":"Q","ax":"C","c":["R"],"z":"C"},
            {"T":"q","S":"MSFT","bp":420.10,"ap":420.20,"bs":2,"as":4,"t":"2026-05-23T14:30:00.001Z","bx":"Q","ax":"C","c":["R"],"z":"C"}
        ]"#;

        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 2);
        assert_eq!(quotes[0].ticker(), "AAPL");
        assert_eq!(quotes[0].bid_price(), 150.50);
        assert_eq!(quotes[0].ask_price(), 150.55);
        assert_eq!(quotes[0].bid_size(), 5);
        assert_eq!(quotes[0].ask_size(), 3);
        assert_eq!(quotes[1].ticker(), "MSFT");
    }

    #[test]
    fn test_parse_quote_messages_filters_non_quote_types() {
        let text = r#"[
            {"T":"connected","msg":"connected"},
            {"T":"success","msg":"authenticated"},
            {"T":"subscription","quotes":["AAPL"]},
            {"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}
        ]"#;

        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker(), "AAPL");
    }

    #[test]
    fn test_parse_quote_messages_handles_invalid_json() {
        let quotes = parse_quote_messages("not valid json");
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_required_fields() {
        // Missing "ap" (ask_price) and "t" (timestamp)
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"bs":5,"as":3}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_returns_empty_for_empty_array() {
        let quotes = parse_quote_messages("[]");
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_invalid_timestamp() {
        let text =
            r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"not-a-timestamp"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_invalid_ticker() {
        // Ticker with more than 5 base characters is rejected by Ticker::new.
        let text = r#"[{"T":"q","S":"TOOLONG","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_ticker_field() {
        let text =
            r#"[{"T":"q","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_bid_price() {
        let text =
            r#"[{"T":"q","S":"AAPL","ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_bid_size_overflow() {
        // bid_size values that exceed i32::MAX cannot be represented and must be dropped.
        let oversized: i64 = i64::from(i32::MAX) + 1;
        let text = format!(
            r#"[{{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":{},"as":3,"t":"2026-05-23T14:30:00.000Z"}}]"#,
            oversized
        );
        let quotes = parse_quote_messages(&text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_ask_size_overflow() {
        let oversized: i64 = i64::from(i32::MAX) + 1;
        let text = format!(
            r#"[{{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":{},"t":"2026-05-23T14:30:00.000Z"}}]"#,
            oversized
        );
        let quotes = parse_quote_messages(&text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_accepts_class_share_ticker() {
        // BRK.B is a valid Alpaca ticker and must be accepted.
        let text = r#"[{"T":"q","S":"BRK.B","bp":300.00,"ap":300.10,"bs":2,"as":2,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker(), "BRK.B");
    }

    #[test]
    fn test_parse_quote_messages_mixed_valid_and_invalid() {
        // Only the valid quote (AAPL) should be returned; the invalid one (TOOLONG) is dropped.
        let text = r#"[
            {"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"},
            {"T":"q","S":"TOOLONG","bp":200.00,"ap":200.10,"bs":1,"as":1,"t":"2026-05-23T14:30:01.000Z"}
        ]"#;
        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker(), "AAPL");
    }

    #[test]
    fn test_parse_quote_messages_timestamp_preserves_value() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        let expected = "2026-05-23T14:30:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        assert_eq!(quotes[0].timestamp(), expected);
    }

    #[test]
    fn test_parse_quote_messages_non_array_json_returns_empty() {
        // The Alpaca stream always sends arrays; a non-array object must be ignored.
        let text = r#"{"T":"q","S":"AAPL","bp":150.50}"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_ask_price() {
        let text =
            r#"[{"T":"q","S":"AAPL","bp":150.50,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_bid_size() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_ask_size() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_skips_missing_timestamp_field() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_single_element_array() {
        let text = r#"[{"T":"q","S":"NVDA","bp":800.00,"ap":800.10,"bs":1,"as":2,"t":"2026-05-23T09:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].ticker(), "NVDA");
        assert_eq!(quotes[0].bid_price(), 800.00);
        assert_eq!(quotes[0].ask_price(), 800.10);
        assert_eq!(quotes[0].bid_size(), 1);
        assert_eq!(quotes[0].ask_size(), 2);
    }

    #[test]
    fn test_parse_quote_messages_ticker_field_is_not_string() {
        // If the "S" field is not a string the message is skipped.
        let text = r#"[{"T":"q","S":12345,"bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_empty_ticker_string_skipped() {
        let text = r#"[{"T":"q","S":"","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_type_field_missing_is_filtered() {
        // A message without a "T" field is treated as a non-quote message type.
        let text = r#"[{"S":"AAPL","bp":150.50,"ap":150.55,"bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_ask_price_is_not_number() {
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":"not-a-number","bs":5,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }

    #[test]
    fn test_parse_quote_messages_bid_size_negative_value_is_rejected() {
        // Negative bid_size is invalid domain data; the parser must reject it.
        let text = r#"[{"T":"q","S":"AAPL","bp":150.50,"ap":150.55,"bs":-1,"as":3,"t":"2026-05-23T14:30:00.000Z"}]"#;
        let quotes = parse_quote_messages(text);
        assert!(quotes.is_empty());
    }
}
