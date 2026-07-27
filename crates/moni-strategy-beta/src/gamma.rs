use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutcomeToken {
    pub label: String,
    pub token_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryCryptoMarket {
    pub market_id: String,
    pub condition_id: String,
    pub question: String,
    pub rules: String,
    pub underlying: String,
    pub outcomes: [OutcomeToken; 2],
    pub start_time_ms: Option<i64>,
    pub end_time_ms: i64,
    pub tick_size: Decimal,
    pub min_order_size: Decimal,
    pub neg_risk: bool,
    pub gamma_fee_rate: Option<Decimal>,
    pub gamma_fee_exponent: Option<i64>,
    pub gamma_fee_taker_only: Option<bool>,
    pub active: bool,
    pub accepting_orders: bool,
}

impl BinaryCryptoMarket {
    pub fn subscribable(&self, now_ms: i64, max_horizon_ms: u64) -> bool {
        self.active
            && self.accepting_orders
            && self.start_time_ms.is_none_or(|start| start <= now_ms)
            && self.end_time_ms > now_ms
            && self.end_time_ms.saturating_sub(now_ms) <= max_horizon_ms as i64
    }
}

#[derive(Clone)]
pub struct GammaClient {
    http: reqwest::Client,
    endpoint: String,
    page_limit: usize,
    max_pages: usize,
}

impl GammaClient {
    pub fn new(endpoint: String, page_limit: usize, max_pages: usize) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .user_agent("moni-strategy-beta/0.1.0")
                .build()
                .expect("static Gamma HTTP configuration is valid"),
            endpoint,
            page_limit,
            max_pages,
        }
    }

    pub async fn discover(
        &self,
        now_ms: i64,
        max_horizon_ms: u64,
    ) -> Result<Vec<BinaryCryptoMarket>> {
        let mut markets = Vec::new();
        for page in 0..self.max_pages {
            let response = self
                .http
                .get(&self.endpoint)
                .query(&[
                    ("tag_slug", "crypto-prices".to_owned()),
                    ("active", "true".to_owned()),
                    ("closed", "false".to_owned()),
                    ("limit", self.page_limit.to_string()),
                    ("offset", page.saturating_mul(self.page_limit).to_string()),
                    ("order", "endDate".to_owned()),
                    ("ascending", "true".to_owned()),
                    ("end_date_min", rfc3339(now_ms)?),
                    (
                        "end_date_max",
                        rfc3339(now_ms.saturating_add(max_horizon_ms as i64))?,
                    ),
                ])
                .send()
                .await
                .context("requesting Gamma crypto market page")?;
            if response.status() == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
                break;
            }
            let response = response
                .error_for_status()
                .context("Gamma crypto market page failed")?;
            let body = response.text().await.context("reading Gamma response")?;
            let raw_count = page_item_count(&body)?;
            markets.extend(parse_markets(&body, now_ms, max_horizon_ms)?);
            if raw_count < self.page_limit {
                break;
            }
        }
        markets.sort_by(|left, right| left.market_id.cmp(&right.market_id));
        markets.dedup_by(|left, right| left.market_id == right.market_id);
        Ok(markets)
    }
}

pub fn parse_markets(
    raw: &str,
    now_ms: i64,
    max_horizon_ms: u64,
) -> Result<Vec<BinaryCryptoMarket>> {
    let value: Value = serde_json::from_str(raw).context("parsing Gamma JSON")?;
    let mut market_values = Vec::new();
    collect_markets(&value, &mut market_values);
    Ok(market_values
        .into_iter()
        .filter_map(|market| parse_market(market, now_ms, max_horizon_ms).ok())
        .collect())
}

fn collect_markets<'a>(value: &'a Value, output: &mut Vec<&'a Value>) {
    match value {
        Value::Array(items) => {
            for item in items {
                if let Some(markets) = item.get("markets").and_then(Value::as_array) {
                    output.extend(markets);
                } else {
                    output.push(item);
                }
            }
        }
        Value::Object(object) => {
            if let Some(items) = object
                .get("events")
                .or_else(|| object.get("data"))
                .or_else(|| object.get("markets"))
                .and_then(Value::as_array)
            {
                for item in items {
                    collect_markets(item, output);
                }
            } else {
                output.push(value);
            }
        }
        _ => {}
    }
}

fn parse_market(value: &Value, now_ms: i64, max_horizon_ms: u64) -> Result<BinaryCryptoMarket> {
    let active = bool_field(value, &["active"]).unwrap_or(false)
        && !bool_field(value, &["closed"]).unwrap_or(false)
        && !bool_field(value, &["archived"]).unwrap_or(false)
        && bool_field(value, &["enableOrderBook", "enable_order_book"]).unwrap_or(false);
    let accepting_orders =
        bool_field(value, &["acceptingOrders", "accepting_orders"]).unwrap_or(false);
    let end_time_ms = timestamp_field(value, &["endDate", "end_date"])?;
    if end_time_ms <= now_ms || end_time_ms.saturating_sub(now_ms) > max_horizon_ms as i64 {
        bail!("outside discovery horizon");
    }
    let market_id = string_field(value, &["id", "marketId", "market_id"])?;
    let condition_id = string_field(value, &["conditionId", "condition_id"])?;
    let question = string_field(value, &["question"])?;
    let rules = optional_string_field(value, &["rules", "description", "resolutionSource"])
        .filter(|rules| !rules.trim().is_empty())
        .context("market resolution rules are required")?;
    let labels = string_array_field(value, &["outcomes"])?;
    let token_ids = string_array_field(value, &["clobTokenIds", "clob_token_ids"])?;
    if labels.len() != 2 || token_ids.len() != 2 || token_ids[0] == token_ids[1] {
        bail!("market must have exactly two distinct outcome tokens");
    }
    if labels[0].trim().is_empty()
        || labels[1].trim().is_empty()
        || labels[0].eq_ignore_ascii_case(&labels[1])
        || token_ids.iter().any(|token| token.trim().is_empty())
    {
        bail!("invalid complementary outcomes");
    }
    let tick_size = decimal_field(
        value,
        &["orderPriceMinTickSize", "order_price_min_tick_size"],
    )?;
    let min_order_size = decimal_field(value, &["orderMinSize", "order_min_size"])?;
    if tick_size <= Decimal::ZERO || min_order_size <= Decimal::ZERO {
        bail!("invalid market size rules");
    }
    let start_time_ms = optional_timestamp_field(value, &["startTime", "start_time"])?;
    let slug = optional_string_field(value, &["slug"]).unwrap_or_default();
    Ok(BinaryCryptoMarket {
        market_id,
        condition_id,
        question,
        rules,
        underlying: infer_underlying(&slug),
        outcomes: [
            OutcomeToken {
                label: labels[0].clone(),
                token_id: token_ids[0].clone(),
            },
            OutcomeToken {
                label: labels[1].clone(),
                token_id: token_ids[1].clone(),
            },
        ],
        start_time_ms,
        end_time_ms,
        tick_size,
        min_order_size,
        neg_risk: bool_field(value, &["negRisk", "neg_risk"]).unwrap_or(false),
        gamma_fee_rate: fee_rate(value),
        gamma_fee_exponent: fee_exponent(value),
        gamma_fee_taker_only: fee_taker_only(value),
        active,
        accepting_orders,
    })
}

fn fee_rate(value: &Value) -> Option<Decimal> {
    value
        .get("feeCurve")
        .or_else(|| value.get("fee_curve"))
        .or_else(|| value.get("feeSchedule"))
        .or_else(|| value.get("fee_schedule"))
        .and_then(|curve| curve.get("rate").or_else(|| curve.get("r")))
        .and_then(decimal_value)
        .or_else(|| {
            value
                .get("fees")
                .and_then(|fees| fees.get("taker"))
                .and_then(|taker| taker.get("rate"))
                .and_then(decimal_value)
        })
}

fn fee_exponent(value: &Value) -> Option<i64> {
    value
        .get("feeCurve")
        .or_else(|| value.get("fee_curve"))
        .or_else(|| value.get("feeSchedule"))
        .or_else(|| value.get("fee_schedule"))
        .and_then(|curve| curve.get("exponent").or_else(|| curve.get("e")))
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
        })
}

fn fee_taker_only(value: &Value) -> Option<bool> {
    value
        .get("feeCurve")
        .or_else(|| value.get("fee_curve"))
        .or_else(|| value.get("feeSchedule"))
        .or_else(|| value.get("fee_schedule"))
        .and_then(|curve| {
            curve
                .get("takerOnly")
                .or_else(|| curve.get("taker_only"))
                .or_else(|| curve.get("t"))
        })
        .and_then(Value::as_bool)
}

fn infer_underlying(slug: &str) -> String {
    slug.split(['-', '_'])
        .next()
        .filter(|part| !part.is_empty())
        .unwrap_or("crypto")
        .to_ascii_uppercase()
}

fn page_item_count(raw: &str) -> Result<usize> {
    let value: Value = serde_json::from_str(raw)?;
    Ok(match value {
        Value::Array(items) => items.len(),
        Value::Object(object) => object
            .get("events")
            .or_else(|| object.get("data"))
            .or_else(|| object.get("markets"))
            .and_then(Value::as_array)
            .map_or(1, Vec::len),
        _ => 0,
    })
}

fn rfc3339(timestamp_ms: i64) -> Result<String> {
    let timestamp = OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000)
        .context("timestamp outside supported range")?;
    timestamp
        .format(&Rfc3339)
        .context("formatting Gamma timestamp")
}

fn bool_field(value: &Value, names: &[&str]) -> Option<bool> {
    names
        .iter()
        .find_map(|name| value.get(*name))
        .and_then(Value::as_bool)
}

fn string_field(value: &Value, names: &[&str]) -> Result<String> {
    optional_string_field(value, names).context("missing required string")
}

fn optional_string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name))
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
}

fn string_array_field(value: &Value, names: &[&str]) -> Result<Vec<String>> {
    let value = names
        .iter()
        .find_map(|name| value.get(*name))
        .context("missing string array")?;
    let value = if let Some(encoded) = value.as_str() {
        serde_json::from_str(encoded).context("parsing JSON-encoded string array")?
    } else {
        value.clone()
    };
    value
        .as_array()
        .context("field is not an array")?
        .iter()
        .map(|value| match value {
            Value::String(value) => Ok(value.clone()),
            Value::Number(value) => Ok(value.to_string()),
            _ => bail!("array contains a non-string value"),
        })
        .collect()
}

fn decimal_field(value: &Value, names: &[&str]) -> Result<Decimal> {
    names
        .iter()
        .find_map(|name| value.get(*name))
        .and_then(decimal_value)
        .context("missing decimal")
}

fn decimal_value(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(value) => Decimal::from_str(value).ok(),
        Value::Number(value) => Decimal::from_str(&value.to_string()).ok(),
        _ => None,
    }
}

fn timestamp_field(value: &Value, names: &[&str]) -> Result<i64> {
    optional_timestamp_field(value, names)?.context("missing timestamp")
}

fn optional_timestamp_field(value: &Value, names: &[&str]) -> Result<Option<i64>> {
    let Some(value) = names.iter().find_map(|name| value.get(*name)) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    if let Some(value) = value.as_i64() {
        return Ok(Some(value));
    }
    let raw = value.as_str().context("timestamp is not a string")?;
    let parsed = OffsetDateTime::parse(raw, &Rfc3339).context("invalid RFC3339 timestamp")?;
    Ok(Some(
        (parsed.unix_timestamp_nanos() / 1_000_000)
            .try_into()
            .context("timestamp outside i64")?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(start: &str, question: &str, outcomes: &str) -> String {
        format!(
            r#"[{{"slug":"btc-price-threshold","markets":[{{
              "id":"m1","conditionId":"0x01","question":"{question}",
              "description":"Resolves from the named price feed.",
              "active":true,"closed":false,"archived":false,
              "enableOrderBook":true,"acceptingOrders":true,
              "startTime":"{start}","endDate":"2026-07-27T12:30:00Z",
              "outcomes":{outcomes},"clobTokenIds":"[\"11\",\"22\"]",
              "orderPriceMinTickSize":"0.01","orderMinSize":"1","negRisk":false,
              "feeCurve":{{"rate":"0.07","exponent":1,"takerOnly":true}}
            }}]}}]"#
        )
    }

    #[test]
    fn discovers_binary_threshold_market_and_keeps_it_out_of_hot_set_before_start() {
        let now = 1_785_153_600_000;
        let raw = fixture(
            "2026-07-27T12:15:00Z",
            "Will BTC exceed $100k?",
            r#"["Yes","No"]"#,
        );
        let markets = parse_markets(&raw, now, 86_400_000).unwrap();
        assert_eq!(markets.len(), 1);
        assert!(!markets[0].subscribable(now, 86_400_000));
        assert_eq!(markets[0].outcomes[0].label, "Yes");
    }

    #[test]
    fn rejects_non_binary_or_duplicate_tokens() {
        let raw = fixture("2026-07-27T10:00:00Z", "BTC?", r#"["Yes","No","Maybe"]"#);
        assert!(
            parse_markets(&raw, 1_785_153_600_000, 86_400_000)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn page_count_preserves_collected_pages_at_a_later_422_boundary() {
        let full = serde_json::to_string(&vec![Value::Null; 100]).unwrap();
        assert_eq!(page_item_count(&full).unwrap(), 100);
        // The client breaks on 422 before parsing/replacing `markets`; this
        // pure count check guards the continue-vs-finish page decision.
        assert_eq!(page_item_count("[]").unwrap(), 0);
    }
}
