use crate::pricing::{Book, Level};
use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, watch};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const SUBSCRIPTION_BATCH_SIZE: usize = 200;
/// Upper bound on the market-feed reconnect delay. Deliberately low:
/// Polymarket's edge resets market sockets constantly, so what drives book
/// staleness is recovery time, not retry volume — every second spent backing
/// off is a second of stale book.
const RECONNECT_DELAY_MAX: Duration = Duration::from_millis(5_000);
/// How long a connection must stay up before its reconnect backoff counts as
/// recovered. Without it the attempt counter only ever grows, so a handful of
/// early drops pin the feed at the maximum delay for the life of the process
/// even once the venue is healthy again.
const STABLE_CONNECTION: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketInfo {
    pub condition_id: String,
    pub token_ids: Vec<String>,
    pub fee_rate: Option<Decimal>,
    pub fee_exponent: Option<i64>,
    pub fee_taker_only: Option<bool>,
    pub tick_size: Decimal,
    pub accepting_orders: bool,
}

#[derive(Clone)]
pub struct RestClient {
    client: reqwest::Client,
    books_endpoint: String,
    market_info_endpoint: String,
}

impl RestClient {
    pub fn new(books_endpoint: String, market_info_endpoint: String, timeout_ms: u64) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_millis(timeout_ms))
                .user_agent("moni-strategy-beta/0.1.0")
                .build()
                .expect("static CLOB HTTP configuration is valid"),
            books_endpoint,
            market_info_endpoint,
        }
    }

    pub async fn books(
        &self,
        token_ids: &[String],
        observed_at_ms: i64,
    ) -> Result<(Vec<Book>, u64)> {
        let started = Instant::now();
        let response = self
            .client
            .post(&self.books_endpoint)
            .json(
                &token_ids
                    .iter()
                    .map(|token_id| serde_json::json!({"token_id": token_id}))
                    .collect::<Vec<_>>(),
            )
            .send()
            .await
            .context("requesting final CLOB books")?
            .error_for_status()
            .context("CLOB books request failed")?;
        let body = response
            .text()
            .await
            .context("reading CLOB books response")?;
        Ok((
            parse_books(&body, observed_at_ms)?,
            started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        ))
    }

    pub async fn market_info(&self, condition_id: &str) -> Result<MarketInfo> {
        let response = self
            .client
            .get(market_info_url(&self.market_info_endpoint, condition_id))
            .send()
            .await
            .context("requesting CLOB market info")?
            .error_for_status()
            .context("CLOB market info request failed")?;
        let body = response.text().await.context("reading CLOB market info")?;
        parse_market_info(&body, condition_id)
    }
}

fn market_info_url(endpoint: &str, condition_id: &str) -> String {
    format!(
        "{}/clob-markets/{}",
        endpoint.trim_end_matches('/'),
        condition_id
    )
}

pub fn parse_books(raw: &str, observed_at_ms: i64) -> Result<Vec<Book>> {
    let values: Vec<Value> = serde_json::from_str(raw).context("parsing CLOB books JSON")?;
    values
        .iter()
        .map(|value| parse_book(value, observed_at_ms))
        .collect()
}

pub fn parse_market_info(raw: &str, expected_condition_id: &str) -> Result<MarketInfo> {
    let value: Value = serde_json::from_str(raw).context("parsing CLOB market info JSON")?;
    let condition_id = string(&value, &["condition_id", "conditionId", "c"])?;
    if condition_id != expected_condition_id {
        bail!("CLOB market info condition id mismatch");
    }
    let tokens = value
        .get("tokens")
        .or_else(|| value.get("t"))
        .and_then(Value::as_array)
        .context("CLOB market info is missing tokens")?;
    let token_ids = tokens
        .iter()
        .map(|token| string(token, &["token_id", "tokenId", "t"]))
        .collect::<Result<Vec<_>>>()?;
    let curve = value
        .get("fee_curve")
        .or_else(|| value.get("feeCurve"))
        .or_else(|| value.get("fd"));
    let fee_rate = curve
        .and_then(|curve| curve.get("rate").or_else(|| curve.get("r")))
        .and_then(decimal);
    let fee_exponent = curve
        .and_then(|curve| curve.get("exponent").or_else(|| curve.get("e")))
        .and_then(Value::as_i64);
    let fee_taker_only = curve
        .and_then(|curve| {
            curve
                .get("taker_only")
                .or_else(|| curve.get("takerOnly"))
                .or_else(|| curve.get("to"))
                .or_else(|| curve.get("t"))
        })
        .and_then(Value::as_bool);
    let tick_size = value
        .get("minimum_tick_size")
        .or_else(|| value.get("minimumTickSize"))
        .or_else(|| value.get("mts"))
        .and_then(decimal)
        .context("CLOB market info is missing tick size")?;
    let accepting_orders = value
        .get("accepting_orders")
        .or_else(|| value.get("acceptingOrders"))
        .or_else(|| value.get("ao"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(MarketInfo {
        condition_id,
        token_ids,
        fee_rate,
        fee_exponent,
        fee_taker_only,
        tick_size,
        accepting_orders,
    })
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionDiff {
    pub subscribe: Vec<String>,
    pub unsubscribe: Vec<String>,
}

pub fn subscription_diff(
    current: &BTreeSet<String>,
    target: &BTreeSet<String>,
) -> SubscriptionDiff {
    SubscriptionDiff {
        subscribe: target.difference(current).cloned().collect(),
        unsubscribe: current.difference(target).cloned().collect(),
    }
}

fn subscription_messages(asset_ids: &[String], operation: Option<&str>) -> Vec<Message> {
    asset_ids
        .chunks(SUBSCRIPTION_BATCH_SIZE)
        .map(|chunk| {
            let mut value = serde_json::json!({
                "assets_ids": chunk,
                "custom_feature_enabled": true,
            });
            if let Some(operation) = operation {
                value["operation"] = Value::String(operation.to_owned());
            } else {
                value["type"] = Value::String("market".to_owned());
            }
            Message::text(value.to_string())
        })
        .collect()
}

pub struct MarketFeed {
    endpoint: String,
    targets: watch::Receiver<BTreeSet<String>>,
    books: Arc<RwLock<HashMap<String, Book>>>,
}

pub fn spawn_sharded_market_feed(
    endpoint: String,
    books: Arc<RwLock<HashMap<String, Book>>>,
    max_assets_per_connection: usize,
    max_total_assets: usize,
) -> watch::Sender<Vec<String>> {
    let shard_count = max_total_assets.div_ceil(max_assets_per_connection);
    let (target_sender, mut target_receiver) = watch::channel(Vec::<String>::new());
    let mut shard_senders = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let (sender, receiver) = watch::channel(BTreeSet::new());
        shard_senders.push(sender);
        tokio::spawn(MarketFeed::new(endpoint.clone(), receiver, books.clone()).run());
    }
    tokio::spawn(async move {
        let mut assignments =
            ShardAssignments::new(shard_count, max_assets_per_connection, max_total_assets);
        loop {
            if target_receiver.changed().await.is_err() {
                return;
            }
            let targets = target_receiver.borrow_and_update().clone();
            let shards = match assignments.assign(&targets) {
                Ok(value) => value,
                Err(error) => {
                    tracing::error!(%error, "CLOB target set rejected");
                    continue;
                }
            };
            for (sender, target) in shard_senders.iter().zip(shards) {
                let _ = sender.send(target);
            }
        }
    });
    target_sender
}

#[derive(Debug)]
struct ShardAssignments {
    shard_count: usize,
    max_per_shard: usize,
    max_total: usize,
    assigned_shard: BTreeMap<String, usize>,
}

impl ShardAssignments {
    fn new(shard_count: usize, max_per_shard: usize, max_total: usize) -> Self {
        Self {
            shard_count,
            max_per_shard,
            max_total,
            assigned_shard: BTreeMap::new(),
        }
    }

    fn assign(&mut self, asset_ids: &[String]) -> Result<Vec<BTreeSet<String>>> {
        if asset_ids.len() > self.max_total {
            bail!(
                "{} assets exceed configured maximum {}",
                asset_ids.len(),
                self.max_total
            );
        }
        let target = asset_ids.iter().cloned().collect::<BTreeSet<_>>();
        self.assigned_shard
            .retain(|asset_id, _| target.contains(asset_id));
        let mut shards = vec![BTreeSet::new(); self.shard_count];
        for (asset_id, shard) in &self.assigned_shard {
            shards[*shard].insert(asset_id.clone());
        }
        for pair in asset_ids.chunks(2) {
            if pair
                .iter()
                .all(|asset_id| self.assigned_shard.contains_key(asset_id))
            {
                continue;
            }
            let preferred = pair
                .iter()
                .find_map(|asset_id| self.assigned_shard.get(asset_id).copied());
            let shard = preferred
                .filter(|shard| shards[*shard].len() + pair.len() <= self.max_per_shard)
                .or_else(|| {
                    shards
                        .iter()
                        .position(|assets| assets.len() + pair.len() <= self.max_per_shard)
                })
                .context("no CLOB shard has capacity for an outcome pair")?;
            for asset_id in pair {
                if self.assigned_shard.contains_key(asset_id) {
                    continue;
                }
                self.assigned_shard.insert(asset_id.clone(), shard);
                shards[shard].insert(asset_id.clone());
            }
        }
        Ok(shards)
    }
}

impl MarketFeed {
    pub fn new(
        endpoint: String,
        targets: watch::Receiver<BTreeSet<String>>,
        books: Arc<RwLock<HashMap<String, Book>>>,
    ) -> Self {
        Self {
            endpoint,
            targets,
            books,
        }
    }

    pub async fn run(mut self) -> ! {
        let mut attempt = 0_u32;
        loop {
            let mut connected_at = None;
            if let Err(error) = self.run_once(&mut connected_at).await {
                tracing::warn!(%error, "CLOB market feed disconnected");
            }
            if connected_at.is_some_and(|at: Instant| at.elapsed() >= STABLE_CONNECTION) {
                attempt = 0;
            }
            tokio::time::sleep(reconnect_delay(attempt)).await;
            attempt = attempt.saturating_add(1);
        }
    }

    async fn run_once(&mut self, connected_at: &mut Option<Instant>) -> Result<()> {
        while self.targets.borrow().is_empty() {
            self.targets
                .changed()
                .await
                .context("CLOB target set channel closed")?;
        }
        let (stream, _) = connect_async(&self.endpoint)
            .await
            .context("connecting CLOB market feed")?;
        *connected_at = Some(Instant::now());
        let (mut sink, mut stream) = stream.split();
        let mut subscribed = self.targets.borrow().clone();
        for message in subscription_messages(&subscribed.iter().cloned().collect::<Vec<_>>(), None)
        {
            sink.send(message)
                .await
                .context("sending initial subscription")?;
        }
        let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    sink.send(Message::Text("PING".into())).await.context("sending CLOB heartbeat")?;
                }
                changed = self.targets.changed() => {
                    changed.context("CLOB target set channel closed")?;
                    let target = self.targets.borrow().clone();
                    let diff = subscription_diff(&subscribed, &target);
                    for message in subscription_messages(&diff.unsubscribe, Some("unsubscribe")) {
                        sink.send(message).await.context("sending unsubscribe")?;
                    }
                    for message in subscription_messages(&diff.subscribe, Some("subscribe")) {
                        sink.send(message).await.context("sending subscribe")?;
                    }
                    if !diff.unsubscribe.is_empty() {
                        let mut books = self.books.write().await;
                        for token in &diff.unsubscribe {
                            books.remove(token);
                        }
                    }
                    subscribed = target;
                }
                frame = stream.next() => {
                    let Some(frame) = frame else { bail!("CLOB stream closed") };
                    match frame.context("reading CLOB frame")? {
                        Message::Text(text) => self.apply_frame(text.as_str(), &subscribed).await,
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                        Message::Close(_) => bail!("CLOB stream closed"),
                        _ => {}
                    }
                }
            }
        }
    }

    async fn apply_frame(&self, raw: &str, subscribed: &BTreeSet<String>) {
        let Ok(value) = serde_json::from_str::<Value>(raw) else {
            return;
        };
        let values = value
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_else(|| std::slice::from_ref(&value));
        for event in values {
            let event_type = event.get("event_type").and_then(Value::as_str);
            if event_type == Some("book") || event.get("bids").is_some() {
                if let Ok(book) = parse_book(event, now_millis())
                    && subscribed.contains(&book.token_id)
                {
                    self.books.write().await.insert(book.token_id.clone(), book);
                }
            } else if event_type == Some("price_change") {
                self.apply_changes(event, subscribed).await;
            }
        }
    }

    async fn apply_changes(&self, event: &Value, subscribed: &BTreeSet<String>) {
        let Some(changes) = event
            .get("price_changes")
            .or_else(|| event.get("changes"))
            .and_then(Value::as_array)
        else {
            return;
        };
        let mut books = self.books.write().await;
        for change in changes {
            let Ok(token_id) = string(change, &["asset_id", "assetId"]) else {
                continue;
            };
            if !subscribed.contains(&token_id) {
                continue;
            }
            let (Some(price), Some(size), Some(side)) = (
                change.get("price").and_then(decimal),
                change.get("size").and_then(decimal),
                change.get("side").and_then(Value::as_str),
            ) else {
                continue;
            };
            let Some(book) = books.get_mut(&token_id) else {
                continue;
            };
            let levels = if side.eq_ignore_ascii_case("buy") {
                &mut book.bids
            } else if side.eq_ignore_ascii_case("sell") {
                &mut book.asks
            } else {
                continue;
            };
            levels.retain(|level| level.price != price);
            if size > Decimal::ZERO {
                levels.push(Level { price, size });
            }
            let _ = book.normalize();
            book.updated_at_ms = timestamp(event).unwrap_or_else(now_millis);
        }
    }
}

fn parse_book(value: &Value, observed_at_ms: i64) -> Result<Book> {
    let mut book = Book {
        market_id: string(value, &["market", "market_id"])?,
        token_id: string(value, &["asset_id", "token_id"])?,
        bids: parse_levels(value.get("bids"))?,
        asks: parse_levels(value.get("asks"))?,
        updated_at_ms: timestamp(value).unwrap_or(observed_at_ms),
    };
    book.normalize()?;
    Ok(book)
}

fn parse_levels(value: Option<&Value>) -> Result<Vec<Level>> {
    value
        .and_then(Value::as_array)
        .context("book side is missing")?
        .iter()
        .map(|level| {
            Ok(Level {
                price: level
                    .get("price")
                    .and_then(decimal)
                    .context("book level price is missing")?,
                size: level
                    .get("size")
                    .and_then(decimal)
                    .context("book level size is missing")?,
            })
        })
        .collect()
}

fn timestamp(value: &Value) -> Option<i64> {
    value
        .get("timestamp")
        .or_else(|| value.get("ts"))
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
        })
}

fn string(value: &Value, names: &[&str]) -> Result<String> {
    names
        .iter()
        .find_map(|name| value.get(*name))
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
        .context("required string is missing")
}

fn decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(value) => Decimal::from_str(value).ok(),
        Value::Number(value) => Decimal::from_str(&value.to_string()).ok(),
        _ => None,
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Jittered exponential backoff for market-feed reconnects: doubles per
/// attempt from 250ms, capped at [`RECONNECT_DELAY_MAX`], then randomized to
/// 50-100% of that value. The jitter matters because the shards drop together
/// when the venue resets connections — without it they all retry in lockstep.
fn reconnect_delay(attempt: u32) -> Duration {
    let base = Duration::from_millis(250)
        .saturating_mul(1_u32 << attempt.min(5))
        .min(RECONNECT_DELAY_MAX);
    let jitter_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or(0);
    base.mul_f64(0.5 + 0.5 * (jitter_nanos % 1000) as f64 / 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_delay_grows_and_caps_with_jitter() {
        for attempt in 0..8 {
            let delay = reconnect_delay(attempt);
            let base = Duration::from_millis(250)
                .saturating_mul(1_u32 << attempt.min(5))
                .min(RECONNECT_DELAY_MAX);
            assert!(delay >= base / 2, "attempt {attempt}: {delay:?} < half base");
            assert!(delay <= base, "attempt {attempt}: {delay:?} over base");
        }
        assert!(reconnect_delay(20) <= RECONNECT_DELAY_MAX);
    }

    #[test]
    fn dynamic_subscription_uses_set_difference() {
        let current = ["1", "2"].into_iter().map(str::to_owned).collect();
        let target = ["2", "3"].into_iter().map(str::to_owned).collect();
        let diff = subscription_diff(&current, &target);
        assert_eq!(diff.unsubscribe, vec!["1"]);
        assert_eq!(diff.subscribe, vec!["3"]);
        let unsubscribe = subscription_messages(&diff.unsubscribe, Some("unsubscribe"));
        assert!(
            unsubscribe[0]
                .to_string()
                .contains("\"operation\":\"unsubscribe\"")
        );
    }

    #[test]
    fn shard_assignments_keep_outcome_pairs_together() {
        let mut assignments = ShardAssignments::new(2, 4, 8);
        let shards = assignments
            .assign(&[
                "a-yes".to_owned(),
                "a-no".to_owned(),
                "b-yes".to_owned(),
                "b-no".to_owned(),
                "c-yes".to_owned(),
                "c-no".to_owned(),
            ])
            .unwrap();
        let shard_for = |token: &str| {
            shards
                .iter()
                .position(|assets| assets.contains(token))
                .unwrap()
        };
        assert_eq!(shard_for("a-yes"), shard_for("a-no"));
        assert_eq!(shard_for("b-yes"), shard_for("b-no"));
        assert_eq!(shard_for("c-yes"), shard_for("c-no"));
    }

    #[test]
    fn parses_and_sorts_batched_rest_books() {
        let books = parse_books(
            r#"[{"market":"m","asset_id":"a","timestamp":"100",
                "bids":[{"price":"0.4","size":"2"},{"price":"0.5","size":"1"}],
                "asks":[{"price":"0.7","size":"1"},{"price":"0.6","size":"2"}]}]"#,
            200,
        )
        .unwrap();
        assert_eq!(books[0].bids[0].price, Decimal::new(5, 1));
        assert_eq!(books[0].asks[0].price, Decimal::new(6, 1));
        assert_eq!(books[0].updated_at_ms, 100);
    }

    #[test]
    fn market_info_requires_current_fee_and_order_metadata() {
        let info = parse_market_info(
            r#"{"c":"c","t":[{"t":"1","o":"Yes"},{"t":"2","o":"No"}],
                "fd":{"r":0.07,"e":1,"to":true},"mts":0.01,"ao":true}"#,
            "c",
        )
        .unwrap();
        assert_eq!(info.fee_rate, Some(Decimal::new(7, 2)));
        assert_eq!(info.fee_exponent, Some(1));
        assert_eq!(info.fee_taker_only, Some(true));
        assert!(info.accepting_orders);
    }

    #[test]
    fn market_info_uses_current_clob_market_path() {
        assert_eq!(
            market_info_url("https://clob.polymarket.com/", "condition-a"),
            "https://clob.polymarket.com/clob-markets/condition-a"
        );
    }
}
