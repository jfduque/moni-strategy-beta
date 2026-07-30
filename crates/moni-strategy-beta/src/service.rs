use crate::clients::Monitor;
use crate::clob::{MarketInfo, RestClient, spawn_sharded_market_feed};
use crate::config::RuntimeConfig;
use crate::decision_store::DecisionStore;
use crate::gamma::{BinaryCryptoMarket, GammaClient};
use crate::gate::{
    CalibrationSample, CircuitState, DurationBucket, GateKey, GateState, append_jsonl,
};
use crate::link::{SubmitOutcome, Submitter, request};
use crate::pricing::{
    Book, Direction, Opportunity, Rejection, best_for_direction, validate_fee_metadata,
    validate_final_books,
};
use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, watch};

const REJECTION_HEARTBEAT_MS: i64 = 15 * 60 * 1_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Decision {
    pub(crate) observed_at_ms: i64,
    pub(crate) market_id: String,
    #[serde(default)]
    pub(crate) condition_id: Option<String>,
    #[serde(default)]
    pub(crate) token_id_a: Option<String>,
    #[serde(default)]
    pub(crate) token_id_b: Option<String>,
    pub(crate) direction: Option<Direction>,
    pub(crate) quantity: Option<Decimal>,
    pub(crate) expected_profit: Option<Decimal>,
    pub(crate) gate_unlocked: bool,
    pub(crate) submitted: bool,
    pub(crate) reason: String,
    #[serde(default)]
    pub(crate) store_coverage_a: Option<bool>,
    #[serde(default)]
    pub(crate) store_coverage_b: Option<bool>,
}

#[derive(Default)]
struct EpisodeTracker {
    active: BTreeMap<(String, Direction), String>,
    sequence: u64,
}

impl EpisodeTracker {
    fn episode(&mut self, market_id: &str, direction: Direction) -> String {
        self.active
            .entry((market_id.to_owned(), direction))
            .or_insert_with(|| {
                self.sequence = self.sequence.saturating_add(1);
                format!(
                    "{market_id}:{}:{}",
                    direction_label(direction),
                    self.sequence
                )
            })
            .clone()
    }

    fn close_other_directions(&mut self, market_id: &str, direction: Direction) {
        self.active
            .retain(|(existing_market, existing_direction), _| {
                existing_market != market_id || *existing_direction == direction
            });
    }

    fn close_market(&mut self, market_id: &str) {
        self.active
            .retain(|(existing_market, _), _| existing_market != market_id);
    }
}

pub struct StrategyService {
    config: RuntimeConfig,
    observe_only: bool,
    gamma: GammaClient,
    rest: RestClient,
    submitter: Submitter,
    monitor: Monitor,
    target_sender: watch::Sender<Vec<String>>,
    books: Arc<RwLock<HashMap<String, Book>>>,
    decisions: DecisionStore,
    gates: GateState,
    circuits: CircuitState,
    episodes: EpisodeTracker,
    submitted: BTreeMap<String, (String, Direction, Decimal, String)>,
    blocked_markets: BTreeSet<String>,
    daily_loss_halted: bool,
    market_info: HashMap<String, MarketInfo>,
    rejection_heartbeats: HashMap<String, (String, i64)>,
    cycle_decisions_written: u64,
}

impl StrategyService {
    pub async fn connect(config: RuntimeConfig, observe_only: bool) -> Result<Self> {
        let api_key = std::env::var(&config.monitor.api_key_env).with_context(|| {
            format!(
                "Monitor key environment variable {} is not set",
                config.monitor.api_key_env
            )
        })?;
        let submitter = Submitter::connect(config.link.endpoint.clone()).await?;
        let monitor = Monitor::connect(
            config.monitor.endpoint.clone(),
            &api_key,
            config.monitor.tenant_id.clone(),
        )
        .await?;
        let books = Arc::new(RwLock::new(HashMap::new()));
        let target_sender = spawn_sharded_market_feed(
            config.clob.ws_endpoint.clone(),
            books.clone(),
            config.clob.max_assets_per_connection,
            config.clob.max_total_assets,
        );
        Ok(Self {
            gamma: GammaClient::new(
                config.discovery.gamma_endpoint.clone(),
                config.discovery.page_limit,
                config.discovery.max_pages,
            ),
            rest: RestClient::new(
                config.clob.rest_endpoint.clone(),
                config.clob.market_info_endpoint.clone(),
                config.quality.rest_timeout_ms,
            ),
            gates: GateState::load(&config.state.gate_state_path)?,
            decisions: DecisionStore::open(&config.state.decision_db_path)?,
            config,
            observe_only,
            submitter,
            monitor,
            target_sender,
            books,
            circuits: CircuitState::default(),
            episodes: EpisodeTracker::default(),
            submitted: BTreeMap::new(),
            blocked_markets: BTreeSet::new(),
            daily_loss_halted: false,
            market_info: HashMap::new(),
            rejection_heartbeats: HashMap::new(),
            cycle_decisions_written: 0,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            self.config.discovery.refresh_interval_secs,
        ));
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    self.gates.save(&self.config.state.gate_state_path)?;
                    return Ok(());
                }
                _ = interval.tick() => {
                    let started = Instant::now();
                    match self.iteration(None).await {
                        Ok(()) => crate::metrics::record_cycle("success", started.elapsed()),
                        Err(error) => {
                            crate::metrics::record_cycle("failed", started.elapsed());
                            tracing::warn!(%error, "strategy-beta evaluation cycle failed");
                        }
                    }
                }
            }
        }
    }

    pub async fn diagnostic_once(mut self, market_filter: &str) -> Result<()> {
        self.iteration(Some(market_filter)).await
    }

    async fn iteration(&mut self, market_filter: Option<&str>) -> Result<()> {
        let started = Instant::now();
        let now_ms = now_millis();
        self.cycle_decisions_written = 0;
        self.reconcile_monitor().await?;
        let markets = self
            .gamma
            .discover(now_ms, self.config.discovery.max_horizon_ms)
            .await?;
        let active_conditions = markets
            .iter()
            .map(|market| market.condition_id.as_str())
            .collect::<BTreeSet<_>>();
        self.market_info
            .retain(|condition_id, _| active_conditions.contains(condition_id.as_str()));
        let active_markets = markets
            .iter()
            .map(|market| market.market_id.as_str())
            .collect::<BTreeSet<_>>();
        self.rejection_heartbeats
            .retain(|market_id, _| active_markets.contains(market_id.as_str()));
        let cache_entries_before = self.market_info.len();
        let subscribable_markets = markets
            .iter()
            .filter(|market| market.subscribable(now_ms, self.config.discovery.max_horizon_ms))
            .count();
        let targets = markets
            .iter()
            .filter(|market| market.subscribable(now_ms, self.config.discovery.max_horizon_ms))
            .flat_map(|market| {
                market
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.token_id.clone())
            })
            .collect();
        self.target_sender
            .send(targets)
            .context("updating CLOB target subscriptions")?;

        for market in markets
            .iter()
            .filter(|market| market_filter.is_none_or(|filter| market.market_id == filter))
        {
            if !market.subscribable(now_ms, self.config.discovery.max_horizon_ms) {
                self.log_decision(market, None, false, "market_not_subscribable", None, None)?;
                continue;
            }
            if let Err(error) = self.evaluate_market(market, now_ms).await {
                self.episodes.close_market(&market.market_id);
                if let Some(bucket) =
                    DurationBucket::from_remaining_ms(market.end_time_ms.saturating_sub(now_ms))
                {
                    for direction in [Direction::BuyMerge, Direction::SplitSell] {
                        self.gates
                            .relock(GateKey { direction, bucket }, "invalid_data");
                    }
                }
                self.log_decision(
                    market,
                    None,
                    false,
                    &format!("rejected:{error}"),
                    None,
                    None,
                )?;
            }
        }
        self.gates.save(&self.config.state.gate_state_path)?;
        let gates_unlocked = self
            .gates
            .summaries()
            .iter()
            .filter(|(_, status)| status.unlocked)
            .count();
        crate::metrics::set_cycle_gauges(
            markets.len(),
            subscribable_markets,
            self.market_info.len(),
            gates_unlocked,
        );
        tracing::info!(
            discovered_markets = markets.len(),
            metadata_cache_entries = self.market_info.len(),
            metadata_cache_new = self.market_info.len().saturating_sub(cache_entries_before),
            decisions_written = self.cycle_decisions_written,
            elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            "strategy-beta evaluation cycle complete"
        );
        Ok(())
    }

    async fn evaluate_market(&mut self, market: &BinaryCryptoMarket, now_ms: i64) -> Result<()> {
        let info = match self.market_info.get(&market.condition_id) {
            Some(info) => info.clone(),
            None => {
                let info = self.rest.market_info(&market.condition_id).await?;
                self.market_info
                    .insert(market.condition_id.clone(), info.clone());
                info
            }
        };
        let fees = validate_market_info(market, &info)?;
        let (ws_a, ws_b) = {
            let books = self.books.read().await;
            (
                books
                    .get(&market.outcomes[0].token_id)
                    .cloned()
                    .context("token A WebSocket book missing")?,
                books
                    .get(&market.outcomes[1].token_id)
                    .cloned()
                    .context("token B WebSocket book missing")?,
            )
        };
        let bucket = DurationBucket::from_remaining_ms(market.end_time_ms.saturating_sub(now_ms))
            .context("market duration is outside calibration buckets")?;
        let mut outside_price_band = true;
        let preliminary = [Direction::BuyMerge, Direction::SplitSell]
            .into_iter()
            .filter_map(|direction| {
                let reserves = effective_reserves(
                    &self.config.reserves,
                    self.gates.status(GateKey { direction, bucket }),
                );
                match best_for_direction(
                    &ws_a,
                    &ws_b,
                    &fees,
                    direction,
                    &self.config.profitability,
                    &reserves,
                    &self.config.risk,
                ) {
                    Ok(opportunity) => {
                        outside_price_band = false;
                        Some(opportunity)
                    }
                    Err(Rejection::OutsidePriceBand) => None,
                    Err(Rejection::NoProfitableDepth) => {
                        outside_price_band = false;
                        None
                    }
                }
            })
            .max_by(|left, right| left.net_profit.cmp(&right.net_profit));
        let Some(preliminary) = preliminary else {
            self.episodes.close_market(&market.market_id);
            let reason = if outside_price_band {
                "outside_price_band"
            } else {
                "no_profitable_depth"
            };
            self.log_decision(market, None, false, reason, None, None)?;
            return Ok(());
        };

        let info = self.rest.market_info(&market.condition_id).await?;
        let fees = validate_market_info(market, &info)?;
        self.market_info.insert(market.condition_id.clone(), info);
        let token_ids = market
            .outcomes
            .iter()
            .map(|outcome| outcome.token_id.clone())
            .collect::<Vec<_>>();
        let (rest_books, round_trip_ms) = self.rest.books(&token_ids, now_ms).await?;
        let rest_a = rest_books
            .iter()
            .find(|book| book.token_id == token_ids[0])
            .context("token A REST book missing")?;
        let rest_b = rest_books
            .iter()
            .find(|book| book.token_id == token_ids[1])
            .context("token B REST book missing")?;
        validate_final_books(
            &ws_a,
            &ws_b,
            rest_a,
            rest_b,
            preliminary.quantity,
            preliminary.direction,
            market.tick_size,
            now_ms,
            round_trip_ms,
            &self.config.quality,
        )?;
        let final_reserves = effective_reserves(
            &self.config.reserves,
            self.gates.status(GateKey {
                direction: preliminary.direction,
                bucket,
            }),
        );
        let final_opportunity = best_for_direction(
            rest_a,
            rest_b,
            &fees,
            preliminary.direction,
            &self.config.profitability,
            &final_reserves,
            &self.config.risk,
        )
        .ok()
        .context("opportunity disappeared in final REST refresh")?;

        self.finish_opportunity(
            market,
            now_ms,
            bucket,
            preliminary,
            final_opportunity,
            rest_a,
            rest_b,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_opportunity(
        &mut self,
        market: &BinaryCryptoMarket,
        now_ms: i64,
        bucket: DurationBucket,
        preliminary: Opportunity,
        final_opportunity: Opportunity,
        rest_a: &Book,
        rest_b: &Book,
    ) -> Result<()> {
        self.episodes
            .close_other_directions(&market.market_id, final_opportunity.direction);
        let episode = self
            .episodes
            .episode(&market.market_id, final_opportunity.direction);
        let key = GateKey {
            direction: final_opportunity.direction,
            bucket,
        };
        let sample = CalibrationSample {
            episode_id: episode,
            observed_at_ms: now_ms,
            terminal: true,
            valid_data: true,
            unresolved_orphan: false,
            net_profit: final_opportunity.net_profit,
            slippage: (preliminary.net_profit - final_opportunity.net_profit).max(Decimal::ZERO),
            latency_cost: self.config.reserves.latency,
            orphan_loss: Decimal::ZERO,
        };
        if self.gates.record(key, sample.clone()) {
            append_jsonl(&self.config.state.calibration_log_path, &sample)?;
            crate::metrics::record_candidate(final_opportunity.direction, bucket);
        }
        let gate = self
            .gates
            .evaluate(
                key,
                now_ms,
                self.config.calibration.minimum_age_hours,
                self.config.calibration.minimum_cycles,
                self.config.calibration.minimum_coverage,
            )
            .clone();
        if !gate.unlocked {
            self.log_decision(
                market,
                Some(&final_opportunity),
                false,
                "calibration_gate_locked",
                None,
                None,
            )?;
            return Ok(());
        }
        if self.observe_only {
            self.log_decision(
                market,
                Some(&final_opportunity),
                false,
                "forced_observe_only",
                None,
                None,
            )?;
            return Ok(());
        }
        if self.blocked_markets.contains(&market.market_id) {
            self.log_decision(
                market,
                Some(&final_opportunity),
                false,
                "market_close_recovery_blocked",
                None,
                None,
            )?;
            return Ok(());
        }
        self.check_risk(market, &final_opportunity)?;
        self.circuits.can_start(
            &market.market_id,
            final_opportunity.direction,
            now_ms,
            self.config.risk.signals_per_minute,
        )?;
        let signal_id = stable_signal_id(market, &final_opportunity, rest_a, rest_b);
        let outcome = self
            .submitter
            .submit(request(
                signal_id.clone(),
                &self.config.link.strategy_id,
                &self.config.link.source,
                market,
                &final_opportunity,
                rest_a.updated_at_ms,
                rest_b.updated_at_ms,
                now_ms,
                self.config.recovery.enabled,
                self.config.recovery.close_lead_ms,
                self.config.risk.max_orphan_loss,
            ))
            .await?;
        crate::metrics::record_submission(match &outcome {
            SubmitOutcome::Accepted => "accepted",
            SubmitOutcome::Duplicate => "duplicate",
            SubmitOutcome::Rejected(_) => "rejected",
            SubmitOutcome::Retriable(_) => "retriable",
        });
        match outcome {
            SubmitOutcome::Accepted | SubmitOutcome::Duplicate => {
                self.circuits.started(
                    market.market_id.clone(),
                    final_opportunity.direction,
                    now_ms,
                );
                self.submitted.insert(
                    signal_id,
                    (
                        market.market_id.clone(),
                        final_opportunity.direction,
                        final_opportunity.capital,
                        market.underlying.clone(),
                    ),
                );
                self.log_decision(
                    market,
                    Some(&final_opportunity),
                    true,
                    "submitted",
                    None,
                    None,
                )?;
            }
            SubmitOutcome::Rejected(reason) | SubmitOutcome::Retriable(reason) => {
                self.log_decision(market, Some(&final_opportunity), false, &reason, None, None)?;
            }
        }
        Ok(())
    }

    fn check_risk(&self, market: &BinaryCryptoMarket, opportunity: &Opportunity) -> Result<()> {
        if self.daily_loss_halted {
            bail!("daily loss circuit breaker is active");
        }
        let market_exposure: Decimal = self
            .submitted
            .values()
            .filter(|(market_id, _, _, _)| market_id == &market.market_id)
            .map(|(_, _, capital, _)| *capital)
            .sum();
        let underlying_exposure: Decimal = self
            .submitted
            .values()
            .filter(|(_, _, _, underlying)| underlying == &market.underlying)
            .map(|(_, _, capital, _)| *capital)
            .sum();
        let aggregate: Decimal = self
            .submitted
            .values()
            .map(|(_, _, capital, _)| *capital)
            .sum();
        if opportunity.capital > self.config.risk.max_per_cycle
            || market_exposure + opportunity.capital > self.config.risk.max_per_market
            || underlying_exposure + opportunity.capital > self.config.risk.max_per_underlying
            || aggregate + opportunity.capital > self.config.risk.max_aggregate
            || self.config.reserves.orphan > self.config.risk.max_orphan_loss
        {
            bail!("strategy risk limit rejected the cycle");
        }
        Ok(())
    }

    async fn reconcile_monitor(&mut self) -> Result<()> {
        let executions = self
            .monitor
            .executions(&self.config.link.strategy_id, 1_000)
            .await?;
        let mut blocked_markets = BTreeSet::new();
        let now_ms = now_millis();
        let day_start_ms = now_ms.saturating_sub(86_400_000);
        let mut daily_profit = Decimal::ZERO;
        for execution in executions {
            if execution.completed_at_ms >= day_start_ms
                && let Ok(realized) = execution.realized_profit.parse::<Decimal>()
            {
                daily_profit += realized;
            }
            if matches!(execution.state.as_str(), "recovering" | "unknown")
                || execution.recovery_action == "halted"
                || matches!(
                    execution.close_recovery_state.as_str(),
                    "running" | "unknown" | "held_to_resolution"
                )
            {
                blocked_markets.insert(execution.market_id.clone());
            }
            if matches!(
                execution.state.as_str(),
                "completed" | "failed" | "risk_rejected"
            ) && let Some((market_id, direction, _, _)) =
                self.submitted.remove(&execution.signal_id)
            {
                self.circuits.terminal(&market_id, direction);
            }
        }
        self.blocked_markets = blocked_markets;
        self.circuits.set_unresolved_orphan(false);
        self.daily_loss_halted = daily_profit <= -self.config.risk.daily_loss_limit;
        if self.daily_loss_halted {
            self.gates.relock_all("daily_loss_limit");
        }
        Ok(())
    }

    fn log_decision(
        &mut self,
        market: &BinaryCryptoMarket,
        opportunity: Option<&Opportunity>,
        submitted: bool,
        reason: &str,
        coverage_a: Option<bool>,
        coverage_b: Option<bool>,
    ) -> Result<()> {
        let observed_at_ms = now_millis();
        if opportunity.is_none()
            && !submitted
            && matches!(
                reason,
                "no_profitable_depth" | "outside_price_band" | "market_not_subscribable"
            )
            && !should_log_rejection(
                &mut self.rejection_heartbeats,
                &market.market_id,
                reason,
                observed_at_ms,
            )
        {
            return Ok(());
        }
        self.decisions.append(&Decision {
            observed_at_ms,
            market_id: market.market_id.clone(),
            condition_id: Some(market.condition_id.clone()),
            token_id_a: market
                .outcomes
                .first()
                .map(|outcome| outcome.token_id.clone()),
            token_id_b: market
                .outcomes
                .get(1)
                .map(|outcome| outcome.token_id.clone()),
            direction: opportunity.map(|value| value.direction),
            quantity: opportunity.map(|value| value.quantity),
            expected_profit: opportunity.map(|value| value.net_profit),
            gate_unlocked: opportunity
                .and_then(|value| {
                    DurationBucket::from_remaining_ms(
                        market.end_time_ms.saturating_sub(observed_at_ms),
                    )
                    .and_then(|bucket| {
                        self.gates.status(GateKey {
                            direction: value.direction,
                            bucket,
                        })
                    })
                })
                .is_some_and(|status| status.unlocked),
            submitted,
            reason: reason.to_owned(),
            store_coverage_a: coverage_a,
            store_coverage_b: coverage_b,
        })?;
        crate::metrics::record_decision(reason);
        self.cycle_decisions_written = self.cycle_decisions_written.saturating_add(1);
        Ok(())
    }
}

fn validate_market_info(
    market: &BinaryCryptoMarket,
    info: &MarketInfo,
) -> Result<crate::pricing::FeeSchedule> {
    let expected_tokens = market
        .outcomes
        .iter()
        .map(|outcome| outcome.token_id.clone())
        .collect::<BTreeSet<_>>();
    if info.token_ids.iter().cloned().collect::<BTreeSet<_>>() != expected_tokens {
        bail!("Gamma/CLOB token mapping mismatch");
    }
    if info.tick_size != market.tick_size {
        bail!("Gamma/CLOB tick size mismatch");
    }
    if !info.accepting_orders {
        bail!("CLOB market is not accepting orders");
    }
    validate_fee_metadata(
        market.gamma_fee_rate,
        market.gamma_fee_exponent,
        market.gamma_fee_taker_only,
        info.fee_rate,
        info.fee_exponent,
        info.fee_taker_only,
    )
}

fn should_log_rejection(
    heartbeats: &mut HashMap<String, (String, i64)>,
    market_id: &str,
    reason: &str,
    observed_at_ms: i64,
) -> bool {
    if heartbeats
        .get(market_id)
        .is_some_and(|(previous, logged_at_ms)| {
            previous == reason
                && observed_at_ms.saturating_sub(*logged_at_ms) < REJECTION_HEARTBEAT_MS
        })
    {
        return false;
    }
    heartbeats.insert(market_id.to_owned(), (reason.to_owned(), observed_at_ms));
    true
}

fn stable_signal_id(
    market: &BinaryCryptoMarket,
    opportunity: &Opportunity,
    book_a: &Book,
    book_b: &Book,
) -> String {
    let raw = format!(
        "{}:{}:{}:{}:{}",
        market.market_id,
        direction_label(opportunity.direction),
        opportunity.quantity,
        book_a.updated_at_ms,
        book_b.updated_at_ms
    );
    format!("beta:{}", hex::encode(Sha256::digest(raw.as_bytes())))
}

fn direction_label(direction: Direction) -> &'static str {
    match direction {
        Direction::BuyMerge => "buy_merge",
        Direction::SplitSell => "split_sell",
    }
}

fn effective_reserves(
    configured: &crate::config::ReserveConfig,
    gate: Option<&crate::gate::GateStatus>,
) -> crate::config::ReserveConfig {
    let Some(gate) = gate else {
        return configured.clone();
    };
    crate::config::ReserveConfig {
        slippage_bps: configured.slippage_bps,
        latency: configured.latency + gate.reserves.slippage_p99 + gate.reserves.latency_p99,
        orphan: configured.orphan.max(gate.reserves.orphan_p99),
        rounding_scale: configured.rounding_scale,
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gamma::OutcomeToken;

    #[test]
    fn repeated_rejections_log_on_change_or_heartbeat() {
        let mut heartbeats = HashMap::new();
        assert!(should_log_rejection(
            &mut heartbeats,
            "market",
            "no_profitable_depth",
            1_000
        ));
        assert!(!should_log_rejection(
            &mut heartbeats,
            "market",
            "no_profitable_depth",
            2_000
        ));
        assert!(should_log_rejection(
            &mut heartbeats,
            "market",
            "outside_price_band",
            3_000
        ));
        assert!(should_log_rejection(
            &mut heartbeats,
            "market",
            "outside_price_band",
            3_000 + REJECTION_HEARTBEAT_MS
        ));
    }

    #[test]
    fn market_info_validation_rejects_changed_token_mapping() {
        let market = BinaryCryptoMarket {
            market_id: "market".to_owned(),
            condition_id: "condition".to_owned(),
            question: "question".to_owned(),
            rules: "rules".to_owned(),
            underlying: "BTC".to_owned(),
            outcomes: [
                OutcomeToken {
                    label: "Yes".to_owned(),
                    token_id: "a".to_owned(),
                },
                OutcomeToken {
                    label: "No".to_owned(),
                    token_id: "b".to_owned(),
                },
            ],
            start_time_ms: None,
            end_time_ms: 10_000,
            tick_size: Decimal::new(1, 2),
            min_order_size: Decimal::ONE,
            neg_risk: false,
            gamma_fee_rate: Some(Decimal::new(7, 2)),
            gamma_fee_exponent: Some(1),
            gamma_fee_taker_only: Some(true),
            active: true,
            accepting_orders: true,
        };
        let info = MarketInfo {
            condition_id: "condition".to_owned(),
            token_ids: vec!["a".to_owned(), "different".to_owned()],
            fee_rate: Some(Decimal::new(7, 2)),
            fee_exponent: Some(1),
            fee_taker_only: Some(true),
            tick_size: Decimal::new(1, 2),
            accepting_orders: true,
        };

        assert!(
            validate_market_info(&market, &info)
                .unwrap_err()
                .to_string()
                .contains("token mapping mismatch")
        );
    }
}
