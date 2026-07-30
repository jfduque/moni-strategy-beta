use crate::config::MetricsConfig;
use crate::gate::DurationBucket;
use crate::pricing::Direction;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::fmt;
use std::time::Duration;

#[derive(Debug)]
pub enum MetricsError {
    InvalidBindAddress { value: String },
    Build(metrics_exporter_prometheus::BuildError),
}

impl fmt::Display for MetricsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBindAddress { value } => {
                write!(
                    formatter,
                    "`metrics.bind` must be a socket address, got `{value}`"
                )
            }
            Self::Build(error) => write!(formatter, "failed to start metrics listener: {error}"),
        }
    }
}

impl std::error::Error for MetricsError {}

pub fn install(config: &MetricsConfig) -> Result<(), MetricsError> {
    let address: std::net::SocketAddr =
        config
            .bind
            .parse()
            .map_err(|_| MetricsError::InvalidBindAddress {
                value: config.bind.clone(),
            })?;
    PrometheusBuilder::new()
        .with_http_listener(address)
        .install()
        .map_err(MetricsError::Build)
}

pub(crate) fn record_cycle(status: &'static str, duration: Duration) {
    metrics::counter!("moni_strategy_beta_evaluation_cycles_total", "status" => status)
        .increment(1);
    metrics::histogram!("moni_strategy_beta_cycle_duration_seconds").record(duration.as_secs_f64());
}

pub(crate) fn set_cycle_gauges(
    discovered: usize,
    subscribable: usize,
    cache_entries: usize,
    gates_unlocked: usize,
) {
    metrics::gauge!("moni_strategy_beta_markets_discovered").set(discovered as f64);
    metrics::gauge!("moni_strategy_beta_markets_subscribable").set(subscribable as f64);
    metrics::gauge!("moni_strategy_beta_metadata_cache_entries").set(cache_entries as f64);
    metrics::gauge!("moni_strategy_beta_gates_unlocked").set(gates_unlocked as f64);
    metrics::gauge!("moni_strategy_beta_last_successful_cycle_timestamp_seconds")
        .set(now_millis() as f64 / 1_000.0);
}

pub(crate) fn record_decision(reason: &str) {
    metrics::counter!(
        "moni_strategy_beta_decisions_total",
        "reason" => decision_reason(reason)
    )
    .increment(1);
}

pub(crate) fn record_candidate(direction: Direction, bucket: DurationBucket) {
    metrics::counter!(
        "moni_strategy_beta_candidate_episodes_total",
        "direction" => direction_label(direction),
        "duration" => duration_label(bucket)
    )
    .increment(1);
}

pub(crate) fn record_submission(outcome: &'static str) {
    metrics::counter!("moni_strategy_beta_submissions_total", "outcome" => outcome).increment(1);
}

fn direction_label(direction: Direction) -> &'static str {
    match direction {
        Direction::BuyMerge => "buy_merge",
        Direction::SplitSell => "split_sell",
    }
}

fn duration_label(bucket: DurationBucket) -> &'static str {
    match bucket {
        DurationBucket::Under15m => "under_15m",
        DurationBucket::From15mTo1h => "15m_to_1h",
        DurationBucket::From1hTo6h => "1h_to_6h",
        DurationBucket::From6hTo24h => "6h_to_24h",
    }
}

fn decision_reason(reason: &str) -> &'static str {
    match reason {
        "outside_price_band" => "outside_price_band",
        "no_profitable_depth" => "no_profitable_depth",
        "market_not_subscribable" => "market_not_subscribable",
        "calibration_gate_locked" => "calibration_gate_locked",
        "forced_observe_only" => "forced_observe_only",
        "submitted" => "submitted",
        value if value.starts_with("rejected:") => "invalid_data",
        _ => "engine_or_risk_rejected",
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

    #[test]
    fn decision_reasons_have_bounded_categories() {
        assert_eq!(
            decision_reason("rejected:Gamma/CLOB tick size mismatch"),
            "invalid_data"
        );
        assert_eq!(
            decision_reason("Unavailable: engine offline"),
            "engine_or_risk_rejected"
        );
        assert_eq!(decision_reason("outside_price_band"), "outside_price_band");
    }
}
