use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::fs;
use std::path::Path;

pub const DEFAULT_CONFIG_PATH: &str = "/etc/moni-strategy-beta/config.toml";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    pub link: LinkConfig,
    pub monitor: MonitorConfig,
    pub store: StoreConfig,
    pub discovery: DiscoveryConfig,
    pub clob: ClobConfig,
    pub quality: QualityConfig,
    pub profitability: ProfitabilityConfig,
    pub reserves: ReserveConfig,
    pub risk: RiskConfig,
    pub calibration: CalibrationConfig,
    pub state: StateConfig,
}

impl RuntimeConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("reading configuration {}", path.display()))?;
        Self::from_toml_str(&raw)
    }

    pub fn from_toml_str(raw: &str) -> Result<Self> {
        let config: Self = toml::from_str(raw).context("parsing strategy-beta configuration")?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.link.endpoint.trim().is_empty()
            || self.link.strategy_id.trim().is_empty()
            || self.link.source.trim().is_empty()
        {
            bail!("link.endpoint, link.strategy_id, and link.source are required");
        }
        if !valid_strategy_id(&self.link.strategy_id) {
            bail!("link.strategy_id must match [a-z0-9_-]{{1,64}}");
        }
        if self.monitor.endpoint.trim().is_empty()
            || self.monitor.tenant_id.trim().is_empty()
            || self.monitor.api_key_env.trim().is_empty()
        {
            bail!("monitor.endpoint, tenant_id, and api_key_env are required");
        }
        if self.discovery.gamma_endpoint.trim().is_empty()
            || self.discovery.page_limit == 0
            || self.discovery.max_pages == 0
            || self.discovery.max_horizon_ms == 0
            || self.discovery.refresh_interval_secs == 0
        {
            bail!("discovery endpoints and positive pagination/horizon settings are required");
        }
        if self.discovery.page_limit > 100 {
            bail!("discovery.page_limit cannot exceed 100");
        }
        if self.clob.ws_endpoint.trim().is_empty()
            || self.clob.rest_endpoint.trim().is_empty()
            || self.clob.market_info_endpoint.trim().is_empty()
        {
            bail!("all CLOB endpoints are required");
        }
        if self.clob.max_assets_per_connection == 0
            || self.clob.max_total_assets < self.clob.max_assets_per_connection
        {
            bail!(
                "clob shard limits must be positive and max_total_assets must be at least max_assets_per_connection"
            );
        }
        if self.quality.max_book_age_ms == 0
            || self.quality.max_token_skew_ms == 0
            || self.quality.rest_timeout_ms == 0
            || self.quality.max_rest_round_trip_ms == 0
        {
            bail!("quality time limits must be positive");
        }
        if self.profitability.minimum_profit <= Decimal::ZERO
            || self.profitability.minimum_return_bps <= Decimal::ZERO
        {
            bail!("profitability thresholds must be positive");
        }
        if self.profitability.depth_fraction <= Decimal::ZERO
            || self.profitability.depth_fraction > Decimal::ONE
        {
            bail!("profitability.depth_fraction must be in (0, 1]");
        }
        if self.profitability.price_band_low_max <= Decimal::ZERO
            || self.profitability.price_band_high_min >= Decimal::ONE
            || self.profitability.price_band_low_max >= self.profitability.price_band_high_min
        {
            bail!("profitability.price_band_low_max must be < price_band_high_min, within (0, 1)");
        }
        if self.risk.max_per_cycle <= Decimal::ZERO
            || self.risk.max_per_market <= Decimal::ZERO
            || self.risk.max_per_underlying <= Decimal::ZERO
            || self.risk.max_aggregate <= Decimal::ZERO
            || self.risk.max_orphan_loss <= Decimal::ZERO
            || self.risk.max_unmatched_inventory <= Decimal::ZERO
            || self.risk.daily_loss_limit <= Decimal::ZERO
        {
            bail!("risk limits must be positive");
        }
        if self.risk.signals_per_minute == 0 {
            bail!("risk.signals_per_minute must be positive");
        }
        if self.calibration.minimum_age_hours < 24
            || self.calibration.minimum_cycles < 100
            || self.calibration.minimum_coverage < Decimal::ZERO
            || self.calibration.minimum_coverage > Decimal::ONE
        {
            bail!("calibration requires >=24h, >=100 cycles, and coverage in [0,1]");
        }
        if self.state.decision_db_path.trim().is_empty()
            || self.state.calibration_log_path.trim().is_empty()
            || self.state.gate_state_path.trim().is_empty()
        {
            bail!("all state paths are required");
        }
        Ok(())
    }
}

fn valid_strategy_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkConfig {
    pub endpoint: String,
    pub strategy_id: String,
    pub source: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorConfig {
    pub endpoint: String,
    pub tenant_id: String,
    pub api_key_env: String,
    pub poll_interval_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreConfig {
    pub endpoint: String,
    pub snapshot_max_age_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryConfig {
    pub gamma_endpoint: String,
    pub page_limit: usize,
    pub max_pages: usize,
    pub max_horizon_ms: u64,
    pub refresh_interval_secs: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClobConfig {
    pub ws_endpoint: String,
    pub rest_endpoint: String,
    pub market_info_endpoint: String,
    #[serde(default = "default_max_assets_per_connection")]
    pub max_assets_per_connection: usize,
    #[serde(default = "default_max_total_assets")]
    pub max_total_assets: usize,
}

const fn default_max_assets_per_connection() -> usize {
    200
}

const fn default_max_total_assets() -> usize {
    10_000
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualityConfig {
    pub max_book_age_ms: u64,
    pub max_token_skew_ms: u64,
    pub max_tick_disagreement: u32,
    pub rest_timeout_ms: u64,
    pub max_rest_round_trip_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfitabilityConfig {
    pub minimum_profit: Decimal,
    pub minimum_return_bps: Decimal,
    pub depth_fraction: Decimal,
    #[serde(default = "default_price_band_low_max")]
    pub price_band_low_max: Decimal,
    #[serde(default = "default_price_band_high_min")]
    pub price_band_high_min: Decimal,
}

fn default_price_band_low_max() -> Decimal {
    Decimal::new(15, 2)
}

fn default_price_band_high_min() -> Decimal {
    Decimal::new(85, 2)
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveConfig {
    pub slippage_bps: Decimal,
    pub latency: Decimal,
    pub orphan: Decimal,
    pub rounding_scale: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskConfig {
    pub max_per_cycle: Decimal,
    pub max_per_market: Decimal,
    pub max_per_underlying: Decimal,
    pub max_aggregate: Decimal,
    pub max_orphan_loss: Decimal,
    pub max_unmatched_inventory: Decimal,
    pub daily_loss_limit: Decimal,
    pub signals_per_minute: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibrationConfig {
    pub minimum_age_hours: u64,
    pub minimum_cycles: usize,
    pub minimum_coverage: Decimal,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    pub decision_db_path: String,
    pub calibration_log_path: String,
    pub gate_state_path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[link]
endpoint = "http://127.0.0.1:50053"
strategy_id = "strategy-beta"
source = "moni-strategy-beta"
[monitor]
endpoint = "http://127.0.0.1:50051"
tenant_id = "tenant-strategy-beta"
api_key_env = "MONI_BETA_MONITOR_KEY"
poll_interval_ms = 1000
[store]
endpoint = "http://127.0.0.1:50054"
snapshot_max_age_ms = 60000
[discovery]
gamma_endpoint = "https://gamma-api.polymarket.com/events"
page_limit = 100
max_pages = 30
max_horizon_ms = 86400000
refresh_interval_secs = 30
[clob]
ws_endpoint = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
rest_endpoint = "https://clob.polymarket.com/books"
market_info_endpoint = "https://clob.polymarket.com"
max_assets_per_connection = 500
max_total_assets = 10000
[quality]
max_book_age_ms = 750
max_token_skew_ms = 250
max_tick_disagreement = 1
rest_timeout_ms = 500
max_rest_round_trip_ms = 500
[profitability]
minimum_profit = "0.10"
minimum_return_bps = "200"
depth_fraction = "0.25"
price_band_low_max = "0.15"
price_band_high_min = "0.85"
[reserves]
slippage_bps = "10"
latency = "0.01"
orphan = "0.02"
rounding_scale = 6
[risk]
max_per_cycle = "5"
max_per_market = "8"
max_per_underlying = "10"
max_aggregate = "20"
max_orphan_loss = "0.50"
max_unmatched_inventory = "5"
daily_loss_limit = "10"
signals_per_minute = 10
[calibration]
minimum_age_hours = 24
minimum_cycles = 100
minimum_coverage = "0.99"
[state]
decision_db_path = "/var/lib/moni-strategy-beta/decisions.sqlite3"
calibration_log_path = "/var/lib/moni-strategy-beta/calibration.jsonl"
gate_state_path = "/var/lib/moni-strategy-beta/gates.json"
"#;

    #[test]
    fn valid_config_parses() {
        let config = RuntimeConfig::from_toml_str(VALID).unwrap();
        assert_eq!(config.discovery.max_pages, 30);
        assert_eq!(config.risk.max_per_cycle, Decimal::from(5));
    }

    #[test]
    fn price_band_defaults_for_existing_configs() {
        let raw = VALID
            .replace("price_band_low_max = \"0.15\"\n", "")
            .replace("price_band_high_min = \"0.85\"\n", "");
        let config = RuntimeConfig::from_toml_str(&raw).unwrap();

        assert_eq!(config.profitability.price_band_low_max, Decimal::new(15, 2));
        assert_eq!(
            config.profitability.price_band_high_min,
            Decimal::new(85, 2)
        );
    }

    #[test]
    fn rejects_inverted_price_band() {
        let raw = VALID.replace(
            "price_band_low_max = \"0.15\"",
            "price_band_low_max = \"0.90\"",
        );
        assert!(RuntimeConfig::from_toml_str(&raw).is_err());
    }

    #[test]
    fn clob_shard_limits_default_for_existing_configs() {
        let raw = VALID
            .replace("max_assets_per_connection = 500\n", "")
            .replace("max_total_assets = 10000\n", "");
        let config = RuntimeConfig::from_toml_str(&raw).unwrap();

        assert_eq!(config.clob.max_assets_per_connection, 200);
        assert_eq!(config.clob.max_total_assets, 10_000);
    }

    #[test]
    fn rejects_subscription_baseline_drift() {
        let raw = VALID.replace("max_pages = 30", "max_pages = 0");
        assert!(RuntimeConfig::from_toml_str(&raw).is_err());
    }

    #[test]
    fn rejects_empty_decision_database_path() {
        let raw = VALID.replace("/var/lib/moni-strategy-beta/decisions.sqlite3", "");
        assert!(RuntimeConfig::from_toml_str(&raw).is_err());
    }

    #[test]
    fn rejects_removed_decision_log_key() {
        let raw = VALID.replace("decision_db_path", "decision_log_path");
        assert!(RuntimeConfig::from_toml_str(&raw).is_err());
    }
}
