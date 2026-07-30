pub mod clients;
pub mod clob;
pub mod config;
pub mod decision_store;
pub mod gamma;
pub mod gate;
pub mod link;
pub mod metrics;
pub mod pricing;
pub mod service;
pub mod store_calibration;

use anyhow::{Context, Result, bail};
use config::{DEFAULT_CONFIG_PATH, RuntimeConfig};
use gamma::GammaClient;
use gate::GateState;
use std::path::PathBuf;

pub async fn run_cli<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    let command = args.get(1).map(String::as_str).unwrap_or("help");
    let config_path = argument_value(&args, "--config")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    match command {
        "serve" => {
            let config = RuntimeConfig::load(&config_path)?;
            let observe_only = args.iter().any(|argument| argument == "--dry-run");
            metrics::install(&config.metrics)?;
            tracing::info!(
                version = env!("CARGO_PKG_VERSION"),
                observe_only,
                "starting strategy-beta"
            );
            service::StrategyService::connect(config, observe_only)
                .await?
                .run()
                .await
        }
        "--discover-only" | "discover-only" => {
            let config = RuntimeConfig::load(&config_path)?;
            let now_ms = now_millis();
            let markets = GammaClient::new(
                config.discovery.gamma_endpoint,
                config.discovery.page_limit,
                config.discovery.max_pages,
            )
            .discover(now_ms, config.discovery.max_horizon_ms)
            .await?;
            let subscribed = markets
                .iter()
                .filter(|market| market.subscribable(now_ms, config.discovery.max_horizon_ms))
                .count();
            println!(
                "discovered={} subscribable={} tokens={}",
                markets.len(),
                subscribed,
                subscribed * 2
            );
            for market in markets {
                println!(
                    "{} {} start={:?} end={} subscribable={}",
                    market.market_id,
                    market.question,
                    market.start_time_ms,
                    market.end_time_ms,
                    market.subscribable(now_ms, config.discovery.max_horizon_ms)
                );
            }
            Ok(())
        }
        "calibration-summary" => {
            let config = RuntimeConfig::load(&config_path)?;
            let state = GateState::load(&config.state.gate_state_path)?;
            for (key, status) in state.summaries() {
                println!(
                    "{:?}/{:?}: unlocked={} cycles={} coverage={} aggregate={} median={} relock={}",
                    key.direction,
                    key.bucket,
                    status.unlocked,
                    status.independent_cycles,
                    status.valid_coverage,
                    status.aggregate_profit,
                    status.median_profit,
                    status.relock_reason.as_deref().unwrap_or("none")
                );
            }
            Ok(())
        }
        "execution-summary" => {
            let config = RuntimeConfig::load(&config_path)?;
            let api_key = std::env::var(&config.monitor.api_key_env).with_context(|| {
                format!(
                    "Monitor key environment variable {} is not set",
                    config.monitor.api_key_env
                )
            })?;
            let mut monitor = clients::Monitor::connect(
                config.monitor.endpoint,
                &api_key,
                config.monitor.tenant_id,
            )
            .await?;
            let executions = monitor.executions(&config.link.strategy_id, 1_000).await?;
            let terminal = executions
                .iter()
                .filter(|execution| {
                    matches!(
                        execution.state.as_str(),
                        "completed" | "failed" | "risk_rejected"
                    )
                })
                .count();
            println!(
                "executions={} terminal={} nonterminal={}",
                executions.len(),
                terminal,
                executions.len().saturating_sub(terminal)
            );
            Ok(())
        }
        "store-calibration-summary" => {
            let config = RuntimeConfig::load(&config_path)?;
            let summary = store_calibration::summarize_config(&config).await?;
            println!(
                "decisions={} eligible={} legacy={} both_legs={} one_leg={} no_legs={} median_snapshot_age_ms={} median_spread={}",
                summary.decisions,
                summary.eligible_decisions,
                summary.legacy_decisions,
                summary.both_legs_covered,
                summary.one_leg_covered,
                summary.no_legs_covered,
                summary
                    .median_snapshot_age_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
                summary
                    .median_spread
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
            );
            Ok(())
        }
        "manual-signal" => {
            let config = RuntimeConfig::load(&config_path)?;
            let market = args.get(2).context("manual-signal requires MARKET_ID")?;
            if argument_value(&args, "--confirm") != Some("MANUAL_DIAGNOSTIC_ONLY") {
                bail!(
                    "manual diagnostic requires --confirm MANUAL_DIAGNOSTIC_ONLY and still enforces every data, risk, and maturity gate"
                );
            }
            service::StrategyService::connect(config, false)
                .await?
                .diagnostic_once(market)
                .await
        }
        _ => {
            println!(
                "usage: moni-strategy-beta <serve|--discover-only|calibration-summary|store-calibration-summary|execution-summary|manual-signal MARKET_ID> [--config PATH] [--dry-run] [--confirm MANUAL_DIAGNOSTIC_ONLY]"
            );
            Ok(())
        }
    }
}

fn argument_value<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|window| window[0] == name)
        .map(|window| window[1].as_str())
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
