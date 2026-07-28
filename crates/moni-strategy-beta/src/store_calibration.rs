use crate::clients::Store;
use crate::config::RuntimeConfig;
use crate::service::Decision;
use anyhow::{Context, Result};
use moni_proto::store::v1::BookSnapshot;
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Summary {
    pub decisions: u64,
    pub eligible_decisions: u64,
    pub legacy_decisions: u64,
    pub both_legs_covered: u64,
    pub one_leg_covered: u64,
    pub no_legs_covered: u64,
    pub median_snapshot_age_ms: Option<f64>,
    pub median_spread: Option<f64>,
}

pub async fn summarize_config(config: &RuntimeConfig) -> Result<Summary> {
    let raw = std::fs::read_to_string(&config.state.decision_log_path)
        .with_context(|| format!("reading {}", config.state.decision_log_path))?;
    let decisions = raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str::<Decision>(line)
                .with_context(|| format!("parsing decision line {}", index + 1))
        })
        .collect::<Result<Vec<_>>>()?;
    let eligible = decisions
        .iter()
        .filter_map(|decision| {
            Some((
                decision.observed_at_ms,
                decision.token_id_a.as_ref()?,
                decision.token_id_b.as_ref()?,
            ))
        })
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return Ok(summarize(
            &decisions,
            &HashMap::new(),
            config.store.snapshot_max_age_ms,
        ));
    }

    let from_ms = eligible
        .iter()
        .map(|(observed_at, _, _)| {
            observed_at.saturating_sub(config.store.snapshot_max_age_ms as i64)
        })
        .min()
        .unwrap_or_default();
    let to_ms = eligible
        .iter()
        .map(|(observed_at, _, _)| *observed_at)
        .max()
        .unwrap_or(from_ms);
    let token_ids = eligible
        .iter()
        .flat_map(|(_, token_a, token_b)| [(*token_a).clone(), (*token_b).clone()])
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut store = Store::connect(config.store.endpoint.clone()).await?;
    let rows = store.snapshots(token_ids, from_ms, to_ms).await?;
    let mut snapshots = HashMap::<String, Vec<BookSnapshot>>::new();
    for row in rows {
        snapshots.entry(row.token_id.clone()).or_default().push(row);
    }
    Ok(summarize(
        &decisions,
        &snapshots,
        config.store.snapshot_max_age_ms,
    ))
}

pub(crate) fn summarize(
    decisions: &[Decision],
    snapshots: &HashMap<String, Vec<BookSnapshot>>,
    max_age_ms: u64,
) -> Summary {
    let mut summary = Summary {
        decisions: decisions.len() as u64,
        ..Summary::default()
    };
    let mut ages = Vec::new();
    let mut spreads = Vec::new();
    for decision in decisions {
        let (Some(token_a), Some(token_b)) = (&decision.token_id_a, &decision.token_id_b) else {
            summary.legacy_decisions = summary.legacy_decisions.saturating_add(1);
            continue;
        };
        summary.eligible_decisions = summary.eligible_decisions.saturating_add(1);
        let a = preceding_snapshot(snapshots.get(token_a), decision.observed_at_ms, max_age_ms);
        let b = preceding_snapshot(snapshots.get(token_b), decision.observed_at_ms, max_age_ms);
        match (a.is_some(), b.is_some()) {
            (true, true) => summary.both_legs_covered = summary.both_legs_covered.saturating_add(1),
            (true, false) | (false, true) => {
                summary.one_leg_covered = summary.one_leg_covered.saturating_add(1)
            }
            (false, false) => summary.no_legs_covered = summary.no_legs_covered.saturating_add(1),
        }
        for snapshot in [a, b].into_iter().flatten() {
            ages.push((decision.observed_at_ms - snapshot.sampled_at_ms) as f64);
            if let Some(spread) = spread(snapshot) {
                spreads.push(spread);
            }
        }
    }
    summary.median_snapshot_age_ms = median(&mut ages);
    summary.median_spread = median(&mut spreads);
    summary
}

fn preceding_snapshot<'a>(
    snapshots: Option<&'a Vec<BookSnapshot>>,
    observed_at_ms: i64,
    max_age_ms: u64,
) -> Option<&'a BookSnapshot> {
    snapshots?
        .iter()
        .filter(|snapshot| {
            snapshot.sampled_at_ms <= observed_at_ms
                && observed_at_ms.saturating_sub(snapshot.sampled_at_ms) <= max_age_ms as i64
        })
        .max_by_key(|snapshot| snapshot.sampled_at_ms)
}

fn spread(snapshot: &BookSnapshot) -> Option<f64> {
    let bid = snapshot
        .bids
        .iter()
        .filter_map(|level| level.price.parse::<f64>().ok())
        .max_by(f64::total_cmp)?;
    let ask = snapshot
        .asks
        .iter()
        .filter_map(|level| level.price.parse::<f64>().ok())
        .min_by(f64::total_cmp)?;
    Some(ask - bid)
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    Some(values[values.len() / 2])
}

#[cfg(test)]
mod tests {
    use super::*;
    use moni_proto::store::v1::BookLevel;

    fn decision(observed_at_ms: i64) -> Decision {
        Decision {
            observed_at_ms,
            market_id: "market".to_owned(),
            condition_id: Some("condition".to_owned()),
            token_id_a: Some("a".to_owned()),
            token_id_b: Some("b".to_owned()),
            direction: None,
            quantity: None,
            expected_profit: None,
            gate_unlocked: false,
            submitted: false,
            reason: "fixture".to_owned(),
            store_coverage_a: None,
            store_coverage_b: None,
        }
    }

    fn snapshot(token_id: &str, sampled_at_ms: i64) -> BookSnapshot {
        BookSnapshot {
            sampled_at_ms,
            market_id: "condition".to_owned(),
            token_id: token_id.to_owned(),
            book_updated_at_ms: sampled_at_ms,
            bids: vec![BookLevel {
                price: "0.40".to_owned(),
                size: "10".to_owned(),
            }],
            asks: vec![BookLevel {
                price: "0.45".to_owned(),
                size: "10".to_owned(),
            }],
        }
    }

    #[test]
    fn joins_both_legs_to_the_nearest_preceding_snapshot() {
        let snapshots = HashMap::from([
            ("a".to_owned(), vec![snapshot("a", 900)]),
            ("b".to_owned(), vec![snapshot("b", 950)]),
        ]);
        let summary = summarize(&[decision(1_000)], &snapshots, 100);
        assert_eq!(summary.both_legs_covered, 1);
        assert_eq!(summary.median_snapshot_age_ms, Some(100.0));
        assert!((summary.median_spread.unwrap() - 0.05).abs() < 1e-9);
    }
}
