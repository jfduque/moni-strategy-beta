use crate::clients::Store;
use crate::config::RuntimeConfig;
use crate::decision_store::{CalibrationRow, DecisionStore};
use anyhow::{Context, Result};
use moni_proto::store::v1::BookSnapshot;
use std::collections::{BTreeSet, HashMap};

const DECISION_BATCH_SIZE: usize = 2_048;

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
    let store_path = &config.state.decision_db_path;
    let decisions =
        DecisionStore::open(store_path).with_context(|| format!("opening {store_path}"))?;
    let through_id = decisions
        .latest_calibration_id()
        .with_context(|| format!("reading {store_path}"))?;
    let mut store = Store::connect(config.store.endpoint.clone()).await?;
    let mut accumulator = Accumulator::default();
    let mut after_id = 0;
    while after_id < through_id {
        let batch = decisions
            .calibration_batch(after_id, through_id, DECISION_BATCH_SIZE)
            .with_context(|| format!("reading {store_path}"))?;
        let Some((last_id, _)) = batch.last() else {
            break;
        };
        after_id = *last_id;
        let rows = batch.iter().map(|(_, row)| row).collect::<Vec<_>>();
        let token_ids = rows
            .iter()
            .flat_map(|row| [&row.token_id_a, &row.token_id_b])
            .filter_map(|token| token.as_ref().cloned())
            .collect::<BTreeSet<_>>();
        let from_ms = rows
            .iter()
            .map(|row| {
                row.observed_at_ms
                    .saturating_sub(config.store.snapshot_max_age_ms as i64)
            })
            .min()
            .unwrap_or_default();
        let to_ms = rows
            .iter()
            .map(|row| row.observed_at_ms)
            .max()
            .unwrap_or(from_ms);
        let mut snapshots = HashMap::<String, Vec<BookSnapshot>>::new();
        if !token_ids.is_empty() {
            for snapshot in store
                .snapshots(token_ids.into_iter().collect(), from_ms, to_ms)
                .await?
            {
                snapshots
                    .entry(snapshot.token_id.clone())
                    .or_default()
                    .push(snapshot);
            }
        }
        for row in rows {
            accumulator.observe(row, &snapshots, config.store.snapshot_max_age_ms);
        }
    }
    Ok(accumulator.finish())
}

#[derive(Default)]
struct Accumulator {
    summary: Summary,
    ages: Vec<f64>,
    spreads: Vec<f64>,
}

impl Accumulator {
    fn observe(
        &mut self,
        row: &CalibrationRow,
        snapshots: &HashMap<String, Vec<BookSnapshot>>,
        max_age_ms: u64,
    ) {
        self.summary.decisions = self.summary.decisions.saturating_add(1);
        let (Some(token_a), Some(token_b)) = (&row.token_id_a, &row.token_id_b) else {
            self.summary.legacy_decisions = self.summary.legacy_decisions.saturating_add(1);
            return;
        };
        self.summary.eligible_decisions = self.summary.eligible_decisions.saturating_add(1);
        let a = preceding_snapshot(snapshots.get(token_a), row.observed_at_ms, max_age_ms);
        let b = preceding_snapshot(snapshots.get(token_b), row.observed_at_ms, max_age_ms);
        match (a.is_some(), b.is_some()) {
            (true, true) => {
                self.summary.both_legs_covered = self.summary.both_legs_covered.saturating_add(1)
            }
            (true, false) | (false, true) => {
                self.summary.one_leg_covered = self.summary.one_leg_covered.saturating_add(1)
            }
            (false, false) => {
                self.summary.no_legs_covered = self.summary.no_legs_covered.saturating_add(1)
            }
        }
        for snapshot in [a, b].into_iter().flatten() {
            self.ages
                .push((row.observed_at_ms - snapshot.sampled_at_ms) as f64);
            if let Some(spread) = spread(snapshot) {
                self.spreads.push(spread);
            }
        }
    }

    fn finish(mut self) -> Summary {
        self.summary.median_snapshot_age_ms = median(&mut self.ages);
        self.summary.median_spread = median(&mut self.spreads);
        self.summary
    }
}

#[cfg(test)]
pub(crate) fn summarize(
    rows: &[CalibrationRow],
    snapshots: &HashMap<String, Vec<BookSnapshot>>,
    max_age_ms: u64,
) -> Summary {
    let mut accumulator = Accumulator::default();
    for row in rows {
        accumulator.observe(row, snapshots, max_age_ms);
    }
    accumulator.finish()
}

fn preceding_snapshot(
    snapshots: Option<&Vec<BookSnapshot>>,
    observed_at_ms: i64,
    max_age_ms: u64,
) -> Option<&BookSnapshot> {
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

    fn decision(observed_at_ms: i64) -> CalibrationRow {
        CalibrationRow {
            observed_at_ms,
            condition_id: Some("condition".to_owned()),
            token_id_a: Some("a".to_owned()),
            token_id_b: Some("b".to_owned()),
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
