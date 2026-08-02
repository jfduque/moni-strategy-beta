use crate::clients::Store;
use crate::config::{ProfitabilityConfig, ReserveConfig, RuntimeConfig};
use crate::decision_store::DecisionStore;
use crate::pricing::{Book, Direction, FeeSchedule, Level, Opportunity, select_best};
use anyhow::{Context, Result};
use moni_proto::store::v1::BookSnapshot;
use rust_decimal::Decimal;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::str::FromStr;

const BATCH_SIZE: usize = 2_048;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Funnel {
    pub paired_observations: u64,
    pub frictionless: u64,
    pub fee_positive: u64,
    pub reserve_positive: u64,
    pub configured: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slice {
    pub stable_episodes: u64,
    pub distinct_markets: u64,
    pub total_profit: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub price_band: String,
    pub minimum_profit: Decimal,
    pub minimum_return_bps: Decimal,
    pub depth_fraction: Decimal,
    pub train: Slice,
    pub validation: Slice,
    pub holdout: Slice,
    pub holdout_pass: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub eligible_decisions: u64,
    pub both_legs_covered: u64,
    pub coverage_bps: u64,
    pub assumed_taker_fee_rate: Decimal,
    pub funnel: Funnel,
    pub selected: Option<Candidate>,
}

#[derive(Clone)]
struct Observation {
    condition_id: String,
    observed_at_ms: i64,
    book_a: Book,
    book_b: Book,
}

#[derive(Clone)]
struct Hit {
    condition_id: String,
    observed_at_ms: i64,
    direction: Direction,
    profit: Decimal,
}

pub async fn summarize_config(config: &RuntimeConfig) -> Result<Report> {
    let decisions = DecisionStore::open(&config.state.decision_db_path)
        .with_context(|| format!("opening {}", config.state.decision_db_path))?;
    let through_id = decisions.latest_calibration_id()?;
    let mut store = Store::connect(config.store.endpoint.clone()).await?;
    let mut after_id = 0;
    let mut eligible_decisions = 0u64;
    let mut both_legs_covered = 0u64;
    let mut seen = HashSet::new();
    let mut observations = Vec::new();

    while after_id < through_id {
        let batch = decisions.calibration_batch(after_id, through_id, BATCH_SIZE)?;
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
        if token_ids.is_empty() {
            continue;
        }
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
        for snapshot in store
            .snapshots(token_ids.into_iter().collect(), from_ms, to_ms)
            .await?
        {
            snapshots
                .entry(snapshot.token_id.clone())
                .or_default()
                .push(snapshot);
        }
        for row in rows {
            let (Some(condition_id), Some(token_a), Some(token_b)) =
                (&row.condition_id, &row.token_id_a, &row.token_id_b)
            else {
                continue;
            };
            eligible_decisions = eligible_decisions.saturating_add(1);
            let Some(a) = preceding(
                snapshots.get(token_a),
                row.observed_at_ms,
                config.store.snapshot_max_age_ms,
            ) else {
                continue;
            };
            let Some(b) = preceding(
                snapshots.get(token_b),
                row.observed_at_ms,
                config.store.snapshot_max_age_ms,
            ) else {
                continue;
            };
            both_legs_covered = both_legs_covered.saturating_add(1);
            let sample_key = (condition_id.clone(), a.sampled_at_ms, b.sampled_at_ms);
            if !seen.insert(sample_key) {
                continue;
            }
            let (Some(book_a), Some(book_b)) = (to_book(a), to_book(b)) else {
                continue;
            };
            if book_a.market_id != condition_id.as_str()
                || book_b.market_id != condition_id.as_str()
                || book_a.updated_at_ms.abs_diff(book_b.updated_at_ms)
                    > config.quality.max_token_skew_ms
            {
                continue;
            }
            observations.push(Observation {
                condition_id: condition_id.clone(),
                observed_at_ms: row.observed_at_ms,
                book_a,
                book_b,
            });
        }
    }

    observations.sort_by_key(|observation| observation.observed_at_ms);
    let coverage_bps = if eligible_decisions == 0 {
        0
    } else {
        both_legs_covered * 10_000 / eligible_decisions
    };
    let fee = FeeSchedule {
        rate: config.store.assumed_taker_fee_rate,
    };
    let funnel = funnel(&observations, config, &fee);
    let selected = select_candidate(&observations, config, &fee, coverage_bps);
    Ok(Report {
        eligible_decisions,
        both_legs_covered,
        coverage_bps,
        assumed_taker_fee_rate: fee.rate,
        funnel,
        selected,
    })
}

fn preceding(
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

fn to_book(snapshot: &BookSnapshot) -> Option<Book> {
    let levels = |input: &[moni_proto::store::v1::BookLevel]| {
        input
            .iter()
            .map(|level| {
                Some(Level {
                    price: Decimal::from_str(&level.price).ok()?,
                    size: Decimal::from_str(&level.size).ok()?,
                })
            })
            .collect::<Option<Vec<_>>>()
    };
    let mut book = Book {
        market_id: snapshot.market_id.clone(),
        token_id: snapshot.token_id.clone(),
        bids: levels(&snapshot.bids)?,
        asks: levels(&snapshot.asks)?,
        updated_at_ms: snapshot.book_updated_at_ms,
    };
    book.normalize().ok()?;
    Some(book)
}

fn funnel(observations: &[Observation], config: &RuntimeConfig, fee: &FeeSchedule) -> Funnel {
    let zero_fee = FeeSchedule {
        rate: Decimal::ZERO,
    };
    let zero_reserves = ReserveConfig {
        slippage_bps: Decimal::ZERO,
        latency: Decimal::ZERO,
        orphan: Decimal::ZERO,
        rounding_scale: config.reserves.rounding_scale,
    };
    let open = open_profitability(config, Decimal::ONE);
    let reserve_open = open_profitability(config, config.profitability.depth_fraction);
    let mut result = Funnel::default();
    for observation in observations {
        result.paired_observations = result.paired_observations.saturating_add(1);
        result.frictionless +=
            has_opportunity(observation, &zero_fee, &open, &zero_reserves, config) as u64;
        result.fee_positive +=
            has_opportunity(observation, fee, &open, &zero_reserves, config) as u64;
        result.reserve_positive +=
            has_opportunity(observation, fee, &reserve_open, &config.reserves, config) as u64;
        result.configured += has_opportunity(
            observation,
            fee,
            &config.profitability,
            &config.reserves,
            config,
        ) as u64;
    }
    result
}

fn has_opportunity(
    observation: &Observation,
    fee: &FeeSchedule,
    profitability: &ProfitabilityConfig,
    reserves: &ReserveConfig,
    config: &RuntimeConfig,
) -> bool {
    select_best(
        &observation.book_a,
        &observation.book_b,
        fee,
        profitability,
        reserves,
        &config.risk,
    )
    .is_some()
}

fn open_profitability(config: &RuntimeConfig, depth_fraction: Decimal) -> ProfitabilityConfig {
    ProfitabilityConfig {
        price_band_enabled: false,
        minimum_profit: Decimal::ZERO,
        minimum_return_bps: Decimal::ZERO,
        depth_fraction,
        price_band_low_max: config.profitability.price_band_low_max,
        price_band_high_min: config.profitability.price_band_high_min,
    }
}

fn select_candidate(
    observations: &[Observation],
    config: &RuntimeConfig,
    fee: &FeeSchedule,
    coverage_bps: u64,
) -> Option<Candidate> {
    if coverage_bps < 9_500 || observations.is_empty() {
        return None;
    }
    let min_time = observations.first()?.observed_at_ms;
    let max_time = observations.last()?.observed_at_ms;
    let span = max_time.saturating_sub(min_time).max(1);
    let split =
        |time: i64| ((time.saturating_sub(min_time) as i128 * 5 / span as i128) as usize).min(4);
    let bands = [
        (true, Decimal::new(15, 2), Decimal::new(85, 2), "0.15/0.85"),
        (true, Decimal::new(25, 2), Decimal::new(75, 2), "0.25/0.75"),
        (true, Decimal::new(35, 2), Decimal::new(65, 2), "0.35/0.65"),
        (true, Decimal::new(45, 2), Decimal::new(55, 2), "0.45/0.55"),
        (false, Decimal::new(25, 2), Decimal::new(75, 2), "disabled"),
    ];
    let mut candidates = Vec::new();
    for (enabled, low, high, label) in bands {
        for minimum_profit in [Decimal::new(2, 2), Decimal::new(5, 2), Decimal::new(10, 2)] {
            for minimum_return_bps in [Decimal::from(30), Decimal::from(60), Decimal::from(100)] {
                for depth_fraction in [
                    Decimal::new(10, 2),
                    Decimal::new(25, 2),
                    Decimal::new(50, 2),
                ] {
                    let profitability = ProfitabilityConfig {
                        price_band_enabled: enabled,
                        minimum_profit,
                        minimum_return_bps,
                        depth_fraction,
                        price_band_low_max: low,
                        price_band_high_min: high,
                    };
                    let hits = observations.iter().filter_map(|observation| {
                        select_best(
                            &observation.book_a,
                            &observation.book_b,
                            fee,
                            &profitability,
                            &config.reserves,
                            &config.risk,
                        )
                        .map(|opportunity| hit(observation, opportunity))
                    });
                    let hits = hits.collect::<Vec<_>>();
                    let slices = [0, 1, 2].map(|part| {
                        let hits = hits.iter().filter(|hit| match part {
                            0 => split(hit.observed_at_ms) < 3,
                            1 => split(hit.observed_at_ms) == 3,
                            _ => split(hit.observed_at_ms) == 4,
                        });
                        episode_slice(hits)
                    });
                    if slices[0].stable_episodes < 20
                        || slices[0].distinct_markets < 10
                        || slices[1].stable_episodes < 5
                        || slices[1].distinct_markets < 3
                    {
                        continue;
                    }
                    let holdout_pass = slices[2].stable_episodes >= 5
                        && slices[2].distinct_markets >= 3
                        && slices[2].total_profit > Decimal::ZERO;
                    candidates.push(Candidate {
                        price_band: label.to_owned(),
                        minimum_profit,
                        minimum_return_bps,
                        depth_fraction,
                        train: slices[0].clone(),
                        validation: slices[1].clone(),
                        holdout: slices[2].clone(),
                        holdout_pass,
                    });
                }
            }
        }
    }
    candidates.into_iter().max_by(|left, right| {
        left.validation
            .total_profit
            .cmp(&right.validation.total_profit)
            .then_with(|| {
                left.validation
                    .stable_episodes
                    .cmp(&right.validation.stable_episodes)
            })
    })
}

fn hit(observation: &Observation, opportunity: Opportunity) -> Hit {
    Hit {
        condition_id: observation.condition_id.clone(),
        observed_at_ms: observation.observed_at_ms,
        direction: opportunity.direction,
        profit: opportunity.net_profit,
    }
}

fn episode_slice<'a>(hits: impl Iterator<Item = &'a Hit>) -> Slice {
    let mut hits = hits.collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        (&left.condition_id, left.direction, left.observed_at_ms).cmp(&(
            &right.condition_id,
            right.direction,
            right.observed_at_ms,
        ))
    });
    let mut stable_episodes = 0u64;
    let mut markets = BTreeSet::new();
    let mut total_profit = Decimal::ZERO;
    let mut index = 0;
    while index < hits.len() {
        let start = index;
        index += 1;
        while index < hits.len()
            && hits[index].condition_id == hits[start].condition_id
            && hits[index].direction == hits[start].direction
            && hits[index]
                .observed_at_ms
                .saturating_sub(hits[index - 1].observed_at_ms)
                <= 45_000
        {
            index += 1;
        }
        if index - start >= 2 {
            stable_episodes = stable_episodes.saturating_add(1);
            markets.insert(hits[start].condition_id.clone());
            total_profit += hits[start..index]
                .iter()
                .map(|hit| hit.profit)
                .min()
                .unwrap_or(Decimal::ZERO);
        }
    }
    Slice {
        stable_episodes,
        distinct_markets: markets.len() as u64,
        total_profit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_episode_requires_two_close_observations() {
        let hits = [
            Hit {
                condition_id: "a".to_owned(),
                observed_at_ms: 1_000,
                direction: Direction::BuyMerge,
                profit: Decimal::new(5, 2),
            },
            Hit {
                condition_id: "a".to_owned(),
                observed_at_ms: 31_000,
                direction: Direction::BuyMerge,
                profit: Decimal::new(4, 2),
            },
            Hit {
                condition_id: "b".to_owned(),
                observed_at_ms: 1_000,
                direction: Direction::BuyMerge,
                profit: Decimal::new(9, 2),
            },
        ];
        let slice = episode_slice(hits.iter());
        assert_eq!(slice.stable_episodes, 1);
        assert_eq!(slice.distinct_markets, 1);
        assert_eq!(slice.total_profit, Decimal::new(4, 2));
    }
}
