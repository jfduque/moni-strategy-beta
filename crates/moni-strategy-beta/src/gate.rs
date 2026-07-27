use crate::pricing::Direction;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurationBucket {
    Under15m,
    From15mTo1h,
    From1hTo6h,
    From6hTo24h,
}

impl DurationBucket {
    pub fn from_remaining_ms(value: i64) -> Option<Self> {
        match value {
            ..=0 => None,
            1..=899_999 => Some(Self::Under15m),
            900_000..=3_599_999 => Some(Self::From15mTo1h),
            3_600_000..=21_599_999 => Some(Self::From1hTo6h),
            21_600_000..=86_400_000 => Some(Self::From6hTo24h),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GateKey {
    pub direction: Direction,
    pub bucket: DurationBucket,
}

impl Serialize for GateKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let direction = match self.direction {
            Direction::BuyMerge => "buy_merge",
            Direction::SplitSell => "split_sell",
        };
        let bucket = match self.bucket {
            DurationBucket::Under15m => "under_15m",
            DurationBucket::From15mTo1h => "15m_to_1h",
            DurationBucket::From1hTo6h => "1h_to_6h",
            DurationBucket::From6hTo24h => "6h_to_24h",
        };
        serializer.serialize_str(&format!("{direction}:{bucket}"))
    }
}

impl<'de> Deserialize<'de> for GateKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let (direction, bucket) = value
            .split_once(':')
            .ok_or_else(|| serde::de::Error::custom("invalid gate key"))?;
        let direction = match direction {
            "buy_merge" => Direction::BuyMerge,
            "split_sell" => Direction::SplitSell,
            _ => return Err(serde::de::Error::custom("invalid gate direction")),
        };
        let bucket = match bucket {
            "under_15m" => DurationBucket::Under15m,
            "15m_to_1h" => DurationBucket::From15mTo1h,
            "1h_to_6h" => DurationBucket::From1hTo6h,
            "6h_to_24h" => DurationBucket::From6hTo24h,
            _ => return Err(serde::de::Error::custom("invalid gate bucket")),
        };
        Ok(Self { direction, bucket })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationSample {
    pub episode_id: String,
    pub observed_at_ms: i64,
    pub terminal: bool,
    pub valid_data: bool,
    pub unresolved_orphan: bool,
    pub net_profit: Decimal,
    pub slippage: Decimal,
    pub latency_cost: Decimal,
    pub orphan_loss: Decimal,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LearnedReserves {
    pub slippage_p99: Decimal,
    pub latency_p99: Decimal,
    pub orphan_p99: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateStatus {
    pub unlocked: bool,
    pub first_observed_at_ms: i64,
    pub independent_cycles: usize,
    pub valid_coverage: Decimal,
    pub aggregate_profit: Decimal,
    pub median_profit: Decimal,
    pub reserves: LearnedReserves,
    pub relock_reason: Option<String>,
}

impl Default for GateStatus {
    fn default() -> Self {
        Self {
            unlocked: false,
            first_observed_at_ms: 0,
            independent_cycles: 0,
            valid_coverage: Decimal::ZERO,
            aggregate_profit: Decimal::ZERO,
            median_profit: Decimal::ZERO,
            reserves: LearnedReserves::default(),
            relock_reason: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GateState {
    statuses: BTreeMap<GateKey, GateStatus>,
    samples: BTreeMap<GateKey, Vec<CalibrationSample>>,
    episodes: BTreeMap<GateKey, BTreeSet<String>>,
}

impl GateState {
    pub fn record(&mut self, key: GateKey, sample: CalibrationSample) -> bool {
        if !self
            .episodes
            .entry(key)
            .or_default()
            .insert(sample.episode_id.clone())
        {
            return false;
        }
        self.samples.entry(key).or_default().push(sample);
        true
    }

    pub fn evaluate(
        &mut self,
        key: GateKey,
        now_ms: i64,
        minimum_age_hours: u64,
        minimum_cycles: usize,
        minimum_coverage: Decimal,
    ) -> &GateStatus {
        let samples = self
            .samples
            .get(&key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let status = self.statuses.entry(key).or_default();
        if status.first_observed_at_ms == 0 {
            status.first_observed_at_ms = samples
                .first()
                .map(|sample| sample.observed_at_ms)
                .unwrap_or(now_ms);
        }
        let terminal = samples
            .iter()
            .filter(|sample| sample.terminal)
            .collect::<Vec<_>>();
        status.independent_cycles = terminal.len();
        let valid = terminal.iter().filter(|sample| sample.valid_data).count();
        status.valid_coverage = if terminal.is_empty() {
            Decimal::ZERO
        } else {
            Decimal::from(valid as u64) / Decimal::from(terminal.len() as u64)
        };
        let mut profits = terminal
            .iter()
            .filter(|sample| sample.valid_data)
            .map(|sample| sample.net_profit)
            .collect::<Vec<_>>();
        profits.sort();
        status.aggregate_profit = profits.iter().copied().sum();
        status.median_profit = median(&profits);
        status.reserves = LearnedReserves {
            slippage_p99: p99(&terminal
                .iter()
                .map(|sample| sample.slippage)
                .collect::<Vec<_>>()),
            latency_p99: p99(&terminal
                .iter()
                .map(|sample| sample.latency_cost)
                .collect::<Vec<_>>()),
            orphan_p99: p99(&terminal
                .iter()
                .map(|sample| sample.orphan_loss)
                .collect::<Vec<_>>()),
        };

        let relock = if samples.last().is_some_and(|sample| !sample.valid_data) {
            Some("invalid_data")
        } else if samples
            .last()
            .is_some_and(|sample| sample.unresolved_orphan)
        {
            Some("unresolved_orphan")
        } else if terminal.len() >= minimum_cycles && status.aggregate_profit <= Decimal::ZERO {
            Some("non_positive_aggregate_profit")
        } else {
            None
        };
        if let Some(reason) = relock {
            status.unlocked = false;
            status.relock_reason = Some(reason.to_owned());
            return status;
        }

        let age_ms = now_ms.saturating_sub(status.first_observed_at_ms);
        status.unlocked = age_ms >= (minimum_age_hours * 60 * 60 * 1_000) as i64
            && terminal.len() >= minimum_cycles
            && status.valid_coverage >= minimum_coverage
            && status.aggregate_profit > Decimal::ZERO
            && status.median_profit > Decimal::ZERO;
        status.relock_reason = None;
        status
    }

    pub fn status(&self, key: GateKey) -> Option<&GateStatus> {
        self.statuses.get(&key)
    }

    pub fn summaries(&self) -> Vec<(GateKey, GateStatus)> {
        self.statuses
            .iter()
            .map(|(key, status)| (*key, status.clone()))
            .collect()
    }

    pub fn relock(&mut self, key: GateKey, reason: &str) {
        let status = self.statuses.entry(key).or_default();
        status.unlocked = false;
        status.relock_reason = Some(reason.to_owned());
    }

    pub fn relock_all(&mut self, reason: &str) {
        for status in self.statuses.values_mut() {
            status.unlocked = false;
            status.relock_reason = Some(reason.to_owned());
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = fs::read_to_string(path)
            .with_context(|| format!("reading gate state {}", path.display()))?;
        serde_json::from_str(&raw).context("parsing gate state")
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating state directory {}", parent.display()))?;
        }
        let temporary = path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(self).context("serializing gate state")?;
        fs::write(&temporary, bytes)
            .with_context(|| format!("writing temporary gate state {}", temporary.display()))?;
        fs::rename(&temporary, path)
            .with_context(|| format!("installing gate state {}", path.display()))?;
        Ok(())
    }
}

fn median(values: &[Decimal]) -> Decimal {
    match values.len() {
        0 => Decimal::ZERO,
        length if length % 2 == 1 => values[length / 2],
        length => (values[length / 2 - 1] + values[length / 2]) / Decimal::from(2),
    }
}

fn p99(values: &[Decimal]) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }
    let mut values = values.to_vec();
    values.sort();
    let index = (values.len() * 99).div_ceil(100).saturating_sub(1);
    values[index]
}

pub fn append_jsonl(path: impl AsRef<Path>, value: &impl Serialize) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating log directory {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening JSONL log {}", path.display()))?;
    serde_json::to_writer(&mut file, value).context("serializing JSONL record")?;
    file.write_all(b"\n").context("terminating JSONL record")?;
    file.sync_data().context("syncing JSONL log")?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct CircuitState {
    active_cycles: BTreeSet<(String, Direction)>,
    unresolved_orphan: bool,
    recent_signals_ms: VecDeque<i64>,
}

impl CircuitState {
    pub fn can_start(
        &mut self,
        market_id: &str,
        direction: Direction,
        now_ms: i64,
        rate: usize,
    ) -> Result<()> {
        while self
            .recent_signals_ms
            .front()
            .is_some_and(|timestamp| now_ms.saturating_sub(*timestamp) >= 60_000)
        {
            self.recent_signals_ms.pop_front();
        }
        if self.unresolved_orphan {
            anyhow::bail!("an unresolved orphan blocks new cycles");
        }
        if self
            .active_cycles
            .contains(&(market_id.to_owned(), direction))
        {
            anyhow::bail!("a nonterminal cycle already exists for market and direction");
        }
        if self.recent_signals_ms.len() >= rate {
            anyhow::bail!("signal rate limit exceeded");
        }
        Ok(())
    }

    pub fn started(&mut self, market_id: String, direction: Direction, now_ms: i64) {
        self.active_cycles.insert((market_id, direction));
        self.recent_signals_ms.push_back(now_ms);
    }

    pub fn terminal(&mut self, market_id: &str, direction: Direction) {
        self.active_cycles
            .remove(&(market_id.to_owned(), direction));
    }

    pub fn set_unresolved_orphan(&mut self, unresolved: bool) {
        self.unresolved_orphan = unresolved;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(index: usize, profit: i64) -> CalibrationSample {
        CalibrationSample {
            episode_id: format!("episode-{index}"),
            observed_at_ms: index as i64,
            terminal: true,
            valid_data: true,
            unresolved_orphan: false,
            net_profit: Decimal::new(profit, 2),
            slippage: Decimal::new(index as i64, 4),
            latency_cost: Decimal::new(1, 3),
            orphan_loss: Decimal::ZERO,
        }
    }

    #[test]
    fn unlocks_only_after_independent_mature_profitable_coverage() {
        let key = GateKey {
            direction: Direction::BuyMerge,
            bucket: DurationBucket::Under15m,
        };
        let mut state = GateState::default();
        for index in 0..100 {
            assert!(state.record(key, sample(index, 1)));
            assert!(!state.record(key, sample(index, 1)));
        }
        let status = state.evaluate(key, 24 * 60 * 60 * 1_000, 24, 100, Decimal::new(99, 2));
        assert!(status.unlocked);
        assert_eq!(status.independent_cycles, 100);
    }

    #[test]
    fn invalid_data_and_orphan_relock() {
        let key = GateKey {
            direction: Direction::SplitSell,
            bucket: DurationBucket::From1hTo6h,
        };
        let mut state = GateState::default();
        let mut invalid = sample(0, 1);
        invalid.valid_data = false;
        state.record(key, invalid);
        let status = state.evaluate(key, 100_000_000, 24, 1, Decimal::ZERO);
        assert!(!status.unlocked);
        assert_eq!(status.relock_reason.as_deref(), Some("invalid_data"));
    }

    #[test]
    fn gate_state_round_trips_without_losing_episode_deduplication() {
        let path = std::env::temp_dir().join(format!(
            "moni-beta-gate-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let key = GateKey {
            direction: Direction::SplitSell,
            bucket: DurationBucket::From1hTo6h,
        };
        let mut state = GateState::default();
        assert!(state.record(key, sample(1, 1)));
        state.save(&path).unwrap();

        let mut restored = GateState::load(&path).unwrap();
        assert!(!restored.record(key, sample(1, 1)));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn circuit_enforces_single_cycle_orphan_and_rate_limit() {
        let mut circuit = CircuitState::default();
        assert!(circuit.can_start("m", Direction::BuyMerge, 0, 1).is_ok());
        circuit.started("m".to_owned(), Direction::BuyMerge, 0);
        assert!(circuit.can_start("m", Direction::BuyMerge, 1, 1).is_err());
        circuit.terminal("m", Direction::BuyMerge);
        assert!(
            circuit
                .can_start("other", Direction::BuyMerge, 1, 1)
                .is_err()
        );
        circuit.set_unresolved_orphan(true);
        assert!(
            circuit
                .can_start("other", Direction::BuyMerge, 60_001, 1)
                .is_err()
        );
    }
}
