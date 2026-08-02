use crate::pricing::Direction;
use crate::service::Decision;
use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::Mutex;

/// Schema for the decision table.
///
/// Reproduced verbatim from the 0.1.6 database still on the lab box, so an
/// existing file opens and appends in place rather than being rewritten --
/// the 0.1.6 source was lost, and its recorded decisions are the only
/// history there is. `IF NOT EXISTS` keeps that path a no-op.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY,
    observed_at_ms INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    condition_id TEXT,
    token_id_a TEXT,
    token_id_b TEXT,
    direction TEXT CHECK (
        direction IS NULL OR direction IN ('buy_merge', 'split_sell')
    ),
    quantity TEXT,
    expected_profit TEXT,
    gate_unlocked INTEGER NOT NULL CHECK (gate_unlocked IN (0, 1)),
    submitted INTEGER NOT NULL CHECK (submitted IN (0, 1)),
    reason TEXT NOT NULL,
    store_coverage_a INTEGER CHECK (
        store_coverage_a IS NULL OR store_coverage_a IN (0, 1)
    ),
    store_coverage_b INTEGER CHECK (
        store_coverage_b IS NULL OR store_coverage_b IN (0, 1)
    )
)";

/// The wire strings for [`Direction`] in the `direction` column.
///
/// The schema's CHECK constraint hardcodes these, so they are duplicated
/// here rather than derived; `direction_matches_serde_representation` guards
/// the two against drifting apart.
fn direction_as_str(direction: Direction) -> &'static str {
    match direction {
        Direction::BuyMerge => "buy_merge",
        Direction::SplitSell => "split_sell",
    }
}

#[cfg(test)]
fn direction_from_str(raw: &str) -> Result<Direction> {
    match raw {
        "buy_merge" => Ok(Direction::BuyMerge),
        "split_sell" => Ok(Direction::SplitSell),
        other => anyhow::bail!("unknown direction {other:?} in decision store"),
    }
}

/// The only decision fields the store-coverage summary needs.
///
/// Deliberately narrow. The live database passes a million rows within a
/// day, and materializing full [`Decision`] values for all of them exceeds
/// the strategy container's 1 GiB limit outright -- the summary is streamed
/// a row at a time instead.
pub(crate) struct CalibrationRow {
    pub(crate) observed_at_ms: i64,
    pub(crate) condition_id: Option<String>,
    pub(crate) token_id_a: Option<String>,
    pub(crate) token_id_b: Option<String>,
}

/// Append-only SQLite store for per-market decisions.
pub(crate) struct DecisionStore {
    connection: Mutex<Connection>,
}

impl DecisionStore {
    /// Opens (creating if absent) the decision database at `path`.
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating decision directory {}", parent.display()))?;
        }
        let connection = Connection::open(path)
            .with_context(|| format!("opening decision database {}", path.display()))?;
        // WAL keeps the writer from blocking the read-only summary commands,
        // which are run against the live file while the service is up.
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .context("enabling WAL on the decision database")?;
        connection
            .execute_batch(SCHEMA)
            .context("creating the decisions table")?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Appends one decision.
    pub(crate) fn append(&self, decision: &Decision) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .expect("decision store mutex poisoned");
        connection
            .execute(
                "INSERT INTO decisions (
                    observed_at_ms, market_id, condition_id, token_id_a, token_id_b,
                    direction, quantity, expected_profit, gate_unlocked, submitted,
                    reason, store_coverage_a, store_coverage_b
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    decision.observed_at_ms,
                    decision.market_id,
                    decision.condition_id,
                    decision.token_id_a,
                    decision.token_id_b,
                    decision.direction.map(direction_as_str),
                    decision.quantity.map(|value| value.to_string()),
                    decision.expected_profit.map(|value| value.to_string()),
                    decision.gate_unlocked,
                    decision.submitted,
                    decision.reason,
                    decision.store_coverage_a,
                    decision.store_coverage_b,
                ],
            )
            .context("appending to the decision store")?;
        Ok(())
    }

    pub(crate) fn latest_calibration_id(&self) -> Result<i64> {
        let connection = self
            .connection
            .lock()
            .expect("decision store mutex poisoned");
        connection
            .query_row("SELECT coalesce(max(id), 0) FROM decisions", [], |row| {
                row.get(0)
            })
            .context("querying latest decision id")
    }

    /// Reads a bounded, stable slice of calibration fields in insertion order.
    pub(crate) fn calibration_batch(
        &self,
        after_id: i64,
        through_id: i64,
        limit: usize,
    ) -> Result<Vec<(i64, CalibrationRow)>> {
        let connection = self
            .connection
            .lock()
            .expect("decision store mutex poisoned");
        let mut statement = connection
            .prepare(
                "SELECT id, observed_at_ms, condition_id, token_id_a, token_id_b
                 FROM decisions
                 WHERE id > ?1 AND id <= ?2
                 ORDER BY id
                 LIMIT ?3",
            )
            .context("preparing the calibration row query")?;
        let mut rows = statement
            .query(params![after_id, through_id, limit as i64])
            .context("querying calibration rows")?;
        let mut batch = Vec::with_capacity(limit);
        while let Some(row) = rows.next().context("reading a calibration row")? {
            batch.push((
                row.get(0).context("reading decision id")?,
                CalibrationRow {
                    observed_at_ms: row.get(1).context("reading observed_at_ms")?,
                    condition_id: row.get(2).context("reading condition_id")?,
                    token_id_a: row.get(3).context("reading token_id_a")?,
                    token_id_b: row.get(4).context("reading token_id_b")?,
                },
            ));
        }
        Ok(batch)
    }

    /// Loads every decision in full. Test-only: it materializes the whole
    /// table, which the live database is far too large for.
    #[cfg(test)]
    pub(crate) fn load_all(&self) -> Result<Vec<Decision>> {
        let connection = self
            .connection
            .lock()
            .expect("decision store mutex poisoned");
        let mut statement = connection
            .prepare(
                "SELECT observed_at_ms, market_id, condition_id, token_id_a, token_id_b,
                        direction, quantity, expected_profit, gate_unlocked, submitted,
                        reason, store_coverage_a, store_coverage_b
                 FROM decisions ORDER BY id",
            )
            .context("preparing the decision query")?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, bool>(8)?,
                    row.get::<_, bool>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, Option<bool>>(11)?,
                    row.get::<_, Option<bool>>(12)?,
                ))
            })
            .context("querying the decision store")?;

        let mut decisions = Vec::new();
        for row in rows {
            let row = row.context("reading a decision row")?;
            decisions.push(Decision {
                observed_at_ms: row.0,
                market_id: row.1,
                condition_id: row.2,
                token_id_a: row.3,
                token_id_b: row.4,
                direction: row.5.as_deref().map(direction_from_str).transpose()?,
                quantity: parse_decimal(row.6.as_deref(), "quantity")?,
                expected_profit: parse_decimal(row.7.as_deref(), "expected_profit")?,
                gate_unlocked: row.8,
                submitted: row.9,
                reason: row.10,
                store_coverage_a: row.11,
                store_coverage_b: row.12,
            });
        }
        Ok(decisions)
    }
}

#[cfg(test)]
use rust_decimal::Decimal;
#[cfg(test)]
use std::str::FromStr;

#[cfg(test)]
fn parse_decimal(raw: Option<&str>, column: &str) -> Result<Option<Decimal>> {
    raw.map(|value| {
        Decimal::from_str(value)
            .with_context(|| format!("parsing {column} {value:?} from the decision store"))
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn decision(reason: &str) -> Decision {
        Decision {
            observed_at_ms: 1_785_000_000_000,
            market_id: "3129270".to_owned(),
            condition_id: Some("0xabc".to_owned()),
            token_id_a: Some("token-a".to_owned()),
            token_id_b: Some("token-b".to_owned()),
            direction: Some(Direction::BuyMerge),
            quantity: Some(Decimal::from_str("5.25").unwrap()),
            expected_profit: Some(Decimal::from_str("0.1234").unwrap()),
            gate_unlocked: true,
            submitted: true,
            reason: reason.to_owned(),
            store_coverage_a: Some(true),
            store_coverage_b: Some(false),
        }
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "moni-beta-{name}-{}-{:?}.sqlite3",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn reads_calibration_rows_in_bounded_insertion_order_batches() {
        let path = temp_path("stream");
        let store = DecisionStore::open(&path).unwrap();
        for index in 0..3 {
            let mut row = decision("no_profitable_depth");
            row.observed_at_ms = 1_000 + index;
            row.token_id_a = Some(format!("a{index}"));
            store.append(&row).unwrap();
        }

        let through_id = store.latest_calibration_id().unwrap();
        let first = store.calibration_batch(0, through_id, 2).unwrap();
        let second = store
            .calibration_batch(first.last().unwrap().0, through_id, 2)
            .unwrap();
        let seen = first
            .into_iter()
            .chain(second)
            .map(|(_, row)| (row.observed_at_ms, row.token_id_a, row.token_id_b))
            .collect::<Vec<_>>();
        assert_eq!(
            seen,
            vec![
                (1_000, Some("a0".to_owned()), Some("token-b".to_owned())),
                (1_001, Some("a1".to_owned()), Some("token-b".to_owned())),
                (1_002, Some("a2".to_owned()), Some("token-b".to_owned())),
            ]
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn calibration_upper_bound_excludes_rows_appended_after_summary_start() {
        let path = temp_path("upper-bound");
        let store = DecisionStore::open(&path).unwrap();
        store.append(&decision("no_profitable_depth")).unwrap();
        let through_id = store.latest_calibration_id().unwrap();
        store.append(&decision("outside_price_band")).unwrap();

        let batch = store.calibration_batch(0, through_id, 10).unwrap();
        assert_eq!(batch.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn round_trips_every_field() {
        let path = temp_path("roundtrip");
        let store = DecisionStore::open(&path).unwrap();
        let original = decision("no_profitable_depth");
        store.append(&original).unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        let loaded = &loaded[0];
        assert_eq!(loaded.observed_at_ms, original.observed_at_ms);
        assert_eq!(loaded.market_id, original.market_id);
        assert_eq!(loaded.condition_id, original.condition_id);
        assert_eq!(loaded.token_id_a, original.token_id_a);
        assert_eq!(loaded.token_id_b, original.token_id_b);
        assert_eq!(loaded.direction, original.direction);
        assert_eq!(loaded.quantity, original.quantity);
        assert_eq!(loaded.expected_profit, original.expected_profit);
        assert_eq!(loaded.gate_unlocked, original.gate_unlocked);
        assert_eq!(loaded.submitted, original.submitted);
        assert_eq!(loaded.reason, original.reason);
        assert_eq!(loaded.store_coverage_a, original.store_coverage_a);
        assert_eq!(loaded.store_coverage_b, original.store_coverage_b);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn nullable_columns_survive_a_round_trip() {
        let path = temp_path("nulls");
        let store = DecisionStore::open(&path).unwrap();
        let mut sparse = decision("market_not_subscribable");
        sparse.condition_id = None;
        sparse.token_id_a = None;
        sparse.token_id_b = None;
        sparse.direction = None;
        sparse.quantity = None;
        sparse.expected_profit = None;
        sparse.store_coverage_a = None;
        sparse.store_coverage_b = None;
        store.append(&sparse).unwrap();

        let loaded = &store.load_all().unwrap()[0];
        assert!(loaded.condition_id.is_none());
        assert!(loaded.direction.is_none());
        assert!(loaded.quantity.is_none());
        assert!(loaded.expected_profit.is_none());
        assert!(loaded.store_coverage_a.is_none());
        assert!(loaded.store_coverage_b.is_none());
        let _ = std::fs::remove_file(&path);
    }

    /// Reopening must append to the existing rows, not replace them: the
    /// 0.1.6 database on the lab is the only copy of that history.
    #[test]
    fn reopening_preserves_existing_rows() {
        let path = temp_path("reopen");
        let store = DecisionStore::open(&path).unwrap();
        store.append(&decision("first")).unwrap();
        drop(store);

        let reopened = DecisionStore::open(&path).unwrap();
        reopened.append(&decision("second")).unwrap();
        let loaded = reopened.load_all().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].reason, "first");
        assert_eq!(loaded[1].reason, "second");
        let _ = std::fs::remove_file(&path);
    }

    /// The CHECK constraint hardcodes the direction strings, so they must
    /// stay in step with the enum's serde representation.
    #[test]
    fn direction_matches_serde_representation() {
        for direction in [Direction::BuyMerge, Direction::SplitSell] {
            let serde_form = serde_json::to_string(&direction).unwrap();
            let serde_form = serde_form.trim_matches('"');
            assert_eq!(direction_as_str(direction), serde_form);
            assert_eq!(direction_from_str(serde_form).unwrap(), direction);
        }
    }
}
