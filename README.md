# moni-strategy-beta

`moni-strategy-beta` is a credential-free Polymarket complete-set signal daemon.
It discovers binary crypto markets ending within 24 hours, maintains paired CLOB
books, verifies every candidate with the batched REST books endpoint, and sends
tenant-agnostic `SubmitCompleteSetSignal` requests to `moni-engine`.

The strategy never places orders or performs split/merge transactions. The
engine owns tenant opt-in, balances, risk, paired execution, recovery, and
wallet operations.

Useful commands:

```text
moni-strategy-beta serve --config /etc/moni-strategy-beta/config.toml
moni-strategy-beta serve --dry-run
moni-strategy-beta --discover-only --config ./config.example.toml
moni-strategy-beta calibration-summary
moni-strategy-beta store-calibration-summary
moni-strategy-beta execution-summary
```

`--dry-run` is forced observe-only. Automatic paper signaling is controlled by
the independently matured direction/duration gates and the engine tenant's
`dry_run` setting; the strategy never changes that setting.

Decisions are stored in the SQLite database configured by
`state.decision_db_path`. When that database is first created, an existing
JSONL file with the same path stem is imported transactionally and retained
unchanged as a backup.

`serve` exposes Prometheus metrics on the private socket configured by
`metrics.bind` (default `127.0.0.1:9465`). Metrics cover cycle health, market
counts, bounded decision-reason categories, independent opportunity episodes,
submission outcomes, cache size, and calibration-gate readiness. They never
label metrics with market, token, condition, tenant, or signal identifiers.

From version 0.1.11, unchanged non-opportunity decisions are recorded on
transition and every 15 minutes rather than on every evaluation cycle.
Opportunity, submission, and error decisions are always recorded.
