use crate::config::{ProfitabilityConfig, QualityConfig, ReserveConfig, RiskConfig};
use anyhow::{Result, bail};
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Book {
    pub market_id: String,
    pub token_id: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub updated_at_ms: i64,
}

impl Book {
    pub fn normalize(&mut self) -> Result<()> {
        validate_levels(&self.bids)?;
        validate_levels(&self.asks)?;
        self.bids
            .sort_by(|left, right| right.price.cmp(&left.price));
        self.asks
            .sort_by(|left, right| left.price.cmp(&right.price));
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    BuyMerge,
    SplitSell,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeeSchedule {
    pub rate: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    OutsidePriceBand,
    NoProfitableDepth,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Opportunity {
    pub direction: Direction,
    pub quantity: Decimal,
    pub leg_a_gross: Decimal,
    pub leg_b_gross: Decimal,
    pub fees: Decimal,
    pub reserves: Decimal,
    pub pair_value: Decimal,
    pub capital: Decimal,
    pub net_profit: Decimal,
    pub return_bps: Decimal,
}

pub fn validate_fee_metadata(
    gamma_rate: Option<Decimal>,
    gamma_exponent: Option<i64>,
    gamma_taker_only: Option<bool>,
    clob_rate: Option<Decimal>,
    clob_exponent: Option<i64>,
    clob_taker_only: Option<bool>,
) -> Result<FeeSchedule> {
    let gamma = gamma_rate.ok_or_else(|| anyhow::anyhow!("Gamma fee curve is missing"))?;
    let gamma_exponent =
        gamma_exponent.ok_or_else(|| anyhow::anyhow!("Gamma fee exponent is missing"))?;
    let clob = clob_rate.ok_or_else(|| anyhow::anyhow!("CLOB fee curve is missing"))?;
    let clob_exponent =
        clob_exponent.ok_or_else(|| anyhow::anyhow!("CLOB fee exponent is missing"))?;
    let gamma_taker_only =
        gamma_taker_only.ok_or_else(|| anyhow::anyhow!("Gamma fee applicability is missing"))?;
    let clob_taker_only =
        clob_taker_only.ok_or_else(|| anyhow::anyhow!("CLOB fee applicability is missing"))?;
    if gamma <= Decimal::ZERO
        || gamma > Decimal::ONE
        || gamma != clob
        || gamma_exponent <= 0
        || gamma_exponent != clob_exponent
        || gamma_taker_only != clob_taker_only
    {
        bail!("Gamma and CLOB fee curves are contradictory");
    }
    Ok(FeeSchedule { rate: gamma })
}

#[allow(clippy::too_many_arguments)]
pub fn validate_final_books(
    ws_a: &Book,
    ws_b: &Book,
    rest_a: &Book,
    rest_b: &Book,
    quantity: Decimal,
    direction: Direction,
    tick_size: Decimal,
    now_ms: i64,
    rest_round_trip_ms: u64,
    quality: &QualityConfig,
) -> Result<()> {
    if rest_round_trip_ms > quality.max_rest_round_trip_ms {
        bail!("final REST refresh exceeded latency limit");
    }
    for (ws, rest) in [(ws_a, rest_a), (ws_b, rest_b)] {
        if ws.token_id != rest.token_id || ws.market_id != rest.market_id {
            bail!("WebSocket and REST books identify different markets or tokens");
        }
        validate_freshness(ws.updated_at_ms, now_ms, quality.max_book_age_ms)?;
        validate_freshness(rest.updated_at_ms, now_ms, quality.max_book_age_ms)?;
    }
    let ws_skew = ws_a.updated_at_ms.abs_diff(ws_b.updated_at_ms);
    let rest_skew = rest_a.updated_at_ms.abs_diff(rest_b.updated_at_ms);
    if ws_skew > quality.max_token_skew_ms || rest_skew > quality.max_token_skew_ms {
        bail!("complementary token book timestamps exceed skew limit");
    }
    let side = match direction {
        Direction::BuyMerge => Side::Ask,
        Direction::SplitSell => Side::Bid,
    };
    let tolerance = tick_size * quantity * Decimal::from(quality.max_tick_disagreement);
    for (ws, rest) in [(ws_a, rest_a), (ws_b, rest_b)] {
        let ws_total = executable_gross(ws, side, quantity)?;
        let rest_total = executable_gross(rest, side, quantity)?;
        if (ws_total - rest_total).abs() > tolerance {
            bail!("WebSocket and REST depth disagree beyond tolerance");
        }
    }
    Ok(())
}

pub fn select_best(
    book_a: &Book,
    book_b: &Book,
    fees: &FeeSchedule,
    profitability: &ProfitabilityConfig,
    reserves: &ReserveConfig,
    risk: &RiskConfig,
) -> Option<Opportunity> {
    [Direction::BuyMerge, Direction::SplitSell]
        .into_iter()
        .filter_map(|direction| {
            best_for_direction(
                book_a,
                book_b,
                fees,
                direction,
                profitability,
                reserves,
                risk,
            )
            .ok()
        })
        .max_by(compare_opportunities)
}

pub fn best_for_direction(
    book_a: &Book,
    book_b: &Book,
    fees: &FeeSchedule,
    direction: Direction,
    profitability: &ProfitabilityConfig,
    reserves: &ReserveConfig,
    risk: &RiskConfig,
) -> Result<Opportunity, Rejection> {
    let side = match direction {
        Direction::BuyMerge => Side::Ask,
        Direction::SplitSell => Side::Bid,
    };
    let levels_a = levels(book_a, side);
    let levels_b = levels(book_b, side);
    let top_price = levels_a.first().ok_or(Rejection::NoProfitableDepth)?.price;
    if profitability.price_band_enabled
        && top_price > profitability.price_band_low_max
        && top_price < profitability.price_band_high_min
    {
        // Polymarket's taker fee is rate * price * (1 - price), which peaks at the
        // 50c coinflip point. Inside the band, round-trip fees alone (~350bps at
        // 50c for a 7% category) exceed any realistic complete-set mispricing, so
        // no minimum_return_bps value could ever clear here — skip without paying
        // for the REST cross-check.
        return Err(Rejection::OutsidePriceBand);
    }
    let common_depth = total_depth(levels_a).min(total_depth(levels_b));
    let participation_cap = common_depth * profitability.depth_fraction;
    if participation_cap <= Decimal::ZERO {
        return Err(Rejection::NoProfitableDepth);
    }
    let mut breakpoints = cumulative_breakpoints(levels_a);
    breakpoints.extend(cumulative_breakpoints(levels_b));
    breakpoints.push(participation_cap);
    breakpoints.sort();
    breakpoints.dedup();
    breakpoints
        .into_iter()
        .filter(|quantity| *quantity > Decimal::ZERO && *quantity <= participation_cap)
        .filter_map(|quantity| evaluate(book_a, book_b, fees, direction, quantity, reserves).ok())
        .filter(|opportunity| {
            opportunity.capital <= risk.max_per_cycle
                && opportunity.net_profit >= profitability.minimum_profit
                && opportunity.return_bps >= profitability.minimum_return_bps
        })
        .max_by(compare_opportunities)
        .ok_or(Rejection::NoProfitableDepth)
}

pub fn evaluate(
    book_a: &Book,
    book_b: &Book,
    fees: &FeeSchedule,
    direction: Direction,
    quantity: Decimal,
    reserves: &ReserveConfig,
) -> Result<Opportunity> {
    if quantity <= Decimal::ZERO {
        bail!("quantity must be positive");
    }
    let side = match direction {
        Direction::BuyMerge => Side::Ask,
        Direction::SplitSell => Side::Bid,
    };
    let (leg_a_gross, fee_a) = executable_with_fee(book_a, side, quantity, fees.rate)?;
    let (leg_b_gross, fee_b) = executable_with_fee(book_b, side, quantity, fees.rate)?;
    let gross = leg_a_gross + leg_b_gross;
    let fees = ceil(fee_a + fee_b, reserves.rounding_scale);
    let reserve = ceil(
        gross * reserves.slippage_bps / Decimal::from(10_000) + reserves.latency + reserves.orphan,
        reserves.rounding_scale,
    );
    let (pair_value, capital, net_profit) = match direction {
        Direction::BuyMerge => {
            let cost = ceil(gross + fees + reserve, reserves.rounding_scale);
            (cost, cost, quantity - cost)
        }
        Direction::SplitSell => {
            let proceeds = floor(gross - fees - reserve, reserves.rounding_scale);
            (proceeds, quantity, proceeds - quantity)
        }
    };
    let return_bps = if capital > Decimal::ZERO {
        net_profit * Decimal::from(10_000) / capital
    } else {
        Decimal::ZERO
    };
    Ok(Opportunity {
        direction,
        quantity,
        leg_a_gross,
        leg_b_gross,
        fees,
        reserves: reserve,
        pair_value,
        capital,
        net_profit,
        return_bps,
    })
}

fn compare_opportunities(left: &Opportunity, right: &Opportunity) -> Ordering {
    left.net_profit
        .cmp(&right.net_profit)
        .then_with(|| right.capital.cmp(&left.capital))
}

#[derive(Clone, Copy)]
enum Side {
    Bid,
    Ask,
}

fn levels(book: &Book, side: Side) -> &[Level] {
    match side {
        Side::Bid => &book.bids,
        Side::Ask => &book.asks,
    }
}

fn executable_gross(book: &Book, side: Side, quantity: Decimal) -> Result<Decimal> {
    executable_with_fee(book, side, quantity, Decimal::ZERO).map(|(gross, _)| gross)
}

fn executable_with_fee(
    book: &Book,
    side: Side,
    quantity: Decimal,
    fee_rate: Decimal,
) -> Result<(Decimal, Decimal)> {
    let mut remaining = quantity;
    let mut gross = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    for level in levels(book, side) {
        if remaining <= Decimal::ZERO {
            break;
        }
        let filled = remaining.min(level.size);
        gross += filled * level.price;
        fee += filled * fee_rate * level.price * (Decimal::ONE - level.price);
        remaining -= filled;
    }
    if remaining > Decimal::ZERO {
        bail!("insufficient executable depth");
    }
    Ok((gross, fee))
}

fn validate_levels(levels: &[Level]) -> Result<()> {
    if levels.iter().any(|level| {
        level.price <= Decimal::ZERO || level.price >= Decimal::ONE || level.size <= Decimal::ZERO
    }) {
        bail!("book contains an invalid price or size");
    }
    Ok(())
}

fn validate_freshness(timestamp_ms: i64, now_ms: i64, max_age_ms: u64) -> Result<()> {
    if timestamp_ms > now_ms || now_ms.saturating_sub(timestamp_ms) as u64 > max_age_ms {
        bail!("book is stale or future-dated");
    }
    Ok(())
}

fn total_depth(levels: &[Level]) -> Decimal {
    levels.iter().map(|level| level.size).sum()
}

fn cumulative_breakpoints(levels: &[Level]) -> Vec<Decimal> {
    let mut total = Decimal::ZERO;
    levels
        .iter()
        .map(|level| {
            total += level.size;
            total
        })
        .collect()
}

fn ceil(value: Decimal, scale: u32) -> Decimal {
    value.round_dp_with_strategy(scale, RoundingStrategy::ToPositiveInfinity)
}

fn floor(value: Decimal, scale: u32) -> Decimal {
    value.round_dp_with_strategy(scale, RoundingStrategy::ToNegativeInfinity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn book(token: &str, bids: &[(&str, &str)], asks: &[(&str, &str)], ts: i64) -> Book {
        let levels = |values: &[(&str, &str)]| {
            values
                .iter()
                .map(|(price, size)| Level {
                    price: d(price),
                    size: d(size),
                })
                .collect()
        };
        Book {
            market_id: "m".to_owned(),
            token_id: token.to_owned(),
            bids: levels(bids),
            asks: levels(asks),
            updated_at_ms: ts,
        }
    }

    fn reserves() -> ReserveConfig {
        ReserveConfig {
            slippage_bps: Decimal::ZERO,
            latency: Decimal::ZERO,
            orphan: Decimal::ZERO,
            rounding_scale: 6,
        }
    }

    fn profitability() -> ProfitabilityConfig {
        ProfitabilityConfig {
            price_band_enabled: true,
            minimum_profit: Decimal::ZERO,
            minimum_return_bps: Decimal::ZERO,
            depth_fraction: Decimal::ONE,
            price_band_low_max: d("0.25"),
            price_band_high_min: d("0.75"),
        }
    }

    fn risk() -> RiskConfig {
        RiskConfig {
            max_per_cycle: d("1000"),
            max_per_market: d("1000"),
            max_per_underlying: d("1000"),
            max_aggregate: d("1000"),
            max_orphan_loss: d("1000"),
            max_unmatched_inventory: d("1000"),
            daily_loss_limit: d("1000"),
            signals_per_minute: 100,
        }
    }

    #[test]
    fn price_band_skips_coinflip_prices_but_allows_extremes() {
        let fees = FeeSchedule {
            rate: Decimal::ZERO,
        };
        let in_band_a = book("a", &[], &[("0.48", "10")], 100);
        let in_band_b = book("b", &[], &[("0.48", "10")], 100);
        assert_eq!(
            best_for_direction(
                &in_band_a,
                &in_band_b,
                &fees,
                Direction::BuyMerge,
                &profitability(),
                &reserves(),
                &risk(),
            ),
            Err(Rejection::OutsidePriceBand)
        );

        let extreme_a = book("a", &[], &[("0.20", "10")], 100);
        let extreme_b = book("b", &[], &[("0.20", "10")], 100);
        assert!(
            best_for_direction(
                &extreme_a,
                &extreme_b,
                &fees,
                Direction::BuyMerge,
                &profitability(),
                &reserves(),
                &risk(),
            )
            .is_ok()
        );
    }

    #[test]
    fn prices_buy_across_multiple_levels_with_per_level_fees() {
        let a = book("a", &[], &[("0.40", "2"), ("0.42", "3")], 100);
        let b = book("b", &[], &[("0.45", "5")], 100);
        let result = evaluate(
            &a,
            &b,
            &FeeSchedule { rate: d("0.07") },
            Direction::BuyMerge,
            d("4"),
            &reserves(),
        )
        .unwrap();
        assert_eq!(result.leg_a_gross, d("1.64"));
        assert_eq!(result.leg_b_gross, d("1.80"));
        assert!(result.fees > Decimal::ZERO);
        assert!(result.net_profit > Decimal::ZERO);
    }

    #[test]
    fn prices_split_sell_and_rounds_proceeds_down() {
        let a = book("a", &[("0.60", "5")], &[], 100);
        let b = book("b", &[("0.55", "5")], &[], 100);
        let result = evaluate(
            &a,
            &b,
            &FeeSchedule { rate: d("0.07") },
            Direction::SplitSell,
            d("2"),
            &reserves(),
        )
        .unwrap();
        assert!(result.pair_value > d("2"));
        assert_eq!(result.capital, d("2"));
    }

    #[test]
    fn rejects_missing_or_contradictory_fee_curves() {
        assert!(
            validate_fee_metadata(
                None,
                Some(1),
                Some(true),
                Some(d("0.07")),
                Some(1),
                Some(true)
            )
            .is_err()
        );
        assert!(
            validate_fee_metadata(
                Some(d("0.07")),
                Some(1),
                Some(true),
                Some(d("0.05")),
                Some(1),
                Some(true),
            )
            .is_err()
        );
        assert!(
            validate_fee_metadata(
                Some(d("0.07")),
                Some(1),
                Some(true),
                Some(d("0.07")),
                Some(2),
                Some(true),
            )
            .is_err()
        );
    }

    #[test]
    fn final_cross_check_rejects_stale_skewed_and_mismatched_books() {
        let quality = QualityConfig {
            max_book_age_ms: 750,
            max_token_skew_ms: 250,
            max_tick_disagreement: 1,
            rest_timeout_ms: 500,
            max_rest_round_trip_ms: 500,
        };
        let a = book("a", &[], &[("0.40", "5")], 900);
        let b = book("b", &[], &[("0.45", "5")], 900);
        assert!(
            validate_final_books(
                &a,
                &b,
                &a,
                &b,
                d("1"),
                Direction::BuyMerge,
                d("0.01"),
                1_000,
                100,
                &quality
            )
            .is_ok()
        );
        let stale = book("a", &[], &[("0.40", "5")], 0);
        assert!(
            validate_final_books(
                &stale,
                &b,
                &stale,
                &b,
                d("1"),
                Direction::BuyMerge,
                d("0.01"),
                1_000,
                100,
                &quality
            )
            .is_err()
        );
        let mismatch = book("a", &[], &[("0.45", "5")], 900);
        assert!(
            validate_final_books(
                &a,
                &b,
                &mismatch,
                &b,
                d("1"),
                Direction::BuyMerge,
                d("0.01"),
                1_000,
                100,
                &quality
            )
            .is_err()
        );
    }
}
