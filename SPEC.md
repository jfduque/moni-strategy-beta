# Polymarket 24-Hour Crypto Complete-Set Rebalancer

## Strategy objective

The **24-Hour Crypto Complete-Set Rebalancer** monitors every eligible Polymarket cryptocurrency market whose scheduled settlement time is less than 24 hours away and trades only when it can lock a positive, outcome-independent profit through complete-set arbitrage. The strategy does not predict whether Bitcoin, Ethereum, Solana, or another cryptocurrency will rise or fall. Instead, it searches for temporary inconsistencies between the combined executable prices of complementary outcome tokens and the fixed collateral value of a complete set.

## Eligible market universe

A market is eligible only when all of the following conditions are satisfied:

- It is categorized as a cryptocurrency market.
- Its scheduled market end time is more than zero and no more than 24 hours away.
- It is active, not closed, not archived, and currently accepting orders.
- It is enabled on the Polymarket CLOB.
- It has exactly two complementary outcomes, such as `YES/NO` or `UP/DOWN`.
- Both outcome-token identifiers are available and valid.
- The market's fee configuration can be retrieved and validated.
- The market rules unambiguously describe how the outcome is resolved.

The scheduled market end time is used for discovery and risk management. It should not be interpreted as a guarantee that oracle resolution, token redemption, or collateral release will occur immediately at that time.

## Core arbitrage principle

For a binary market, one unit of each complementary outcome token forms a complete set backed by one unit of collateral. Therefore, an equal quantity of both outcomes can be merged into collateral before resolution, or held until resolution with one token ultimately paying the full collateral amount.

For a candidate quantity `q`, the strategy calculates the complete executable acquisition cost:

```text
buy_pair_cost(q)
  = executable_ask_cost(outcome_a, q)
  + executable_ask_cost(outcome_b, q)
  + taker_fees(q)
  + order_rounding_cost
  + expected_slippage
  + latency_reserve
  + orphan_leg_recovery_reserve
  + minimum_required_profit
```

A buy-and-merge opportunity is valid only when:

```text
buy_pair_cost(q) < q
```

The expected locked profit is:

```text
locked_profit(q) = q - all_in_pair_cost(q)
```

The bot must calculate this using the full order-book depth required to execute `q`. It must never rely solely on the displayed best ask, midpoint, last trade, or indicative market price.

## Reverse split-and-sell arbitrage

The strategy also evaluates the reverse condition. When the combined executable bids for both outcomes exceed the collateral required to create a complete set, the bot may split collateral into equal outcome tokens and sell both sides.

For quantity `q`:

```text
sell_pair_proceeds(q)
  = executable_bid_proceeds(outcome_a, q)
  + executable_bid_proceeds(outcome_b, q)
  - taker_fees(q)
  - order_rounding_cost
  - expected_slippage
  - latency_reserve
  - orphan_leg_recovery_reserve
```

A split-and-sell opportunity is valid only when:

```text
sell_pair_proceeds(q) > q + minimum_required_profit
```

The expected locked profit is:

```text
locked_profit(q) = all_in_pair_proceeds(q) - q
```

The collateral split should be completed before sell orders are submitted so the strategy does not depend on borrowing outcome tokens or maintaining an unintended short position.

## Fee-aware pricing

Fees must be read from the live market configuration and included in every profitability calculation. The engine must not assume that every crypto market has the same fee schedule merely because it has a similar settlement duration.

When the currently published Polymarket crypto taker-fee formula applies, the fee for a filled outcome-token order is modeled as:

```text
fee = shares × 0.07 × price × (1 - price)
```

The fee is largest around a token price of `0.50` and smaller near `0.00` or `1.00`. Each leg must be calculated independently using its expected execution prices and quantities across the order book.

Maker rebates must not be included in the pre-trade profitability threshold. They may be recorded as additional realized revenue only after Polymarket has credited them.

## Order execution

Both complementary legs should be submitted as close together as possible using fill-or-kill orders. The candidate quantity must not exceed the common profitable depth available on both sides.

A paired submission is not assumed to be atomic. Even when orders are sent in a single batch, each order may be accepted, rejected, or filled independently. After submission, the engine must inspect the result of every order and reconcile actual fills before treating the arbitrage as complete.

The preferred execution sequence for buy-and-merge arbitrage is:

1. Read synchronized order books for both outcome tokens.
2. Calculate every profitable candidate quantity from cumulative depth.
3. Select the quantity with the best risk-adjusted locked profit, subject to exposure limits.
4. Revalidate prices, fees, market status, and available balances immediately before submission.
5. Submit equal-sized FOK buy orders for both outcomes concurrently.
6. Confirm the actual filled quantity and price of each leg.
7. If both legs filled equally, merge the complete set into collateral.
8. If only one leg filled, execute the configured orphan-leg recovery procedure immediately.
9. Record realized profit, fees, latency, slippage, rejected orders, and recovery costs.

The preferred execution sequence for split-and-sell arbitrage is:

1. Identify profitable combined bid depth.
2. Split the required collateral into equal complementary tokens.
3. Revalidate both books immediately before submission.
4. Submit equal-sized FOK sell orders for both outcomes concurrently.
5. Confirm both actual fills.
6. Recover any unmatched inventory if one order fails.
7. Record realized proceeds and all execution costs.

## Orphan-leg recovery

The primary execution risk is that one leg fills while the complementary leg fails or becomes unprofitable. The bot must never silently retain this position as a directional bet.

When an orphaned leg occurs, the engine should evaluate the following recovery actions in order:

1. Complete the missing leg immediately when the total recovery cost remains below the configured maximum loss.
2. Cancel any remaining unfilled quantity.
3. Sell the filled leg back into the book when this produces a smaller loss than completing the pair.
4. Hedge the exposure in a closely related liquid venue only when an approved hedge model exists and its basis risk is within limits.
5. Escalate to a circuit breaker when the unmatched position cannot be neutralized safely.

The profitability threshold for normal entries must exceed a conservative estimate of orphan-leg recovery risk. This reserve should be learned from observed high-percentile recovery losses rather than selected arbitrarily.

## Position sizing

The strategy selects the largest quantity for which all required order-book levels remain profitable after fees and safety reserves, while respecting the following limits:

- Maximum capital per market.
- Maximum capital per underlying cryptocurrency.
- Maximum aggregate capital across all open executions.
- Maximum unmatched outcome-token inventory.
- Maximum expected loss from orphan-leg recovery.
- Maximum order-book participation rate.
- Minimum locked profit in absolute terms.
- Minimum locked return on deployed collateral.
- Minimum profit per second of expected capital lockup.

A larger nominal arbitrage is not automatically better. The selected quantity should maximize expected net profit after accounting for fill probability, book deterioration, latency, and recovery risk.

## Market-data and safety gates

No order may be submitted unless all mandatory gates pass:

- Both order books are live and have timestamps within the configured freshness threshold.
- The difference between the two book timestamps is below the synchronization threshold.
- WebSocket and REST market data agree within the configured tolerance.
- The market is still active and accepting orders.
- The scheduled end time remains inside the permitted horizon.
- No market-resolution or order-acceptance cutoff has already passed.
- Both outcome tokens map to the same market and complementary outcomes.
- Fee metadata is present and valid.
- The calculated edge remains positive after a final pre-trade refresh.
- Available collateral and token balances are sufficient.
- Trading permissions, live-trading flags, tenant settings, and risk controls permit the order.
- No market, asset, exchange, data-quality, or execution circuit breaker is active.

The strategy must reject stale books, uncertain market classifications, malformed outcome mappings, missing fee data, insufficient common depth, and opportunities whose edge is smaller than the full configured safety reserve.

## Profitability threshold

The minimum required edge should include both an absolute and a relative requirement:

```text
expected_locked_profit >= minimum_profit_usd
```

and

```text
expected_locked_profit / capital_deployed >= minimum_return_bps
```

An optional time-normalized threshold may also be applied:

```text
expected_locked_profit / expected_lock_seconds >= minimum_profit_per_second
```

The trade is accepted only when all enabled profitability thresholds pass. This prevents the engine from deploying capital for economically insignificant returns or from taking execution risk for an edge that is positive only before operational costs.

## Capital release

When equal quantities of both outcome tokens have been acquired, the preferred action is to merge them into collateral immediately after settlement of the token transfers. Immediate merging removes outcome exposure and releases capital without waiting for the market's final resolution.

Holding a complete set until resolution may be permitted only when merging is unavailable, temporarily more expensive than waiting, or restricted by an operational issue. Such positions must still be recorded as complete-set inventory rather than directional exposure.

## Market-duration considerations

Expanding from 5-minute, 15-minute, and 1-hour markets to all crypto markets settling within 24 hours increases the number and diversity of opportunities, but also introduces different liquidity and capital-efficiency profiles:

- Very short markets may show more frequent price dislocations but provide little time to recover from execution errors.
- Multi-hour markets may offer deeper books but can lock capital longer when complete sets cannot be merged immediately.
- Markets close to settlement may experience rapid probability changes, wider spreads, disappearing liquidity, and higher orphan-leg risk.
- Thin markets may display large apparent edges that disappear when full executable depth and fees are included.

Duration should therefore affect safety reserves, minimum profit, maximum position size, and permitted proximity to market closure, but it must not change the core requirement that profit be locked through complementary positions rather than predicted direction.

## Required telemetry

Every evaluated opportunity should record:

- Market identifier and outcome-token identifiers.
- Underlying cryptocurrency.
- Scheduled end time and remaining duration.
- Order-book timestamps and data-source latency.
- Candidate quantity.
- Cumulative ask and bid depth used.
- Expected average execution price for each leg.
- Expected and realized fees.
- Expected and realized slippage.
- Submission and acknowledgement latency.
- Fill status and filled quantity for each leg.
- Orphan-leg incidents and recovery actions.
- Merge or split transaction status and cost.
- Expected locked profit.
- Realized locked profit.
- Capital lock duration.
- Maker rebates received after execution.
- Rejection reason for every skipped opportunity.

These observations should be used to refine safety buffers, estimate fill probability, identify unreliable markets, and measure net profitability after all operational costs.

## Performance metrics

The strategy should be evaluated using realized, not theoretical, results:

- Net realized profit after all fees and recovery losses.
- Profit per completed arbitrage.
- Profit per unit of collateral deployed.
- Profit per hour of capital usage.
- Opportunity-to-fill conversion rate.
- Frequency of two-leg completion.
- Orphan-leg rate.
- Average and worst orphan-leg loss.
- Expected-versus-realized slippage.
- Order rejection and timeout rates.
- Merge and split success rates.
- Profitability by market duration bucket.
- Profitability by underlying cryptocurrency.
- Profitability by time remaining at entry.

Backtests must use historical order-book depth, latency assumptions, realistic fees, order-size constraints, and non-atomic leg execution. Midpoint-only or best-price-only backtests are not sufficient evidence that the strategy is profitable.

## Strategy invariant

The strategy's central invariant is:

> No trade is allowed unless the engine can reasonably expect to complete a buy-and-merge or split-and-sell cycle with a strictly positive all-in locked profit after fees, depth, slippage, latency, rounding, operational costs, and orphan-leg risk.

The expanded 24-hour market scope increases opportunity discovery, but it does not convert the strategy into momentum trading, late-window prediction, or any other directional speculation.

## References

- [Polymarket trading fees](https://docs.polymarket.com/trading/fees)
- [Polymarket order creation and execution](https://docs.polymarket.com/developers/CLOB/orders/create-order)
- [Polymarket position and token concepts](https://docs.polymarket.com/concepts/positions-tokens)
- [Conditional Token Framework split operation](https://docs.polymarket.com/developers/CTF/split)
- [Polymarket market details](https://docs.polymarket.com/market-data/market-details)
