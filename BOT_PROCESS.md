# Bot Process and Tracked Polymarkets

## What This Bot Is Doing

This bot runs a recurring 15-minute trading loop on Polymarket crypto up/down markets.  
It is designed to:

1. Find the current/nearby 15-minute markets for configured symbols.
2. Place entry limit buys (default 10 shares per side at 0.46) on both outcomes.
3. Manage one-sided fills with hedge or buy-hold logic in late stages.
4. Manage risk with bail conditions when a hedge becomes unfavorable.
5. Merge/redeem resolved positions on Polygon after market close.

Main runtime file: `selbot/bot.py`.

---

## Markets Being Tracked

### Symbol Set

The bot tracks symbols from:

- `PLACE_15_SYMBOLS` (default: `btc,eth`)

So by default it tracks **BTC** and **ETH** 15-minute up/down markets.

### Market Slug Format

For each symbol and 15-minute start timestamp:

- `btc-updown-15m-<timestamp>`
- `eth-updown-15m-<timestamp>`

The timestamp is Unix seconds aligned to 900-second boundaries (`WINDOW_S=900`).

### Market Discovery Window

Each loop, the bot checks three timestamps:

- previous window start
- current window start
- next window start

It attempts placement only inside a placement window after market open:

- `PLACE_15_PLACE_WINDOW_S` (default `90` seconds)

---

## End-to-End Process

## 1) Startup and Recovery

On startup, the bot:

- loads env and API clients (Gamma/CLOB/WebSocket/on-chain)
- optionally scans recent markets for open balances
- subscribes market tokens to WebSocket feed
- resumes tracking unresolved positions for redeem/merge

## 2) Placement

For each eligible symbol+timestamp market:

- fetches market metadata and token IDs (`UP` token, `DOWN` token)
- places limit BUY on both sides
- records market state in memory (`past_markets`)
- writes trade log entries in `logs/place_15_YYYYMMDD.jsonl`

Core defaults:

- `PLACE_15_ENTRY_PRICE=0.46`
- `PLACE_15_SHARES=10`

## 3) Fill Monitoring and Position Management

The bot continuously checks token balances and orderbook asks.

### A) Dual-Hedge Mode (default behavior)

Strategy mode:

- `PLACE_15_STRATEGY=dual_hedge` (default)

Behavior:

- if one side fills and the other does not, hedge engine manages timing bands
- late window can force hedge or sell-filled side depending on EV and price bands
- bail logic can exit when unfilled-side ask implies poor expected value

### B) Buy-Hold Late-Stage Mode

Strategy mode:

- `PLACE_15_STRATEGY=buy_hold_91`

Behavior in one-sided situations:

- when remaining time is within `PLACE_15_BUY_HOLD_TRIGGER_REMAINING_S` (default `180`)
- and favored side ask is at/above `PLACE_15_BUY_HOLD_TRIGGER_PRICE` (default `0.91`)
- bot buys the favored side toward `PLACE_15_BUY_HOLD_TARGET_SHARES` (default `110`)

This is directional exposure (not guaranteed arbitrage).

### C) Profit Boost (Both Sides Already Filled)

Independent late-stage boost:

- if both sides are already filled at base size
- and remaining time <= `PLACE_15_PROFIT_BOOST_REMAINING_S` (default `120`)
- and either side ask >= `PLACE_15_PROFIT_BOOST_TRIGGER_PRICE` (default `0.90`)
- buy extra `PLACE_15_PROFIT_BOOST_SHARES` (default `10`) on the stronger side

## 4) Merge and Redeem

Merge (complete sets → USDC) works at any time, not just after close.
Redeem (winning side → USDC) requires market resolution.

A dedicated background loop (`merge_redeem_loop`) handles both operations
with adaptive timing:

- **Aggressive (every 5s):** when a market window ended within the last 2 minutes,
  or when both sides filled during an active market
- **Normal (every 15s):** older pending markets
- **Idle (every 30s):** no pending operations

Merge flow:

- Detects complete sets (both UP + DN tokens held) using fresh balance checks
- Merges immediately — even during the active market window — to free capital
- If both sides had equal balance (ideal case), marks position as fully closed
- Uses exponential backoff on failures (10s, 20s, 40s... up to 5 min)

Redeem flow:

- Starts checking 30 seconds after market window end (time-based heuristic)
- Polls Gamma API for closure status
- If both sides still have balance, defers to merge first
- Redeems one-sided remainder (winning tokens only)
- Uses exponential backoff on failures

---

## Important Environment Variables

Core runtime:

- `PLACE_15_SYMBOLS` (default `btc,eth`)
- `PLACE_15_ENTRY_PRICE` (default `0.46`)
- `PLACE_15_SHARES` (default `10`)
- `PLACE_15_PLACE_WINDOW_S` (default `90`)

Strategy controls:

- `PLACE_15_STRATEGY` (`dual_hedge` or `buy_hold_91`)
- `PLACE_15_BUY_HOLD_TRIGGER_PRICE` (default `0.91`)
- `PLACE_15_BUY_HOLD_TRIGGER_REMAINING_S` (default `180`)
- `PLACE_15_BUY_HOLD_TARGET_SHARES` (default `110`)

Late boost controls:

- `PLACE_15_PROFIT_BOOST_TRIGGER_PRICE` (default `0.90`)
- `PLACE_15_PROFIT_BOOST_REMAINING_S` (default `120`)
- `PLACE_15_PROFIT_BOOST_SHARES` (default `10`)

---

## Operational Notes

- Display off is fine; system sleep/hibernate will stop execution.
- Keep the machine awake and network-stable for continuous operation.
- Monitor live events with `scripts/monitor.py`.
- Review `logs/place_15_YYYYMMDD.jsonl` for post-trade analysis.
