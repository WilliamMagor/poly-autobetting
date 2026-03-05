# CLAUDE.md — Architecture Guide for LLM Sessions

> Read this file first when starting a new session. It tells you what matters,
> what's dead, and where to find things.

## What This Bot Does

Automated BTC 15-minute dual-side arbitrage on Polymarket.
Places limit BUY orders on both UP and DOWN outcomes at ~46c each.
If both fill, combined cost <$1 for guaranteed $1 payout = profit.
Rotates to new markets every 15 minutes, runs 24/7.

## The One File That Matters

**`selbot/bot.py`** — the entire bot. ~1050 lines, single file.
This is the only file you need to read/edit for normal operation.

### bot.py Section Map (search for "# ==" to jump between sections)

| Section | What It Does |
|---------|-------------|
| TRADE LOG | JSONL logging with day rotation |
| CONFIG | All constants, env vars, timing |
| HELPERS | Slug builder, validators |
| MARKET API | get_market_info (Gamma API), persistent httpx |
| SDK INIT | CLOB client + on-chain redeemer |
| ORDERS | place_order, sell_at_bid, buy_hedge |
| HEDGE ENGINE | check_hedge_trailing (4-phase trailing stop) |
| ORDER UTILS | get_best_ask_safe, cancel_market_orders |
| BAIL ENGINE | check_bail_out (EV-based bail schedule) |
| ON-CHAIN | merge_market, redeem_market (Polygon txs) |
| BALANCE | check_token_balance (delegated to selbot/prices.py) |
| REDEEM LOOP | try_merge_all, try_redeem_all |
| MAIN LOOP | run(), hedge_loop, market rotation |
| ENTRY POINT | _main() with auto-restart + crash backoff |

### Key Constants (in CONFIG section)

| Constant | Default | What |
|----------|---------|------|
| PRICE | 0.46 | Entry price per side |
| SHARES_PER_SIDE | 10 | Shares per order |
| WINDOW_S | 900 | 15-minute market window |
| HEDGE_START_AFTER_S | 240 | Start hedging at 4 min |
| HEDGE_LATE_AFTER_S | 720 | Emergency zone at 12 min |
| HEDGE_MAX_COMBINED | 0.98 | EV cap for hedge |
| STALE_CANCEL_S | 660 | Cancel unfilled at 11 min |
| BAIL_SCHEDULE | 4 bands | EV-based bail thresholds |

### How Hedge Works (4 phases)

1. **Phase 1 (0-4min):** Detect fills, track trailing floor. No action.
2. **Phase 2 (4-10min):** Cancel unfilled limit, trail with tight band.
3. **Phase 3 (10-12min):** Widen band. Keep trailing.
4. **Phase 4 (12min+):** Force-buy if filled side at 85c+. Sell-filled as last resort.

### Config via Environment

All env vars use `PLACE_15_` prefix.
See `.env.example` for full list.

## Supporting Files

| File | What |
|------|------|
| `selbot/prices.py` | Balance check (check_token_balance) + TTL cache |
| `src/bot/ws_book_feed.py` | WebSocket orderbook feed |
| `src/bot/math_engine.py` | Fee calculation (FeeParams, taker_fee_usdc) |
| `src/bot/types.py` | Data types (PositionState, FillEvent, etc.) |
| `src/bot/fill_monitor.py` | FillDeduplicator for crash-safe fill tracking |
| `scripts/monitor.py` | Live terminal dashboard (reads trade logs) |

## Running the Bot

```bash
venv\Scripts\activate
python selbot/bot.py
```

Auto-restarts on crash with exponential backoff (5s → 120s).
Ctrl+C or SIGTERM for graceful shutdown.

## Trade Logs

Written to `logs/place_15_YYYYMMDD.jsonl`.
Auto-rotates at midnight. One JSON object per event.
Events: `orders_placed`, `fill_detected`, `hedge_trigger`, `hedge_buy`,
`hedge_force_buy`, `hedge_sell_filled`, `bail`, `merge`, `redeem`,
`stale_cancel`, `window_missed`, `crash_restart`.

## Common Tasks for LLM

- **Change entry price:** Edit `PRICE` default in CONFIG section
- **Adjust hedge timing:** Edit `HEDGE_START_AFTER_S` etc. in CONFIG section
- **Adjust bail schedule:** Edit `BAIL_SCHEDULE` in CONFIG section
- **Add new feature:** Add to relevant section per the Section Map above
- **Debug issue:** Read trade log JSONL, check events around the timestamp
