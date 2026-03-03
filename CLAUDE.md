# CLAUDE.md — Architecture Guide for LLM Sessions

> Read this file first when starting a new session. It tells you what matters,
> what's dead, and where to find things.

## What This Bot Does

Automated BTC 15-minute prediction market arbitrage on Polymarket.
Places limit BUY orders on both UP and DOWN outcomes at ~46c each.
If both fill, combined cost <$1 for guaranteed $1 payout = profit.
Rotates to new markets every 15 minutes, 24/7.

## The One File That Matters

**`scripts/place_45.py`** — the entire bot. ~1350 lines, single file.
This is the only file you need to read/edit for normal operation.
It imports only `WSBookFeed` from `src/bot/ws_book_feed.py`.

### place_45.py Section Map (line numbers are approximate)

Search for `# ==` to jump between sections. Line numbers are approximate.

| Section | What It Does |
|---------|-------------|
| L1-56 | Module docstring (section map + hedge/bail summary) + imports |
| L60-100 | **TRADE LOG** — JSONL logging with day rotation |
| L104-175 | **CONFIG** — all constants, env vars, timing |
| L177-215 | **HELPERS** — slug builder, validators |
| L217-315 | **MARKET API** — get_market_info (Gamma API), persistent httpx |
| L318-365 | **SDK INIT** — CLOB client + on-chain redeemer |
| L367-470 | **ORDERS** — place_order, sell_at_bid, buy_hedge |
| L473-685 | **HEDGE ENGINE** — check_hedge_trailing (4-phase trailing stop) |
| L689-720 | **ORDER UTILS** — get_best_ask_safe, cancel_market_orders |
| L725-805 | **BAIL ENGINE** — check_bail_out (EV-based bail schedule) |
| L807-910 | **ON-CHAIN** — merge_market, redeem_market (Polygon txs) |
| L911-955 | **BALANCE** — check_token_balance + TTL cache |
| L957-1090 | **REDEEM LOOP** — try_redeem_all, try_merge_all |
| L1093-1425 | **MAIN LOOP** — run(), hedge_loop, market rotation |
| L1426-1470 | **ENTRY POINT** — _main() with auto-restart + crash backoff |

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

1. **Phase 1 (0-240s):** Detect fills, track trailing floor. No action.
2. **Phase 2 (240-600s):** Cancel unfilled limit, trail with tight band (0.48).
3. **Phase 3 (600-720s):** Widen band (0.53). Keep trailing.
4. **Phase 4 (720s+):** Wide band (0.54). Force-buy if filled side at 85c+. Sell-filled as last resort.

### Config via Environment

All env vars use `PLACE_15_` prefix with `PLACE_45_` fallback.
See `.env.example` for full list.

## Supporting Files

| File | Used? | What |
|------|-------|------|
| `src/bot/ws_book_feed.py` | **YES** | WebSocket orderbook feed (imported by place_45.py) |
| `src/config.py` | no | Fee constants (place_45.py has its own) |
| `src/analysis/calculator.py` | no | P&L calculator (standalone analysis tool) |

## Dead Code (not imported by anything)

These exist for reference/analysis but are NOT used by the bot:

| File | What It Was |
|------|-------------|
| `src/bot/runner.py` (5000 lines!) | Separate bot framework (penny ladder strategies) |
| `src/bot/client.py` | Exchange adapter (not used by place_45.py) |
| `src/bot/order_engine.py` | Ladder builder (not used by place_45.py) |
| `src/bot/risk_engine.py` | Circuit breakers (not used by place_45.py) |
| `src/bot/fill_monitor.py` | Fill detection (not used by place_45.py) |
| `src/bot/position_tracker.py` | Position tracking (not used by place_45.py) |
| `src/bot/market_scheduler.py` | Market discovery (not used by place_45.py) |
| `src/bot/session_loop.py` | Session primitives (not used by place_45.py) |
| `src/bot/rebalance.py` | Imbalance logic (not used by place_45.py) |
| `src/bot/state_manager.py` | State persistence (not used by place_45.py) |
| `src/bot/math_engine.py` | Math formulas (not used by place_45.py) |
| `src/bot/alerts.py` | Telegram alerts (not used by place_45.py) |
| `src/bot/backtest.py` | Backtesting (not used by place_45.py) |
| `src/bot/bot_config.py` | Config loader (not used by place_45.py) |
| `src/bot/types.py` | Data structures (not used by place_45.py) |
| `src/api/data_api.py` | User trade history (unused) |
| `src/api/polygonscan.py` | On-chain analytics (unused) |
| `src/api/gamma.py` | Market metadata (place_45.py has inline version) |
| `src/api/clob.py` | Orderbook API (place_45.py uses SDK directly) |
| `src/analysis/strategy.py` | Trade pairing analysis (unused) |
| `src/analysis/activity_watch.py` | Activity dedup (unused) |
| `src/monitor/orderbook.py` | Old WS monitor (superseded by ws_book_feed.py) |
| `src/monitor/spreads.py` | Spread scanner (standalone tool) |

## Running the Bot

```bash
source venv/bin/activate
python scripts/place_45.py
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

- **Change entry price:** Edit `PRICE` default in CONFIG section (~L81)
- **Adjust hedge timing:** Edit `HEDGE_START_AFTER_S` etc. in CONFIG section (~L98-114)
- **Adjust bail schedule:** Edit `BAIL_SCHEDULE` in CONFIG section (~L116-125)
- **Add new feature:** Add to relevant section per the Section Map above
- **Debug issue:** Read trade log JSONL, check events around the timestamp
- **Check strategy math:** Run `python src/analysis/calculator.py`
