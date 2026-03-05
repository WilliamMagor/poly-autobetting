"""
BTC 15-minute dual-side arbitrage bot for Polymarket (REST-only).

Strategy: Buy both UP and DOWN at ~46c each. If both fill, cost <$1 for
guaranteed $1 payout. Rotates every 15 minutes, runs 24/7.

Unlike place_45.py, this version uses REST API for prices instead of
WebSocket — simpler, more reliable on VPS, fewer dependencies.

Usage: python -m selbot.bot   (from project root)
   or: python selbot/bot.py

SECTION MAP (search for "# ==" to jump between sections):
  == TRADE LOG       JSONL logging with day rotation
  == CONFIG          All constants, env vars, timing
  == HELPERS         Slug builder, timestamp math
  == HEDGE ENGINE    4-phase trailing-stop hedge for one-sided fills
  == BAIL ENGINE     EV-based bail schedule (4 time bands)
  == REDEEM LOOP     try_redeem_all, try_merge_all
  == MAIN LOOP       run(), hedge_loop, market rotation
  == ENTRY POINT     _main() with auto-restart wrapper

HEDGE (one-sided fill):
  Phase 1 (0-4min): Detect fills, track price floor. No action.
  Phase 2 (4-10min): Cancel unfilled limit, trail with tight band.
  Phase 3 (10-12min): Widen band. Keep trailing.
  Phase 4 (12min+): Emergency zone. Force-buy or sell-filled.

BAIL: Sell held side when unfilled side's ask too high for time remaining.
  >5min: never | 2-5min: 0.85 | 30s-2min: 0.75 | <30s: 0.65
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time

# Add project root to path so we can import from src/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from selbot.polymarket import (
    init_clob_client, init_redeemer, get_market_info,
    place_order, sell_at_bid, buy_hedge, cancel_market_orders,
    merge_market, redeem_market, close_http_client,
)
from selbot.prices import (
    get_best_ask, check_token_balance, BalanceCheckError, clear_cache,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("selbot")


# ============================================================================
# == TRADE LOG — JSONL logging with day rotation
# ============================================================================
_trade_log = None
_trade_log_date = ""
_trade_log_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs"
)


def open_trade_log():
    global _trade_log, _trade_log_date
    os.makedirs(_trade_log_dir, exist_ok=True)
    _trade_log_date = time.strftime("%Y%m%d")
    path = os.path.join(_trade_log_dir, f"selbot_{_trade_log_date}.jsonl")
    _trade_log = open(path, "a", buffering=1)
    log.info("Trade log: %s", path)


def _rotate_trade_log_if_needed():
    global _trade_log, _trade_log_date
    today = time.strftime("%Y%m%d")
    if today == _trade_log_date:
        return
    if _trade_log is not None:
        try:
            _trade_log.close()
        except Exception:
            pass
    _trade_log_date = today
    path = os.path.join(_trade_log_dir, f"selbot_{today}.jsonl")
    _trade_log = open(path, "a", buffering=1)
    log.info("Trade log rotated: %s", path)


def log_trade(event: str, **fields):
    if _trade_log is None:
        return
    _rotate_trade_log_if_needed()
    record = {"ts": int(time.time()), "event": event, **fields}
    try:
        _trade_log.write(json.dumps(record) + "\n")
    except Exception as e:
        log.warning("Trade log write failed: %s", e)


# ============================================================================
# == CONFIG — All constants, env vars, timing
# ============================================================================
TIMEFRAME = "15m"
WINDOW_S = 900

def _env(key: str, default: str) -> str:
    return os.getenv(f"SELBOT_{key}", os.getenv(f"PLACE_15_{key}", default))

PRICE = float(_env("ENTRY_PRICE", "0.46"))
SHARES_PER_SIDE = int(_env("SHARES", "10"))
MARKET_PERIOD = WINDOW_S

NEW_MARKET_PLACE_WINDOW_SECONDS = int(_env("PLACE_WINDOW_S", "90"))

HEDGE_START_AFTER_S = int(_env("HEDGE_START_S", "240"))
HEDGE_MID_AFTER_S = int(_env("HEDGE_MID_S", "600"))
HEDGE_LATE_AFTER_S = int(_env("HEDGE_LATE_S", "720"))
HEDGE_BAND_EARLY = 1.0 - PRICE - 0.06
HEDGE_BAND_MID = 1.0 - PRICE - 0.01
HEDGE_BAND_LATE = 1.0 - PRICE
HEDGE_TRAILING_DELTA = float(_env("HEDGE_TRAILING_DELTA", "0.03"))
HEDGE_FORCE_BUY_FILLED_THRESHOLD = float(_env("HEDGE_FORCE_THRESHOLD", "0.85"))
HEDGE_MAX_COMBINED = float(_env("HEDGE_MAX_COMBINED", "0.98"))

BAIL_SCHEDULE = [
    (300, 999.0),   # >5 min remaining: never bail
    (120, 0.85),    # 2-5 min remaining: extreme only
    (30,  0.75),    # 30s-2 min: moderate
    (0,   0.65),    # <30s: emergency exit
]

STALE_CANCEL_S = int(_env("STALE_CANCEL_S", "660"))

FRESH_START = _env("FRESH_START", "0").strip().lower() in ("1", "true", "yes")
MAX_OPEN_MARKETS = int(_env("MAX_OPEN_MARKETS", "0"))

MERGE_REDEEM_INTERVAL_S = 30
NEXT_MARKET_PREFETCH_SEC = 120

# Fee params (BTC crypto markets: feeRate=0.25, exponent=2)
FEE_RATE = 0.25
FEE_EXPONENT = 2


def _taker_fee(shares: float, price: float) -> float:
    """Calculate taker fee for a given trade."""
    if price <= 0 or price >= 1:
        return 0.0
    return shares * price * FEE_RATE * (price * (1 - price)) ** FEE_EXPONENT


# ============================================================================
# == HELPERS — Slug builder, timestamp math
# ============================================================================

def next_market_timestamps(now_ts: int) -> list[int]:
    ts = (now_ts // MARKET_PERIOD) * MARKET_PERIOD
    return [ts - MARKET_PERIOD, ts, ts + MARKET_PERIOD]


def get_market_slug(timestamp: int) -> str:
    return f"btc-updown-15m-{timestamp}"


# ============================================================================
# == HEDGE ENGINE — 4-phase trailing-stop hedge for one-sided fills
# ============================================================================

async def check_hedge_trailing(client, past_markets: dict):
    """Time-windowed trailing-stop hedge for one-sided fills.

    Phases:
      1 (0-240s):   Detect fills, track floor. No action.
      2 (240-600s): Cancel unfilled limit, trail with tight band.
      3 (600-720s): Widen band.
      4 (720s+):    Emergency. Force-buy or sell-filled.
    """
    now = int(time.time())

    for ts, info in list(past_markets.items()):
        if info.get("hedge_done") or info.get("redeemed") or info.get("closed") or info.get("bailed"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        placed_at = info.get("placed_at", 0)
        if not up_token or not dn_token or not placed_at:
            continue

        time_elapsed = now - placed_at
        time_remaining = WINDOW_S - time_elapsed

        # --- Step 1: Detect one-sided fill ---
        if not info.get("hedge_side"):
            try:
                up_bal = check_token_balance(client, up_token)
                dn_bal = check_token_balance(client, dn_token)
            except BalanceCheckError:
                continue

            if up_bal > 0 and dn_bal == 0:
                hedge_side, filled_bal = "dn", up_bal
                filled_side_label = "UP"
            elif dn_bal > 0 and up_bal == 0:
                hedge_side, filled_bal = "up", dn_bal
                filled_side_label = "DN"
            else:
                continue

            log.info(
                "  HEDGE detected one-sided fill at %ds: %s filled, tracking %s",
                time_elapsed, filled_side_label, hedge_side.upper(),
            )
            info["hedge_side"] = hedge_side
            info["filled_bal"] = filled_bal
            info["trailing_min_ask"] = None
            log_trade("fill_detected", market_ts=ts,
                      filled_side=filled_side_label,
                      filled_bal=filled_bal, hedge_side=hedge_side.upper(),
                      elapsed_s=time_elapsed)

        hedge_side: str = info["hedge_side"]
        hedge_token = up_token if hedge_side == "up" else dn_token
        filled_token = dn_token if hedge_side == "up" else up_token
        filled_label = "UP" if hedge_side == "dn" else "DN"

        # --- Step 2: Track trailing minimum ask ---
        hedge_ask = await asyncio.to_thread(get_best_ask, client, hedge_token)
        if hedge_ask is None:
            continue

        prev_min = info.get("trailing_min_ask")
        if prev_min is None or hedge_ask < prev_min:
            info["trailing_min_ask"] = hedge_ask
            if prev_min is not None:
                log.info("  HEDGE trail: new low for %s ask=%.3f",
                         hedge_side.upper(), hedge_ask)

        trailing_min = info["trailing_min_ask"]

        # Phase 1: Grace period
        if time_elapsed < HEDGE_START_AFTER_S:
            continue

        # --- Step 3: Cancel unfilled limit once ---
        if not info.get("hedge_cancelled"):
            log.info("  HEDGE cancelling unfilled %s limit at %ds",
                     hedge_side.upper(), time_elapsed)
            cancel_market_orders(client, {hedge_token})
            info["hedge_cancelled"] = True

        # --- Step 4: Select band based on phase ---
        if time_elapsed >= HEDGE_LATE_AFTER_S:
            band = HEDGE_BAND_LATE
            filled_ask = await asyncio.to_thread(get_best_ask, client, filled_token)
            if filled_ask is not None and filled_ask >= HEDGE_FORCE_BUY_FILLED_THRESHOLD:
                log.info(
                    "  HEDGE FORCE-BUY: filled side ask=%.2f >= %.2f, "
                    "hedge %s ask=%.3f (remaining=%ds)",
                    filled_ask, HEDGE_FORCE_BUY_FILLED_THRESHOLD,
                    hedge_side.upper(), hedge_ask, time_remaining,
                )
                log_trade("hedge_force_buy", market_ts=ts,
                          hedge_side=hedge_side.upper(),
                          filled_ask=filled_ask, hedge_ask=hedge_ask,
                          elapsed_s=time_elapsed, remaining_s=time_remaining)
                buy_hedge(client, hedge_token, SHARES_PER_SIDE,
                          hedge_side.upper(), hedge_ask, WINDOW_S)
                info["hedge_done"] = True
                continue
        elif time_elapsed >= HEDGE_MID_AFTER_S:
            band = HEDGE_BAND_MID
        else:
            band = HEDGE_BAND_EARLY

        # --- Step 5: EV guard ---
        combined = PRICE + hedge_ask
        hedge_fee = _taker_fee(SHARES_PER_SIDE, hedge_ask)
        hedge_ev = SHARES_PER_SIDE * (1.0 - PRICE - hedge_ask) - hedge_fee
        if (combined > HEDGE_MAX_COMBINED or hedge_ev <= 0) and band != HEDGE_BAND_LATE:
            log.info(
                "  HEDGE EV-skip: combined=%.3f cap=%.2f fee=%.4f "
                "EV=%.4f (elapsed=%ds)",
                combined, HEDGE_MAX_COMBINED, hedge_fee,
                hedge_ev, time_elapsed,
            )
            continue

        # --- Step 5b: Valid-bounce check ---
        valid_bounce = (trailing_min is not None
                        and trailing_min < (band - HEDGE_TRAILING_DELTA))

        # --- Step 5c: Sell filled side if hedge uneconomical (late only) ---
        if hedge_ask >= band:
            if valid_bounce:
                log.info(
                    "  HEDGE VALID-BOUNCE: %s ask=%.3f > band=%.2f "
                    "but low=%.3f < band-delta — buying through band",
                    hedge_side.upper(), hedge_ask, band, trailing_min,
                )
            elif time_elapsed >= HEDGE_LATE_AFTER_S:
                try:
                    sell_bal = check_token_balance(client, filled_token)
                except Exception:
                    sell_bal = info.get("filled_bal", SHARES_PER_SIDE)
                if sell_bal <= 0:
                    info["hedge_done"] = True
                    continue
                log.info(
                    "  HEDGE SELL-FILLED: %s ask=%.3f >= band=%.2f at %ds — "
                    "selling %s %.0f (remaining=%ds)",
                    hedge_side.upper(), hedge_ask, band, time_elapsed,
                    filled_label, sell_bal, time_remaining,
                )
                log_trade("hedge_sell_filled", market_ts=ts,
                          hedge_side=hedge_side.upper(),
                          hedge_ask=hedge_ask, band=band,
                          filled_bal=sell_bal,
                          elapsed_s=time_elapsed, remaining_s=time_remaining)
                cancel_market_orders(client, {hedge_token})
                sell_at_bid(client, filled_token, sell_bal, filled_label,
                            window_s=WINDOW_S)
                info["hedge_done"] = True
                info["bailed"] = True
                continue
            else:
                log.info(
                    "  HEDGE wait: %s ask=%.3f >= band=%.2f "
                    "(elapsed=%ds, remaining=%ds)",
                    hedge_side.upper(), hedge_ask, band,
                    time_elapsed, time_remaining,
                )
                continue

        # --- Step 6: Dynamic bounce threshold ---
        span = max(1, HEDGE_LATE_AFTER_S - HEDGE_START_AFTER_S)
        progress = min(1.0, (time_elapsed - HEDGE_START_AFTER_S) / span)
        min_bounce = HEDGE_TRAILING_DELTA * (1.0 - progress * 0.67)

        # --- Step 7: Trailing-stop trigger ---
        if trailing_min is None:
            continue
        bounce = hedge_ask - trailing_min
        log.info(
            "  HEDGE trail: %s ask=%.3f low=%.3f bounce=%.3f/%.3f "
            "(elapsed=%ds band=%.2f remaining=%ds)",
            hedge_side.upper(), hedge_ask, trailing_min,
            bounce, min_bounce, time_elapsed, band, time_remaining,
        )
        triggered = bounce >= min_bounce or valid_bounce
        if triggered:
            reason = "valid_bounce" if valid_bounce else "trailing"
            log.info(
                "  HEDGE TRIGGER: %s bounced %.3f -> %.3f "
                "(delta=%.3f >= %.3f%s)",
                hedge_side.upper(), trailing_min, hedge_ask,
                bounce, min_bounce,
                ", valid-bounce" if valid_bounce else "",
            )
            log_trade("hedge_trigger", market_ts=ts,
                      hedge_side=hedge_side.upper(),
                      trail_min=round(trailing_min, 4),
                      ask=round(hedge_ask, 4),
                      bounce=round(bounce, 4),
                      min_bounce=round(min_bounce, 4),
                      band=band, reason=reason,
                      elapsed_s=time_elapsed,
                      remaining_s=time_remaining)
            try:
                current_bal = check_token_balance(client, hedge_token)
            except Exception:
                continue
            if current_bal >= SHARES_PER_SIDE - 0.01:
                log.info(
                    "  HEDGE skipped: already hold %.1f %s shares",
                    current_bal, hedge_side.upper(),
                )
                info["hedge_done"] = True
                continue
            oid = buy_hedge(client, hedge_token, SHARES_PER_SIDE,
                            hedge_side.upper(), hedge_ask, WINDOW_S)
            log_trade("hedge_buy", market_ts=ts,
                      side=hedge_side.upper(),
                      price=round(hedge_ask, 4),
                      shares=SHARES_PER_SIDE,
                      order_id=oid, elapsed_s=time_elapsed,
                      remaining_s=time_remaining)
            info["hedge_done"] = True


# ============================================================================
# == BAIL ENGINE — EV-based bail schedule (4 time bands)
# ============================================================================

async def check_bail_out(client, past_markets: dict):
    """EV-based bail: sell held side when unfilled side's ask exceeds
    the time-based threshold."""
    now = int(time.time())

    for ts, info in list(past_markets.items()):
        if info.get("bailed") or info.get("redeemed") or info.get("hedge_done"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        if not up_token or not dn_token:
            continue
        if info.get("closed"):
            continue

        placed_at = info.get("placed_at", 0)
        if not placed_at:
            continue

        time_elapsed = now - placed_at
        time_remaining = WINDOW_S - time_elapsed

        bail_price = 999.0
        for threshold_remaining, threshold_price in BAIL_SCHEDULE:
            if time_remaining >= threshold_remaining:
                bail_price = threshold_price
                break

        if bail_price >= 999.0:
            continue

        up_ask = await asyncio.to_thread(get_best_ask, client, up_token)
        dn_ask = await asyncio.to_thread(get_best_ask, client, dn_token)
        if up_ask is None or dn_ask is None:
            continue

        if up_ask <= bail_price and dn_ask <= bail_price:
            continue

        try:
            up_bal = check_token_balance(client, up_token)
            dn_bal = check_token_balance(client, dn_token)
        except Exception:
            continue

        bail = False
        if dn_ask > bail_price and dn_bal == 0 and up_bal > 0:
            log.info("  BAIL: DN ask=%.2f > %.2f (remaining=%ds). "
                     "Selling UP %.0f shares",
                     dn_ask, bail_price, time_remaining, up_bal)
            log_trade("bail", market_ts=ts, trigger_side="DN",
                      trigger_ask=dn_ask, bail_price=bail_price,
                      sell_side="UP", sell_bal=up_bal,
                      remaining_s=time_remaining)
            cancel_market_orders(client, {up_token, dn_token})
            sell_at_bid(client, up_token, up_bal, "UP", window_s=WINDOW_S)
            bail = True
        elif up_ask > bail_price and up_bal == 0 and dn_bal > 0:
            log.info("  BAIL: UP ask=%.2f > %.2f (remaining=%ds). "
                     "Selling DN %.0f shares",
                     up_ask, bail_price, time_remaining, dn_bal)
            log_trade("bail", market_ts=ts, trigger_side="UP",
                      trigger_ask=up_ask, bail_price=bail_price,
                      sell_side="DN", sell_bal=dn_bal,
                      remaining_s=time_remaining)
            cancel_market_orders(client, {up_token, dn_token})
            sell_at_bid(client, dn_token, dn_bal, "DN", window_s=WINDOW_S)
            bail = True

        if bail:
            info["bailed"] = True


# ============================================================================
# == REDEEM LOOP — try_redeem_all, try_merge_all
# ============================================================================

async def try_merge_all(client, redeemer, past_markets: dict):
    """Merge complete sets (UP + DN) into USDC after market closes."""
    if not redeemer:
        return

    merged = []
    for ts, info in list(past_markets.items()):
        if info.get("redeemed"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        condition_id = info.get("condition_id", "")
        title = info.get("title", get_market_slug(ts))

        if not info.get("closed"):
            slug = get_market_slug(ts)
            try:
                mkt = await get_market_info(slug, WINDOW_S)
            except Exception:
                continue
            if not mkt["closed"]:
                continue
            info["closed"] = True
            if not condition_id:
                condition_id = mkt["conditionId"]
                info["condition_id"] = condition_id
            if not up_token:
                up_token = mkt["up_token"]
                info["up_token"] = up_token
            if not dn_token:
                dn_token = mkt["dn_token"]
                info["dn_token"] = dn_token

        if not condition_id:
            continue

        try:
            up_bal = check_token_balance(client, up_token)
            dn_bal = check_token_balance(client, dn_token)
        except Exception:
            continue
        merge_amount = min(up_bal, dn_bal)
        if merge_amount <= 0:
            continue

        merge_fails = info.get("merge_fails", 0)
        if merge_fails >= 5:
            continue

        log.info("  Merging %s (%.1f complete sets)...", title[:50], merge_amount)
        ok = await asyncio.to_thread(merge_market, redeemer, condition_id, merge_amount)
        log_trade("merge", market_ts=ts, title=title, sets=merge_amount, ok=ok)
        if ok:
            merged.append(ts)
        else:
            info["merge_fails"] = merge_fails + 1
            if merge_fails + 1 >= 5:
                log.warning("  Merge gave up on %s after 5 failures", title[:50])

    if merged:
        log.info("  Merged %d market(s)", len(merged))


async def try_redeem_all(client, redeemer, past_markets: dict):
    """Redeem resolved positions."""
    if not redeemer:
        return

    redeemed = []
    for ts, info in list(past_markets.items()):
        if info.get("redeemed"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        condition_id = info.get("condition_id", "")
        neg_risk = info.get("neg_risk", False)
        title = info.get("title", get_market_slug(ts))

        if not info.get("closed"):
            slug = get_market_slug(ts)
            try:
                mkt = await get_market_info(slug, WINDOW_S)
            except Exception:
                continue
            if not mkt["closed"]:
                continue
            info["closed"] = True
            if not condition_id:
                condition_id = mkt["conditionId"]
                info["condition_id"] = condition_id
            if not neg_risk:
                neg_risk = mkt.get("neg_risk", False)
                info["neg_risk"] = neg_risk
            if not up_token:
                up_token = mkt["up_token"]
                info["up_token"] = up_token
            if not dn_token:
                dn_token = mkt["dn_token"]
                info["dn_token"] = dn_token

        if not condition_id:
            log.warning("  No conditionId for %s — cannot redeem", title[:40])
            continue

        try:
            up_bal = check_token_balance(client, up_token)
            dn_bal = check_token_balance(client, dn_token)
        except Exception:
            continue

        if up_bal <= 0 and dn_bal <= 0:
            info["redeemed"] = True
            continue

        redeem_fails = info.get("redeem_fails", 0)
        if redeem_fails >= 5:
            continue

        log.info("  Redeeming %s (UP=%.1f, DN=%.1f)...", title[:50], up_bal, dn_bal)
        ok = await asyncio.to_thread(
            redeem_market, redeemer, condition_id, neg_risk, up_bal, dn_bal
        )
        log_trade("redeem", market_ts=ts, title=title,
                  up_bal=up_bal, dn_bal=dn_bal, ok=ok)
        if ok:
            info["redeemed"] = True
            redeemed.append(ts)
        else:
            info["redeem_fails"] = redeem_fails + 1
            if redeem_fails + 1 >= 5:
                log.warning("  Redeem gave up on %s after 5 failures", title[:50])

    if redeemed:
        log.info("  Redeemed %d market(s)", len(redeemed))


# ============================================================================
# == MAIN LOOP — run(), hedge_loop, market rotation
# ============================================================================

_shutdown = False


async def hedge_loop(client, past_markets: dict):
    """Background 1-second loop for trailing hedge checks."""
    while not _shutdown:
        try:
            await check_hedge_trailing(client, past_markets)
        except Exception as e:
            log.debug("hedge_loop error: %s", e)
        await asyncio.sleep(1)


async def run():
    global _shutdown

    open_trade_log()
    log.info("Initializing SDK...")
    client = init_clob_client()
    redeemer = init_redeemer()

    log.info("SDK ready. Placing %.0fc orders on BTC %s markets.",
             PRICE * 100, TIMEFRAME)
    log.info("  Fee params: rate=%.2f exponent=%.0f (max eff. rate ~1.56%% at 50c)",
             FEE_RATE, FEE_EXPONENT)
    if redeemer:
        log.info("Auto-redeem enabled (on-chain via %s).\n", redeemer[1].address)
    else:
        log.info("Auto-redeem DISABLED (no private key / RPC).\n")

    if FRESH_START:
        log.info("FRESH_START=1: clearing all caches.")
        clear_cache()

    placed_markets = set()
    past_markets = {}
    last_merge_redeem_ts = 0

    asyncio.create_task(hedge_loop(client, past_markets))

    # Scan recent markets for unredeemed positions
    now = int(time.time())
    if FRESH_START:
        log.info("Startup scan skipped (FRESH_START=1). Starting clean.")
    else:
        log.info("Scanning recent markets for unredeemed positions...")
        scan_ts = (now - 7200) // MARKET_PERIOD * MARKET_PERIOD
        while scan_ts < now:
            slug = get_market_slug(scan_ts)
            try:
                mkt = await get_market_info(slug, WINDOW_S)
                past_markets[scan_ts] = {
                    "redeemed": False,
                    "closed": mkt["closed"],
                    "up_token": mkt["up_token"],
                    "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"],
                    "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug),
                    "placed_at": 0 if mkt["closed"] else scan_ts,
                }
                secs_since_open = now - scan_ts
                if mkt["closed"] or secs_since_open > NEW_MARKET_PLACE_WINDOW_SECONDS:
                    placed_markets.add(scan_ts)
            except Exception:
                pass
            scan_ts += MARKET_PERIOD
        log.info("Found %d recent markets to track.\n", len(past_markets))

    next_market_cache: dict = {"ts": None, "mkt": None}

    async def prefetch_next_market(next_ts: int, cache: dict):
        if cache.get("ts") == next_ts:
            return
        try:
            mkt = await get_market_info(get_market_slug(next_ts), WINDOW_S)
            cache["ts"] = next_ts
            cache["mkt"] = mkt
        except Exception:
            pass

    while not _shutdown:
        now = int(time.time())
        timestamps = next_market_timestamps(now)
        next_market_ts = (now // MARKET_PERIOD) * MARKET_PERIOD + MARKET_PERIOD

        for ts in timestamps:
            if ts in placed_markets:
                continue

            slug = get_market_slug(ts)

            if (ts == next_market_ts
                    and next_market_cache.get("ts") == ts
                    and next_market_cache.get("mkt")):
                mkt = next_market_cache["mkt"]
                next_market_cache["ts"] = None
                next_market_cache["mkt"] = None
            else:
                try:
                    mkt = await get_market_info(slug, WINDOW_S)
                except Exception as e:
                    secs_until_ts = ts - now
                    if secs_until_ts <= 60:
                        log.info("Market %s lookup failed (%ds): %s",
                                 slug, secs_until_ts, e)
                    continue

            secs_until = ts - now

            log.info("=" * 60)
            log.info("MARKET: %s", mkt["title"])
            log.info("  Slug: %s", slug)
            log.info("  Starts in: %ds", max(0, secs_until))
            log.info("  UP token:  %s...", mkt["up_token"][:20])
            log.info("  DN token:  %s...", mkt["dn_token"][:20])
            log.info("")

            # Balance + live orders in parallel
            def _get_orders():
                try:
                    return client.get_orders()
                except Exception:
                    return []

            def _safe_balance(token_id):
                try:
                    return check_token_balance(client, token_id)
                except Exception:
                    return 0.0

            up_bal, dn_bal, live_orders = await asyncio.gather(
                asyncio.to_thread(_safe_balance, mkt["up_token"]),
                asyncio.to_thread(_safe_balance, mkt["dn_token"]),
                asyncio.to_thread(_get_orders),
            )

            if up_bal > 0 or dn_bal > 0:
                log.info("  Already have position (UP=%.1f, DN=%.1f) — skipping",
                         up_bal, dn_bal)
                placed_markets.add(ts)
                past_markets[ts] = {
                    "redeemed": False, "closed": mkt["closed"],
                    "up_token": mkt["up_token"], "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"],
                    "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug),
                    "placed_at": ts,
                }
                continue

            market_tokens = {mkt["up_token"], mkt["dn_token"]}
            existing = [o for o in (live_orders or [])
                        if o.get("status") == "LIVE"
                        and o.get("asset_id") in market_tokens]
            if existing:
                log.info("  Already have %d live order(s) — skipping",
                         len(existing))
                placed_markets.add(ts)
                past_markets[ts] = {
                    "redeemed": False, "closed": mkt["closed"],
                    "up_token": mkt["up_token"], "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"],
                    "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug),
                    "placed_at": ts,
                }
                continue

            secs_elapsed = now - ts
            if secs_elapsed > NEW_MARKET_PLACE_WINDOW_SECONDS:
                log.info("  Skipping — outside %ds window (%ds elapsed)",
                         NEW_MARKET_PLACE_WINDOW_SECONDS, secs_elapsed)
                log_trade("window_missed", market_ts=ts, slug=slug,
                          elapsed_s=secs_elapsed)
                placed_markets.add(ts)
                past_markets[ts] = {
                    "redeemed": False, "closed": mkt["closed"],
                    "up_token": mkt["up_token"], "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"],
                    "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug),
                    "placed_at": 0,
                }
                continue

            if MAX_OPEN_MARKETS > 0:
                open_count = sum(
                    1 for _ts, _info in past_markets.items()
                    if not _info.get("redeemed")
                    and not _info.get("bailed")
                    and not _info.get("closed")
                )
                if open_count >= MAX_OPEN_MARKETS:
                    log.info("  Skipping — at max open markets (%d >= %d)",
                             open_count, MAX_OPEN_MARKETS)
                    continue

            entry_price = PRICE

            up_id, dn_id = await asyncio.gather(
                asyncio.to_thread(place_order, client, mkt["up_token"],
                                  "UP  ", SHARES_PER_SIDE, entry_price, WINDOW_S),
                asyncio.to_thread(place_order, client, mkt["dn_token"],
                                  "DOWN", SHARES_PER_SIDE, entry_price, WINDOW_S),
            )

            placed = (1 if up_id else 0) + (1 if dn_id else 0)
            log.info("  Done: %d orders placed. Max cost: $%.2f",
                     placed, SHARES_PER_SIDE * entry_price * 2)
            log.info("")
            log_trade("orders_placed", market_ts=ts, slug=slug,
                      title=mkt.get("title", slug), price=entry_price,
                      shares=SHARES_PER_SIDE,
                      up_id=up_id, dn_id=dn_id, sides_placed=placed,
                      elapsed_s=now - ts)

            if placed == 0:
                log.info("  Both orders failed — will retry next loop.")
                continue

            placed_markets.add(ts)
            past_markets[ts] = {
                "redeemed": False,
                "closed": mkt["closed"],
                "up_token": mkt["up_token"],
                "dn_token": mkt["dn_token"],
                "condition_id": mkt["conditionId"],
                "neg_risk": mkt.get("neg_risk", False),
                "title": mkt.get("title", slug),
                "placed_at": ts,
            }

        # Log REST prices for active markets
        for ts, info in past_markets.items():
            if info.get("redeemed") or info.get("closed"):
                continue
            up_ask = await asyncio.to_thread(
                get_best_ask, client, info.get("up_token", "")
            )
            dn_ask = await asyncio.to_thread(
                get_best_ask, client, info.get("dn_token", "")
            )
            if up_ask is not None or dn_ask is not None:
                elapsed = now - info.get("placed_at", now)
                hedge_state = ""
                if info.get("hedge_done"):
                    hedge_state = " [HEDGED]"
                elif info.get("hedge_side"):
                    hedge_state = f" [TRAILING {info['hedge_side'].upper()}]"
                log.info(
                    "  [%d +%ds] REST: UP=%.2f  DN=%.2f%s",
                    ts, elapsed, up_ask or 0, dn_ask or 0, hedge_state,
                )

        # Stale cancel (11 min into market)
        for _ts, _info in past_markets.items():
            if _info.get("redeemed") or _info.get("closed") or _info.get("bailed"):
                continue
            _placed = _info.get("placed_at", 0)
            if (_placed and (now - _placed) >= STALE_CANCEL_S
                    and not _info.get("stale_cancelled")):
                _tokens = set()
                if _info.get("up_token"):
                    _tokens.add(_info["up_token"])
                if _info.get("dn_token"):
                    _tokens.add(_info["dn_token"])
                if _tokens:
                    log.info("  STALE-CANCEL: market %d (%ds elapsed)",
                             _ts, now - _placed)
                    cancel_market_orders(client, _tokens)
                    _info["stale_cancelled"] = True
                    log_trade("stale_cancel", market_ts=_ts,
                              elapsed_s=now - _placed)

        # Bail check
        await check_bail_out(client, past_markets)

        # Merge + redeem (throttled to every 30s)
        if now - last_merge_redeem_ts >= MERGE_REDEEM_INTERVAL_S:
            await try_merge_all(client, redeemer, past_markets)
            await try_redeem_all(client, redeemer, past_markets)
            last_merge_redeem_ts = now

        # Adaptive sleep
        seconds_until = next_market_ts - now
        sleep_s = 0.5 if seconds_until <= 30 else (1 if seconds_until <= 120 else 3)

        if 0 < seconds_until <= NEXT_MARKET_PREFETCH_SEC:
            asyncio.create_task(
                prefetch_next_market(next_market_ts, next_market_cache)
            )
        await asyncio.sleep(sleep_s)

        log.info("[Next market in %ds] Tracking %d markets",
                 seconds_until, len(past_markets))

        # Cleanup
        HARD_TTL_S = 21600  # 6 hours
        NORMAL_TTL_S = 7200  # 2 hours
        stale = []
        for ts, info in past_markets.items():
            age = now - ts
            if age > HARD_TTL_S:
                if not info.get("redeemed"):
                    log.warning("  Hard TTL: dropping market %d (%ds old)",
                                ts, age)
                    log_trade("hard_ttl_drop", market_ts=ts, age_s=age)
                stale.append(ts)
            elif age > NORMAL_TTL_S and info.get("redeemed"):
                stale.append(ts)
        for ts in stale:
            del past_markets[ts]
        placed_markets = {ts for ts in placed_markets
                          if ts > now - 2 * MARKET_PERIOD}

    # Clean shutdown
    log.info("Shutting down...")
    await close_http_client()


# ============================================================================
# == ENTRY POINT — _main() with auto-restart wrapper
# ============================================================================

def _main():
    """Entry point with auto-restart on crash (24/7 operation)."""
    global _shutdown

    def _signal_handler(sig, frame):
        global _shutdown
        _shutdown = True
        log.info("\nShutdown requested (signal %s).", sig)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    backoff = 5
    while not _shutdown:
        try:
            asyncio.run(run())
            break
        except KeyboardInterrupt:
            log.info("\nStopped.")
            break
        except SystemExit:
            break
        except Exception as e:
            log.error("Bot crashed: %s — restarting in %ds...", e, backoff)
            log_trade("crash_restart", error=str(e), backoff_s=backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)

    if _trade_log is not None:
        try:
            _trade_log.close()
        except Exception:
            pass
    log.info("Bot exited.")


if __name__ == "__main__":
    _main()
