"""
BTC 15-minute dual-side arbitrage bot for Polymarket — fully API-driven.

Strategy: Buy both UP and DOWN at ~46c each. If both fill, cost <$1 for
guaranteed $1 payout. Rotates every 15 minutes, runs 24/7.

Orders placed via CLOB API (signed with private key).
Merge/redeem via Builder Relayer (gasless, through proxy wallet).
Real-time prices via WebSocket feed.

Usage:
    cd C:\\Users\\123\\poly-autobetting
    venv\\Scripts\\python.exe selbot/bot.py

SECTION MAP (search for "# ==" to jump between sections):
  == TRADE LOG       JSONL logging with day rotation
  == CONFIG          All constants, env vars, timing
  == HELPERS         Slug builder, validators
  == MARKET API      get_market_info (Gamma API), httpx client
  == SDK INIT        CLOB client + Builder Relayer setup
  == ORDERS          place_order, sell_at_bid, buy_hedge
  == HEDGE ENGINE    4-phase trailing-stop hedge for one-sided fills
  == ORDER UTILS     get_best_ask_safe, cancel_market_orders
  == BAIL ENGINE     EV-based bail schedule (4 time bands)
  == ON-CHAIN        merge_market_api, redeem_market_api (Builder Relayer)
  == BALANCE         check_token_balance + TTL cache (in prices.py)
  == REDEEM LOOP     try_merge_all, try_redeem_all
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
import re
import signal
import sys
import time

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.bot.ws_book_feed import WSBookFeed
from src.bot.math_engine import FeeParams, taker_fee_usdc, effective_fee_rate
from src.bot.types import PositionState
from src.bot.fill_monitor import FillDeduplicator

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("selbot")


# ============================================================================
# == TRADE LOG — JSONL logging with day rotation
# ============================================================================

_trade_log = None
_trade_log_date = ""
_trade_log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")


def open_trade_log():
    global _trade_log, _trade_log_date
    os.makedirs(_trade_log_dir, exist_ok=True)
    _trade_log_date = time.strftime("%Y%m%d")
    path = os.path.join(_trade_log_dir, f"place_15_{_trade_log_date}.jsonl")
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
    path = os.path.join(_trade_log_dir, f"place_15_{today}.jsonl")
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

WINDOW_S = 900
MARKET_PERIOD = WINDOW_S
GAMMA_URL = "https://gamma-api.polymarket.com"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"


def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


PRICE = float(_env("PLACE_15_ENTRY_PRICE", "0.46"))
SHARES_PER_SIDE = int(_env("PLACE_15_SHARES", "10"))
NEW_MARKET_PLACE_WINDOW_SECONDS = int(_env("PLACE_15_PLACE_WINDOW_S", "90"))

ENTRY_PRICE_DYNAMIC = _env("PLACE_15_ENTRY_PRICE_DYNAMIC", "0").strip().lower() in ("1", "true", "yes")
ENTRY_PRICE_VOL_HIGH = float(_env("PLACE_15_ENTRY_VOL_HIGH", "0.45"))
ENTRY_PRICE_VOL_LOW = float(_env("PLACE_15_ENTRY_VOL_LOW", "0.47"))

HEDGE_START_AFTER_S = int(_env("PLACE_15_HEDGE_START_S", "240"))
HEDGE_MID_AFTER_S = int(_env("PLACE_15_HEDGE_MID_S", "600"))
HEDGE_LATE_AFTER_S = int(_env("PLACE_15_HEDGE_LATE_S", "720"))
HEDGE_BAND_EARLY = 1.0 - PRICE - 0.06
HEDGE_BAND_MID = 1.0 - PRICE - 0.01
HEDGE_BAND_LATE = 1.0 - PRICE
HEDGE_TRAILING_DELTA = float(_env("PLACE_15_HEDGE_TRAILING_DELTA", "0.03"))
HEDGE_FORCE_BUY_FILLED_THRESHOLD = float(_env("PLACE_15_HEDGE_FORCE_THRESHOLD", "0.85"))
HEDGE_MAX_COMBINED = float(_env("PLACE_15_HEDGE_MAX_COMBINED", "0.98"))
HEDGE_USE_REST_FALLBACK = _env("PLACE_15_REST_FALLBACK", "0").strip().lower() in ("1", "true", "yes")

STALE_CANCEL_S = int(_env("PLACE_15_STALE_CANCEL_S", "660"))

BAIL_SCHEDULE = [
    (300, 999.0),   # >5 min remaining: never bail
    (120, 0.85),    # 2-5 min remaining: extreme only
    (30,  0.75),    # 30s-2 min remaining: moderate
    (0,   0.65),    # <30s remaining: emergency exit
]

FRESH_START = _env("PLACE_15_FRESH_START", "0").strip().lower() in ("1", "true", "yes")
MAX_OPEN_MARKETS = int(_env("PLACE_15_MAX_OPEN_MARKETS", "0"))
NEXT_MARKET_PREFETCH_SEC = 120
MERGE_REDEEM_INTERVAL_S = 30

_FEE_PARAMS = FeeParams.for_crypto()
_FILL_DEDUP: FillDeduplicator | None = None
_SLUG_RE = re.compile(r"^btc-updown-15m-\d+$")

# Persistent httpx client
_http_client: httpx.AsyncClient | None = None


# ============================================================================
# == HELPERS — Slug builder, validators
# ============================================================================


def next_market_timestamps(now_ts: int) -> list[int]:
    ts = (now_ts // MARKET_PERIOD) * MARKET_PERIOD
    return [ts - MARKET_PERIOD, ts, ts + MARKET_PERIOD]


def get_market_slug(timestamp: int) -> str:
    return f"btc-updown-15m-{timestamp}"


def _validate_token_id(token_id: str, field_name: str) -> str:
    if not token_id or not isinstance(token_id, str):
        raise ValueError(f"Invalid {field_name}: expected non-empty string, got {token_id}")
    if not token_id.isdigit():
        raise ValueError(f"Invalid {field_name}: expected numeric string, got {token_id}")
    return token_id


# ============================================================================
# == MARKET API — get_market_info (Gamma API), httpx client
# ============================================================================


def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=15,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://polymarket.com/",
                "Origin": "https://polymarket.com",
            },
        )
    return _http_client


def _check_json_response(r) -> None:
    ct = r.headers.get("content-type", "")
    body_start = r.text[:100].lstrip()
    if "html" in ct.lower() or body_start.startswith("<!"):
        raise ValueError(
            f"Gamma API returned HTML instead of JSON "
            f"(Cloudflare block?). status={r.status_code} body={r.text[:120]!r}"
        )


def _parse_gamma_market(m: dict) -> dict | None:
    from datetime import datetime
    tokens_raw = m.get("clobTokenIds", [])
    tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
    if not isinstance(tokens, list) or len(tokens) < 2:
        return None
    condition_id = m.get("conditionId", "")
    if not condition_id or not condition_id.startswith("0x") or len(condition_id) < 64:
        return None
    return {
        "up_token": str(tokens[0]),
        "dn_token": str(tokens[1]),
        "title": m.get("question", m.get("slug", "")),
        "conditionId": condition_id,
        "closed": bool(m.get("closed", False)),
        "neg_risk": bool(m.get("negRisk", False)),
    }


async def _fallback_market_scan(ts: int) -> dict | None:
    from datetime import datetime
    c = _get_http_client()
    try:
        r = await c.get(f"{GAMMA_URL}/markets", params={
            "limit": 200, "active": "true", "closed": "false",
            "order": "endDate", "ascending": "true",
        })
        r.raise_for_status()
        _check_json_response(r)
        markets = r.json()
    except Exception as e:
        log.warning("Fallback market scan failed: %s", e)
        return None

    for m in markets:
        question = str(m.get("question", "")).lower()
        mslug = str(m.get("slug", "")).lower()
        if not (("btc" in question or "btc" in mslug) and ("15m" in mslug or "updown" in mslug)):
            continue
        start_date = m.get("eventStartTime") or m.get("startDate") or ""
        end_date = m.get("endDate") or ""
        if not start_date or not end_date:
            continue
        try:
            t_start = datetime.fromisoformat(start_date.replace("Z", "+00:00")).timestamp()
            t_end = datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        if abs(t_end - t_start - WINDOW_S) > 60 or abs(int(t_start) - ts) > 60:
            continue
        result = _parse_gamma_market(m)
        if result:
            log.info("Fallback market discovery: %s", m.get("question", mslug)[:80])
            return result
    return None


async def get_market_info(slug: str) -> dict:
    if not slug or not _SLUG_RE.match(slug):
        raise ValueError(f"Invalid slug format: {slug}")
    c = _get_http_client()
    r = await c.get(f"{GAMMA_URL}/events", params={"slug": slug})
    r.raise_for_status()
    _check_json_response(r)
    data = r.json()

    if not data:
        ts = int(slug.rsplit("-", 1)[-1])
        log.info("Slug %s not indexed yet — trying fallback scan", slug)
        fallback = await _fallback_market_scan(ts)
        if fallback:
            return fallback
        raise ValueError(f"No event found for slug: {slug}")

    if not isinstance(data, list) or not data or "markets" not in data[0]:
        raise ValueError(f"Invalid API response for {slug}")
    markets = data[0].get("markets", [])
    if not markets:
        raise ValueError(f"Empty markets list for {slug}")

    m = markets[0]
    if "clobTokenIds" not in m:
        raise ValueError(f"Market missing 'clobTokenIds' field for {slug}")
    tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
    if not isinstance(tokens, list) or len(tokens) < 2:
        raise ValueError(f"Invalid clobTokenIds for {slug}")

    condition_id = m.get("conditionId", "")
    return {
        "up_token": _validate_token_id(tokens[0], "up_token"),
        "dn_token": _validate_token_id(tokens[1], "dn_token"),
        "title": m.get("question", slug),
        "conditionId": condition_id,
        "closed": bool(m.get("closed", False)),
        "neg_risk": bool(m.get("negRisk", False)),
    }


# ============================================================================
# == SDK INIT — CLOB client + Builder Relayer setup
# ============================================================================

_clob_client = None
_relay_client = None


def init_clob_client():
    from py_clob_client.client import ClobClient
    pk = os.environ["POLYMARKET_PRIVATE_KEY"]
    funder = os.getenv("POLYMARKET_FUNDER", "")
    kwargs = {"funder": funder} if funder else {}
    client = ClobClient(
        "https://clob.polymarket.com",
        chain_id=137,
        key=pk,
        signature_type=2,
        **kwargs,
    )
    creds = client.derive_api_key()
    log.info("Derived API creds from private key")
    client.set_api_creds(creds)
    return client


def init_relay_client():
    """Initialize the Builder Relayer for gasless merge/redeem via proxy wallet."""
    global _relay_client
    from py_builder_relayer_client.client import RelayClient
    from py_builder_signing_sdk.config import BuilderConfig, BuilderApiKeyCreds

    pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    api_key = os.environ.get("POLYMARKET_BUILDER_API_KEY", "")
    secret = os.environ.get("POLYMARKET_BUILDER_SECRET", "")
    passphrase = os.environ.get("POLYMARKET_BUILDER_PASSPHRASE", "")

    if not pk or not api_key or not secret or not passphrase:
        log.warning("Builder Relayer creds missing — merge/redeem disabled")
        return None

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=api_key, secret=secret, passphrase=passphrase,
        )
    )
    client = RelayClient("https://relayer-v2.polymarket.com", 137, pk, builder_config)
    _relay_client = client
    log.info("Builder Relayer ready (gasless merge/redeem via proxy wallet)")
    return client


# ============================================================================
# == ORDERS — place_order, sell_at_bid, buy_hedge
# ============================================================================


def place_order(client, token_id: str, side_label: str, shares: float, price: float):
    """Place a single limit buy order. Returns order ID or None."""
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=round(shares, 1),
        side=BUY,
        expiration=int(time.time()) + WINDOW_S,
    )
    for attempt in range(1, 4):
        try:
            signed = client.create_order(order_args)
            result = client.post_order(signed, OrderType.GTD)
            oid = result.get("orderID", result.get("id", ""))
            if oid:
                log.info("  %s BUY %.0f @ %.2f  [%s]", side_label, shares, price, oid[:12])
                return oid
            else:
                log.warning("  %s no order ID: %s", side_label, result)
        except Exception as e:
            log.warning("  %s order failed (attempt %d/3): %s", side_label, attempt, e)
        if attempt < 3:
            time.sleep(1)
    log.error("  %s order failed after 3 attempts", side_label)
    return None


def sell_at_bid(client, token_id: str, shares: float, side_label: str,
                max_attempts: int = 3, check_interval: float = 2.0) -> str | None:
    """Place a limit SELL at the current best bid, with retry if unfilled."""
    from py_clob_client.order_builder.constants import SELL
    from py_clob_client.clob_types import OrderArgs, OrderType

    for attempt in range(1, max_attempts + 1):
        try:
            book = client.get_order_book(token_id)
            if not book.bids:
                log.warning("  %s no bids to sell into", side_label)
                return None
            best_bid = float(book.bids[0].price)
            log.info("  %s SELL %.0f @ %.2f (best bid, attempt %d/%d)",
                     side_label, shares, best_bid, attempt, max_attempts)

            order_args = OrderArgs(
                token_id=token_id,
                price=best_bid,
                size=round(shares, 1),
                side=SELL,
                expiration=int(time.time()) + WINDOW_S,
            )
            signed = client.create_order(order_args)
            result = client.post_order(signed, OrderType.GTD)
            oid = result.get("orderID", result.get("id", ""))
            if not oid:
                log.warning("  %s SELL no order ID: %s", side_label, result)
                continue

            log.info("  %s SELL placed [%s]", side_label, oid[:12])
            if attempt == max_attempts:
                return oid

            time.sleep(check_interval)
            try:
                order_detail = client.get_order(oid)
                status = str((order_detail or {}).get("status", "")).upper()
                matched = float((order_detail or {}).get("size_matched", 0))
                original = float((order_detail or {}).get("original_size", shares))
                if status != "LIVE" or matched >= original - 0.01:
                    log.info("  %s SELL filled (status=%s)", side_label, status)
                    return oid
            except Exception:
                return oid

            log.info("  %s SELL still unfilled — cancelling to re-place", side_label)
            try:
                client.cancel(oid)
            except Exception as e:
                log.warning("  %s SELL cancel failed: %s", side_label, e)
                return oid
        except Exception as e:
            log.error("  %s SELL failed (attempt %d/%d): %s", side_label, attempt, max_attempts, e)
            if attempt < max_attempts:
                time.sleep(1)
    return None


def buy_hedge(client, token_id: str, shares: float, side_label: str, price: float) -> str | None:
    """Place a limit BUY for the hedge side."""
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    order_args = OrderArgs(
        token_id=token_id,
        price=round(price, 2),
        size=round(shares, 1),
        side=BUY,
        expiration=int(time.time()) + WINDOW_S,
    )
    for attempt in range(1, 4):
        try:
            signed = client.create_order(order_args)
            result = client.post_order(signed, OrderType.GTD)
            oid = result.get("orderID", result.get("id", ""))
            if oid:
                log.info("  HEDGE %s BUY %.0f @ %.2f  [%s]", side_label, shares, price, oid[:12])
                return oid
            else:
                log.warning("  HEDGE %s no order ID: %s", side_label, result)
        except Exception as e:
            log.warning("  HEDGE %s order failed (attempt %d/3): %s", side_label, attempt, e)
        if attempt < 3:
            time.sleep(1)
    log.error("  HEDGE %s order failed after 3 attempts", side_label)
    return None


# ============================================================================
# == HEDGE ENGINE — 4-phase trailing-stop hedge for one-sided fills
# ============================================================================


async def check_hedge_trailing(client, ws_feed: WSBookFeed, past_markets: dict):
    """Time-windowed trailing-stop hedge for one-sided fills."""
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

        # Step 1: Detect one-sided fill
        if not info.get("hedge_side"):
            try:
                up_bal = check_token_balance(client, up_token)
                dn_bal = check_token_balance(client, dn_token)
            except Exception:
                continue

            if up_bal > 0 and dn_bal == 0:
                hedge_side, filled_bal, filled_side_label = "dn", up_bal, "UP"
            elif dn_bal > 0 and up_bal == 0:
                hedge_side, filled_bal, filled_side_label = "up", dn_bal, "DN"
            else:
                continue

            fill_id = f"fill-{ts}-{filled_side_label}-{int(filled_bal * 100)}"
            if _FILL_DEDUP is not None and not _FILL_DEDUP.process(fill_id):
                info["hedge_side"] = hedge_side
                info["filled_bal"] = filled_bal
                info["trailing_min_ask"] = None
                log.info("  HEDGE restored fill at %ds: %s filled (dedup)", time_elapsed, filled_side_label)
                continue

            pos: PositionState | None = info.get("position")
            if pos is not None:
                if filled_side_label == "UP":
                    pos.up_shares = float(up_bal)
                    pos.up_cost = float(up_bal) * PRICE
                else:
                    pos.down_shares = float(dn_bal)
                    pos.down_cost = float(dn_bal) * PRICE

            log.info("  HEDGE detected one-sided fill at %ds: %s filled, tracking %s",
                     time_elapsed, filled_side_label, hedge_side.upper())
            info["hedge_side"] = hedge_side
            info["filled_bal"] = filled_bal
            info["trailing_min_ask"] = None
            log_trade("fill_detected", market_ts=ts, filled_side=filled_side_label,
                      filled_bal=filled_bal, hedge_side=hedge_side.upper(), elapsed_s=time_elapsed)

        hedge_side: str = info["hedge_side"]
        hedge_token = up_token if hedge_side == "up" else dn_token
        filled_token = dn_token if hedge_side == "up" else up_token
        filled_label = "UP" if hedge_side == "dn" else "DN"

        # Step 2: Track trailing minimum
        hedge_ask = get_best_ask_safe(ws_feed, client, hedge_token)
        if hedge_ask is None:
            continue

        prev_min = info.get("trailing_min_ask")
        if prev_min is None or hedge_ask < prev_min:
            info["trailing_min_ask"] = hedge_ask
            if prev_min is not None:
                log.info("  HEDGE trail: new low for %s ask=%.3f", hedge_side.upper(), hedge_ask)
        trailing_min = info["trailing_min_ask"]

        # Phase 1: Grace period
        if time_elapsed < HEDGE_START_AFTER_S:
            continue

        # Step 3: Cancel unfilled limit once
        if not info.get("hedge_cancelled"):
            log.info("  HEDGE cancelling unfilled %s limit at %ds", hedge_side.upper(), time_elapsed)
            cancel_market_orders(client, {hedge_token})
            info["hedge_cancelled"] = True

        # Step 4: Select band based on phase
        if time_elapsed >= HEDGE_LATE_AFTER_S:
            band = HEDGE_BAND_LATE
            filled_ask = get_best_ask_safe(ws_feed, client, filled_token)
            if filled_ask is not None and filled_ask >= HEDGE_FORCE_BUY_FILLED_THRESHOLD:
                log.info("  HEDGE FORCE-BUY: filled ask=%.2f >= %.2f, hedge %s ask=%.3f (remaining=%ds)",
                         filled_ask, HEDGE_FORCE_BUY_FILLED_THRESHOLD,
                         hedge_side.upper(), hedge_ask, time_remaining)
                log_trade("hedge_force_buy", market_ts=ts, hedge_side=hedge_side.upper(),
                          filled_ask=filled_ask, hedge_ask=hedge_ask,
                          elapsed_s=time_elapsed, remaining_s=time_remaining)
                buy_hedge(client, hedge_token, SHARES_PER_SIDE, hedge_side.upper(), hedge_ask)
                info["hedge_done"] = True
                continue
        elif time_elapsed >= HEDGE_MID_AFTER_S:
            band = HEDGE_BAND_MID
        else:
            band = HEDGE_BAND_EARLY

        # Step 5: EV guard
        combined = PRICE + hedge_ask
        try:
            hedge_fee = taker_fee_usdc(SHARES_PER_SIDE, hedge_ask, _FEE_PARAMS)
            eff_rate = effective_fee_rate(hedge_ask, _FEE_PARAMS)
        except ValueError:
            hedge_fee, eff_rate = 0.0, 0.0
        hedge_ev = SHARES_PER_SIDE * (1.0 - PRICE - hedge_ask) - hedge_fee
        if (combined > HEDGE_MAX_COMBINED or hedge_ev <= 0) and band != HEDGE_BAND_LATE:
            log.info("  HEDGE EV-skip: combined=%.3f cap=%.2f EV=%.4f (elapsed=%ds)",
                     combined, HEDGE_MAX_COMBINED, hedge_ev, time_elapsed)
            continue

        # Step 5b: Valid-bounce check
        valid_bounce = trailing_min is not None and trailing_min < (band - HEDGE_TRAILING_DELTA)

        # Step 5c: Sell filled side if hedge uneconomical (late phase only)
        if hedge_ask >= band:
            if valid_bounce:
                log.info("  HEDGE VALID-BOUNCE: %s ask=%.3f > band=%.2f but low=%.3f — buying through",
                         hedge_side.upper(), hedge_ask, band, trailing_min)
            elif time_elapsed >= HEDGE_LATE_AFTER_S:
                try:
                    sell_bal = check_token_balance(client, filled_token)
                except Exception:
                    sell_bal = info.get("filled_bal", SHARES_PER_SIDE)
                if sell_bal <= 0:
                    info["hedge_done"] = True
                    continue
                log.info("  HEDGE SELL-FILLED: %s ask=%.3f >= band=%.2f — selling %s %.0f",
                         hedge_side.upper(), hedge_ask, band, filled_label, sell_bal)
                log_trade("hedge_sell_filled", market_ts=ts, hedge_side=hedge_side.upper(),
                          hedge_ask=hedge_ask, band=band, filled_bal=sell_bal,
                          elapsed_s=time_elapsed, remaining_s=time_remaining)
                cancel_market_orders(client, {hedge_token})
                sell_at_bid(client, filled_token, sell_bal, filled_label)
                info["hedge_done"] = True
                info["bailed"] = True
                continue
            else:
                log.info("  HEDGE wait: %s ask=%.3f >= band=%.2f (elapsed=%ds)",
                         hedge_side.upper(), hedge_ask, band, time_elapsed)
                continue

        # Step 6: Dynamic bounce threshold
        span = max(1, HEDGE_LATE_AFTER_S - HEDGE_START_AFTER_S)
        progress = min(1.0, (time_elapsed - HEDGE_START_AFTER_S) / span)
        min_bounce = HEDGE_TRAILING_DELTA * (1.0 - progress * 0.67)

        # Step 7: Trailing-stop trigger
        if trailing_min is None:
            continue
        bounce = hedge_ask - trailing_min
        log.info("  HEDGE trail: %s ask=%.3f low=%.3f bounce=%.3f/%.3f (elapsed=%ds band=%.2f)",
                 hedge_side.upper(), hedge_ask, trailing_min, bounce, min_bounce, time_elapsed, band)
        triggered = bounce >= min_bounce or valid_bounce
        if triggered:
            reason = "valid_bounce" if valid_bounce else "trailing"
            log.info("  HEDGE TRIGGER: %s bounced %.3f -> %.3f (delta=%.3f >= %.3f)",
                     hedge_side.upper(), trailing_min, hedge_ask, bounce, min_bounce)
            log_trade("hedge_trigger", market_ts=ts, hedge_side=hedge_side.upper(),
                      trail_min=round(trailing_min, 4), ask=round(hedge_ask, 4),
                      bounce=round(bounce, 4), min_bounce=round(min_bounce, 4),
                      band=band, reason=reason, elapsed_s=time_elapsed, remaining_s=time_remaining)
            try:
                current_bal = check_token_balance(client, hedge_token)
            except Exception:
                continue
            if current_bal >= SHARES_PER_SIDE - 0.01:
                log.info("  HEDGE skipped: already hold %.1f %s shares", current_bal, hedge_side.upper())
                info["hedge_done"] = True
                continue
            oid = buy_hedge(client, hedge_token, SHARES_PER_SIDE, hedge_side.upper(), hedge_ask)
            log_trade("hedge_buy", market_ts=ts, side=hedge_side.upper(),
                      price=round(hedge_ask, 4), shares=SHARES_PER_SIDE,
                      order_id=oid, elapsed_s=time_elapsed, remaining_s=time_remaining)
            info["hedge_done"] = True


# ============================================================================
# == ORDER UTILS — get_best_ask_safe, cancel_market_orders
# ============================================================================


def get_best_ask_safe(ws_feed: WSBookFeed, client, token_id: str) -> float | None:
    """Best ask from WS; REST fallback if enabled."""
    ask = ws_feed.get_best_ask(token_id)
    if ask is not None:
        return ask
    if not HEDGE_USE_REST_FALLBACK:
        return None
    try:
        book = client.get_order_book(token_id)
        if book and book.asks:
            return float(book.asks[0].price)
    except Exception:
        pass
    return None


def cancel_market_orders(client, token_ids: set):
    """Cancel all live orders for the given token IDs."""
    try:
        orders = client.get_orders()
        to_cancel = [o["id"] for o in orders
                     if o.get("status") == "LIVE"
                     and o.get("asset_id") in token_ids
                     and "id" in o]
        if to_cancel:
            client.cancel_orders(to_cancel)
            log.info("  Cancelled %d order(s)", len(to_cancel))
    except Exception as e:
        log.error("  Cancel failed: %s", e)


# ============================================================================
# == BAIL ENGINE — EV-based bail schedule (4 time bands)
# ============================================================================


async def check_bail_out(client, ws_feed: WSBookFeed, past_markets: dict):
    """Sell filled side if other side's ask exceeds time-based threshold."""
    now = int(time.time())

    for ts, info in list(past_markets.items()):
        if info.get("bailed") or info.get("redeemed") or info.get("hedge_done") or info.get("closed"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        if not up_token or not dn_token:
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

        up_ask = get_best_ask_safe(ws_feed, client, up_token)
        dn_ask = get_best_ask_safe(ws_feed, client, dn_token)
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
            log.info("  BAIL: DN ask=%.2f > %.2f (remaining=%ds). Selling UP %.0f",
                     dn_ask, bail_price, time_remaining, up_bal)
            log_trade("bail", market_ts=ts, trigger_side="DN", trigger_ask=dn_ask,
                      bail_price=bail_price, sell_side="UP", sell_bal=up_bal,
                      remaining_s=time_remaining)
            cancel_market_orders(client, {up_token, dn_token})
            sell_at_bid(client, up_token, up_bal, "UP")
            bail = True
        elif up_ask > bail_price and up_bal == 0 and dn_bal > 0:
            log.info("  BAIL: UP ask=%.2f > %.2f (remaining=%ds). Selling DN %.0f",
                     up_ask, bail_price, time_remaining, dn_bal)
            log_trade("bail", market_ts=ts, trigger_side="UP", trigger_ask=up_ask,
                      bail_price=bail_price, sell_side="DN", sell_bal=dn_bal,
                      remaining_s=time_remaining)
            cancel_market_orders(client, {up_token, dn_token})
            sell_at_bid(client, dn_token, dn_bal, "DN")
            bail = True

        if bail:
            info["bailed"] = True


# ============================================================================
# == ON-CHAIN — merge/redeem via Builder Relayer (gasless, proxy wallet)
# ============================================================================


def _encode_merge_calldata(condition_id: str, amount: float) -> str | None:
    """Build mergePositions calldata for the CTF contract."""
    from web3 import Web3
    cond_hex = condition_id[2:] if condition_id.startswith("0x") else condition_id
    if len(cond_hex) != 64:
        log.error("  Invalid conditionId length: %d", len(cond_hex))
        return None
    amount_wei = int(amount * 1e6)
    if amount_wei <= 0:
        return None
    w3 = Web3()
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(CTF_ADDRESS),
        abi=[{"name": "mergePositions", "type": "function", "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"},
        ], "outputs": []}],
    )
    return contract.encode_abi(
        abi_element_identifier="mergePositions",
        args=[Web3.to_checksum_address(USDC_ADDRESS), b"\x00" * 32,
              bytes.fromhex(cond_hex), [1, 2], amount_wei],
    )


def _encode_redeem_calldata(condition_id: str, neg_risk: bool,
                            up_bal: float, dn_bal: float) -> tuple[str, str] | None:
    """Build redeemPositions calldata. Returns (target_address, hex_calldata)."""
    from web3 import Web3
    cond_hex = condition_id[2:] if condition_id.startswith("0x") else condition_id
    if len(cond_hex) != 64:
        log.error("  Invalid conditionId length: %d", len(cond_hex))
        return None
    cond_bytes = bytes.fromhex(cond_hex)
    w3 = Web3()

    if neg_risk:
        amounts = [int(up_bal * 1e6), int(dn_bal * 1e6)]
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(NEG_RISK_ADAPTER),
            abi=[{"name": "redeemPositions", "type": "function", "inputs": [
                {"name": "conditionId", "type": "bytes32"},
                {"name": "amounts", "type": "uint256[]"},
            ], "outputs": []}],
        )
        calldata = contract.encode_abi(
            abi_element_identifier="redeemPositions", args=[cond_bytes, amounts])
        return NEG_RISK_ADAPTER, calldata
    else:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CTF_ADDRESS),
            abi=[{"name": "redeemPositions", "type": "function", "inputs": [
                {"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "indexSets", "type": "uint256[]"},
            ], "outputs": []}],
        )
        calldata = contract.encode_abi(
            abi_element_identifier="redeemPositions",
            args=[Web3.to_checksum_address(USDC_ADDRESS), b"\x00" * 32, cond_bytes, [1, 2]])
        return CTF_ADDRESS, calldata


def _submit_relayer_tx(txns, label: str, retries: int = 3) -> bool:
    """Submit transaction(s) via Builder Relayer with retry."""
    if not _relay_client:
        log.warning("Relayer not initialized — cannot %s", label)
        return False
    for attempt in range(1, retries + 1):
        try:
            resp = _relay_client.execute(txns, label)
            tx_id = resp.transaction_id or "?"
            log.info("  %s relayer tx submitted (attempt %d): %s", label, attempt, tx_id[:24])
            result = resp.wait()
            if result:
                tx_hash = result.get("transactionHash", "?")
                log.info("  %s CONFIRMED: %s", label, tx_hash[:20])
                return True
            else:
                log.warning("  %s timed out or failed (attempt %d/%d)", label, attempt, retries)
        except Exception as e:
            log.warning("  %s relayer error (attempt %d/%d): %s", label, attempt, retries, e)
        if attempt < retries:
            time.sleep(3)
    log.error("  %s FAILED after %d attempts", label, retries)
    return False


def merge_market_api(condition_id: str, amount: float) -> bool:
    """Merge complete sets (1 UP + 1 DN -> $1 USDC) via Builder Relayer."""
    from py_builder_relayer_client.models import SafeTransaction, OperationType
    calldata = _encode_merge_calldata(condition_id, amount)
    if not calldata:
        return False
    tx = SafeTransaction(to=CTF_ADDRESS, operation=OperationType.Call, data=calldata, value="0")
    return _submit_relayer_tx([tx], f"Merge {amount:.0f} sets")


def redeem_market_api(condition_id: str, neg_risk: bool,
                      up_bal: float = 0.0, dn_bal: float = 0.0) -> bool:
    """Redeem resolved positions via Builder Relayer."""
    from py_builder_relayer_client.models import SafeTransaction, OperationType
    result = _encode_redeem_calldata(condition_id, neg_risk, up_bal, dn_bal)
    if not result:
        return False
    target, calldata = result
    tx = SafeTransaction(to=target, operation=OperationType.Call, data=calldata, value="0")
    return _submit_relayer_tx([tx], "Redeem positions")


# ============================================================================
# == BALANCE — check_token_balance (delegated to prices.py)
# ============================================================================

from selbot.prices import check_token_balance, BalanceCheckError, clear_cache as clear_balance_cache


# ============================================================================
# == REDEEM LOOP — try_merge_all, try_redeem_all
# ============================================================================


async def try_merge_all(client, past_markets: dict):
    """Merge complete sets (UP + DN) into USDC after market closes."""
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
                mkt = await get_market_info(slug)
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
        ok = await asyncio.to_thread(merge_market_api, condition_id, merge_amount)
        log_trade("merge", market_ts=ts, title=title, sets=merge_amount, ok=ok)
        if ok:
            merged.append(ts)
        else:
            info["merge_fails"] = merge_fails + 1
            if merge_fails + 1 >= 5:
                log.warning("  Merge gave up on %s after 5 failures", title[:50])

    if merged:
        log.info("  Merged %d market(s)", len(merged))


async def try_redeem_all(client, past_markets: dict):
    """Redeem resolved positions after market closes."""
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
                mkt = await get_market_info(slug)
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
        ok = await asyncio.to_thread(redeem_market_api, condition_id, neg_risk, up_bal, dn_bal)
        log_trade("redeem", market_ts=ts, title=title, up_bal=up_bal, dn_bal=dn_bal, ok=ok)
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


async def hedge_loop(client, ws_feed: WSBookFeed, past_markets: dict):
    """Fast 1-second loop for trailing hedge checks (background task)."""
    while not _shutdown:
        try:
            await check_hedge_trailing(client, ws_feed, past_markets)
        except Exception as e:
            log.debug("hedge_loop error: %s", e)
        await asyncio.sleep(1)


async def run():
    global _shutdown, _FILL_DEDUP
    open_trade_log()
    log.info("Initializing SDK...")
    client = init_clob_client()
    relay = init_relay_client()

    log.info("SDK ready. Placing %.0fc orders on BTC 15m markets.", PRICE * 100)
    log.info("  Fee params: rate=%.2f exponent=%.0f (max eff. rate=~1.56%% at 50c)",
             _FEE_PARAMS.fee_rate, _FEE_PARAMS.exponent)
    if relay:
        log.info("Gasless merge/redeem enabled via Builder Relayer.")
    else:
        log.info("Merge/redeem DISABLED (missing Builder API credentials).")

    dedup_path = os.path.join(_trade_log_dir, "seen_fills.json")
    _FILL_DEDUP = FillDeduplicator(dedup_path)

    if FRESH_START:
        log.info("FRESH_START=1: clearing all caches and skipping startup scan.")
        _FILL_DEDUP.clear()
        clear_balance_cache()
        global _http_client
        if _http_client and not _http_client.is_closed:
            await _http_client.aclose()
            _http_client = None
    else:
        log.info("Fill deduplicator: %d fills loaded from %s", _FILL_DEDUP.count, dedup_path)

    # Start WebSocket feed for real-time prices
    ws_feed = WSBookFeed()

    placed_markets = set()
    past_markets = {}
    last_merge_redeem_ts = 0

    # Start fast hedge loop (1s interval) as background task
    asyncio.create_task(hedge_loop(client, ws_feed, past_markets))

    # Scan recent markets (last 2 hours) for unredeemed positions
    now = int(time.time())
    if FRESH_START:
        log.info("Startup scan skipped (FRESH_START=1).")
    else:
        log.info("Scanning recent markets for unredeemed positions...")
        scan_ts = (now - 7200) // MARKET_PERIOD * MARKET_PERIOD
        while scan_ts < now:
            slug = get_market_slug(scan_ts)
            try:
                mkt = await get_market_info(slug)
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
                if not mkt["closed"]:
                    if not ws_feed.is_connected:
                        await ws_feed.start([mkt["up_token"], mkt["dn_token"]])
                    else:
                        await ws_feed.subscribe([mkt["up_token"], mkt["dn_token"]])
            except Exception:
                pass
            scan_ts += MARKET_PERIOD
        log.info("Found %d recent markets to track for redemption.\n", len(past_markets))

    # Pre-fetch cache for next market
    next_market_cache = {"ts": None, "mkt": None}

    async def prefetch_next_market(next_ts: int, cache: dict):
        if cache.get("ts") == next_ts:
            return
        try:
            mkt = await get_market_info(get_market_slug(next_ts))
            cache["ts"] = next_ts
            cache["mkt"] = mkt
        except Exception:
            pass

    while not _shutdown:
        now = int(time.time())
        timestamps = next_market_timestamps(now)
        next_market_ts = (now // MARKET_PERIOD) * MARKET_PERIOD + MARKET_PERIOD

        # ----------------------------------------------------------------
        # 1. PLACEMENT
        # ----------------------------------------------------------------
        for ts in timestamps:
            if ts in placed_markets:
                continue

            slug = get_market_slug(ts)

            if ts == next_market_ts and next_market_cache.get("ts") == ts and next_market_cache.get("mkt"):
                mkt = next_market_cache["mkt"]
                next_market_cache["ts"] = None
                next_market_cache["mkt"] = None
            else:
                try:
                    mkt = await get_market_info(slug)
                except Exception as e:
                    secs_until_ts = ts - now
                    if secs_until_ts > 60:
                        log.debug("Market %s not available yet (+%ds): %s", slug, secs_until_ts, e)
                    else:
                        log.info("Market %s lookup failed (%ds): %s", slug, secs_until_ts, e)
                    continue

            secs_until = ts - now
            log.info("=" * 60)
            log.info("MARKET: %s", mkt["title"])
            log.info("  Slug: %s", slug)
            log.info("  Starts in: %ds", max(0, secs_until))
            log.info("  UP token:  %s...", mkt["up_token"][:20])
            log.info("  DN token:  %s...", mkt["dn_token"][:20])

            # Subscribe WS feed
            if not ws_feed.is_connected:
                await ws_feed.start([mkt["up_token"], mkt["dn_token"]])
            else:
                await ws_feed.subscribe([mkt["up_token"], mkt["dn_token"]])

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
                log.info("  Already have position (UP=%.1f, DN=%.1f) — skipping", up_bal, dn_bal)
                placed_markets.add(ts)
                past_markets[ts] = {
                    "redeemed": False, "closed": mkt["closed"],
                    "up_token": mkt["up_token"], "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"], "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug), "placed_at": ts,
                }
                continue

            market_tokens = {mkt["up_token"], mkt["dn_token"]}
            existing = [o for o in (live_orders or [])
                        if o.get("status") == "LIVE" and o.get("asset_id") in market_tokens]
            if existing:
                log.info("  Already have %d live order(s) — skipping", len(existing))
                placed_markets.add(ts)
                past_markets[ts] = {
                    "redeemed": False, "closed": mkt["closed"],
                    "up_token": mkt["up_token"], "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"], "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug), "placed_at": ts,
                }
                continue

            secs_elapsed = now - ts
            if secs_elapsed > NEW_MARKET_PLACE_WINDOW_SECONDS:
                log.info("  Skipping — outside %ds window (%ds elapsed)", NEW_MARKET_PLACE_WINDOW_SECONDS, secs_elapsed)
                log_trade("window_missed", market_ts=ts, slug=slug, elapsed_s=secs_elapsed)
                placed_markets.add(ts)
                past_markets[ts] = {
                    "redeemed": False, "closed": mkt["closed"],
                    "up_token": mkt["up_token"], "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"], "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug), "placed_at": 0,
                }
                continue

            if MAX_OPEN_MARKETS > 0:
                open_count = sum(1 for _info in past_markets.values()
                                 if not _info.get("redeemed") and not _info.get("bailed") and not _info.get("closed"))
                if open_count >= MAX_OPEN_MARKETS:
                    log.info("  Skipping — at max open markets (%d >= %d)", open_count, MAX_OPEN_MARKETS)
                    continue

            # Dynamic entry price
            entry_price = PRICE
            if ENTRY_PRICE_DYNAMIC:
                up_ask = ws_feed.get_best_ask(mkt["up_token"])
                dn_ask = ws_feed.get_best_ask(mkt["dn_token"])
                if up_ask is not None and dn_ask is not None:
                    spread = up_ask + dn_ask
                    if spread > 1.05:
                        entry_price = ENTRY_PRICE_VOL_HIGH
                    elif spread < 0.95:
                        entry_price = ENTRY_PRICE_VOL_LOW
                    log.info("  Dynamic entry: spread=%.3f -> price=%.2f", spread, entry_price)

            # Place both sides in parallel
            up_id, dn_id = await asyncio.gather(
                asyncio.to_thread(place_order, client, mkt["up_token"], "UP  ", SHARES_PER_SIDE, entry_price),
                asyncio.to_thread(place_order, client, mkt["dn_token"], "DOWN", SHARES_PER_SIDE, entry_price),
            )

            placed = (1 if up_id else 0) + (1 if dn_id else 0)
            log.info("  Done: %d orders placed. Max cost: $%.2f\n", placed, SHARES_PER_SIDE * entry_price * 2)
            log_trade("orders_placed", market_ts=ts, slug=slug, title=mkt.get("title", slug),
                      price=entry_price, shares=SHARES_PER_SIDE,
                      up_id=up_id, dn_id=dn_id, sides_placed=placed,
                      elapsed_s=now - ts, dynamic=ENTRY_PRICE_DYNAMIC)

            if placed == 0:
                log.info("  Both orders returned null — will retry next loop.")
                continue

            placed_markets.add(ts)
            past_markets[ts] = {
                "redeemed": False, "closed": mkt["closed"],
                "up_token": mkt["up_token"], "dn_token": mkt["dn_token"],
                "condition_id": mkt["conditionId"], "neg_risk": mkt.get("neg_risk", False),
                "title": mkt.get("title", slug), "placed_at": ts,
                "position": PositionState(),
            }

        # Log WS prices for active markets
        for ts, info in past_markets.items():
            if info.get("redeemed") or info.get("closed"):
                continue
            up_ask = ws_feed.get_best_ask(info.get("up_token", ""))
            dn_ask = ws_feed.get_best_ask(info.get("dn_token", ""))
            if up_ask is not None or dn_ask is not None:
                elapsed = now - info.get("placed_at", now)
                hedge_state = ""
                if info.get("hedge_done"):
                    hedge_state = " [HEDGED]"
                elif info.get("hedge_side"):
                    hedge_state = f" [TRAILING {info['hedge_side'].upper()}]"
                log.info("  [%d +%ds] WS: UP=%.2f  DN=%.2f%s",
                         ts, elapsed, up_ask or 0, dn_ask or 0, hedge_state)

        # ----------------------------------------------------------------
        # 2. STALE CANCEL
        # ----------------------------------------------------------------
        for _ts, _info in past_markets.items():
            if _info.get("redeemed") or _info.get("closed") or _info.get("bailed"):
                continue
            _placed = _info.get("placed_at", 0)
            if _placed and (now - _placed) >= STALE_CANCEL_S and not _info.get("stale_cancelled"):
                _tokens = set()
                if _info.get("up_token"):
                    _tokens.add(_info["up_token"])
                if _info.get("dn_token"):
                    _tokens.add(_info["dn_token"])
                if _tokens:
                    log.info("  STALE-CANCEL: market %d (%ds elapsed)", _ts, now - _placed)
                    cancel_market_orders(client, _tokens)
                    _info["stale_cancelled"] = True
                    log_trade("stale_cancel", market_ts=_ts, elapsed_s=now - _placed)

        # ----------------------------------------------------------------
        # 3. WS HEALTH CHECK
        # ----------------------------------------------------------------
        ws_age = ws_feed.last_message_age
        if ws_age > 90.0 and ws_feed.is_connected:
            log.warning("WS feed stale (%.0fs since last message) — forcing reconnect", ws_age)
            await ws_feed.force_reconnect()

        # ----------------------------------------------------------------
        # 4. BAIL CHECK
        # ----------------------------------------------------------------
        await check_bail_out(client, ws_feed, past_markets)

        # ----------------------------------------------------------------
        # 5. MERGE + REDEEM (throttled to every 30s)
        # ----------------------------------------------------------------
        if now - last_merge_redeem_ts >= MERGE_REDEEM_INTERVAL_S:
            await try_merge_all(client, past_markets)
            await try_redeem_all(client, past_markets)
            last_merge_redeem_ts = now

        # ----------------------------------------------------------------
        # 6. ADAPTIVE SLEEP + CLEANUP
        # ----------------------------------------------------------------
        seconds_until = next_market_ts - now
        sleep_s = 0.5 if seconds_until <= 30 else (1 if seconds_until <= 120 else 3)

        if seconds_until <= NEXT_MARKET_PREFETCH_SEC and seconds_until > 0:
            asyncio.create_task(prefetch_next_market(next_market_ts, next_market_cache))
        await asyncio.sleep(sleep_s)

        log.info("[Next market in %ds] Checked %d markets", seconds_until, len(timestamps))

        # Cleanup: hard TTL 6h, normal TTL 2h for redeemed
        HARD_TTL_S = 21600
        NORMAL_TTL_S = 7200
        stale = []
        for ts, info in past_markets.items():
            age = now - ts
            if age > HARD_TTL_S:
                if not info.get("redeemed"):
                    log.warning("  Hard TTL: dropping market %d after %ds (unredeemed)", ts, age)
                    log_trade("hard_ttl_drop", market_ts=ts, age_s=age)
                stale.append(ts)
            elif age > NORMAL_TTL_S and info.get("redeemed"):
                stale.append(ts)
        for ts in stale:
            del past_markets[ts]
        placed_markets = {ts for ts in placed_markets if ts > now - 2 * MARKET_PERIOD}

    # Clean shutdown
    log.info("Shutting down...")
    if _FILL_DEDUP is not None:
        _FILL_DEDUP.save()
        log.info("Fill deduplicator saved (%d fills).", _FILL_DEDUP.count)
    await ws_feed.close()
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()


# ============================================================================
# == ENTRY POINT — _main() with auto-restart wrapper
# ============================================================================


def _main():
    global _shutdown

    def _signal_handler(sig, frame):
        global _shutdown
        _shutdown = True
        log.info("\nShutdown requested (signal %s). Finishing current cycle...", sig)

    signal.signal(signal.SIGINT, _signal_handler)
    try:
        signal.signal(signal.SIGTERM, _signal_handler)
    except (OSError, ValueError):
        pass  # SIGTERM not available on Windows in some contexts

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
