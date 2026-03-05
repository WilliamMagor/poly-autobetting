"""
BTC 15-minute dual-side arbitrage bot for Polymarket — fully API-driven.

Strategy: Buy both UP and DOWN at ~46c each. If both fill, cost <$1 for
guaranteed $1 payout. Rotates every 15 minutes, runs 24/7.

Orders placed via CLOB API (signed with private key).
Merge/redeem via direct on-chain Polygon transactions (web3).
Real-time prices via WebSocket feed.

Usage:
    cd C:\\Users\\123\\poly-autobetting
    venv\\Scripts\\python.exe selbot/bot.py

SECTION MAP (search for "# ==" to jump between sections):
  == TRADE LOG       JSONL logging with day rotation
  == CONFIG          All constants, env vars, timing
  == HELPERS         Slug builder, validators
  == MARKET API      get_market_info (Gamma API), httpx client
  == SDK INIT        CLOB client + on-chain redeemer setup
  == ORDERS          place_order, sell_at_bid, buy_hedge
  == HEDGE ENGINE    4-phase trailing-stop hedge for one-sided fills
  == ORDER UTILS     get_best_ask_safe, cancel_market_orders
  == BAIL ENGINE     EV-based bail schedule (4 time bands)
  == ON-CHAIN        merge_market, redeem_market (Polygon txs)
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
import threading
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
HEDGE_CANCEL_AFTER_S = int(_env("PLACE_15_HEDGE_CANCEL_S", "720"))
HEDGE_BAND_EARLY = 1.0 - PRICE - 0.06
HEDGE_BAND_MID = 1.0 - PRICE - 0.01
HEDGE_BAND_LATE = float(_env("PLACE_15_HEDGE_LATE_BAND", "0.70"))
HEDGE_TRAILING_DELTA = float(_env("PLACE_15_HEDGE_TRAILING_DELTA", "0.03"))
HEDGE_FORCE_BUY_FILLED_THRESHOLD = float(_env("PLACE_15_HEDGE_FORCE_THRESHOLD", "0.85"))
HEDGE_MAX_COMBINED = float(_env("PLACE_15_HEDGE_MAX_COMBINED", "0.98"))
HEDGE_USE_REST_FALLBACK = _env("PLACE_15_REST_FALLBACK", "0").strip().lower() in ("1", "true", "yes")
HEDGE_LOOP_INTERVAL_S = int(_env("PLACE_15_HEDGE_LOOP_S", "5"))
HEDGE_ENABLED = _env("PLACE_15_HEDGE_ENABLED", "1").strip().lower() in ("1", "true", "yes")
HEDGE_RETRY_INTERVAL_S = int(_env("PLACE_15_HEDGE_RETRY_S", "10"))
STRATEGY_MODE = _env("PLACE_15_STRATEGY", "dual_hedge").strip().lower()
BUY_HOLD_TRIGGER_PRICE = float(_env("PLACE_15_BUY_HOLD_TRIGGER_PRICE", "0.91"))
BUY_HOLD_TRIGGER_REMAINING_S = int(_env("PLACE_15_BUY_HOLD_TRIGGER_REMAINING_S", "180"))
BUY_HOLD_TARGET_SHARES = int(_env("PLACE_15_BUY_HOLD_TARGET_SHARES", "110"))
PROFIT_BOOST_TRIGGER_PRICE = float(_env("PLACE_15_PROFIT_BOOST_TRIGGER_PRICE", "0.90"))
PROFIT_BOOST_REMAINING_S = int(_env("PLACE_15_PROFIT_BOOST_REMAINING_S", "120"))
PROFIT_BOOST_SHARES = int(_env("PLACE_15_PROFIT_BOOST_SHARES", "10"))
INTER_ASSET_STAGGER_S = float(_env("PLACE_15_INTER_ASSET_STAGGER_S", "2.0"))
MARKET_SYMBOLS = tuple(
    s.strip().lower() for s in _env("PLACE_15_SYMBOLS", "btc,eth").split(",") if s.strip()
) or ("btc", "eth")

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
_SLUG_RE = re.compile(r"^(btc|eth)-updown-15m-\d+$")

# Persistent httpx client
_http_client: httpx.AsyncClient | None = None


# ============================================================================
# == HELPERS — Slug builder, validators
# ============================================================================


def next_market_timestamps(now_ts: int) -> list[int]:
    ts = (now_ts // MARKET_PERIOD) * MARKET_PERIOD
    return [ts - MARKET_PERIOD, ts, ts + MARKET_PERIOD]


def get_market_slug(symbol: str, timestamp: int) -> str:
    return f"{symbol}-updown-15m-{timestamp}"


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


async def _fallback_market_scan(ts: int, symbol: str) -> dict | None:
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
        if not ((symbol in question or symbol in mslug) and ("15m" in mslug or "updown" in mslug)):
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
        symbol = slug.split("-", 1)[0].lower()
        fallback = await _fallback_market_scan(ts, symbol)
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
# == SDK INIT — CLOB client + on-chain redeemer setup
# ============================================================================


def init_clob_client():
    """Initialize py-clob-client SDK."""
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


def init_redeemer():
    """Initialize merge/redeem. Prefers Builder Relayer (executes from Safe/proxy where tokens live).
    Falls back to direct web3 (only works if EOA holds tokens). Returns None if neither available."""
    pk = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    if not pk:
        log.warning("No POLYMARKET_PRIVATE_KEY — auto-redeem disabled")
        return None

    # Prefer relayer: positions are in Safe/proxy when using signature_type=2; relayer executes from there
    builder_key = os.getenv("POLYMARKET_BUILDER_API_KEY", "").strip()
    builder_secret = os.getenv("POLYMARKET_BUILDER_SECRET", "").strip()
    builder_phrase = os.getenv("POLYMARKET_BUILDER_PASSPHRASE", "").strip()
    if builder_key and builder_secret and builder_phrase:
        try:
            from py_builder_relayer_client.client import RelayClient
            from py_builder_signing_sdk.config import BuilderConfig
            from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds

            builder_config = BuilderConfig(
                local_builder_creds=BuilderApiKeyCreds(
                    key=builder_key,
                    secret=builder_secret,
                    passphrase=builder_phrase,
                )
            )
            relayer_url = os.getenv("POLYMARKET_RELAYER_URL", "https://relayer-v2.polymarket.com")
            client = RelayClient(
                relayer_url,
                137,
                private_key=pk,
                builder_config=builder_config,
            )
            log.info("Merge/redeem via Builder Relayer (Safe/proxy)")
            return ("relayer", client)
        except Exception as e:
            log.warning("Builder Relayer init failed (%s) — falling back to direct", e)

    # Fallback: direct web3 (works only if EOA holds tokens, e.g. non-proxy)
    from web3 import Web3
    from eth_account import Account

    polygon_rpc = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
    w3 = Web3(Web3.HTTPProvider(polygon_rpc))
    if not w3.is_connected():
        log.warning("Cannot connect to Polygon RPC (%s) — auto-redeem disabled", polygon_rpc)
        return None

    account = Account.from_key(pk)
    log.info("Merge/redeem via direct web3 (EOA — ensure tokens are in EOA, not proxy)")
    return ("direct", (w3, account))


# ============================================================================
# == ORDERS — place_order, sell_at_bid, buy_hedge
# ============================================================================


def place_order(client, token_id: str, side_label: str, shares: float, price: float,
                market_end_ts: int | None = None):
    """Place a single limit buy order. Returns order ID or None.
    Orders are posted as GTC (good-'til-cancelled)."""
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=round(shares, 1),
        side=BUY,
        expiration=0,
    )
    for attempt in range(1, 4):
        try:
            signed = client.create_order(order_args)
            result = client.post_order(signed, OrderType.GTC)
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
                max_attempts: int = 3, check_interval: float = 2.0,
                market_end_ts: int | None = None) -> str | None:
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
                expiration=0,
            )
            signed = client.create_order(order_args)
            result = client.post_order(signed, OrderType.GTC)
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


def buy_hedge(client, token_id: str, shares: float, side_label: str, price: float,
               market_end_ts: int | None = None) -> str | None:
    """Place a limit BUY for the hedge side."""
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    order_args = OrderArgs(
        token_id=token_id,
        price=round(price, 2),
        size=round(shares, 1),
        side=BUY,
        expiration=0,
    )
    for attempt in range(1, 4):
        try:
            signed = client.create_order(order_args)
            result = client.post_order(signed, OrderType.GTC)
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

    def _maybe_buy_hold_scale(
        market_ts: int,
        info: dict,
        up_token: str,
        dn_token: str,
        up_bal: float,
        dn_bal: float,
        time_remaining: int,
    ) -> bool:
        if STRATEGY_MODE != "buy_hold_91":
            return False
        if time_remaining <= 0 or time_remaining > BUY_HOLD_TRIGGER_REMAINING_S:
            return False

        # buy_hold_91 scaling is only for one-sided positions.
        if (up_bal > 0 and dn_bal > 0) or (up_bal <= 0 and dn_bal <= 0):
            return False

        up_ask = get_best_ask_safe(ws_feed, client, up_token)
        dn_ask = get_best_ask_safe(ws_feed, client, dn_token)
        candidates: list[tuple[str, str, float, float]] = []
        if up_ask is not None and up_ask >= BUY_HOLD_TRIGGER_PRICE:
            candidates.append(("UP", up_token, up_ask, up_bal))
        if dn_ask is not None and dn_ask >= BUY_HOLD_TRIGGER_PRICE:
            candidates.append(("DN", dn_token, dn_ask, dn_bal))
        if not candidates:
            return False
        side_label, token_id, side_ask, side_bal = max(candidates, key=lambda item: item[2])

        if not info.get("buy_hold_signal_logged"):
            log.info(
                "  BUY-HOLD signal: favored %s ask=%.3f >= %.2f with %ds left — scaling toward %.0f shares",
                side_label, side_ask, BUY_HOLD_TRIGGER_PRICE, time_remaining, float(BUY_HOLD_TARGET_SHARES)
            )
            log_trade(
                "buy_hold_signal",
                market_ts=market_ts,
                strategy=STRATEGY_MODE,
                favored_side=side_label,
                favored_ask=round(side_ask, 4),
                trigger_price=BUY_HOLD_TRIGGER_PRICE,
                remaining_s=time_remaining,
            )
            info["buy_hold_signal_logged"] = True
        info["buy_hold"] = True
        info["hedge_done"] = True

        target_shares = max(float(SHARES_PER_SIDE), float(BUY_HOLD_TARGET_SHARES))
        remaining = max(0.0, target_shares - side_bal)
        if remaining <= 0.01:
            info["buy_hold_scaled"] = True
            log.info("  BUY-HOLD scale complete: %s already at %.1f shares", side_label, side_bal)
            return True

        last_try = info.get("buy_hold_last_try_ts", 0)
        if now - last_try < HEDGE_RETRY_INTERVAL_S:
            return True

        log.info(
            "  BUY-HOLD SCALE: %s ask=%.3f >= %.2f with %ds left — buying %.1f shares to %.0f target",
            side_label,
            side_ask,
            BUY_HOLD_TRIGGER_PRICE,
            time_remaining,
            remaining,
            target_shares,
        )
        oid = buy_hedge(
            client,
            token_id,
            remaining,
            side_label,
            side_ask,
            market_ts + WINDOW_S,
        )
        log_trade(
            "buy_hold_scale_buy",
            market_ts=market_ts,
            strategy=STRATEGY_MODE,
            side=side_label,
            price=round(side_ask, 4),
            shares=round(remaining, 4),
            target_shares=target_shares,
            order_id=oid,
            trigger_price=BUY_HOLD_TRIGGER_PRICE,
            remaining_s=time_remaining,
        )
        info["buy_hold_last_try_ts"] = now
        return True

    def _maybe_late_profit_boost(
        market_ts: int,
        info: dict,
        up_token: str,
        dn_token: str,
        up_bal: float,
        dn_bal: float,
        time_remaining: int,
    ) -> None:
        if info.get("late_boost_done"):
            return
        if time_remaining <= 0 or time_remaining > PROFIT_BOOST_REMAINING_S:
            return

        min_original = SHARES_PER_SIDE - 0.01
        if up_bal < min_original or dn_bal < min_original:
            return

        up_ask = get_best_ask_safe(ws_feed, client, up_token)
        dn_ask = get_best_ask_safe(ws_feed, client, dn_token)

        candidates: list[tuple[str, str, float, float]] = []
        if up_ask is not None and up_ask >= PROFIT_BOOST_TRIGGER_PRICE:
            candidates.append(("UP", up_token, up_ask, up_bal))
        if dn_ask is not None and dn_ask >= PROFIT_BOOST_TRIGGER_PRICE:
            candidates.append(("DN", dn_token, dn_ask, dn_bal))
        if not candidates:
            return

        side_label, token_id, side_ask, side_bal = max(candidates, key=lambda item: item[2])
        target_bal = SHARES_PER_SIDE + PROFIT_BOOST_SHARES - 0.01
        if side_bal >= target_bal:
            info["late_boost_done"] = True
            log.info("  PROFIT BOOST skipped: %s already at %.1f shares", side_label, side_bal)
            return

        log.info(
            "  PROFIT BOOST BUY: %s ask=%.3f >= %.2f with %ds left — buying +%d shares",
            side_label,
            side_ask,
            PROFIT_BOOST_TRIGGER_PRICE,
            time_remaining,
            PROFIT_BOOST_SHARES,
        )
        oid = buy_hedge(
            client,
            token_id,
            float(PROFIT_BOOST_SHARES),
            side_label,
            side_ask,
            market_ts + WINDOW_S,
        )
        log_trade(
            "profit_boost_buy",
            market_ts=market_ts,
            side=side_label,
            price=round(side_ask, 4),
            shares=PROFIT_BOOST_SHARES,
            order_id=oid,
            trigger_price=PROFIT_BOOST_TRIGGER_PRICE,
            remaining_s=time_remaining,
        )
        info["late_boost_done"] = True

    for ts, info in list(past_markets.items()):
        if info.get("redeemed") or info.get("closed") or info.get("bailed"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        placed_at = info.get("placed_at", 0)
        if not up_token or not dn_token or not placed_at:
            continue

        time_elapsed = now - placed_at
        time_remaining = WINDOW_S - time_elapsed

        try:
            up_bal = check_token_balance(client, up_token)
            dn_bal = check_token_balance(client, dn_token)
        except Exception:
            continue

        if info.get("hedge_done"):
            _maybe_buy_hold_scale(ts, info, up_token, dn_token, up_bal, dn_bal, time_remaining)
            _maybe_late_profit_boost(ts, info, up_token, dn_token, up_bal, dn_bal, time_remaining)
            continue

        # Step 1: Detect one-sided fill
        if not info.get("hedge_side"):
            if up_bal > 0 and dn_bal == 0:
                hedge_side, filled_bal, filled_side_label = "dn", up_bal, "UP"
            elif dn_bal > 0 and up_bal == 0:
                hedge_side, filled_bal, filled_side_label = "up", dn_bal, "DN"
            elif up_bal >= SHARES_PER_SIDE - 0.01 and dn_bal >= SHARES_PER_SIDE - 0.01:
                log.info("  HEDGE reconciled: both sides filled (UP=%.1f, DN=%.1f) — done", up_bal, dn_bal)
                info["hedge_done"] = True
                _maybe_late_profit_boost(ts, info, up_token, dn_token, up_bal, dn_bal, time_remaining)
                continue
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

        # Optional strategy: in late window, buy favored side if very strong.
        if STRATEGY_MODE == "buy_hold_91":
            if _maybe_buy_hold_scale(ts, info, up_token, dn_token, up_bal, dn_bal, time_remaining):
                continue

        # Reconciliation: if both sides filled (e.g. hedge side filled after we entered trailing), we're done
        if up_bal > 0 and dn_bal > 0:
            log.info("  HEDGE reconciled: both sides filled (UP=%.1f, DN=%.1f) — done", up_bal, dn_bal)
            info["hedge_done"] = True
            _maybe_late_profit_boost(ts, info, up_token, dn_token, up_bal, dn_bal, time_remaining)
            continue
        hedge_bal = up_bal if hedge_side == "up" else dn_bal

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

        # Delay hedge actions until late window (ex: 3 minutes before market end)
        if time_elapsed < HEDGE_CANCEL_AFTER_S:
            continue

        # Step 3: Cancel unfilled limit once (late window)
        if not info.get("hedge_cancelled"):
            log.info("  HEDGE cancelling unfilled %s limit at %ds", hedge_side.upper(), time_elapsed)
            cancel_market_orders(client, {hedge_token})
            info["hedge_cancelled"] = True

        # In late window, if ask is <= configured cap (default 0.70), attempt immediate hedge buy.
        if hedge_ask <= HEDGE_BAND_LATE:
            if hedge_bal >= SHARES_PER_SIDE - 0.01:
                info["hedge_done"] = True
                continue
            remaining = max(0.0, SHARES_PER_SIDE - hedge_bal)
            last_try = info.get("hedge_last_try_ts", 0)
            if now - last_try < HEDGE_RETRY_INTERVAL_S:
                next_retry_in_s = max(0, HEDGE_RETRY_INTERVAL_S - (now - last_try))
                log.info(
                    "  HEDGE retry-wait: %s bal=%.4f remaining=%.4f next_retry_in_s=%ds",
                    hedge_side.upper(), hedge_bal, remaining, next_retry_in_s
                )
                continue
            log.info("  HEDGE LATE-BUY: %s ask=%.3f <= %.2f (elapsed=%ds)",
                     hedge_side.upper(), hedge_ask, HEDGE_BAND_LATE, time_elapsed)
            log.info("  HEDGE retry: %s bal=%.4f remaining=%.4f next_retry_in_s=%ds",
                     hedge_side.upper(), hedge_bal, remaining, HEDGE_RETRY_INTERVAL_S)
            oid = buy_hedge(client, hedge_token, remaining, hedge_side.upper(), hedge_ask, ts + WINDOW_S)
            log_trade("hedge_buy", market_ts=ts, side=hedge_side.upper(),
                      price=round(hedge_ask, 4), shares=remaining,
                      order_id=oid, reason="late_window",
                      elapsed_s=time_elapsed, remaining_s=time_remaining)
            info["hedge_last_try_ts"] = now
            continue

        # Step 4: Select band based on phase
        if time_elapsed >= HEDGE_LATE_AFTER_S:
            band = HEDGE_BAND_LATE
            filled_ask = get_best_ask_safe(ws_feed, client, filled_token)
            if filled_ask is not None and filled_ask >= HEDGE_FORCE_BUY_FILLED_THRESHOLD:
                if hedge_bal >= SHARES_PER_SIDE - 0.01:
                    info["hedge_done"] = True
                    continue
                remaining = max(0.0, SHARES_PER_SIDE - hedge_bal)
                last_try = info.get("hedge_last_try_ts", 0)
                if now - last_try < HEDGE_RETRY_INTERVAL_S:
                    next_retry_in_s = max(0, HEDGE_RETRY_INTERVAL_S - (now - last_try))
                    log.info(
                        "  HEDGE retry-wait: %s bal=%.4f remaining=%.4f next_retry_in_s=%ds",
                        hedge_side.upper(), hedge_bal, remaining, next_retry_in_s
                    )
                    continue
                log.info("  HEDGE FORCE-BUY: filled ask=%.2f >= %.2f, hedge %s ask=%.3f (remaining=%ds)",
                         filled_ask, HEDGE_FORCE_BUY_FILLED_THRESHOLD,
                         hedge_side.upper(), hedge_ask, time_remaining)
                log.info("  HEDGE retry: %s bal=%.4f remaining=%.4f next_retry_in_s=%ds",
                         hedge_side.upper(), hedge_bal, remaining, HEDGE_RETRY_INTERVAL_S)
                log_trade("hedge_force_buy", market_ts=ts, hedge_side=hedge_side.upper(),
                          filled_ask=filled_ask, hedge_ask=hedge_ask,
                          elapsed_s=time_elapsed, remaining_s=time_remaining)
                buy_hedge(client, hedge_token, remaining, hedge_side.upper(), hedge_ask, ts + WINDOW_S)
                info["hedge_last_try_ts"] = now
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
        if hedge_ask > band:
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
                sell_at_bid(client, filled_token, sell_bal, filled_label, market_end_ts=ts + WINDOW_S)
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
            remaining = max(0.0, SHARES_PER_SIDE - current_bal)
            last_try = info.get("hedge_last_try_ts", 0)
            if now - last_try < HEDGE_RETRY_INTERVAL_S:
                next_retry_in_s = max(0, HEDGE_RETRY_INTERVAL_S - (now - last_try))
                log.info(
                    "  HEDGE retry-wait: %s bal=%.4f remaining=%.4f next_retry_in_s=%ds",
                    hedge_side.upper(), current_bal, remaining, next_retry_in_s
                )
                continue
            log.info("  HEDGE retry: %s bal=%.4f remaining=%.4f next_retry_in_s=%ds",
                     hedge_side.upper(), current_bal, remaining, HEDGE_RETRY_INTERVAL_S)
            oid = buy_hedge(client, hedge_token, remaining, hedge_side.upper(), hedge_ask, ts + WINDOW_S)
            log_trade("hedge_buy", market_ts=ts, side=hedge_side.upper(),
                      price=round(hedge_ask, 4), shares=remaining,
                      order_id=oid, elapsed_s=time_elapsed, remaining_s=time_remaining)
            info["hedge_last_try_ts"] = now


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
            sell_at_bid(client, up_token, up_bal, "UP", market_end_ts=ts + WINDOW_S)
            bail = True
        elif up_ask > bail_price and up_bal == 0 and dn_bal > 0:
            log.info("  BAIL: UP ask=%.2f > %.2f (remaining=%ds). Selling DN %.0f",
                     up_ask, bail_price, time_remaining, dn_bal)
            log_trade("bail", market_ts=ts, trigger_side="UP", trigger_ask=up_ask,
                      bail_price=bail_price, sell_side="DN", sell_bal=dn_bal,
                      remaining_s=time_remaining)
            cancel_market_orders(client, {up_token, dn_token})
            sell_at_bid(client, dn_token, dn_bal, "DN", market_end_ts=ts + WINDOW_S)
            bail = True

        if bail:
            info["bailed"] = True


# ============================================================================
# == ON-CHAIN — merge_market, redeem_market (Polygon transactions)
# ============================================================================


class NonceManager:
    """Thread-safe nonce manager that prevents nonce collisions between
    back-to-back merge and redeem transactions."""

    def __init__(self):
        self._lock = threading.Lock()
        self._nonce: int | None = None

    def get_nonce(self, w3, address: str) -> int:
        with self._lock:
            on_chain = w3.eth.get_transaction_count(address)
            if self._nonce is None or on_chain > self._nonce:
                self._nonce = on_chain
            nonce = self._nonce
            self._nonce += 1
            return nonce

    def reset(self):
        with self._lock:
            self._nonce = None


_nonce_mgr = NonceManager()


def _send_tx(w3, account, tx_fn, gas: int, label: str) -> bool:
    """Build, sign, send, and wait for an on-chain transaction.
    Shared by merge_market and redeem_market."""
    try:
        nonce = _nonce_mgr.get_nonce(w3, account.address)
        tx = tx_fn.build_transaction({
            "from": account.address,
            "nonce": nonce,
            "gas": gas,
            "gasPrice": w3.eth.gas_price,
            "chainId": 137,
        })
        signed = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("  %s tx sent: %s", label, tx_hash.hex()[:20])
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] == 1:
            log.info("  %s CONFIRMED: %s", label, tx_hash.hex()[:20])
            return True
        log.error("  %s tx FAILED (status=0): %s", label, tx_hash.hex()[:20])
        _nonce_mgr.reset()
        return False
    except Exception as e:
        log.error("  %s error: %s", label, e)
        _nonce_mgr.reset()
        return False


def _get_w3_for_encode():
    """Web3 instance for encoding (needs provider for build_transaction)."""
    from web3 import Web3
    rpc = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
    return Web3(Web3.HTTPProvider(rpc))


def _merge_tx_data(condition_id: str, amount: float) -> tuple[str, str]:
    """Build merge calldata. Returns (contract_address, hex_data)."""
    from web3 import Web3
    w3 = _get_w3_for_encode()
    cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)
    amount_wei = int(amount * 1e6)
    abi = [{"name": "mergePositions", "type": "function", "inputs": [
        {"name": "collateralToken", "type": "address"},
        {"name": "parentCollectionId", "type": "bytes32"},
        {"name": "conditionId", "type": "bytes32"},
        {"name": "partition", "type": "uint256[]"},
        {"name": "amount", "type": "uint256"},
    ], "outputs": []}]
    contract = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=abi)
    tx_fn = contract.functions.mergePositions(
        Web3.to_checksum_address(USDC_ADDRESS), b"\x00" * 32, cond_bytes, [1, 2], amount_wei
    )
    tx = tx_fn.build_transaction({
        "from": "0x0000000000000000000000000000000000000001",
        "gas": 200_000,
        "gasPrice": 1,
    })
    return CTF_ADDRESS, tx["data"]


def _redeem_tx_data(condition_id: str, neg_risk: bool, up_bal: float, dn_bal: float) -> tuple[str, str]:
    """Build redeem calldata. Returns (contract_address, hex_data)."""
    from web3 import Web3
    w3 = _get_w3_for_encode()
    cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)
    if neg_risk:
        amounts = [int(up_bal * 1e6), int(dn_bal * 1e6)]
        abi = [{"name": "redeemPositions", "type": "function", "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amounts", "type": "uint256[]"},
        ], "outputs": []}]
        contract = w3.eth.contract(address=Web3.to_checksum_address(NEG_RISK_ADAPTER), abi=abi)
        tx_fn = contract.functions.redeemPositions(cond_bytes, amounts)
    else:
        abi = [{"name": "redeemPositions", "type": "function", "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ], "outputs": []}]
        contract = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=abi)
        tx_fn = contract.functions.redeemPositions(
            Web3.to_checksum_address(USDC_ADDRESS), b"\x00" * 32, cond_bytes, [1, 2]
        )
    tx = tx_fn.build_transaction({
        "from": "0x0000000000000000000000000000000000000001",
        "gas": 300_000,
        "gasPrice": 1,
    })
    addr = NEG_RISK_ADAPTER if neg_risk else CTF_ADDRESS
    return addr, tx["data"]


def merge_market(redeemer, condition_id: str, amount: float) -> bool:
    """Merge complete sets (1 UP + 1 DN) into USDC on-chain.
    amount = number of full sets to merge. Returns True on success."""
    from web3 import Web3

    amount_wei = int(amount * 1e6)
    if amount_wei <= 0:
        return False

    if redeemer[0] == "relayer":
        from py_builder_relayer_client.models import SafeTransaction, OperationType
        relay_client = redeemer[1]
        to_addr, data = _merge_tx_data(condition_id, amount)
        tx = SafeTransaction(to=to_addr, operation=OperationType.Call, data=data, value="0")
        try:
            resp = relay_client.execute([tx], "Merge positions")
            log.info("  Merge tx sent via relayer: %s", resp.transaction_id or resp.transaction_hash or "pending")
            result = resp.wait()
            if result is not None:
                log.info("  Merge CONFIRMED via relayer")
                return True
            log.error("  Merge FAILED via relayer")
            return False
        except Exception as e:
            log.error("  Merge relayer error: %s", e)
            return False

    w3, account = redeemer[1]
    cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)
    abi = [{"name": "mergePositions", "type": "function", "inputs": [
        {"name": "collateralToken", "type": "address"},
        {"name": "parentCollectionId", "type": "bytes32"},
        {"name": "conditionId", "type": "bytes32"},
        {"name": "partition", "type": "uint256[]"},
        {"name": "amount", "type": "uint256"},
    ], "outputs": []}]
    contract = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=abi)
    tx_fn = contract.functions.mergePositions(
        Web3.to_checksum_address(USDC_ADDRESS), b"\x00" * 32, cond_bytes, [1, 2], amount_wei
    )
    return _send_tx(w3, account, tx_fn, 200_000, "Merge")


def redeem_market(redeemer, condition_id: str, neg_risk: bool,
                  up_bal: float = 0.0, dn_bal: float = 0.0) -> bool:
    """Redeem resolved positions via on-chain Polygon transaction.
    Returns True on success."""
    from web3 import Web3

    if redeemer[0] == "relayer":
        from py_builder_relayer_client.models import SafeTransaction, OperationType
        relay_client = redeemer[1]
        to_addr, data = _redeem_tx_data(condition_id, neg_risk, up_bal, dn_bal)
        tx = SafeTransaction(to=to_addr, operation=OperationType.Call, data=data, value="0")
        try:
            resp = relay_client.execute([tx], "Redeem positions")
            log.info("  Redeem tx sent via relayer: %s", resp.transaction_id or resp.transaction_hash or "pending")
            result = resp.wait()
            if result is not None:
                log.info("  Redeem CONFIRMED via relayer")
                return True
            log.error("  Redeem FAILED via relayer")
            return False
        except Exception as e:
            log.error("  Redeem relayer error: %s", e)
            return False

    w3, account = redeemer[1]
    cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)
    if neg_risk:
        amounts = [int(up_bal * 1e6), int(dn_bal * 1e6)]
        abi = [{"name": "redeemPositions", "type": "function", "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amounts", "type": "uint256[]"},
        ], "outputs": []}]
        contract = w3.eth.contract(address=Web3.to_checksum_address(NEG_RISK_ADAPTER), abi=abi)
        tx_fn = contract.functions.redeemPositions(cond_bytes, amounts)
    else:
        abi = [{"name": "redeemPositions", "type": "function", "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ], "outputs": []}]
        contract = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=abi)
        tx_fn = contract.functions.redeemPositions(
            Web3.to_checksum_address(USDC_ADDRESS), b"\x00" * 32, cond_bytes, [1, 2]
        )
    return _send_tx(w3, account, tx_fn, 300_000, "Redeem")


# ============================================================================
# == BALANCE — check_token_balance (delegated to prices.py)
# ============================================================================

from selbot.prices import check_token_balance, BalanceCheckError, clear_cache as clear_balance_cache


# ============================================================================
# == REDEEM LOOP — try_merge_all, try_redeem_all
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
        slug = info.get("slug", "")
        title = info.get("title", slug or str(ts))

        if not info.get("closed"):
            if not slug:
                continue
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
        ok = await asyncio.to_thread(merge_market, redeemer, condition_id, merge_amount)
        log_trade("merge", symbol=info.get("symbol"), market_ts=ts, title=title, sets=merge_amount, ok=ok)
        if ok:
            merged.append(ts)
        else:
            info["merge_fails"] = merge_fails + 1
            if merge_fails + 1 >= 5:
                log.warning("  Merge gave up on %s after 5 failures", title[:50])

    if merged:
        log.info("  Merged %d market(s)", len(merged))


async def try_redeem_all(client, redeemer, past_markets: dict):
    """Redeem resolved positions after market closes."""
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
        slug = info.get("slug", "")
        title = info.get("title", slug or str(ts))

        if not info.get("closed"):
            if not slug:
                continue
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
        ok = await asyncio.to_thread(redeem_market, redeemer, condition_id, neg_risk, up_bal, dn_bal)
        log_trade("redeem", symbol=info.get("symbol"), market_ts=ts, title=title, up_bal=up_bal, dn_bal=dn_bal, ok=ok)
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
    """Background loop for trailing hedge checks. 5s default (15-min market)."""
    while not _shutdown:
        try:
            await check_hedge_trailing(client, ws_feed, past_markets)
        except Exception as e:
            log.debug("hedge_loop error: %s", e)
        await asyncio.sleep(HEDGE_LOOP_INTERVAL_S)


async def run():
    global _shutdown, _FILL_DEDUP
    open_trade_log()
    log.info("Initializing SDK...")
    client = init_clob_client()
    redeemer = init_redeemer()

    symbols = tuple(dict.fromkeys(MARKET_SYMBOLS))
    log.info("SDK ready. Placing %.0fc orders on %s 15m markets.", PRICE * 100, "/".join(s.upper() for s in symbols))
    log.info("  Fee params: rate=%.2f exponent=%.0f (max eff. rate=~1.56%% at 50c)",
             _FEE_PARAMS.fee_rate, _FEE_PARAMS.exponent)
    if redeemer:
        mode = redeemer[0] if isinstance(redeemer, tuple) else "direct"
        log.info("On-chain merge/redeem enabled (%s).", "relayer" if mode == "relayer" else "direct Polygon tx")
    else:
        log.info("Merge/redeem DISABLED (missing credentials or RPC).")

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

    placed_markets: dict[str, set[int]] = {sym: set() for sym in symbols}
    past_markets_by_symbol: dict[str, dict[int, dict]] = {sym: {} for sym in symbols}
    last_merge_redeem_ts = 0

    # Start hedge loop as background task (if enabled)
    if HEDGE_ENABLED:
        for sym in symbols:
            asyncio.create_task(hedge_loop(client, ws_feed, past_markets_by_symbol[sym]))
    else:
        log.info("Hedging DISABLED (PLACE_15_HEDGE_ENABLED=0)")

    # Scan recent markets (last 2 hours) for unredeemed positions
    now = int(time.time())
    if FRESH_START:
        log.info("Startup scan skipped (FRESH_START=1).")
    else:
        log.info("Scanning recent markets for unredeemed positions...")
        for sym in symbols:
            scan_ts = (now - 7200) // MARKET_PERIOD * MARKET_PERIOD
            while scan_ts < now:
                slug = get_market_slug(sym, scan_ts)
                try:
                    mkt = await get_market_info(slug)
                    past_markets_by_symbol[sym][scan_ts] = {
                        "symbol": sym,
                        "slug": slug,
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
                        placed_markets[sym].add(scan_ts)
                    if not mkt["closed"]:
                        if not ws_feed.is_connected:
                            await ws_feed.start([mkt["up_token"], mkt["dn_token"]])
                        else:
                            await ws_feed.subscribe([mkt["up_token"], mkt["dn_token"]])
                except Exception:
                    pass
                scan_ts += MARKET_PERIOD
        tracked = sum(len(v) for v in past_markets_by_symbol.values())
        log.info("Found %d recent markets to track for redemption.\n", tracked)

    # Pre-fetch cache for next markets
    next_market_cache: dict[str, dict] = {sym: {"ts": None, "mkt": None} for sym in symbols}

    async def prefetch_next_market(symbol: str, next_ts: int, cache: dict):
        if cache.get("ts") == next_ts:
            return
        try:
            mkt = await get_market_info(get_market_slug(symbol, next_ts))
            cache["ts"] = next_ts
            cache["mkt"] = mkt
        except Exception:
            pass

    def _total_open_count() -> int:
        return sum(
            1
            for _past in past_markets_by_symbol.values()
            for _info in _past.values()
            if not _info.get("redeemed") and not _info.get("bailed") and not _info.get("closed")
        )

    while not _shutdown:
        now = int(time.time())
        timestamps = next_market_timestamps(now)
        next_market_ts = (now // MARKET_PERIOD) * MARKET_PERIOD + MARKET_PERIOD

        # ----------------------------------------------------------------
        # 1. PLACEMENT
        # ----------------------------------------------------------------
        for ts in timestamps:
            for sym_idx, sym in enumerate(symbols):
                if ts in placed_markets[sym]:
                    continue

                slug = get_market_slug(sym, ts)
                cache = next_market_cache[sym]
                if ts == next_market_ts and cache.get("ts") == ts and cache.get("mkt"):
                    mkt = cache["mkt"]
                    cache["ts"] = None
                    cache["mkt"] = None
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
                log.info("MARKET [%s]: %s", sym.upper(), mkt["title"])
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
                    placed_markets[sym].add(ts)
                    past_markets_by_symbol[sym][ts] = {
                        "symbol": sym,
                        "slug": slug,
                        "redeemed": False,
                        "closed": mkt["closed"],
                        "up_token": mkt["up_token"],
                        "dn_token": mkt["dn_token"],
                        "condition_id": mkt["conditionId"],
                        "neg_risk": mkt.get("neg_risk", False),
                        "title": mkt.get("title", slug),
                        "placed_at": ts,
                    }
                    continue

                market_tokens = {mkt["up_token"], mkt["dn_token"]}
                existing = [
                    o for o in (live_orders or [])
                    if o.get("status") == "LIVE" and o.get("asset_id") in market_tokens
                ]
                if existing:
                    log.info("  Already have %d live order(s) — skipping", len(existing))
                    placed_markets[sym].add(ts)
                    past_markets_by_symbol[sym][ts] = {
                        "symbol": sym,
                        "slug": slug,
                        "redeemed": False,
                        "closed": mkt["closed"],
                        "up_token": mkt["up_token"],
                        "dn_token": mkt["dn_token"],
                        "condition_id": mkt["conditionId"],
                        "neg_risk": mkt.get("neg_risk", False),
                        "title": mkt.get("title", slug),
                        "placed_at": ts,
                    }
                    continue

                secs_elapsed = now - ts
                if secs_elapsed > NEW_MARKET_PLACE_WINDOW_SECONDS:
                    log.info(
                        "  Skipping — outside %ds window (%ds elapsed)",
                        NEW_MARKET_PLACE_WINDOW_SECONDS,
                        secs_elapsed,
                    )
                    log_trade("window_missed", symbol=sym, market_ts=ts, slug=slug, elapsed_s=secs_elapsed)
                    placed_markets[sym].add(ts)
                    past_markets_by_symbol[sym][ts] = {
                        "symbol": sym,
                        "slug": slug,
                        "redeemed": False,
                        "closed": mkt["closed"],
                        "up_token": mkt["up_token"],
                        "dn_token": mkt["dn_token"],
                        "condition_id": mkt["conditionId"],
                        "neg_risk": mkt.get("neg_risk", False),
                        "title": mkt.get("title", slug),
                        "placed_at": 0,
                    }
                    continue

                if MAX_OPEN_MARKETS > 0:
                    open_count = _total_open_count()
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

                # Small stagger so BTC/ETH entries are not simultaneous
                if sym_idx > 0 and INTER_ASSET_STAGGER_S > 0:
                    await asyncio.sleep(INTER_ASSET_STAGGER_S)

                market_end = ts + WINDOW_S
                up_id, dn_id = await asyncio.gather(
                    asyncio.to_thread(
                        place_order,
                        client,
                        mkt["up_token"],
                        "UP  ",
                        SHARES_PER_SIDE,
                        entry_price,
                        market_end,
                    ),
                    asyncio.to_thread(
                        place_order,
                        client,
                        mkt["dn_token"],
                        "DOWN",
                        SHARES_PER_SIDE,
                        entry_price,
                        market_end,
                    ),
                )

                placed = (1 if up_id else 0) + (1 if dn_id else 0)
                log.info("  Done: %d orders placed. Max cost: $%.2f\n", placed, SHARES_PER_SIDE * entry_price * 2)
                log_trade(
                    "orders_placed",
                    symbol=sym,
                    market_ts=ts,
                    slug=slug,
                    title=mkt.get("title", slug),
                    price=entry_price,
                    shares=SHARES_PER_SIDE,
                    up_id=up_id,
                    dn_id=dn_id,
                    sides_placed=placed,
                    elapsed_s=now - ts,
                    dynamic=ENTRY_PRICE_DYNAMIC,
                )

                if placed == 0:
                    log.info("  Both orders returned null — will retry next loop.")
                    continue

                placed_markets[sym].add(ts)
                past_markets_by_symbol[sym][ts] = {
                    "symbol": sym,
                    "slug": slug,
                    "redeemed": False,
                    "closed": mkt["closed"],
                    "up_token": mkt["up_token"],
                    "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"],
                    "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug),
                    "placed_at": ts,
                    "position": PositionState(),
                }

        # Log WS prices for active markets
        for sym in symbols:
            for ts, info in past_markets_by_symbol[sym].items():
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
                    log.info(
                        "  [%s %d +%ds] WS: UP=%.2f  DN=%.2f%s",
                        sym.upper(),
                        ts,
                        elapsed,
                        up_ask or 0,
                        dn_ask or 0,
                        hedge_state,
                    )

        # ----------------------------------------------------------------
        # 2. WS HEALTH CHECK
        # ----------------------------------------------------------------
        ws_age = ws_feed.last_message_age
        if ws_age > 90.0 and ws_feed.is_connected:
            log.warning("WS feed stale (%.0fs since last message) — forcing reconnect", ws_age)
            await ws_feed.force_reconnect()

        # ----------------------------------------------------------------
        # 3. STALE CANCEL
        # ----------------------------------------------------------------
        for sym in symbols:
            for _ts, _info in past_markets_by_symbol[sym].items():
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
                        log.info("  STALE-CANCEL: %s market %d (%ds elapsed)", sym.upper(), _ts, now - _placed)
                        cancel_market_orders(client, _tokens)
                        _info["stale_cancelled"] = True
                        log_trade("stale_cancel", symbol=sym, market_ts=_ts, elapsed_s=now - _placed)

        # ----------------------------------------------------------------
        # 4. BAIL CHECK
        # ----------------------------------------------------------------
        for sym in symbols:
            await check_bail_out(client, ws_feed, past_markets_by_symbol[sym])

        # ----------------------------------------------------------------
        # 5. MERGE + REDEEM (throttled to every 30s)
        # ----------------------------------------------------------------
        if now - last_merge_redeem_ts >= MERGE_REDEEM_INTERVAL_S:
            for sym in symbols:
                await try_merge_all(client, redeemer, past_markets_by_symbol[sym])
                await try_redeem_all(client, redeemer, past_markets_by_symbol[sym])
            last_merge_redeem_ts = now

        # ----------------------------------------------------------------
        # 6. ADAPTIVE SLEEP + CLEANUP (15-min market — no need to poll aggressively)
        # ----------------------------------------------------------------
        seconds_until = next_market_ts - now
        sleep_s = 2 if seconds_until <= 60 else (5 if seconds_until <= 300 else 10)

        if seconds_until <= NEXT_MARKET_PREFETCH_SEC and seconds_until > 0:
            for sym in symbols:
                asyncio.create_task(prefetch_next_market(sym, next_market_ts, next_market_cache[sym]))
        await asyncio.sleep(sleep_s)

        log.info("[Next market in %ds] Checked %d markets x %d symbols", seconds_until, len(timestamps), len(symbols))

        # Cleanup: hard TTL 6h, normal TTL 2h for redeemed
        HARD_TTL_S = 21600
        NORMAL_TTL_S = 7200
        for sym in symbols:
            stale = []
            for ts, info in past_markets_by_symbol[sym].items():
                age = now - ts
                if age > HARD_TTL_S:
                    if not info.get("redeemed"):
                        log.warning("  Hard TTL: dropping %s market %d after %ds (unredeemed)", sym.upper(), ts, age)
                        log_trade("hard_ttl_drop", symbol=sym, market_ts=ts, age_s=age)
                    stale.append(ts)
                elif age > NORMAL_TTL_S and info.get("redeemed"):
                    stale.append(ts)
            for ts in stale:
                del past_markets_by_symbol[sym][ts]
            placed_markets[sym] = {ts for ts in placed_markets[sym] if ts > now - 2 * MARKET_PERIOD}

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
