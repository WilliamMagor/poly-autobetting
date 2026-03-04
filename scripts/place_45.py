"""
BTC 15-minute dual-side arbitrage bot for Polymarket.

Strategy: Buy both UP and DOWN at ~46c each. If both fill, cost <$1 for
guaranteed $1 payout. Rotates every 15 minutes, runs 24/7.

Usage: python scripts/place_45.py

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
  == ON-CHAIN        merge_market, redeem_market (Polygon transactions)
  == BALANCE         check_token_balance + TTL cache
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
import sys
import threading
import time

import httpx

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.bot.ws_book_feed import WSBookFeed
from src.bot.math_engine import FeeParams, taker_fee_usdc, effective_fee_rate
from src.bot.types import PositionState
from src.bot.fill_monitor import FillDeduplicator

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("place15")

# ============================================================================
# == TRADE LOG — JSONL logging with day rotation
# ============================================================================
_trade_log = None   # file handle, opened in run()
_trade_log_date = "" # date string for current file (for day rollover)
_trade_log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")

def open_trade_log():
    """Open (or create) today's JSONL trade log. Called once at startup."""
    global _trade_log, _trade_log_date
    os.makedirs(_trade_log_dir, exist_ok=True)
    _trade_log_date = time.strftime("%Y%m%d")
    path = os.path.join(_trade_log_dir, f"place_15_{_trade_log_date}.jsonl")
    _trade_log = open(path, "a", buffering=1)  # line-buffered
    log.info("Trade log: %s", path)

def _rotate_trade_log_if_needed():
    """Reopen log file when the date rolls over (24/7 operation)."""
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
    """Append one structured event to today's JSONL trade log."""
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
WINDOW_S = 900             # 15 minutes (single source of truth for all timing)

# --- Config (env-overridable with PLACE_15_ prefix; falls back to old PLACE_45_ vars) ---
def _env(key_15: str, key_45_fallback: str, default: str) -> str:
    """Read PLACE_15_* env var, fall back to old PLACE_45_* for backwards compat."""
    return os.getenv(key_15, os.getenv(key_45_fallback, default))

PRICE = float(_env("PLACE_15_ENTRY_PRICE", "PLACE_45_ENTRY_PRICE", "0.46"))
SHARES_PER_SIDE = int(_env("PLACE_15_SHARES", "PLACE_45_SHARES", "10"))
GAMMA_URL = "https://gamma-api.polymarket.com"
MARKET_PERIOD = WINDOW_S   # alias — all timing derives from WINDOW_S
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# --- Dynamic entry price (optional) ---
ENTRY_PRICE_DYNAMIC = _env("PLACE_15_ENTRY_PRICE_DYNAMIC", "", "0").strip().lower() in ("1", "true", "yes")
ENTRY_PRICE_VOL_HIGH = float(_env("PLACE_15_ENTRY_VOL_HIGH", "", "0.45"))   # widen when vol high
ENTRY_PRICE_VOL_LOW  = float(_env("PLACE_15_ENTRY_VOL_LOW", "", "0.47"))    # tighten when vol low

# --- Trailing hedge config (re-tuned for 15-minute windows) ---
# Only place initial orders within the first N seconds of a market period.
NEW_MARKET_PLACE_WINDOW_SECONDS = int(_env("PLACE_15_PLACE_WINDOW_S", "", "90"))
# After one side fills, cancel unfilled limit and trail the cheap side.
# Time windows (seconds elapsed since MARKET START):
HEDGE_START_AFTER_S = int(_env("PLACE_15_HEDGE_START_S", "", "240"))   # 4-min INTO 15-min market: start acting
# Note: fills are detected from t=0 and trailing floor is tracked immediately,
# but no cancel/buy actions happen until HEDGE_START_AFTER_S.
# These are phases WITHIN the 15-minute market, not market durations:
#   Phase 1 (0-240s / 0-4min):   Detect fills, track floor. No action — let other side fill.
#   Phase 2 (240-600s / 4-10min): Cancel unfilled limit, trail with tight band.
#   Phase 3 (600-720s / 10-12min): Widen band. Keep trailing.
#   Phase 4 (720s+ / 12-15min):   Emergency — force-buy or sell filled side.
HEDGE_MID_AFTER_S   = int(_env("PLACE_15_HEDGE_MID_S", "", "600"))    # 10-min: widen band
HEDGE_LATE_AFTER_S  = int(_env("PLACE_15_HEDGE_LATE_S", "", "720"))   # 12-min: force-buy window
# Band thresholds: PRICE + hedge_ask must stay below 1.00 to be profitable.
# Relaxed for 15m: more room for vol to create hedge opportunities.
HEDGE_BAND_EARLY = 1.0 - PRICE - 0.06   # conservative (e.g., 0.48 — combined 0.94)
HEDGE_BAND_MID   = 1.0 - PRICE - 0.01   # moderate (e.g., 0.53 — combined 0.99)
HEDGE_BAND_LATE  = 1.0 - PRICE          # wide (e.g., 0.54 — breakeven)
# Trailing delta: buy when ask bounces this much above its lowest point.
HEDGE_TRAILING_DELTA = float(_env("PLACE_15_HEDGE_TRAILING_DELTA", "", "0.03"))
# Force-buy threshold: if the FILLED side's ask reached this level,
# the unfilled side is very cheap → buy at market regardless of trailing.
HEDGE_FORCE_BUY_FILLED_THRESHOLD = float(_env("PLACE_15_HEDGE_FORCE_THRESHOLD", "", "0.85"))
# Hedge EV cap: never hedge when hedge_ask + PRICE > this (accounts for taker fees).
# At PRICE=0.46, 0.98 means hedge_ask must be < 0.52 for the guard to pass.
HEDGE_MAX_COMBINED = float(_env("PLACE_15_HEDGE_MAX_COMBINED", "", "0.98"))

# --- EV-based bail schedule (relaxed for volatile 15-minute BTC markets) ---
# Each entry: (seconds_remaining_threshold, bail_price_threshold)
# Bail only when the *unfilled* side's ask exceeds threshold.
# Relaxed vs 5m: more time for prices to swing back, so we wait longer.
BAIL_SCHEDULE = [
    (300, 999.0),   # >5 min remaining: never bail (let volatility play out)
    (120, 0.85),    # 2-5 min remaining: bail only on extreme (unfilled side 85c+)
    (30,  0.75),    # 30s-2 min remaining: moderate threshold
    (0,   0.65),    # <30s remaining: emergency exit
]

# --- Stale order cancellation ---
STALE_CANCEL_S = int(_env("PLACE_15_STALE_CANCEL_S", "", "660"))  # cancel unfilled orders at 11min (660s)

# --- Fresh start flag ---
# Set PLACE_15_FRESH_START=1 to skip the startup scan and clear all caches.
# Use this when redeploying to a new VPS or after a long gap — avoids stale state
# from old runs (mismatched market timestamps, wrong clock, stale balance cache).
FRESH_START = _env("PLACE_15_FRESH_START", "", "0").strip().lower() in ("1", "true", "yes")

# --- Risk limits (optional) ---
MAX_OPEN_MARKETS = int(_env("PLACE_15_MAX_OPEN_MARKETS", "PLACE_45_MAX_OPEN_MARKETS", "0"))
HEDGE_USE_REST_FALLBACK = _env("PLACE_15_REST_FALLBACK", "PLACE_45_REST_FALLBACK", "0").strip().lower() in ("1", "true", "yes")

# --- Integrity / heartbeat (reserved for runner.py / session_loop.py integration) ---
INTEGRITY_INTERVAL_S = int(_env("PLACE_15_INTEGRITY_INTERVAL_S", "", "3"))
HEARTBEAT_INTERVAL_S = int(_env("PLACE_15_HEARTBEAT_INTERVAL_S", "", "5"))

# --- Fee parameters (Polymarket CLOB taker fee model) ---
# BTC crypto markets: feeRate=0.25, exponent=2. Max effective rate ~1.56% at 50c.
# Maker orders (our limit buys) pay ZERO fee — only taker hedges incur fees.
_FEE_PARAMS = FeeParams.for_crypto()

# --- Fill deduplicator (crash-safe, persisted between restarts) ---
_FILL_DEDUP: FillDeduplicator | None = None  # initialized in run()

# --- Speed: pre-fetch next market when this many seconds until start
NEXT_MARKET_PREFETCH_SEC = 120  # 2 min before (longer window = more time to prefetch)


# ============================================================================
# == HELPERS — Slug builder, validators
# ============================================================================


def next_market_timestamps(now_ts: int) -> list[int]:
    """Return [currently_trading, next, after_that] 15m market timestamps.

    The slug timestamp is the START time of the market.
    """
    ts = (now_ts // MARKET_PERIOD) * MARKET_PERIOD
    return [ts - MARKET_PERIOD, ts, ts + MARKET_PERIOD]


def get_market_slug(timestamp: int) -> str:
    """Return the market slug for a given timestamp (15-minute BTC market)."""
    return f"btc-updown-15m-{timestamp}"


def _validate_token_id(token_id: str, field_name: str) -> str:
    """Validate that a token ID is a non-empty hex string."""
    if not token_id or not isinstance(token_id, str):
        raise ValueError(f"Invalid {field_name}: expected non-empty string, got {token_id}")
    # Polymarket token IDs are typically numeric strings
    if not token_id.isdigit():
        raise ValueError(f"Invalid {field_name}: expected numeric string, got {token_id}")
    return token_id


def _validate_condition_id(condition_id: str) -> str:
    """Validate that a condition ID is a valid Ethereum-style hex string."""
    if not condition_id or not isinstance(condition_id, str):
        raise ValueError(f"Invalid conditionId: expected non-empty string, got {condition_id}")
    if not condition_id.startswith("0x"):
        raise ValueError(f"Invalid conditionId: must start with 0x, got {condition_id}")
    if len(condition_id) < 64:
        raise ValueError(f"Invalid conditionId: too short, got {condition_id}")
    return condition_id


# ============================================================================
# == MARKET API — get_market_info (Gamma API), httpx client
# ============================================================================

# Persistent HTTP client — reuses connections across calls (faster than per-call).
# Lazy-initialized on first use, closed on shutdown.
import re
_http_client: httpx.AsyncClient | None = None
_SLUG_RE = re.compile(r"^btc-updown-15m-\d+$")


def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        # Browser-like headers required on VPS/datacenter IPs where Cloudflare
        # guards Gamma API — without these it returns an HTML challenge page
        # instead of JSON, causing r.json() to throw a JSONDecodeError.
        _http_client = httpx.AsyncClient(
            timeout=15,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://polymarket.com/",
                "Origin": "https://polymarket.com",
            },
        )
    return _http_client


def _check_json_response(r) -> None:
    """Raise a clear error if the API returned HTML instead of JSON.

    Cloudflare (common on VPS IPs) returns HTML challenge/block pages with
    status 200 or 403. Without this check the error is a cryptic JSONDecodeError.
    """
    ct = r.headers.get("content-type", "")
    body_start = r.text[:100].lstrip()
    if "html" in ct.lower() or body_start.startswith("<!"):
        raise ValueError(
            f"Gamma API returned HTML instead of JSON "
            f"(Cloudflare block? VPS IP may need whitelisting). "
            f"status={r.status_code} content-type={ct!r} body={r.text[:120]!r}"
        )


def _parse_gamma_market(m: dict) -> dict | None:
    """Parse a single Gamma market dict into our internal format.

    Returns dict with up_token, dn_token, etc., or None if parsing fails.
    Shared by both slug-lookup and fallback-scan paths.
    """
    from datetime import datetime

    tokens_raw = m.get("clobTokenIds", [])
    tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
    if not isinstance(tokens, list) or len(tokens) < 2:
        return None

    condition_id = m.get("conditionId", "")
    if not condition_id or not condition_id.startswith("0x") or len(condition_id) < 64:
        return None

    fee_rate_bps = None
    try:
        fee_raw = m.get("fee") or m.get("feeRateBps") or m.get("fee_rate_bps")
        if fee_raw is not None:
            fee_rate_bps = float(fee_raw)
    except (ValueError, TypeError):
        pass

    return {
        "up_token": str(tokens[0]),
        "dn_token": str(tokens[1]),
        "title": m.get("question", m.get("slug", "")),
        "conditionId": condition_id,
        "closed": bool(m.get("closed", False)),
        "neg_risk": bool(m.get("negRisk", False)),
        "fee_rate_bps": fee_rate_bps,
    }


async def _fallback_market_scan(ts: int) -> dict | None:
    """Scan active markets to find BTC 15-min market at given slot timestamp.

    Gamma's slug index sometimes lags by minutes at market open.
    This is the fallback used by market_scheduler.py for the same reason.
    Scans /markets?active=true and filters by BTC + 15-min + slot match.
    """
    from datetime import datetime
    c = _get_http_client()
    try:
        r = await c.get(f"{GAMMA_URL}/markets", params={
            "limit": 200,
            "active": "true",
            "closed": "false",
            "order": "endDate",
            "ascending": "true",
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
        is_btc = "btc" in question or "btc" in mslug or "bitcoin" in question
        is_15m = "15m" in mslug or "15 min" in question or "updown" in mslug
        if not (is_btc and is_15m):
            continue

        # Validate duration matches WINDOW_S (~900s).
        # IMPORTANT: use eventStartTime (actual 15-min window open), NOT startDate
        # which is the event CREATION date (days ago → gives 800000s "duration").
        start_date = m.get("eventStartTime") or m.get("startDate") or m.get("start_date_iso") or ""
        end_date = m.get("endDate") or m.get("end_date_iso") or ""
        if not start_date or not end_date:
            continue
        try:
            t_start = datetime.fromisoformat(start_date.replace("Z", "+00:00")).timestamp()
            t_end = datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        duration = t_end - t_start
        if abs(duration - WINDOW_S) > 60:
            log.debug("Fallback skip: duration=%.0fs (expected %ds) for %s", duration, WINDOW_S, mslug)
            continue  # not a 15-min market
        if abs(int(t_start) - ts) > 60:
            log.debug("Fallback skip: slot mismatch t_start=%d expected ts=%d for %s", int(t_start), ts, mslug)
            continue  # doesn't match expected time slot

        result = _parse_gamma_market(m)
        if result:
            log.info("Fallback market discovery: %s", m.get("question", mslug)[:80])
            return result

    return None


async def get_market_info(slug: str) -> dict:
    """Fetch market info from Gamma API with input validation.

    Primary path: slug-based /events lookup.
    Fallback path: active market scan when Gamma's slug index lags
    (this is common at market open — see market_scheduler.py for context).
    """
    if not slug or not isinstance(slug, str):
        raise ValueError(f"Invalid slug: expected non-empty string, got {slug}")
    if len(slug) > 200:
        raise ValueError(f"Invalid slug: too long (max 200 chars), got {len(slug)}")
    if not _SLUG_RE.match(slug):
        raise ValueError(f"Invalid slug format: expected btc-updown-15m-<timestamp>, got {slug}")

    c = _get_http_client()
    r = await c.get(f"{GAMMA_URL}/events", params={"slug": slug})
    r.raise_for_status()
    _check_json_response(r)
    data = r.json()

    if not data:
        # Gamma slug index sometimes lags — try active market scan as fallback
        ts = int(slug.rsplit("-", 1)[-1])
        log.info("Slug %s not indexed yet — trying fallback scan (ts=%d)", slug, ts)
        fallback = await _fallback_market_scan(ts)
        if fallback:
            return fallback
        raise ValueError(f"No event found for slug: {slug} (fallback scan also failed)")

    # Validate response structure
    if not isinstance(data, list) or len(data) == 0:
        raise ValueError(f"Invalid API response: expected non-empty list, got {type(data)}")
    if "markets" not in data[0]:
        raise ValueError(f"Invalid API response: missing 'markets' field")
    if not isinstance(data[0]["markets"], list) or len(data[0]["markets"]) == 0:
        raise ValueError(f"Invalid API response: empty markets list")

    m = data[0]["markets"][0]

    # Validate required fields exist
    if "clobTokenIds" not in m:
        raise ValueError(f"Invalid market: missing 'clobTokenIds' field")

    tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
    if not isinstance(tokens, list) or len(tokens) < 2:
        raise ValueError(f"Invalid clobTokenIds: expected list of 2 tokens, got {tokens}")

    # Validate token IDs
    up_token = _validate_token_id(tokens[0], "up_token")
    dn_token = _validate_token_id(tokens[1], "dn_token")

    # Validate and extract conditionId
    condition_id = m.get("conditionId", "")
    if condition_id:
        condition_id = _validate_condition_id(condition_id)

    # --- Duration sanity check (must be ~900s for 15-minute markets) ---
    # ONLY use eventStartTime — the actual 15-min window open time.
    # Do NOT fall back to startDate (event CREATION date, days ago → ~800000s "duration").
    # If eventStartTime is absent, skip the check entirely rather than false-failing.
    end_date = m.get("endDate") or m.get("end_date_iso") or ""
    event_start_time = m.get("eventStartTime") or ""
    if not event_start_time:
        log.debug("eventStartTime absent for %s — skipping duration check", slug)
    elif end_date:
        from datetime import datetime
        try:
            t_end = datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp()
            t_start = datetime.fromisoformat(event_start_time.replace("Z", "+00:00")).timestamp()
            duration = t_end - t_start
            if abs(duration - WINDOW_S) > 60:  # allow 1 min tolerance
                log.warning("wrong_market_duration: %.0fs (expected %ds) for %s", duration, WINDOW_S, slug)
                log_trade("wrong_market_duration", slug=slug, duration=duration, expected=WINDOW_S)
                raise ValueError(f"Market duration {duration}s != expected {WINDOW_S}s")
        except (ValueError, TypeError) as e:
            if "duration" in str(e):
                raise  # re-raise our own duration error
            # Parse error — log but don't block (dates may be in different format)
            log.debug("Could not parse market dates for duration check: %s", e)

    # --- Extract fee_rate_bps if available (dynamic per-token fees) ---
    fee_rate_bps = None
    try:
        fee_raw = m.get("fee") or m.get("feeRateBps") or m.get("fee_rate_bps")
        if fee_raw is not None:
            fee_rate_bps = float(fee_raw)
    except (ValueError, TypeError):
        pass

    return {
        "up_token": up_token,
        "dn_token": dn_token,
        "title": m.get("question", slug),
        "conditionId": condition_id,
        "closed": bool(m.get("closed", False)),
        "neg_risk": bool(m.get("negRisk", False)),
        "fee_rate_bps": fee_rate_bps,
    }


# ============================================================================
# == SDK INIT — CLOB client + on-chain redeemer setup
# ============================================================================


def init_clob_client():
    """Initialize py-clob-client SDK."""
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds

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

    # Derive API credentials from private key (more reliable than stored creds)
    creds = client.derive_api_key()
    log.info("Derived API creds from private key")
    client.set_api_creds(creds)
    return client


def init_redeemer():
    """Initialize direct on-chain redeemer. Returns (w3, account) or None."""
    from web3 import Web3
    from eth_account import Account

    pk = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    if not pk:
        log.warning("No POLYMARKET_PRIVATE_KEY — auto-redeem disabled")
        return None

    polygon_rpc = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
    w3 = Web3(Web3.HTTPProvider(polygon_rpc))
    if not w3.is_connected():
        log.warning("Cannot connect to Polygon RPC (%s) — auto-redeem disabled", polygon_rpc)
        return None

    account = Account.from_key(pk)
    return (w3, account)


# ============================================================================
# == ORDERS — place_order, sell_at_bid, buy_hedge
# ============================================================================


def place_order(client, token_id: str, side_label: str, shares: float, price: float):
    """Place a single limit buy order. Returns order ID or None."""
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    expiration = int(time.time()) + WINDOW_S  # expire with market (15 min)
    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=round(shares, 1),
        side=BUY,
        expiration=expiration,
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
            time.sleep(1)  # shorter delay for faster retry at market open
    log.error("  %s order failed after 3 attempts", side_label)
    return None


def sell_at_bid(client, token_id: str, shares: float, side_label: str,
                max_attempts: int = 3, check_interval: float = 2.0) -> str | None:
    """Place a limit SELL at the current best bid, with retry if unfilled.

    After placing, waits check_interval seconds and checks if the order is
    still LIVE. If so, cancels and re-places at the new best bid. Repeats
    up to max_attempts times to handle bid movement.
    """
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
                    log.info("  %s SELL filled (status=%s, matched=%.1f)", side_label, status, matched)
                    return oid
            except Exception:
                return oid

            log.info("  %s SELL still unfilled — cancelling to re-place at new bid", side_label)
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
    """Place a limit BUY for the hedge side (post-one-sided-fill).

    Uses the same limit-order path as the original placement but at
    the current market price rather than the fixed entry price.
    Retries up to 3 times with 2s delay.
    Returns order ID or None on failure.
    """
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    expiration = int(time.time()) + WINDOW_S  # expire with market
    order_args = OrderArgs(
        token_id=token_id,
        price=round(price, 2),
        size=round(shares, 1),
        side=BUY,
        expiration=expiration,
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
            time.sleep(1)  # 1s retry (speed matters for hedge timing)
    log.error("  HEDGE %s order failed after 3 attempts", side_label)
    return None


# ============================================================================
# == HEDGE ENGINE — 4-phase trailing-stop hedge for one-sided fills
# ============================================================================


async def check_hedge_trailing(client, ws_feed: "WSBookFeed", past_markets: dict):
    """Time-windowed trailing-stop hedge for one-sided fills (15-minute markets).

    Phased approach for 15m windows:
      Phase 1 (0 – 240s): Detect fills and track trailing floor, but take no
        hedge action. Gives unfilled limit time to fill naturally.
      Phase 2 (240s – 600s): Cancel unfilled limit, start trailing hedge with
        tight band (HEDGE_BAND_EARLY). EV guard blocks unprofitable hedges.
      Phase 3 (600s – 720s): Widen band to HEDGE_BAND_MID.
      Phase 4 (>720s): HEDGE_BAND_LATE. Force-buy eligible. EV guard relaxed.

    State stored per market in past_markets:
      hedge_side:        "up" | "dn" | None  — which side needs to be hedged
      filled_bal:        float — balance of the filled side at detection time
      hedge_cancelled:   bool — unfilled limit was cancelled
      hedge_done:        bool — hedge buy was placed or filled side sold
      trailing_min_ask:  float | None — lowest ask seen for unfilled side
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

        # --- Step 1: Detect one-sided fill (no time gate — start tracking early) ---
        if not info.get("hedge_side"):
            try:
                up_bal = check_token_balance(client, up_token)
                dn_bal = check_token_balance(client, dn_token)
            except Exception:
                continue

            if up_bal > 0 and dn_bal == 0:
                hedge_side, filled_bal = "dn", up_bal
                filled_side_label = "UP"
            elif dn_bal > 0 and up_bal == 0:
                hedge_side, filled_bal = "up", dn_bal
                filled_side_label = "DN"
            else:
                continue

            # Fill deduplication: generate a synthetic fill ID from the market
            # state. Prevents the same one-sided fill from being re-processed
            # on restart (balance persists across restarts, hedge state doesn't).
            fill_id = f"fill-{ts}-{filled_side_label}-{int(filled_bal * 100)}"
            if _FILL_DEDUP is not None and not _FILL_DEDUP.process(fill_id):
                # Already processed this fill — hedge state must have been lost
                # on restart. Restore from balance.
                info["hedge_side"] = hedge_side
                info["filled_bal"] = filled_bal
                info["trailing_min_ask"] = None
                log.info(
                    "  HEDGE restored one-sided fill at %ds: %s filled (dedup: fill already known)",
                    time_elapsed, filled_side_label,
                )
                continue

            # Update per-market PositionState for risk tracking
            pos: PositionState | None = info.get("position")
            if pos is not None:
                if filled_side_label == "UP":
                    pos.up_shares = float(up_bal)
                    pos.up_cost = float(up_bal) * PRICE
                else:
                    pos.down_shares = float(dn_bal)
                    pos.down_cost = float(dn_bal) * PRICE

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

        # --- Step 2: Track trailing minimum from fill detection ---
        hedge_ask = get_best_ask_safe(ws_feed, client, hedge_token)
        if hedge_ask is None:
            continue

        prev_min = info.get("trailing_min_ask")
        if prev_min is None or hedge_ask < prev_min:
            info["trailing_min_ask"] = hedge_ask
            if prev_min is not None:
                log.info("  HEDGE trail: new low for %s ask=%.3f", hedge_side.upper(), hedge_ask)

        trailing_min = info["trailing_min_ask"]

        # Phase 1: Grace period — wait for natural dual fill
        if time_elapsed < HEDGE_START_AFTER_S:
            continue

        # --- Step 3: Cancel unfilled limit once ---
        if not info.get("hedge_cancelled"):
            log.info("  HEDGE cancelling unfilled %s limit at %ds", hedge_side.upper(), time_elapsed)
            cancel_market_orders(client, {hedge_token})
            info["hedge_cancelled"] = True

        # --- Step 4: Select band based on phase ---
        if time_elapsed >= HEDGE_LATE_AFTER_S:
            band = HEDGE_BAND_LATE
            # Force-buy: filled side at 0.85+ means hedge side is very cheap
            filled_ask = get_best_ask_safe(ws_feed, client, filled_token)
            if filled_ask is not None and filled_ask >= HEDGE_FORCE_BUY_FILLED_THRESHOLD:
                log.info(
                    "  HEDGE FORCE-BUY: filled side ask=%.2f >= %.2f, hedge %s ask=%.3f (remaining=%ds)",
                    filled_ask, HEDGE_FORCE_BUY_FILLED_THRESHOLD,
                    hedge_side.upper(), hedge_ask, time_remaining,
                )
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

        # --- Step 5: EV guard — never hedge if fee-adjusted EV is negative ---
        # Taker fee for the hedge order (maker entry = zero fee; hedge = taker fee).
        # Formula: fee = shares * price * feeRate * (price * (1-price))^exponent
        combined = PRICE + hedge_ask
        try:
            hedge_fee = taker_fee_usdc(SHARES_PER_SIDE, hedge_ask, _FEE_PARAMS)
            eff_rate = effective_fee_rate(hedge_ask, _FEE_PARAMS)
        except ValueError:
            hedge_fee, eff_rate = 0.0, 0.0
        hedge_ev = SHARES_PER_SIDE * (1.0 - PRICE - hedge_ask) - hedge_fee
        if (combined > HEDGE_MAX_COMBINED or hedge_ev <= 0) and band != HEDGE_BAND_LATE:
            log.info(
                "  HEDGE EV-skip: combined=%.3f cap=%.2f fee=%.4f (rate=%.2f%%) "
                "EV=%.4f (elapsed=%ds)",
                combined, HEDGE_MAX_COMBINED, hedge_fee, eff_rate * 100,
                hedge_ev, time_elapsed,
            )
            continue

        # --- Step 5b: Valid-bounce check ---
        valid_bounce = trailing_min is not None and trailing_min < (band - HEDGE_TRAILING_DELTA)

        # --- Step 5c: Sell filled side if hedge is uneconomical (late phase only) ---
        # Relaxed for 15m: only give up and sell in the late window. Before that,
        # keep trailing — volatile BTC markets often swing back.
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
                log_trade("hedge_sell_filled", market_ts=ts, hedge_side=hedge_side.upper(),
                          hedge_ask=hedge_ask, band=band, filled_bal=sell_bal,
                          elapsed_s=time_elapsed, remaining_s=time_remaining)
                cancel_market_orders(client, {hedge_token})
                sell_at_bid(client, filled_token, sell_bal, filled_label)
                info["hedge_done"] = True
                info["bailed"] = True
                continue
            else:
                log.info(
                    "  HEDGE wait: %s ask=%.3f >= band=%.2f (elapsed=%ds, remaining=%ds)",
                    hedge_side.upper(), hedge_ask, band, time_elapsed, time_remaining,
                )
                continue

        # --- Step 6: Dynamic bounce threshold — relax as time runs out ---
        # Spans from HEDGE_START_AFTER_S to HEDGE_LATE_AFTER_S
        span = max(1, HEDGE_LATE_AFTER_S - HEDGE_START_AFTER_S)
        progress = min(1.0, (time_elapsed - HEDGE_START_AFTER_S) / span)
        min_bounce = HEDGE_TRAILING_DELTA * (1.0 - progress * 0.67)

        # --- Step 7: Trailing-stop trigger — buy on bounce from floor ---
        if trailing_min is None:
            continue
        bounce = hedge_ask - trailing_min
        log.info(
            "  HEDGE trail: %s ask=%.3f low=%.3f bounce=%.3f/%.3f (elapsed=%ds band=%.2f remaining=%ds)",
            hedge_side.upper(), hedge_ask, trailing_min,
            bounce, min_bounce, time_elapsed, band, time_remaining,
        )
        triggered = bounce >= min_bounce or valid_bounce
        if triggered:
            reason = "valid_bounce" if valid_bounce else "trailing"
            log.info(
                "  HEDGE TRIGGER: %s bounced %.3f -> %.3f (delta=%.3f >= %.3f%s)",
                hedge_side.upper(), trailing_min, hedge_ask, bounce, min_bounce,
                ", valid-bounce" if valid_bounce else "",
            )
            log_trade("hedge_trigger", market_ts=ts, hedge_side=hedge_side.upper(),
                      trail_min=round(trailing_min, 4), ask=round(hedge_ask, 4),
                      bounce=round(bounce, 4), min_bounce=round(min_bounce, 4),
                      band=band, reason=reason, elapsed_s=time_elapsed,
                      remaining_s=time_remaining)
            try:
                current_bal = check_token_balance(client, hedge_token)
            except Exception:
                continue  # can't confirm balance — retry next tick
            if current_bal >= SHARES_PER_SIDE - 0.01:
                log.info(
                    "  HEDGE skipped: already hold %.1f %s shares",
                    current_bal, hedge_side.upper(),
                )
                log_trade("hedge_balance_skip", market_ts=ts,
                          hedge_side=hedge_side.upper(), current_bal=current_bal)
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


def get_best_ask_safe(ws_feed: "WSBookFeed", client, token_id: str) -> float | None:
    """Best ask from WS; if None and HEDGE_USE_REST_FALLBACK, fetch from REST order book."""
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
    """EV-based bail schedule for one-sided fills (15-minute markets).

    Uses BAIL_SCHEDULE: list of (seconds_remaining_threshold, bail_price).
    Bail fires when the *other* side's ask exceeds the threshold for the
    current time band AND we only hold one side (naked directional risk).
    """
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

        # Determine bail threshold from schedule
        bail_price = 999.0  # default: no bail
        for threshold_remaining, threshold_price in BAIL_SCHEDULE:
            if time_remaining >= threshold_remaining:
                bail_price = threshold_price
                break

        if bail_price >= 999.0:
            continue  # bail disabled for this time band

        # Get prices from WS feed (or REST fallback if enabled)
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
            log.info("  BAIL: DN ask=%.2f > %.2f (remaining=%ds). Selling UP %.0f shares",
                     dn_ask, bail_price, time_remaining, up_bal)
            log_trade("bail", market_ts=ts, trigger_side="DN", trigger_ask=dn_ask,
                      bail_price=bail_price, sell_side="UP", sell_bal=up_bal,
                      remaining_s=time_remaining)
            cancel_market_orders(client, {up_token, dn_token})
            sell_at_bid(client, up_token, up_bal, "UP")
            bail = True
        elif up_ask > bail_price and up_bal == 0 and dn_bal > 0:
            log.info("  BAIL: UP ask=%.2f > %.2f (remaining=%ds). Selling DN %.0f shares",
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


def merge_market(redeemer, condition_id: str, amount: float) -> bool:
    """Merge complete sets (1 UP + 1 DN) into USDC on-chain. Can be done before or after resolution.
    amount = number of full sets to merge (min of UP and DN balances). Returns True on success."""
    from web3 import Web3

    w3, account = redeemer
    cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)
    amount_wei = int(amount * 1e6)  # USDC 6 decimals
    if amount_wei <= 0:
        return False

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


def redeem_market(redeemer, condition_id: str, neg_risk: bool, up_bal: float = 0.0, dn_bal: float = 0.0) -> bool:
    """Redeem resolved positions via direct on-chain Polygon transaction. Returns True on success."""
    from web3 import Web3

    w3, account = redeemer
    cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)

    if neg_risk:
        NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
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
# == BALANCE — check_token_balance + TTL cache
# ============================================================================


class BalanceCheckError(Exception):
    """Raised when balance check fails — callers should retry, not assume 0."""
    pass


# Simple TTL cache for balance checks.
# hedge_loop calls check_token_balance every 1s per market — without caching
# that's 2+ API calls/sec per active market, 24/7.  A 3s TTL cuts traffic ~67%
# while keeping responses fresh enough for 15-minute markets.
_balance_cache: dict[str, tuple[float, float]] = {}  # token_id → (balance, expiry_ts)
BALANCE_CACHE_TTL = 3.0  # seconds


def check_token_balance(client, token_id: str, skip_cache: bool = False) -> float:
    """Check CTF token balance via CLOB API (returns shares).

    Uses a 3-second TTL cache to reduce API spam from the 1s hedge loop.
    Pass skip_cache=True for critical paths (e.g., right before placing an order).

    Raises BalanceCheckError on API failure so callers don't confuse
    'API down' with 'no position' — which would cause false hedge triggers.
    """
    now = time.time()
    if not skip_cache and token_id in _balance_cache:
        cached_bal, expiry = _balance_cache[token_id]
        if now < expiry:
            return cached_bal

    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
    try:
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id, signature_type=2)
        )
        result = int(bal.get("balance", "0")) / 1e6
        _balance_cache[token_id] = (result, now + BALANCE_CACHE_TTL)
        return result
    except Exception as e:
        log.warning("Balance check failed for %s: %s", token_id[:16], e)
        raise BalanceCheckError(f"Balance check failed: {e}") from e


# ============================================================================
# == REDEEM LOOP — try_redeem_all, try_merge_all
# ============================================================================


async def try_redeem_all(client, redeemer, past_markets: dict):
    """Check all past markets and redeem any with unredeemed balances.

    Uses conditionId / neg_risk cached at placement time to avoid
    one API call per market per loop tick.  Falls back to a fresh
    get_market_info() fetch when those fields are missing (startup
    scan entries added before this version).
    """
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

        # Determine if market is closed — prefer cached flag, fallback to API
        if not info.get("closed"):
            # Fetch only if we don't already know it's closed
            slug = get_market_slug(ts)
            try:
                mkt = await get_market_info(slug)
            except Exception:
                continue
            if not mkt["closed"]:
                continue  # not resolved yet
            # Cache the closed state and fill in any missing fields
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

        # Check if we hold any tokens
        try:
            up_bal = check_token_balance(client, up_token)
            dn_bal = check_token_balance(client, dn_token)
        except Exception:
            continue  # API down — retry next loop

        if up_bal <= 0 and dn_bal <= 0:
            info["redeemed"] = True  # nothing to redeem
            continue

        redeem_fails = info.get("redeem_fails", 0)
        if redeem_fails >= 5:
            continue
        log.info("  Redeeming %s (UP=%.1f, DN=%.1f)...", title[:50], up_bal, dn_bal)
        ok = await asyncio.to_thread(redeem_market, redeemer, condition_id, neg_risk, up_bal, dn_bal)
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


async def try_merge_all(client, redeemer, past_markets: dict):
    """Merge complete sets (UP + DN) into USDC only after the market has ended (closed).
    Run before redeem: merge first (get $1 per set back), then redeem any remaining."""
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

        # Only merge after market has ended (same as redeem)
        if not info.get("closed"):
            slug = get_market_slug(ts)
            try:
                mkt = await get_market_info(slug)
            except Exception:
                continue
            if not mkt["closed"]:
                continue  # market not ended yet
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
            continue  # API down — retry next loop
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


# ============================================================================
# == MAIN LOOP — run(), hedge_loop, market rotation
# ============================================================================


async def hedge_loop(client, ws_feed: "WSBookFeed", past_markets: dict):
    """Fast 1-second loop for trailing hedge checks.

    Runs as a background task so hedge triggers aren't delayed by the
    main loop sleep. Shares past_markets dict by reference —
    never reassign past_markets in the main loop, only mutate in place.
    """
    while not _shutdown:
        try:
            await check_hedge_trailing(client, ws_feed, past_markets)
        except Exception as e:
            log.debug("hedge_loop error: %s", e)
        await asyncio.sleep(1)


# Global stop flag for graceful shutdown (checked by main loop)
_shutdown = False

async def run():
    global _shutdown, _FILL_DEDUP
    open_trade_log()
    log.info("Initializing SDK...")
    client = init_clob_client()
    redeemer = init_redeemer()

    log.info("SDK ready. Placing %.0fc orders on BTC %s markets.", PRICE * 100, TIMEFRAME)
    log.info("  Fee params: rate=%.2f exponent=%.0f (max eff. rate=~1.56%% at 50c)",
             _FEE_PARAMS.fee_rate, _FEE_PARAMS.exponent)
    if redeemer:
        log.info("Auto-redeem enabled (on-chain via %s).\n", redeemer[1].address)
    else:
        log.info("Auto-redeem DISABLED (no private key / RPC).\n")

    # Initialize fill deduplicator (crash-safe, persists seen fills across restarts)
    dedup_path = os.path.join(_trade_log_dir, "seen_fills.json")
    _FILL_DEDUP = FillDeduplicator(dedup_path)

    if FRESH_START:
        # Clear all runtime state — useful when deploying to a new VPS or after
        # a long gap where cached market timestamps / clock drift would cause issues.
        log.info("FRESH_START=1: clearing all caches and skipping startup scan.")
        _FILL_DEDUP.clear()
        _balance_cache.clear()
        # Recreate the HTTP client so stale connections are dropped
        global _http_client
        if _http_client and not _http_client.is_closed:
            await _http_client.aclose()
            _http_client = None
    else:
        log.info("Fill deduplicator: %d fills loaded from %s", _FILL_DEDUP.count, dedup_path)

    # Start WebSocket feed for real-time prices
    ws_feed = WSBookFeed()

    placed_markets = set()
    # Track past markets: {ts: {redeemed, bailed, up_token, dn_token, closed}}
    # NOTE: never reassign this dict — hedge_loop holds a reference to it.
    past_markets = {}
    MERGE_REDEEM_INTERVAL_S = 30
    last_merge_redeem_ts = 0

    # Start fast hedge loop (1s interval) as a background task
    asyncio.create_task(hedge_loop(client, ws_feed, past_markets))

    # Scan recent markets (last 2 hours) for unredeemed positions on startup.
    # Skipped when FRESH_START=1 — bot starts with a completely clean slate.
    now = int(time.time())
    if FRESH_START:
        log.info("Startup scan skipped (FRESH_START=1). Starting clean.")
    else:
        log.info("Scanning recent markets for unredeemed positions...")
        # Scan last 2 hours of 15-minute markets (7200s / 900s = 8 markets)
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
                    # Use market start as anchor so hedge windows work correctly
                    # if this market has a one-sided fill when we pick it up.
                    "placed_at": 0 if mkt["closed"] else scan_ts,
                }
                # IMPORTANT: Only add to placed_markets (preventing re-placement) if:
                #   - market is closed (no point placing), OR
                #   - we're past the placement window (too late to enter)
                # If we're still within the window, let the main loop decide
                # (it will check balance + live orders before placing).
                secs_since_open = now - scan_ts
                if mkt["closed"] or secs_since_open > NEW_MARKET_PLACE_WINDOW_SECONDS:
                    placed_markets.add(scan_ts)
                else:
                    log.info(
                        "Startup: market %d is open and within %ds window (%ds elapsed) — "
                        "leaving for main loop to handle",
                        scan_ts, NEW_MARKET_PLACE_WINDOW_SECONDS, secs_since_open,
                    )
                # Subscribe to WS feed so prices are available for active markets
                if not mkt["closed"]:
                    if not ws_feed.is_connected:
                        await ws_feed.start([mkt["up_token"], mkt["dn_token"]])
                    else:
                        await ws_feed.subscribe([mkt["up_token"], mkt["dn_token"]])
            except Exception:
                pass
            scan_ts += MARKET_PERIOD
        log.info("Found %d recent markets to track for redemption.\n", len(past_markets))

    # Pre-fetch cache for next market (avoids get_market_info wait when new period starts)
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

        for ts in timestamps:
            if ts in placed_markets:
                continue

            slug = get_market_slug(ts)

            # Use pre-fetched market data if we have it (saves one round-trip at market open)
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
                        # Future market — silently wait
                        log.debug("Market %s not available yet (+%ds): %s", slug, secs_until_ts, e)
                    else:
                        # Current/near market — this is unexpected, log visibly
                        log.info("Market %s lookup failed (%ds): %s", slug, secs_until_ts, e)
                    continue

            secs_until = ts - now

            log.info("=" * 60)
            log.info("MARKET: %s", mkt["title"])
            log.info("  Slug: %s", slug)
            log.info("  Starts in: %ds", max(0, secs_until))
            log.info("  UP token:  %s...", mkt["up_token"][:20])
            log.info("  DN token:  %s...", mkt["dn_token"][:20])
            log.info("")

            # Subscribe to WS feed for this market's tokens
            if not ws_feed.is_connected:
                await ws_feed.start([mkt["up_token"], mkt["dn_token"]])
            else:
                await ws_feed.subscribe([mkt["up_token"], mkt["dn_token"]])

            # Balance + live orders in parallel (one round-trip instead of three)
            def _get_orders():
                try:
                    return client.get_orders()
                except Exception:
                    return []
            def _safe_balance(token_id):
                try:
                    return check_token_balance(client, token_id)
                except Exception:
                    return 0.0  # safe default for placement check (worst case: place duplicate orders)
            up_bal, dn_bal, live_orders = await asyncio.gather(
                asyncio.to_thread(_safe_balance, mkt["up_token"]),
                asyncio.to_thread(_safe_balance, mkt["dn_token"]),
                asyncio.to_thread(_get_orders),
            )
            if up_bal > 0 or dn_bal > 0:
                log.info("  Already have position (UP=%.1f, DN=%.1f) — skipping", up_bal, dn_bal)
                placed_markets.add(ts)
                past_markets[ts] = {
                    "redeemed": False,
                    "closed": mkt["closed"],
                    "up_token": mkt["up_token"],
                    "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"],
                    "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug),
                    # Use ts so hedge activates if this is a one-sided fill
                    "placed_at": ts,
                }
                continue

            # Skip if we already have live orders on this market (live_orders from parallel fetch above)
            market_tokens = {mkt["up_token"], mkt["dn_token"]}
            existing = [o for o in (live_orders or [])
                        if o.get("status") == "LIVE"
                        and o.get("asset_id") in market_tokens]
            if existing:
                log.info("  Already have %d live order(s) — skipping", len(existing))
                placed_markets.add(ts)
                past_markets[ts] = {
                    "redeemed": False,
                    "closed": mkt["closed"],
                    "up_token": mkt["up_token"],
                    "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"],
                    "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug),
                    # Use ts so hedge activates when one of these orders fills
                    "placed_at": ts,
                }
                continue

            # Enforce placement window (first N seconds of each market).
            # ts is the market START timestamp; secs_elapsed is negative for
            # future markets (we still pre-register them) and 0-900 for live ones.
            secs_elapsed = now - ts
            if secs_elapsed > NEW_MARKET_PLACE_WINDOW_SECONDS:
                log.info(
                    "  Skipping placement — outside %ds window (%ds elapsed since market start)",
                    NEW_MARKET_PLACE_WINDOW_SECONDS, secs_elapsed,
                )
                log_trade("window_missed", market_ts=ts, slug=slug, elapsed_s=secs_elapsed)
                placed_markets.add(ts)
                past_markets[ts] = {
                    "redeemed": False,
                    "closed": mkt["closed"],
                    "up_token": mkt["up_token"],
                    "dn_token": mkt["dn_token"],
                    "condition_id": mkt["conditionId"],
                    "neg_risk": mkt.get("neg_risk", False),
                    "title": mkt.get("title", slug),
                    "placed_at": 0,  # no orders placed; hedge won't activate
                }
                continue

            # Optional cap: don't open new markets if we're at max open (risk limit)
            if MAX_OPEN_MARKETS > 0:
                open_count = sum(1 for _ts, _info in past_markets.items()
                                if not _info.get("redeemed") and not _info.get("bailed") and not _info.get("closed"))
                if open_count >= MAX_OPEN_MARKETS:
                    log.info(
                        "  Skipping placement — at max open markets (%d >= %d)",
                        open_count, MAX_OPEN_MARKETS,
                    )
                    log_trade("max_open_skipped", market_ts=ts, slug=slug, open_count=open_count, max=MAX_OPEN_MARKETS)
                    continue  # retry next loop when a slot frees

            # Determine entry price (static or dynamic based on orderbook shape)
            entry_price = PRICE
            if ENTRY_PRICE_DYNAMIC:
                up_ask = ws_feed.get_best_ask(mkt["up_token"])
                dn_ask = ws_feed.get_best_ask(mkt["dn_token"])
                if up_ask is not None and dn_ask is not None:
                    spread = up_ask + dn_ask
                    if spread > 1.05:  # high vol / wide spread
                        entry_price = ENTRY_PRICE_VOL_HIGH
                    elif spread < 0.95:  # low vol / tight spread
                        entry_price = ENTRY_PRICE_VOL_LOW
                    log.info("  Dynamic entry: spread=%.3f → price=%.2f", spread, entry_price)

            # Place both sides in parallel so they hit the book at nearly the same time
            up_id, dn_id = await asyncio.gather(
                asyncio.to_thread(place_order, client, mkt["up_token"], "UP  ", SHARES_PER_SIDE, entry_price),
                asyncio.to_thread(place_order, client, mkt["dn_token"], "DOWN", SHARES_PER_SIDE, entry_price),
            )

            placed = (1 if up_id else 0) + (1 if dn_id else 0)
            log.info("  Done: %d orders placed. Max cost: $%.2f", placed, SHARES_PER_SIDE * entry_price * 2)
            log.info("")
            log_trade("orders_placed", market_ts=ts, slug=slug,
                      title=mkt.get("title", slug), price=entry_price, shares=SHARES_PER_SIDE,
                      up_id=up_id, dn_id=dn_id, sides_placed=placed,
                      elapsed_s=now - ts, dynamic=ENTRY_PRICE_DYNAMIC)

            if placed == 0:
                log.info("  Both orders returned null — market may not be open yet. Will retry next loop.")
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
                # Use market start timestamp (ts) as anchor, not wall-clock
                # placement time. time_elapsed = now - ts, so hedge windows
                # (240s, 600s, 720s) are relative to market start,
                # not to when place_order() returned.
                "placed_at": ts,
                # PositionState tracks fills for this market (updated by hedge_loop)
                "position": PositionState(),
            }

        # Log WS prices for active (unresolved, unhedged) markets
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
                log.info(
                    "  [%d +%ds] WS: UP=%.2f  DN=%.2f%s",
                    ts, elapsed, up_ask or 0, dn_ask or 0, hedge_state,
                )

        # Cancel stale unfilled orders (e.g., one-sided markets where hedge hasn't triggered yet)
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
                    log.info("  STALE-CANCEL: cancelling orders for market %d (%ds elapsed)", _ts, now - _placed)
                    cancel_market_orders(client, _tokens)
                    _info["stale_cancelled"] = True
                    log_trade("stale_cancel", market_ts=_ts, elapsed_s=now - _placed)

        # WS health check: force reconnect if no messages received in 90s
        ws_age = ws_feed.last_message_age
        if ws_age > 90.0 and ws_feed.is_connected:
            log.warning("WS feed stale (%.0fs since last message) — forcing reconnect", ws_age)
            await ws_feed.force_reconnect()

        # EV-based bail: sell filled side if other side's ask exceeds time-based threshold
        await check_bail_out(client, ws_feed, past_markets)
        # Merge + redeem are on-chain txs that don't need sub-second latency.
        # Throttle to every MERGE_REDEEM_INTERVAL_S to reduce API spam.
        if now - last_merge_redeem_ts >= MERGE_REDEEM_INTERVAL_S:
            await try_merge_all(client, redeemer, past_markets)
            await try_redeem_all(client, redeemer, past_markets)
            last_merge_redeem_ts = now

        # Faster poll when close to next market start (better chance to fill both sides)
        seconds_until = next_market_ts - now
        sleep_s = 0.5 if seconds_until <= 30 else (1 if seconds_until <= 120 else 3)
        # Pre-fetch next market in background so we have it when the new period starts
        if seconds_until <= NEXT_MARKET_PREFETCH_SEC and seconds_until > 0:
            asyncio.create_task(prefetch_next_market(next_market_ts, next_market_cache))
        await asyncio.sleep(sleep_s)

        # Log status with countdown
        log.info("[Next market in %ds] Checked %d markets", seconds_until, len(timestamps))

        # Clean old entries. Hard TTL of 6 hours — if merge/redeem hasn't
        # succeeded by then, the market is stuck and needs manual attention.
        # Standard cleanup at 2h for markets that are fully redeemed.
        HARD_TTL_S = 21600  # 6 hours
        NORMAL_TTL_S = 7200  # 2 hours
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
        # placed_markets must survive long enough to cover the previous period
        # (next_market_timestamps always returns ts-MARKET_PERIOD which is ~15 min old).
        # Prune at 2×MARKET_PERIOD so the previous period is never re-placed.
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
    """Entry point with auto-restart on crash (24/7 operation).

    Restarts with exponential backoff (5s → 10s → 20s → ... → 120s max).
    Keyboard interrupt or SIGTERM triggers a clean shutdown.
    """
    import signal
    global _shutdown

    def _signal_handler(sig, frame):
        global _shutdown
        _shutdown = True
        log.info("\nShutdown requested (signal %s). Finishing current cycle...", sig)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    backoff = 5
    while not _shutdown:
        try:
            asyncio.run(run())
            break  # clean exit (shutdown flag set)
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
    # Close trade log cleanly
    if _trade_log is not None:
        try:
            _trade_log.close()
        except Exception:
            pass
    log.info("Bot exited.")


if __name__ == "__main__":
    _main()
