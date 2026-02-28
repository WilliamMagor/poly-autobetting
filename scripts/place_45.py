"""
Simple script: place 45c limit orders on both UP and DOWN for BTC 5-min markets.
10 shares each side. Rotates to next market every 5 minutes.
- Auto-merges complete sets (UP+DN) into USDC on-chain only after the market has ended.
- Auto-redeems resolved positions (remaining tokens) via direct on-chain Polygon transaction.

Optional env: PLACE_45_MAX_OPEN_MARKETS (cap open markets), PLACE_45_REST_FALLBACK=1 (REST ask fallback).

Usage: python scripts/place_45.py

--- Bail vs Hedge (one-sided fill) ---
  BAIL: When only one side filled and the *other* side's ask goes above 0.72 (BAIL_PRICE),
  we sell the side we hold (the "losing" side) at the bid and cancel orders. That exits
  the naked directional risk instead of holding to resolution (where the loser goes to $0).
  So bail does not buy the other side; it cuts losses by selling what we have.

  HEDGE (trailing): We *do* buy the unfilled side to complete the set, but only when
  it's cheap enough (bands) and when price has bounced off a floor (trailing stop):
  - 2-min window (120–180s): band 0.45 → only buy if hedge_ask < 0.45; trigger when
    ask >= lowest_ask + 0.03 (HEDGE_TRAILING_DELTA).
  - 3-min window (180–240s): band 0.55 → buy if hedge_ask < 0.55; same trailing idea,
    min_bounce relaxes to ~0.01 by 240s.
  So we track lowest_ask on the unfilled side and buy when ask bounces up by the delta.
"""

import asyncio
import json
import logging
import os
import sys
import time

import httpx

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.bot.ws_book_feed import WSBookFeed

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("place45")

# --- Structured trade log (JSONL) ---
# One JSON object per decision event, written to logs/place_45_YYYYMMDD.jsonl.
# Designed for Claude Code to analyze and surface algo improvements.
_trade_log = None  # file handle, opened in run()

def open_trade_log():
    """Open (or create) today's JSONL trade log. Called once at startup."""
    global _trade_log
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    date_str = time.strftime("%Y%m%d")
    path = os.path.join(log_dir, f"place_45_{date_str}.jsonl")
    _trade_log = open(path, "a", buffering=1)  # line-buffered
    log.info("Trade log: %s", path)

def log_trade(event: str, **fields):
    """Append one structured event to today's JSONL trade log."""
    if _trade_log is None:
        return
    record = {"ts": int(time.time()), "event": event, **fields}
    _trade_log.write(json.dumps(record) + "\n")


# --- Config ---
PRICE = 0.45
SHARES_PER_SIDE = 10     # Reduced for 5min markets (more frequent rotations)
BAIL_PRICE = 0.72        # if one side's ask > 72c and we only hold the other → sell held side (exit risk)
GAMMA_URL = "https://gamma-api.polymarket.com"
MARKET_PERIOD = 300      # 5 minutes (300 seconds)
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# --- Trailing hedge config (from Rust bot analysis) ---
# Only place initial orders within the first N seconds of a market period.
# Rust bot uses NEW_MARKET_PLACE_WINDOW_SECONDS = 15.
NEW_MARKET_PLACE_WINDOW_SECONDS = 15
# After one side fills, cancel unfilled limit and trail the cheap side.
# Time windows (seconds elapsed since MARKET START — same anchor as Rust bot):
HEDGE_START_AFTER_S = 120      # 2-min: start trailing
HEDGE_3MIN_AFTER_S  = 180      # 3-min: widen band
HEDGE_4MIN_AFTER_S  = 240      # 4-min: force-buy window
# Band thresholds: PRICE + hedge_ask must stay below 1.00 to be profitable.
# band_2min = 1.0 - PRICE - 0.10 = 0.45  (conservative, requires cheap hedge)
# band_3min = 1.0 - PRICE        = 0.55  (wider, closer to breakeven)
HEDGE_BAND_2MIN = 1.0 - PRICE - 0.10   # 0.45
HEDGE_BAND_3MIN = 1.0 - PRICE          # 0.55
# Trailing delta: buy when ask bounces this much above its lowest point.
HEDGE_TRAILING_DELTA = 0.03
# Force-buy threshold at 4-min: if the FILLED side's ask reached this level,
# the unfilled side is very cheap → buy at market regardless of trailing.
HEDGE_FORCE_BUY_FILLED_THRESHOLD = 0.85

# --- Risk limits (optional) ---
# Max number of markets we have open orders or positions in (0 = no cap)
MAX_OPEN_MARKETS = int(os.getenv("PLACE_45_MAX_OPEN_MARKETS", "0"))
# If WS has no ask, fall back to REST order book for hedge/bail (slower but safer)
HEDGE_USE_REST_FALLBACK = os.getenv("PLACE_45_REST_FALLBACK", "0").strip().lower() in ("1", "true", "yes")

# --- Speed: pre-fetch next market when this many seconds until start
NEXT_MARKET_PREFETCH_SEC = 60


def next_market_timestamps(now_ts: int) -> list[int]:
    """Return [currently_trading, next, after_that] 5m market timestamps.

    The slug timestamp is the START time of the market.
    """
    # 5-minute markets: calculate current period timestamp
    ts = (now_ts // MARKET_PERIOD) * MARKET_PERIOD
    # ts = next market to start. ts-300 = currently trading market.
    return [ts - MARKET_PERIOD, ts, ts + MARKET_PERIOD]


def get_market_slug(timestamp: int) -> str:
    """Return the market slug for a given timestamp (5-minute BTC market)."""
    return f"btc-updown-5m-{timestamp}"


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


async def get_market_info(slug: str) -> dict:
    """Fetch market info from gamma API with input validation."""
    # Validate slug format to prevent injection
    if not slug or not isinstance(slug, str):
        raise ValueError(f"Invalid slug: expected non-empty string, got {slug}")
    if len(slug) > 200:
        raise ValueError(f"Invalid slug: too long (max 200 chars), got {len(slug)}")
    # Only allow expected slug pattern
    import re
    if not re.match(r"^btc-updown-5m-\d+$", slug):
        raise ValueError(f"Invalid slug format: expected btc-updown-5m-<timestamp>, got {slug}")

    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{GAMMA_URL}/events", params={"slug": slug})
        r.raise_for_status()
        data = r.json()

    if not data:
        raise ValueError(f"No event found for slug: {slug}")

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

    return {
        "up_token": up_token,
        "dn_token": dn_token,
        "title": m.get("question", slug),
        "conditionId": condition_id,
        "closed": bool(m.get("closed", False)),
        "neg_risk": bool(m.get("negRisk", False)),
    }


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


def place_order(client, token_id: str, side_label: str, shares: float, price: float):
    """Place a single limit buy order. Returns order ID or None."""
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    expiration = int(time.time()) + 3600  # 1 hour
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


def sell_at_bid(client, token_id: str, shares: float, side_label: str) -> str | None:
    """Place a limit SELL at the current best bid. Returns order ID or None."""
    from py_clob_client.order_builder.constants import SELL
    from py_clob_client.clob_types import OrderArgs, OrderType

    try:
        book = client.get_order_book(token_id)
        if not book.bids:
            log.warning("  %s no bids to sell into", side_label)
            return None
        best_bid = float(book.bids[0].price)
        log.info("  %s SELL %.0f @ %.2f (best bid)", side_label, shares, best_bid)

        order_args = OrderArgs(
            token_id=token_id,
            price=best_bid,
            size=round(shares, 1),
            side=SELL,
            expiration=int(time.time()) + 300,  # 5 min expiry
        )
        signed = client.create_order(order_args)
        result = client.post_order(signed, OrderType.GTD)
        oid = result.get("orderID", result.get("id", ""))
        if oid:
            log.info("  %s SELL placed [%s]", side_label, oid[:12])
            return oid
        else:
            log.warning("  %s SELL no order ID: %s", side_label, result)
    except Exception as e:
        log.error("  %s SELL failed: %s", side_label, e)
    return None


def buy_hedge(client, token_id: str, shares: float, side_label: str, price: float) -> str | None:
    """Place a limit BUY for the hedge side (post-one-sided-fill).

    Uses the same limit-order path as the original placement but at
    the current market price rather than the fixed 0.45 entry price.
    Retries up to 3 times with 2s delay (matches Rust bot retry logic).
    Returns order ID or None on failure.
    """
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    expiration = int(time.time()) + 600  # 10-min expiry (market ends in <4 min anyway)
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
            time.sleep(2)
    log.error("  HEDGE %s order failed after 3 attempts", side_label)
    return None


async def check_hedge_trailing(client, ws_feed: "WSBookFeed", past_markets: dict):
    """Time-windowed trailing-stop hedge for one-sided fills.

    Ported from Rust bot (main_dual_limit_045_5m_btc.rs).

    Strategy:
      After one limit order fills and the other doesn't, the market has moved
      directionally. Rather than holding a naked directional position to
      resolution, we:
        1. Detect one-sided fill as soon as it happens (no 120s wait) so we
           start tracking the trailing floor from the moment of fill.
        2. Wait until 120s before cancelling the unfilled limit (gives it a
           chance to fill naturally in the first 2 minutes).
        3. Track the lowest ask of the unfilled side (trail the floor).
        4. Buy when the price BOUNCES UP by min_bounce (dynamic, starts at
           HEDGE_TRAILING_DELTA=0.03 and relaxes to ~0.01 at 240s).
        5. Apply progressive band thresholds so we only buy if profitable:
             120-180s: hedge_ask < HEDGE_BAND_2MIN (0.45)  →  total < 0.90
             180-240s: hedge_ask < HEDGE_BAND_3MIN (0.55)  →  total < 1.00
        6. If hedge_ask >= band after 180s, sell the filled side instead of
           waiting — hedging would cost more than $1.00, so exit gracefully.
        7. At 240s+: force-buy if the FILLED side's ask >= 0.85 (the market
           is very confident, unfilled side is cheap ~0.15).

    Timing anchor: placed_at == ts (market START timestamp), so time_elapsed
    mirrors the Rust bot's (MARKET_PERIOD - time_remaining_seconds) exactly.

    State stored per market in past_markets:
      hedge_side:        "up" | "dn" | None  — which side needs to be hedged
      filled_bal:        float — balance of the filled side at detection time
      hedge_cancelled:   bool — unfilled limit was cancelled
      hedge_done:        bool — hedge buy was placed or filled side sold
      trailing_min_ask:  float | None — lowest ask seen for unfilled side
    """
    now = int(time.time())

    for ts, info in list(past_markets.items()):
        # Skip markets that are fully handled (bailed = we already sold filled side)
        if info.get("hedge_done") or info.get("redeemed") or info.get("closed") or info.get("bailed"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        placed_at = info.get("placed_at", 0)
        if not up_token or not dn_token or not placed_at:
            continue

        time_elapsed = now - placed_at

        # --- Step 1: Detect one-sided fill (no time gate — start tracking early) ---
        if not info.get("hedge_side"):
            try:
                up_bal = check_token_balance(client, up_token)
                dn_bal = check_token_balance(client, dn_token)
            except Exception:
                continue

            if up_bal > 0 and dn_bal == 0:
                hedge_side, filled_bal = "dn", up_bal
            elif dn_bal > 0 and up_bal == 0:
                hedge_side, filled_bal = "up", dn_bal
            else:
                # Both filled (complete set) or neither filled — nothing to do yet
                continue

            log.info(
                "  HEDGE detected one-sided fill at %ds: %s filled, tracking %s",
                time_elapsed,
                "UP" if hedge_side == "dn" else "DN",
                hedge_side.upper(),
            )
            info["hedge_side"] = hedge_side
            info["filled_bal"] = filled_bal
            info["trailing_min_ask"] = None
            log_trade("fill_detected", market_ts=ts,
                      filled_side="UP" if hedge_side == "dn" else "DN",
                      filled_bal=filled_bal, hedge_side=hedge_side.upper(),
                      elapsed_s=time_elapsed)

        hedge_side: str = info["hedge_side"]
        hedge_token = up_token if hedge_side == "up" else dn_token
        filled_token = dn_token if hedge_side == "up" else up_token
        filled_label = "UP" if hedge_side == "dn" else "DN"

        # --- Step 2: Track trailing minimum from fill detection (before 120s) ---
        hedge_ask = get_best_ask_safe(ws_feed, client, hedge_token)
        if hedge_ask is None:
            continue

        prev_min = info.get("trailing_min_ask")
        if prev_min is None or hedge_ask < prev_min:
            info["trailing_min_ask"] = hedge_ask
            if prev_min is not None:
                log.info("  HEDGE trail: new low for %s ask=%.3f", hedge_side.upper(), hedge_ask)

        trailing_min = info["trailing_min_ask"]

        # Don't cancel or buy until 120s — give unfilled limit a chance to fill
        if time_elapsed < HEDGE_START_AFTER_S:
            continue

        # --- Step 3: Cancel unfilled limit once at 120s ---
        if not info.get("hedge_cancelled"):
            log.info("  HEDGE cancelling unfilled %s limit", hedge_side.upper())
            cancel_market_orders(client, {hedge_token})
            info["hedge_cancelled"] = True

        # --- Step 4: Time-windowed band threshold ---
        if time_elapsed >= HEDGE_4MIN_AFTER_S:
            band = HEDGE_BAND_3MIN
            # Force-buy: filled side at 0.85+ means hedge side is very cheap
            filled_ask = get_best_ask_safe(ws_feed, client, filled_token)
            if filled_ask is not None and filled_ask >= HEDGE_FORCE_BUY_FILLED_THRESHOLD:
                log.info(
                    "  HEDGE FORCE-BUY: filled side ask=%.2f >= %.2f, hedge %s ask=%.3f",
                    filled_ask, HEDGE_FORCE_BUY_FILLED_THRESHOLD,
                    hedge_side.upper(), hedge_ask,
                )
                log_trade("hedge_force_buy", market_ts=ts, hedge_side=hedge_side.upper(),
                          filled_ask=filled_ask, hedge_ask=hedge_ask, elapsed_s=time_elapsed)
                buy_hedge(client, hedge_token, SHARES_PER_SIDE, hedge_side.upper(), hedge_ask)
                info["hedge_done"] = True
                continue
        elif time_elapsed >= HEDGE_3MIN_AFTER_S:
            band = HEDGE_BAND_3MIN
        else:
            band = HEDGE_BAND_2MIN

        # --- Step 5: Valid-bounce check (Rust port) ---
        # If trailing_min was deeply below the band (< band - delta), a bounce
        # through the band is high-confidence — buy even if ask is now above band.
        valid_bounce = trailing_min is not None and trailing_min < (band - HEDGE_TRAILING_DELTA)

        # --- Step 5b: Sell filled side if hedge becomes uneconomical (after 3min) ---
        if hedge_ask >= band:
            if valid_bounce:
                log.info(
                    "  HEDGE VALID-BOUNCE: %s ask=%.3f > band=%.2f "
                    "but low=%.3f < band-delta — buying through band",
                    hedge_side.upper(), hedge_ask, band, trailing_min,
                )
                # Fall through to Steps 6-7 below
            elif time_elapsed >= HEDGE_3MIN_AFTER_S:
                # Use current balance so we don't try to sell more than we hold (e.g. after manual sell or bail)
                try:
                    sell_bal = check_token_balance(client, filled_token)
                except Exception:
                    sell_bal = info.get("filled_bal", SHARES_PER_SIDE)
                if sell_bal <= 0:
                    info["hedge_done"] = True
                    continue
                log.info(
                    "  HEDGE SELL-FILLED: %s ask=%.3f >= band=%.2f at %ds — "
                    "selling %s %.0f (hedging would cost >$1.00)",
                    hedge_side.upper(), hedge_ask, band, time_elapsed,
                    filled_label, sell_bal,
                )
                log_trade("hedge_sell_filled", market_ts=ts, hedge_side=hedge_side.upper(),
                          hedge_ask=hedge_ask, band=band, filled_bal=sell_bal,
                          elapsed_s=time_elapsed)
                cancel_market_orders(client, {hedge_token})
                sell_at_bid(client, filled_token, sell_bal, filled_label)
                info["hedge_done"] = True
                info["bailed"] = True
                continue
            else:
                log.info(
                    "  HEDGE wait: %s ask=%.3f >= band=%.2f (elapsed=%ds)",
                    hedge_side.upper(), hedge_ask, band, time_elapsed,
                )
                continue

        # --- Step 6: Dynamic bounce threshold — relax as time runs out ---
        # Starts at HEDGE_TRAILING_DELTA (0.03) at 120s, decays to ~0.01 at 240s
        progress = min(1.0, (time_elapsed - HEDGE_START_AFTER_S) /
                            (HEDGE_4MIN_AFTER_S - HEDGE_START_AFTER_S))
        min_bounce = HEDGE_TRAILING_DELTA * (1.0 - progress * 0.67)

        # --- Step 7: Trailing-stop trigger — buy on bounce from floor ---
        if trailing_min is None:
            continue  # no floor yet (e.g. WS had no data until now)
        bounce = hedge_ask - trailing_min
        log.info(
            "  HEDGE trail: %s ask=%.3f low=%.3f bounce=%.3f/%.3f (elapsed=%ds band=%.2f)",
            hedge_side.upper(), hedge_ask, trailing_min,
            bounce, min_bounce, time_elapsed, band,
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
                      band=band, reason=reason, elapsed_s=time_elapsed)
            # --- Balance check before buy (prevents double-position) ---
            current_bal = check_token_balance(client, hedge_token)
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
                      order_id=oid, elapsed_s=time_elapsed)
            info["hedge_done"] = True


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


async def check_bail_out(client, ws_feed: WSBookFeed, past_markets: dict):
    """If one side > 72c and other side has 0 balance → cancel + sell filled side."""
    for ts, info in list(past_markets.items()):
        if info.get("bailed") or info.get("redeemed"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        if not up_token or not dn_token:
            continue

        if info.get("closed"):
            continue  # already resolved, redeem handles it

        # Get prices from WS feed (or REST fallback if enabled)
        up_ask = get_best_ask_safe(ws_feed, client, up_token)
        dn_ask = get_best_ask_safe(ws_feed, client, dn_token)
        if up_ask is None or dn_ask is None:
            continue  # no price data yet

        if up_ask <= BAIL_PRICE and dn_ask <= BAIL_PRICE:
            continue  # no bail needed

        # Price triggered — check balances via CLOB API
        try:
            up_bal = check_token_balance(client, up_token)
            dn_bal = check_token_balance(client, dn_token)
        except Exception:
            continue

        bail = False
        # DN expensive (DN winning) + we only hold UP (DN unfilled) → sell UP
        if dn_ask > BAIL_PRICE and dn_bal == 0 and up_bal > 0:
            log.info("  BAIL: DN ask=%.2f > %.2f, DN unfilled. Selling UP %.0f shares",
                     dn_ask, BAIL_PRICE, up_bal)
            cancel_market_orders(client, {up_token, dn_token})
            sell_at_bid(client, up_token, up_bal, "UP")
            bail = True
        # UP expensive (UP winning) + we only hold DN (UP unfilled) → sell DN
        elif up_ask > BAIL_PRICE and up_bal == 0 and dn_bal > 0:
            log.info("  BAIL: UP ask=%.2f > %.2f, UP unfilled. Selling DN %.0f shares",
                     up_ask, BAIL_PRICE, dn_bal)
            cancel_market_orders(client, {up_token, dn_token})
            sell_at_bid(client, dn_token, dn_bal, "DN")
            bail = True

        if bail:
            info["bailed"] = True


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
    try:
        nonce = w3.eth.get_transaction_count(account.address)
        tx = tx_fn.build_transaction({
            "from": account.address,
            "nonce": nonce,
            "gas": 200_000,
            "gasPrice": w3.eth.gas_price,
            "chainId": 137,
        })
        signed = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("  Merge tx sent: %s (%s sets)", tx_hash.hex()[:20], amount)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] == 1:
            log.info("  Merge CONFIRMED: %s", tx_hash.hex()[:20])
            return True
        log.error("  Merge tx FAILED (status=0): %s", tx_hash.hex()[:20])
        return False
    except Exception as e:
        log.error("  Merge error: %s", e)
        return False


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

    try:
        nonce = w3.eth.get_transaction_count(account.address)
        tx = tx_fn.build_transaction({
            "from": account.address,
            "nonce": nonce,
            "gas": 300_000,
            "gasPrice": w3.eth.gas_price,
            "chainId": 137,
        })
        signed = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        log.info("  Redeem tx sent: %s", tx_hash.hex()[:20])
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] == 1:
            log.info("  Redeem CONFIRMED: %s", tx_hash.hex()[:20])
            return True
        else:
            log.error("  Redeem tx FAILED (status=0): %s", tx_hash.hex()[:20])
            return False
    except Exception as e:
        log.error("  Redeem error: %s", e)
        return False




def check_token_balance(client, token_id: str) -> float:
    """Check CTF token balance via CLOB API (returns shares)."""
    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
    try:
        bal = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id, signature_type=2)
        )
        return int(bal.get("balance", "0")) / 1e6
    except Exception:
        return 0.0


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
        title = info.get("title", f"btc-updown-5m-{ts}")

        # Determine if market is closed — prefer cached flag, fallback to API
        if not info.get("closed"):
            # Fetch only if we don't already know it's closed
            slug = f"btc-updown-5m-{ts}"
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
        up_bal = check_token_balance(client, up_token)
        dn_bal = check_token_balance(client, dn_token)

        if up_bal <= 0 and dn_bal <= 0:
            info["redeemed"] = True  # nothing to redeem
            continue

        log.info("  Redeeming %s (UP=%.1f, DN=%.1f)...", title[:50], up_bal, dn_bal)
        ok = redeem_market(redeemer, condition_id, neg_risk, up_bal, dn_bal)
        log_trade("redeem", market_ts=ts, title=title, up_bal=up_bal, dn_bal=dn_bal, ok=ok)
        if ok:
            info["redeemed"] = True
            redeemed.append(ts)

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
        title = info.get("title", f"btc-updown-5m-{ts}")

        # Only merge after market has ended (same as redeem)
        if not info.get("closed"):
            slug = f"btc-updown-5m-{ts}"
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

        up_bal = check_token_balance(client, up_token)
        dn_bal = check_token_balance(client, dn_token)
        merge_amount = min(up_bal, dn_bal)
        if merge_amount <= 0:
            continue

        log.info("  Merging %s (%.1f complete sets)...", title[:50], merge_amount)
        ok = merge_market(redeemer, condition_id, merge_amount)
        log_trade("merge", market_ts=ts, title=title, sets=merge_amount, ok=ok)
        if ok:
            merged.append(ts)

    if merged:
        log.info("  Merged %d market(s)", len(merged))


async def hedge_loop(client, ws_feed: "WSBookFeed", past_markets: dict):
    """Fast 1-second loop for trailing hedge checks.

    Runs as a background task so hedge triggers aren't delayed by the
    5-second main loop sleep. Shares past_markets dict by reference —
    never reassign past_markets in the main loop, only mutate in place.
    """
    while True:
        try:
            await check_hedge_trailing(client, ws_feed, past_markets)
        except Exception as e:
            log.debug("hedge_loop error: %s", e)
        await asyncio.sleep(1)


async def run():
    # Set up signal handler for graceful shutdown
    import signal
    def signal_handler(sig, frame):
        log.info("\nStopped.")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    open_trade_log()
    log.info("Initializing SDK...")
    client = init_clob_client()
    redeemer = init_redeemer()

    log.info("SDK ready. Placing 45c orders on BTC 5m markets.")
    if redeemer:
        log.info("Auto-redeem enabled (on-chain via %s).\n", redeemer[1].address)
    else:
        log.info("Auto-redeem DISABLED (no private key / RPC).\n")

    # Start WebSocket feed for real-time prices
    ws_feed = WSBookFeed()

    placed_markets = set()
    # Track past markets: {ts: {redeemed, bailed, up_token, dn_token, closed}}
    # NOTE: never reassign this dict — hedge_loop holds a reference to it.
    past_markets = {}

    # Start fast hedge loop (1s interval) as a background task
    asyncio.create_task(hedge_loop(client, ws_feed, past_markets))

    # Scan recent markets (last 2 hours) for unredeemed positions on startup
    now = int(time.time())
    log.info("Scanning recent markets for unredeemed positions...")
    # Scan last 2 hours of 5-minute markets (7200s / 300s = 24 markets)
    scan_ts = (now - 7200) // MARKET_PERIOD * MARKET_PERIOD
    while scan_ts < now:
        slug = f"btc-updown-5m-{scan_ts}"
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
            placed_markets.add(scan_ts)
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
            mkt = await get_market_info(f"btc-updown-5m-{next_ts}")
            cache["ts"] = next_ts
            cache["mkt"] = mkt
        except Exception:
            pass

    while True:
        now = int(time.time())
        timestamps = next_market_timestamps(now)
        next_market_ts = (now // MARKET_PERIOD) * MARKET_PERIOD + MARKET_PERIOD

        for ts in timestamps:
            if ts in placed_markets:
                continue

            slug = f"btc-updown-5m-{ts}"

            # Use pre-fetched market data if we have it (saves one round-trip at market open)
            if ts == next_market_ts and next_market_cache.get("ts") == ts and next_market_cache.get("mkt"):
                mkt = next_market_cache["mkt"]
                next_market_cache["ts"] = None
                next_market_cache["mkt"] = None
            else:
                try:
                    mkt = await get_market_info(slug)
                except Exception as e:
                    log.debug("Market %s not available yet: %s", slug, e)
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
            up_bal, dn_bal, live_orders = await asyncio.gather(
                asyncio.to_thread(check_token_balance, client, mkt["up_token"]),
                asyncio.to_thread(check_token_balance, client, mkt["dn_token"]),
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

            # Enforce 15-second placement window (matching Rust bot).
            # ts is the market START timestamp; secs_elapsed is negative for
            # future markets (we still pre-register them) and 0-300 for live ones.
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
                open_count = sum(1 for _ts, _info in past_markets.items() if not _info.get("redeemed"))
                if open_count >= MAX_OPEN_MARKETS:
                    log.info(
                        "  Skipping placement — at max open markets (%d >= %d)",
                        open_count, MAX_OPEN_MARKETS,
                    )
                    log_trade("max_open_skipped", market_ts=ts, slug=slug, open_count=open_count, max=MAX_OPEN_MARKETS)
                    continue  # retry next loop when a slot frees

            # Place both sides in parallel so they hit the book at nearly the same time
            up_id, dn_id = await asyncio.gather(
                asyncio.to_thread(place_order, client, mkt["up_token"], "UP  ", SHARES_PER_SIDE, PRICE),
                asyncio.to_thread(place_order, client, mkt["dn_token"], "DOWN", SHARES_PER_SIDE, PRICE),
            )

            placed = (1 if up_id else 0) + (1 if dn_id else 0)
            log.info("  Done: %d orders placed. Max cost: $%.2f", placed, SHARES_PER_SIDE * PRICE * 2)
            log.info("")
            log_trade("orders_placed", market_ts=ts, slug=slug,
                      title=mkt.get("title", slug), price=PRICE, shares=SHARES_PER_SIDE,
                      up_id=up_id, dn_id=dn_id, sides_placed=placed,
                      elapsed_s=now - ts)

            placed_markets.add(ts)
            past_markets[ts] = {
                "redeemed": False,
                "closed": mkt["closed"],
                "up_token": mkt["up_token"],
                "dn_token": mkt["dn_token"],
                "condition_id": mkt["conditionId"],
                "neg_risk": mkt.get("neg_risk", False),
                "title": mkt.get("title", slug),
                # FIX: use market start timestamp (ts) as anchor, not wall-clock
                # placement time. Matches Rust bot: time_elapsed = now - ts, so
                # hedge windows (120s, 180s, 240s) are relative to market start,
                # not to when place_order() returned.
                "placed_at": ts,
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

        # Bail out early if one side > 72c and we hold the other (sell filled side)
        await check_bail_out(client, ws_feed, past_markets)
        # Merge complete sets (UP+DN) into USDC only after market has ended
        await try_merge_all(client, redeemer, past_markets)
        # Redeem resolved markets (remaining winning/losing tokens after resolution)
        await try_redeem_all(client, redeemer, past_markets)

        # Faster poll when close to next market start (better chance to fill both sides)
        seconds_until = next_market_ts - now
        sleep_s = 0.5 if seconds_until <= 15 else (1 if seconds_until <= 60 else 2)
        # Pre-fetch next market in background so we have it when the new period starts
        if seconds_until <= NEXT_MARKET_PREFETCH_SEC and seconds_until > 0:
            asyncio.create_task(prefetch_next_market(next_market_ts, next_market_cache))
        await asyncio.sleep(sleep_s)

        # Log status with countdown
        log.info("[Next market in %ds] Checked %d markets", seconds_until, len(timestamps))

        # Clean very old entries (keep last 2 hours for redemption).
        # Use in-place deletion to preserve the reference held by hedge_loop.
        stale = [ts for ts in past_markets if ts <= now - 7200]
        for ts in stale:
            del past_markets[ts]
        # FIX: placed_markets must survive long enough to cover the previous period
        # (next_market_timestamps always returns ts-300 which is ~5 min old).
        # Prune at 2×MARKET_PERIOD so the previous period is never re-placed.
        placed_markets = {ts for ts in placed_markets if ts > now - 2 * MARKET_PERIOD}


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("\nStopped.")
