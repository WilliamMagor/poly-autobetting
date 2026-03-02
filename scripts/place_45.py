"""
Dual-fill + merge bot for Polymarket BTC 5-min Up/Down binary markets.

Strategy: place maker-only GTC limit buys at PRICE on both UP and DN, 10 shares
each side.  When both fill, merge for $1.00 USDC.  When only one fills, hedge
quickly (≤ INSTANT_HEDGE_MAX) or bail via EV-based sliding thresholds.

Key quant rules (v2):
  * Polymarket fee curve:  maker 0%, taker ≈ 1.5% peak near p=0.50.
  * Maker-only entry:  only post limit if best_ask > our price (rests on book).
  * Instant hedge:  after HEDGE_GRACE_S, buy unfilled if ask ≤ INSTANT_HEDGE_MAX
    AND fee-adjusted profit ≥ MIN_HEDGE_EDGE.
  * Trailing hedge:  same cap as instant; no bands above INSTANT_HEDGE_MAX.
  * No force-buy:  if no hedge opportunity, let position ride to resolution.
  * EV-based bail:  compare bail P&L (including taker fee + slippage) vs
    hold-to-resolution EV; only bail when it's strictly better.
  * On-chain fill verification:  confirm CTF balance on Polygon before hedging.
  * Volatility + liquidity filters:  skip rounds when conditions are bad.
  * Gas ceiling:  delay non-urgent merge/redeem when gas is elevated.
  * Position sizing:  parameterized, scales from BASE_SHARES upward.
  * Session risk:  configurable loss limit, consecutive-bail pause.

Auto-merges complete sets (UP+DN) into USDC on-chain after market ends.
Auto-redeems resolved positions via direct on-chain Polygon transaction.

Usage: python scripts/place_45.py
"""

import asyncio
import json
import logging
import os
import re
import signal
import sys
import time
from collections import deque

import httpx

try:
    from rich import box as _rbox
    from rich.layout import Layout as _Layout
    from rich.live import Live as _Live
    from rich.panel import Panel as _Panel
    from rich.table import Table as _Table
    from rich.text import Text as _Text
    _RICH = True
except ImportError:
    _RICH = False
    _rbox = _Layout = _Live = _Panel = _Table = _Text = None

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

# Ring buffer of recent events shown in the live dashboard (newest first).
_dashboard_events: deque = deque(maxlen=18)
_DASH_EVENTS = {
    "orders_placed", "fill_detected", "hedge_trigger", "hedge_buy",
    "bail_execute", "merge", "redeem", "conviction_add_placed",
    "ghost_fill", "bail_pause", "session_loss_skip", "stale_cancel",
    "vol_skip_low", "depth_skip", "maker_skip_both", "window_missed",
}

def open_trade_log():
    """Open (or create) today's JSONL trade log. Called once at startup.

    When rich is available, also redirects Python logging to a .log text file
    so the terminal is left clean for the live dashboard.
    """
    global _trade_log
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    date_str = time.strftime("%Y%m%d")
    path = os.path.join(log_dir, f"place_45_{date_str}.jsonl")
    _trade_log = open(path, "a", buffering=1)  # line-buffered

    if _RICH:
        # Redirect all log.* output to a text file — the rich dashboard takes over the terminal.
        text_path = os.path.join(log_dir, f"place_45_{date_str}.log")
        fh = logging.FileHandler(text_path, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(fh)
        log.info("Dashboard mode active — console logs → %s", text_path)
    else:
        log.info("Trade log: %s", path)

def log_trade(event: str, **fields):
    """Append one structured event to today's JSONL trade log."""
    if _trade_log is None:
        return
    record = {"ts": int(time.time()), "event": event, **fields}
    _trade_log.write(json.dumps(record) + "\n")
    if _RICH and event in _DASH_EVENTS:
        _dashboard_events.appendleft(record)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG — all tunables, env-overridable
# ═══════════════════════════════════════════════════════════════════════════════

# --- Entry price & position sizing ---
PRICE = float(os.getenv("PLACE_45_PRICE", "0.45"))
BASE_SHARES = int(os.getenv("PLACE_45_SHARES", "10"))
SHARES_PER_SIDE = BASE_SHARES  # alias; can be dynamically reduced by vol filter

# --- Polymarket fee model (exact formula for 5-min/15-min crypto) ---
# Taker fee = shares * fee_rate * (p * (1-p))^2.
# fee_rate defaults to 0.25 (Polymarket's documented rate for crypto markets).
# Maker fee = 0.  We treat initial limits as maker-only.
FEE_RATE = float(os.getenv("PLACE_45_FEE_RATE", "0.25"))

# --- Maker-only entry guard ---
# Minimum gap between our limit price and best_ask for the order to rest
# as maker on the book (never cross spread).
MAKER_MIN_GAP = float(os.getenv("PLACE_45_MAKER_GAP", "0.01"))
# If best_ask is too close, we can place at best_ask - MAKER_MIN_GAP,
# but never below this floor.
MAKER_PRICE_FLOOR = float(os.getenv("PLACE_45_MAKER_FLOOR", "0.42"))

# --- Hedge logic ---
# Grace lowered 30→15s: real fills land at 4–42s in (per log data); starting the hedge
# watch at 15s catches asks while the book is still close to the entry price.
HEDGE_GRACE_S = int(os.getenv("PLACE_45_HEDGE_GRACE_S", "15"))
# At PRICE=0.45 the profitable hedge range extends to ~0.527 (net ≥ $0.03 after fee).
# Raised from 0.52 → 0.53 so the full profitable range is captured.
INSTANT_HEDGE_MAX = float(os.getenv("PLACE_45_HEDGE_MAX", "0.53"))
# Lowered from $0.05 → $0.03: accepts hedges up to ~0.530 which are still net-positive.
# $0.03 is still a meaningful edge floor; prevents hedging at a loss.
MIN_HEDGE_EDGE = float(os.getenv("PLACE_45_MIN_HEDGE_EDGE", "0.03"))
# Trailing bounce threshold lowered 0.02 → 0.015: volatile 5-min BTC markets reverse
# in smaller increments; 1.5c bounce is still confirmation without being hair-trigger.
HEDGE_TRAILING_DELTA = float(os.getenv("PLACE_45_HEDGE_DELTA", "0.015"))

# --- Bail logic (EV-based, time-gated) ---
# Opened 30s earlier (180→150): gives more time to exit at a decent bid before
# the book thins out in the final 2.5 minutes of a losing position.
BAIL_NO_BAIL_BEFORE_S = int(os.getenv("PLACE_45_BAIL_NO_BEFORE", "150"))
# Sliding thresholds on *unfilled* side ask.  If unfilled ask > threshold at
# that time bucket, we consider bailing (but EV check must also pass).
# All tightened 3c: volatile BTC markets mean the filled side's bid deteriorates
# fast once a trend is established — cutting earlier preserves more capital.
BAIL_THRESHOLD_180 = float(os.getenv("PLACE_45_BAIL_T180", "0.65"))
BAIL_THRESHOLD_120 = float(os.getenv("PLACE_45_BAIL_T120", "0.58"))
BAIL_THRESHOLD_60  = float(os.getenv("PLACE_45_BAIL_T60",  "0.53"))
# Minimum bid-side depth (shares) to accept a bail exit without excess slippage.
BAIL_MIN_BID_DEPTH = float(os.getenv("PLACE_45_BAIL_MIN_DEPTH", "5.0"))
# Raised from $0.30 → $0.40: with 5-min rounds, freeing $9 of capital even 2 min
# early realistically captures another dual-fill.  Makes bail more competitive vs hold.
BAIL_CAPITAL_OPP = float(os.getenv("PLACE_45_BAIL_CAP_OPP", "0.40"))

# --- Conviction add (increase exposure when holding to resolution) ---
# After a one-sided fill where neither hedge nor bail fired, if BTC spot has moved
# decisively in the direction of our held token AND Polymarket hasn't priced it in
# yet, buy additional shares at market (taker) to increase directional exposure.
# This exploits the typical 15-45s lag between BTC spot ticks and CLOB updates.
CONV_ADD_ENABLED = os.getenv("PLACE_45_CONV_ADD", "1").strip().lower() in ("1", "true", "yes")
# Only fire inside this time window before resolution (seconds remaining).
CONV_ADD_MAX_S = int(os.getenv("PLACE_45_CONV_MAX_S", "90"))
CONV_ADD_MIN_S = int(os.getenv("PLACE_45_CONV_MIN_S", "15"))
# BTC must have moved at least this % from placement price in direction of held side.
CONV_ADD_BTC_MOVE_PCT = float(os.getenv("PLACE_45_CONV_BTC_PCT", "0.25"))
# Minimum gap between BTC-implied probability and current Polymarket ask (the edge).
CONV_ADD_EDGE_MIN = float(os.getenv("PLACE_45_CONV_EDGE", "0.06"))
# Hard cap on additional shares per conviction add (half-Kelly scaled, never above this).
CONV_ADD_MAX_SHARES = int(os.getenv("PLACE_45_CONV_MAX_SHARES", "5"))
# Don't add if session net P&L is already below this threshold (loss gate).
CONV_ADD_MAX_LOSS = float(os.getenv("PLACE_45_CONV_MAX_LOSS", "15.0"))

# --- Session risk ---
SESSION_LOSS_LIMIT = float(os.getenv("PLACE_45_LOSS_LIMIT", "30.0"))
# Reduced from 3 → 2: two consecutive bails signals a trending BTC regime where
# dual-fills won't land.  Pausing earlier limits drawdown from the streak.
CONSECUTIVE_BAIL_PAUSE = int(os.getenv("PLACE_45_BAIL_PAUSE_N", "2"))
CONSECUTIVE_BAIL_PAUSE_S = int(os.getenv("PLACE_45_BAIL_PAUSE_S", "900"))  # 15 min
# Reduced from 0.5 → 0.4: after a bail streak, return at 40% size (4 shares) rather
# than 50% — more conservative re-entry while the trend may still be in play.
BAIL_REDUCE_FACTOR = float(os.getenv("PLACE_45_BAIL_REDUCE", "0.4"))

# --- Volatility & liquidity filters ---
# BTC 5-min ATR% thresholds.
VOL_MIN_ATR_PCT = float(os.getenv("PLACE_45_VOL_MIN", "0.03"))   # skip if < 0.03%
# Tightened 0.50→0.40%: above 0.40% ATR the market is in a trending/breakout regime
# where one-sided fills are likely and hedge prices spike above INSTANT_HEDGE_MAX fast.
# Reducing size earlier is the single best risk-adjusted lever for volatile BTC.
VOL_MAX_ATR_PCT = float(os.getenv("PLACE_45_VOL_MAX", "0.40"))   # start reducing above 0.40%
# Hard cap tightened 1.0→0.75%: above 0.75% ATR BTC is breaking out — floor to 20% size.
VOL_HARD_CAP_PCT = float(os.getenv("PLACE_45_VOL_HARD_CAP", "0.75"))  # floor at 20% size above this
# Minimum orderbook depth (shares) within 3c of PRICE on each side.
MIN_BOOK_DEPTH = float(os.getenv("PLACE_45_MIN_DEPTH", "15.0"))

# --- Gas ceiling (Polygon, gwei) ---
GAS_CEILING_GWEI = float(os.getenv("PLACE_45_GAS_CEIL", "100.0"))
# Non-urgent = merge/redeem.  Urgent = hedge/bail (never skip).

# --- Stale cancel & placement window ---
STALE_CANCEL_BEFORE_END_S = int(os.getenv("PLACE_45_STALE_CANCEL", "60"))
NEW_MARKET_PLACE_WINDOW_SECONDS = 60  # skip if market started > 60s ago (long merges can delay the loop)

# --- Infrastructure ---
GAMMA_URL = "https://gamma-api.polymarket.com"
MARKET_PERIOD = 300      # 5 minutes (300 seconds)
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
MAX_OPEN_MARKETS = int(os.getenv("PLACE_45_MAX_OPEN_MARKETS", "0"))
HEDGE_USE_REST_FALLBACK = os.getenv("PLACE_45_REST_FALLBACK", "0").strip().lower() in ("1", "true", "yes")
NEXT_MARKET_PREFETCH_SEC = 60

# --- On-chain verification ---
ONCHAIN_VERIFY = os.getenv("PLACE_45_ONCHAIN_VERIFY", "1").strip().lower() in ("1", "true", "yes")
INTEGRITY_INTERVAL_S = float(os.getenv("PLACE_45_INTEGRITY_INTERVAL_S", "3.0"))
BLACKLIST_ENABLED = os.getenv("PLACE_45_BLACKLIST_ENABLED", "1").strip().lower() in ("1", "true", "yes")
# Blacklist file for suspicious counterparties.
BLACKLIST_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "logs", "blacklist.json",
)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS — fees, EV, volatility, on-chain, gas
# ═══════════════════════════════════════════════════════════════════════════════

def polymarket_taker_fee(price: float, shares: float, fee_rate: float = None) -> float:
    """Polymarket taker fee in USDC (exact formula for 5-min/15-min crypto).

    fee = shares * fee_rate * (p * (1-p))^2

    At p=0.50 with fee_rate=0.25:  fee = 10 * 0.25 * 0.0625 = $0.15625/share.
    At p=0.45:                      fee = 10 * 0.25 * (0.45*0.55)^2 ≈ $0.152.
    Maker fee = 0 (our GTC limits rest on the book).
    """
    if fee_rate is None:
        fee_rate = FEE_RATE
    p = max(0.01, min(0.99, price))
    return round(shares * fee_rate * (p * (1.0 - p)) ** 2, 6)


def compute_hedge_ev(fill_price: float, hedge_price: float, shares: float) -> tuple[float, float]:
    """Expected P&L for a hedge trade after taker fee + gas.

    Returns (net_pnl, fee_dollars).
    fill_price is maker (0 fee).  hedge_price is taker.
    """
    fill_cost = fill_price * shares
    hedge_cost = hedge_price * shares
    hedge_fee = polymarket_taker_fee(hedge_price, shares)
    merge_return = 1.0 * shares
    gas_est = 0.01
    net_pnl = merge_return - fill_cost - hedge_cost - hedge_fee - gas_est
    return round(net_pnl, 4), round(hedge_fee, 4)


def compute_bail_ev(
    fill_price: float,
    sell_bid: float,
    shares: float,
    unfilled_ask: float,
    time_remaining_s: float,
) -> tuple[float, float, bool]:
    """Compare bail-now EV vs hold-to-resolution EV.

    Returns (bail_pnl, hold_ev, should_bail).
    Bail includes taker fee on the sale.
    Hold uses market-implied win probability from the *filled* side's bid.
    Capital opportunity adds value to freeing funds early.
    """
    sell_proceeds = sell_bid * shares
    sell_fee = polymarket_taker_fee(sell_bid, shares)
    bail_pnl = sell_proceeds - sell_fee - (fill_price * shares)

    # Market-implied win probability: use BOTH sides for consistency.
    # Filled side bid ≈ market's view of filled win prob.
    # Unfilled ask ≈ market's view of unfilled win prob.
    # In a binary market: filled_win ≈ 1 - unfilled_win.
    # Average the two estimates for a more robust signal.
    filled_implied = sell_bid
    unfilled_implied = 1.0 - unfilled_ask  # if unfilled ask is 0.70, filled win ≈ 0.30
    win_prob = max(0.05, min(0.95, (filled_implied + unfilled_implied) / 2.0))
    hold_ev = (win_prob * 1.0 * shares) - (fill_price * shares)

    # Capital opportunity: freeing capital to redeploy scales with time left.
    capital_opp = BAIL_CAPITAL_OPP * (time_remaining_s / MARKET_PERIOD)
    bail_adjusted = bail_pnl + capital_opp

    return round(bail_pnl, 4), round(hold_ev, 4), bail_adjusted > hold_ev


async def get_btc_volatility() -> dict:
    """Fetch BTC 5-min klines from Binance and compute short-term ATR metrics."""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "5m", "limit": 12},
            )
            r.raise_for_status()
            klines = r.json()

        closes = [float(k[4]) for k in klines]
        highs = [float(k[2]) for k in klines]
        lows = [float(k[3]) for k in klines]

        true_ranges = []
        for i in range(1, len(klines)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            true_ranges.append(tr)

        atr = sum(true_ranges) / len(true_ranges) if true_ranges else 0
        last_price = closes[-1] if closes else 85000
        atr_pct = (atr / last_price) * 100

        return {"atr": atr, "atr_pct": atr_pct, "last_price": last_price, "ok": True}
    except Exception as e:
        log.warning("Volatility check failed: %s", e)
        return {"atr": 0, "atr_pct": 0, "last_price": 0, "ok": False}


def check_book_depth(client, token_id: str, near_price: float, min_depth: float = 15.0) -> dict:
    """Check orderbook depth within 3c of a price level."""
    try:
        book = client.get_order_book(token_id)
        ask_depth = sum(
            float(a.size)
            for a in (book.asks or [])
            if float(a.price) <= near_price + 0.03
        )
        bid_depth = sum(
            float(b.size)
            for b in (book.bids or [])
            if float(b.price) >= near_price - 0.03
        )
        best_ask = float(book.asks[0].price) if book.asks else None
        best_bid = float(book.bids[0].price) if book.bids else None
        return {
            "ask_depth": ask_depth,
            "bid_depth": bid_depth,
            "best_ask": best_ask,
            "best_bid": best_bid,
            "sufficient": ask_depth >= min_depth and bid_depth >= min_depth,
        }
    except Exception:
        return {
            "ask_depth": 0, "bid_depth": 0,
            "best_ask": None, "best_bid": None,
            "sufficient": False,
        }


def get_bid_depth_at(client, token_id: str, min_price: float) -> float:
    """Total bid size available at or above min_price (for slippage check)."""
    try:
        book = client.get_order_book(token_id)
        return sum(float(b.size) for b in (book.bids or []) if float(b.price) >= min_price)
    except Exception:
        return 0.0


def verify_onchain_balance(redeemer, token_id: str) -> float:
    """Query CTF ERC-1155 balanceOf on Polygon for actual on-chain balance.

    Returns balance in shares (float).  Returns -1.0 if verification fails
    (so callers can distinguish 'cannot verify' from 'balance is 0').
    """
    if not redeemer:
        return -1.0
    from web3 import Web3

    w3, account = redeemer
    abi = [
        {
            "name": "balanceOf",
            "type": "function",
            "inputs": [
                {"name": "account", "type": "address"},
                {"name": "id", "type": "uint256"},
            ],
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
        }
    ]
    contract = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=abi)
    try:
        raw = contract.functions.balanceOf(account.address, int(token_id)).call()
        return raw / 1e6
    except Exception as e:
        log.warning("On-chain balance check failed for %s: %s", token_id[:12], e)
        return -1.0


def check_order_still_live(client, order_id: str) -> bool:
    """Verify that an order still exists on the book (ghost-fill protection)."""
    if not order_id:
        return False
    try:
        order = client.get_order(order_id)
        return order.get("status") in ("LIVE", "MATCHED")
    except Exception:
        return False


def get_polygon_gas_gwei(redeemer) -> float:
    """Current Polygon gas price in gwei.  Returns 0 if unavailable."""
    if not redeemer:
        return 0.0
    try:
        w3, _ = redeemer
        return w3.eth.gas_price / 1e9
    except Exception:
        return 0.0


def load_blacklist() -> set:
    """Load counterparty blacklist from disk."""
    try:
        with open(BLACKLIST_PATH, "r") as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_blacklist(bl: set):
    """Persist counterparty blacklist to disk."""
    try:
        os.makedirs(os.path.dirname(BLACKLIST_PATH), exist_ok=True)
        with open(BLACKLIST_PATH, "w") as f:
            json.dump(list(bl), f)
    except Exception:
        pass


def _market_entry(mkt: dict, slug: str, placed_at: int = 0, **extra) -> dict:
    """Build a past_markets entry dict from a market info response.

    All repeated past_markets[ts] = {...} blocks use this helper so that
    the field set is defined exactly once and callers only supply what differs.
    Extra kwargs (e.g. up_order_id, shares, btc_at_place) are merged in.
    """
    return {
        "redeemed": False,
        "closed": mkt["closed"],
        "up_token": mkt["up_token"],
        "dn_token": mkt["dn_token"],
        "condition_id": mkt["conditionId"],
        "neg_risk": mkt.get("neg_risk", False),
        "title": mkt.get("title", slug),
        "placed_at": placed_at,
        **extra,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS — timestamps, slugs, validation
# ═══════════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════════
# API FUNCTIONS — market info, SDK init (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

async def get_market_info(slug: str) -> dict:
    """Fetch market info from gamma API with input validation."""
    # Validate slug format to prevent injection
    if not slug or not isinstance(slug, str):
        raise ValueError(f"Invalid slug: expected non-empty string, got {slug}")
    if len(slug) > 200:
        raise ValueError(f"Invalid slug: too long (max 200 chars), got {len(slug)}")
    # Only allow expected slug pattern
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


# ═══════════════════════════════════════════════════════════════════════════════
# ORDER FUNCTIONS — place, sell, hedge buy (minor additions for logging)
# ═══════════════════════════════════════════════════════════════════════════════

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
    """Place a limit SELL at the current best bid. Returns order ID or None.

    Retries up to 3 times with 1s delay. A bail sell that silently fails
    leaves the position stranded, so we always retry before giving up.
    """
    from py_clob_client.order_builder.constants import SELL
    from py_clob_client.clob_types import OrderArgs, OrderType

    for attempt in range(1, 4):
        try:
            book = client.get_order_book(token_id)
            if not book.bids:
                log.warning("  %s no bids to sell into (attempt %d/3)", side_label, attempt)
                if attempt < 3:
                    time.sleep(1)
                    continue
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
                log.warning("  %s SELL no order ID: %s (attempt %d/3)", side_label, result, attempt)
        except Exception as e:
            log.error("  %s SELL failed (attempt %d/3): %s", side_label, attempt, e)
        if attempt < 3:
            time.sleep(1)
    log.error("  %s SELL failed after 3 attempts — position may be stranded!", side_label)
    return None


def buy_hedge(client, token_id: str, shares: float, side_label: str, price: float) -> str | None:
    """Place a limit BUY for the hedge side (post-one-sided-fill).

    Uses the same limit-order path as the original placement but at
    the current market price rather than the fixed entry price.
    Retries up to 3 times with 1s delay (5-min markets need fast execution).
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
            time.sleep(1)
    log.error("  HEDGE %s order failed after 3 attempts", side_label)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# HEDGE LOGIC — instant + trailing with fee-aware EV, no force-buy
# ═══════════════════════════════════════════════════════════════════════════════

async def check_hedge_trailing(
    client,
    ws_feed: "WSBookFeed",
    past_markets: dict,
    redeemer,
    session_stats: dict,
):
    """Time-windowed hedge logic for one-sided fills.

    v2 changes vs original:
      1. On-chain fill verification before treating a fill as real.
      2. Cancel unfilled limit IMMEDIATELY on detecting one-sided fill.
      3. 30s grace period (configurable) before any hedge action.
      4. Instant hedge if ask ≤ INSTANT_HEDGE_MAX (0.52) AND EV ≥ MIN_HEDGE_EDGE.
      5. Trailing hedge also capped at INSTANT_HEDGE_MAX with EV check.
      6. NO force-buy — if > 0.52, fall through to bail logic or hold.
      7. Track hedge_price in info for session P&L.
    """
    now = int(time.time())

    for ts, info in list(past_markets.items()):
        # Skip fully-handled markets
        if info.get("hedge_done") or info.get("redeemed") or info.get("closed") or info.get("bailed"):
            continue
        if info.get("compromised"):
            continue  # ghost-fill flagged market

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        placed_at = info.get("placed_at", 0)
        if not up_token or not dn_token or not placed_at:
            continue

        # Use per-market shares (may differ from current session shares due to vol/bail sizing)
        shares = info.get("shares", session_stats.get("current_shares", SHARES_PER_SIDE))
        time_elapsed = now - placed_at

        # ── Step 1: Detect one-sided fill ──────────────────────────────────
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
                # Both filled (complete set) or neither filled — nothing to do
                continue

            # ── Step 1b: On-chain verification ─────────────────────────────
            filled_token_id = up_token if hedge_side == "dn" else dn_token
            if ONCHAIN_VERIFY and redeemer:
                # Run in thread so the RPC call never stalls the event loop.
                onchain_bal = await asyncio.to_thread(verify_onchain_balance, redeemer, filled_token_id)
                if onchain_bal == 0:
                    # API says filled but on-chain says 0 → ghost fill!
                    log.warning(
                        "  WARNING GHOST FILL detected at %ds: API says %s filled=%.1f "
                        "but on-chain=0. Flagging market as compromised.",
                        time_elapsed,
                        "UP" if hedge_side == "dn" else "DN",
                        filled_bal,
                    )
                    log_trade(
                        "ghost_fill", market_ts=ts,
                        filled_side="UP" if hedge_side == "dn" else "DN",
                        api_bal=filled_bal, onchain_bal=0,
                    )
                    # Cancel all orders for this market
                    cancel_market_orders(client, {up_token, dn_token})
                    info["compromised"] = True
                    # Blacklist: try to extract counterparty from fill metadata
                    if BLACKLIST_ENABLED:
                        bl = session_stats.get("_blacklist", set())
                        # Attempt to get maker/taker address from order fill
                        for oid_key in ("up_order_id", "dn_order_id"):
                            oid = info.get(oid_key)
                            if not oid:
                                continue
                            try:
                                order = client.get_order(oid)
                                counterparty = order.get("maker_address") or order.get("taker_address")
                                if counterparty:
                                    bl.add(counterparty)
                                    log.warning("  BLACKLIST added counterparty: %s", counterparty[:16])
                                    log_trade("blacklist_add", address=counterparty, reason="ghost_fill")
                            except Exception:
                                pass
                        session_stats["_blacklist"] = bl
                        save_blacklist(bl)
                    continue
                elif onchain_bal >= 0 and onchain_bal < filled_bal * 0.9:
                    log.warning(
                        "  WARNING Balance mismatch: API=%.1f on-chain=%.1f for %s. "
                        "Using on-chain value.",
                        filled_bal, onchain_bal,
                        "UP" if hedge_side == "dn" else "DN",
                    )
                    filled_bal = onchain_bal

            log.info(
                "  HEDGE detected one-sided fill at %ds: %s filled (%.1f), "
                "will hedge %s",
                time_elapsed,
                "UP" if hedge_side == "dn" else "DN",
                filled_bal,
                hedge_side.upper(),
            )
            info["hedge_side"] = hedge_side
            info["filled_bal"] = filled_bal
            info["trailing_min_ask"] = None
            log_trade(
                "fill_detected", market_ts=ts,
                filled_side="UP" if hedge_side == "dn" else "DN",
                filled_bal=filled_bal, hedge_side=hedge_side.upper(),
                elapsed_s=time_elapsed, onchain_verified=ONCHAIN_VERIFY,
            )

            # ── Step 1c: Cancel unfilled limit IMMEDIATELY ─────────────────
            hedge_token = up_token if hedge_side == "up" else dn_token
            log.info("  HEDGE cancelling unfilled %s limit immediately", hedge_side.upper())
            cancel_market_orders(client, {hedge_token})
            info["hedge_cancelled"] = True

        # ── Step 2: Get current state ──────────────────────────────────────
        hedge_side: str = info["hedge_side"]
        hedge_token = up_token if hedge_side == "up" else dn_token
        filled_token = dn_token if hedge_side == "up" else up_token
        filled_label = "UP" if hedge_side == "dn" else "DN"

        hedge_ask = get_best_ask_safe(ws_feed, client, hedge_token)
        if hedge_ask is None:
            continue

        # Track trailing minimum
        prev_min = info.get("trailing_min_ask")
        if prev_min is None or hedge_ask < prev_min:
            info["trailing_min_ask"] = hedge_ask
            if prev_min is not None:
                log.info("  HEDGE trail: new low for %s ask=%.3f", hedge_side.upper(), hedge_ask)
        trailing_min = info["trailing_min_ask"]

        # ── Step 3: Grace period — wait for natural fill ───────────────────
        if time_elapsed < HEDGE_GRACE_S:
            continue

        # ── Step 4: Periodic integrity check (every INTEGRITY_INTERVAL_S) ────
        # Verify filled position hasn't vanished (ghost-fill / liquidity-drain).
        last_integrity = info.get("last_integrity_check", 0)
        if now - last_integrity >= INTEGRITY_INTERVAL_S:
            info["last_integrity_check"] = now
            # Verify filled side balance hasn't disappeared
            try:
                current_filled_bal = check_token_balance(client, filled_token)
                if current_filled_bal <= 0 and info.get("filled_bal", 0) > 0:
                    log.warning(
                        "  WARNING Filled position vanished! %s was %.1f, now 0. "
                        "Flagging market as compromised.",
                        filled_label, info["filled_bal"],
                    )
                    log_trade("position_vanished", market_ts=ts, side=filled_label)
                    info["compromised"] = True
                    cancel_market_orders(client, {up_token, dn_token})
                    continue
            except Exception:
                pass

        # ── Step 5: Hedge decision — instant or trailing ───────────────────
        if hedge_ask > INSTANT_HEDGE_MAX:
            # Too expensive — log and let bail logic handle it
            log.info(
                "  HEDGE wait: %s ask=%.3f > cap=%.2f (elapsed=%ds)",
                hedge_side.upper(), hedge_ask, INSTANT_HEDGE_MAX, time_elapsed,
            )
            continue

        # Fee-aware EV check — use actual fill price (may differ from PRICE if maker-adjusted)
        actual_fill_price = info.get(
            "dn_price" if hedge_side == "up" else "up_price", PRICE
        )  # filled side's actual entry price
        net_pnl, fee = compute_hedge_ev(actual_fill_price, hedge_ask, shares)
        if net_pnl < MIN_HEDGE_EDGE:
            log.info(
                "  HEDGE skip: %s ask=%.3f, EV=$%.3f < min_edge=$%.2f (fee=$%.3f)",
                hedge_side.upper(), hedge_ask, net_pnl, MIN_HEDGE_EDGE, fee,
            )
            continue

        # Decide: instant trigger or trailing bounce trigger
        # Instant: fires within HEDGE_GRACE_S*2 of grace end (first 60s window after grace).
        # After that window, require a trailing bounce to confirm direction reversal.
        instant_window = time_elapsed < (HEDGE_GRACE_S * 2)
        instant_trigger = instant_window  # ask is ≤ cap AND EV positive AND within window
        trailing_trigger = False
        if trailing_min is not None:
            bounce = hedge_ask - trailing_min
            min_bounce = HEDGE_TRAILING_DELTA * max(0.33, 1.0 - (time_elapsed - HEDGE_GRACE_S) / 270.0)
            trailing_trigger = bounce >= min_bounce

        if not instant_trigger and not trailing_trigger:
            log.info(
                "  HEDGE wait-trailing: %s ask=%.3f (EV ok) but no bounce yet "
                "(low=%.3f, elapsed=%ds, past instant window)",
                hedge_side.upper(), hedge_ask, trailing_min or 0, time_elapsed,
            )
            continue

        if instant_trigger or trailing_trigger:
            reason = "instant" if instant_trigger else "trailing_bounce"
            log.info(
                "  HEDGE TRIGGER (%s): %s ask=%.3f, EV=+$%.3f, fee=$%.3f (elapsed=%ds)",
                reason, hedge_side.upper(), hedge_ask, net_pnl, fee, time_elapsed,
            )
            log_trade(
                "hedge_trigger", market_ts=ts, hedge_side=hedge_side.upper(),
                ask=round(hedge_ask, 4), ev=net_pnl, fee=fee,
                trail_min=round(trailing_min, 4) if trailing_min else None,
                reason=reason, elapsed_s=time_elapsed,
            )

            # Balance check before buy (prevents double-position)
            current_bal = check_token_balance(client, hedge_token)
            if current_bal >= shares - 0.01:
                log.info(
                    "  HEDGE skipped: already hold %.1f %s shares",
                    current_bal, hedge_side.upper(),
                )
                info["hedge_done"] = True
                continue

            oid = buy_hedge(client, hedge_token, shares, hedge_side.upper(), hedge_ask)
            log_trade(
                "hedge_buy", market_ts=ts, side=hedge_side.upper(),
                price=round(hedge_ask, 4), shares=shares,
                order_id=oid, ev=net_pnl, fee=fee, elapsed_s=time_elapsed,
            )
            info["hedge_done"] = True
            info["hedge_price"] = hedge_ask
            session_stats["hedged"] = session_stats.get("hedged", 0) + 1
            session_stats["total_fees"] = session_stats.get("total_fees", 0) + fee


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


def get_best_bid_safe(ws_feed: "WSBookFeed", client, token_id: str) -> float | None:
    """Best bid from WS; REST fallback if enabled."""
    bid = ws_feed.get_best_bid(token_id)
    if bid is not None:
        return bid
    if not HEDGE_USE_REST_FALLBACK:
        return None
    try:
        book = client.get_order_book(token_id)
        if book and book.bids:
            return float(book.bids[0].price)
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


# ═══════════════════════════════════════════════════════════════════════════════
# BAIL LOGIC — EV-based, time-gated, slippage-aware
# ═══════════════════════════════════════════════════════════════════════════════

async def check_bail_out(
    client,
    ws_feed: WSBookFeed,
    past_markets: dict,
    session_stats: dict,
):
    """EV-based bail for one-sided fills.

    v2 changes:
      * No bail when > 180s remaining (only hedge or wait).
      * Sliding thresholds on unfilled side ask, time-gated.
      * Before bailing, check bid depth to limit slippage.
      * Compute bail EV vs hold-to-resolution EV (including taker fee).
      * Only bail if bail EV > hold EV (with capital opportunity credit).
      * Track in session_stats.
    """
    now = int(time.time())

    for ts, info in list(past_markets.items()):
        if info.get("bailed") or info.get("redeemed") or info.get("hedge_done"):
            continue
        if info.get("compromised"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        placed_at = info.get("placed_at", 0)
        if not up_token or not dn_token or not placed_at:
            continue
        if info.get("closed"):
            continue  # already resolved, redeem handles it

        time_elapsed = now - placed_at
        time_remaining = max(0, MARKET_PERIOD - time_elapsed)

        # ── No bail before this time gate ──────────────────────────────────
        if time_remaining > BAIL_NO_BAIL_BEFORE_S:
            continue  # > 180s left → only hedge or wait

        # ── Get prices ─────────────────────────────────────────────────────
        up_ask = get_best_ask_safe(ws_feed, client, up_token)
        dn_ask = get_best_ask_safe(ws_feed, client, dn_token)
        if up_ask is None or dn_ask is None:
            continue

        # ── Determine which threshold to use ───────────────────────────────
        if time_remaining > 120:
            bail_threshold = BAIL_THRESHOLD_180
        elif time_remaining > 60:
            bail_threshold = BAIL_THRESHOLD_120
        else:
            bail_threshold = BAIL_THRESHOLD_60

        # ── Check if either side triggers ──────────────────────────────────
        # "unfilled side ask > threshold" means hedging is too expensive.
        bail_candidate = None
        unfilled_ask = None

        # Check balances to determine which side (if any) we hold
        try:
            up_bal = check_token_balance(client, up_token)
            dn_bal = check_token_balance(client, dn_token)
        except Exception:
            continue

        if dn_ask > bail_threshold and dn_bal == 0 and up_bal > 0:
            # DN expensive (unfilled), we hold UP — consider selling UP
            bail_candidate = ("UP", up_token, up_bal)
            unfilled_ask = dn_ask
        elif up_ask > bail_threshold and up_bal == 0 and dn_bal > 0:
            # UP expensive (unfilled), we hold DN — consider selling DN
            bail_candidate = ("DN", dn_token, dn_bal)
            unfilled_ask = up_ask

        if not bail_candidate:
            continue

        label, token, bal = bail_candidate

        # ── Slippage check: bid depth ──────────────────────────────────────
        sell_bid = get_best_bid_safe(ws_feed, client, token)
        if sell_bid is None:
            log.info("  BAIL skip: no bid for %s", label)
            continue

        bid_depth = get_bid_depth_at(client, token, sell_bid - 0.02)
        if bid_depth < BAIL_MIN_BID_DEPTH:
            log.info(
                "  BAIL skip: bid depth %.1f < min %.1f for %s at %.2f",
                bid_depth, BAIL_MIN_BID_DEPTH, label, sell_bid,
            )
            continue

        # ── EV comparison (use actual fill price, not global PRICE) ─────────
        # Determine which side was filled and its actual entry price
        if label == "UP":
            actual_fill_price = info.get("up_price", PRICE)
        else:
            actual_fill_price = info.get("dn_price", PRICE)
        bail_pnl, hold_ev, should_bail = compute_bail_ev(
            actual_fill_price, sell_bid, bal, unfilled_ask, time_remaining,
        )

        log.info(
            "  BAIL eval: %s bid=%.2f, bail_pnl=$%.2f, hold_ev=$%.2f, "
            "should_bail=%s (unfilled_ask=%.2f, %ds left, threshold=%.2f)",
            label, sell_bid, bail_pnl, hold_ev,
            should_bail, unfilled_ask, time_remaining, bail_threshold,
        )
        log_trade(
            "bail_eval", market_ts=ts, side=label,
            sell_bid=sell_bid, bail_pnl=bail_pnl, hold_ev=hold_ev,
            should_bail=should_bail, unfilled_ask=unfilled_ask,
            time_remaining=time_remaining, bid_depth=bid_depth,
        )

        if not should_bail:
            continue

        # ── Execute bail ───────────────────────────────────────────────────
        taker_fee = polymarket_taker_fee(sell_bid, bal)
        log.info(
            "  BAIL EXECUTE: selling %s %.1f @ bid %.2f (fee=$%.3f, "
            "bail_pnl=$%.2f vs hold_ev=$%.2f)",
            label, bal, sell_bid, taker_fee, bail_pnl, hold_ev,
        )
        log_trade(
            "bail_execute", market_ts=ts, side=label, shares=bal,
            sell_bid=sell_bid, fee=taker_fee,
            bail_pnl=bail_pnl, hold_ev=hold_ev,
        )
        cancel_market_orders(client, {up_token, dn_token})
        sell_at_bid(client, token, bal, label)

        info["bailed"] = True
        info["bail_price"] = sell_bid

        # ── Update session stats ───────────────────────────────────────────
        session_stats["bailed"] = session_stats.get("bailed", 0) + 1
        session_stats["consecutive_bails"] = session_stats.get("consecutive_bails", 0) + 1
        session_stats["total_fees"] = session_stats.get("total_fees", 0) + taker_fee
        session_stats["total_cost"] = session_stats.get("total_cost", 0) + (PRICE * bal)
        realized = sell_bid * bal - taker_fee
        session_stats["total_return"] = session_stats.get("total_return", 0) + realized


# ═══════════════════════════════════════════════════════════════════════════════
# STALE CANCEL — cancel unfilled orders near market end
# ═══════════════════════════════════════════════════════════════════════════════

async def cancel_stale_orders(client, past_markets: dict):
    """At T+4:00, cancel unfilled orders when neither side filled."""
    now = int(time.time())
    for ts, info in list(past_markets.items()):
        if info.get("hedge_done") or info.get("redeemed") or info.get("closed"):
            continue
        if info.get("bailed") or info.get("stale_cancelled") or info.get("compromised"):
            continue

        placed_at = info.get("placed_at", 0)
        if not placed_at:
            continue

        time_elapsed = now - placed_at
        if time_elapsed < (MARKET_PERIOD - STALE_CANCEL_BEFORE_END_S):
            continue  # not yet at the cancel window

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        if not up_token or not dn_token:
            continue

        # Only cancel if NEITHER side filled
        if info.get("hedge_side"):
            continue  # one side already filled; hedge/bail handles it

        try:
            up_bal = check_token_balance(client, up_token)
            dn_bal = check_token_balance(client, dn_token)
        except Exception:
            continue

        if up_bal > 0 or dn_bal > 0:
            continue  # has a position — not stale

        log.info("  STALE CANCEL: neither side filled at %ds, cancelling orders", time_elapsed)
        log_trade("stale_cancel", market_ts=ts, elapsed_s=time_elapsed)
        cancel_market_orders(client, {up_token, dn_token})
        info["stale_cancelled"] = True


# ═══════════════════════════════════════════════════════════════════════════════
# ON-CHAIN FUNCTIONS — merge, redeem, token balance (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════════
# REDEMPTION — try_redeem_all, try_merge_all (merge now tracks session P&L)
# ═══════════════════════════════════════════════════════════════════════════════

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


async def try_merge_all(client, redeemer, past_markets: dict, session_stats: dict):
    """Merge complete sets (UP + DN) into USDC only after the market has ended (closed).

    Run before redeem: merge first (get $1 per set back), then redeem any remaining.
    v2: tracks dual_fills, hedged counts, and P&L in session_stats.
    """
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

            # ── Track session P&L ──────────────────────────────────────────
            if info.get("hedge_done") and info.get("hedge_price"):
                # Hedged round: cost = fill + hedge + taker fee on hedge
                hedge_p = info["hedge_price"]
                fee = polymarket_taker_fee(hedge_p, merge_amount)
                cost = PRICE * merge_amount + hedge_p * merge_amount + fee
                session_stats["hedged"] = session_stats.get("hedged", 0)  # already incremented
                session_stats["total_cost"] = session_stats.get("total_cost", 0) + cost
                session_stats["total_return"] = session_stats.get("total_return", 0) + (1.0 * merge_amount)
            else:
                # Natural dual-fill: cost = 2 * fill price, no taker fee
                cost = PRICE * merge_amount * 2
                session_stats["dual_fills"] = session_stats.get("dual_fills", 0) + 1
                session_stats["total_cost"] = session_stats.get("total_cost", 0) + cost
                session_stats["total_return"] = session_stats.get("total_return", 0) + (1.0 * merge_amount)

            # Successful merge resets consecutive bail counter
            session_stats["consecutive_bails"] = 0

    if merged:
        log.info("  Merged %d market(s)", len(merged))


# ═══════════════════════════════════════════════════════════════════════════════
# CONVICTION ADD — increase exposure when BTC spot leads Polymarket
# ═══════════════════════════════════════════════════════════════════════════════

async def check_conviction_add(
    client,
    ws_feed: "WSBookFeed",
    past_markets: dict,
    session_stats: dict,
    vol_cache: dict,
):
    """Increase exposure on a one-sided hold when BTC spot has moved decisively
    in the direction of our held token but Polymarket hasn't caught up yet.

    Why this works: Polymarket's CLOB typically lags BTC spot by 15-45s because
    limit-order LPs don't update quotes in real time.  When BTC has already moved
    ≥ 0.25% from our placement price, the true win probability is higher than the
    current ask implies.  We size into that gap with half-Kelly taker shares.

    Conditions:
      1. One-sided fill (hedge_side set), no hedge/bail/conv_add yet.
      2. time_remaining in [CONV_ADD_MIN_S, CONV_ADD_MAX_S].
      3. BTC moved ≥ CONV_ADD_BTC_MOVE_PCT in direction of held token,
         measured from the BTC price at order placement.
      4. Polymarket ask for held token < BTC-implied probability − CONV_ADD_EDGE_MIN.
      5. Fee-adjusted EV of the add is positive.
      6. Session net P&L not below −CONV_ADD_MAX_LOSS.
    """
    if not CONV_ADD_ENABLED:
        return

    now = int(time.time())
    btc_data = vol_cache.get("data") or {}
    btc_now = btc_data.get("last_price", 0)
    if btc_now <= 0:
        return

    # Session loss gate
    net_pnl = session_stats.get("total_return", 0) - session_stats.get("total_cost", 0)
    if net_pnl < -CONV_ADD_MAX_LOSS:
        return

    for ts, info in list(past_markets.items()):
        # Only for one-sided fills that are still open (no hedge/bail/prior add)
        if not info.get("hedge_side"):
            continue
        if info.get("hedge_done") or info.get("bailed") or info.get("conv_added"):
            continue
        if info.get("redeemed") or info.get("closed") or info.get("compromised"):
            continue

        placed_at = info.get("placed_at", 0)
        if not placed_at:
            continue

        time_elapsed = now - placed_at
        time_remaining = max(0, MARKET_PERIOD - time_elapsed)

        if time_remaining > CONV_ADD_MAX_S or time_remaining < CONV_ADD_MIN_S:
            continue

        # hedge_side = the UNFILLED side; filled side is the opposite
        hedge_side = info["hedge_side"]          # "up" or "dn" (the side we need to complete)
        filled_label = "UP" if hedge_side == "dn" else "DN"
        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        filled_token = dn_token if hedge_side == "up" else up_token

        # ── BTC directional check ───────────────────────────────────────────
        btc_at_place = info.get("btc_at_place", 0)
        if btc_at_place <= 0:
            continue

        btc_move_pct = ((btc_now - btc_at_place) / btc_at_place) * 100

        if filled_label == "UP" and btc_move_pct < CONV_ADD_BTC_MOVE_PCT:
            continue  # BTC hasn't moved enough upward
        if filled_label == "DN" and btc_move_pct > -CONV_ADD_BTC_MOVE_PCT:
            continue  # BTC hasn't moved enough downward

        # ── Polymarket lag check ────────────────────────────────────────────
        # Approximate BTC-implied probability from move magnitude.
        # Linear: 0.25% → ~0.62, 0.50% → ~0.74, 1.0% → ~0.80.  Cap at 0.92.
        abs_move = abs(btc_move_pct)
        btc_implied_prob = min(0.92, 0.50 + min(0.42, abs_move * 0.48))

        filled_ask = get_best_ask_safe(ws_feed, client, filled_token)
        if filled_ask is None or filled_ask >= 0.95:
            continue

        edge = btc_implied_prob - filled_ask
        if edge < CONV_ADD_EDGE_MIN:
            log.debug(
                "  CONV_ADD skip %s: edge=%.3f < min=%.3f "
                "(ask=%.3f impl=%.3f move=%+.3f%%)",
                filled_label, edge, CONV_ADD_EDGE_MIN,
                filled_ask, btc_implied_prob, btc_move_pct,
            )
            continue

        # ── Half-Kelly sizing ───────────────────────────────────────────────
        # f* = (p - ask) / (1 - ask) for a binary win/loss bet.
        # Use half-Kelly to account for model uncertainty.
        kelly_full = edge / max(0.01, 1.0 - filled_ask)
        base = info.get("shares", BASE_SHARES)
        add_shares = max(1, min(CONV_ADD_MAX_SHARES, round(kelly_full * 0.5 * base)))

        # ── EV check ───────────────────────────────────────────────────────
        add_fee = polymarket_taker_fee(filled_ask, add_shares)
        ev = (btc_implied_prob * add_shares) - (filled_ask * add_shares) - add_fee
        if ev <= 0:
            continue

        log.info(
            "  CONV_ADD TRIGGER: %s ask=%.3f btc_impl=%.3f edge=%.3f "
            "btc_move=%+.3f%% add=%d shares EV=+$%.3f (%ds left)",
            filled_label, filled_ask, btc_implied_prob, edge,
            btc_move_pct, add_shares, ev, time_remaining,
        )
        log_trade(
            "conviction_add_trigger", market_ts=ts, side=filled_label,
            ask=round(filled_ask, 4), btc_implied=round(btc_implied_prob, 4),
            edge=round(edge, 4), btc_move_pct=round(btc_move_pct, 3),
            add_shares=add_shares, ev=round(ev, 4),
            time_remaining=time_remaining,
        )

        oid = buy_hedge(client, filled_token, add_shares, f"{filled_label}+conv", filled_ask)
        log_trade(
            "conviction_add_placed", market_ts=ts, side=filled_label,
            ask=round(filled_ask, 4), add_shares=add_shares,
            order_id=oid, ev=round(ev, 4),
        )

        info["conv_added"] = True
        info["conv_add_shares"] = add_shares
        info["conv_add_price"] = filled_ask
        session_stats["conv_adds"] = session_stats.get("conv_adds", 0) + 1
        session_stats["total_cost"] = (
            session_stats.get("total_cost", 0) + filled_ask * add_shares + add_fee
        )
        session_stats["total_fees"] = session_stats.get("total_fees", 0) + add_fee


# ═══════════════════════════════════════════════════════════════════════════════
# BACKGROUND LOOPS — hedge + order integrity
# ═══════════════════════════════════════════════════════════════════════════════

async def hedge_loop(
    client,
    ws_feed: "WSBookFeed",
    past_markets: dict,
    redeemer,
    session_stats: dict,
):
    """Fast 1-second loop for hedge checks + order integrity monitoring.

    Runs as a background task so hedge triggers aren't delayed by the
    main loop sleep. Logs a heartbeat every 60s so the operator can confirm
    the loop is alive even when no hedge fires.
    """
    tick = 0
    while True:
        try:
            await check_hedge_trailing(client, ws_feed, past_markets, redeemer, session_stats)
        except Exception as e:
            log.debug("hedge_loop error: %s", e)
        tick += 1
        if tick % 60 == 0:
            active = sum(
                1 for i in past_markets.values()
                if not i.get("redeemed") and not i.get("closed")
            )
            log.debug("hedge_loop heartbeat: tick=%d active_markets=%d", tick, active)
        await asyncio.sleep(1)


# ═══════════════════════════════════════════════════════════════════════════════
# LIVE DASHBOARD — rendered when `rich` is installed
# ═══════════════════════════════════════════════════════════════════════════════

_EV_FMT = {
    "orders_placed":         ("ORDER",   lambda e: f"UP+DN {e.get('sides_placed',0)} side(s) @ {e.get('up_price') or e.get('dn_price',0):.2f}"),
    "fill_detected":         ("FILL",    lambda e: f"{e.get('filled_side','?')} filled → hold {e.get('hedge_side','').upper()}"),
    "hedge_trigger":         ("HEDGE",   lambda e: f"{e.get('hedge_side','?').upper()} ask={e.get('ask',0):.3f}  EV=+${e.get('ev',0):.3f}  [{e.get('reason','')}]"),
    "hedge_buy":             ("HEDGED",  lambda e: f"{e.get('side','?')} @ {e.get('price',0):.3f} × {e.get('shares',0):.0f}sh"),
    "bail_execute":          ("BAIL",    lambda e: f"{e.get('side','?')} @ {e.get('sell_bid',0):.3f}  pnl=${e.get('bail_pnl',0):.2f}"),
    "merge":                 ("MERGE",   lambda e: f"{e.get('sets',0):.1f} sets  {'✓' if e.get('ok') else '✗ FAILED'}"),
    "redeem":                ("REDEEM",  lambda e: f"UP={e.get('up_bal',0):.1f} DN={e.get('dn_bal',0):.1f}  {'✓' if e.get('ok') else '✗'}"),
    "conviction_add_placed": ("CONV+",   lambda e: f"{e.get('side','?')} +{e.get('add_shares',0)}sh @ {e.get('ask',0):.3f}"),
    "ghost_fill":            ("GHOST!",  lambda e: f"{e.get('filled_side','?')} api={e.get('api_bal',0):.1f} chain=0"),
    "bail_pause":            ("PAUSE",   lambda e: f"{e.get('consecutive',0)} bails → {e.get('pause_s',0)//60}min"),
    "session_loss_skip":     ("LOSS LIM",lambda e: f"net=${e.get('net_pnl',0):.2f}"),
    "stale_cancel":          ("STALE",   lambda e: f"no fill at {e.get('elapsed_s',0)}s"),
    "vol_skip_low":          ("VOL",     lambda e: f"ATR={e.get('atr_pct',0):.3f}% too quiet"),
    "depth_skip":            ("DEPTH",   lambda e: "book too thin"),
    "maker_skip_both":       ("MAKER",   lambda e: "both sides crossed"),
    "window_missed":         ("MISSED",  lambda e: f"discovered {e.get('elapsed_s',0)}s late"),
}
_EV_COLORS = {
    "ORDER": "cyan", "FILL": "yellow", "HEDGE": "bright_yellow",
    "HEDGED": "green", "BAIL": "red", "MERGE": "green", "REDEEM": "green",
    "CONV+": "magenta", "GHOST!": "bright_red bold", "PAUSE": "red",
    "LOSS LIM": "bright_red", "STALE": "dim", "VOL": "dim",
    "DEPTH": "dim", "MAKER": "dim", "MISSED": "dim",
}

def _render_dashboard(session_stats: dict, past_markets: dict) -> "_Layout":
    """Build the rich Layout for the live terminal dashboard."""
    layout = _Layout()
    layout.split_column(
        _Layout(name="header", size=3),
        _Layout(name="body"),
        _Layout(name="footer", size=1),
    )
    layout["body"].split_row(
        _Layout(name="stats", ratio=1),
        _Layout(name="events", ratio=2),
    )

    pnl = session_stats.get("total_return", 0) - session_stats.get("total_cost", 0)
    pnl_style = "bold green" if pnl >= 0 else "bold red"

    # ── Header ────────────────────────────────────────────────────────────────
    hdr = _Text(justify="center")
    hdr.append("POLY-AUTOBETTER  ", style="bold cyan")
    hdr.append("BTC 5-min Up/Down  ", style="dim white")
    hdr.append("Net P&L: ", style="bold white")
    hdr.append(f"${pnl:+.2f}", style=pnl_style)
    layout["header"].update(_Panel(hdr, box=_rbox.HORIZONTALS, border_style="blue"))

    # ── Stats ─────────────────────────────────────────────────────────────────
    tbl = _Table(box=None, show_header=False, padding=(0, 1))
    tbl.add_column("k", style="dim", width=14)
    tbl.add_column("v")

    def _row(k, v, style="bold white"):
        tbl.add_row(k, _Text(str(v), style=style))

    _row("Rounds",     session_stats.get("rounds", 0))
    _row("Net P&L",    f"${pnl:+.2f}", pnl_style)
    _row("Fees",       f"${session_stats.get('total_fees', 0):.3f}", "dim")
    tbl.add_row("", "")
    _row("Dual fills", session_stats.get("dual_fills", 0),  "green")
    _row("Hedged",     session_stats.get("hedged", 0),      "yellow")
    _row("Bailed",     session_stats.get("bailed", 0),      "red")
    _row("Conv adds",  session_stats.get("conv_adds", 0),   "magenta")
    tbl.add_row("", "")
    _row("Skip vol",   session_stats.get("skipped_vol", 0),   "dim")
    _row("Skip depth", session_stats.get("skipped_depth", 0), "dim")
    _row("Skip maker", session_stats.get("skipped_maker", 0), "dim")

    # Recent markets
    now = int(time.time())
    active = [(mts, i) for mts, i in past_markets.items() if not i.get("redeemed")]
    if active:
        tbl.add_row("", "")
        tbl.add_row(_Text("MARKETS", style="dim"), "")
        for mts, info in sorted(active)[-6:]:
            if info.get("hedge_done"):    status, sty = "HEDGED",  "green"
            elif info.get("bailed"):      status, sty = "BAILED",  "red"
            elif info.get("closed"):      status, sty = "CLOSED",  "dim"
            elif info.get("hedge_side"):  status, sty = f"HOLD {info['hedge_side'].upper()}", "yellow"
            elif info.get("placed_at"):   status, sty = "OPEN",    "cyan"
            else:                         status, sty = "–",        "dim"
            elapsed = now - info.get("placed_at", now) if info.get("placed_at") else 0
            t = time.strftime("%H:%M", time.localtime(mts))
            tbl.add_row(f"  {t}", _Text(f"{status} +{elapsed}s", style=f"bold {sty}"))

    layout["stats"].update(_Panel(tbl, title="[bold]SESSION[/bold]", border_style="blue"))

    # ── Events ────────────────────────────────────────────────────────────────
    ev_tbl = _Table(box=_rbox.SIMPLE, show_header=True, padding=(0, 1), expand=True)
    ev_tbl.add_column("Time",   style="dim", width=8)
    ev_tbl.add_column("Event",               width=9)
    ev_tbl.add_column("Detail")

    for ev in list(_dashboard_events):
        event = ev.get("event", "")
        ts = ev.get("ts", 0)
        t = time.strftime("%H:%M:%S", time.localtime(ts)) if ts else "?"
        label, fn = _EV_FMT.get(event, (event, lambda e: ""))
        detail = fn(ev)
        color = _EV_COLORS.get(label, "white")
        ev_tbl.add_row(t, _Text(label, style=f"bold {color}"), detail)

    layout["events"].update(_Panel(ev_tbl, title="[bold]EVENTS[/bold]", border_style="blue"))

    # ── Footer ────────────────────────────────────────────────────────────────
    log_name = os.path.basename(_trade_log.name) if _trade_log else "no log"
    layout["footer"].update(_Text(
        f"  {log_name}   {time.strftime('%H:%M:%S')}   Ctrl+C to stop",
        style="dim",
    ))
    return layout


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════

async def run():
    # Session stats dict must exist before signal_handler closure references it.
    session_stats: dict = {}

    def signal_handler(sig, frame):
        log.info("\nStopped — dumping final session summary...")
        if session_stats:
            net = session_stats.get("total_return", 0) - session_stats.get("total_cost", 0)
            log.info(
                "  ════ FINAL SESSION SUMMARY ════\n"
                "    Rounds: %d | Dual fills: %d | Hedged: %d | Bailed: %d | Conv adds: %d\n"
                "    Skipped (vol/depth/maker): %d/%d/%d\n"
                "    Total cost: $%.2f | Total return: $%.2f\n"
                "    Net P&L: $%.2f | Fees: $%.2f",
                session_stats.get("rounds", 0),
                session_stats.get("dual_fills", 0),
                session_stats.get("hedged", 0),
                session_stats.get("bailed", 0),
                session_stats.get("conv_adds", 0),
                session_stats.get("skipped_vol", 0),
                session_stats.get("skipped_depth", 0),
                session_stats.get("skipped_maker", 0),
                session_stats.get("total_cost", 0),
                session_stats.get("total_return", 0),
                net,
                session_stats.get("total_fees", 0),
            )
            log_trade(
                "session_final",
                rounds=session_stats.get("rounds", 0),
                dual_fills=session_stats.get("dual_fills", 0),
                hedged=session_stats.get("hedged", 0),
                bailed=session_stats.get("bailed", 0),
                conv_adds=session_stats.get("conv_adds", 0),
                net_pnl=net,
                fees=session_stats.get("total_fees", 0),
            )
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    open_trade_log()
    log.info("Initializing SDK...")
    client = init_clob_client()
    redeemer = init_redeemer()

    log.info("SDK ready. Entry price=%.2f, shares=%d per side.", PRICE, BASE_SHARES)
    log.info("  Hedge cap=%.2f, grace=%ds, min edge=$%.2f", INSTANT_HEDGE_MAX, HEDGE_GRACE_S, MIN_HEDGE_EDGE)
    log.info("  Bail thresholds: 180s=%.2f, 120s=%.2f, 60s=%.2f (no bail before %ds)",
             BAIL_THRESHOLD_180, BAIL_THRESHOLD_120, BAIL_THRESHOLD_60, BAIL_NO_BAIL_BEFORE_S)
    log.info("  Session loss limit=$%.0f, consecutive bail pause=%d", SESSION_LOSS_LIMIT, CONSECUTIVE_BAIL_PAUSE)
    log.info("  Vol filter: min=%.2f%% max=%.2f%%, min book depth=%.0f",
             VOL_MIN_ATR_PCT, VOL_MAX_ATR_PCT, MIN_BOOK_DEPTH)
    log.info("  Gas ceiling=%.0f gwei, on-chain verify=%s", GAS_CEILING_GWEI, ONCHAIN_VERIFY)
    log.info(
        "  Conviction add: %s  btc_move≥%.2f%%  edge≥%.2f  max_add=%d shares  window=%d-%ds",
        "ON" if CONV_ADD_ENABLED else "OFF",
        CONV_ADD_BTC_MOVE_PCT, CONV_ADD_EDGE_MIN,
        CONV_ADD_MAX_SHARES, CONV_ADD_MIN_S, CONV_ADD_MAX_S,
    )
    if redeemer:
        log.info("Auto-redeem enabled (on-chain via %s).\n", redeemer[1].address)
    else:
        log.info("Auto-redeem DISABLED (no private key / RPC).\n")

    # Start WebSocket feed for real-time prices
    ws_feed = WSBookFeed()

    placed_markets = set()
    # Track past markets: {ts: {redeemed, bailed, up_token, dn_token, closed, ...}}
    # NOTE: never reassign this dict — hedge_loop holds a reference to it.
    past_markets = {}

    # ── Session statistics ─────────────────────────────────────────────────
    # Use .update() so the signal_handler closure always references the same dict.
    session_stats.update({
        "rounds": 0,
        "dual_fills": 0,
        "hedged": 0,
        "bailed": 0,
        "conv_adds": 0,
        "skipped_vol": 0,
        "skipped_depth": 0,
        "skipped_maker": 0,
        "total_cost": 0.0,
        "total_return": 0.0,
        "total_fees": 0.0,
        "consecutive_bails": 0,
        "paused_until": 0,
        "current_shares": BASE_SHARES,
        "_blacklist": load_blacklist() if BLACKLIST_ENABLED else set(),
    })

    # Start fast hedge loop (1s interval) as a background task
    asyncio.create_task(hedge_loop(client, ws_feed, past_markets, redeemer, session_stats))

    # ── Scan recent markets (last 2 hours) for unredeemed positions ────────
    # Fetch all historical slugs in parallel rather than sequentially.
    now = int(time.time())
    log.info("Scanning recent markets for unredeemed positions...")

    scan_ts_list = []
    _s = (now - 7200) // MARKET_PERIOD * MARKET_PERIOD
    while _s < now:
        scan_ts_list.append(_s)
        _s += MARKET_PERIOD

    async def _fetch_scan(s_ts: int):
        try:
            return s_ts, await get_market_info(f"btc-updown-5m-{s_ts}")
        except Exception:
            return s_ts, None

    scan_results = await asyncio.gather(*[_fetch_scan(s) for s in scan_ts_list])
    for s_ts, mkt in scan_results:
        if mkt is None:
            continue
        slug = f"btc-updown-5m-{s_ts}"
        placed_at = 0 if mkt["closed"] else s_ts
        past_markets[s_ts] = _market_entry(mkt, slug, placed_at=placed_at)
        placed_markets.add(s_ts)
        if not mkt["closed"]:
            if not ws_feed.is_connected:
                await ws_feed.start([mkt["up_token"], mkt["dn_token"]])
            else:
                await ws_feed.subscribe([mkt["up_token"], mkt["dn_token"]])
    log.info("Found %d recent markets to track for redemption.\n", len(past_markets))

    # Pre-fetch cache for next market
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

    # Cache volatility across rounds (refresh every 60s)
    vol_cache = {"data": None, "fetched_at": 0}

    # ── Live dashboard (replaces console output when rich is installed) ────────
    _live = _Live(
        _render_dashboard(session_stats, past_markets),
        refresh_per_second=2,
        screen=True,
    ) if _RICH else None
    if _live:
        _live.start()

    try:
     while True:
        now = int(time.time())
        timestamps = next_market_timestamps(now)
        next_market_ts = (now // MARKET_PERIOD) * MARKET_PERIOD + MARKET_PERIOD

        for ts in timestamps:
            if ts in placed_markets:
                continue

            slug = f"btc-updown-5m-{ts}"

            # Use pre-fetched market data if we have it
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

            # Balance + live orders in parallel
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
                past_markets[ts] = _market_entry(mkt, slug, placed_at=ts)
                continue

            # Skip if we already have live orders on this market
            market_tokens = {mkt["up_token"], mkt["dn_token"]}
            existing = [o for o in (live_orders or [])
                        if o.get("status") == "LIVE"
                        and o.get("asset_id") in market_tokens]
            if existing:
                log.info("  Already have %d live order(s) — skipping", len(existing))
                placed_markets.add(ts)
                past_markets[ts] = _market_entry(mkt, slug, placed_at=ts)
                continue

            # ── Placement window check ─────────────────────────────────────
            secs_elapsed = now - ts
            if secs_elapsed > NEW_MARKET_PLACE_WINDOW_SECONDS:
                log.info(
                    "  Skipping placement — outside %ds window (%ds elapsed)",
                    NEW_MARKET_PLACE_WINDOW_SECONDS, secs_elapsed,
                )
                log_trade("window_missed", market_ts=ts, slug=slug, elapsed_s=secs_elapsed)
                placed_markets.add(ts)
                past_markets[ts] = _market_entry(mkt, slug)
                continue

            # ── PRE-MARKET FAST PATH ────────────────────────────────────────
            # Market hasn't started yet (secs_elapsed < 0). Skip all filters —
            # vol/depth/blacklist conditions at T-120s are irrelevant to a market
            # that starts in 2 minutes. Place both sides at PRICE immediately.
            # If the CLOB rejects pre-market orders, don't mark as placed so we
            # retry every tick until the market goes live.
            if secs_elapsed < 0:
                shares = session_stats["current_shares"]
                log.info(
                    "  PRE-MARKET: placing UP+DN for %s (%ds until start)",
                    slug, -secs_elapsed,
                )
                up_id, dn_id = await asyncio.gather(
                    asyncio.to_thread(place_order, client, mkt["up_token"], "UP  ", shares, PRICE),
                    asyncio.to_thread(place_order, client, mkt["dn_token"], "DOWN", shares, PRICE),
                )
                placed = (1 if up_id else 0) + (1 if dn_id else 0)
                if placed == 0:
                    log.info("  PRE-MARKET: CLOB not accepting yet — retrying next tick")
                    continue  # do NOT mark as placed
                log_trade(
                    "orders_placed", market_ts=ts, slug=slug,
                    title=mkt.get("title", slug),
                    up_price=PRICE, dn_price=PRICE, shares=shares,
                    up_id=up_id, dn_id=dn_id, sides_placed=placed,
                    elapsed_s=secs_elapsed,
                )
                session_stats["rounds"] = session_stats.get("rounds", 0) + 1
                placed_markets.add(ts)
                past_markets[ts] = _market_entry(
                    mkt, slug, placed_at=ts,
                    up_order_id=up_id, dn_order_id=dn_id,
                    up_price=PRICE, dn_price=PRICE, shares=shares,
                    btc_at_place=vol_cache.get("data", {}).get("last_price", 0),
                )
                continue

            # ── Max open markets cap ───────────────────────────────────────
            if MAX_OPEN_MARKETS > 0:
                open_count = sum(1 for _ts, _info in past_markets.items() if not _info.get("redeemed"))
                if open_count >= MAX_OPEN_MARKETS:
                    log.info("  Skipping — at max open markets (%d >= %d)", open_count, MAX_OPEN_MARKETS)
                    continue

            # ── SESSION RISK: loss limit ───────────────────────────────────
            net_pnl = session_stats["total_return"] - session_stats["total_cost"]
            if net_pnl <= -SESSION_LOSS_LIMIT:
                log.warning(
                    "  SESSION LOSS LIMIT: P&L=$%.2f ≤ -$%.0f. Skipping.",
                    net_pnl, SESSION_LOSS_LIMIT,
                )
                log_trade("session_loss_skip", market_ts=ts, net_pnl=net_pnl, limit=SESSION_LOSS_LIMIT)
                placed_markets.add(ts)
                past_markets[ts] = _market_entry(mkt, slug)
                continue

            # ── SESSION RISK: consecutive bail pause ───────────────────────
            if session_stats["consecutive_bails"] >= CONSECUTIVE_BAIL_PAUSE:
                if now < session_stats.get("paused_until", 0):
                    remaining = session_stats["paused_until"] - now
                    log.info(
                        "  BAIL PAUSE: %d consecutive bails, paused for %ds more",
                        session_stats["consecutive_bails"], remaining,
                    )
                    continue
                elif session_stats.get("paused_until", 0) == 0:
                    session_stats["paused_until"] = now + CONSECUTIVE_BAIL_PAUSE_S
                    log.warning(
                        "  BAIL PAUSE: %d consecutive bails — pausing %ds. "
                        "Reducing shares to %d.",
                        session_stats["consecutive_bails"],
                        CONSECUTIVE_BAIL_PAUSE_S,
                        int(BASE_SHARES * BAIL_REDUCE_FACTOR),
                    )
                    session_stats["current_shares"] = max(1, int(BASE_SHARES * BAIL_REDUCE_FACTOR))
                    log_trade(
                        "bail_pause", consecutive=session_stats["consecutive_bails"],
                        pause_s=CONSECUTIVE_BAIL_PAUSE_S,
                        reduced_shares=session_stats["current_shares"],
                    )
                    continue
                else:
                    # Pause expired — resume with reduced shares, reset consecutive count
                    log.info("  BAIL PAUSE expired. Resuming with %d shares.", session_stats["current_shares"])
                    session_stats["paused_until"] = 0
                    session_stats["consecutive_bails"] = 0  # reset to prevent re-pause loop

            shares = session_stats["current_shares"]

            # ── VOLATILITY FILTER ──────────────────────────────────────────
            if now - vol_cache["fetched_at"] > 60 or vol_cache["data"] is None:
                vol_cache["data"] = await get_btc_volatility()
                vol_cache["fetched_at"] = now

            vol = vol_cache["data"]
            if vol.get("ok"):
                atr_pct = vol["atr_pct"]
                if atr_pct < VOL_MIN_ATR_PCT:
                    log.info(
                        "  VOL SKIP: ATR=%.3f%% < min %.2f%% — BTC too quiet, dual fill unlikely",
                        atr_pct, VOL_MIN_ATR_PCT,
                    )
                    log_trade("vol_skip_low", market_ts=ts, atr_pct=atr_pct, threshold=VOL_MIN_ATR_PCT)
                    session_stats["skipped_vol"] = session_stats.get("skipped_vol", 0) + 1
                    placed_markets.add(ts)
                    past_markets[ts] = _market_entry(mkt, slug)
                    continue
                if atr_pct > VOL_MAX_ATR_PCT:
                    reduced = max(1, int(shares * 0.5))
                    log.info(
                        "  VOL HIGH: ATR=%.3f%% > %.2f%% — reducing shares %d→%d",
                        atr_pct, VOL_MAX_ATR_PCT, shares, reduced,
                    )
                    log_trade("vol_reduce", market_ts=ts, atr_pct=atr_pct, shares_before=shares, shares_after=reduced)
                    shares = reduced

            # ── BOOK SNAPSHOT (needed for maker-price adjustment below) ───────
            # Depth check removed: as a maker strategy, thin book just means slower
            # fills — it doesn't hurt us. Bail logic has its own depth check at
            # bail time (BAIL_MIN_BID_DEPTH) which is the only place it matters.
            up_depth_info, dn_depth_info = await asyncio.gather(
                asyncio.to_thread(check_book_depth, client, mkt["up_token"], PRICE, 0),
                asyncio.to_thread(check_book_depth, client, mkt["dn_token"], PRICE, 0),
            )

            # ── BLACKLIST CHECK ────────────────────────────────────────────
            if BLACKLIST_ENABLED:
                bl = session_stats.get("_blacklist", set())
                if bl:
                    # Check if opposing book has blacklisted addresses
                    # (best-effort: check top-of-book maker via REST)
                    skip_bl = False
                    for tid in (mkt["up_token"], mkt["dn_token"]):
                        try:
                            book = client.get_order_book(tid)
                            for ask in (book.asks or [])[:3]:
                                owner = getattr(ask, "owner", None) or getattr(ask, "maker_address", None)
                                if owner and owner in bl:
                                    log.warning(
                                        "  BLACKLIST HIT: %s on book for %s — skipping market",
                                        owner[:16], tid[:12],
                                    )
                                    log_trade("blacklist_skip", market_ts=ts, address=owner)
                                    skip_bl = True
                                    break
                        except Exception:
                            pass
                        if skip_bl:
                            break
                    if skip_bl:
                        placed_markets.add(ts)
                        past_markets[ts] = _market_entry(mkt, slug)
                        continue

            # ── MAKER-ONLY ORDER PLACEMENT ─────────────────────────────────
            # Only post limit if best_ask > our price so it rests as maker (0 fee).
            # If best_ask ≤ our price, adjust down or skip that side.
            up_best_ask = up_depth_info.get("best_ask")
            dn_best_ask = dn_depth_info.get("best_ask")

            up_price = PRICE
            dn_price = PRICE

            if up_best_ask is not None and up_best_ask <= PRICE:
                adjusted = round(up_best_ask - MAKER_MIN_GAP, 2)
                if adjusted >= MAKER_PRICE_FLOOR:
                    up_price = adjusted
                    log.info("  MAKER adj UP: best_ask=%.2f, placing at %.2f", up_best_ask, up_price)
                else:
                    up_price = None
                    log.info("  MAKER skip UP: best_ask=%.2f too low (floor=%.2f)", up_best_ask, MAKER_PRICE_FLOOR)

            if dn_best_ask is not None and dn_best_ask <= PRICE:
                adjusted = round(dn_best_ask - MAKER_MIN_GAP, 2)
                if adjusted >= MAKER_PRICE_FLOOR:
                    dn_price = adjusted
                    log.info("  MAKER adj DN: best_ask=%.2f, placing at %.2f", dn_best_ask, dn_price)
                else:
                    dn_price = None
                    log.info("  MAKER skip DN: best_ask=%.2f too low (floor=%.2f)", dn_best_ask, MAKER_PRICE_FLOOR)

            if up_price is None and dn_price is None:
                log.info("  MAKER SKIP: both sides have best_ask ≤ PRICE — no maker opportunity")
                log_trade("maker_skip_both", market_ts=ts, up_ask=up_best_ask, dn_ask=dn_best_ask)
                session_stats["skipped_maker"] = session_stats.get("skipped_maker", 0) + 1
                placed_markets.add(ts)
                past_markets[ts] = _market_entry(mkt, slug)
                continue

            # ── Place orders (parallel when both sides active) ─────────────
            up_id = None
            dn_id = None

            if up_price is not None and dn_price is not None:
                up_id, dn_id = await asyncio.gather(
                    asyncio.to_thread(place_order, client, mkt["up_token"], "UP  ", shares, up_price),
                    asyncio.to_thread(place_order, client, mkt["dn_token"], "DOWN", shares, dn_price),
                )
            elif up_price is not None:
                up_id = await asyncio.to_thread(place_order, client, mkt["up_token"], "UP  ", shares, up_price)
            elif dn_price is not None:
                dn_id = await asyncio.to_thread(place_order, client, mkt["dn_token"], "DOWN", shares, dn_price)

            placed = (1 if up_id else 0) + (1 if dn_id else 0)
            max_cost = shares * ((up_price or 0) + (dn_price or 0))
            log.info("  Done: %d orders placed. Max cost: $%.2f (shares=%d)", placed, max_cost, shares)
            log.info("")

            if placed == 0:
                # All order attempts failed (e.g. CLOB not yet accepting pre-market orders).
                # Do NOT mark as placed — let the bot retry on the next loop tick.
                log.warning("  Both orders failed for %s — will retry next tick", slug)
                log_trade("orders_failed", market_ts=ts, slug=slug, elapsed_s=now - ts)
                continue

            log_trade(
                "orders_placed", market_ts=ts, slug=slug,
                title=mkt.get("title", slug),
                up_price=up_price, dn_price=dn_price, shares=shares,
                up_id=up_id, dn_id=dn_id, sides_placed=placed,
                elapsed_s=now - ts,
            )

            session_stats["rounds"] = session_stats.get("rounds", 0) + 1

            placed_markets.add(ts)
            past_markets[ts] = _market_entry(
                mkt, slug, placed_at=ts,
                up_order_id=up_id,
                dn_order_id=dn_id,
                up_price=up_price,
                dn_price=dn_price,
                shares=shares,
                # BTC spot price at placement — used by conviction add to measure
                # directional move before attempting to increase exposure.
                btc_at_place=vol_cache.get("data", {}).get("last_price", 0),
            )

        # ── Log WS prices for active markets ───────────────────────────────
        for ts, info in past_markets.items():
            if info.get("redeemed") or info.get("closed"):
                continue
            up_ask = ws_feed.get_best_ask(info.get("up_token", ""))
            dn_ask = ws_feed.get_best_ask(info.get("dn_token", ""))
            if up_ask is not None or dn_ask is not None:
                elapsed = now - info.get("placed_at", now)
                hedge_state = ""
                if info.get("compromised"):
                    hedge_state = " [COMPROMISED]"
                elif info.get("hedge_done"):
                    hedge_state = " [HEDGED]"
                elif info.get("hedge_side"):
                    hedge_state = f" [TRAILING {info['hedge_side'].upper()}]"
                log.debug(
                    "  [%d +%ds] WS: UP=%.2f  DN=%.2f%s",
                    ts, elapsed, up_ask or 0, dn_ask or 0, hedge_state,
                )

        # ── Bail, conviction add, stale cancel, merge, redeem ──────────────
        await check_bail_out(client, ws_feed, past_markets, session_stats)
        await check_conviction_add(client, ws_feed, past_markets, session_stats, vol_cache)
        await cancel_stale_orders(client, past_markets)
        await try_merge_all(client, redeemer, past_markets, session_stats)
        await try_redeem_all(client, redeemer, past_markets)

        # ── Session summary (every 10 rounds) ─────────────────────────────
        if session_stats["rounds"] > 0 and session_stats["rounds"] % 10 == 0:
            net = session_stats["total_return"] - session_stats["total_cost"]
            log.info(
                "  ════ SESSION SUMMARY (after %d rounds) ════\n"
                "    Dual fills: %d | Hedged: %d | Bailed: %d | Conv adds: %d\n"
                "    Skipped (vol/depth/maker): %d/%d/%d\n"
                "    Total cost: $%.2f | Total return: $%.2f\n"
                "    Net P&L: $%.2f | Fees: $%.2f\n"
                "    Current shares: %d | Consecutive bails: %d",
                session_stats["rounds"],
                session_stats.get("dual_fills", 0),
                session_stats.get("hedged", 0),
                session_stats.get("bailed", 0),
                session_stats.get("conv_adds", 0),
                session_stats.get("skipped_vol", 0),
                session_stats.get("skipped_depth", 0),
                session_stats.get("skipped_maker", 0),
                session_stats["total_cost"],
                session_stats["total_return"],
                net,
                session_stats.get("total_fees", 0),
                session_stats["current_shares"],
                session_stats["consecutive_bails"],
            )
            log_trade(
                "session_summary", rounds=session_stats["rounds"],
                dual_fills=session_stats.get("dual_fills", 0),
                hedged=session_stats.get("hedged", 0),
                bailed=session_stats.get("bailed", 0),
                conv_adds=session_stats.get("conv_adds", 0),
                net_pnl=net, fees=session_stats.get("total_fees", 0),
            )

        # ── Sleep / prefetch / dashboard ──────────────────────────────────
        seconds_until = next_market_ts - now
        sleep_s = 0.5 if seconds_until <= 15 else (1 if seconds_until <= 60 else 2)
        if seconds_until <= NEXT_MARKET_PREFETCH_SEC and seconds_until > 0:
            asyncio.create_task(prefetch_next_market(next_market_ts, next_market_cache))
        if _live:
            _live.update(_render_dashboard(session_stats, past_markets))
        await asyncio.sleep(sleep_s)

        log.debug("[Next market in %ds] Checked %d markets", seconds_until, len(timestamps))

        # ── Prune old entries ──────────────────────────────────────────────
        stale = [ts for ts in past_markets if ts <= now - 7200]
        for ts in stale:
            del past_markets[ts]
        placed_markets = {ts for ts in placed_markets if ts > now - 2 * MARKET_PERIOD}

    finally:
        if _live:
            _live.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("\nStopped.")
