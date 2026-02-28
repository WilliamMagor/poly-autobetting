"""
Simple script: place 45c limit orders on both UP and DOWN for BTC 5-min markets.
10 shares each side. Rotates to next market every 5 minutes.
Auto-redeems resolved positions via direct on-chain Polygon transaction.

Usage: python scripts/place_45.py
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


# --- Config ---
PRICE = 0.45
SHARES_PER_SIDE = 10     # Reduced for 5min markets (more frequent rotations)
BAIL_PRICE = 0.72        # (legacy) if one side > 72c and other side unfilled → bail out
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
        log.error("  %s order failed: %s", side_label, e)
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
        log.error("  HEDGE %s order failed: %s", side_label, e)
    return None


async def check_hedge_trailing(client, ws_feed: "WSBookFeed", past_markets: dict):
    """Time-windowed trailing-stop hedge for one-sided fills.

    Ported from Rust bot (main_dual_limit_045_5m_btc.rs).

    Strategy:
      After one limit order fills and the other doesn't, the market has moved
      directionally. Rather than holding a naked directional position to
      resolution, we:
        1. Cancel the unfilled limit order (it's now above fair value).
        2. Track the lowest ask of the unfilled side (trail the floor).
        3. Buy when the price BOUNCES UP by HEDGE_TRAILING_DELTA (confirms floor).
        4. Apply progressive band thresholds so we only buy if profitable:
             120-180s: hedge_ask < HEDGE_BAND_2MIN (0.45)  →  total < 0.90
             180-240s: hedge_ask < HEDGE_BAND_3MIN (0.55)  →  total < 1.00
        5. At 240s+: force-buy if the FILLED side's ask >= 0.85 (the market is
           very confident, so the unfilled side is now very cheap ~0.15).

    Timing anchor: placed_at == ts (market START timestamp), so time_elapsed
    mirrors the Rust bot's (MARKET_PERIOD - time_remaining_seconds) exactly.

    State stored per market in past_markets:
      hedge_side:        "up" | "dn" | None  — which side needs to be hedged
      hedge_cancelled:   bool — unfilled limit was cancelled
      hedge_done:        bool — hedge buy was placed
      trailing_min_ask:  float | None — lowest ask seen for unfilled side
    """
    now = int(time.time())

    for ts, info in list(past_markets.items()):
        # Skip markets that are fully handled
        if info.get("hedge_done") or info.get("redeemed") or info.get("closed"):
            continue

        up_token = info.get("up_token", "")
        dn_token = info.get("dn_token", "")
        placed_at = info.get("placed_at", 0)
        if not up_token or not dn_token or not placed_at:
            continue

        time_elapsed = now - placed_at

        # Don't start hedge logic until 2 minutes into the market
        if time_elapsed < HEDGE_START_AFTER_S:
            continue

        # --- Step 1: Detect one-sided fill (once, then cache result) ---
        if not info.get("hedge_side"):
            try:
                up_bal = check_token_balance(client, up_token)
                dn_bal = check_token_balance(client, dn_token)
            except Exception:
                continue

            if up_bal > 0 and dn_bal == 0:
                hedge_side = "dn"
            elif dn_bal > 0 and up_bal == 0:
                hedge_side = "up"
            else:
                # Both filled (complete set) or neither filled — nothing to do
                continue

            log.info(
                "  HEDGE detected one-sided fill at %ds: %s filled, need to hedge %s",
                time_elapsed,
                "UP" if hedge_side == "dn" else "DN",
                hedge_side.upper(),
            )
            info["hedge_side"] = hedge_side
            info["trailing_min_ask"] = None

        hedge_side: str = info["hedge_side"]
        hedge_token = up_token if hedge_side == "up" else dn_token
        filled_token = dn_token if hedge_side == "up" else up_token

        # --- Step 2: Cancel unfilled limit (once) ---
        if not info.get("hedge_cancelled"):
            log.info("  HEDGE cancelling unfilled %s limit", hedge_side.upper())
            cancel_market_orders(client, {hedge_token})
            info["hedge_cancelled"] = True

        # --- Step 3: Get current ask for the hedge side from WS ---
        hedge_ask = ws_feed.get_best_ask(hedge_token)
        if hedge_ask is None:
            continue

        # Update trailing minimum
        prev_min = info.get("trailing_min_ask")
        if prev_min is None or hedge_ask < prev_min:
            info["trailing_min_ask"] = hedge_ask
            if prev_min is not None:
                log.info("  HEDGE trail: new low for %s ask=%.3f", hedge_side.upper(), hedge_ask)

        trailing_min = info["trailing_min_ask"]

        # --- Step 4: Time-windowed band threshold ---
        if time_elapsed >= HEDGE_4MIN_AFTER_S:
            band = HEDGE_BAND_3MIN
            # Force-buy check: if the filled side is priced very high (0.85+),
            # the hedge side is very cheap. Buy at market immediately.
            filled_ask = ws_feed.get_best_ask(filled_token)
            if filled_ask is not None and filled_ask >= HEDGE_FORCE_BUY_FILLED_THRESHOLD:
                log.info(
                    "  HEDGE FORCE-BUY: filled side ask=%.2f >= %.2f, hedge %s ask=%.3f",
                    filled_ask, HEDGE_FORCE_BUY_FILLED_THRESHOLD,
                    hedge_side.upper(), hedge_ask,
                )
                buy_hedge(client, hedge_token, SHARES_PER_SIDE, hedge_side.upper(), hedge_ask)
                info["hedge_done"] = True
                continue
        elif time_elapsed >= HEDGE_3MIN_AFTER_S:
            band = HEDGE_BAND_3MIN
        else:
            band = HEDGE_BAND_2MIN

        # --- Step 5: Only buy if still profitable at current hedge price ---
        if hedge_ask >= band:
            log.info(
                "  HEDGE wait: %s ask=%.3f >= band=%.2f (elapsed=%ds)",
                hedge_side.upper(), hedge_ask, band, time_elapsed,
            )
            continue

        # --- Step 6: Trailing-stop trigger — buy on bounce from floor ---
        bounce = hedge_ask - trailing_min
        log.info(
            "  HEDGE trail: %s ask=%.3f low=%.3f bounce=%.3f/%.3f (elapsed=%ds band=%.2f)",
            hedge_side.upper(), hedge_ask, trailing_min,
            bounce, HEDGE_TRAILING_DELTA, time_elapsed, band,
        )
        if bounce >= HEDGE_TRAILING_DELTA:
            log.info(
                "  HEDGE TRIGGER: %s bounced %.3f -> %.3f (delta=%.3f >= %.3f)",
                hedge_side.upper(), trailing_min, hedge_ask, bounce, HEDGE_TRAILING_DELTA,
            )
            buy_hedge(client, hedge_token, SHARES_PER_SIDE, hedge_side.upper(), hedge_ask)
            info["hedge_done"] = True


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

        # Get prices from WS feed (real-time, no API call)
        up_ask = ws_feed.get_best_ask(up_token)
        dn_ask = ws_feed.get_best_ask(dn_token)
        if up_ask is None or dn_ask is None:
            continue  # no WS data yet

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
        if ok:
            info["redeemed"] = True
            redeemed.append(ts)

    if redeemed:
        log.info("  Redeemed %d market(s)", len(redeemed))


async def run():
    # Set up signal handler for graceful shutdown
    import signal
    def signal_handler(sig, frame):
        log.info("\nStopped.")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
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
    past_markets = {}

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
                # placed_at unknown for startup-scanned markets; hedge won't activate
                "placed_at": 0,
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

    while True:
        now = int(time.time())
        timestamps = next_market_timestamps(now)

        for ts in timestamps:
            if ts in placed_markets:
                continue

            slug = f"btc-updown-5m-{ts}"

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

            # Skip if we already hold tokens (filled from previous run or restart)
            up_bal = check_token_balance(client, mkt["up_token"])
            dn_bal = check_token_balance(client, mkt["dn_token"])
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
                    # placed_at unknown (pre-existing position); hedge won't activate
                    "placed_at": 0,
                }
                continue

            # Skip if we already have live orders on this market
            try:
                live_orders = client.get_orders()
                market_tokens = {mkt["up_token"], mkt["dn_token"]}
                existing = [o for o in live_orders
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
                        # placed_at unknown (pre-existing orders); hedge won't activate
                        "placed_at": 0,
                    }
                    continue
            except Exception:
                pass  # if check fails, proceed with placement

            # Enforce 15-second placement window (matching Rust bot).
            # ts is the market START timestamp; secs_elapsed is negative for
            # future markets (we still pre-register them) and 0-300 for live ones.
            secs_elapsed = now - ts
            if secs_elapsed > NEW_MARKET_PLACE_WINDOW_SECONDS:
                log.info(
                    "  Skipping placement — outside %ds window (%ds elapsed since market start)",
                    NEW_MARKET_PLACE_WINDOW_SECONDS, secs_elapsed,
                )
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

            up_id = place_order(client, mkt["up_token"], "UP  ", SHARES_PER_SIDE, PRICE)
            dn_id = place_order(client, mkt["dn_token"], "DOWN", SHARES_PER_SIDE, PRICE)

            placed = (1 if up_id else 0) + (1 if dn_id else 0)
            log.info("  Done: %d orders placed. Max cost: $%.2f", placed, SHARES_PER_SIDE * PRICE * 2)
            log.info("")

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

        # Run time-windowed trailing hedge for one-sided fills
        await check_hedge_trailing(client, ws_feed, past_markets)

        # Try redeeming resolved markets
        await try_redeem_all(client, redeemer, past_markets)

        # Wait and check for next market
        await asyncio.sleep(5)

        # Calculate time until next market
        next_market_ts = (now // MARKET_PERIOD) * MARKET_PERIOD + MARKET_PERIOD
        seconds_until = next_market_ts - now

        # Log status with countdown
        log.info("[Next market in %ds] Checked %d markets", seconds_until, len(timestamps))

        # Clean very old entries (keep last 2 hours for redemption)
        past_markets = {ts: v for ts, v in past_markets.items() if ts > now - 7200}
        # FIX: placed_markets must survive long enough to cover the previous period
        # (next_market_timestamps always returns ts-300 which is ~5 min old).
        # Prune at 2×MARKET_PERIOD so the previous period is never re-placed.
        placed_markets = {ts for ts in placed_markets if ts > now - 2 * MARKET_PERIOD}


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("\nStopped.")
