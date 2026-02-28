"""
Simple script: place 45c limit orders on both UP and DOWN for BTC 5-min markets.
10 shares each side. Rotates to next market every 5 minutes.
Auto-redeems resolved positions via Polymarket's gasless relayer.

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
BAIL_PRICE = 0.72        # if one side > 72c and other side unfilled → bail out
GAMMA_URL = "https://gamma-api.polymarket.com"
MARKET_PERIOD = 300      # 5 minutes (300 seconds)
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"


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


def init_relayer():
    """Initialize builder relayer client for gasless redeems."""
    from py_builder_relayer_client.client import RelayClient
    from py_builder_signing_sdk.config import BuilderConfig, BuilderApiKeyCreds

    bk = os.getenv("POLYMARKET_BUILDER_API_KEY", "")
    bs = os.getenv("POLYMARKET_BUILDER_SECRET", "")
    bp = os.getenv("POLYMARKET_BUILDER_PASSPHRASE", "")
    if not (bk and bs and bp):
        log.warning("No builder creds — auto-redeem disabled")
        return None

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(key=bk, secret=bs, passphrase=bp)
    )
    return RelayClient(
        relayer_url="https://relayer-v2.polymarket.com",
        chain_id=137,
        private_key=os.environ["POLYMARKET_PRIVATE_KEY"],
        builder_config=builder_config,
    )


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


def redeem_market(relayer, condition_id: str, neg_risk: bool) -> bool:
    """Redeem resolved positions via gasless relayer. Returns True on success."""
    from web3 import Web3
    from eth_abi import encode
    from py_builder_relayer_client.models import SafeTransaction, OperationType

    if neg_risk:
        # NegRiskAdapter.redeemPositions(bytes32, uint256[])
        # We don't know exact amounts, but we can pass max uint for both
        # Actually for neg-risk we need the adapter address and different encoding
        NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
        cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)
        # Pass [max, max] — contract will only burn what's available
        max_uint = 2**256 - 1
        selector = Web3.keccak(text="redeemPositions(bytes32,uint256[])")[:4]
        params = encode(["bytes32", "uint256[]"], [cond_bytes, [max_uint, max_uint]])
        target = NEG_RISK_ADAPTER
    else:
        # CTF.redeemPositions(address, bytes32, bytes32, uint256[])
        cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)
        selector = Web3.keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4]
        params = encode(
            ["address", "bytes32", "bytes32", "uint256[]"],
            [USDC_ADDRESS, b"\x00" * 32, cond_bytes, [1, 2]],
        )
        target = CTF_ADDRESS

    calldata = "0x" + (selector + params).hex()
    tx = SafeTransaction(to=target, operation=OperationType.Call, data=calldata, value="0")

    try:
        resp = relayer.execute([tx], "Redeem positions")
        log.info("  Redeem submitted: %s", resp.transaction_id)

        # Poll for result
        for _ in range(20):
            time.sleep(3)
            status = relayer.get_transaction(resp.transaction_id)
            if isinstance(status, list):
                status = status[0]
            state = status.get("state", "")
            if "CONFIRMED" in state:
                log.info("  Redeem CONFIRMED: %s", status.get("transactionHash", "")[:20])
                return True
            if "FAILED" in state or "INVALID" in state:
                log.error("  Redeem FAILED: %s", status.get("errorMsg", "")[:80])
                return False
        log.warning("  Redeem timeout — check manually")
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


async def try_redeem_all(client, relayer, past_markets: dict):
    """Check all past markets and redeem any with unredeemed balances."""
    if not relayer:
        return

    redeemed = []
    for ts, info in list(past_markets.items()):
        if info.get("redeemed"):
            continue

        slug = f"btc-updown-5m-{ts}"
        try:
            mkt = await get_market_info(slug)
        except Exception:
            continue

        if not mkt["closed"]:
            continue  # not resolved yet

        # Check if we hold any tokens
        up_bal = check_token_balance(client,mkt["up_token"])
        dn_bal = check_token_balance(client,mkt["dn_token"])

        if up_bal <= 0 and dn_bal <= 0:
            info["redeemed"] = True  # nothing to redeem
            continue

        log.info("  Redeeming %s (UP=%.1f, DN=%.1f)...", mkt["title"][:50], up_bal, dn_bal)
        ok = redeem_market(relayer, mkt["conditionId"], mkt.get("neg_risk", False))
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
    relayer = init_relayer()
    
    log.info("SDK ready. Placing 45c orders on BTC 5m markets.")
    if relayer:
        log.info("Auto-redeem enabled (gasless relayer).\n")
    else:
        log.info("Auto-redeem DISABLED (no builder creds).\n")

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
                "up_token": mkt["up_token"],
                "dn_token": mkt["dn_token"],
            }
            placed_markets.add(scan_ts)
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
                past_markets[ts] = {"redeemed": False, "up_token": mkt["up_token"], "dn_token": mkt["dn_token"]}
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
                    past_markets[ts] = {"redeemed": False, "up_token": mkt["up_token"], "dn_token": mkt["dn_token"]}
                    continue
            except Exception:
                pass  # if check fails, proceed with placement

            up_id = place_order(client, mkt["up_token"], "UP  ", SHARES_PER_SIDE, PRICE)
            dn_id = place_order(client, mkt["dn_token"], "DOWN", SHARES_PER_SIDE, PRICE)

            placed = (1 if up_id else 0) + (1 if dn_id else 0)
            log.info("  Done: %d orders placed. Max cost: $%.2f", placed, SHARES_PER_SIDE * PRICE * 2)
            log.info("")

            placed_markets.add(ts)
            past_markets[ts] = {"redeemed": False, "up_token": mkt["up_token"], "dn_token": mkt["dn_token"]}

        # Log WS prices for active markets
        for ts, info in past_markets.items():
            if info.get("bailed") or info.get("redeemed"):
                continue
            up_ask = ws_feed.get_best_ask(info.get("up_token", ""))
            dn_ask = ws_feed.get_best_ask(info.get("dn_token", ""))
            if up_ask is not None or dn_ask is not None:
                log.info("  [%d] WS prices: UP ask=%.2f  DN ask=%.2f",
                         ts, up_ask or 0, dn_ask or 0)

        # Bail-out disabled — holding through resolution is +EV at 45c entry
        # await check_bail_out(client, ws_feed, past_markets)

        # Try redeeming resolved markets
        await try_redeem_all(client, relayer, past_markets)

        # Wait and check for next market (shorter interval for more responsive market detection)
        await asyncio.sleep(5)

        # Calculate time until next market
        next_market_ts = (now // MARKET_PERIOD) * MARKET_PERIOD + MARKET_PERIOD
        seconds_until = next_market_ts - now
        
        # Log status with countdown
        log.info("[Next market in %ds] Checked %d markets", seconds_until, len(timestamps))

        # Clean very old entries (keep last 2 hours for redemption)
        past_markets = {ts: v for ts, v in past_markets.items() if ts > now - 7200}
        placed_markets = {ts for ts in placed_markets if ts > now - MARKET_PERIOD}


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("\nStopped.")
