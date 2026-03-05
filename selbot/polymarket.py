"""Polymarket API helpers — market discovery, CLOB orders, on-chain merge/redeem.

Handles all interaction with Polymarket's Gamma API (market metadata),
CLOB API (order placement), and Polygon chain (merge/redeem).
"""

import json
import logging
import os
import re
import threading
import time

import httpx

log = logging.getLogger("selbot.polymarket")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GAMMA_URL = "https://gamma-api.polymarket.com"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

_SLUG_RE = re.compile(r"^btc-updown-15m-\d+$")

# ---------------------------------------------------------------------------
# HTTP Client (persistent, browser-like headers for Cloudflare)
# ---------------------------------------------------------------------------
_http_client: httpx.AsyncClient | None = None


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


async def close_http_client():
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None


def _check_json_response(r) -> None:
    """Raise if API returned HTML instead of JSON (Cloudflare block)."""
    ct = r.headers.get("content-type", "")
    body_start = r.text[:100].lstrip()
    if "html" in ct.lower() or body_start.startswith("<!"):
        raise ValueError(
            f"Gamma API returned HTML instead of JSON "
            f"(Cloudflare block?). status={r.status_code} body={r.text[:120]!r}"
        )


# ---------------------------------------------------------------------------
# Market Discovery (Gamma API)
# ---------------------------------------------------------------------------

def _parse_gamma_market(m: dict) -> dict | None:
    """Parse a single Gamma market dict into our internal format."""
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


async def _fallback_market_scan(ts: int, window_s: int) -> dict | None:
    """Scan active markets when Gamma's slug index lags."""
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

        start_date = m.get("eventStartTime") or m.get("startDate") or ""
        end_date = m.get("endDate") or ""
        if not start_date or not end_date:
            continue
        try:
            t_start = datetime.fromisoformat(start_date.replace("Z", "+00:00")).timestamp()
            t_end = datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        duration = t_end - t_start
        if abs(duration - window_s) > 60:
            continue
        if abs(int(t_start) - ts) > 60:
            continue

        result = _parse_gamma_market(m)
        if result:
            log.info("Fallback market discovery: %s", m.get("question", mslug)[:80])
            return result
    return None


async def get_market_info(slug: str, window_s: int = 900) -> dict:
    """Fetch market info from Gamma API. Falls back to active market scan."""
    if not slug or not _SLUG_RE.match(slug):
        raise ValueError(f"Invalid slug: {slug}")

    c = _get_http_client()
    r = await c.get(f"{GAMMA_URL}/events", params={"slug": slug})
    r.raise_for_status()
    _check_json_response(r)
    data = r.json()

    if not data:
        ts = int(slug.rsplit("-", 1)[-1])
        log.info("Slug %s not indexed yet — fallback scan", slug)
        fallback = await _fallback_market_scan(ts, window_s)
        if fallback:
            return fallback
        raise ValueError(f"No event found for slug: {slug}")

    if not isinstance(data, list) or len(data) == 0:
        raise ValueError(f"Invalid API response for {slug}")
    if "markets" not in data[0] or not data[0]["markets"]:
        raise ValueError(f"Empty markets list for {slug}")

    m = data[0]["markets"][0]
    tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
    if not isinstance(tokens, list) or len(tokens) < 2:
        raise ValueError(f"Invalid clobTokenIds for {slug}")

    condition_id = m.get("conditionId", "")

    end_date = m.get("endDate") or ""
    event_start_time = m.get("eventStartTime") or ""
    if event_start_time and end_date:
        from datetime import datetime
        try:
            t_end = datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp()
            t_start = datetime.fromisoformat(event_start_time.replace("Z", "+00:00")).timestamp()
            duration = t_end - t_start
            if abs(duration - window_s) > 60:
                raise ValueError(f"Market duration {duration}s != expected {window_s}s")
        except (ValueError, TypeError) as e:
            if "duration" in str(e):
                raise

    return {
        "up_token": str(tokens[0]),
        "dn_token": str(tokens[1]),
        "title": m.get("question", slug),
        "conditionId": condition_id,
        "closed": bool(m.get("closed", False)),
        "neg_risk": bool(m.get("negRisk", False)),
    }


# ---------------------------------------------------------------------------
# SDK Init
# ---------------------------------------------------------------------------

def init_clob_client():
    """Initialize py-clob-client SDK with derived API creds."""
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
    """Initialize web3 for on-chain merge/redeem. Returns (w3, account) or None."""
    from web3 import Web3
    from eth_account import Account

    pk = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    if not pk:
        log.warning("No POLYMARKET_PRIVATE_KEY — auto-redeem disabled")
        return None

    polygon_rpc = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
    w3 = Web3(Web3.HTTPProvider(polygon_rpc))
    if not w3.is_connected():
        log.warning("Cannot connect to Polygon RPC — auto-redeem disabled")
        return None

    account = Account.from_key(pk)
    return (w3, account)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def place_order(client, token_id: str, side_label: str, shares: float,
                price: float, window_s: int = 900) -> str | None:
    """Place a single limit BUY order. Returns order ID or None."""
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    expiration = int(time.time()) + window_s
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
            time.sleep(1)
    log.error("  %s order failed after 3 attempts", side_label)
    return None


def sell_at_bid(client, token_id: str, shares: float, side_label: str,
                max_attempts: int = 3, check_interval: float = 2.0,
                window_s: int = 900) -> str | None:
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
                expiration=int(time.time()) + window_s,
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

            log.info("  %s SELL still unfilled — re-placing at new bid", side_label)
            try:
                client.cancel(oid)
            except Exception as e:
                log.warning("  %s SELL cancel failed: %s", side_label, e)
                return oid

        except Exception as e:
            log.error("  %s SELL failed (attempt %d/%d): %s",
                      side_label, attempt, max_attempts, e)
            if attempt < max_attempts:
                time.sleep(1)
    return None


def buy_hedge(client, token_id: str, shares: float, side_label: str,
              price: float, window_s: int = 900) -> str | None:
    """Place a limit BUY for the hedge side."""
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.clob_types import OrderArgs, OrderType

    expiration = int(time.time()) + window_s
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
                log.info("  HEDGE %s BUY %.0f @ %.2f  [%s]",
                         side_label, shares, price, oid[:12])
                return oid
            else:
                log.warning("  HEDGE %s no order ID: %s", side_label, result)
        except Exception as e:
            log.warning("  HEDGE %s order failed (attempt %d/3): %s",
                        side_label, attempt, e)
        if attempt < 3:
            time.sleep(1)
    log.error("  HEDGE %s order failed after 3 attempts", side_label)
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


# ---------------------------------------------------------------------------
# On-Chain: Merge & Redeem (Polygon)
# ---------------------------------------------------------------------------

class NonceManager:
    """Thread-safe nonce manager for back-to-back on-chain transactions."""

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
    """Build, sign, send, and wait for an on-chain transaction."""
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
    """Merge complete sets (1 UP + 1 DN) into USDC on-chain."""
    from web3 import Web3

    w3, account = redeemer
    cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)
    amount_wei = int(amount * 1e6)
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


def redeem_market(redeemer, condition_id: str, neg_risk: bool,
                  up_bal: float = 0.0, dn_bal: float = 0.0) -> bool:
    """Redeem resolved positions on-chain."""
    from web3 import Web3

    w3, account = redeemer
    cond_bytes = bytes.fromhex(condition_id[2:] if condition_id.startswith("0x") else condition_id)

    if neg_risk:
        amounts = [int(up_bal * 1e6), int(dn_bal * 1e6)]
        abi = [{"name": "redeemPositions", "type": "function", "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amounts", "type": "uint256[]"},
        ], "outputs": []}]
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(NEG_RISK_ADAPTER), abi=abi
        )
        tx_fn = contract.functions.redeemPositions(cond_bytes, amounts)
    else:
        abi = [{"name": "redeemPositions", "type": "function", "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ], "outputs": []}]
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(CTF_ADDRESS), abi=abi
        )
        tx_fn = contract.functions.redeemPositions(
            Web3.to_checksum_address(USDC_ADDRESS), b"\x00" * 32, cond_bytes, [1, 2]
        )

    return _send_tx(w3, account, tx_fn, 300_000, "Redeem")
