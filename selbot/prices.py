"""Price helpers — REST-only orderbook queries via CLOB API.

No WebSocket dependency. All prices come from get_order_book() which is
reliable on VPS without persistent WS connections.
"""

import logging
import time

log = logging.getLogger("selbot.prices")

_balance_cache: dict[str, tuple[float, float]] = {}
BALANCE_CACHE_TTL = 3.0


def get_best_ask(client, token_id: str) -> float | None:
    """Get best ask price from CLOB REST orderbook."""
    try:
        book = client.get_order_book(token_id)
        if book and book.asks:
            return float(book.asks[0].price)
    except Exception as e:
        log.debug("get_best_ask failed for %s: %s", token_id[:16], e)
    return None


def get_best_bid(client, token_id: str) -> float | None:
    """Get best bid price from CLOB REST orderbook."""
    try:
        book = client.get_order_book(token_id)
        if book and book.bids:
            return float(book.bids[0].price)
    except Exception as e:
        log.debug("get_best_bid failed for %s: %s", token_id[:16], e)
    return None


class BalanceCheckError(Exception):
    """Raised when balance check fails — callers should retry, not assume 0."""


def check_token_balance(client, token_id: str, skip_cache: bool = False) -> float:
    """Check CTF token balance via CLOB API (returns shares).

    Uses a 3-second TTL cache to reduce API spam.
    Raises BalanceCheckError on failure so callers don't confuse
    'API down' with 'no position'.
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


def clear_cache():
    """Clear the balance cache (call on fresh start)."""
    _balance_cache.clear()
