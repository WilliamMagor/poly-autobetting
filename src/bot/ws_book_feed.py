"""WebSocket order book feed for real-time price updates.

Connects to Polymarket's CLOB WebSocket endpoint for live order book data.
Falls back gracefully — the REST get_order_book() path remains available.

Architecture:
- Single WS connection per bot instance
- Subscribes to token_id channels for book snapshots + price_change deltas
- Maintains local book cache: dict[token_id] -> {bids: [...], asks: [...]}
- Thread-safe via asyncio (single event loop)
- Reconnects with exponential backoff on disconnect
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL_S = 20.0
IDLE_TIMEOUT_S = 60.0
MAX_BACKOFF_S = 60.0


class WSBookFeed:
    """WebSocket-based order book feed with automatic reconnection.

    Usage:
        feed = WSBookFeed()
        await feed.start(["token_id_1", "token_id_2"])
        book = feed.get_book("token_id_1")  # {"bids": [...], "asks": [...]}
        await feed.close()
    """

    def __init__(self, url: str = WS_URL) -> None:
        self._url = url
        self._token_ids: list[str] = []
        self._books: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
        self._last_update_ts: Dict[str, float] = {}
        self._ws: Any = None
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._connected = asyncio.Event()
        self._backoff_s = 1.0
        self._last_msg_ts: float = 0.0

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    @property
    def last_message_age(self) -> float:
        """Seconds since last WS message was received. 0 if never connected."""
        if self._last_msg_ts == 0.0:
            return 0.0
        return time.time() - self._last_msg_ts

    def get_book(self, token_id: str) -> Optional[Dict]:
        """Get cached order book for a token. Returns None if no data."""
        book = self._books.get(token_id)
        if not book:
            return None
        # Check staleness — if no update in 60s, consider stale
        last_ts = self._last_update_ts.get(token_id, 0)
        if time.time() - last_ts > IDLE_TIMEOUT_S:
            logger.debug("WS book stale for %s (%.0fs old)", token_id[:12], time.time() - last_ts)
            return None
        return book

    def get_best_bid(self, token_id: str) -> Optional[float]:
        """Get best bid price from cached book."""
        book = self.get_book(token_id)
        if not book or not book.get("bids"):
            return None
        try:
            return max(float(b["price"]) for b in book["bids"])
        except (KeyError, ValueError):
            return None

    def get_best_ask(self, token_id: str) -> Optional[float]:
        """Get best ask price from cached book."""
        book = self.get_book(token_id)
        if not book or not book.get("asks"):
            return None
        try:
            return min(float(a["price"]) for a in book["asks"])
        except (KeyError, ValueError):
            return None

    async def start(self, token_ids: list[str]) -> None:
        """Start the WS feed for the given token IDs."""
        self._token_ids = list(token_ids)
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        # Wait briefly for initial connection
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("WS feed did not connect within 5s — will retry in background")

    async def subscribe(self, token_ids: list[str]) -> None:
        """Subscribe to additional token IDs on existing connection."""
        new_ids = [t for t in token_ids if t not in self._token_ids]
        if not new_ids:
            return
        self._token_ids.extend(new_ids)
        if self._ws is not None:
            await self._send_subscribe(new_ids)

    async def force_reconnect(self) -> None:
        """Force-close current WS and let _run_loop reconnect automatically."""
        logger.warning("WS force_reconnect requested")
        self._connected.clear()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def close(self) -> None:
        """Stop the WS feed."""
        self._stop_event.set()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._connected.clear()
        self._ws = None

    async def _run_loop(self) -> None:
        """Main loop: connect, subscribe, listen, reconnect on failure."""
        try:
            import websockets
        except ImportError:
            logger.error("websockets package not installed — WS feed disabled")
            return

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    self._url,
                    ping_interval=PING_INTERVAL_S,
                    ping_timeout=PING_INTERVAL_S,
                    close_timeout=5.0,
                    max_size=2**20,  # 1MB
                ) as ws:
                    self._ws = ws
                    self._connected.set()
                    self._backoff_s = 1.0
                    logger.info("WS book feed connected to %s", self._url)

                    # Subscribe to all token IDs
                    if self._token_ids:
                        await self._send_subscribe(self._token_ids)

                    # Listen for messages
                    self._last_msg_ts = time.time()
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        self._last_msg_ts = time.time()
                        try:
                            msg = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode())
                            self._handle_message(msg)
                        except json.JSONDecodeError:
                            logger.debug("WS non-JSON message: %s", raw[:100] if raw else "")
                        except Exception as e:
                            logger.warning("WS message handling error: %s", e)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected.clear()
                self._ws = None
                if self._stop_event.is_set():
                    break
                logger.warning(
                    "WS feed disconnected: %s — reconnecting in %.1fs",
                    e, self._backoff_s,
                )
                await asyncio.sleep(self._backoff_s)
                self._backoff_s = min(self._backoff_s * 2, MAX_BACKOFF_S)

        self._connected.clear()
        self._ws = None

    async def _send_subscribe(self, token_ids: list[str]) -> None:
        """Send subscribe message for token IDs."""
        if self._ws is None:
            return
        msg = {
            "assets_ids": token_ids,
            "type": "market",
        }
        try:
            await self._ws.send(json.dumps(msg))
            logger.info("WS subscribed to %d tokens", len(token_ids))
        except Exception as e:
            logger.warning("WS subscribe failed: %s", e)

    def _handle_message(self, msg: Any) -> None:
        """Process a WS message: book snapshot or price_change delta."""
        if not isinstance(msg, dict):
            # Could be a list of messages
            if isinstance(msg, list):
                for m in msg:
                    self._handle_message(m)
            return

        event_type = msg.get("event_type", "")

        if event_type == "book":
            self._handle_book_snapshot(msg)
        elif event_type == "price_change":
            self._handle_price_change(msg)
        elif event_type == "tick_size_change":
            pass  # Informational, not needed for book cache
        elif "market" in str(msg.get("type", "")):
            # Subscription confirmation
            pass
        else:
            # Unknown event type — could be heartbeat or other
            logger.debug("WS unknown event: %s", event_type or str(msg)[:80])

    def _handle_book_snapshot(self, msg: dict) -> None:
        """Replace entire book for an asset with fresh snapshot."""
        asset_id = msg.get("asset_id", "")
        if not asset_id:
            return

        bids = msg.get("bids", [])
        asks = msg.get("asks", [])

        # Normalize to list of {"price": str, "size": str}
        self._books[asset_id] = {
            "bids": self._normalize_levels(bids),
            "asks": self._normalize_levels(asks),
        }
        self._last_update_ts[asset_id] = time.time()
        logger.debug(
            "WS book snapshot: %s — %d bids, %d asks",
            asset_id[:12], len(bids), len(asks),
        )

    def _handle_price_change(self, msg: dict) -> None:
        """Apply incremental price_change updates to cached book."""
        changes = msg.get("price_changes", msg.get("changes", []))
        if not isinstance(changes, list):
            return

        for change in changes:
            asset_id = change.get("asset_id", "")
            if not asset_id:
                continue

            # Initialize book if not present
            if asset_id not in self._books:
                self._books[asset_id] = {"bids": [], "asks": []}

            book = self._books[asset_id]
            price = change.get("price", "")
            size = change.get("size", "0")
            side = change.get("side", "").upper()

            if not price:
                continue

            if side == "BUY":
                self._update_level(book["bids"], price, size)
            elif side == "SELL":
                self._update_level(book["asks"], price, size)

            self._last_update_ts[asset_id] = time.time()

    @staticmethod
    def _normalize_levels(levels: list) -> list[dict[str, str]]:
        """Normalize book levels to [{"price": str, "size": str}]."""
        result = []
        for lvl in levels:
            if isinstance(lvl, dict):
                result.append({
                    "price": str(lvl.get("price", "")),
                    "size": str(lvl.get("size", "0")),
                })
        return result

    @staticmethod
    def _update_level(levels: list[dict[str, str]], price: str, size: str) -> None:
        """Update or insert a price level. Remove if size is 0."""
        price_str = str(price)
        size_float = float(size) if size else 0.0

        # Find existing level
        for i, lvl in enumerate(levels):
            if lvl["price"] == price_str:
                if size_float <= 0:
                    levels.pop(i)
                else:
                    lvl["size"] = str(size)
                return

        # Not found — insert if size > 0
        if size_float > 0:
            levels.append({"price": price_str, "size": str(size)})
