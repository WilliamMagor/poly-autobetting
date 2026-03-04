"""Fill monitor: detects fills by polling order status (size_matched deltas).

Strategy: we placed the orders, so we know all order_ids.
Poll GET /data/orders?market={condition_id} → one request returns ALL our orders.
Compare size_matched with last known value → delta = new fill.
Orders that disappear → query GET /data/order/{id} for final status.

No maker_address filtering needed. No get_trades parsing.
Single-writer pattern: publishes FillEvents to queue, runner handles state.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
from typing import Dict, Optional, Set

from src.bot.types import FillEvent
from src.bot.client import ExchangeClient

logger = logging.getLogger(__name__)


class FillMonitor:
    """Detects fills by tracking order size_matched deltas."""

    def __init__(
        self,
        client: ExchangeClient,
        fill_queue: asyncio.Queue,
        market: str,
        up_token: str,
        down_token: str,
    ):
        self._client = client
        self._queue = fill_queue
        self._market = market  # condition_id
        self._up_token = up_token
        self._down_token = down_token
        self._last_matched: Dict[str, float] = {}  # order_id -> last size_matched
        # Monotonic high-water mark per order to survive stale/non-monotonic API snapshots.
        self._max_matched: Dict[str, float] = {}
        self._registered_ids: Set[str] = set()  # only track these orders
        # Last SUCCESSFUL poll timestamp (used for stale detector).
        self._last_success_ts: float = time.time()
        self._last_attempt_ts: float = self._last_success_ts
        # How many consecutive polls we could not resolve a disappeared order.
        self._detail_miss_count: Dict[str, int] = {}
        self._total_fills: int = 0
        self._taker_order_ids: Set[str] = set()
        self._taker_meta: Dict[str, dict] = {}  # oid -> {price, size, token_id, registered_ts}
        self._taker_last_retry_ts: Dict[str, float] = {}  # oid -> last get_order attempt
        self._terminal_statuses: Set[str] = {
            "MATCHED",
            "FILLED",
            "EXECUTED",
            "CANCELLED",
            "CANCELED",
            "EXPIRED",
            "REJECTED",
            "FAILED",
        }

    def _token_to_side(self, asset_id: str) -> str:
        """Map asset_id (token) to 'up' or 'down'."""
        if asset_id == self._up_token:
            return "up"
        elif asset_id == self._down_token:
            return "down"
        return "unknown"

    async def poll_fills(self) -> int:
        """Poll order statuses. Detect fills via size_matched deltas.

        Returns count of new fills detected.
        """
        try:
            orders = await self._client.get_open_orders(self._market)
        except Exception as e:
            logger.error("FillMonitor poll failed: %s", e)
            self._last_attempt_ts = time.time()
            return 0

        new_count = 0
        seen_ids: Set[str] = set()
        now_ts = int(time.time())

        for o in orders:
            oid = o.get("id", o.get("order_id", ""))
            if not oid:
                continue
            if oid not in self._registered_ids:
                continue  # ignore orders not placed by this session
            seen_ids.add(oid)
            self._detail_miss_count.pop(oid, None)

            reported_matched = float(o.get("size_matched", 0))
            old_matched = max(
                self._last_matched.get(oid, 0.0),
                self._max_matched.get(oid, 0.0),
            )
            if reported_matched + 0.001 < old_matched:
                logger.debug(
                    "Non-monotonic size_matched for %s: %.4f -> %.4f (ignoring regression)",
                    oid[:12], old_matched, reported_matched,
                )
            size_matched = max(reported_matched, old_matched)
            delta = size_matched - old_matched

            if delta > 0.001:  # meaningful fill
                price = float(o.get("price", 0))
                side = self._token_to_side(o.get("asset_id", ""))
                trader_side = "TAKER" if oid in self._taker_order_ids else "MAKER"
                fill = FillEvent(
                    trade_id=f"om-{oid}-{int(size_matched*10000)}",
                    order_id=oid,
                    side=side,
                    price=price,
                    filled_shares=delta,
                    usdc_cost=delta * price,
                    trader_side=trader_side,
                    timestamp=now_ts,
                )
                await self._queue.put(fill)
                new_count += 1
                self._total_fills += 1
                logger.info(
                    "Fill detected: %s %s %.2f @ %.2f (matched %.2f/%.2f)",
                    side, oid[:12], delta, price,
                    size_matched, float(o.get("original_size", 0)),
                )

            self._last_matched[oid] = size_matched
            self._max_matched[oid] = size_matched

        # Check orders that disappeared (fully filled or cancelled)
        disappeared = set(self._last_matched.keys()) - seen_ids
        for oid in list(disappeared):
            old_matched = max(
                self._last_matched.get(oid, 0.0),
                self._max_matched.get(oid, 0.0),
            )
            should_drop = False
            detail_observed = False

            # Taker orders: throttle get_order to every 5s (not every poll)
            if oid in self._taker_order_ids:
                last_retry = self._taker_last_retry_ts.get(oid, 0)
                if time.time() - last_retry < 5.0:
                    continue  # skip this poll, retry later
                self._taker_last_retry_ts[oid] = time.time()

            try:
                detail = await self._client.get_order(oid)
                if detail is not None and not isinstance(detail, dict):
                    detail = {}
                if detail:
                    detail_observed = True
                    reported_final = float(detail.get("size_matched", 0))
                    if reported_final + 0.001 < old_matched:
                        logger.debug(
                            "Non-monotonic final size_matched for %s: %.4f -> %.4f (clamping)",
                            oid[:12], old_matched, reported_final,
                        )
                    final_matched = max(reported_final, old_matched)
                    delta = final_matched - old_matched
                    status = str(detail.get("status", "UNKNOWN")).upper()
                    if delta > 0.001:
                        price = float(detail.get("price", 0))
                        side = self._token_to_side(detail.get("asset_id", ""))
                        trader_side = "TAKER" if oid in self._taker_order_ids else "MAKER"
                        fill = FillEvent(
                            trade_id=f"om-{oid}-final-{int(final_matched*10000)}",
                            order_id=oid,
                            side=side,
                            price=price,
                            filled_shares=delta,
                            usdc_cost=delta * price,
                            trader_side=trader_side,
                            timestamp=now_ts,
                        )
                        await self._queue.put(fill)
                        new_count += 1
                        self._total_fills += 1
                        logger.info(
                            "Final fill: %s %s %.2f @ %.2f (status=%s)",
                            side, oid[:12], delta, price, status,
                        )
                    else:
                        logger.debug("Order %s disappeared (status=%s, no new fill)", oid[:12], status)
                    self._max_matched[oid] = final_matched
                    self._last_matched[oid] = final_matched
                    # Drop from tracking only once exchange confirms terminal state.
                    # Non-terminal responses can happen during API lag/re-anchor churn.
                    should_drop = status in self._terminal_statuses
            except Exception as e:
                # Taker orders: API indexing can take 30s+.  Keep retrying —
                # get_order will eventually return real size_matched.
                # Synthetic fill only as last resort after 60s.
                if oid in self._taker_order_ids:
                    tmeta = self._taker_meta.get(oid)
                    age = time.time() - tmeta["registered_ts"] if tmeta else 999
                    if age >= 60.0 and tmeta:
                        delta = tmeta["size"] - old_matched
                        if delta > 0.001:
                            price = tmeta["price"]
                            side = self._token_to_side(tmeta["token_id"])
                            fill = FillEvent(
                                trade_id=f"om-{oid}-taker-synth",
                                order_id=oid,
                                side=side,
                                price=price,
                                filled_shares=delta,
                                usdc_cost=delta * price,
                                trader_side="TAKER",
                                timestamp=now_ts,
                            )
                            await self._queue.put(fill)
                            new_count += 1
                            self._total_fills += 1
                            logger.info(
                                "Taker synthetic fill: %s %s %.2f @ %.2f (%.0fs, get_order failed)",
                                side, oid[:12], delta, price, age,
                            )
                        should_drop = True
                        self._taker_meta.pop(oid, None)
                    else:
                        logger.debug("Taker order %s: retrying get_order (%.0fs): %s", oid[:12], age, e)
                else:
                    logger.warning("Failed to check disappeared order %s: %s", oid[:12], e)
            if should_drop:
                self._last_matched.pop(oid, None)
                self._max_matched.pop(oid, None)
                self._registered_ids.discard(oid)
                self._detail_miss_count.pop(oid, None)
                self._taker_last_retry_ts.pop(oid, None)
            elif detail_observed:
                # We got an explicit non-terminal detail response; keep tracking without
                # incrementing miss counters (this is not a lookup miss).
                self._detail_miss_count.pop(oid, None)
            else:
                misses = self._detail_miss_count.get(oid, 0) + 1
                self._detail_miss_count[oid] = misses
                # Taker orders: skip 6-miss drop — they retry until 60s time limit
                if oid in self._taker_order_ids:
                    pass  # time-based handler above manages taker lifecycle
                elif misses >= 6:
                    logger.warning(
                        "Dropping unresolved order %s after %d detail misses (keeping max_matched=%.4f)",
                        oid[:12], misses, self._max_matched.get(oid, 0.0),
                    )
                    self._last_matched.pop(oid, None)
                    # Fix #57a: Keep _max_matched as high-water mark.
                    # If this order reappears in a later poll, the delta
                    # will be calculated from the last known fill level,
                    # not from 0 (which would double-count).
                    # self._max_matched.pop(oid, None)  — intentionally NOT cleared
                    self._detail_miss_count.pop(oid, None)
                    # Keep in _registered_ids so reappearance is tracked

        now = time.time()
        self._last_attempt_ts = now
        self._last_success_ts = now
        if new_count > 0:
            logger.info("Poll: %d new fills (total: %d)", new_count, self._total_fills)
        return new_count

    def register_order(self, order_id: str) -> None:
        """Register a newly placed order so we track it from size_matched=0.

        This ensures that even if the order fills and disappears before the
        next poll, the disappeared-order logic will catch it via get_order().
        Only registered orders are tracked — prevents phantom fills from
        previous sessions or external wallet activity.
        """
        if order_id:
            self._registered_ids.add(order_id)
            if order_id not in self._last_matched:
                self._last_matched[order_id] = 0.0
            if order_id not in self._max_matched:
                self._max_matched[order_id] = self._last_matched[order_id]

    def register_taker_order(
        self, order_id: str,
        price: float = 0, size: float = 0, token_id: str = "",
    ) -> None:
        """Mark an order as taker-placed (no post_only).

        Stores metadata so we can create synthetic fills when get_order fails
        (taker orders often fill instantly and aren't queryable via the CLOB API).
        """
        self._taker_order_ids.add(order_id)
        if price > 0 and size > 0:
            self._taker_meta[order_id] = {
                "price": price, "size": size, "token_id": token_id,
                "registered_ts": time.time(),
            }
        self.register_order(order_id)

    def clear_tracking(self, order_ids: Optional[set] = None) -> None:
        """Clear tracked orders (call after re-anchor to avoid stale lookups)."""
        if order_ids is None:
            self._last_matched.clear()
            self._max_matched.clear()
            self._registered_ids.clear()
            self._taker_meta.clear()
            self._taker_last_retry_ts.clear()
        else:
            for oid in order_ids:
                self._last_matched.pop(oid, None)
                self._max_matched.pop(oid, None)
                self._registered_ids.discard(oid)
                self._taker_meta.pop(oid, None)
                self._taker_last_retry_ts.pop(oid, None)

    async def run(self, interval: float = 5.0, stop_event: Optional[asyncio.Event] = None) -> None:
        """Continuously poll for fills until stop_event is set."""
        logger.info("Fill monitor started (interval=%.1fs, market=%s)", interval, self._market[:16])
        while True:
            if stop_event and stop_event.is_set():
                logger.info("Fill monitor stopping (stop event)")
                break
            try:
                await self.poll_fills()
            except Exception as e:
                logger.error("Fill monitor poll error: %s", e)

            # Sleep in small increments so we respond to stop quickly
            for _ in range(int(interval * 10)):
                if stop_event and stop_event.is_set():
                    break
                await asyncio.sleep(0.1)

    @property
    def last_poll_age(self) -> float:
        """Seconds since last successful poll."""
        return time.time() - self._last_success_ts

    @property
    def is_stale(self) -> bool:
        """True if last poll was more than 30 seconds ago."""
        return self.last_poll_age > 30.0

    @property
    def total_fills(self) -> int:
        return self._total_fills


class OrderReconciler:
    """Secondary check: detect orders cancelled by exchange (not fills)."""

    def __init__(self, client: ExchangeClient):
        self._client = client
        self._cancelled_statuses = {
            "CANCELLED",
            "CANCELED",
            "EXPIRED",
            "REJECTED",
            "FAILED",
        }
        self._filled_statuses = {
            "MATCHED",
            "FILLED",
            "EXECUTED",
        }

    async def reconcile(
        self,
        market: str,
        order_map: Dict[str, dict],
        recent_fill_order_ids: Set[str],
    ) -> list[str]:
        """Find orders in our map that disappeared from exchange.

        Returns list of order_ids that were cancelled by exchange
        (in our map, not on exchange, not recently filled).
        """
        try:
            exchange_orders = await self._client.get_open_orders(market)
            exchange_ids = {o.get("id", o.get("order_id", "")) for o in exchange_orders}
        except Exception as e:
            logger.error("Reconciliation failed: %s", e)
            return []

        missing = []
        for order_id in order_map:
            if order_id in exchange_ids or order_id in recent_fill_order_ids:
                continue

            # Confirm terminal status before releasing local state.
            # Without this check, "temporarily missing from open orders" can be
            # misclassified as exchange-cancelled even when it actually matched.
            try:
                detail = await self._client.get_order(order_id)
            except Exception as e:
                logger.warning("Reconciliation get_order failed for %s: %s", order_id[:12], e)
                continue

            status = str((detail or {}).get("status", "")).upper()
            if status in self._cancelled_statuses:
                missing.append(order_id)
            elif status in self._filled_statuses:
                logger.debug("Reconciliation: %s resolved as filled (%s)", order_id[:12], status)
            else:
                # Unknown/unset status: be conservative and keep local tracking
                # until a later poll confirms terminal cancellation.
                logger.debug(
                    "Reconciliation: %s unresolved (status=%s), keeping local state",
                    order_id[:12],
                    status or "UNKNOWN",
                )

        if missing:
            logger.warning("Reconciliation: %d orders disappeared (exchange-cancelled)", len(missing))
        return missing


class FillDeduplicator:
    """Crash-safe fill deduplication using a persisted seen_trade_ids set.

    Suitable for integration into place_45.py as a standalone module —
    no ExchangeClient dependency.  Atomically persists seen trade IDs to
    disk so a crash-restart doesn't double-count fills.

    Usage:
        dedup = FillDeduplicator("logs/seen_fills.json")
        if dedup.process("trade-abc-123"):
            # new fill — update position
        dedup.save()  # call periodically or on shutdown
    """

    def __init__(self, persist_path: Optional[str] = None) -> None:
        self._seen: Set[str] = set()
        self._path = persist_path
        if persist_path:
            self._load()

    def _load(self) -> None:
        """Load persisted trade IDs from disk (called at startup)."""
        if not self._path:
            return
        try:
            with open(self._path) as f:
                data = json.load(f)
            self._seen = set(data.get("seen_trade_ids", []))
            logger.info(
                "FillDeduplicator: loaded %d trade IDs from %s",
                len(self._seen), self._path,
            )
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning("FillDeduplicator: failed to load %s: %s", self._path, e)

    def save(self) -> None:
        """Persist seen trade IDs to disk (atomic write via temp file)."""
        if not self._path:
            return
        data = {"seen_trade_ids": list(self._seen)}
        dir_ = os.path.dirname(os.path.abspath(self._path))
        try:
            with tempfile.NamedTemporaryFile("w", dir=dir_, delete=False, suffix=".tmp") as f:
                json.dump(data, f)
                tmp = f.name
            os.replace(tmp, self._path)
        except Exception as e:
            logger.warning("FillDeduplicator: failed to save %s: %s", self._path, e)

    def process(self, trade_id: str) -> bool:
        """Check and mark a trade_id atomically.

        Returns True if trade_id is NEW (should be processed).
        Returns False if already seen (duplicate — skip).
        """
        if trade_id in self._seen:
            logger.debug("FillDeduplicator: duplicate trade_id=%s (skipping)", trade_id)
            return False
        self._seen.add(trade_id)
        return True

    def is_seen(self, trade_id: str) -> bool:
        """Return True if trade_id was already processed."""
        return trade_id in self._seen

    def clear(self) -> None:
        """Clear all seen trade IDs (call at session start if needed)."""
        self._seen.clear()

    @property
    def count(self) -> int:
        """Number of unique trade IDs seen."""
        return len(self._seen)
