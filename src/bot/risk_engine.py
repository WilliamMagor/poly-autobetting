"""Risk engine: circuit breakers that ACT.

Every check returns a RiskVerdict with a concrete action.
All breakers MUST act (cancel orders, change state) -- not just log.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from src.bot.types import BotConfig, BotState, FillEvent, PositionState

logger = logging.getLogger(__name__)


@dataclass
class RiskLimits:
    """Simple standalone risk limits for the dual-side arb bot.

    Used by RiskEngine.evaluate() which requires no BotConfig/BotState —
    suitable for place_45.py integration without the full bot framework.
    """
    max_vwap_ratio: float = 1.02         # CANCEL_ALL when combined VWAP >= this
    max_vwap_shares: float = 200.0       # ...but only when total_shares >= this
    max_imbalance_ratio: float = 0.15    # CANCEL_ALL when imbalance ratio >= this
    max_imbalance_shares: float = 100.0  # ...but only when total_shares >= this


class RiskAction(Enum):
    """Actions that risk engine can mandate."""
    NONE = "none"
    STOP_ADDING = "stop_adding"
    CANCEL_HEAVY_SIDE = "cancel_heavy_side"
    CANCEL_ALL = "cancel_all"
    KILL_SESSION = "kill_session"
    KILL_DAY = "kill_day"
    PAUSE = "pause"


@dataclass
class RiskVerdict:
    """Result of a risk check."""
    action: RiskAction
    reason: str
    details: dict = field(default_factory=dict)


class RiskEngine:
    """Evaluates position risk and returns actionable verdicts."""

    def __init__(self, config: BotConfig):
        self._config = config
        self._consecutive_api_failures: int = 0
        self._daily_market_count: int = 0

    def check_position(self, position: PositionState, state: BotState) -> RiskVerdict:
        """Check all position-level circuit breakers. Returns highest-priority action.

        Priority order (highest first):
        1. VWAP graduated stops -> CANCEL_ALL
        2. Share imbalance -> CANCEL_ALL
        3. Early imbalance -> CANCEL_ALL
        4. Unhedged exposure -> CANCEL_HEAVY_SIDE
        5. Capital exceeded -> STOP_ADDING
        """
        EPS = 1e-9

        # 1. Graduated VWAP stops
        if position.combined_vwap >= 1.02 - EPS and position.total_shares > 200:
            return RiskVerdict(RiskAction.CANCEL_ALL,
                f"Combined VWAP {position.combined_vwap:.4f} >= 1.02 at {position.total_shares:.0f} shares")
        if position.combined_vwap >= 1.00 - EPS and position.total_shares > 500:
            return RiskVerdict(RiskAction.CANCEL_ALL,
                f"Combined VWAP {position.combined_vwap:.4f} >= 1.00 at {position.total_shares:.0f} shares")

        # 1c. Early VWAP stop — prevent building losing positions (fix #6)
        if (position.combined_vwap >= self._config.vwap_stop_adding_threshold - EPS
                and position.total_shares > 30):
            return RiskVerdict(RiskAction.STOP_ADDING,
                f"Combined VWAP {position.combined_vwap:.4f} approaching 1.0 at {position.total_shares:.0f} shares")

        # 2. Share imbalance (>15% at >100 shares)
        if (position.share_imbalance > self._config.imbalance_threshold - EPS
                and position.total_shares > 100):
            return RiskVerdict(RiskAction.CANCEL_ALL,
                f"Imbalance {position.share_imbalance:.1%} exceeds {self._config.imbalance_threshold:.0%} at {position.total_shares:.0f} shares",
                {"heavy_side": position.excess_side})

        # 3. Early imbalance: one side has 0 when total > 100
        if position.total_shares > 100:
            if position.up_shares == 0 or position.down_shares == 0:
                return RiskVerdict(RiskAction.CANCEL_ALL,
                    f"Early imbalance: up={position.up_shares:.0f} down={position.down_shares:.0f}")

        # 4. Unhedged exposure
        if position.unhedged_exposure > self._config.unhedged_exposure_limit - EPS:
            return RiskVerdict(RiskAction.CANCEL_HEAVY_SIDE,
                f"Unhedged exposure ${position.unhedged_exposure:.2f} exceeds ${self._config.unhedged_exposure_limit}",
                {"heavy_side": position.excess_side})

        # 5. Trade count limit (fix #5)
        if state.total_trades >= self._config.max_trades:
            return RiskVerdict(RiskAction.STOP_ADDING,
                f"Trade count {state.total_trades} reached limit {self._config.max_trades}")

        # 6. Capital exceeded
        if position.total_cost >= self._config.session_capital_limit - EPS:
            return RiskVerdict(RiskAction.STOP_ADDING,
                f"Capital ${position.total_cost:.2f} reached limit ${self._config.session_capital_limit}")

        return RiskVerdict(RiskAction.NONE, "Position healthy")

    def check_entry(self, daily_pnl: float, hourly_pnl: float, daily_markets: int) -> RiskVerdict:
        """Check if we're allowed to enter a new market."""
        if daily_markets >= self._config.max_daily_markets:
            return RiskVerdict(RiskAction.KILL_DAY, f"Daily market count {daily_markets} >= {self._config.max_daily_markets}")
        return RiskVerdict(RiskAction.NONE, "Entry allowed")

    def check_fill_monitor(self, last_poll_age: float) -> RiskVerdict:
        """Check if fill monitor is responsive."""
        if last_poll_age > 30.0:
            return RiskVerdict(RiskAction.CANCEL_ALL,
                f"Fill monitor stale ({last_poll_age:.0f}s since last poll)")
        return RiskVerdict(RiskAction.NONE, "Monitor OK")

    def on_api_failure(self) -> RiskVerdict:
        """Record API failure. 3 consecutive -> kill day."""
        self._consecutive_api_failures += 1
        if self._consecutive_api_failures >= 3:
            return RiskVerdict(RiskAction.KILL_DAY,
                f"{self._consecutive_api_failures} consecutive API failures")
        return RiskVerdict(RiskAction.NONE,
            f"API failure #{self._consecutive_api_failures}")

    def on_api_success(self) -> None:
        """Reset API failure counter on success."""
        self._consecutive_api_failures = 0

    def check_taker_fill(self, fill: FillEvent) -> RiskVerdict:
        """Alert on taker fill but don't stop trading."""
        if fill.trader_side == "TAKER":
            return RiskVerdict(RiskAction.NONE,
                "Taker fill detected -- alert only",
                {"fill": fill.trade_id, "alert": True})
        return RiskVerdict(RiskAction.NONE, "Maker fill OK")

    def increment_daily_markets(self) -> None:
        self._daily_market_count += 1

    def reset_session(self) -> None:
        """Reset per-session counters."""
        self._consecutive_api_failures = 0

    def evaluate(self, pos_state: PositionState, limits: Optional[RiskLimits] = None) -> List[dict]:
        """Check circuit breakers against a PositionState.

        Returns a list of triggered action dicts (empty = all clear).
        Each dict has keys: action (str), reason (str), and metric details.

        Designed for place_45.py integration — requires no BotState/BotConfig,
        just a PositionState and optional RiskLimits (defaults to conservative).
        """
        if limits is None:
            limits = RiskLimits()

        triggered = []

        # Circuit breaker 1: VWAP ratio — buying at combined price > 1.02
        if (pos_state.total_shares >= limits.max_vwap_shares
                and pos_state.combined_vwap >= limits.max_vwap_ratio):
            triggered.append({
                "action": "CANCEL_ALL",
                "reason": "VWAP_RATIO",
                "vwap": round(pos_state.combined_vwap, 4),
                "total_shares": pos_state.total_shares,
                "threshold": limits.max_vwap_ratio,
            })
            logger.warning(
                "RiskEngine: VWAP_RATIO triggered — combined_vwap=%.4f >= %.2f "
                "at %.0f shares",
                pos_state.combined_vwap, limits.max_vwap_ratio, pos_state.total_shares,
            )

        # Circuit breaker 2: Share imbalance — one side too heavy
        if (pos_state.total_shares >= limits.max_imbalance_shares
                and pos_state.share_imbalance >= limits.max_imbalance_ratio):
            triggered.append({
                "action": "CANCEL_ALL",
                "reason": "IMBALANCE",
                "imbalance": round(pos_state.share_imbalance, 4),
                "total_shares": pos_state.total_shares,
                "threshold": limits.max_imbalance_ratio,
            })
            logger.warning(
                "RiskEngine: IMBALANCE triggered — imbalance=%.1f%% >= %.0f%% "
                "at %.0f shares",
                pos_state.share_imbalance * 100,
                limits.max_imbalance_ratio * 100,
                pos_state.total_shares,
            )

        return triggered

    def reset_day(self) -> None:
        """Reset daily counters."""
        self._daily_market_count = 0
        self._consecutive_api_failures = 0
