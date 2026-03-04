"""Core mathematical formulas for the Polymarket penny-ladder bot."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import List, Tuple

from src.bot.types import BotConfig, EdgeTier, LadderLevel, PositionState


# ---------------------------------------------------------------------------
# Fee calculation (Polymarket CLOB taker fee model)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FeeParams:
    """Polymarket CLOB taker fee parameters.

    The fee formula is:  fee = C * p * feeRate * (p * (1-p))^exponent
    where C = shares, p = price.

    For crypto markets (BTC up/down): feeRate=0.25, exponent=2.
    Max effective rate is ~1.56% at p=0.50.
    Maker orders pay zero fee.
    """
    fee_rate: float = 0.25   # crypto markets: 25% of the vol component
    exponent: float = 2.0    # squared variance term

    @classmethod
    def for_crypto(cls) -> "FeeParams":
        """Default params for Polymarket crypto markets (BTC up/down)."""
        return cls(fee_rate=0.25, exponent=2.0)


def taker_fee_usdc(shares: float, price: float, fee: FeeParams) -> float:
    """Polymarket taker fee in USDC.

    Formula: fee = C * p * feeRate * (p * (1-p))^exponent
    where C=shares, p=price in (0, 1).

    Returns 0.0 for maker orders (zero maker fee).
    Raises ValueError for invalid inputs.
    """
    if not (0.0 < price < 1.0):
        raise ValueError(f"price must be in (0, 1), got {price}")
    if shares <= 0:
        raise ValueError(f"shares must be positive, got {shares}")
    return shares * price * fee.fee_rate * (price * (1.0 - price)) ** fee.exponent


def effective_fee_rate(price: float, fee: FeeParams) -> float:
    """Effective taker fee as a fraction of notional (fee / (shares * price)).

    = feeRate * (p * (1-p))^exponent
    Useful for understanding the fee drag at a given price level.
    Returns 0.0 for invalid prices.
    """
    if not (0.0 < price < 1.0):
        return 0.0
    return fee.fee_rate * (price * (1.0 - price)) ** fee.exponent


def snap_to_tick(price: float, tick_size: float) -> float:
    """Snap price to nearest valid tick (alias for round_to_tick)."""
    return round_to_tick(price, tick_size)


def estimate_initial_edge(up_ask: float, down_ask: float) -> EdgeTier:
    """Conservative ask-based estimate of market edge.

    Returns edge tier based on combined best asks.
    """
    combined = up_ask + down_ask
    if combined >= 1.00:
        return EdgeTier.NEGATIVE
    edge = 1.0 - combined
    if edge > 0.05:
        return EdgeTier.WIDE
    if edge > 0.02:
        return EdgeTier.MEDIUM
    if edge > 0.005:
        return EdgeTier.TIGHT
    return EdgeTier.NEGATIVE


def estimate_ladder_edge(ladder_up: List[LadderLevel], ladder_down: List[LadderLevel]) -> float:
    """Improved edge estimate: avg of first 5 ladder levels per side.

    Returns edge as float (positive = profitable).
    """
    if not ladder_up or not ladder_down:
        return 0.0
    avg_up = mean(level.price_cents / 100.0 for level in ladder_up[:5])
    avg_down = mean(level.price_cents / 100.0 for level in ladder_down[:5])
    return 1.0 - (avg_up + avg_down)


def identify_outsider(up_ask: float, down_ask: float) -> Tuple[str, float]:
    """Continuous outsider score, clamped to >= 0.

    Returns (side, score) where side is "up" or "down" and score is 0.0-1.0.
    Score of 0.0 means no outsider lean (favorite or 50/50).
    """
    up_score = max(0.0, (0.50 - up_ask) / 0.50)
    down_score = max(0.0, (0.50 - down_ask) / 0.50)
    if up_score > down_score:
        return ("up", up_score)
    return ("down", down_score)


def get_side_bias(outsider_score: float, position: PositionState) -> float:
    """Inventory-driven bias factor.

    Returns float in [0.3, 0.75] where 0.5 = balanced.
    Values > 0.5 bias toward the lagging side.
    """
    imbalance = position.share_imbalance
    if imbalance < 0.05:
        return 0.5
    if imbalance < 0.10:
        return 0.5 + 0.3 * outsider_score  # range [0.5, 0.65] for score in [0, 0.5]
    return 0.75  # force rebalance toward lagging side


def should_stop_adding(position: PositionState, config: BotConfig) -> bool:
    """Check all circuit breaker conditions for stopping new orders.

    v2.3: graduated combined_vwap (data shows negative edge can be profitable).
    Uses epsilon comparison for float thresholds.
    """
    EPS = 1e-9
    return any([
        position.total_cost >= config.session_capital_limit - EPS,
        position.total_shares >= config.max_trades - EPS,  # using total_shares as proxy for trades
        # graduated VWAP stop
        position.combined_vwap >= 1.02 - EPS and position.total_shares > 200,
        position.combined_vwap >= 1.00 - EPS and position.total_shares > 500,
        # primary guard
        position.unhedged_exposure > config.unhedged_exposure_limit - EPS,
    ])


def recalculate_live_edge(position: PositionState, initial_edge: float) -> bool:
    """Check if live edge still holds after 30s of fills.

    Returns True if edge is acceptable, False if collapsed.
    """
    if initial_edge <= 0:
        return False
    live_edge = 1.0 - position.combined_vwap
    return live_edge >= initial_edge * 0.3


def estimate_taker_fee(shares: float, price: float) -> float:
    """Estimate taker fee for 15-min crypto markets.

    Official formula: fee = C * p * feeRate * (p * (1-p))^exponent
    where feeRate=0.25, exponent=2. Max effective rate: 1.56% at p=0.50.
    py-clob-client handles actual fees automatically; this is for P&L display.
    """
    p = price
    return shares * p * 0.25 * (p * (1.0 - p)) ** 2


def clamp_price(price: float, tick_size: float) -> float:
    """Clamp price to valid range [tick_size, 1.0 - tick_size]."""
    return max(tick_size, min(price, 1.0 - tick_size))


def round_to_tick(price: float, tick_size: float) -> float:
    """Round price to nearest tick, then to 2 decimal places."""
    return round(round(price / tick_size) * tick_size, 2)
