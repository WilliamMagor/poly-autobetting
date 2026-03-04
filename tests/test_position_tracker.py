"""Unit tests for PositionTracker and PositionState."""
import sys
import os

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.bot.types import PositionState, FillEvent
from src.bot.position_tracker import PositionTracker


# ---------------------------------------------------------------------------
# PositionState properties
# ---------------------------------------------------------------------------

def test_empty_position():
    pos = PositionState()
    assert pos.up_shares == 0.0
    assert pos.down_shares == 0.0
    assert pos.total_shares == 0.0
    assert pos.total_cost == 0.0
    assert pos.is_dual is False
    assert pos.imbalance_ratio == 0.0
    assert pos.vwap_cost_ratio == 0.0
    assert pos.worst_case_exposure_usdc == 0.0


def test_balanced_position():
    pos = PositionState(up_shares=10.0, down_shares=10.0, up_cost=4.6, down_cost=4.6)
    assert pos.total_shares == 20.0
    assert pos.is_dual is True
    assert pos.imbalance_ratio == 0.0
    assert abs(pos.vwap_cost_ratio - 0.92) < 1e-9   # 0.46 + 0.46
    assert abs(pos.up_avg_price - 0.46) < 1e-9
    assert abs(pos.dn_avg_price - 0.46) < 1e-9
    # worst_case_exposure: cost - min(up, dn) = 9.2 - 10 = -0.8 (profitable!)
    assert abs(pos.worst_case_exposure_usdc - (9.2 - 10.0)) < 1e-9


def test_one_sided_up_only():
    pos = PositionState(up_shares=10.0, down_shares=0.0, up_cost=4.6, down_cost=0.0)
    assert pos.is_dual is False
    assert pos.dn_shares == 0.0
    assert pos.imbalance_ratio == 1.0
    # worst_case: 4.6 - min(10, 0) = 4.6 - 0 = 4.6
    assert abs(pos.worst_case_exposure_usdc - 4.6) < 1e-9


def test_imbalance_ratio_partial():
    pos = PositionState(up_shares=15.0, down_shares=5.0, up_cost=6.9, down_cost=2.3)
    # excess = 10, total = 20 → imbalance = 0.5
    assert abs(pos.imbalance_ratio - 0.5) < 1e-9


def test_vwap_cost_ratio_alias():
    pos = PositionState(up_shares=10.0, down_shares=10.0, up_cost=4.6, down_cost=4.8)
    assert pos.vwap_cost_ratio == pos.combined_vwap


def test_dn_shares_alias():
    pos = PositionState(down_shares=7.0)
    assert pos.dn_shares == 7.0


# ---------------------------------------------------------------------------
# PositionTracker fill processing
# ---------------------------------------------------------------------------

def _make_fill(trade_id, side, shares, price):
    return FillEvent(
        trade_id=trade_id,
        order_id="ord-x",
        side=side,
        price=price,
        filled_shares=shares,
        usdc_cost=shares * price,
        trader_side="MAKER",
        timestamp=1000,
    )


def test_tracker_on_fill_up():
    tracker = PositionTracker()
    tracker.on_fill(_make_fill("t1", "up", 10.0, 0.46))
    assert tracker.position.up_shares == 10.0
    assert abs(tracker.position.up_cost - 4.6) < 1e-9


def test_tracker_on_fill_down():
    tracker = PositionTracker()
    tracker.on_fill(_make_fill("t1", "down", 10.0, 0.45))
    assert tracker.position.down_shares == 10.0


def test_tracker_deduplication():
    """Feed same trade_id twice — position updates only once."""
    tracker = PositionTracker()
    fill = _make_fill("trade-abc", "up", 10.0, 0.46)
    tracker.on_fill(fill)
    tracker.on_fill(fill)  # duplicate — must be ignored

    assert tracker.position.up_shares == 10.0   # NOT 20
    assert tracker.fill_count == 1               # only 1 fill recorded


def test_tracker_different_fills_accumulate():
    tracker = PositionTracker()
    tracker.on_fill(_make_fill("t1", "up", 10.0, 0.46))
    tracker.on_fill(_make_fill("t2", "up", 5.0, 0.44))
    assert tracker.position.up_shares == 15.0


def test_tracker_vwap_correct():
    tracker = PositionTracker()
    tracker.on_fill(_make_fill("t1", "up", 10.0, 0.46))   # cost 4.60
    tracker.on_fill(_make_fill("t2", "up", 10.0, 0.42))   # cost 4.20
    # VWAP = (4.60 + 4.20) / 20 = 0.44
    assert abs(tracker.position.up_vwap - 0.44) < 1e-9


def test_tracker_hedged_profit():
    tracker = PositionTracker()
    tracker.on_fill(_make_fill("t1", "up", 10.0, 0.46))
    tracker.on_fill(_make_fill("t2", "down", 10.0, 0.46))
    # hedged_shares = 10, combined_vwap = 0.92, profit = 10 * (1 - 0.92) = 0.80
    assert abs(tracker.position.hedged_profit - 0.80) < 1e-9


def test_tracker_reset():
    tracker = PositionTracker()
    tracker.on_fill(_make_fill("t1", "up", 10.0, 0.46))
    tracker.reset()
    assert tracker.position.up_shares == 0.0
    assert tracker.fill_count == 0
    # After reset, same trade_id is accepted again
    tracker.on_fill(_make_fill("t1", "up", 10.0, 0.46))
    assert tracker.position.up_shares == 10.0
