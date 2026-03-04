"""Unit tests for src/bot/math_engine.py — fee calc and tick helpers."""
import math
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.bot.math_engine import FeeParams, taker_fee_usdc, effective_fee_rate, snap_to_tick, round_to_tick


# ---------------------------------------------------------------------------
# FeeParams
# ---------------------------------------------------------------------------

def test_fee_params_defaults():
    fp = FeeParams()
    assert fp.fee_rate == 0.25
    assert fp.exponent == 2.0


def test_fee_params_for_crypto():
    fp = FeeParams.for_crypto()
    assert fp.fee_rate == 0.25
    assert fp.exponent == 2.0


def test_fee_params_frozen():
    fp = FeeParams()
    with pytest.raises((AttributeError, TypeError)):
        fp.fee_rate = 0.5  # type: ignore


# ---------------------------------------------------------------------------
# taker_fee_usdc
# ---------------------------------------------------------------------------

def test_taker_fee_at_50c():
    """At p=0.50 the vol component (p*(1-p))^2 = 0.0625.
    fee = 1 share * 0.50 * 0.25 * 0.0625 = 0.0078125
    Max effective rate ~1.5625% of notional.
    """
    fp = FeeParams.for_crypto()
    fee = taker_fee_usdc(1.0, 0.50, fp)
    expected = 1.0 * 0.50 * 0.25 * (0.50 * 0.50) ** 2
    assert abs(fee - expected) < 1e-10


def test_taker_fee_at_46c():
    """Typical entry price used by the bot."""
    fp = FeeParams.for_crypto()
    fee = taker_fee_usdc(10.0, 0.46, fp)
    p = 0.46
    expected = 10.0 * p * 0.25 * (p * (1.0 - p)) ** 2
    assert abs(fee - expected) < 1e-9


def test_taker_fee_scales_linearly_with_shares():
    fp = FeeParams.for_crypto()
    fee1 = taker_fee_usdc(1.0, 0.50, fp)
    fee10 = taker_fee_usdc(10.0, 0.50, fp)
    assert abs(fee10 - 10 * fee1) < 1e-10


def test_taker_fee_invalid_price_zero():
    fp = FeeParams.for_crypto()
    with pytest.raises(ValueError):
        taker_fee_usdc(1.0, 0.0, fp)


def test_taker_fee_invalid_price_one():
    fp = FeeParams.for_crypto()
    with pytest.raises(ValueError):
        taker_fee_usdc(1.0, 1.0, fp)


def test_taker_fee_invalid_shares():
    fp = FeeParams.for_crypto()
    with pytest.raises(ValueError):
        taker_fee_usdc(0.0, 0.50, fp)


def test_taker_fee_invalid_negative_shares():
    fp = FeeParams.for_crypto()
    with pytest.raises(ValueError):
        taker_fee_usdc(-5.0, 0.50, fp)


# ---------------------------------------------------------------------------
# effective_fee_rate
# ---------------------------------------------------------------------------

def test_effective_fee_rate_at_50c():
    fp = FeeParams.for_crypto()
    rate = effective_fee_rate(0.50, fp)
    expected = 0.25 * (0.50 * 0.50) ** 2  # 0.015625
    assert abs(rate - expected) < 1e-10


def test_effective_fee_rate_invalid_price():
    fp = FeeParams.for_crypto()
    assert effective_fee_rate(0.0, fp) == 0.0
    assert effective_fee_rate(1.0, fp) == 0.0
    assert effective_fee_rate(-0.1, fp) == 0.0


def test_effective_fee_rate_lower_near_edges():
    """Fee rate drops toward 0 as price approaches 0 or 1 (p*(1-p) shrinks)."""
    fp = FeeParams.for_crypto()
    rate_50 = effective_fee_rate(0.50, fp)
    rate_10 = effective_fee_rate(0.10, fp)
    rate_90 = effective_fee_rate(0.90, fp)
    assert rate_50 > rate_10
    assert rate_50 > rate_90


# ---------------------------------------------------------------------------
# snap_to_tick / round_to_tick
# ---------------------------------------------------------------------------

def test_snap_to_tick_already_on_tick():
    assert snap_to_tick(0.46, 0.01) == pytest.approx(0.46)


def test_snap_to_tick_rounds_to_nearest():
    # 0.465 should round to 0.47 (nearest cent)
    result = snap_to_tick(0.465, 0.01)
    assert result == pytest.approx(0.46) or result == pytest.approx(0.47)


def test_snap_to_tick_matches_round_to_tick():
    for p in [0.341, 0.4567, 0.999, 0.001]:
        assert snap_to_tick(p, 0.01) == round_to_tick(p, 0.01)


def test_snap_to_tick_sub_cent():
    result = snap_to_tick(0.456, 0.01)
    assert result == pytest.approx(0.46)
