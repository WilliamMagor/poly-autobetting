"""Unit tests for FillDeduplicator in src/bot/fill_monitor.py."""
import json
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.bot.fill_monitor import FillDeduplicator


# ---------------------------------------------------------------------------
# Basic deduplication
# ---------------------------------------------------------------------------

def test_new_trade_returns_true():
    dedup = FillDeduplicator()
    assert dedup.process("trade-001") is True


def test_duplicate_trade_returns_false():
    dedup = FillDeduplicator()
    dedup.process("trade-001")
    assert dedup.process("trade-001") is False


def test_different_trade_ids_both_accepted():
    dedup = FillDeduplicator()
    assert dedup.process("trade-001") is True
    assert dedup.process("trade-002") is True


def test_same_trade_twice_position_only_changes_once():
    """Canonical test: feeding the same fill twice must not double-count."""
    from src.bot.types import PositionState
    pos = PositionState()
    dedup = FillDeduplicator()

    def apply_fill(trade_id: str, shares: float, price: float, side: str):
        if dedup.process(trade_id):
            if side == "up":
                pos.up_shares += shares
                pos.up_cost += shares * price
            else:
                pos.down_shares += shares
                pos.down_cost += shares * price

    apply_fill("fill-abc-1", 10.0, 0.46, "up")
    apply_fill("fill-abc-1", 10.0, 0.46, "up")  # duplicate — must be ignored

    assert pos.up_shares == 10.0
    assert abs(pos.up_cost - 4.60) < 1e-9


def test_count_increments_for_new_only():
    dedup = FillDeduplicator()
    assert dedup.count == 0
    dedup.process("a")
    assert dedup.count == 1
    dedup.process("a")  # duplicate
    assert dedup.count == 1
    dedup.process("b")
    assert dedup.count == 2


def test_is_seen():
    dedup = FillDeduplicator()
    assert dedup.is_seen("x") is False
    dedup.process("x")
    assert dedup.is_seen("x") is True


def test_clear():
    dedup = FillDeduplicator()
    dedup.process("a")
    dedup.process("b")
    assert dedup.count == 2
    dedup.clear()
    assert dedup.count == 0
    assert dedup.process("a") is True  # now accepted again


# ---------------------------------------------------------------------------
# Disk persistence
# ---------------------------------------------------------------------------

def test_persist_and_reload():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "seen_fills.json")

        # First instance: mark some fills
        d1 = FillDeduplicator(path)
        d1.process("fill-001")
        d1.process("fill-002")
        d1.save()

        assert os.path.exists(path)

        # Second instance: reload from disk
        d2 = FillDeduplicator(path)
        assert d2.count == 2
        assert d2.is_seen("fill-001")
        assert d2.is_seen("fill-002")
        assert not d2.is_seen("fill-003")

        # Duplicate check survives restart
        assert d2.process("fill-001") is False
        assert d2.process("fill-003") is True


def test_persist_atomic_write():
    """Verify saved file is valid JSON and contains expected structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "seen_fills.json")
        d = FillDeduplicator(path)
        d.process("trade-x")
        d.process("trade-y")
        d.save()

        with open(path) as f:
            data = json.load(f)

        assert "seen_trade_ids" in data
        assert set(data["seen_trade_ids"]) == {"trade-x", "trade-y"}


def test_missing_file_handled_gracefully():
    """No crash if persist file doesn't exist yet."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "nonexistent.json")
        d = FillDeduplicator(path)  # should not raise
        assert d.count == 0


def test_no_persist_path_save_is_noop():
    """FillDeduplicator works fine without a persist path."""
    d = FillDeduplicator()
    d.process("a")
    d.save()  # should not raise
    assert d.count == 1
