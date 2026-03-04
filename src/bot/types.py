"""Data types for the Polymarket penny-ladder market-making bot."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional


class MarketPhase(Enum):
    """Market lifecycle phases."""
    WAITING = "waiting"
    DISCOVERED = "discovered"
    PRE_ENTRY = "pre_entry"
    ACTIVE = "active"
    WINDING_DOWN = "winding_down"
    CLOSED = "closed"
    RESOLVED = "resolved"
    SETTLED = "settled"


class EdgeTier(Enum):
    """Edge classification tiers."""
    WIDE = "wide"          # >5c edge
    MEDIUM = "medium"      # 2-5c edge
    TIGHT = "tight"        # 0.5-2c edge
    NEGATIVE = "negative"  # <0 — skip


@dataclass
class MarketInfo:
    """Information about a single 15-min BTC market."""
    condition_id: str
    up_token: str
    down_token: str
    open_ts: float       # from API field start_date_iso
    close_ts: float      # from API field end_date_iso
    tick_size: float     # from CLOB API
    min_order_size: float  # from OrderBookSummary


@dataclass
class LadderLevel:
    """A single price level in the penny ladder."""
    price_cents: int          # price in cents (e.g., 45 = $0.45)
    target_shares: float      # shares to place at this level
    order_id: Optional[str] = None
    filled_shares: float = 0.0
    replenish_count: int = 0  # cap at MAX_REPLENISHMENTS
    placed_at: float = 0.0    # time.time() when order was placed
    last_fill_at: float = 0.0 # time.time() when level last fully filled (survives replenish)


@dataclass
class PositionState:
    """Tracks cumulative position across fills."""
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0      # cumulative USDC spent on Up
    down_cost: float = 0.0     # cumulative USDC spent on Down

    @property
    def up_vwap(self) -> float:
        """Volume-weighted average price for Up side."""
        return self.up_cost / self.up_shares if self.up_shares > 0 else 0.0

    @property
    def down_vwap(self) -> float:
        """Volume-weighted average price for Down side."""
        return self.down_cost / self.down_shares if self.down_shares > 0 else 0.0

    @property
    def combined_vwap(self) -> float:
        """Combined VWAP = up_vwap + down_vwap (NOT total_cost / min_shares)."""
        return self.up_vwap + self.down_vwap

    @property
    def hedged_shares(self) -> float:
        """Number of fully hedged share pairs."""
        return min(self.up_shares, self.down_shares)

    @property
    def hedged_profit(self) -> float:
        """Profit from hedged pairs: hedged * (1 - combined_vwap)."""
        if self.hedged_shares <= 0:
            return 0.0
        return self.hedged_shares * (1.0 - self.combined_vwap)

    @property
    def excess_shares(self) -> float:
        """Unhedged share count."""
        return abs(self.up_shares - self.down_shares)

    @property
    def excess_side(self) -> str:
        """Which side has excess: 'up' or 'down'."""
        return "up" if self.up_shares > self.down_shares else "down"

    @property
    def total_cost(self) -> float:
        """Total USDC spent."""
        return self.up_cost + self.down_cost

    @property
    def total_shares(self) -> float:
        """Total shares across both sides."""
        return self.up_shares + self.down_shares

    @property
    def avg_cost_per_share(self) -> float:
        """Average cost per share across both sides. Target: < 0.50."""
        if self.total_shares <= 0:
            return 0.0
        return self.total_cost / self.total_shares

    @property
    def expected_profit(self) -> float:
        """Expected profit at resolution: total_shares/2 - total_cost."""
        return self.total_shares / 2.0 - self.total_cost

    @property
    def share_imbalance(self) -> float:
        """Imbalance ratio: excess / total (0 if no shares)."""
        if self.total_shares <= 0:
            return 0.0
        return self.excess_shares / self.total_shares

    @property
    def unhedged_exposure(self) -> float:
        """Dollar exposure from imbalance."""
        return abs(self.up_cost - self.down_cost)

    @property
    def risk_pnl_if_up(self) -> float:
        """P&L if Up wins (no rebate — conservative)."""
        return self.up_shares * 1.0 - self.total_cost

    @property
    def risk_pnl_if_down(self) -> float:
        """P&L if Down wins (no rebate — conservative)."""
        return self.down_shares * 1.0 - self.total_cost

    @property
    def risk_worst_case(self) -> float:
        """Worst-case P&L (no rebate)."""
        if self.up_shares <= 0 and self.down_shares <= 0:
            return 0.0
        return min(self.risk_pnl_if_up, self.risk_pnl_if_down)

    # --- Aliases / additional properties (per plan spec) ---

    @property
    def dn_shares(self) -> float:
        """Alias for down_shares (plan spec naming)."""
        return self.down_shares

    @property
    def up_avg_price(self) -> float:
        """Average (VWAP) price paid for Up shares."""
        return self.up_vwap

    @property
    def dn_avg_price(self) -> float:
        """Average (VWAP) price paid for Down shares."""
        return self.down_vwap

    @property
    def is_dual(self) -> bool:
        """True when we hold both Up and Down shares."""
        return self.up_shares > 0 and self.down_shares > 0

    @property
    def imbalance_ratio(self) -> float:
        """Alias for share_imbalance: excess / total (0 if no shares).

        0 = perfectly balanced, 1 = completely one-sided.
        """
        return self.share_imbalance

    @property
    def vwap_cost_ratio(self) -> float:
        """Combined VWAP: up_vwap + down_vwap.

        < 1.0 = profitable (guaranteed $1 payout minus entry cost > 0).
        Alias for combined_vwap with an intent-revealing name.
        """
        return self.combined_vwap

    @property
    def worst_case_exposure_usdc(self) -> float:
        """Maximum possible dollar loss at resolution.

        = total_cost - min(up_shares, down_shares)
        Positive = exposed to loss; negative = guaranteed profit regardless of outcome.
        """
        if self.up_shares <= 0 and self.down_shares <= 0:
            return 0.0
        hedged = min(self.up_shares, self.down_shares)
        return self.total_cost - hedged


@dataclass
class FillEvent:
    """A detected fill from the exchange."""
    trade_id: str
    order_id: str
    side: str              # "up" or "down"
    price: float
    filled_shares: float
    usdc_cost: float
    trader_side: str       # "MAKER" or "TAKER"
    timestamp: int


@dataclass
class BotConfig:
    """Bot configuration parameters with sensible defaults."""
    session_capital_limit: float = 300.0
    shares_per_order: float = 10.0
    order_delay_ms: int = 300
    max_replenishments: int = 3
    entry_delay_s: float = 7.0
    max_trades: int = 350
    winding_down_buffer_s: int = 60
    heartbeat_interval: int = 15
    daily_loss_limit: float = 200.0
    hourly_loss_limit: float = 100.0
    worst_case_pnl_limit: float = -100.0
    unhedged_exposure_limit: float = 100.0
    imbalance_threshold: float = 0.15
    max_daily_markets: int = 50
    # 0 = unlimited. Useful for tiny-live to auto-stop after N sessions.
    max_run_sessions: int = 0
    # 0 = unlimited. Auto-stop after N minutes wall-clock runtime.
    max_run_minutes: float = 0.0
    # Cooldown for re-checking same market after "no edge".
    no_edge_cooldown_s: float = 20.0
    # Reactivity controls.
    fill_poll_interval_s: float = 0.2
    monitor_loop_interval_s: float = 0.2
    reanchor_check_interval_s: float = 5.0
    reanchor_drift_threshold: float = 0.02
    reanchor_min_time_left_s: float = 90.0
    reanchor_min_action_cooldown_s: float = 4.0
    reanchor_side_fill_grace_s: float = 2.0
    reanchor_out_of_range_ratio: float = 0.35
    reanchor_min_edge_gain: float = 0.003
    # Allow re-anchoring to a worse buy price only when imbalance is truly extreme.
    reanchor_worse_price_imbalance_emergency: float = 0.30
    # Entry aggressiveness: allow slight negative expected combined for inventory alpha.
    # 1.0 => strict positive hedge edge only.
    entry_max_expected_combined: float = 1.010
    # Inventory-driven rebalance thresholds.
    rebalance_soft_imbalance: float = 0.08
    rebalance_hard_imbalance: float = 0.15
    rebalance_min_total_shares: float = 20.0
    rebalance_cancel_cooldown_s: float = 5.0
    # Fix #2: Per-order VWAP profitability check — reject orders pushing combined above this.
    max_combined_vwap: float = 0.99
    # Fix #6: Early VWAP stop — stop adding when combined approaches 1.0.
    vwap_stop_adding_threshold: float = 0.98
    # Fix #14: Taker flatten — active rebalancing in WINDING_DOWN.
    taker_flatten_imbalance: float = 0.15
    taker_flatten_max_notional_pct: float = 0.20
    # Per-side cost cap: each side may not exceed this fraction of session_capital_limit.
    per_side_cost_cap_pct: float = 0.60
    # Rebalance ladder cap: max levels to place during hard-rebalance re-anchor.
    rebalance_max_levels: int = 5
    # Hot-side detection: if this fraction of a side's levels fill in one drain cycle,
    # the side is marked "hot" and new placements are paused.
    hot_side_fill_ratio: float = 0.50
    hot_side_cooldown_s: float = 10.0
    # CANCEL_ALL escalation: after this many consecutive no-op CANCEL_ALLs, kill session.
    cancel_all_noop_escalation: int = 3
    # Smart cancellation: when re-anchoring, keep still-valid side orders and cancel only stale ones.
    smart_cancel_enabled: bool = False
    # Live position reconciliation (from exchange trades) to correct local drift.
    position_reconcile_interval_s: float = 5.0
    position_reconcile_tolerance_shares: float = 0.1
    position_reconcile_tolerance_cost: float = 0.25
    # Mid-session taker rebalance: buy lagging side when excess exceeds break-even.
    taker_rebalance_enabled: bool = True
    taker_rebalance_cooldown_s: float = 10.0
    taker_rebalance_min_total_shares: float = 10.0
    # Entry gate: reject markets where edge can't survive N excess fills.
    entry_min_excess_tolerance: int = 1
    # Strategy selection: "active" (original) or "passive" (full-range penny ladder).
    strategy: str = "active"
    # Passive strategy: price step between levels in cents.
    passive_step_cents: int = 1
    # Passive strategy: lowest price to consider in cents (filtered by $1 notional).
    passive_floor_cents: int = 2
    # Passive strategy: how often to check for ladder expansion (seconds).
    passive_expand_interval_s: float = 5.0
    # Passive strategy: max replenishments per level (999 = effectively unlimited).
    passive_replenish_cap: int = 999
    # Passive strategy: max depth below current price in cents (0 = no limit, use floor).
    # Full range is the intended default — cheap fills drag VWAP down for profit.
    passive_max_depth_cents: int = 0
    # Passive strategy: enable ladder expansion when price moves up.
    # ON by default — but only expands the LAGGING side when imbalanced.
    # When UP is heavy, only DOWN can expand (adding fills to balance).
    # Heavy side expansion is skipped to prevent worsening imbalance.
    passive_expansion_enabled: bool = False
    # Passive strategy: hard ceiling for expansion levels in cents.
    # Safety net only — VWAP projection is the primary guard.
    # 95c allows expansion anywhere meaningful (99c shares are worth ~1c profit).
    passive_expand_max_cents: int = 95
    # Passive strategy: VWAP cap for expansion (tighter than main max_combined_vwap).
    # 0.970 gives 3% buffer before reaching 1.0 (vs 0.990 which was too close).
    passive_expand_vwap_cap: float = 0.970
    # Passive strategy: profit lock — when worst_case >= this threshold,
    # stop ALL replenishment and expansion to protect the winning position.
    # worst_case = min(pnl_if_up, pnl_if_down) — guaranteed profit at ANY resolution.
    # Hysteresis: locks at threshold, unlocks at (threshold + unlock_buffer).
    passive_profit_lock_threshold: float = 50.0
    passive_profit_lock_unlock_buffer: float = -3.0
    # Minimum total shares (up+down) before profit lock can engage.
    # Prevents locking a trivial position (e.g., 14+14 shares, $0.21 profit).
    passive_profit_lock_min_shares: float = 50.0
    # Hard price cap: never buy above this price (cents).
    passive_max_buy_price_cents: int = 50
    # Avg cost target: tracked for logging, not used as a hard stop.
    passive_avg_cost_target: float = 0.498
    # Imbalance brake: stop replenishing/expanding the heavy side when its share
    # of total shares exceeds this ratio.  0.60 = 60% (e.g. 90 up / 60 down).
    # Set to 1.0 to disable.
    passive_imbalance_cap: float = 0.60
    # One-sided kill switch: abort if one side has 0 shares after this many seconds.
    passive_one_sided_abort_s: float = 5.0
    # One-sided kill switch: minimum fills before checking.
    passive_one_sided_min_fills: int = 3
    # Number of independent orders to place at each price level.
    # 1 = standard. 2 = two orders per cent (one fills → other stays live while replenishing).
    passive_orders_per_level: int = 1
    # Skew entry guard: reject markets where either side exceeds this price.
    # 0.57 = conservative. 0.70 = relaxed (allows entering 65/35 markets).
    passive_max_entry_price: float = 0.57
    # --- Hedge-sell strategy params ---
    # Mark-to-market profit target to trigger selling (USDC).
    hedge_sell_profit_target: float = 3.0
    # Minimum acceptable mtm profit during selling — if drops below, taker dump.
    hedge_sell_min_profit: float = 3.0
    # How often to check mtm and re-place sell orders (seconds).
    hedge_sell_check_interval_s: float = 1.0
    # Re-place sell orders when best_bid drifts by this many cents.
    hedge_sell_redrift_cents: int = 1
    # After this many seconds in selling mode without full fill, go taker.
    hedge_sell_sell_timeout_s: float = 30.0
    # --- Trend strategy params ---
    trend_levels_per_side: int = 7           # levels per side in windowed ladder
    trend_step_cents: int = 1                # price step between levels
    trend_max_buy_price_cents: int = 58      # hard price ceiling — never buy above 58c
    trend_avg_cost_target: float = 0.498     # stop placing when avg approaches 50c
    trend_reanchor_drift: float = 0.03       # re-anchor when bid drifts by 3c (base)
    trend_reanchor_cooldown_s: float = 5.0   # min time between re-anchors per side
    trend_reanchor_check_interval_s: float = 1.0  # min interval between drift checks
    trend_fill_grace_s: float = 1.5          # don't re-anchor right after a fill
    trend_replenish_cap: int = 5             # max replenishments per level
    trend_ev_lock_threshold: float = 0.0     # 0 = disabled; freeze when EV >= this
    trend_order_delay_ms: int = 80           # order delay (faster than active)
    trend_max_shares_per_side: int = 0       # 0 = auto (capital/avg_cost_target); hard per-side cap
    trend_balance_ratio: float = 1.5         # throttle heavy side when ratio > this
    trend_worst_lock_enabled: bool = False   # optional: freeze when worst_case >= threshold
    trend_worst_lock_threshold: float = 5.0  # worst_case >= this → freeze (meaningful profit)
    trend_worst_lock_unlock: float = -5.0    # resume only if worst drops below this
    # Trend taker entry: burst of taker orders at session start for instant two-sided position.
    # NOTE: combined asks >= $1.00 + fees, so entry has NEGATIVE edge. Only for speed.
    trend_taker_entry_enabled: bool = False        # disabled by default
    trend_taker_entry_orders: int = 3              # taker orders per side at entry
    trend_taker_entry_shares: float = 0.0          # shares per taker order (0 = use shares_per_order)
    # Trend taker rebalance: buy lagging side when imbalance exceeds threshold.
    trend_taker_rebalance_enabled: bool = False
    trend_taker_rebalance_deficit: int = 15        # trigger when abs(up - down) > this
    trend_taker_rebalance_shares: int = 0          # 0 = auto (use shares_per_order); buy per cycle
    trend_taker_rebalance_cooldown_s: float = 5.0  # prevents chasing during fast moves
    trend_taker_budget_pct: float = 0.25           # max taker spend as % of session capital
    trend_rebalance_avg_cost_limit: float = 0.505     # relaxed avg cost target for lagging-side rebalancing
    # Graduated taker tiers — triggered by worst-case P&L, not share deficit.
    # worst_case = min(up_shares, down_shares) - total_cost
    # Tier fires when worst_case drops below threshold AND deficit > shares_per_order.
    trend_taker_tier1_worst: float = 0.0      # gentle: worst just went negative
    trend_taker_tier2_worst: float = -15.0    # moderate: meaningful loss
    trend_taker_tier3_worst: float = -40.0    # urgent: serious loss
    trend_taker_grace_period_s: float = 5.0   # no taker rebalance during initial position building
    trend_taker_tier1_cooldown_s: float = 15.0
    trend_taker_tier2_cooldown_s: float = 8.0
    trend_taker_tier3_cooldown_s: float = 5.0
    # Legacy share-deficit thresholds (kept for backward compat, not used by default)
    trend_taker_tier1_deficit: int = 30
    trend_taker_tier2_deficit: int = 70
    trend_taker_tier3_deficit: int = 150
    # Rolling-window hot-side detection: trigger cooldown when fills_in_window >= threshold
    hot_side_rolling_window_s: float = 15.0
    hot_side_rolling_threshold: int = 8
    # Fix #68: Profit protection mode — freeze heavy side when profitable
    trend_profit_protect_threshold: float = 20.0  # worst >= this → protection mode
    # Fix #69: Proportional pair premium — accept unprofitable pairs when worst is negative.
    # For every $1 worst goes negative, accept this many cents of pair premium.
    # E.g., 0.005 → worst=-$20 allows 10c premium, worst=-$36 allows 18c premium.
    trend_taker_pair_premium_per_dollar: float = 0.005


@dataclass
class BotState:
    """Full bot state for crash recovery and monitoring."""
    phase: MarketPhase = MarketPhase.WAITING
    market: Optional[MarketInfo] = None
    position: PositionState = field(default_factory=PositionState)
    active_orders: Dict[str, LadderLevel] = field(default_factory=dict)
    order_map: Dict[str, dict] = field(default_factory=dict)
    daily_pnl: float = 0.0
    hourly_pnl: float = 0.0
    session_worst_case_pnl: float = 0.0
    reserved_notional: float = 0.0
    total_trades: int = 0
    session_id: str = ""
    taker_notional_used: float = 0.0
    cancel_race_buffer: float = 0.0  # notional from recently cancelled orders that might fill
