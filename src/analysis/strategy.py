"""Strategy analyzer for the target trader."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict

from .calculator import calculate_arbitrage_pnl, TradeResult


@dataclass
class MarketTrade:
    """A trade in a specific market."""
    timestamp: datetime
    market_id: str
    market_question: str
    outcome: str  # "Up" or "Down"
    side: str  # "buy" or "sell"
    price: float
    shares: float
    is_maker: bool
    token_id: str


@dataclass
class MarketPair:
    """A pair of Up+Down trades in the same market."""
    market_id: str
    market_question: str
    up_trade: Optional[MarketTrade]
    down_trade: Optional[MarketTrade]
    pnl: Optional[TradeResult]


def group_trades_by_market(trades: list[MarketTrade]) -> dict[str, list[MarketTrade]]:
    """Group trades by market ID."""
    grouped = defaultdict(list)
    for trade in trades:
        grouped[trade.market_id].append(trade)
    return dict(grouped)


def find_arbitrage_pairs(trades: list[MarketTrade]) -> list[MarketPair]:
    """Find Up+Down pairs that could be arbitrage.

    Looks for trades where trader bought both Up and Down
    in the same market within a short time window.
    """
    grouped = group_trades_by_market(trades)
    pairs = []

    for market_id, market_trades in grouped.items():
        # Separate by outcome
        up_trades = [t for t in market_trades if t.outcome.lower() in ("up", "yes")]
        down_trades = [t for t in market_trades if t.outcome.lower() in ("down", "no")]

        # Only buys (opening positions)
        up_buys = [t for t in up_trades if t.side == "buy"]
        down_buys = [t for t in down_trades if t.side == "buy"]

        if not up_buys or not down_buys:
            continue

        # Match trades by time proximity (within 15 minutes)
        for up in up_buys:
            for down in down_buys:
                time_diff = abs((up.timestamp - down.timestamp).total_seconds())
                if time_diff < 900:  # 15 minutes
                    # Calculate P&L
                    pnl = calculate_arbitrage_pnl(
                        up_price=up.price,
                        down_price=down.price,
                        shares=min(up.shares, down.shares),
                        is_maker=up.is_maker and down.is_maker,
                    )

                    pairs.append(MarketPair(
                        market_id=market_id,
                        market_question=up.market_question,
                        up_trade=up,
                        down_trade=down,
                        pnl=pnl,
                    ))

    return pairs


def analyze_trader_strategy(pairs: list[MarketPair]) -> dict:
    """Analyze the trader's strategy from their trade pairs."""
    if not pairs:
        return {"error": "No pairs to analyze"}

    total_pairs = len(pairs)
    profitable_pairs = [p for p in pairs if p.pnl and p.pnl.net_profit > 0]
    maker_pairs = [p for p in pairs if p.up_trade and p.up_trade.is_maker]

    total_profit = sum(p.pnl.net_profit for p in pairs if p.pnl)
    total_cost = sum(p.pnl.net_cost for p in pairs if p.pnl)

    avg_up_price = sum(p.up_trade.price for p in pairs if p.up_trade) / total_pairs
    avg_down_price = sum(p.down_trade.price for p in pairs if p.down_trade) / total_pairs
    avg_spread = avg_up_price + avg_down_price

    return {
        "total_pairs": total_pairs,
        "profitable_pairs": len(profitable_pairs),
        "win_rate": len(profitable_pairs) / total_pairs * 100 if total_pairs else 0,
        "maker_pairs": len(maker_pairs),
        "maker_rate": len(maker_pairs) / total_pairs * 100 if total_pairs else 0,
        "total_profit": total_profit,
        "total_cost": total_cost,
        "roi": total_profit / total_cost * 100 if total_cost else 0,
        "avg_profit_per_pair": total_profit / total_pairs if total_pairs else 0,
        "avg_up_price": avg_up_price,
        "avg_down_price": avg_down_price,
        "avg_spread": avg_spread,
    }


def print_strategy_analysis(analysis: dict):
    """Print strategy analysis results."""
    print("\n" + "=" * 60)
    print("TRADER STRATEGY ANALYSIS")
    print("=" * 60)

    print(f"\n📊 Overview:")
    print(f"  Total Pairs: {analysis['total_pairs']}")
    print(f"  Profitable: {analysis['profitable_pairs']} ({analysis['win_rate']:.1f}%)")
    print(f"  Maker Orders: {analysis['maker_pairs']} ({analysis['maker_rate']:.1f}%)")

    print(f"\n💰 P&L:")
    print(f"  Total Profit: ${analysis['total_profit']:.2f}")
    print(f"  Total Cost: ${analysis['total_cost']:.2f}")
    print(f"  ROI: {analysis['roi']:.2f}%")
    print(f"  Avg Profit/Pair: ${analysis['avg_profit_per_pair']:.2f}")

    print(f"\n📈 Pricing:")
    print(f"  Avg Up Price: ${analysis['avg_up_price']:.4f}")
    print(f"  Avg Down Price: ${analysis['avg_down_price']:.4f}")
    print(f"  Avg Total Spread: ${analysis['avg_spread']:.4f}")

    if analysis['avg_spread'] < 1.0:
        print(f"\n✅ Spread < $1.00 confirms arbitrage strategy!")
        print(f"   Gross profit per pair: ${1.0 - analysis['avg_spread']:.4f}")
    else:
        print(f"\n⚠️ Spread >= $1.00 - not pure arbitrage")

    print("=" * 60)
