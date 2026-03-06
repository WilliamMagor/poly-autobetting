# poly-autobetting

Automated trading bot for Polymarket 15-minute prediction markets (BTC/ETH by default). Places configurable limit orders on UP and DOWN outcomes, monitors positions with real-time WebSocket prices, and auto-redeems resolved positions on-chain.

## How It Works

The bot targets 15-minute up/down binary markets on Polymarket for configured symbols (default: BTC and ETH). It places limit buy orders on both sides (UP and DOWN) for each market at a configurable entry price (default 46c). When both sides fill, the combined cost is < $1.00 for a guaranteed $1.00 payout. It automatically rotates to new markets every 15 minutes.

- **Dual-Side Limit Orders** — places limit buys on both UP and DOWN at configurable entry price
- **Auto-Rotation** — detects and places orders on upcoming 15-minute markets (900s window)
- **WebSocket Price Feed** — real-time order book monitoring with auto-reconnect
- **EV-Based Hedge/Bail** — time-phased trailing hedge and EV-based bail schedule
- **Buy-Hold Option** — optional late-stage favored-side scaling in `buy_hold_91` mode
- **Late Profit Boost** — optional extra buy when both sides are filled and one side is strong near close
- **Auto-Redeem** — redeems resolved positions via direct on-chain Polygon transaction

See `BOT_PROCESS.md` for a full process walkthrough, market slug format, and strategy details.

## Project Structure

```
selbot/
  bot.py           # Main bot script
  prices.py        # Balance check + TTL cache
  requirements.txt
src/bot/
  ws_book_feed.py  # WebSocket order book feed
  math_engine.py   # Fee calculation
  types.py         # Data types (PositionState, FillEvent)
  fill_monitor.py  # FillDeduplicator for crash-safe fill tracking
scripts/
  monitor.py       # Live terminal dashboard (reads trade logs)
logs/              # Trade logs (place_15_YYYYMMDD.jsonl)
```

## Setup

### Prerequisites

- Python 3.10+
- Polymarket account with API credentials

### Installation

```bash
git clone https://github.com/WilliamMagor/poly-autobetting.git
cd poly-autobetting

python -m venv venv
venv\Scripts\activate   # Windows
# source venv/bin/activate  # Linux/Mac

pip install -r selbot/requirements.txt
```

### Configuration

```bash
cp .env.example .env
```

Edit `.env` with your values:

| Variable | Description |
|---|---|
| `POLYMARKET_PRIVATE_KEY` | Your Polygon wallet private key |
| `POLYMARKET_FUNDER` | Funder/proxy wallet address (optional) |
| `POLYGON_RPC_URL` | Polygon RPC URL (optional, has default) |

## Usage

```bash
venv\Scripts\activate
python selbot/bot.py
```

The bot will place limit orders on both UP and DOWN for the current and upcoming configured 15-minute markets (default BTC/ETH), then loop to check for new markets, manage hedges/bails/late-stage strategy logic, and redeem resolved positions.

### Monitor Dashboard

Run in a separate terminal to view live trade activity:

```bash
python scripts/monitor.py
```

## Disclaimer

This software is for educational purposes. Trading on prediction markets involves risk. Use at your own discretion.
