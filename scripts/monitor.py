"""
Live terminal dashboard for the BTC 15-min bot (selbot/bot.py).

Reads today's JSONL trade log and renders a rich UI.
Run in a separate terminal alongside the bot:

    python scripts/monitor.py

Install dependency if needed:  pip install rich
"""

import json
import time
from datetime import datetime
from pathlib import Path

from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

LOG_DIR = Path(__file__).parent.parent / "logs"
REFRESH_INTERVAL = 2
MAX_DETAIL_LEN = 60

SHOW_EVENTS = {
    "orders_placed", "fill_detected", "hedge_trigger", "hedge_buy",
    "hedge_force_buy", "hedge_sell_filled", "hedge_balance_skip",
    "bail", "merge", "redeem", "stale_cancel", "window_missed",
    "max_open_skipped", "crash_restart",
}

EVENT_COLORS = {
    "ORDER":    "cyan",
    "FILL":     "yellow",
    "HEDGE":    "bright_yellow",
    "HEDGED":   "green",
    "FORCE":    "bright_yellow bold",
    "SOLD":     "bright_red",
    "BAL SKIP": "dim yellow",
    "BAIL":     "red",
    "MERGED":   "green",
    "REDEEMED": "green",
    "STALE":    "dim",
    "MISSED":   "dim",
    "MAX OPEN": "dim",
    "CRASH":    "bright_red bold",
}


def find_log() -> Path | None:
    date_str = time.strftime("%Y%m%d")
    p = LOG_DIR / f"place_15_{date_str}.jsonl"
    return p if p.exists() else None


def load_events_incremental(
    path: Path,
    last_pos: int,
    cached_events: list[dict],
) -> tuple[list[dict], int]:
    """
    Read only new lines from the log file. Returns (all_events, new_file_pos).
    If file was truncated, falls back to full read.
    """
    try:
        size = path.stat().st_size
        if size < last_pos:
            return load_events_full(path), size
        with open(path, "r") as f:
            f.seek(last_pos)
            new_lines = f.readlines()
            new_pos = f.tell()
    except (OSError, IOError):
        return cached_events, last_pos

    if not new_lines:
        return cached_events, new_pos

    events = list(cached_events)
    for line in new_lines:
        line = line.strip()
        if line:
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return events, new_pos


def load_events_full(path: Path) -> list[dict]:
    """Full read when file changed or first load."""
    events = []
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception:
        pass
    return events


def build_state(events: list[dict]) -> dict:
    state = {
        "rounds": 0,
        "dual_fills": 0,
        "hedged": 0,
        "bailed": 0,
        "net_pnl": 0.0,
        "total_fees": 0.0,
        "markets": {},
        "recent": [],
    }

    for ev in events:
        event = ev.get("event", "")
        mts = ev.get("market_ts")

        # Market state tracking
        if event == "orders_placed" and mts:
            state["rounds"] += 1
            state["markets"][mts] = {
                "status": "OPEN",
                "placed_at": ev.get("ts", 0),
                "sides": ev.get("sides_placed", 0),
            }
        elif event == "fill_detected" and mts and mts in state["markets"]:
            state["markets"][mts]["status"] = f"HOLD {ev.get('filled_side', '?')}"
        elif event == "hedge_buy" and mts and mts in state["markets"]:
            state["markets"][mts]["status"] = "HEDGED"
            state["hedged"] += 1
        elif event == "hedge_force_buy" and mts and mts in state["markets"]:
            state["markets"][mts]["status"] = "HEDGED"
            state["hedged"] += 1
        elif event == "hedge_sell_filled" and mts and mts in state["markets"]:
            state["markets"][mts]["status"] = "SOLD"
        elif event == "bail" and mts and mts in state["markets"]:
            state["markets"][mts]["status"] = "BAILED"
            state["bailed"] += 1
        elif event == "merge" and mts and mts in state["markets"]:
            state["markets"][mts]["status"] = "MERGED ✓" if ev.get("ok") else "MERGE ✗"
            if ev.get("ok"):
                state["dual_fills"] += 1
        elif event == "redeem" and mts and mts in state["markets"]:
            state["markets"][mts]["status"] = "REDEEMED ✓" if ev.get("ok") else "REDEEM ✗"

        if event in SHOW_EVENTS:
            state["recent"].append(ev)

    # Keep only last 10 markets and 14 recent events
    recent_keys = sorted(state["markets"])[-10:]
    state["markets"] = {k: state["markets"][k] for k in recent_keys}
    state["recent"] = state["recent"][-14:]
    return state


def rel_time(ts: int) -> str:
    """Human-readable relative time: 'now', '1m ago', '5m ago'."""
    if not ts:
        return "??"
    diff = int(time.time()) - ts
    if diff < 10:
        return "now"
    if diff < 60:
        return f"{diff}s"
    if diff < 3600:
        return f"{diff // 60}m"
    return f"{diff // 3600}h"


def _trunc(s: str, max_len: int = MAX_DETAIL_LEN) -> str:
    return (s[: max_len - 1] + "…") if len(s) > max_len else s


def fmt_event(ev: dict) -> tuple[str, str, str, str]:
    """Returns (time_str, rel_str, label, detail) for an event row."""
    ts = ev.get("ts", 0)
    t = datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "??:??:??"
    event = ev.get("event", "")

    dispatch = {
        "orders_placed":         ("ORDER",    f"UP+DN placed, {ev.get('sides_placed',0)} side(s)"),
        "fill_detected":         ("FILL",     f"{ev.get('filled_side','?')} filled → hold {ev.get('hedge_side','').upper()}"),
        "hedge_trigger":         ("HEDGE",    f"{ev.get('hedge_side','?').upper()} ask={ev.get('ask',0):.3f}  EV=+${ev.get('ev',0):.3f}  [{ev.get('reason','')}]"),
        "hedge_buy":             ("HEDGED",   f"{ev.get('side','?')} @ {ev.get('price',0):.3f} × {ev.get('shares',0):.0f}sh  EV=+${ev.get('ev',0):.3f}"),
        "hedge_force_buy":       ("FORCE",    f"{ev.get('hedge_side','?')} force-buy @ ask  [{ev.get('reason','')}]"),
        "hedge_sell_filled":     ("SOLD",     f"sold filled {ev.get('hedge_side','?')} side  [{ev.get('reason','')}]"),
        "hedge_balance_skip":    ("BAL SKIP", f"insufficient balance for hedge"),
        "bail":                  ("BAIL",     f"{ev.get('trigger_side','?')} ask={ev.get('trigger_ask',0):.3f}  band={ev.get('secs_left',0):.0f}s left"),
        "merge":                 ("MERGED",   f"{ev.get('sets',0):.1f} sets  {'✓' if ev.get('ok') else '✗ FAILED'}"),
        "redeem":                ("REDEEMED", f"UP={ev.get('up_bal',0):.1f} DN={ev.get('dn_bal',0):.1f}  {'✓' if ev.get('ok') else '✗ FAILED'}"),
        "stale_cancel":          ("STALE",    f"no fill at {ev.get('elapsed_s',0):.0f}s — cancelled"),
        "window_missed":         ("MISSED",   f"placement window passed ({ev.get('elapsed_s',0):.0f}s elapsed)"),
        "max_open_skipped":      ("MAX OPEN", f"{ev.get('open_count',0)}/{ev.get('max',0)} markets open — skipped"),
        "crash_restart":         ("CRASH",    f"{ev.get('error','')}  backoff={ev.get('backoff_s',0):.0f}s"),
    }
    label, detail = dispatch.get(event, (event, ""))
    return t, rel_time(ts), label, _trunc(detail)


def build_dashboard(state: dict, log_path: Path | None, last_ts: int, event_count: int) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=1),
    )
    layout["body"].split_row(
        Layout(name="stats", ratio=1),
        Layout(name="events", ratio=2),
    )

    # ── Header ────────────────────────────────────────────────────────────────
    pnl = state["net_pnl"]
    pnl_style = "bold green" if pnl >= 0 else "bold red"
    hdr = Text(justify="center")
    hdr.append("POLY-AUTOBETTER  ", style="bold cyan")
    hdr.append("BTC 15-min Up/Down Bot  ", style="dim white")
    hdr.append("Net P&L: ", style="bold white")
    hdr.append(f"${pnl:+.2f}", style=pnl_style)
    layout["header"].update(Panel(hdr, box=box.HORIZONTALS, border_style="blue"))

    # ── Stats panel ───────────────────────────────────────────────────────────
    tbl = Table(box=None, show_header=False, padding=(0, 1))
    tbl.add_column("k", style="dim", width=14)
    tbl.add_column("v", style="bold white")

    def row(k, v, style="bold white"):
        tbl.add_row(k, Text(str(v), style=style))

    row("Rounds",     state["rounds"])
    row("Net P&L",    f"${pnl:+.2f}", pnl_style)
    row("Fees paid",  f"${state['total_fees']:.3f}", "dim")
    tbl.add_row("", "")
    row("Dual fills", state["dual_fills"],  "green")
    row("Hedged",     state["hedged"],      "yellow")
    row("Bailed",     state["bailed"],      "red")

    # Recent markets (last 5)
    if state["markets"]:
        tbl.add_row("", "")
        tbl.add_row(Text("MARKETS", style="dim"), "")
        for mts in sorted(state["markets"])[-5:]:
            info = state["markets"][mts]
            status = info.get("status", "?")
            status_style = (
                "green"  if "MERGED" in status or "REDEEMED" in status or status == "HEDGED"
                else "red"    if "BAIL" in status or "✗" in status or status == "SOLD"
                else "yellow" if "HOLD" in status
                else "cyan"   if status == "OPEN"
                else "white"
            )
            t = datetime.fromtimestamp(mts).strftime("%H:%M")
            tbl.add_row(f"  {t}", Text(status, style=status_style))

    layout["stats"].update(Panel(tbl, title="[bold]SESSION[/bold]", border_style="blue"))

    # ── Recent events panel ───────────────────────────────────────────────────
    ev_tbl = Table(box=box.SIMPLE, show_header=True, padding=(0, 1), expand=True)
    ev_tbl.add_column("Time",    style="dim", width=8)
    ev_tbl.add_column("Ago",     style="dim", width=5)
    ev_tbl.add_column("Event",                 width=10)
    ev_tbl.add_column("Detail",  style="white")

    for ev in reversed(state["recent"]):
        t, rel, label, detail = fmt_event(ev)
        color = EVENT_COLORS.get(label, "white")
        ev_tbl.add_row(t, rel, Text(label, style=f"bold {color}"), detail)

    layout["events"].update(Panel(ev_tbl, title="[bold]EVENTS[/bold] (newest ↑)", border_style="blue"))

    # ── Footer ────────────────────────────────────────────────────────────────
    age = int(time.time()) - last_ts if last_ts else 999
    log_name = log_path.name if log_path else "no log found — is the bot running?"
    live_str = "[bold green]● live[/bold green]" if age < 15 else "[dim]○ idle[/dim]"
    footer = Text(
        f"  {log_name}   {live_str}   last: {age}s ago   {event_count} events   {datetime.now().strftime('%H:%M:%S')}",
        style="dim",
    )
    layout["footer"].update(footer)

    return layout


def main():
    console = Console()
    cached_events: list[dict] = []
    last_pos = 0
    last_log_path: Path | None = None

    with Live(console=console, refresh_per_second=1, screen=True) as live:
        while True:
            log_path = find_log()
            if log_path:
                # Incremental load: only read new lines when same file; full reload on path change
                if log_path != last_log_path:
                    events = load_events_full(log_path)
                    last_pos = log_path.stat().st_size if log_path.exists() else 0
                    last_log_path = log_path
                else:
                    events, last_pos = load_events_incremental(log_path, last_pos, cached_events)
                cached_events = events
                state = build_state(events)
                last_ts = events[-1].get("ts", 0) if events else 0
            else:
                state = build_state([])
                last_ts = 0
                last_log_path = None
                cached_events = []

            live.update(build_dashboard(state, log_path, last_ts, len(cached_events)))
            time.sleep(REFRESH_INTERVAL)


if __name__ == "__main__":
    main()
