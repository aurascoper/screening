#!/usr/bin/env python3
"""Contained STRONG_BUY cron entrypoint (live execution and cross-repo MEXC reads disabled).

G0 Containment Notice (Issue #18):
- Cross-repository sys.path injection of ~/Developer/live_trading and execution.mexc_client is removed.
- Authenticated MEXC position/asset reads and order submissions are disabled in aurascoper/screening.
- Passing --live fails closed with exit code 2 even if DISARM is removed.
- When DISARM is present (the repository default), exits 0 immediately without network or disk scan side effects.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import os
from pathlib import Path
from typing import Sequence

HERE = Path(__file__).resolve().parent
DISARM = HERE / "DISARM"
CSV = HERE / "tv_scan_mexc_usdc_4h.csv"
LOG_DIR = HERE / "logs"


def log(msg: str, *, log_dir: Path = LOG_DIR) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    line = f"[{ts}] {msg}"
    print(line)
    with (log_dir / "strong_buys_cron.log").open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def main(
    argv: Sequence[str] | None = None,
    *,
    disarm_path: Path = DISARM,
    csv_path: Path = CSV,
    log_dir: Path = LOG_DIR,
) -> int:
    ap = argparse.ArgumentParser(description="Contained STRONG_BUY preview logger (live execution disabled).")
    ap.add_argument("--live", action="store_true", help="DISABLED under Issue #18 G0 containment.")
    args = ap.parse_args(list(argv) if argv is not None else None)

    if args.live:
        log(
            "G0-BLOCKED: --live execution and cross-repo live_trading MEXC client access are disabled.",
            log_dir=log_dir,
        )
        return 2

    if os.path.exists(disarm_path):
        log("DISARM sentinel present - exiting without action.", log_dir=log_dir)
        return 0

    if not csv_path.exists():
        log(f"Preview CSV not present ({csv_path.name}) - exiting cleanly.", log_dir=log_dir)
        return 0

    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    picks = [r.get("mexc_symbol", "?") for r in rows if r.get("rating") == "STRONG_BUY"]
    log(f"Keyless local preview only: {len(picks)} STRONG_BUY symbol(s): {', '.join(picks)}", log_dir=log_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
