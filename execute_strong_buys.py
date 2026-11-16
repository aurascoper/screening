#!/usr/bin/env python3
"""Keyless preview sizing helper for screener BUY/STRONG_BUY rows.

G0 Containment Notice (Issue #18):
- aurascoper/screening is strictly candidate discovery; alerts are hypotheses, never trade authorization.
- All imports of unversioned external ~/Developer/mexc_futures (mexc_market_order) have been removed.
- Passing --live fails closed immediately with exit code 2 before reading credentials or contacting MEXC.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Sequence
import urllib.request

MEXC = "https://contract.mexc.com/api/v1/contract"
_SIDE_OPEN = {"LONG": 1, "SHORT": 3}
_ORDER_TYPE_MARKET = 5
MAX_PREVIEW_LEVERAGE = 10


class G0ContainmentError(PermissionError):
    """Raised when live execution or external executor loading is attempted in screening."""


def calculate_order_quantity(
    margin_usdc: float,
    leverage: int,
    price: float,
    contract_size: float,
    min_vol: float,
    vol_unit: float = 1.0,
    fee_buffer: float = 0.001,
) -> int:
    """Pure local contract sizing math for preview calculations (zero external imports)."""
    if margin_usdc <= 0 or leverage <= 0 or price <= 0 or contract_size <= 0 or vol_unit <= 0:
        raise ValueError("margin, leverage, price, contract_size, and vol_unit must be positive")
    notional = margin_usdc * float(leverage) * (1.0 - fee_buffer)
    raw_contracts = (notional / price) / contract_size
    stepped = math.floor(raw_contracts / vol_unit) * vol_unit
    vol = int(round(stepped))
    if vol < min_vol:
        raise ValueError(f"sized volume {vol} below minVol {min_vol}")
    return vol


def _public(path: str) -> dict:
    req = urllib.request.Request(MEXC + path, headers={"User-Agent": "screening-preview/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def spec(symbol: str) -> tuple[float, float, float, float, int]:
    d = _public(f"/detail?symbol={symbol}").get("data", {})
    t = _public(f"/ticker?symbol={symbol}").get("data", {})
    return (
        float(t["lastPrice"]),
        float(d["contractSize"]),
        float(d.get("minVol", 1)),
        float(d.get("volUnit", 1)),
        int(d.get("maxLeverage", 1)),
    )


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Keyless preview of screener rows (live execution disabled).")
    ap.add_argument("--csv", default="tv_scan_mexc_usdc_1d.csv", help="Screener CSV to inspect.")
    ap.add_argument("--ratings", nargs="*", default=["STRONG_BUY"], help="Ratings to preview.")
    ap.add_argument("--margin", type=float, default=24.75, help="Hypothetical isolated margin (USDC).")
    ap.add_argument("--leverage", type=int, default=10, help="Hypothetical leverage (capped at 10x).")
    ap.add_argument("--side", default="LONG", choices=["LONG", "SHORT"])
    ap.add_argument("--live", action="store_true", help="DISABLED under Issue #18 G0 containment.")
    args = ap.parse_args(list(argv) if argv is not None else None)

    if args.live:
        print(
            "[G0-BLOCKED] --live execution in aurascoper/screening is permanently disabled "
            "(Issue #18: candidate discovery only; external mexc_futures executor quarantined)."
        )
        return 2

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return 1

    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    target_ratings = set(args.ratings or [])
    picks = [r for r in rows if r.get("rating") in target_ratings]
    if not picks:
        print(f"No rows rated {sorted(target_ratings)} in {args.csv}.")
        return 0

    capped_req_lev = min(max(1, args.leverage), MAX_PREVIEW_LEVERAGE)
    print(
        f"PREVIEW ONLY: {len(picks)} pick(s) from {args.csv} rated {sorted(target_ratings)} "
        f"| margin ${args.margin} | up to {capped_req_lev}x | {args.side}\n"
    )

    for r in picks:
        sym = str(r["mexc_symbol"])
        base = str(r.get("base") or sym.replace("_USDC", ""))
        price, csize, minv, vunit, maxlev = spec(sym)
        lev = min(capped_req_lev, maxlev)
        try:
            vol = calculate_order_quantity(args.margin, lev, price, csize, minv, vunit)
        except ValueError as exc:
            print(f"[skip] {sym}: {exc}")
            continue
        hypothetical = {
            "symbol": sym,
            "vol": vol,
            "side": _SIDE_OPEN[args.side],
            "type": _ORDER_TYPE_MARKET,
            "openType": 1,
            "leverage": lev,
        }
        notional = vol * csize * price
        print(f"{sym}  rating={r['rating']}  price={price}  lev={lev}x")
        print(f"  vol={vol} contracts (~{vol * csize:g} {base}, ~${notional:,.0f} notional)")
        print(f"  hypothetical_preview: {json.dumps(hypothetical)}\n")

    print("PREVIEW only - live execution and external mexc_futures imports are disabled under G0.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
