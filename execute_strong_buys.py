#!/usr/bin/env python3
"""Bridge: screener BUY/STRONG_BUY rows -> sized MEXC USDC-perp market orders.

Default = keyless PREVIEW: sizes each pick from live public price + contractSize
and prints the exact /order/submit payload that WOULD be sent. Nothing leaves
your machine. With --live AND MEXC_API_KEY/MEXC_API_SECRET set, it sets leverage
then submits each order via the mexc_futures executor.

  python3 execute_strong_buys.py --csv tv_scan_mexc_usdc_1d.csv            # WLD preview
  python3 execute_strong_buys.py --csv tv_scan_mexc_usdc_1d.csv --ratings STRONG_BUY BUY
  python3 execute_strong_buys.py --csv ... --leverage 100 --live           # submit (needs keys)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

import pandas as pd

MEXC = "https://contract.mexc.com/api/v1/contract"


def _load_executor():
    """Import the mexc_futures executor lazily, so this file is importable even
    where that package isn't on disk (e.g. a cloud checkout that only screens +
    reports and never goes --live). Override the location with MEXC_FUTURES_DIR.
    """
    sys.path.insert(0, os.environ.get("MEXC_FUTURES_DIR", os.path.expanduser("~/Developer/mexc_futures")))
    from mexc_market_order import (  # noqa: E402
        MexcContractClient, calculate_order_quantity,
        _SIDE_OPEN, _POSITION_TYPE, _ORDER_TYPE_MARKET,
    )
    return MexcContractClient, calculate_order_quantity, _SIDE_OPEN, _POSITION_TYPE, _ORDER_TYPE_MARKET


def _public(path: str) -> dict:
    req = urllib.request.Request(MEXC + path, headers={"User-Agent": "exec/preview"})
    return json.loads(urllib.request.urlopen(req, timeout=15).read())


def spec(symbol: str):
    d = _public(f"/detail?symbol={symbol}").get("data", {})
    t = _public(f"/ticker?symbol={symbol}").get("data", {})
    return (
        float(t["lastPrice"]),
        float(d["contractSize"]),
        float(d.get("minVol", 1)),
        float(d.get("volUnit", 1)),
        int(d.get("maxLeverage", 1)),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Execute screener BUY/STRONG_BUY as MEXC USDC orders.")
    ap.add_argument("--csv", default="tv_scan_mexc_usdc_1d.csv", help="Screener output to read.")
    ap.add_argument("--ratings", nargs="*", default=["STRONG_BUY"], help="Ratings to act on.")
    ap.add_argument("--margin", type=float, default=24.75, help="Isolated margin per position (USDC).")
    ap.add_argument("--leverage", type=int, default=50, help="Target leverage (capped at maxLeverage).")
    ap.add_argument("--side", default="LONG", choices=["LONG", "SHORT"])
    ap.add_argument("--live", action="store_true", help="Actually set leverage + submit (needs keys).")
    args = ap.parse_args()
    (MexcContractClient, calculate_order_quantity,
     _SIDE_OPEN, _POSITION_TYPE, _ORDER_TYPE_MARKET) = _load_executor()

    df = pd.read_csv(args.csv)
    picks = df[df["rating"].isin(args.ratings)].copy()
    if picks.empty:
        print(f"No rows rated {args.ratings} in {args.csv}.")
        return 0

    api_key = os.environ.get("MEXC_API_KEY")
    # accept either secret name; live_trading/.env uses MEXC_SECRET_KEY
    api_secret = os.environ.get("MEXC_API_SECRET") or os.environ.get("MEXC_SECRET_KEY")
    client = None
    if args.live:
        if not (api_key and api_secret):
            print("[abort] --live requires MEXC_API_KEY and MEXC_API_SECRET (or MEXC_SECRET_KEY).")
            return 2
        client = MexcContractClient(api_key, api_secret)

    print(f"{'LIVE' if args.live else 'PREVIEW'}: {len(picks)} pick(s) from {args.csv} "
          f"rated {args.ratings} | margin ${args.margin} | up to {args.leverage}x | {args.side}\n")

    for _, r in picks.iterrows():
        sym = r["mexc_symbol"]
        base = r.get("base", sym.replace("_USDC", ""))
        price, csize, minv, vunit, maxlev = spec(sym)
        lev = min(args.leverage, maxlev)
        try:
            vol = calculate_order_quantity(args.margin, lev, price, csize, minv, vunit)
        except ValueError as e:
            print(f"[skip] {sym}: {e}")
            continue
        order = {
            "symbol": sym, "vol": vol, "side": _SIDE_OPEN[args.side],
            "type": _ORDER_TYPE_MARKET, "openType": 1, "leverage": lev,
        }
        notional = vol * csize * price
        print(f"{sym}  rating={r['rating']}  price={price}  lev={lev}x")
        print(f"  vol={vol} contracts  (~{vol * csize:g} {base}, ~${notional:,.0f} notional on ${args.margin} margin)")
        print(f"  payload: {json.dumps(order)}")
        if args.live:
            pt = _POSITION_TYPE[args.side]
            print(f"  [leverage] {json.dumps(client.set_leverage(sym, lev, 1, pt))}")
            print(f"  [order]    {json.dumps(client.submit_order(order))}")
        print()

    if not args.live:
        print("PREVIEW only — no order was sent. Re-run with --live (and MEXC_API_KEY/SECRET) to submit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
