#!/usr/bin/env python3
"""Screen the MEXC USDC-margined perpetual universe for TradingView BUY / STRONG_BUY.

Universe : every *_USDC contract live on MEXC futures (the EXECUTABLE set) —
           pulled from https://contract.mexc.com/api/v1/contract/detail.
Rating   : TradingView technical summary on the UNDERLYING coin, resolved via a
           liquid-exchange fallback ladder (the proxy pattern from top_500_4h.py),
           because that is where TradingView has the deepest data. MEXC-native
           perp symbols are tried first, then Binance/Bybit USDT, then *USD.
Output   : tv_scan_mexc_usdc_<tf>.csv joining each contract's spec
           (contractSize, maxLeverage, last) to its rating, STRONG_BUY flagged.

  python3 screen_mexc_usdc.py --intervals 4h
  python3 screen_mexc_usdc.py --intervals 4h 1d --limit 25   # quick subset
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from typing import List, Optional, Tuple

import pandas as pd
from tradingview_ta import TA_Handler, Interval

MEXC = "https://contract.mexc.com/api/v1/contract"
INTERVAL_MAP = {
    "1h": Interval.INTERVAL_1_HOUR,
    "4h": Interval.INTERVAL_4_HOURS,
    "1d": Interval.INTERVAL_1_DAY,
    "1w": Interval.INTERVAL_1_WEEK,
}


def mexc_get(path: str) -> dict:
    req = urllib.request.Request(path, headers={"User-Agent": "screener/usdc"})
    return json.loads(urllib.request.urlopen(req, timeout=15).read())


def usdc_universe() -> List[dict]:
    """Every live *_USDC perp with its executable spec."""
    detail = mexc_get(f"{MEXC}/detail").get("data", [])
    tick = {r["symbol"]: r for r in mexc_get(f"{MEXC}/ticker").get("data", [])}
    rows = []
    for r in detail:
        sym = str(r.get("symbol", ""))
        if not sym.endswith("_USDC"):
            continue
        rows.append({
            "mexc_symbol": sym,
            "base": sym[: -len("_USDC")],          # AVAX_USDC -> AVAX
            "contractSize": r.get("contractSize"),
            "maxLeverage": r.get("maxLeverage"),
            "last": tick.get(sym, {}).get("lastPrice"),
        })
    return rows


def tv_candidates(base: str) -> List[Tuple[str, str]]:
    """(exchange, symbol) ladder: venue-native perp first, then liquid proxies."""
    s = base.upper().replace("_", "").replace("-", "").replace(".", "")
    return [
        ("MEXC", f"{s}USDC.P"),   # venue-native USDC perp (best match if TV has it)
        ("MEXC", f"{s}USDT.P"),   # venue-native USDT perp
        ("BINANCE", f"{s}USDT"),  # deepest TV data
        ("BYBIT", f"{s}USDT"),
        ("COINBASE", f"{s}USD"),
        ("KRAKEN", f"{s}USD"),
    ]


def tv_rating(base: str, interval, sleep: float) -> dict:
    """Return the first resolvable TradingView summary across the ladder."""
    for ex, sym in tv_candidates(base):
        try:
            h = TA_Handler(symbol=sym, exchange=ex, screener="crypto", interval=interval)
            summ = h.get_analysis().summary  # RECOMMENDATION, BUY, SELL, NEUTRAL
            return {
                "rating": summ.get("RECOMMENDATION"),
                "tv_source": f"{ex}:{sym}",
                "tv_buy": summ.get("BUY"),
                "tv_sell": summ.get("SELL"),
                "tv_neutral": summ.get("NEUTRAL"),
            }
        except Exception:
            time.sleep(sleep)
    return {"rating": None, "tv_source": None, "tv_buy": None, "tv_sell": None, "tv_neutral": None}


def scan(intervals: List[str], sleep: float, limit: Optional[int]) -> None:
    universe = usdc_universe()
    if limit:
        universe = universe[:limit]
    print(f"MEXC USDC perp universe: {len(universe)} contracts", flush=True)

    for tf in intervals:
        iv = INTERVAL_MAP[tf]
        rows = []
        for i, u in enumerate(universe, 1):
            r = tv_rating(u["base"], iv, sleep)
            rows.append({**u, "timeframe": tf, **r})
            if i % 10 == 0:
                print(f"[{tf}] {i}/{len(universe)} scanned...", flush=True)
            time.sleep(sleep)

        df = pd.DataFrame(rows)
        # Rank: STRONG_BUY above BUY, then by oscillator BUY count.
        order = {"STRONG_BUY": 0, "BUY": 1}
        df["_rk"] = df["rating"].map(order).fillna(9)
        df = df.sort_values(["_rk", "tv_buy"], ascending=[True, False]).drop(columns="_rk")

        out_all = f"tv_scan_mexc_usdc_{tf}_all.csv"
        df.to_csv(out_all, index=False)

        buys = df[df["rating"].isin(["BUY", "STRONG_BUY"])]
        out_buys = f"tv_scan_mexc_usdc_{tf}.csv"
        buys.to_csv(out_buys, index=False)

        n_strong = int((df["rating"] == "STRONG_BUY").sum())
        n_buy = int((df["rating"] == "BUY").sum())
        print(f"[{tf}] wrote {out_buys} ({len(buys)} buy/strong_buy) and {out_all} (full). "
              f"STRONG_BUY={n_strong}, BUY={n_buy}", flush=True)
        if n_strong:
            cols = ["mexc_symbol", "rating", "tv_source", "tv_buy", "tv_sell", "last", "maxLeverage"]
            print("\nSTRONG_BUY (executable on MEXC USDC):")
            print(df[df["rating"] == "STRONG_BUY"][cols].to_string(index=False))


def main() -> int:
    p = argparse.ArgumentParser(description="Screen MEXC USDC perps for TradingView BUY/STRONG_BUY.")
    p.add_argument("--intervals", nargs="*", default=["4h"], choices=list(INTERVAL_MAP))
    p.add_argument("--sleep", type=float, default=0.15, help="Seconds between TV calls.")
    p.add_argument("--limit", type=int, default=None, help="Scan only first N contracts (debug).")
    args = p.parse_args()
    try:
        scan(args.intervals, args.sleep, args.limit)
        return 0
    except Exception as e:  # noqa: BLE001
        print(f"[error] {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
