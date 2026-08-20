#!/usr/bin/env python3
"""Screen MEXC perpetual contracts for TradingView BUY / STRONG_BUY.

Universe : live MEXC perps from contract/detail, filtered by quote
           (--quote USDC | USDT | all). The EXECUTABLE set.
Rating   : TradingView technical summary on the UNDERLYING coin, resolved via a
           liquid-exchange fallback ladder (venue-native MEXC:<base><quote>.P
           first, then Binance/Bybit USDT, then *USD) — deepest data wins.
Output   : tv_scan_mexc_<quote>_<tf>.csv joining each contract's spec
           (contractSize, maxLeverage, last) to its rating, STRONG_BUY flagged.

  python3 screen_mexc_usdc.py --intervals 4h                 # USDC only (default)
  python3 screen_mexc_usdc.py --intervals 4h 1d --quote all  # every MEXC perp
  python3 screen_mexc_usdc.py --intervals 4h --quote USDT --limit 25
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


def mexc_get(path: str, retries: int = 3, timeout: int = 25) -> dict:
    """GET + JSON-decode with retry/backoff — the /detail and /ticker fetches are
    large and occasionally slow; one transient timeout shouldn't kill a long scan.
    """
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(path, headers={"User-Agent": "screener/usdc"})
            return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise last


def usdc_universe(quote: str = "USDC") -> List[dict]:
    """Live MEXC perp universe with executable spec. quote='USDC'|'USDT'|'all'.

    Default 'USDC' preserves the original behavior, so screen_transitions / the
    cloud routine (which call usdc_universe() with no args) stay unchanged.
    """
    detail = mexc_get(f"{MEXC}/detail").get("data", [])
    tick = {r["symbol"]: r for r in mexc_get(f"{MEXC}/ticker").get("data", [])}
    rows = []
    for r in detail:
        sym = str(r.get("symbol", ""))
        if "_" not in sym:
            continue
        base, _, q = sym.rpartition("_")          # AVAX_USDC -> ('AVAX','_','USDC')
        if quote != "all" and q != quote:
            continue
        rows.append({
            "mexc_symbol": sym,
            "base": base,
            "quote": q,
            "contractSize": r.get("contractSize"),
            "maxLeverage": r.get("maxLeverage"),
            "last": tick.get(sym, {}).get("lastPrice"),
        })
    return rows


def tv_candidates(base: str, native_quote: str = "USDC") -> List[Tuple[str, str]]:
    """(exchange, symbol) ladder: venue-native perp first, then liquid proxies."""
    s = base.upper().replace("_", "").replace("-", "").replace(".", "")
    nq = native_quote.upper()
    ladder = [
        ("MEXC", f"{s}{nq}.P"),    # venue-native perp in its own quote (best match)
        ("MEXC", f"{s}USDT.P"),    # MEXC USDT perp
        ("BINANCE", f"{s}USDT"),   # deepest TV data
        ("BYBIT", f"{s}USDT"),
        ("COINBASE", f"{s}USD"),
        ("KRAKEN", f"{s}USD"),
    ]
    seen, out = set(), []
    for pair in ladder:
        if pair not in seen:
            seen.add(pair)
            out.append(pair)
    return out


class RateLimited(RuntimeError):
    """TradingView is refusing requests, as distinct from a symbol simply not being listed."""


# LADDER CIRCUIT BREAKER.
#
# tv_rating walks a 6-deep venue ladder and, before 2026-08-20, treated every failure the
# same way. Those failures mean two opposite things: "BASE is not listed on Kraken" is a
# normal miss that should advance to the next rung, while an HTTP 429 is a global stop.
# Conflating them means that under throttling EVERY rung of EVERY symbol raises, so a
# 78-symbol scan issues 78 x 6 = 468 requests instead of 78 — request volume rises 6x
# precisely when the limiter is punishing attempt count. Measured on 2026-08-20 a throttled
# 4h run took 26 minutes against the ~4 the schedule assumes, and coverage decayed 45 -> 17
# -> 10 -> 0 across successive runs because each attempt deepened the penalty for the next.
#
# A symbol genuinely absent from all six venues is rare and isolated. K of them in a row is
# not a property of the universe, it is the endpoint refusing everyone. Stop there: walking
# the remaining ladders cannot succeed and actively makes recovery slower.
#
# This is a circuit breaker, not a retry. There is deliberately no backoff-and-continue —
# per the operator note in com.aurascoper.mexc-4h-transitions.plist, "slowing down after
# tripping the limiter does not help; only waiting does."
LADDER_BREAKER_K = 5
_consecutive_exhausted = 0


def tv_rating(base: str, interval, sleep: float, native_quote: str = "USDC") -> dict:
    """Return the first resolvable TradingView summary across the ladder.

    Raises RateLimited once LADDER_BREAKER_K symbols in a row exhaust every rung.
    """
    global _consecutive_exhausted
    for ex, sym in tv_candidates(base, native_quote):
        try:
            h = TA_Handler(symbol=sym, exchange=ex, screener="crypto", interval=interval)
            summ = h.get_analysis().summary  # RECOMMENDATION, BUY, SELL, NEUTRAL
            _consecutive_exhausted = 0
            return {
                "rating": summ.get("RECOMMENDATION"),
                "tv_source": f"{ex}:{sym}",
                "tv_buy": summ.get("BUY"),
                "tv_sell": summ.get("SELL"),
                "tv_neutral": summ.get("NEUTRAL"),
            }
        except Exception:
            time.sleep(sleep)
    _consecutive_exhausted += 1
    if _consecutive_exhausted >= LADDER_BREAKER_K:
        raise RateLimited(
            f"{_consecutive_exhausted} consecutive symbols exhausted all "
            f"{len(tv_candidates(base, native_quote))} ladder rungs (last: {base}) — the "
            f"endpoint is refusing requests. Aborting rather than issuing the rest of the "
            f"scan's requests into a limiter that penalises attempt count.")
    return {"rating": None, "tv_source": None, "tv_buy": None, "tv_sell": None, "tv_neutral": None}


def scan(intervals: List[str], sleep: float, limit: Optional[int], quote: str = "USDC") -> None:
    universe = usdc_universe(quote)
    if limit:
        universe = universe[:limit]
    label = quote.lower()
    print(f"MEXC {quote} perp universe: {len(universe)} contracts", flush=True)

    for tf in intervals:
        iv = INTERVAL_MAP[tf]
        rows = []
        for i, u in enumerate(universe, 1):
            r = tv_rating(u["base"], iv, sleep, u.get("quote", "USDC"))
            rows.append({**u, "timeframe": tf, **r})
            if i % 25 == 0:
                print(f"[{tf}] {i}/{len(universe)} scanned...", flush=True)
            time.sleep(sleep)

        df = pd.DataFrame(rows)
        # Rank: STRONG_BUY above BUY, then by oscillator BUY count.
        order = {"STRONG_BUY": 0, "BUY": 1}
        df["_rk"] = df["rating"].map(order).fillna(9)
        df = df.sort_values(["_rk", "tv_buy"], ascending=[True, False]).drop(columns="_rk")

        out_all = f"tv_scan_mexc_{label}_{tf}_all.csv"
        df.to_csv(out_all, index=False)

        buys = df[df["rating"].isin(["BUY", "STRONG_BUY"])]
        out_buys = f"tv_scan_mexc_{label}_{tf}.csv"
        buys.to_csv(out_buys, index=False)

        n_strong = int((df["rating"] == "STRONG_BUY").sum())
        n_buy = int((df["rating"] == "BUY").sum())
        print(f"[{tf}] wrote {out_buys} ({len(buys)} buy/strong_buy) and {out_all} (full). "
              f"STRONG_BUY={n_strong}, BUY={n_buy}", flush=True)
        if n_strong:
            cols = ["mexc_symbol", "quote", "rating", "tv_source", "tv_buy", "tv_sell", "last", "maxLeverage"]
            print(f"\nSTRONG_BUY (MEXC {quote} perps):")
            print(df[df["rating"] == "STRONG_BUY"][cols].to_string(index=False))


def main() -> int:
    p = argparse.ArgumentParser(description="Screen MEXC perps for TradingView BUY/STRONG_BUY.")
    p.add_argument("--intervals", nargs="*", default=["4h"], choices=list(INTERVAL_MAP))
    p.add_argument("--sleep", type=float, default=0.15, help="Seconds between TV calls.")
    p.add_argument("--limit", type=int, default=None, help="Scan only first N contracts (debug).")
    p.add_argument("--quote", default="USDC", help="Quote filter: 'USDC' | 'USDT' | 'all'.")
    args = p.parse_args()
    try:
        scan(args.intervals, args.sleep, args.limit, args.quote)
        return 0
    except Exception as e:  # noqa: BLE001
        print(f"[error] {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
