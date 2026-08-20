#!/usr/bin/env python3
"""Detect RATING TRANSITIONS in the MEXC USDC perp universe across runs.

A single screen is a snapshot ("WLD is STRONG_BUY now"); it cannot see a change
("WLD *became* STRONG_BUY"). This persists each run's rating-per-symbol and
diffs the new scan against the stored one, surfacing upgrades/downgrades and —
the literal ask — BUY -> STRONG_BUY crossings, written as execution triggers.

State : state/mexc_usdc_<tf>.json  (symbol -> {rating, ts})
Output: transitions_mexc_usdc_<tf>.csv (all moves) and
        triggers_mexc_usdc_<tf>.csv    (rows that upgraded INTO STRONG_BUY,
                                         schema-compatible with execute_strong_buys.py)

  python3 screen_transitions.py --tf 1d                       # live scan + diff
  python3 screen_transitions.py --tf 1d --from-csv tv_scan_mexc_usdc_1d_all.csv
  python3 screen_transitions.py --tf 1d --selftest            # pure-logic unit tests
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from screen_mexc_usdc import usdc_universe, tv_rating, INTERVAL_MAP, RateLimited

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "state"
RANK = {"STRONG_SELL": -2, "SELL": -1, "NEUTRAL": 0, "BUY": 1, "STRONG_BUY": 2}
MIN_LIVE_COVERAGE = float(os.environ.get("MEXC_STRONGBUY_MIN_COVERAGE", "0.5"))


# ── pure core ────────────────────────────────────────────────────────────────
def classify(prev: Optional[str], curr: Optional[str]) -> Tuple[str, bool, bool]:
    """(direction, into_strong_buy, strict_buy_to_strong_buy). Pure, no I/O.

    direction: NEW | GONE | UP | DOWN | FLAT
    into_strong_buy        : curr == STRONG_BUY and prev != STRONG_BUY (any upgrade in)
    strict_buy_to_strong_buy: prev == BUY and curr == STRONG_BUY (the literal ask)
    """
    if prev is None:
        return ("NEW", curr == "STRONG_BUY", False)
    if curr is None:
        return ("GONE", False, False)
    delta = RANK.get(curr, 0) - RANK.get(prev, 0)
    direction = "UP" if delta > 0 else "DOWN" if delta < 0 else "FLAT"
    into_sb = curr == "STRONG_BUY" and prev != "STRONG_BUY"
    strict = prev == "BUY" and curr == "STRONG_BUY"
    return (direction, into_sb, strict)


def _selftest() -> int:
    cases = [
        (("BUY", "STRONG_BUY"), ("UP", True, True)),
        (("NEUTRAL", "STRONG_BUY"), ("UP", True, False)),
        (("STRONG_BUY", "STRONG_BUY"), ("FLAT", False, False)),
        (("STRONG_BUY", "BUY"), ("DOWN", False, False)),
        (("BUY", "BUY"), ("FLAT", False, False)),
        ((None, "STRONG_BUY"), ("NEW", True, False)),
        (("BUY", None), ("GONE", False, False)),
        (("SELL", "BUY"), ("UP", False, False)),
    ]
    ok = True
    for (prev, curr), want in cases:
        got = classify(prev, curr)
        flag = "ok " if got == want else "FAIL"
        if got != want:
            ok = False
        print(f"  [{flag}] {prev!s:>11} -> {curr!s:<11} = {got}")
    print("selftest:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


# ── I/O edges ────────────────────────────────────────────────────────────────
def current_from_csv(path: str) -> Dict[str, dict]:
    df = pd.read_csv(path)
    out = {}
    for _, r in df.iterrows():
        out[r["mexc_symbol"]] = {
            "rating": r.get("rating"),
            "base": r.get("base"),
            "contractSize": r.get("contractSize"),
            "maxLeverage": r.get("maxLeverage"),
            "last": r.get("last"),
        }
    return out


def current_from_live(tf: str, sleep: float, quote: str = "USDC") -> Dict[str, dict]:
    iv = INTERVAL_MAP[tf]
    out = {}
    uni = usdc_universe(quote)
    print(f"live scan: {len(uni)} {quote} contracts on {tf}", flush=True)
    for i, u in enumerate(uni, 1):
        r = tv_rating(u["base"], iv, sleep, u.get("quote", "USDC"))
        out[u["mexc_symbol"]] = {
            "rating": r["rating"], "base": u["base"], "quote": u.get("quote"),
            "contractSize": u["contractSize"], "maxLeverage": u["maxLeverage"], "last": u["last"],
        }
        if i % 50 == 0:
            print(f"  {i}/{len(uni)}...", flush=True)
        time.sleep(sleep)
    return out


def load_state(tf: str, label: str = "usdc") -> Dict[str, str]:
    f = STATE_DIR / f"mexc_{label}_{tf}.json"
    if not f.exists():
        return {}
    return json.loads(f.read_text()).get("ratings", {})


def state_is_live(tf: str, label: str = "usdc") -> bool:
    """True only if the stored state was written from a LIVE TradingView scan.

    Added 2026-08-19. A cloud run whose TradingView endpoint was unreachable fell back to
    a committed snapshot, wrote it as state, and the next diff reported 60 days of drift as
    4h transitions. Live spot-checks disagreed with that state on ~70% of reachable symbols
    (BTC and SOL were reported as downgrades out of STRONG_BUY while both read STRONG_BUY
    live). Diffing against a baseline of unknown provenance manufactures signal.
    """
    f = STATE_DIR / f"mexc_{label}_{tf}.json"
    if not f.exists():
        return False
    try:
        return json.loads(f.read_text()).get("source") == "live"
    except (OSError, ValueError):
        return False


def save_state(tf: str, current: Dict[str, dict], now: float, label: str = "usdc",
               source: str = "live") -> None:
    STATE_DIR.mkdir(exist_ok=True)
    f = STATE_DIR / f"mexc_{label}_{tf}.json"
    ratings = {sym: {"rating": c["rating"], "ts": now} for sym, c in current.items()}
    # `source` is load-bearing: state_is_live() gates transition reporting on it.
    f.write_text(json.dumps(
        {"updated": now, "source": source, "ratings": ratings}, indent=2))


def run(tf: str, from_csv: Optional[str], sleep: float, quote: str = "USDC",
        allow_stale_baseline: bool = False) -> int:
    now = time.time()
    label = quote.lower()
    live_scan = from_csv is None
    current = current_from_csv(from_csv) if from_csv else current_from_live(tf, sleep, quote)
    if live_scan:
        resolved = sum(1 for c in current.values() if c.get("rating") in RANK)
        coverage = resolved / len(current) if current else 0.0
        print(f"live scan coverage: {resolved}/{len(current)} = {coverage:.1%}", flush=True)
        if not current or coverage < MIN_LIVE_COVERAGE:
            print(f"[refused] live scan incomplete: only {resolved}/{len(current)} ratings "
                  f"resolved ({coverage:.0%} < {MIN_LIVE_COVERAGE:.0%}); previous state "
                  f"left intact", file=sys.stderr)
            return 2
    prev = load_state(tf, label)
    first_run = not prev

    # Refuse to report transitions against a baseline that was never written live.
    if prev and not state_is_live(tf, label) and not allow_stale_baseline:
        print(f"[refused] state/mexc_{label}_{tf}.json has no live provenance "
              f"(source != 'live'). Diffing against it manufactures transitions: on "
              f"2026-08-19 that reported a 60-day drift as a 4h scan. Run once from a "
              f"machine that can reach TradingView to establish a live baseline, or pass "
              f"--allow-stale-baseline to override deliberately.", file=sys.stderr)
        if live_scan:
            save_state(tf, current, now, label, source="live")
            print(f"[ok] wrote a LIVE baseline from this scan "
                  f"({len(current)} symbols) — the next run will diff cleanly.")
            return 0
        return 2

    moves = []
    for sym, c in current.items():
        p_rating = prev.get(sym, {}).get("rating") if isinstance(prev.get(sym), dict) else prev.get(sym)
        direction, into_sb, strict = classify(p_rating, c["rating"])
        if direction in ("FLAT",):
            continue
        moves.append({
            "mexc_symbol": sym, "base": c["base"], "quote": c.get("quote"),
            "prev_rating": p_rating, "rating": c["rating"],
            "direction": direction, "into_strong_buy": into_sb, "buy_to_strong_buy": strict,
            "contractSize": c["contractSize"], "maxLeverage": c["maxLeverage"], "last": c["last"],
        })

    save_state(tf, current, now, label)

    if first_run:
        print(f"BASELINE captured for {tf}: {len(current)} symbols. "
              f"No prior state to diff — re-run later to see transitions.")
        return 0

    mv = pd.DataFrame(moves)
    if mv.empty:
        print(f"[{tf}] no rating transitions since last run.")
        return 0

    order = {"UP": 0, "NEW": 1, "DOWN": 2, "GONE": 3}
    mv["_o"] = mv["direction"].map(order).fillna(9)
    mv = mv.sort_values(["into_strong_buy", "_o"], ascending=[False, True]).drop(columns="_o")
    mv.to_csv(f"transitions_mexc_{label}_{tf}.csv", index=False)

    triggers = mv[mv["into_strong_buy"]].copy()
    if not triggers.empty:
        triggers.to_csv(f"triggers_mexc_{label}_{tf}.csv", index=False)

    ups = mv[mv["direction"] == "UP"]
    downs = mv[mv["direction"] == "DOWN"]
    print(f"\n[{tf}] {len(mv)} transition(s): {len(ups)} up, {len(downs)} down, "
          f"{len(triggers)} into STRONG_BUY.")
    if not triggers.empty:
        print("\n*** UPGRADES INTO STRONG_BUY (execution triggers) ***")
        cols = ["mexc_symbol", "prev_rating", "rating", "buy_to_strong_buy", "last", "maxLeverage"]
        print(triggers[cols].to_string(index=False))
        print(f"\n-> feed to executor:  ./venv/bin/python execute_strong_buys.py "
              f"--csv triggers_mexc_{label}_{tf}.csv --ratings STRONG_BUY")
    if not ups.empty:
        print("\nOther upgrades:")
        print(ups[ups["rating"] != "STRONG_BUY"][["mexc_symbol", "prev_rating", "rating"]].to_string(index=False))
    if not downs.empty:
        print("\nDowngrades (exit / avoid):")
        print(downs[["mexc_symbol", "prev_rating", "rating"]].to_string(index=False))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Detect MEXC USDC rating transitions across runs.")
    ap.add_argument("--tf", default="1d", choices=list(INTERVAL_MAP))
    ap.add_argument("--from-csv", default=None, help="Read current ratings from a *_all.csv instead of live TV.")
    ap.add_argument("--sleep", type=float, default=0.12)
    ap.add_argument("--quote", default="USDC", help="Quote filter: 'USDC' | 'USDT' | 'all' (full universe).")
    ap.add_argument("--allow-stale-baseline", action="store_true",
                    help="Diff against a baseline with no live provenance (not advised).")
    ap.add_argument("--selftest", action="store_true", help="Run the pure classify() unit tests and exit.")
    args = ap.parse_args()
    if args.selftest:
        return _selftest()
    try:
        return run(args.tf, args.from_csv, args.sleep, args.quote,
                   args.allow_stale_baseline)
    except RateLimited as e:
        # Distinct from a generic error: nothing is wrong with the code or the universe, the
        # endpoint is throttling. Exit before touching state so the previous ratings survive.
        # The remedy is TIME, not another run — re-running now deepens the penalty.
        print(f"[throttled] {e}", file=sys.stderr)
        print("[throttled] previous state left intact. Do NOT re-run to 'refresh': each "
              "attempt lengthens the penalty. Let the limiter decay, then run ONCE and read "
              "the coverage line before resuming the schedule.", file=sys.stderr)
        return 3
    except Exception as e:  # noqa: BLE001
        print(f"[error] {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
