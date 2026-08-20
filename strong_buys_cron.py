#!/usr/bin/env python3
"""Scheduled STRONG_BUY retry — regenerate the 4h USDC scan, then fire guarded.

Designed for launchd (com.aurascoper.strongbuys-retry, weekday 08:35 CT):
  1. DISARM sentinel: if ~/Developer/screening/DISARM exists, exit untouched.
  2. Regenerate tv_scan_mexc_usdc_4h.csv fresh (never fire a stale scan).
  3. Read positions + assets via the live_trading signed client. TRUST NO
     EMPTY LIST: success:true required on the raw envelope or we abort.
  4. Fire only STRONG_BUY picks with NO existing position (no stacking on
     repeat runs), margin $24.75 each at min(50x, contract max), CROSS.
  5. First submission failure aborts the rest — one probe, not 16 spam calls,
     while the API order path stays broken (generic code-500 as of 08-17).
  6. Everything appends to logs/strong_buys_cron.log (+ .jsonl for machines).

Default is PREVIEW (sizes and logs, sends nothing); the plist passes --live.
"""
from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LT = os.path.expanduser("~/Developer/live_trading")
sys.path.insert(0, LT)

DISARM = os.path.join(HERE, "DISARM")
CSV = os.path.join(HERE, "tv_scan_mexc_usdc_4h.csv")
LOG_DIR = os.path.join(HERE, "logs")
MARGIN = float(os.environ.get("SB_MARGIN_USDC", "24.75"))
LEVERAGE = int(os.environ.get("SB_LEVERAGE", "50"))
MIN_FREE_RATIO = 1.5   # available USDC must exceed planned margin by this factor


def log(msg: str) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    line = f"[{datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')}] {msg}"
    print(line)
    with open(os.path.join(LOG_DIR, "strong_buys_cron.log"), "a") as fh:
        fh.write(line + "\n")


def jlog(row: dict) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(os.path.join(LOG_DIR, "strong_buys_cron.jsonl"), "a") as fh:
        fh.write(json.dumps(row, separators=(",", ":")) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="Actually submit orders.")
    args = ap.parse_args()

    if os.path.exists(DISARM):
        log("DISARM sentinel present — exiting without action.")
        return 0

    log(f"run start | mode={'LIVE' if args.live else 'PREVIEW'} margin=${MARGIN} lev<={LEVERAGE}x")

    # 1. Fresh scan (never fire stale data).
    #
    # --sleep 3.0 IS LOAD-BEARING. Without it screen_mexc_usdc.py uses its 0.12s default,
    # which is the documented poison: TradingView rate-limits on attempt COUNT with a
    # lengthening penalty, and under throttling tv_rating walks all six ladder rungs for
    # every symbol, so a scan issues ~468 requests instead of 78. This job ran unspaced
    # every weekday and re-poisoned the limiter each time — on 2026-08-20 it fired at
    # 13:35Z, six hours into a cooling window, and a test seven hours after THAT was still
    # fully throttled. See com.aurascoper.mexc-4h-transitions.plist for the forensics.
    scan = subprocess.run(
        [os.path.join(HERE, "venv", "bin", "python3"),
         os.path.join(HERE, "screen_mexc_usdc.py"), "--intervals", "4h", "--sleep", "3.0"],
        cwd=HERE, capture_output=True, text=True, timeout=900,
    )
    if scan.returncode != 0:
        log(f"ABORT: scan failed rc={scan.returncode}: {scan.stderr[-300:]}")
        return 1
    log("scan regenerated: " + (scan.stdout.strip().splitlines()[-1] if scan.stdout.strip() else "?"))

    import pandas as pd
    df = pd.read_csv(CSV)
    picks = df[df["rating"] == "STRONG_BUY"]
    if picks.empty:
        log("no STRONG_BUY rows this run — done.")
        return 0

    # 2. Live account state, envelope-checked.
    from execution.mexc_client import make_client
    client = make_client()
    pos_env = client._signed_get("/api/v1/private/position/open_positions", {})
    assets_env = client.assets()
    if not (pos_env.get("success") and assets_env.get("success")):
        log("ABORT: positions/assets read not success:true — phantom-read guard.")
        return 1
    held = {p["symbol"] for p in (pos_env.get("data") or []) if float(p.get("holdVol", 0) or 0) > 0}
    usdc = next((r for r in (assets_env.get("data") or []) if r.get("currency") == "USDC"), {})
    available = float(usdc.get("availableBalance", 0) or 0)

    todo = [r for _, r in picks.iterrows() if r["mexc_symbol"] not in held]
    skipped = [r["mexc_symbol"] for _, r in picks.iterrows() if r["mexc_symbol"] in held]
    if skipped:
        log(f"skip (position already open): {', '.join(skipped)}")
    if not todo:
        log("all STRONG_BUY picks already held — done.")
        return 0
    planned = MARGIN * len(todo)
    if available < planned * MIN_FREE_RATIO:
        log(f"ABORT: available USDC ${available:.2f} < {MIN_FREE_RATIO}x planned margin ${planned:.2f}.")
        return 1
    log(f"{len(todo)} pick(s) to fire | planned margin ${planned:.2f} | available ${available:.2f}")

    # 3. Fire, aborting the batch on the first failed submission.
    fired = 0
    for r in todo:
        sym = r["mexc_symbol"]
        det = client.contract_detail(sym).get("data", {})
        csize = float(det.get("contractSize", 1) or 1)
        minv = float(det.get("minVol", 1) or 1)
        maxlev = int(det.get("maxLeverage", 1) or 1)
        price = float(client.ticker(sym).get("data", {}).get("lastPrice", 0) or 0)
        if price <= 0:
            log(f"[skip] {sym}: no price")
            continue
        lev = min(LEVERAGE, maxlev)
        vol = math.floor(MARGIN * lev * 0.999 / price / csize)
        if vol < minv:
            log(f"[skip] {sym}: sized vol {vol} < minVol {minv}")
            continue
        body = {"symbol": sym, "vol": int(vol), "side": 1, "type": 5, "openType": 2, "leverage": lev}
        if not args.live:
            log(f"[preview] {sym} vol={vol} lev={lev}x (~${vol * csize * price:,.0f} notional)")
            continue
        resp = client.submit_order(body)
        jlog({"ts": datetime.datetime.now(datetime.timezone.utc).isoformat(), "sym": sym,
              "body": body, "resp": resp})
        if resp.get("success"):
            fired += 1
            log(f"[FILLED] {sym} vol={vol} lev={lev}x orderId={((resp.get('data') or {}) or {}).get('orderId')}")
        else:
            log(f"[FAIL] {sym} code={resp.get('code')} {str(resp.get('message'))[:80]} — "
                "aborting remaining picks (order path still closed).")
            break

    log(f"run end | fired={fired}/{len(todo)}" + ("" if args.live else " (preview)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
