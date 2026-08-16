#!/usr/bin/env python3
"""Egress-free fallback for screen_transitions.py.

The cloud execution environment blocks all crypto APIs (MEXC, TradingView,
Binance, etc.). The workaround uses two data sources that ARE reachable:

1. state/mexc_usdc_{tf}.json  — committed symbol list (universe fallback)
2. data/mexc_usdc_{tf}_all.csv — committed by the GitHub Actions workflow
   fetch-mexc-ratings.yml, fetched via raw.githubusercontent.com

Workflow:
  - GitHub Actions runner (open egress) runs screen_mexc_usdc.py every 4h
    and commits data/mexc_usdc_{tf}_all.csv to the repo.
  - This module reads that CSV via raw.githubusercontent.com (reachable).
  - screen_transitions.current_from_live() calls current_from_github() when
    MEXC or TradingView is unreachable.
"""
from __future__ import annotations

import io
import json
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "state"
GITHUB_RAW = "https://raw.githubusercontent.com/aurascoper/screening/main"


def offline_universe(tf: str) -> List[dict]:
    """Read state/mexc_usdc_{tf}.json; return same shape as usdc_universe().

    contractSize, maxLeverage, last are None (not stored in state file).
    Used as a last-resort symbol list when the GitHub CSV is also unavailable.
    """
    state_path = STATE_DIR / f"mexc_usdc_{tf}.json"
    data = json.loads(state_path.read_text())
    rows = []
    for sym in data.get("ratings", {}):
        base = sym[: -len("_USDC")]
        rows.append({
            "mexc_symbol": sym,
            "base": base,
            "contractSize": None,
            "maxLeverage": None,
            "last": None,
        })
    return rows


def current_from_github(tf: str) -> Dict[str, dict]:
    """Fetch data/mexc_usdc_{tf}_all.csv committed by the GitHub Actions workflow.

    Returns same Dict[mexc_symbol, dict] shape as current_from_live() so that
    screen_transitions.run() works unchanged.

    Returns {} if the CSV has not been committed yet (first run before Actions fires).
    """
    # Prefer local file (if git pull already fetched it) over network
    local = ROOT / "data" / f"mexc_usdc_{tf}_all.csv"
    if local.exists():
        try:
            return _parse_csv(local.read_text())
        except Exception:
            pass  # fall through to remote fetch

    url = f"{GITHUB_RAW}/data/mexc_usdc_{tf}_all.csv"
    req = urllib.request.Request(url, headers={"User-Agent": "screener/offline"})
    try:
        content = urllib.request.urlopen(req, timeout=15).read().decode()
    except Exception as exc:
        print(f"[offline] GitHub CSV unavailable ({exc.__class__.__name__}) — no fresh ratings", flush=True)
        return {}

    try:
        result = _parse_csv(content)
        print(f"[offline] loaded {len(result)} symbols from GitHub ({tf})", flush=True)
        return result
    except Exception as exc:
        print(f"[offline] failed to parse GitHub CSV: {exc}", flush=True)
        return {}


def _parse_csv(text: str) -> Dict[str, dict]:
    """Parse a tv_scan_mexc_usdc_*_all.csv into the current_from_live() dict shape."""
    df = pd.read_csv(io.StringIO(text))
    out: Dict[str, dict] = {}
    for _, r in df.iterrows():
        sym = r.get("mexc_symbol")
        if pd.isna(sym):
            continue
        out[str(sym)] = {
            "rating":       r.get("rating") if not pd.isna(r.get("rating", float("nan"))) else None,
            "base":         r.get("base"),
            "contractSize": r.get("contractSize"),
            "maxLeverage":  r.get("maxLeverage"),
            "last":         r.get("last"),
        }
    return out
