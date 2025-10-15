
import argparse
import sys
import time
from typing import List, Tuple, Optional

import pandas as pd
from io import StringIO
import requests
from tradingview_ta import TA_Handler, Interval

# Defaults (can be overridden by CLI)
SLEEP_BETWEEN_CALLS = 0.15  # seconds to avoid rate limits
HTTP_TIMEOUT = 20

# Exchanges to try for US equities in rough order of likelihood
EQUITY_EXCHANGES = ["NASDAQ", "NYSE", "AMEX"]

UA_HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )
}

def _try_read_html(url: str) -> Optional[list]:
    try:
        resp = requests.get(url, headers=UA_HDRS, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        # Parse the HTML string via StringIO to avoid FutureWarning in pandas
        return pd.read_html(StringIO(resp.text))
    except Exception:
        return None

def fetch_sp500() -> List[str]:
    """Fetch S&P 500 tickers using robust fallbacks to avoid 403 and HTML changes."""
    # Try several Wikipedia variants first
    candidates = [
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies?oldformat=true",
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies?action=render",
    ]

    tables = None
    for url in candidates:
        tables = _try_read_html(url)
        if tables:
            break

    # Non-wiki fallback (community dataset; may lag a bit)
    if not tables:
        try:
            resp = requests.get(
                "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv",
                headers=UA_HDRS, timeout=HTTP_TIMEOUT
            )
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text))
            col = None
            for c in df.columns:
                if str(c).strip().lower() in ("symbol", "ticker", "tickers"):
                    col = c
                    break
            if col is None:
                raise RuntimeError("Could not find ticker column in fallback CSV.")
            syms = (
                df[col]
                .astype(str)
                .str.replace(".", "-", regex=False)
                .str.replace("/", "-", regex=False)
                .str.replace(" ", "", regex=False)
                .str.upper()
                .tolist()
            )
            return syms
        except Exception as e:
            raise RuntimeError("Failed to fetch S&P 500 list from all sources.") from e

    # Wikipedia path: first table generally holds the constituents
    df = None
    for t in tables:
        # Find a table that actually has a Symbol/Ticker column
        for c in t.columns:
            if str(c).strip().lower() in ("symbol", "ticker", "tickers"):
                df = t
                col = c
                break
        if df is not None:
            break
    if df is None:
        raise RuntimeError("Could not find a constituents table on Wikipedia page.")

    syms = (
        df[col]
        .astype(str)
        .str.replace(".", "-", regex=False)  # e.g., BRK.B -> BRK-B first
        .str.replace("/", "-", regex=False)
        .str.replace(" ", "", regex=False)
        .str.upper()
        .tolist()
    )
    return syms

def read_sp500_csv(csv_path: str) -> List[str]:
    """Read S&P 500 tickers from a CSV with a 'ticker' or 'symbol' column."""
    df = pd.read_csv(csv_path)
    ticker_col = None
    for c in df.columns:
        if "ticker" in str(c).lower() or "symbol" in str(c).lower():
            ticker_col = c
            break
    if ticker_col is None:
        raise RuntimeError("CSV must contain a 'ticker' or 'symbol' column for S&P 500.")
    syms = (
        df[ticker_col]
        .astype(str)
        .str.replace(".", "-", regex=False)
        .str.replace("/", "-", regex=False)
        .str.replace(" ", "", regex=False)
        .str.upper()
        .tolist()
    )
    return syms

def read_russell3000(csv_path: str) -> List[str]:
    """Read Russell 3000 tickers from a CSV that has a 'ticker' or 'symbol' column (any case)."""
    df = pd.read_csv(csv_path)
    # find ticker-like column
    ticker_col = None
    for c in df.columns:
        if "ticker" in str(c).lower() or "symbol" in str(c).lower():
            ticker_col = c
            break
    if ticker_col is None:
        raise RuntimeError("CSV must contain a 'ticker' or 'symbol' column.")
    syms = (
        df[ticker_col]
        .astype(str)
        .str.replace(".", "-", regex=False)
        .str.replace("/", "-", regex=False)
        .str.replace(" ", "", regex=False)
        .str.upper()
        .tolist()
    )
    return syms

def tv_symbol_candidates(ticker: str) -> List[Tuple[str, str]]:
    """
    Build TradingView (exchange, symbol) pairs to try.
    Handle class tickers by trying both '-' and '.' separators (e.g., BRK-B and BRK.B).
    """
    cands = {ticker}
    if "-" in ticker:
        cands.add(ticker.replace("-", "."))
    if "." in ticker:
        cands.add(ticker.replace(".", "-"))
    pairs = []
    for t in cands:
        for ex in EQUITY_EXCHANGES:
            pairs.append((ex, t))
    return pairs

def get_tv_rating(exchange: str, symbol: str, interval: str) -> Optional[str]:
    """
    Query TradingView technicals for a given (exchange, symbol, interval).
    Returns recommendation string or None if not available.
    """
    try:
        h = TA_Handler(symbol=symbol, exchange=exchange, screener="america", interval=interval)
        summary = h.get_analysis().summary  # dict with RECOMMENDATION, BUY/SELL counts, etc.
        return summary.get("RECOMMENDATION")
    except Exception:
        return None

def scan_universe(tickers: List[str], label: str, intervals: List[str]) -> None:
    """
    For each timeframe in `intervals`, write a CSV of rows rated BUY or STRONG_BUY.
    Output file: tv_scan_{label}_{tf}.csv
    """
    for tf in intervals:
        rows = []
        for i, t in enumerate(tickers, 1):
            for ex, sym in tv_symbol_candidates(t):
                rec = get_tv_rating(ex, sym, tf)
                if rec in ("BUY", "STRONG_BUY"):
                    rows.append({"ticker": t, "tv_exchange": ex, "tv_symbol": sym, "rating": rec, "timeframe": tf})
                    break
                elif rec is not None:
                    # got a valid response but not buy/strong_buy; stop trying alternatives
                    break
            if i % 25 == 0:
                print(f"[{label} {tf}] scanned {i}/{len(tickers)}...", flush=True)
            time.sleep(SLEEP_BETWEEN_CALLS)

        columns = ["ticker", "tv_exchange", "tv_symbol", "rating", "timeframe"]
        out = pd.DataFrame(rows, columns=columns)
        if not out.empty:
            out = out.sort_values(["rating", "ticker"], ascending=[True, True])
        out_file = f"tv_scan_{label}_{tf.replace(' ', '')}.csv"
        out.to_csv(out_file, index=False)
        print(f"Wrote {out_file} with {len(out)} tickers rated BUY/STRONG_BUY.")

def main():
    parser = argparse.ArgumentParser(description="Scan S&P 500 and Russell 3000 on TradingView for BUY/STRONG_BUY.")
    parser.add_argument("--sp500_csv", type=str, default=None,
                        help="Optional local CSV for S&P 500 tickers (must include 'ticker' or 'symbol').")
    parser.add_argument("--r3000_csv", type=str, default=None,
                        help="Path to Russell 3000 tickers CSV (must include 'ticker' or 'symbol' column).")
    parser.add_argument("--skip_sp500", action="store_true", help="Skip S&P 500 scan.")
    parser.add_argument("--only", type=str, default=None, choices=["sp500", "r3000"],
                        help="Scan only one universe.")
    parser.add_argument("--intervals", type=str, nargs="*", default=["4h", "1w"],
                        help="Intervals to scan (choices: 4h, 1w).")
    parser.add_argument("--sleep", type=float, default=0.15,
                        help="Seconds to sleep between TradingView calls (default 0.15).")
    parser.add_argument("--start", type=int, default=0,
                        help="Start index (inclusive) into the ticker list for slicing/resume.")
    parser.add_argument("--end", type=int, default=None,
                        help="End index (exclusive) into the ticker list for slicing/resume.")

    args = parser.parse_args()

    # Map human intervals to tradingview-ta constants
    interval_map = {
        "4h": Interval.INTERVAL_4_HOURS,
        "1w": Interval.INTERVAL_1_WEEK,
    }
    intervals = []
    for tf in args.intervals:
        if tf not in interval_map:
            print(f"Unsupported interval: {tf}. Choices are: {list(interval_map.keys())}", file=sys.stderr)
            sys.exit(1)
        intervals.append(interval_map[tf])

    # Override global sleep
    global SLEEP_BETWEEN_CALLS
    SLEEP_BETWEEN_CALLS = float(args.sleep)

    if args.only in (None, "sp500") and not args.skip_sp500:
        if args.sp500_csv:
            print(f"Loading S&P 500 from {args.sp500_csv}...")
            sp = read_sp500_csv(args.sp500_csv)
        else:
            print("Fetching S&P 500 constituents (robust mode)...")
            sp = fetch_sp500()

        # Apply slicing
        if args.end is not None:
            sp = sp[args.start:args.end]
        else:
            sp = sp[args.start:]
        print(f"Loaded {len(sp)} S&P 500 tickers (slice {args.start}:{args.end}).")
        scan_universe(sp, label="sp500", intervals=intervals)

    if args.only in (None, "r3000"):
        if not args.r3000_csv:
            print("Russell 3000 CSV not provided. Skipping Russell 3000. Use --r3000_csv path/to/file.csv", file=sys.stderr)
        else:
            r3k = read_russell3000(args.r3000_csv)
            if args.end is not None:
                r3k = r3k[args.start:args.end]
            else:
                r3k = r3k[args.start:]
            print(f"Loaded {len(r3k)} Russell 3000 tickers from CSV (slice {args.start}:{args.end}).")
            scan_universe(r3k, label="russell3000", intervals=intervals)

if __name__ == "__main__":
    main()
