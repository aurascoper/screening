# pip install tradingview-ta pandas requests
from tradingview_ta import TA_Handler, Interval, Exchange
import requests, pandas as pd, time

# 1) Get top 500 coins (example via CoinGecko)
cg = requests.get("https://api.coingecko.com/api/v3/coins/markets",
                  params={"vs_currency":"usd","order":"market_cap_desc","per_page":250,"page":1}).json()
cg += requests.get("https://api.coingecko.com/api/v3/coins/markets",
                   params={"vs_currency":"usd","order":"market_cap_desc","per_page":250,"page":2}).json()

# 2) Build candidate TradingView symbols (preferring Binance USDT pairs)
def tv_symbols(symbol):
    s = symbol.upper().replace(".", "").replace("-", "")
    return [("BINANCE", f"{s}USDT"),
            ("BYBIT",   f"{s}USDT"),
            ("COINBASE",f"{s}USD"),
            ("KRAKEN",  f"{s}USD")]

rows = []
for c in cg:
    sym = c["symbol"]
    ok = False
    for ex, tvsym in tv_symbols(sym):
        try:
            h = TA_Handler(symbol=tvsym, exchange=ex, screener="crypto", interval=Interval.INTERVAL_4_HOURS)
            r = h.get_analysis().summary  # dict with RECOMMENDATION
            rec = r.get("RECOMMENDATION","")
            if rec in ("BUY","STRONG_BUY"):
                rows.append({
                    "name": c["name"], "symbol": sym.upper(),
                    "tv_exchange": ex, "tv_symbol": tvsym,
                    "market_cap": c["market_cap"], "rating_4h": rec
                })
            ok = True
            break
        except Exception:
            time.sleep(0.2)
    # continue even if no TV pair found

df = pd.DataFrame(rows).sort_values("market_cap", ascending=False)
df.to_csv("tv_4h_buy_top500.csv", index=False)
print(df.head(20))

