# Screening 4 hour, weekly Standards and Poors 500, Russel 3K and top 500 crypto market caps on Trading View

Step 1: `python3 -m venv venv` then `source venv/bin/activate`

Step 2: `pip install tradingview-ta pandas requests lxml`

Step 3: for crypto, run `python3 top_500_4h.py` for SP500 `python3 tv_equity_scanner.py`

Step 4 (optional): the Russell 3K CSV in the repository is the top 1000 trading volumes, to screen buys: 

`python tv_equity_scanner.py --only r3000 --r3000_csv russell-3000-index-10-14-2025.csv --intervals 4h`
