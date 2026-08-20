# TWO jobs UNLOADED — cooling window active (2026-08-20)

`com.aurascoper.mexc-4h-transitions` (06:25Z) and `com.aurascoper.strongbuys-retry` (20:0xZ,
operator-approved) were both unloaded deliberately. Neither is broken. Both plist files are
intact; only the launchd registration was removed.

**`strongbuys-retry` FIRES LIVE ORDERS** — `strong_buys_cron.py --live`, margin $24.75,
leverage up to 50x, weekdays 08:35 local. It fired 4 picks on 2026-08-18. While it is
unloaded, no autonomous strong-buy entries occur. That is a real change to trading behaviour
and must not be left un-restored by accident.

## Why

**Both** jobs scan TradingView, so the window was never actually quiet. `strong_buys_cron.py`
shells out to `screen_mexc_usdc.py --intervals 4h` with **no `--sleep`**, so it uses the 0.12s
default — the documented poison — and it re-entered the window at 13:35Z on 2026-08-20, six
hours in. A test at 19:38Z, thirteen hours into the "window", was still fully throttled.

TradingView is rate-limiting. Coverage decayed 45/78 -> 17/78 -> 10/78 -> 0/78 across
successive runs because the limiter penalises *attempt count* with a lengthening penalty, and
every run was making it worse. A throttled scan verified at 06:22Z still exhausted all six
ladder rungs on the first five symbols, so the penalty had not decayed at all.

The plist's own note says it: "slowing down after tripping the limiter does not help; only
waiting does." The fix for "we keep getting 0" is to scan **less**.

## The window only actually STARTED at 2026-08-20T20:07Z

Everything before that was not a cooling window — `strongbuys-retry` was still scanning
unspaced every weekday, and it fired at 13:35Z. The 19:38Z test failing after "thirteen hours"
is therefore not evidence about how long the penalty lasts; it is evidence the window was
contaminated. **Count from 20:07Z, the first moment nothing was hitting the endpoint.**

## Resume procedure — do these in order

1. Wait. Hours, not minutes, measured from 20:07Z. Nothing else recovers the quota.
2. Run **one** scan by hand and read the coverage line:
   ```
   cd ~/Developer/screening && ./venv/bin/python screen_transitions.py --tf 4h --sleep 3.0
   ```
   - `live scan coverage: N/78` above 50% -> recovered, go to step 3.
   - `[throttled] ...` (exit 3) -> the window was too short. Wait longer.
     **Do not re-run to "refresh"** — that is what caused this.
3. Only after a clean scan, reload the schedule:
   ```
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.aurascoper.mexc-4h-transitions.plist
   ```
4. **Decide deliberately about `strongbuys-retry`** — it is a LIVE 50x order path, so
   reloading it is a trading decision, not cleanup:
   ```
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.aurascoper.strongbuys-retry.plist
   ```
   Before reloading, fix the cause: give its `screen_mexc_usdc.py` call an explicit
   `--sleep 3.0` (`strong_buys_cron.py:65`), or it re-poisons the limiter every weekday.
5. Delete this file.

## What is safe while it stays unloaded

Nothing trades on this. The Pi's strong-buy guard is fail-closed, so rotation stays paused and
stops plus the capital floor keep running. Held positions are covered by
`MEXC_DISCRETIONARY_SYMBOLS`. The cost of leaving it unloaded is only that orphan rotation
does not resume.

## Related changes made the same day

- `screen_mexc_usdc.py` — ladder circuit breaker; a throttled scan now aborts in ~2 min after
  ~30 requests instead of running 26 min and issuing ~468.
- `screen_transitions.py` — publication already fails closed below `MIN_LIVE_COVERAGE`, plus
  an explicit `RateLimited` path that exits 3 and leaves state intact.
