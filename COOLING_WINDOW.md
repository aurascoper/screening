# 4h scan is UNLOADED — cooling window active (2026-08-20 ~06:25Z)

`com.aurascoper.mexc-4h-transitions` was unloaded deliberately. It is not broken.

## Why

TradingView is rate-limiting. Coverage decayed 45/78 -> 17/78 -> 10/78 -> 0/78 across
successive runs because the limiter penalises *attempt count* with a lengthening penalty, and
every run was making it worse. A throttled scan verified at 06:22Z still exhausted all six
ladder rungs on the first five symbols, so the penalty had not decayed at all.

The plist's own note says it: "slowing down after tripping the limiter does not help; only
waiting does." The fix for "we keep getting 0" is to scan **less**.

## Resume procedure — do these in order

1. Wait. Hours, not minutes. Nothing else recovers the quota.
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
4. Delete this file.

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
