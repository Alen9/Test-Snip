"""
Verifies the sell-retry price-lock fix.
========================================
Simulates many noisy price paths. For each one, runs BOTH the OLD buggy
retry logic (re-checks reason/price fresh on every retry — the loophole) and
the NEW fixed logic (locks reason/price at the first trigger) on the exact
same price path, so it's an apples-to-apples comparison.

Run:  python3 test_price_lock_fix.py

What to look for in the output: under OLD logic, a meaningful % of stop-loss
triggers get "rescued" into a take-profit close purely by waiting through
retries. Under NEW logic, that number should be exactly 0% by construction.
"""
import random

# --- same constants/formulas as the live app, for a faithful test ---
SELL_LAND_RATE = 0.65
SELL_LAND_RATE_CRASH = 0.35
SELL_RETRY_GIVEUP_SEC = 120
POLL_INTERVAL_SEC = 20
LAND_RATE_TIP_SENSITIVITY = 0.5
MAX_LAND_RATE = 0.85
TRADE_EUR = 50.0


def scaled_rate(base, tip_mult):
    r = base * (1 + LAND_RATE_TIP_SENSITIVITY * (tip_mult - 1))
    return max(0.02, min(r, MAX_LAND_RATE))


def sim_path(tp, sl, hold_sec, sigma, drift, seed):
    """A noisy multiplicative price path until tp/sl/timeout first triggers,
    then a bit further (so retries have room to play out)."""
    rng = random.Random(seed)
    ratio = 1.0
    t = 0
    path = [(t, ratio)]
    trigger = None
    extra_ticks = SELL_RETRY_GIVEUP_SEC // POLL_INTERVAL_SEC + 2
    while True:
        t += POLL_INTERVAL_SEC
        ratio *= 2.71828 ** rng.gauss(drift, sigma)
        path.append((t, ratio))
        if trigger is None:
            if ratio >= tp:
                trigger = ("tp", t, ratio)
            elif ratio <= (1 - sl):
                trigger = ("sl", t, ratio)
            elif t >= hold_sec:
                trigger = ("timeout", t, ratio)
        if trigger and t >= trigger[1] + extra_ticks * POLL_INTERVAL_SEC:
            break
        if t > 3000:  # safety cap
            if trigger is None:
                trigger = ("timeout", t, ratio)
            break
    return path, trigger


def run_old(path, trigger, tp, sl, tip_mult, seed):
    """OLD buggy logic: reason/price re-evaluated fresh on every retry."""
    rng = random.Random(seed + 999)
    trig_reason, trig_t, _ = trigger
    for t, ratio in path:
        if t < trig_t:
            continue
        reason = ("tp" if ratio >= tp else "sl" if ratio <= (1 - sl) else trig_reason)
        land = scaled_rate(SELL_LAND_RATE_CRASH if reason == "sl" else SELL_LAND_RATE, tip_mult)
        if rng.random() <= land or (t - trig_t) >= SELL_RETRY_GIVEUP_SEC:
            return reason, ratio
    return trig_reason, path[-1][1]


def run_new(path, trigger, tip_mult, seed):
    """NEW fixed logic: reason/price locked at first trigger."""
    rng = random.Random(seed + 999)
    trig_reason, trig_t, trig_ratio = trigger
    for t, ratio in path:
        if t < trig_t:
            continue
        land = scaled_rate(SELL_LAND_RATE_CRASH if trig_reason == "sl" else SELL_LAND_RATE, tip_mult)
        if rng.random() <= land or (t - trig_t) >= SELL_RETRY_GIVEUP_SEC:
            return trig_reason, trig_ratio      # always the ORIGINAL price/reason
    return trig_reason, trig_ratio


def trial(tp, sl, hold_sec, sigma, drift, tip_mult, seed):
    path, trigger = sim_path(tp, sl, hold_sec, sigma, drift, seed)
    old_reason, old_ratio = run_old(path, trigger, tp, sl, tip_mult, seed)
    new_reason, new_ratio = run_new(path, trigger, tip_mult, seed)
    return trigger[0], old_reason, old_ratio, new_reason, new_ratio


def summarize(label, tip_mult, n=20000, tp=2.9, sl=0.35, hold_sec=200,
             sigma=0.35, drift=-0.02):
    rescued = 0        # old logic closed as something BETTER than the true trigger
    same_reason_drift = 0.0   # even without a full "rescue", old logic can land
    same_reason_n = 0         # at a less-bad price than the original trigger
    old_pnl = new_pnl = 0.0
    for i in range(n):
        trig, old_r, old_ratio, new_r, new_ratio = trial(
            tp, sl, hold_sec, sigma, drift, tip_mult, seed=i)
        if trig == "sl" and old_r != "sl":
            rescued += 1
        elif old_r == trig:
            same_reason_n += 1
            same_reason_drift += (old_ratio - new_ratio)   # >0 = old got a better price
        old_pnl += (old_ratio - 1.0) * TRADE_EUR
        new_pnl += (new_ratio - 1.0) * TRADE_EUR
    print(f"\n--- {label}  (tip_mult={tip_mult}, {n} trials) ---")
    print(f"  full 'rescues' — sl relabeled as something better (OLD, buggy) : "
          f"{rescued/n*100:.1f}%  <- the dramatic, rare case")
    if same_reason_n:
        print(f"  same-label price drift while retrying (OLD, buggy)  : "
              f"avg {same_reason_drift/same_reason_n*100:+.2f}% better price than the "
              f"true trigger  <- the common, subtler case")
    print(f"  total P&L under OLD (buggy) logic : {old_pnl:+,.2f} EUR")
    print(f"  total P&L under NEW (fixed) logic : {new_pnl:+,.2f} EUR")
    print(f"  inflation from the bug            : {old_pnl-new_pnl:+,.2f} EUR "
          f"({(old_pnl-new_pnl)/max(abs(new_pnl),1)*100:+.0f}% vs fixed)")


if __name__ == "__main__":
    print("Testing at baseline tip_mult=1.0 (no fee-bidding advantage)...")
    summarize("baseline tip", tip_mult=1.0)
    print("\nTesting at evolved tip_mult=2.9 (near max — what your leaders evolved to)...")
    summarize("high tip (near max)", tip_mult=2.9)
