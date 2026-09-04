"""
Self-audit: inspects the saved state + trade logs and flags anything suspicious,
so the bot's behaviour can be checked without guessing from the dashboard.

Run on the server:
    ./venv/bin/python3 audit.py            (audits snipe, smart, hunt)
    ./venv/bin/python3 audit.py smart      (one mode)

Paste the output for review.
"""
import csv
import json
import os
import sys
import time

LAMPORTS = 1_000_000_000
TOKEN_UNITS = 1_000_000
START_CASH = float(os.environ.get("START_CASH_EUR", "100000"))
TRADE_EUR = float(os.environ.get("TRADE_EUR", "50"))
VOL = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip() or "."


def p(*a):
    print(*a)


def audit_mode(mode):
    sp = os.path.join(VOL, f"state_{mode}.json")
    tp = os.path.join(VOL, f"trades_{mode}.csv")
    p("=" * 56)
    p(f"  AUDIT — {mode}")
    p("=" * 56)
    if not os.path.exists(sp):
        p(f"  no state file ({sp}) — hasn't run/saved yet")
        return

    d = json.load(open(sp))
    str0 = d.get("strategies", [])
    p(f"  day {d.get('day_index')}  gen {d.get('generation')}  "
      f"strategies {len(str0)}")

    flags = []
    now = time.time()
    tot_open = 0
    for s in str0:
        cash = s.get("cash", 0)
        pos = s.get("positions", {})
        tot_open += len(pos)
        eq = cash + sum(x.get("value_eur", 0) for x in pos.values())

        # ---- sanity flags ----
        if eq > START_CASH * 50:
            flags.append(f"combo #{s.get('combo_id')}: equity €{eq:,.0f} "
                         f"— IMPLAUSIBLE (runaway-value bug?)")
        if cash < 0:
            flags.append(f"combo #{s.get('combo_id')}: negative cash €{cash:,.0f}")
        if len(pos) > 60:
            flags.append(f"combo #{s.get('combo_id')}: {len(pos)} open "
                         f"— above position cap")
        frozen = 0
        for x in pos.values():
            v = x.get("value_eur", 0)
            if v > TRADE_EUR * 500 + 1:
                flags.append(f"combo #{s.get('combo_id')}: a position worth "
                             f"€{v:,.0f} — over the 500x cap!")
            # a position stuck exactly at cost with no last_change movement
            if abs(v - x.get("cost_eur", TRADE_EUR)) < 1e-9:
                frozen += 1
        if frozen and len(pos):
            pctf = frozen / len(pos) * 100
            if pctf > 40:
                flags.append(f"combo #{s.get('combo_id')}: {frozen}/{len(pos)} "
                             f"positions ({pctf:.0f}%) still at entry value "
                             f"— possibly unpriced/frozen")

    # ---- trade log stats ----
    reasons = {}
    pnl_sum = 0.0
    n = wins = 0
    big = None
    if os.path.exists(tp):
        for r in csv.DictReader(open(tp)):
            try:
                v = float(r["pnl_eur"])
            except (KeyError, ValueError):
                continue
            n += 1
            pnl_sum += v
            if v > 0:
                wins += 1
            reasons[r.get("reason", "?")] = reasons.get(r.get("reason", "?"), 0) + 1
            if big is None or v > big[0]:
                big = (v, r.get("reason"), r.get("mint"))

    p(f"  open positions: {tot_open}")
    if n:
        p(f"  closed trades : {n}   win {wins/n*100:.0f}%   "
          f"realised P&L €{pnl_sum:,.2f}")
        p(f"  exit reasons  : " + ", ".join(f"{k}={v}" for k, v in
                                            sorted(reasons.items(), key=lambda x: -x[1])))
        if big:
            p(f"  biggest win   : €{big[0]:,.2f} ({big[1]})")
    else:
        p("  closed trades : none yet")

    p("-" * 56)
    if flags:
        p("  ⚠ FLAGS:")
        for f in flags:
            p("    - " + f)
    else:
        p("  ✓ no anomalies detected — looks healthy")
    p("")


def main():
    modes = [sys.argv[1]] if len(sys.argv) > 1 else ["snipe", "smart", "hunt"]
    p(f"\naudit @ {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}  "
      f"(START_CASH €{START_CASH:,.0f}, TRADE €{TRADE_EUR})\n")
    for m in modes:
        try:
            audit_mode(m)
        except Exception as e:
            p(f"  [audit error for {m}] {type(e).__name__}: {e}\n")


if __name__ == "__main__":
    main()
