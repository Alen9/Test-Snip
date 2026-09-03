"""
pump.fun evolving paper-trading desk — all-in-one
=================================================
ONE service. ONE PumpPortal connection feeds THREE evolving brains
(snipe / smart / hunt) that trade side by side, share the same dRPC price
reads, and show up on ONE dashboard with a league tab.

Why one service: PumpPortal allows only one websocket connection, and one
long-running process is far more stable on Railway than four. This design
respects both.

SIMULATION ONLY — no wallet, no keys, no real trades. It reads chain state
and moves imaginary euros.

Run:  pip install aiohttp ; python app.py ; open http://localhost:8080
"""

import asyncio
import base64
import csv
import json
import os
import random
import struct
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
RPC_HTTP = os.environ.get("RPC_HTTP", "").strip()      # dRPC (or any) Solana HTTP endpoint
PUMPPORTAL_WSS = os.environ.get("PUMPPORTAL_WSS", "wss://pumpportal.fun/api/data")
PORT = int(os.environ.get("PORT", "8080"))
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "")

# Which brains to run in this one process.
MODES = [m.strip().lower() for m in os.environ.get("MODES", "snipe,smart,hunt").split(",")
         if m.strip().lower() in ("snipe", "smart", "hunt")]
if not MODES:
    MODES = ["snipe"]

START_CASH_EUR = float(os.environ.get("START_CASH_EUR", "100000"))
TRADE_EUR = float(os.environ.get("TRADE_EUR", "50"))
FEE_BPS = 100

POOL_SIZE = int(os.environ.get("POOL_SIZE", "12"))
EVOLVE_INTERVAL_SEC = int(os.environ.get("EVOLVE_INTERVAL_SEC", "86400"))  # per day
MAX_POS_PER_STRAT = int(os.environ.get("MAX_POS_PER_STRAT", "40"))
POLL_INTERVAL_SEC = float(os.environ.get("POLL_INTERVAL_SEC", "20"))
HUNT_WATCH_MAX = int(os.environ.get("HUNT_WATCH_MAX", "20"))
HUNT_MAX_AGE = int(os.environ.get("HUNT_MAX_AGE", "1800"))

_VOL = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
STATE_DIR = _VOL if _VOL else "."
try:
    os.makedirs(STATE_DIR, exist_ok=True)
except Exception as _e:
    print(f"[init] {_e}", flush=True)

LAMPORTS = 1_000_000_000
TOKEN_UNITS = 1_000_000

_EXIT_GENES = {"tp": (1.2, 5.0), "sl": (0.20, 0.80), "hold": (30, 300)}
_MODE_GENES = {
    "snipe": {**_EXIT_GENES, "dev_max": (0.3, 25.0)},
    "smart": {**_EXIT_GENES, "dev_max": (0.3, 25.0), "dev_min": (0.0, 3.0),
              "top_hold_max": (20.0, 95.0)},
    "hunt":  {**_EXIT_GENES, "dev_max": (0.3, 25.0),
              "mom_pct": (5.0, 80.0), "mom_window": (10, 120)},
}
_INT_GENES = ("hold", "mom_window")


def now_hms():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def now_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def state_path(mode):
    return os.path.join(STATE_DIR, f"state_{mode}.json")


def csv_path(mode):
    return os.path.join(STATE_DIR, f"trades_{mode}.csv")


# ---------------------------------------------------------------------------
# Curve math
# ---------------------------------------------------------------------------
def parse_curve(raw):
    if len(raw) < 49:
        return None
    vt, vs, rt, rs, ts = struct.unpack_from("<QQQQQ", raw, 8)
    return {"vt": vt, "vs": vs, "complete": bool(raw[48])}


def buy_quote(c, sol_in_lamports):
    sol_in = sol_in_lamports * (10_000 - FEE_BPS) // 10_000
    if sol_in <= 0 or c["vs"] <= 0:
        return 0
    return c["vt"] * sol_in // (c["vs"] + sol_in)


def sell_quote(c, tokens):
    if tokens <= 0:
        return 0
    out = c["vs"] * tokens // (c["vt"] + tokens)
    return out * (10_000 - FEE_BPS) // 10_000


def spot_price(c):
    if not isinstance(c, dict) or c["vt"] == 0:
        return 0.0
    return (c["vs"] / LAMPORTS) / (c["vt"] / TOKEN_UNITS)


def parse_pp_token(d):
    mint = d.get("mint")
    curve = d.get("bondingCurveKey") or d.get("bonding_curve")
    if not mint or not curve:
        return None
    dev = d.get("solAmount")
    info = {"mint": mint, "curve": curve,
            "creator": d.get("traderPublicKey") or mint,
            "dev_buy_sol": float(dev) if dev is not None else None}
    vt, vs = d.get("vTokensInBondingCurve"), d.get("vSolInBondingCurve")
    try:
        if vt and vs:
            info["reserves"] = {"vt": int(float(vt) * TOKEN_UNITS),
                                "vs": int(float(vs) * LAMPORTS), "complete": False}
    except (TypeError, ValueError):
        pass
    return info


# ---------------------------------------------------------------------------
# Genes
# ---------------------------------------------------------------------------
def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def _round_gene(k, v):
    if k in _INT_GENES:
        return int(round(v))
    if k in ("tp", "sl"):
        return round(v, 2)
    return round(v, 1)


def rand_genome(gb):
    return {k: _round_gene(k, random.uniform(lo, hi)) for k, (lo, hi) in gb.items()}


def mutate(g, gb):
    ng = dict(g)
    for k, (lo, hi) in gb.items():
        if k in ng and random.random() < 0.5:
            ng[k] = _round_gene(k, clamp(ng[k] * random.uniform(0.7, 1.3), lo, hi))
    return ng


# ---------------------------------------------------------------------------
# Shared RPC (dRPC) — only for valuing open positions
# ---------------------------------------------------------------------------
async def rpc(session, method, params):
    if not RPC_HTTP:
        return None
    try:
        async with session.post(RPC_HTTP, json={"jsonrpc": "2.0", "id": 1,
                "method": method, "params": params},
                timeout=aiohttp.ClientTimeout(total=8)) as r:
            return (await r.json()).get("result")
    except Exception:
        return None


async def get_curve(session, curve):
    res = await rpc(session, "getAccountInfo",
                    [curve, {"encoding": "base64", "commitment": "processed"}])
    if res is None:
        return None
    if not res.get("value"):
        return "GONE"
    try:
        return parse_curve(base64.b64decode(res["value"]["data"][0]))
    except Exception:
        return None


async def get_top_holder_pct(session, mint):
    res = await rpc(session, "getTokenLargestAccounts", [mint, {"commitment": "processed"}])
    if not res or not res.get("value"):
        return None
    amts = sorted((int(a["amount"]) for a in res["value"]), reverse=True)
    if len(amts) < 2:
        return 0.0
    circ = sum(amts) - amts[0]
    return amts[1] / circ * 100 if circ > 0 else 0.0


# ---------------------------------------------------------------------------
@dataclass
class Strategy:
    id: int
    genome: dict
    cash: float
    positions: dict = field(default_factory=dict)
    realized: float = 0.0
    trades: int = 0
    wins: int = 0
    window_start_equity: float = 0.0
    combo_id: int = 0
    born_day: int = 0
    days_alive: int = 0
    days_won: int = 0
    cum_pnl: float = 0.0
    day_pnls: list = field(default_factory=list)


class Pool:
    def __init__(self, mode):
        self.mode = mode
        self.gb = _MODE_GENES[mode]
        self.sol_eur = 150.0
        self.strategies = []
        self.next_id = 1
        self.next_combo_id = 1
        self.generation = 1
        self.day_index = 0
        self.last_evolve = time.time()
        self.launches = deque(maxlen=40)
        self.pv = deque(maxlen=360)
        self.recent = deque(maxlen=30)
        self.ledger = deque(maxlen=90)
        self.best_ever = None
        self.watch = {}
        for _ in range(POOL_SIZE):
            self._new_strategy(rand_genome(self.gb))

    def _new_strategy(self, genome, born_day=0):
        st = Strategy(id=self.next_id, genome=genome, cash=START_CASH_EUR,
                      window_start_equity=START_CASH_EUR,
                      combo_id=self.next_combo_id, born_day=born_day)
        self.next_id += 1
        self.next_combo_id += 1
        self.strategies.append(st)
        return st

    def equity(self, st):
        return st.cash + sum(p["value_eur"] for p in st.positions.values())

    def best_equity(self):
        return max((self.equity(s) for s in self.strategies), default=START_CASH_EUR)

    # ---- entry (no RPC: price comes from the PumpPortal event) ----
    def on_new_token(self, info):
        info = dict(info)
        info["age"] = time.time()
        self.launches.appendleft(info)
        if self.mode == "hunt":
            w = {"curve": info["curve"], "first_seen": time.time(), "prices": [],
                 "dev_buy": info["dev_buy_sol"]}
            if info.get("reserves"):
                w["prices"].append((time.time(), spot_price(info["reserves"])))
            self.watch[info["mint"]] = w
            while len(self.watch) > HUNT_WATCH_MAX:
                oldest = min(self.watch, key=lambda m: self.watch[m]["first_seen"])
                self.watch.pop(oldest, None)
            return
        c1 = info.get("reserves")
        if not isinstance(c1, dict) or c1.get("complete"):
            return
        for st in self.strategies:
            self.try_enter(st, info, c1)

    def _enter(self, st, mint, curve, c):
        tokens = buy_quote(c, int((TRADE_EUR / self.sol_eur) * LAMPORTS))
        if tokens <= 0:
            return
        st.cash -= TRADE_EUR
        st.positions[mint] = {"mint": mint, "curve": curve, "tokens": tokens,
                              "cost_eur": TRADE_EUR, "entry_ts": time.time(),
                              "value_eur": TRADE_EUR}

    def try_enter(self, st, info, c1):
        mint, g = info["mint"], st.genome
        if mint in st.positions or st.cash < TRADE_EUR:
            return
        if len(st.positions) >= MAX_POS_PER_STRAT:
            return
        dev = info.get("dev_buy_sol")
        if dev is not None:
            if "dev_max" in g and dev > g["dev_max"]:
                return
            if "dev_min" in g and dev < g["dev_min"]:
                return
        if "top_hold_max" in g and info.get("top_hold") is not None:
            if info["top_hold"] > g["top_hold_max"]:
                return
        self._enter(st, mint, info["curve"], c1)

    def try_enter_hunt(self, st, mint, w, c):
        g = st.genome
        if mint in st.positions or st.cash < TRADE_EUR:
            return
        if len(st.positions) >= MAX_POS_PER_STRAT:
            return
        dev = w.get("dev_buy")
        if dev is not None and "dev_max" in g and dev > g["dev_max"]:
            return
        mom = self._momentum(w, g["mom_window"])
        if mom is None or mom < g["mom_pct"] / 100:
            return
        self._enter(st, mint, w["curve"], c)

    @staticmethod
    def _momentum(w, window_sec):
        prices = w["prices"]
        if len(prices) < 2:
            return None
        now_t, now_p = prices[-1]
        past = prices[0]
        for t, p in prices:
            if t <= now_t - window_sec:
                past = (t, p)
        return (now_p / past[1] - 1) if past[1] > 0 else None

    # ---- valuation + exits (uses curves read once by the shared tracker) ----
    def value_and_exit(self, curves):
        for st in self.strategies:
            g = st.genome
            for mint in list(st.positions):
                pos = st.positions[mint]
                c = curves.get(pos["curve"])
                if c == "GONE":
                    self.close(st, mint, 0.0, "rug")
                    continue
                if not isinstance(c, dict):
                    continue
                val = (sell_quote(c, pos["tokens"]) / LAMPORTS) * self.sol_eur
                pos["value_eur"] = val
                held = time.time() - pos["entry_ts"]
                ratio = val / pos["cost_eur"]
                reason = ("graduated" if c["complete"]
                          else "tp" if ratio >= g["tp"]
                          else "sl" if ratio <= (1 - g["sl"])
                          else "timeout" if held >= g["hold"] else None)
                if reason:
                    self.close(st, mint, val, reason)

    def hunt_check(self, curves, now):
        for mint in list(self.watch):
            w = self.watch[mint]
            if now - w["first_seen"] > HUNT_MAX_AGE:
                self.watch.pop(mint, None)
                continue
            c = curves.get(w["curve"])
            if c == "GONE" or (isinstance(c, dict) and c["complete"]):
                self.watch.pop(mint, None)
                continue
            if not isinstance(c, dict):
                continue
            w["prices"].append((now, spot_price(c)))
            w["prices"] = w["prices"][-40:]
            for st in self.strategies:
                self.try_enter_hunt(st, mint, w, c)

    def close(self, st, mint, proceeds, reason):
        pos = st.positions.pop(mint)
        pnl = proceeds - pos["cost_eur"]
        st.cash += proceeds
        st.realized += pnl
        st.trades += 1
        if pnl > 0:
            st.wins += 1
        self.recent.appendleft({"mint": mint, "id": st.id, "reason": reason,
                                "pnl": round(pnl, 2), "time": now_hms()})
        self.csv_append(st, pos, pnl, reason)

    def curves_needed(self):
        s = set()
        for st in self.strategies:
            for pos in st.positions.values():
                s.add(pos["curve"])
        for w in self.watch.values():
            s.add(w["curve"])
        return s

    def evolve(self):
        self.day_index += 1
        for st in self.strategies:
            day_score = self.equity(st) - st.window_start_equity
            st.day_pnls.append(round(day_score, 2))
            st.day_pnls = st.day_pnls[-60:]
            st.cum_pnl += day_score
        ranked = sorted(self.strategies, key=lambda s: s.day_pnls[-1], reverse=True)
        winner = ranked[0]
        winner.days_won += 1
        self.ledger.appendleft({"day": self.day_index, "date": now_date(),
                                "combo_id": winner.combo_id, "genome": dict(winner.genome),
                                "day_pnl": winner.day_pnls[-1],
                                "cum_pnl": round(winner.cum_pnl, 2),
                                "days_alive": winner.days_alive + 1})
        keep = ranked[:max(1, len(ranked) // 2)]
        best_cum = max(self.strategies, key=lambda s: s.cum_pnl)
        if best_cum not in keep:
            keep.append(best_cum)
        keep_ids = {id(s) for s in keep}
        parents = [s.genome for s in keep]
        for st in self.strategies:
            if id(st) in keep_ids:
                st.days_alive += 1
            else:
                st.genome = mutate(random.choice(parents), self.gb)
                st.cash = START_CASH_EUR
                st.positions = {}
                st.realized = st.trades = st.wins = 0
                st.combo_id = self.next_combo_id
                self.next_combo_id += 1
                st.born_day = self.day_index
                st.days_alive = st.days_won = 0
                st.cum_pnl = 0.0
                st.day_pnls = []
                st.id = self.next_id
                self.next_id += 1
        for st in self.strategies:
            st.window_start_equity = self.equity(st)
        champ = max(self.strategies, key=lambda s: s.cum_pnl)
        if not self.best_ever or champ.cum_pnl > self.best_ever["cum_pnl"]:
            self.best_ever = {"combo_id": champ.combo_id, "genome": dict(champ.genome),
                              "cum_pnl": round(champ.cum_pnl, 2),
                              "days_alive": champ.days_alive, "days_won": champ.days_won}
        self.generation += 1
        self.last_evolve = time.time()
        print(f"[{self.mode} evolve] day {self.day_index}: combo #{winner.combo_id} "
              f"day {winner.day_pnls[-1]:+.2f}€ cum {winner.cum_pnl:+.2f}€", flush=True)

    def snapshot(self):
        board = []
        for st in self.strategies:
            eq = self.equity(st)
            last = st.day_pnls[-1] if st.day_pnls else 0
            board.append({"id": st.id, "combo": st.combo_id, "genome": st.genome,
                          "equity": round(eq, 2),
                          "realized": round(st.realized, 2), "trades": st.trades,
                          "winrate": round(st.wins / st.trades * 100) if st.trades else 0,
                          "open": len(st.positions), "days": st.days_alive,
                          "won": st.days_won, "cum": round(st.cum_pnl, 2),
                          "rising": st.days_alive <= 6 and st.cum_pnl > 0 and last > 0,
                          "holds": [{"mint": p["mint"],
                                     "pnl": round((p["value_eur"] / p["cost_eur"] - 1) * 100)}
                                    for p in st.positions.values()]})
        board.sort(key=lambda x: x["equity"], reverse=True)
        t = time.time()
        return {"mode": self.mode, "generation": self.generation,
                "pool": len(self.strategies), "day_index": self.day_index,
                "sol_eur": round(self.sol_eur, 2), "trade_eur": TRADE_EUR,
                "start_cash": START_CASH_EUR,
                "evolve_in": max(0, int(EVOLVE_INTERVAL_SEC - (t - self.last_evolve))),
                "champion": board[0] if board else None, "board": board,
                "best_ever": self.best_ever, "ledger": list(self.ledger)[:30],
                "total_realized": round(sum(s.realized for s in self.strategies), 2),
                "launches": [{"mint": l["mint"], "dev_buy": l["dev_buy_sol"],
                              "age_sec": int(t - l["age"])} for l in self.launches],
                "recent": list(self.recent), "pv": list(self.pv)}

    # ---- persistence (per mode) ----
    def save(self):
        data = {"generation": self.generation, "last_evolve": self.last_evolve,
                "next_id": self.next_id, "day_index": self.day_index,
                "next_combo_id": self.next_combo_id, "ledger": list(self.ledger),
                "best_ever": self.best_ever, "strategies": [
                    {"id": s.id, "genome": s.genome, "cash": s.cash,
                     "positions": s.positions, "realized": s.realized,
                     "trades": s.trades, "wins": s.wins,
                     "window_start_equity": s.window_start_equity,
                     "combo_id": s.combo_id, "born_day": s.born_day,
                     "days_alive": s.days_alive, "days_won": s.days_won,
                     "cum_pnl": s.cum_pnl, "day_pnls": s.day_pnls}
                    for s in self.strategies]}
        try:
            p = state_path(self.mode)
            tmp = p + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, p)
        except Exception as e:
            print(f"[{self.mode} save] {e}", flush=True)

    def load(self):
        p = state_path(self.mode)
        if not os.path.exists(p):
            return
        try:
            with open(p) as f:
                data = json.load(f)
            self.generation = data.get("generation", 1)
            self.last_evolve = data.get("last_evolve", time.time())
            self.next_id = data.get("next_id", 1)
            self.day_index = data.get("day_index", 0)
            self.next_combo_id = data.get("next_combo_id", 1)
            self.ledger = deque(data.get("ledger", []), maxlen=90)
            self.best_ever = data.get("best_ever")
            self.strategies = [
                Strategy(id=d["id"], genome=d["genome"], cash=d["cash"],
                         positions=d.get("positions", {}), realized=d.get("realized", 0.0),
                         trades=d.get("trades", 0), wins=d.get("wins", 0),
                         window_start_equity=d.get("window_start_equity", START_CASH_EUR),
                         combo_id=d.get("combo_id", 0), born_day=d.get("born_day", 0),
                         days_alive=d.get("days_alive", 0), days_won=d.get("days_won", 0),
                         cum_pnl=d.get("cum_pnl", 0.0), day_pnls=d.get("day_pnls", []))
                for d in data.get("strategies", [])]
            print(f"[{self.mode} load] resumed day {self.day_index}, "
                  f"{len(self.strategies)} strategies", flush=True)
        except Exception as e:
            print(f"[{self.mode} load] {e}", flush=True)

    def csv_append(self, st, pos, pnl, reason):
        try:
            p = csv_path(self.mode)
            new = not os.path.exists(p)
            with open(p, "a", newline="") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(["time", "strategy_id", "mint", "reason", "pnl_eur",
                                "genome"])
                w.writerow([now_hms(), st.id, pos["mint"], reason, round(pnl, 3),
                            json.dumps(st.genome)])
        except Exception as e:
            print(f"[{self.mode} csv] {e}", flush=True)

    def reset(self):
        self.strategies = []
        self.next_id = self.next_combo_id = 1
        self.generation = 1
        self.day_index = 0
        self.last_evolve = time.time()
        self.pv.clear()
        self.recent.clear()
        self.ledger.clear()
        self.watch = {}
        self.best_ever = None
        for _ in range(POOL_SIZE):
            self._new_strategy(rand_genome(self.gb))
        self.save()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
async def pumpportal_stream(app):
    session, pools = app["session"], app["pools"]
    headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://pumpportal.fun"}
    need_holders = any(p.mode == "smart" for p in pools.values())
    backoff = 5
    while True:
        try:
            async with session.ws_connect(PUMPPORTAL_WSS, max_msg_size=0,
                                          heartbeat=30, headers=headers) as ws:
                await ws.send_str(json.dumps({"method": "subscribeNewToken"}))
                backoff = 5
                print("subscribed to pump.fun launches (via PumpPortal, free)", flush=True)
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        d = json.loads(msg.data)
                    except Exception:
                        continue
                    if not (isinstance(d, dict) and d.get("mint")
                            and (d.get("bondingCurveKey") or d.get("bonding_curve"))):
                        continue
                    info = parse_pp_token(d)
                    if not info:
                        continue
                    if "reserves" not in info:
                        c = await get_curve(session, info["curve"])
                        if isinstance(c, dict):
                            info["reserves"] = c
                    if need_holders:
                        info["top_hold"] = await get_top_holder_pct(session, info["mint"])
                    for p in pools.values():
                        p.on_new_token(info)
                    await broadcast_all(app)
        except Exception as e:
            hint = ("  (PumpPortal timeout — bans last ~1h, only ONE connection)"
                    if "502" in str(e) else "")
            print(f"[pumpportal reconnect in {backoff}s] {e}{hint}", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)


async def shared_tracker(app):
    session, pools = app["session"], app["pools"]
    while True:
        curves = {}
        for p in pools.values():
            for cv in p.curves_needed():
                curves[cv] = None
        for cv in list(curves):
            curves[cv] = await get_curve(session, cv)
        now = time.time()
        for p in pools.values():
            p.value_and_exit(curves)
            if p.mode == "hunt":
                p.hunt_check(curves, now)
            p.pv.append({"t": int(now), "v": round(p.best_equity(), 2)})
            if now - p.last_evolve >= EVOLVE_INTERVAL_SEC:
                p.evolve()
            p.save()
        await broadcast_all(app)
        await asyncio.sleep(POLL_INTERVAL_SEC)


async def price_feed(app):
    session, pools = app["session"], app["pools"]
    while True:
        try:
            url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=eur"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                rate = (await r.json())["solana"]["eur"]
            for p in pools.values():
                p.sol_eur = rate
        except Exception:
            pass
        await asyncio.sleep(60)


def combined_snapshot(app):
    pools = app["pools"]
    snaps = {m: pools[m].snapshot() for m in pools}
    league = sorted(
        [{"mode": m, "total_realized": s["total_realized"],
          "champ_pnl": s["champion"]["realized"] if s["champion"] else 0,
          "champ_eq": s["champion"]["equity"] if s["champion"] else 0,
          "best_cum": (s["best_ever"] or {}).get("cum_pnl"),
          "day": s["day_index"], "pool": s["pool"],
          "genome": s["champion"]["genome"] if s["champion"] else {}}
         for m, s in snaps.items()],
        key=lambda x: x["champ_pnl"], reverse=True)
    return {"modes": list(pools.keys()), "pools": snaps, "league": league,
            "sol_eur": next(iter(snaps.values()))["sol_eur"] if snaps else 0}


async def broadcast_all(app):
    clients = app["clients"]
    if not clients:
        return
    msg = json.dumps(combined_snapshot(app))
    for ws in list(clients):
        try:
            await ws.send_str(msg)
        except Exception:
            clients.discard(ws)


# ---------------------------------------------------------------------------
# Web
# ---------------------------------------------------------------------------
def authed(request):
    if not ACCESS_TOKEN:
        return True
    return (request.query.get("k") == ACCESS_TOKEN
            or request.headers.get("X-Access-Token") == ACCESS_TOKEN)


_DENY = "Unauthorized — add ?k=YOUR_TOKEN to the URL"


async def h_health(request):
    return web.Response(text="ok")


async def h_index(request):
    # Public shell (returns 200 for any probe). All DATA stays behind the token
    # via /api/state and /ws, so this is not a data leak.
    return web.Response(text=PAGE, content_type="text/html")


async def h_state(request):
    if not authed(request):
        return web.Response(status=401, text=_DENY)
    return web.json_response(combined_snapshot(request.app))


async def h_reset(request):
    if not authed(request):
        return web.Response(status=401, text=_DENY)
    mode = (await request.json()).get("mode")
    pools = request.app["pools"]
    if mode in pools:
        pools[mode].reset()
    await broadcast_all(request.app)
    return web.json_response({"ok": True})


async def h_ws(request):
    if not authed(request):
        return web.Response(status=401, text=_DENY)
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    request.app["clients"].add(ws)
    await ws.send_str(json.dumps(combined_snapshot(request.app)))
    try:
        async for _ in ws:
            pass
    finally:
        request.app["clients"].discard(ws)
    return ws


async def keepalive(app):
    # Ping our own PUBLIC url so Railway's edge sees real inbound traffic and
    # never judges the app idle. localhost wouldn't count — it must be the
    # public domain. Harmless if sleep isn't the cause.
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    url = f"https://{domain}/health" if domain else f"http://127.0.0.1:{PORT}/health"
    await asyncio.sleep(2)
    while True:
        try:
            async with app["session"].get(url, timeout=aiohttp.ClientTimeout(total=8)):
                pass
        except Exception:
            pass
        await asyncio.sleep(5)


async def on_startup(app):
    app["session"] = aiohttp.ClientSession()
    app["clients"] = set()
    pools = {}
    for m in MODES:
        p = Pool(m)
        p.load()
        pools[m] = p
    app["pools"] = pools
    app["tasks"] = [asyncio.create_task(pumpportal_stream(app)),
                    asyncio.create_task(shared_tracker(app)),
                    asyncio.create_task(price_feed(app)),
                    asyncio.create_task(keepalive(app))]


async def on_cleanup(app):
    for t in app["tasks"]:
        t.cancel()
    await app["session"].close()


def make_app():
    app = web.Application()
    app.add_routes([web.get("/", h_index), web.get("/health", h_health),
                    web.get("/api/state", h_state), web.post("/api/reset", h_reset),
                    web.get("/ws", h_ws)])
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


PAGE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>pump desk</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root{--bg:#0d0f1a;--panel:#151932;--panel2:#1a1f3d;--line:#262c4a;--ink:#eceefb;
    --mut:#868cb2;--gain:#38e0b0;--loss:#ff7a8a;--accent:#7c6cff;--accent2:#a99bff;--gold:#f5c451}
  *{box-sizing:border-box}
  body{margin:0 auto;background:var(--bg);color:var(--ink);font-family:Archivo,system-ui,sans-serif;
    padding:14px 12px 60px;max-width:680px;-webkit-font-smoothing:antialiased}
  .mono{font-family:"JetBrains Mono",monospace;font-variant-numeric:tabular-nums}
  .up{color:var(--gain)}.down{color:var(--loss)}
  h2{font-size:13px;font-weight:600;color:var(--mut);margin:22px 0 9px}
  .sim{font-size:11px;color:var(--accent2);margin-bottom:10px}
  .tabs{display:flex;gap:6px;overflow-x:auto;padding-bottom:4px;margin-bottom:6px}
  .tab{flex:none;font-family:Archivo;font-weight:600;font-size:13px;padding:8px 13px;
    border-radius:10px;border:1px solid var(--line);background:var(--panel);color:var(--mut);cursor:pointer}
  .tab.on{background:var(--accent);color:#fff;border-color:var(--accent)}
  .hero{background:linear-gradient(160deg,var(--panel),var(--panel2));border:1px solid var(--line);
    border-radius:16px;padding:16px}
  .hlabel{font-size:12px;color:var(--mut);display:flex;gap:7px;align-items:center}
  .crown{color:var(--gold)}
  .big{font-family:"JetBrains Mono",monospace;font-weight:700;font-size:34px;margin:3px 0 6px}
  .genes{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 2px}
  .gene{display:flex;flex-direction:column;gap:2px;background:var(--bg);border:1px solid var(--line);
    border-radius:10px;padding:6px 10px;min-width:60px}
  .gl{font-size:10px;color:var(--mut);text-transform:uppercase}
  .gv{font-family:"JetBrains Mono",monospace;font-size:13px;font-weight:600}
  .subrow{display:flex;gap:16px;margin-top:10px;font-size:13px;flex-wrap:wrap}
  .status{display:flex;gap:13px;font-size:12px;color:var(--mut);margin-top:12px;flex-wrap:wrap;align-items:center}
  .reset{background:transparent;border:1px solid var(--line);color:var(--mut);
    padding:6px 11px;border-radius:9px;font-weight:600;font-size:12px;cursor:pointer}
  canvas{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:8px;margin-top:6px}
  .coin{font-family:"JetBrains Mono",monospace;font-size:12.5px;color:var(--accent2);text-decoration:none}
  .scard{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:13px;margin-bottom:10px}
  .scard.lead{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent) inset}
  .shead{display:flex;align-items:baseline;gap:9px}
  .rank{color:var(--mut);font-family:"JetBrains Mono",monospace;font-size:13px}
  .eq{font-size:16px;font-weight:700}.pl{margin-left:auto;font-weight:600}
  .smeta{font-size:12px;color:var(--mut);margin-top:5px}
  .holds{margin-top:9px;font-size:12px;line-height:1.9;color:var(--mut)}
  .rrow{display:flex;align-items:center;gap:9px;padding:8px 2px;border-top:1px solid var(--line);font-size:13px}
  .tag{font-size:11px;color:var(--mut);background:var(--bg);border:1px solid var(--line);border-radius:7px;padding:2px 7px}
  .rt{margin-left:auto;color:var(--mut);font-size:11px}
  .hof{background:linear-gradient(160deg,#1c1733,#241b3f);border:1px solid var(--gold);border-radius:14px;padding:13px}
  .lrow{display:flex;align-items:center;gap:9px;padding:8px 2px;border-top:1px solid var(--line);font-size:12.5px;flex-wrap:wrap}
  .badge{font-size:10px;font-weight:700;color:#0d0f1a;background:var(--gold);border-radius:6px;padding:1px 6px;margin-left:6px}
  .rise{font-size:10px;font-weight:700;color:var(--gain);border:1px solid var(--gain);border-radius:6px;padding:1px 6px;margin-left:6px}
  .lgcard{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:15px;margin-bottom:11px}
  .lgcard.win{border-color:var(--gold);box-shadow:0 0 0 1px var(--gold) inset}
  .medal{font-size:18px}.launches{line-height:2.1}
  .empty{color:var(--mut);font-size:13px;padding:12px 2px}
  .foot{color:var(--mut);font-size:11px;margin-top:20px;line-height:1.5}
</style></head>
<body>
<div class="sim">all-in-one · simulated cash · no real money</div>
<div class="tabs" id="tabs"></div>
<div id="view"></div>
<div class="foot" id="foot"></div>
<script>
const K=new URLSearchParams(location.search).get('k')||'';
const Q=K?('?k='+encodeURIComponent(K)):'';
const MODES={snipe:"🎯 Sniper",smart:"🛡️ Smart",hunt:"📈 Hunter"};
const GLABEL={tp:'take',sl:'stop',hold:'hold',dev_max:'dev ≤',dev_min:'dev ≥',
  top_hold_max:'top ≤',mom_pct:'pump ≥',mom_window:'window'};
let S=null, tab='league', chart=null, _timer=null;
function eur(n){return "€"+Number(n).toFixed(2)}
function sgn(n){return (n>=0?"+":"")+Number(n).toFixed(2)}
function pct(n){return (n>=0?"+":"")+Math.round(n)+"%"}
function short(a){return a.slice(0,4)+"…"+a.slice(-4)}
function coin(m,l){return `<a class="coin" target="_blank" rel="noopener" href="https://pump.fun/coin/${m}">${l||short(m)}</a>`}
function gval(k,v){if(k==='sl')return Math.round(v*100)+'%';if(k==='tp')return v+'×';
  if(k==='hold'||k==='mom_window')return v+'s';if(k==='dev_max'||k==='dev_min')return v+'◎';return v+'%';}
function genes(g){return '<div class="genes">'+Object.keys(g).map(k=>
  `<div class="gene"><span class="gl">${GLABEL[k]||k}</span><span class="gv">${gval(k,g[k])}</span></div>`).join('')+'</div>';}
function genesShort(g){return Object.keys(g).map(k=>(GLABEL[k]||k)+' '+gval(k,g[k])).join(' · ');}
const REASON={tp:"2× hit",sl:"stopped",timeout:"timed out",rug:"rugged",graduated:"graduated"};

function drawTabs(){
  const t=['league',...S.modes];
  document.getElementById('tabs').innerHTML=t.map(x=>
    `<div class="tab ${x===tab?'on':''}" onclick="pick('${x}')">${x==='league'?'🏁 League':MODES[x]||x}</div>`).join('');
}
function pick(x){tab=x;draw();}

function draw(){
  try{
    drawTabs();
    const v=document.getElementById('view');
    if(tab==='league'){ v.innerHTML=leagueHTML(); document.getElementById('foot').textContent=
      'Ranked by total realised P&L. Simulated — a leader is a hypothesis, not proof.'; return; }
    const p=S.pools[tab]; if(!p){v.innerHTML='<div class="empty">no data</div>';return;}
    v.innerHTML=poolHTML(p);
    drawChart(p.pv);
    document.getElementById('foot').textContent=
      `${MODES[tab]||tab} · pool P&L ${sgn(p.total_realized)}€ · SOL €${p.sol_eur} · simulated`;
  }catch(e){ console.error('draw error', e); }
}

function leagueHTML(){
  const MED=["🥇","🥈","🥉"];
  return '<h2>Which brain is winning</h2>'+S.league.map((r,i)=>{
    const cl=r.total_realized>=0?'up':'down';
    return `<div class="lgcard ${i===0?'win':''}">
      <div class="shead"><span class="medal">${MED[i]||''}</span>
        <span class="eq">${MODES[r.mode]||r.mode}</span>
        <span class="pl mono ${cl}">${sgn(r.total_realized)}€</span></div>
      <div class="smeta">champion equity ${eur(r.champ_eq)} · best combo ${r.best_cum!=null?sgn(r.best_cum)+'€':'—'} · day ${r.day}</div>
      ${genes(r.genome||{})}</div>`;}).join('')
    +`<div class="smeta" style="margin-top:10px">Tap a brain's tab above for its full board.</div>`;
}

function poolHTML(p){
  const c=p.champion||{}; let h='';
  h+=`<div class="hero"><div class="hlabel"><span class="crown">♛</span> Best strategy right now</div>
    <div class="big">${c.equity!=null?eur(c.equity):'€—'}</div>${genes(c.genome||{})}
    <div class="subrow"><span>P&L <b class="mono ${(c.realized||0)>=0?'up':'down'}">${sgn(c.realized||0)}€</b></span>
      <span>trades <b class="mono">${c.trades||0}</b></span><span>win <b class="mono">${c.winrate||0}%</b></span></div>
    <div class="status"><span>gen <b class="mono">${p.generation}</b></span>
      <span>day <b class="mono">${p.day_index}</b></span>
      <span>evolves in <b class="mono">${Math.floor(p.evolve_in/3600)}h</b></span>
      <button class="reset" onclick="resetMode('${p.mode}')">Reset ${p.mode}</button></div></div>`;
  h+='<h2>Portfolio value</h2><canvas id="chart" height="150"></canvas>';
  const be=p.best_ever;
  h+='<h2>🏆 Hall of fame</h2>'+(be?`<div class="hof"><div class="hlabel">combo #${be.combo_id} · best ever</div>
    <div class="big ${be.cum_pnl>=0?'up':'down'}">${sgn(be.cum_pnl)}€</div>${genes(be.genome)}
    <div class="smeta">survived ${be.days_alive}d · won ${be.days_won}d</div></div>`
    :'<div class="empty">after first daily evolve…</div>');
  h+='<h2>Daily champions</h2>'+(p.ledger.length?p.ledger.map(d=>
    `<div class="lrow"><span class="mono" style="color:var(--mut);min-width:52px">Day ${d.day}</span>
     <span class="gv" style="flex:1">${genesShort(d.genome)}</span>
     <span class="mono ${d.day_pnl>=0?'up':'down'}">${sgn(d.day_pnl)}€</span></div>`).join('')
    :'<div class="empty">no days yet…</div>');
  h+='<h2>Recent snipes</h2>'+(p.recent.length?p.recent.map(r=>
    `<div class="rrow">${coin(r.mint)}<span class="tag">${REASON[r.reason]||r.reason}</span>
     <span class="mono ${r.pnl>=0?'up':'down'}">${sgn(r.pnl)}€</span><span class="rt">#${r.id} · ${r.time}</span></div>`).join('')
    :'<div class="empty">no closed trades yet…</div>');
  h+='<h2>Leaderboard</h2>'+p.board.map((b,i)=>{
    const cl=b.realized>=0?'up':'down';
    const tags=(i===0?'<span class="badge">LEADER</span>':'')+(b.rising?'<span class="rise">🔥 RISING</span>':'');
    const holds=(b.holds&&b.holds.length)?`<div class="holds">holding: `+b.holds.map(x=>
      coin(x.mint)+` <span class="${x.pnl>=0?'up':'down'}">${pct(x.pnl)}</span>`).join(' · ')+`</div>`:'';
    return `<div class="scard ${i===0?'lead':''}"><div class="shead"><span class="rank">#${i+1}</span>
      <span class="eq mono">${eur(b.equity)}</span><span class="pl mono ${cl}">${sgn(b.realized)}€</span></div>
      ${genes(b.genome)}<div class="smeta">combo #${b.combo}${tags} · alive ${b.days}d · won ${b.won}d · cum <span class="${b.cum>=0?'up':'down'}">${sgn(b.cum)}€</span></div>
      <div class="smeta">${b.trades} trades · ${b.winrate}% win · holding ${b.open}</div>${holds}</div>`;}).join('');
  h+='<h2>Live launches</h2>'+(p.launches.length?
    '<div class="launches">'+p.launches.map(l=>coin(l.mint,short(l.mint)+(l.dev_buy!=null?' ('+l.dev_buy.toFixed(2)+'◎)':'')+' · '+l.age_sec+'s')).join('<br>')+'</div>'
    :'<div class="empty">waiting for the next mint…</div>');
  return h;
}

function drawChart(pv){
  const cv=document.getElementById('chart'); if(!cv) return;
  const data=(pv||[]).map(p=>p.v);
  if(chart){try{chart.destroy();}catch(e){}}
  chart=new Chart(cv,{type:'line',data:{labels:data.map(_=>''),datasets:[{data,
    borderColor:'#f5c451',borderWidth:2,fill:true,backgroundColor:'rgba(245,196,81,.1)',
    tension:.25,pointRadius:0}]},options:{animation:false,plugins:{legend:{display:false}},
    scales:{x:{display:false},y:{ticks:{color:'#868cb2',font:{family:"JetBrains Mono"}},grid:{color:'#262c4a'}}}}});
}
async function resetMode(m){if(confirm('Reset '+m+' back to fresh random strategies?'))
  await fetch('/api/reset'+Q,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({mode:m})});}

function render(s){
  const first=(S===null);
  S=s;
  if(tab!=='league' && !s.modes.includes(tab)) tab='league';
  if(first){ draw(); return; }          // paint immediately on first load
  if(_timer) return;                     // otherwise coalesce rapid updates
  _timer=setTimeout(()=>{ _timer=null; draw(); }, 1200);
}
function connect(){const proto=location.protocol==='https:'?'wss:':'ws:';
  const ws=new WebSocket(proto+'//'+location.host+'/ws'+Q);
  ws.onmessage=e=>render(JSON.parse(e.data));ws.onclose=()=>setTimeout(connect,1500);}
connect();
</script>
</body></html>"""


if __name__ == "__main__":
    print(f"MODES={','.join(MODES)}  data=pumpportal  rpc={'set' if RPC_HTTP else 'MISSING'}  "
          f"cash=€{START_CASH_EUR:.0f}  trade=€{TRADE_EUR}  evolve/{EVOLVE_INTERVAL_SEC}s  "
          f"dashboard -> http://localhost:{PORT}")
    print("SIMULATED CASH ONLY — no wallet, no real trades\n")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)
