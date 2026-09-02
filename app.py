"""
pump.fun evolving paper-trading desk
====================================
Runs a POOL of trading strategies side by side against the same live pump.fun
launches. Each strategy has its own settings (entry filter + exit rules) and its
own fake bankroll. Every EVOLVE_INTERVAL it ranks them by money made, kills the
losers, and breeds mutated copies of the winners. Over time the pool drifts
toward settings that made the most simulated money.

SIMULATION ONLY. No wallet, no private key, no real transactions. It reads
Solana state and moves imaginary euros. You cannot win or lose real money here.

Because nothing real is bought, every strategy can evaluate every launch at once
for free — that's what makes the side-by-side search fair and fast.

Honest warning: an optimizer will always "find" a winner. Much of it is luck
fitted to the launches it happened to see, and the sim is optimistic (assumes
every buy lands and every sell fills). Treat the top strategy as a hypothesis,
not a money printer.

Run:  pip install aiohttp ; python app.py  ; open http://localhost:8080
"""

import asyncio
import base64
import csv
import hashlib
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
RPC_HTTP = os.environ.get("RPC_HTTP", "https://mainnet.helius-rpc.com/?api-key=b842bf4c-c718-48ca-92d0-dbdc408e0b0c")
RPC_WSS = os.environ.get("RPC_WSS", "wss://mainnet.helius-rpc.com/?api-key=b842bf4c-c718-48ca-92d0-dbdc408e0b0c")
PORT = int(os.environ.get("PORT", "8080"))
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "")  # set on Railway to lock the dashboard

START_CASH_EUR = float(os.environ.get("START_CASH_EUR", "100000"))
TRADE_EUR = float(os.environ.get("TRADE_EUR", "50"))
SIM_LATENCY_MS = int(os.environ.get("SIM_LATENCY_MS", "600"))
FEE_BPS = 100

POOL_SIZE = int(os.environ.get("POOL_SIZE", "12"))
EVOLVE_INTERVAL_SEC = int(os.environ.get("EVOLVE_INTERVAL_SEC", "3600"))  # 1h; 86400 = per day
MAX_POS_PER_STRAT = int(os.environ.get("MAX_POS_PER_STRAT", "40"))
POLL_INTERVAL_SEC = float(os.environ.get("POLL_INTERVAL_SEC", "3"))  # raise to 6-8 on free RPC

# Which brain this instance runs: "snipe" (basic), "smart" (anti-rug filters),
# or "hunt" (momentum on tokens that already have some life). Deploy the same
# repo three times with a different BOT_MODE each.
BOT_MODE = os.environ.get("BOT_MODE", "snipe").strip().lower()
if BOT_MODE not in ("snipe", "smart", "hunt", "league"):
    BOT_MODE = "snipe"

# league mode: comma-separated "label|https://bot-url" entries to aggregate.
PEERS = []
for _part in os.environ.get("PEERS", "").split(","):
    _part = _part.strip()
    if "|" in _part:
        _lbl, _url = _part.split("|", 1)
        PEERS.append((_lbl.strip(), _url.strip().rstrip("/")))

# hunt-only knobs
HUNT_WATCH_MAX = int(os.environ.get("HUNT_WATCH_MAX", "30"))    # tokens tracked at once
HUNT_MAX_AGE = int(os.environ.get("HUNT_MAX_AGE", "1800"))      # drop a token after this many s
HUNT_SCAN_SEC = float(os.environ.get("HUNT_SCAN_SEC", "12"))    # how often to re-price the watchlist

# Where to save. On Railway, a mounted volume auto-sets RAILWAY_VOLUME_MOUNT_PATH,
# so we use that directly — no hand-typed path to get mangled by phone autocorrect.
_VOL = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
if _VOL:
    STATE_PATH = os.path.join(_VOL, "state.json")
    CSV_PATH = os.path.join(_VOL, "trades.csv")
else:
    STATE_PATH = os.environ.get("STATE_PATH", "state.json").strip()
    CSV_PATH = os.environ.get("CSV_PATH", "trades.csv").strip()
try:
    for _p in (STATE_PATH, CSV_PATH):
        _d = os.path.dirname(_p)
        if _d:
            os.makedirs(_d, exist_ok=True)
except Exception as _e:
    print(f"[init] could not create save dir: {_e}", flush=True)

# Where launch data comes from. "pumpportal" = free purpose-built pump.fun feed
# (no key, no Helius credits burned). "helius" = old firehose (expensive).
DATA_SOURCE = os.environ.get("DATA_SOURCE", "pumpportal").strip().lower()
PUMPPORTAL_WSS = os.environ.get("PUMPPORTAL_WSS", "wss://pumpportal.fun/api/data")

# ---------------------------------------------------------------------------
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_DISC = bytes([24, 30, 200, 40, 5, 28, 7, 119])
LAMPORTS = 1_000_000_000
TOKEN_UNITS = 1_000_000
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# Strategy "genes" and their allowed ranges, chosen per bot mode.
_EXIT_GENES = {
    "tp":   (1.2, 5.0),     # take profit multiple (x cost)
    "sl":   (0.20, 0.80),   # stop loss (fraction of cost lost)
    "hold": (30, 300),      # max hold seconds
}
_MODE_GENES = {
    "snipe": {**_EXIT_GENES,
              "slip": (8.0, 50.0),      # skip if it ran up more than this % while landing
              "dev_max": (0.3, 25.0)},  # skip if dev bought more than this many SOL
    "smart": {**_EXIT_GENES,
              "slip": (8.0, 50.0),
              "dev_max": (0.3, 25.0),   # dev bought too much = dump risk
              "dev_min": (0.0, 3.0),    # dev bought too little = no skin in game
              "top_hold_max": (20.0, 95.0)},  # skip if biggest holder owns > this % of float
    "hunt":  {**_EXIT_GENES,
              "dev_max": (0.3, 25.0),
              "mom_pct": (5.0, 80.0),   # only buy if price pumped at least this % ...
              "mom_window": (10, 120)}, # ... within this many seconds
}
GENE_BOUNDS = _MODE_GENES.get(BOT_MODE, _MODE_GENES["snipe"])
_INT_GENES = ("hold", "mom_window")


def b58decode(s):
    n = 0
    for ch in s:
        n = n * 58 + _B58.index(ch)
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\x00" * (len(s) - len(s.lstrip("1"))) + body


def b58encode(b):
    n = int.from_bytes(b, "big")
    out = ""
    while n > 0:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    return "1" * (len(b) - len(b.lstrip(b"\x00"))) + out


# Anchor event discriminators = first 8 bytes of sha256("event:<Name>").
# pump.fun emits these in "Program data:" log lines, so we can read a new
# launch (and the dev's bundled buy) straight from the logs — no slow second
# transaction lookup that could fail and make us miss the launch.
CREATE_EVENT_DISC = hashlib.sha256(b"event:CreateEvent").digest()[:8]
TRADE_EVENT_DISC = hashlib.sha256(b"event:TradeEvent").digest()[:8]


def now_hms():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def now_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


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
    """Parse a PumpPortal subscribeNewToken event into our launch shape.
    Its payload carries the mint, bonding curve, virtual reserves (= price)
    and the dev's SOL buy — so we can enter with no RPC call at all."""
    mint = d.get("mint")
    curve = d.get("bondingCurveKey") or d.get("bonding_curve")
    if not mint or not curve:
        return None
    dev = d.get("solAmount")
    info = {"mint": mint, "curve": curve,
            "creator": d.get("traderPublicKey") or mint,
            "dev_buy_sol": float(dev) if dev is not None else None}
    vt = d.get("vTokensInBondingCurve")
    vs = d.get("vSolInBondingCurve")
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


def rand_genome():
    return {k: _round_gene(k, random.uniform(lo, hi))
            for k, (lo, hi) in GENE_BOUNDS.items()}


def mutate(g):
    ng = dict(g)
    for k, (lo, hi) in GENE_BOUNDS.items():
        if k in ng and random.random() < 0.5:          # mutate ~half the genes
            ng[k] = _round_gene(k, clamp(ng[k] * random.uniform(0.7, 1.3), lo, hi))
    return ng


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
    combo_id: int = 0                       # stable identity of this combination
    born_day: int = 0                       # day index it was created
    days_alive: int = 0                     # daily evolutions survived
    days_won: int = 0                       # days it was the #1 performer
    cum_pnl: float = 0.0                    # total P&L over its whole life
    day_pnls: list = field(default_factory=list)  # per-day scores


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------
class Pool:
    def __init__(self, session):
        self.s = session
        self.sol_eur = 150.0
        self.strategies = []
        self.next_id = 1
        self.generation = 1
        self.last_evolve = time.time()
        self.launches = deque(maxlen=40)
        self.pv = deque(maxlen=360)
        self.recent = deque(maxlen=30)      # global feed of recent closes (coin + result)
        self.day_index = 0                  # counts daily evolutions
        self.next_combo_id = 1
        self.ledger = deque(maxlen=90)       # one record per day: that day's winner
        self.best_ever = None                # all-time best combination by cumulative P&L
        self.watch = {}                      # hunt mode: tokens being tracked for momentum
        self.clients = set()
        for _ in range(POOL_SIZE):
            self._new_strategy(rand_genome())

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
        return max((self.equity(st) for st in self.strategies), default=START_CASH_EUR)

    # ---- RPC ----
    async def rpc(self, method, params):
        try:
            async with self.s.post(RPC_HTTP, json={"jsonrpc": "2.0", "id": 1,
                    "method": method, "params": params},
                    timeout=aiohttp.ClientTimeout(total=8)) as r:
                return (await r.json()).get("result")
        except Exception:
            return None

    async def get_tx(self, sig, tries=4):
        p = [sig, {"encoding": "json", "commitment": "confirmed",
                   "maxSupportedTransactionVersion": 0}]
        for i in range(tries):
            res = await self.rpc("getTransaction", p)
            if res:
                return res
            await asyncio.sleep(0.4 * (i + 1))
        return None

    async def get_curve(self, curve):
        res = await self.rpc("getAccountInfo",
                             [curve, {"encoding": "base64", "commitment": "processed"}])
        if res is None:
            return None            # read failed (network / rate limit) — try again later
        if not res.get("value"):
            return "GONE"          # account confirmed not to exist = rugged / removed
        return parse_curve(base64.b64decode(res["value"]["data"][0]))

    # ---- detection + entry ----
    def parse_create(self, tx):
        msg = tx["transaction"]["message"]
        keys = msg["accountKeys"]
        for ix in msg["instructions"]:
            if keys[ix["programIdIndex"]] != PUMP_PROGRAM:
                continue
            try:
                data = b58decode(ix["data"])
            except Exception:
                continue
            if data[:8] == CREATE_DISC:
                a = ix["accounts"]
                meta = tx.get("meta") or {}
                pre, post = meta.get("preBalances"), meta.get("postBalances")
                dev = None
                if pre and post:
                    dev = max((pre[0] - post[0]) - meta.get("fee", 0), 0) / LAMPORTS
                return {"mint": keys[a[0]], "curve": keys[a[2]],
                        "creator": keys[a[7]], "dev_buy_sol": dev}
        return None

    async def on_signature(self, sig, detected_at, logs):
        info = self.parse_events(logs)          # primary: read straight from logs
        if not info:                            # fallback: slower tx lookup
            tx = await self.get_tx(sig)
            info = self.parse_create(tx) if tx else None
        if not info:
            return
        info["age"] = time.time()
        self.launches.appendleft(info)
        await self.broadcast()

        if BOT_MODE == "hunt":                  # don't snipe — add to the watchlist
            self.watch[info["mint"]] = {"curve": info["curve"],
                                        "first_seen": time.time(), "prices": [],
                                        "dev_buy": info["dev_buy_sol"]}
            while len(self.watch) > HUNT_WATCH_MAX:
                oldest = min(self.watch, key=lambda m: self.watch[m]["first_seen"])
                self.watch.pop(oldest, None)
            return

        c0 = await self.get_curve(info["curve"])
        p0 = spot_price(c0)
        elapsed = time.monotonic() - detected_at
        await asyncio.sleep(max(SIM_LATENCY_MS / 1000 - elapsed, 0))
        c1 = await self.get_curve(info["curve"])
        if not isinstance(c1, dict) or c1["complete"]:
            return
        p1 = spot_price(c1)
        if BOT_MODE == "smart":                 # anti-rug: read holder concentration
            info["top_hold"] = await self.get_top_holder_pct(info["mint"])
        for st in self.strategies:
            self.try_enter(st, info, p0, p1, c1)
        await self.broadcast()

    async def on_new_token(self, info):
        """PumpPortal path: launch data (incl. price + dev buy) comes in the
        event, so entry costs no RPC. Only open positions later cost reads."""
        info["age"] = time.time()
        self.launches.appendleft(info)
        await self.broadcast()

        if BOT_MODE == "hunt":
            w = {"curve": info["curve"], "first_seen": time.time(),
                 "prices": [], "dev_buy": info["dev_buy_sol"]}
            if info.get("reserves"):
                w["prices"].append((time.time(), spot_price(info["reserves"])))
            self.watch[info["mint"]] = w
            while len(self.watch) > HUNT_WATCH_MAX:
                oldest = min(self.watch, key=lambda m: self.watch[m]["first_seen"])
                self.watch.pop(oldest, None)
            return

        c1 = info.get("reserves") or await self.get_curve(info["curve"])
        if not isinstance(c1, dict) or c1.get("complete"):
            return
        price = spot_price(c1)
        if BOT_MODE == "smart":
            info["top_hold"] = await self.get_top_holder_pct(info["mint"])
        for st in self.strategies:
            self.try_enter(st, info, price, price, c1)   # p0=p1 → slip gene inert here
        await self.broadcast()

    async def hunt_scan(self):
        """Hunt mode: re-price the watchlist and buy tokens that are pumping."""
        while True:
            t = time.time()
            for mint in list(self.watch):
                w = self.watch[mint]
                if t - w["first_seen"] > HUNT_MAX_AGE:
                    self.watch.pop(mint, None)
                    continue
                c = await self.get_curve(w["curve"])
                if c == "GONE" or (isinstance(c, dict) and c["complete"]):
                    self.watch.pop(mint, None)
                    continue
                if not isinstance(c, dict):
                    continue
                w["prices"].append((t, spot_price(c)))
                w["prices"] = w["prices"][-40:]
                for st in self.strategies:
                    self.try_enter_hunt(st, mint, w, c)
            await self.broadcast()
            await asyncio.sleep(HUNT_SCAN_SEC)

    def parse_events(self, logs):
        """Read the CreateEvent (mint, curve) and the dev's bundled buy straight
        from the 'Program data:' log lines. Returns None if no launch here."""
        mint = curve = creator = None
        dev = None
        for line in logs:
            if not line.startswith("Program data: "):
                continue
            try:
                raw = base64.b64decode(line[14:])
            except Exception:
                continue
            disc = raw[:8]
            if disc == CREATE_EVENT_DISC:
                try:
                    off = 8
                    for _ in range(3):          # skip name, symbol, uri (borsh strings)
                        ln = int.from_bytes(raw[off:off + 4], "little")
                        off += 4 + ln
                    mint = b58encode(raw[off:off + 32]); off += 32
                    curve = b58encode(raw[off:off + 32]); off += 32
                    creator = b58encode(raw[off:off + 32])
                except Exception:
                    mint = curve = None
            elif disc == TRADE_EVENT_DISC and dev is None:
                try:                            # first trade in the tx = dev's buy
                    sol_amount = int.from_bytes(raw[8 + 32:8 + 32 + 8], "little")
                    dev = sol_amount / LAMPORTS
                except Exception:
                    pass
        if mint and curve and len(mint) >= 32 and len(curve) >= 32:
            return {"mint": mint, "curve": curve,
                    "creator": creator or mint, "dev_buy_sol": dev}
        return None

    async def get_top_holder_pct(self, mint):
        """Approx: biggest non-curve holder's share of circulating supply, at entry.
        The bonding curve holds the most supply, so we exclude the single largest
        account and measure the next-largest against the rest. Higher = riskier."""
        res = await self.rpc("getTokenLargestAccounts", [mint, {"commitment": "processed"}])
        if not res or not res.get("value"):
            return None
        amts = sorted((int(a["amount"]) for a in res["value"]), reverse=True)
        if len(amts) < 2:
            return 0.0
        circulating = sum(amts) - amts[0]      # exclude the curve (largest)
        if circulating <= 0:
            return 0.0
        return amts[1] / circulating * 100

    def _enter(self, st, mint, curve, c):
        """Open a simulated position in `mint` against curve state `c`."""
        sol_in = int((TRADE_EUR / self.sol_eur) * LAMPORTS)
        tokens = buy_quote(c, sol_in)
        if tokens <= 0:
            return
        st.cash -= TRADE_EUR
        st.positions[mint] = {
            "mint": mint, "curve": curve, "tokens": tokens,
            "cost_eur": TRADE_EUR, "entry_ts": time.time(), "value_eur": TRADE_EUR,
        }

    def try_enter(self, st, info, p0, p1, c1):
        """Snipe / smart entry: decide at launch whether to buy."""
        mint = info["mint"]
        g = st.genome
        if mint in st.positions or st.cash < TRADE_EUR:
            return
        if len(st.positions) >= MAX_POS_PER_STRAT:
            return
        dev = info.get("dev_buy_sol")
        if dev is not None:
            if "dev_max" in g and dev > g["dev_max"]:
                return                                  # dev grabbed too much
            if "dev_min" in g and dev < g["dev_min"]:
                return                                  # dev has no skin in the game
        if "top_hold_max" in g and info.get("top_hold") is not None:
            if info["top_hold"] > g["top_hold_max"]:
                return                                  # supply too concentrated
        if "slip" in g and p0 > 0 and p1 > 0 and (p1 / p0 - 1) > g["slip"] / 100:
            return                                      # too much run-up = would miss
        self._enter(st, mint, info["curve"], c1)

    def try_enter_hunt(self, st, mint, w, c):
        """Hunt entry: buy a token that already exists if it's pumping."""
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
            return                                      # not pumping enough yet
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
        pp = past[1]
        return (now_p / pp - 1) if pp > 0 else None

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

    # ---- loops ----
    async def price_feed(self):
        while True:
            try:
                url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=eur"
                async with self.s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    self.sol_eur = (await r.json())["solana"]["eur"]
            except Exception:
                pass
            await asyncio.sleep(60)

    async def tracker(self):
        while True:
            curves = {}
            for st in self.strategies:
                for pos in st.positions.values():
                    curves.setdefault(pos["curve"], None)
            for cv in list(curves):
                curves[cv] = await self.get_curve(cv)       # read each token once

            for st in self.strategies:
                g = st.genome
                for mint in list(st.positions):
                    pos = st.positions[mint]
                    c = curves.get(pos["curve"])
                    if c == "GONE":                       # curve account gone = rug
                        self.close(st, mint, 0.0, "rug")
                        continue
                    if not isinstance(c, dict):           # read failed (throttle/blip)
                        continue                          # skip; retry next cycle, never fake a loss
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

            self.pv.append({"t": int(time.time()), "v": round(self.best_equity(), 2)})
            if time.time() - self.last_evolve >= EVOLVE_INTERVAL_SEC:
                self.evolve()
            self.save()
            await self.broadcast()
            await asyncio.sleep(POLL_INTERVAL_SEC)

    def evolve(self):
        self.day_index += 1
        # 1. score the day for every strategy and fold into its lifetime record
        for st in self.strategies:
            day_score = self.equity(st) - st.window_start_equity
            st.day_pnls.append(round(day_score, 2))
            st.day_pnls = st.day_pnls[-60:]
            st.cum_pnl += day_score

        ranked = sorted(self.strategies, key=lambda st: st.day_pnls[-1], reverse=True)
        winner = ranked[0]
        winner.days_won += 1

        # 2. record this day's champion in the permanent ledger
        self.ledger.appendleft({
            "day": self.day_index, "date": now_date(),
            "combo_id": winner.combo_id, "genome": dict(winner.genome),
            "day_pnl": winner.day_pnls[-1], "cum_pnl": round(winner.cum_pnl, 2),
            "days_alive": winner.days_alive + 1, "days_won": winner.days_won,
        })

        # 3. survivors = top half of the day, PLUS a rescue for the all-time best
        #    cumulative combo so a proven long-run winner never dies on one bad day
        keep = ranked[:max(1, len(ranked) // 2)]
        best_cum = max(self.strategies, key=lambda st: st.cum_pnl)
        if best_cum not in keep:
            keep.append(best_cum)
        keep_ids = {id(s) for s in keep}
        parents = [st.genome for st in keep]

        for st in self.strategies:
            if id(st) in keep_ids:
                st.days_alive += 1                       # survived another day
            else:                                        # rebreed from a winner = new combo
                st.genome = mutate(random.choice(parents))
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

        # 4. update all-time hall-of-fame record
        champ = max(self.strategies, key=lambda st: st.cum_pnl)
        if not self.best_ever or champ.cum_pnl > self.best_ever["cum_pnl"]:
            self.best_ever = {"combo_id": champ.combo_id, "genome": dict(champ.genome),
                              "cum_pnl": round(champ.cum_pnl, 2),
                              "days_alive": champ.days_alive, "days_won": champ.days_won}

        self.generation += 1
        self.last_evolve = time.time()
        print(f"[evolve] day {self.day_index}: winner combo #{winner.combo_id} "
              f"(day {winner.day_pnls[-1]:+.2f}€, cum {winner.cum_pnl:+.2f}€)", flush=True)

    # ---- state / io ----
    def snapshot(self):
        board = []
        for st in self.strategies:
            eq = self.equity(st)
            last_day = st.day_pnls[-1] if st.day_pnls else 0
            board.append({
                "id": st.id, "combo": st.combo_id, "genome": st.genome,
                "equity": round(eq, 2),
                "score": round(eq - st.window_start_equity, 2),
                "realized": round(st.realized, 2), "trades": st.trades,
                "winrate": round(st.wins / st.trades * 100) if st.trades else 0,
                "open": len(st.positions),
                "days": st.days_alive, "won": st.days_won,
                "cum": round(st.cum_pnl, 2),
                "rising": st.days_alive <= 6 and st.cum_pnl > 0 and last_day > 0,
                "holds": [{"mint": p["mint"],
                           "pnl": round((p["value_eur"] / p["cost_eur"] - 1) * 100)}
                          for p in st.positions.values()],
            })
        board.sort(key=lambda x: x["equity"], reverse=True)
        t = time.time()
        return {
            "generation": self.generation, "pool": len(self.strategies),
            "sol_eur": round(self.sol_eur, 2), "trade_eur": TRADE_EUR,
            "start_cash": START_CASH_EUR, "day_index": self.day_index,
            "mode": BOT_MODE, "watch": len(self.watch),
            "evolve_in": max(0, int(EVOLVE_INTERVAL_SEC - (t - self.last_evolve))),
            "champion": board[0] if board else None, "board": board,
            "best_ever": self.best_ever, "ledger": list(self.ledger)[:30],
            "total_realized": round(sum(st.realized for st in self.strategies), 2),
            "launches": [{"mint": l["mint"], "dev_buy": l["dev_buy_sol"],
                          "age_sec": int(t - l["age"])} for l in self.launches],
            "recent": list(self.recent),
            "pv": list(self.pv),
        }

    async def broadcast(self):
        if not self.clients:
            return
        msg = json.dumps(self.snapshot())
        for ws in list(self.clients):
            try:
                await ws.send_str(msg)
            except Exception:
                self.clients.discard(ws)

    def save(self):
        data = {"generation": self.generation, "last_evolve": self.last_evolve,
                "next_id": self.next_id, "day_index": self.day_index,
                "next_combo_id": self.next_combo_id, "ledger": list(self.ledger),
                "best_ever": self.best_ever, "strategies": [
                    {"id": st.id, "genome": st.genome, "cash": st.cash,
                     "positions": st.positions, "realized": st.realized,
                     "trades": st.trades, "wins": st.wins,
                     "window_start_equity": st.window_start_equity,
                     "combo_id": st.combo_id, "born_day": st.born_day,
                     "days_alive": st.days_alive, "days_won": st.days_won,
                     "cum_pnl": st.cum_pnl, "day_pnls": st.day_pnls}
                    for st in self.strategies]}
        try:
            d = os.path.dirname(STATE_PATH)
            if d:
                os.makedirs(d, exist_ok=True)
            tmp = STATE_PATH + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, STATE_PATH)
        except Exception as e:
            print(f"[save] {e}", flush=True)

    def load(self):
        if not os.path.exists(STATE_PATH):
            return
        try:
            with open(STATE_PATH) as f:
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
            print(f"[load] resumed day {self.day_index}, gen {self.generation}, "
                  f"{len(self.strategies)} strategies, {len(self.ledger)} ledger days",
                  flush=True)
        except Exception as e:
            print(f"[load] {e}", flush=True)

    def csv_append(self, st, pos, pnl, reason):
        try:
            new = not os.path.exists(CSV_PATH)
            with open(CSV_PATH, "a", newline="") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(["time", "strategy_id", "mint", "reason", "pnl_eur",
                                "tp", "sl", "hold", "slip", "dev_max"])
                g = st.genome
                w.writerow([now_hms(), st.id, pos["mint"], reason, round(pnl, 3),
                            g["tp"], g["sl"], g["hold"], g["slip"], g["dev_max"]])
        except Exception as e:
            print(f"[csv] {e}", flush=True)

    async def reset(self):
        self.strategies = []
        self.next_id = 1
        self.next_combo_id = 1
        self.generation = 1
        self.day_index = 0
        self.last_evolve = time.time()
        self.pv.clear()
        self.recent.clear()
        self.ledger.clear()
        self.best_ever = None
        for _ in range(POOL_SIZE):
            self._new_strategy(rand_genome())
        self.save()
        await self.broadcast()


# ---------------------------------------------------------------------------
# League: aggregate several bot instances into one table
# ---------------------------------------------------------------------------
class League:
    def __init__(self, session):
        self.s = session
        self.data = {}          # label -> summary dict
        self.clients = set()

    async def reset(self):      # no-op so the shared /api/reset handler is happy
        pass

    async def poll(self):
        while True:
            for label, base in PEERS:
                url = base + "/api/state" + (f"?k={ACCESS_TOKEN}" if ACCESS_TOKEN else "")
                try:
                    async with self.s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                        st = await r.json()
                    champ = st.get("champion") or {}
                    be = st.get("best_ever") or {}
                    self.data[label] = {
                        "mode": st.get("mode"), "ok": True,
                        "champ_eq": champ.get("equity"),
                        "genome": champ.get("genome") or {},
                        "total_realized": st.get("total_realized"),
                        "day": st.get("day_index"), "pool": st.get("pool"),
                        "best_cum": be.get("cum_pnl"),
                        "start_cash": st.get("start_cash", 1000),
                    }
                except Exception as e:
                    self.data[label] = {"ok": False, "err": str(e)[:70]}
            await self.broadcast()
            await asyncio.sleep(10)

    def snapshot(self):
        rows = []
        for label, base in PEERS:
            d = self.data.get(label, {"ok": False, "err": "no data yet"})
            rows.append({"label": label, "url": base, **d})
        rows.sort(key=lambda r: (r.get("total_realized") if r.get("ok") else -9e9),
                  reverse=True)
        return {"mode": "league", "rows": rows, "peers": len(PEERS)}

    async def broadcast(self):
        if not self.clients:
            return
        msg = json.dumps(self.snapshot())
        for ws in list(self.clients):
            try:
                await ws.send_str(msg)
            except Exception:
                self.clients.discard(ws)


# ---------------------------------------------------------------------------
# Solana subscription
# ---------------------------------------------------------------------------
async def pumpportal_stream(pool):
    headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://pumpportal.fun"}
    backoff = 5
    while True:
        try:
            async with pool.s.ws_connect(PUMPPORTAL_WSS, max_msg_size=0,
                                         heartbeat=30, headers=headers) as ws:
                await ws.send_str(json.dumps({"method": "subscribeNewToken"}))
                backoff = 5
                print("subscribed to pump.fun launches (via PumpPortal, free)",
                      flush=True)
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        d = json.loads(msg.data)
                    except Exception:
                        continue
                    if not isinstance(d, dict):
                        continue
                    # ignore the subscribe-confirmation and anything without a mint
                    if d.get("mint") and (d.get("bondingCurveKey") or d.get("bonding_curve")):
                        info = parse_pp_token(d)
                        if info:
                            asyncio.create_task(pool.on_new_token(info))
        except Exception as e:
            hint = ("  (looks like a PumpPortal timeout — bans last ~1h and only ONE "
                    "connection is allowed) ") if "502" in str(e) else ""
            print(f"[pumpportal reconnect in {backoff}s] {e}{hint}", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)          # up to 5 min so a ban can expire


async def solana_stream(pool):
    sub = {"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
           "params": [{"mentions": [PUMP_PROGRAM]}, {"commitment": "processed"}]}
    backoff = 2
    while True:
        try:
            async with pool.s.ws_connect(RPC_WSS, max_msg_size=0) as ws:
                await ws.send_str(json.dumps(sub))
                await ws.receive()
                backoff = 2                        # reset once connected
                print("subscribed to pump.fun launches", flush=True)
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    detected_at = time.monotonic()
                    data = json.loads(msg.data)
                    if data.get("method") != "logsNotification":
                        continue
                    v = data["params"]["result"]["value"]
                    if v.get("err") is not None:
                        continue
                    if any(l == "Program log: Instruction: Create"
                           for l in v.get("logs", [])):
                        asyncio.create_task(pool.on_signature(
                            v["signature"], detected_at, v.get("logs", [])))
        except Exception as e:
            print(f"[reconnect in {backoff}s] {e}", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)          # ease off instead of hammering


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
    return web.Response(text="ok")            # public, no auth — for Railway healthchecks


async def h_index(request):
    if not authed(request):
        return web.Response(status=401, text=_DENY)
    return web.Response(text=(LEAGUE_PAGE if BOT_MODE == "league" else PAGE),
                        content_type="text/html")


async def h_state(request):
    if not authed(request):
        return web.Response(status=401, text=_DENY)
    return web.json_response(request.app["pool"].snapshot())


async def h_reset(request):
    if not authed(request):
        return web.Response(status=401, text=_DENY)
    await request.app["pool"].reset()
    return web.json_response({"ok": True})


async def h_ws(request):
    if not authed(request):
        return web.Response(status=401, text=_DENY)
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    pool = request.app["pool"]
    pool.clients.add(ws)
    await ws.send_str(json.dumps(pool.snapshot()))
    try:
        async for _ in ws:
            pass
    finally:
        pool.clients.discard(ws)
    return ws


async def on_startup(app):
    app["session"] = aiohttp.ClientSession()
    if BOT_MODE == "league":
        league = League(app["session"])
        app["pool"] = league
        app["tasks"] = [asyncio.create_task(league.poll())]
        return
    pool = Pool(app["session"])
    pool.load()
    app["pool"] = pool
    stream = pumpportal_stream if DATA_SOURCE == "pumpportal" else solana_stream
    app["tasks"] = [
        asyncio.create_task(stream(pool)),
        asyncio.create_task(pool.tracker()),
        asyncio.create_task(pool.price_feed()),
    ]
    if BOT_MODE == "hunt":
        app["tasks"].append(asyncio.create_task(pool.hunt_scan()))


async def on_cleanup(app):
    for t in app["tasks"]:
        t.cancel()
    await app["session"].close()


def make_app():
    app = web.Application()
    app.add_routes([
        web.get("/", h_index),
        web.get("/health", h_health),
        web.get("/api/state", h_state),
        web.post("/api/reset", h_reset),
        web.get("/ws", h_ws),
    ])
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
PAGE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>evolving desk</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root{--bg:#0d0f1a;--panel:#151932;--panel2:#1a1f3d;--line:#262c4a;
    --ink:#eceefb;--mut:#868cb2;--gain:#38e0b0;--loss:#ff7a8a;
    --accent:#7c6cff;--accent2:#a99bff;--gold:#f5c451}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);font-family:Archivo,system-ui,sans-serif;
    padding:16px 14px 60px;max-width:680px;margin:0 auto;-webkit-font-smoothing:antialiased}
  .mono{font-family:"JetBrains Mono",monospace;font-variant-numeric:tabular-nums}
  h2{font-size:13px;font-weight:600;color:var(--mut);margin:24px 0 10px}
  .sim{display:inline-block;font-size:11px;color:var(--accent2);border:1px solid var(--line);
    border-radius:999px;padding:2px 9px;margin-bottom:14px}
  .up{color:var(--gain)}.down{color:var(--loss)}
  .hero{background:linear-gradient(160deg,var(--panel),var(--panel2));border:1px solid var(--line);
    border-radius:18px;padding:18px}
  .hlabel{font-size:12px;color:var(--mut);display:flex;align-items:center;gap:7px}
  .crown{color:var(--gold)}
  .big{font-family:"JetBrains Mono",monospace;font-weight:700;font-size:38px;
    line-height:1.05;margin:3px 0 8px;letter-spacing:-.02em}
  .genes{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0 2px}
  .gene{display:flex;flex-direction:column;gap:2px;background:var(--bg);
    border:1px solid var(--line);border-radius:10px;padding:7px 11px;min-width:66px}
  .gl{font-size:10px;color:var(--mut);text-transform:uppercase;letter-spacing:.04em}
  .gv{font-family:"JetBrains Mono",monospace;font-size:14px;font-weight:600;color:var(--ink)}
  .coin{font-family:"JetBrains Mono",monospace;font-size:12.5px;color:var(--accent2);
    text-decoration:none;white-space:nowrap}
  .coin:active{opacity:.55}
  .subrow{display:flex;gap:16px;margin-top:12px;font-size:13px;flex-wrap:wrap}
  .subrow b{font-weight:600}
  .status{display:flex;gap:14px;font-size:12px;color:var(--mut);margin-top:14px;
    align-items:center;flex-wrap:wrap}
  .reset{background:transparent;border:1px solid var(--line);color:var(--mut);
    padding:6px 11px;border-radius:9px;font-family:Archivo;font-weight:600;font-size:12px;cursor:pointer}
  canvas{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:8px;margin-top:6px}
  .scard{background:var(--panel);border:1px solid var(--line);border-radius:14px;
    padding:14px;margin-bottom:11px}
  .scard.lead{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent) inset}
  .shead{display:flex;align-items:baseline;gap:10px}
  .rank{color:var(--mut);font-family:"JetBrains Mono",monospace;font-size:13px}
  .eq{font-size:16px;font-weight:700}
  .pl{margin-left:auto;font-size:14px;font-weight:600}
  .smeta{font-size:12px;color:var(--mut);margin-top:6px}
  .holds{margin-top:10px;font-size:12.5px;line-height:1.95;color:var(--mut)}
  .rrow{display:flex;align-items:center;gap:10px;padding:9px 2px;
    border-top:1px solid var(--line);font-size:13px}
  .rrow:first-child{border-top:none}
  .tag{font-size:11px;color:var(--mut);background:var(--bg);border:1px solid var(--line);
    border-radius:7px;padding:2px 7px}
  .rt{margin-left:auto;color:var(--mut);font-size:11px}
  .launches{line-height:2.2}
  .hof{background:linear-gradient(160deg,#1c1733,#241b3f);border:1px solid var(--gold);
    border-radius:14px;padding:14px}
  .hoflab{font-size:12px;color:var(--gold)}
  .hofbig{font-family:"JetBrains Mono",monospace;font-weight:700;font-size:26px;margin:4px 0}
  .lrow{display:flex;align-items:center;gap:9px;padding:9px 2px;
    border-top:1px solid var(--line);font-size:12.5px;flex-wrap:wrap}
  .lrow:first-child{border-top:none}
  .lday{font-family:"JetBrains Mono",monospace;color:var(--mut);min-width:52px}
  .badge{font-size:10px;font-weight:700;color:#0d0f1a;background:var(--gold);
    border-radius:6px;padding:1px 6px;margin-left:6px}
  .rise{font-size:10px;font-weight:700;color:var(--gain);border:1px solid var(--gain);
    border-radius:6px;padding:1px 6px;margin-left:6px}
  .empty{color:var(--mut);font-size:13px;padding:12px 2px}
  .foot{color:var(--mut);font-size:11px;margin-top:22px;line-height:1.5}
</style></head>
<body>
<div class="sim" id="modebadge">simulated cash · evolving strategy pool · no real money</div>

<div class="hero">
  <div class="hlabel"><span class="crown">♛</span> Best strategy right now</div>
  <div class="big" id="champEq">€—</div>
  <div id="champGenes"></div>
  <div class="subrow">
    <span>All-time P&amp;L <b class="mono" id="champPnl">—</b></span>
    <span>Trades <b class="mono" id="champTrades">—</b></span>
    <span>Win <b class="mono" id="champWin">—</b></span>
  </div>
  <div class="status">
    <span>Generation <b class="mono" id="gen">—</b></span>
    <span>Pool <b class="mono" id="pool">—</b></span>
    <span>Evolves in <b class="mono" id="evolve">—</b></span>
    <button class="reset" onclick="resetAll()">New random pool</button>
  </div>
</div>

<h2>Best strategy equity over time</h2>
<canvas id="chart" height="150"></canvas>

<h2>🏆 Hall of fame — best combination ever</h2>
<div id="hof"><div class="empty">Fills in after the first daily evolve…</div></div>

<h2>Daily champions — winner of each day</h2>
<div id="ledger"><div class="empty">No days completed yet…</div></div>

<h2>Recent snipes — what got bought &amp; how it ended</h2>
<div id="recent"><div class="empty">No closed trades yet…</div></div>

<h2>Leaderboard — living strategies, best first</h2>
<div id="board"><div class="empty">Warming up…</div></div>

<h2>Live launches (tap to open on pump.fun)</h2>
<div id="launches" class="launches"><div class="empty">Waiting for the next mint…</div></div>

<div class="foot" id="foot"></div>

<script>
const K=new URLSearchParams(location.search).get('k')||'';
const Q=K?('?k='+encodeURIComponent(K)):'';
let chart;
function eur(n){return "€"+n.toFixed(2)}
function sgn(n){return (n>=0?"+":"")+n.toFixed(2)}
function pct(n){return (n>=0?"+":"")+Math.round(n)+"%"}
function short(a){return a.slice(0,4)+"…"+a.slice(-4)}
function coin(m,label){return `<a class="coin" target="_blank" rel="noopener" href="https://pump.fun/coin/${m}">${label||short(m)}</a>`}
const REASON={tp:"2× hit",sl:"stopped out",timeout:"timed out",rug:"rugged",
  dead:"went dead",graduated:"graduated",manual:"sold"};
const GLABEL={tp:'take',sl:'stop',hold:'hold',slip:'slip',dev_max:'dev ≤',
  dev_min:'dev ≥',top_hold_max:'top ≤',mom_pct:'pump ≥',mom_window:'window'};
function gval(k,v){
  if(k==='sl')return Math.round(v*100)+'%';
  if(k==='tp')return v+'×';
  if(k==='hold'||k==='mom_window')return v+'s';
  if(k==='dev_max'||k==='dev_min')return v+'◎';
  return v+'%';
}
function chip(l,v){return `<div class="gene"><span class="gl">${l}</span><span class="gv">${v}</span></div>`}
function genes(g){return `<div class="genes">`+Object.keys(g).map(k=>chip(GLABEL[k]||k,gval(k,g[k]))).join('')+`</div>`}
function genesShort(g){return Object.keys(g).map(k=>(GLABEL[k]||k)+' '+gval(k,g[k])).join(' · ')}

function render(s){
  const MODES={snipe:"🎯 SNIPER (basic)",smart:"🛡️ SMART SNIPER (anti-rug)",hunt:"📈 HUNTER (momentum)"};
  document.getElementById("modebadge").textContent=
    (MODES[s.mode]||s.mode)+" · simulated cash · no real money"+(s.mode==="hunt"?" · watching "+s.watch:"");
  const c=s.champion;
  if(c){
    document.getElementById("champEq").textContent=eur(c.equity);
    document.getElementById("champGenes").innerHTML=genes(c.genome);
    const p=document.getElementById("champPnl");
    p.textContent=sgn(c.realized)+"€"; p.className="mono "+(c.realized>=0?"up":"down");
    document.getElementById("champTrades").textContent=c.trades;
    document.getElementById("champWin").textContent=c.winrate+"%";
  }
  document.getElementById("gen").textContent=s.generation;
  document.getElementById("pool").textContent=s.pool;
  const m=Math.floor(s.evolve_in/60), sec=s.evolve_in%60;
  document.getElementById("evolve").textContent=m+"m "+sec+"s";

  const hof=s.best_ever;
  document.getElementById("hof").innerHTML = hof?
    `<div class="hof"><div class="hoflab">combo #${hof.combo_id} · best cumulative P&L ever</div>
     <div class="hofbig ${hof.cum_pnl>=0?'up':'down'}">${sgn(hof.cum_pnl)}€</div>
     ${genes(hof.genome)}
     <div class="smeta">survived ${hof.days_alive} days · won ${hof.days_won} days</div></div>`
    : '<div class="empty">Fills in after the first daily evolve…</div>';

  document.getElementById("ledger").innerHTML = (s.ledger&&s.ledger.length)?
    s.ledger.map(d=>{
      return `<div class="lrow"><span class="lday">Day ${d.day}</span>
        <span class="gene" style="flex-direction:row;gap:6px;padding:4px 8px">
          <span class="gv">${genesShort(d.genome)}</span></span>
        <span class="mono ${d.day_pnl>=0?'up':'down'}">${sgn(d.day_pnl)}€</span>
        <span class="rt">#${d.combo_id}${d.days_alive>1?' · '+d.days_alive+'d streak':''}</span></div>`}).join("")
    : '<div class="empty">No days completed yet…</div>';

  document.getElementById("board").innerHTML=s.board.map((b,i)=>{
    const cl=b.realized>=0?"up":"down";
    const holds=(b.holds&&b.holds.length)?
      `<div class="holds">holding: `+b.holds.map(h=>
        coin(h.mint)+` <span class="${h.pnl>=0?'up':'down'}">${pct(h.pnl)}</span>`).join(" · ")+`</div>`:"";
    const tags=(i===0?'<span class="badge">LEADER</span>':'')+(b.rising?'<span class="rise">🔥 RISING</span>':'');
    return `<div class="scard ${i===0?'lead':''}">
      <div class="shead"><span class="rank">#${i+1}</span>
        <span class="eq mono">${eur(b.equity)}</span>
        <span class="pl mono ${cl}">${sgn(b.realized)}€</span></div>
      ${genes(b.genome)}
      <div class="smeta">combo #${b.combo}${tags} · alive ${b.days}d · won ${b.won}d · cum <span class="${b.cum>=0?'up':'down'}">${sgn(b.cum)}€</span></div>
      <div class="smeta">${b.trades} trades · ${b.winrate}% win · holding ${b.open}</div>
      ${holds}</div>`}).join("");

  document.getElementById("recent").innerHTML = (s.recent&&s.recent.length)?
    s.recent.map(r=>`<div class="rrow">${coin(r.mint)}
      <span class="tag">${REASON[r.reason]||r.reason}</span>
      <span class="mono ${r.pnl>=0?'up':'down'}">${sgn(r.pnl)}€</span>
      <span class="rt">#${r.id} · ${r.time}</span></div>`).join("")
    : '<div class="empty">No closed trades yet…</div>';

  document.getElementById("launches").innerHTML = s.launches.length?
    s.launches.map(l=>coin(l.mint, short(l.mint)
      +(l.dev_buy!=null?" ("+l.dev_buy.toFixed(2)+"◎)":"")+" · "+l.age_sec+"s")).join("<br>")
    : '<div class="empty">Waiting for the next mint…</div>';

  document.getElementById("foot").textContent=
    `Pool total P&L ${sgn(s.total_realized)}€ · each strategy trades €${s.trade_eur} · `
    +`SOL €${s.sol_eur} · simulated, no wallet · the top strategy is a hypothesis, not a guarantee`;
  drawChart(s.pv);
}

function drawChart(pv){
  const data=pv.map(p=>p.v), labels=pv.map(_=>"");
  if(!chart){chart=new Chart(document.getElementById("chart"),{type:"line",
    data:{labels,datasets:[{data,borderColor:"#f5c451",borderWidth:2,fill:true,
      backgroundColor:"rgba(245,196,81,.10)",tension:.25,pointRadius:0}]},
    options:{animation:false,plugins:{legend:{display:false}},
      scales:{x:{display:false},y:{ticks:{color:"#868cb2",font:{family:"JetBrains Mono"}},
        grid:{color:"#262c4a"}}}}});}
  else{chart.data.labels=labels;chart.data.datasets[0].data=data;chart.update();}
}

async function resetAll(){if(confirm("Throw away the whole pool and start from random settings?"))
  await fetch("/api/reset"+Q,{method:"POST"});}

function connect(){
  const proto=location.protocol==="https:"?"wss:":"ws:";
  const ws=new WebSocket(proto+"//"+location.host+"/ws"+Q);
  ws.onmessage=e=>render(JSON.parse(e.data));
  ws.onclose=()=>setTimeout(connect,1500);
}
connect();
</script>
</body></html>"""


LEAGUE_PAGE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>bot league</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root{--bg:#0d0f1a;--panel:#151932;--panel2:#1a1f3d;--line:#262c4a;--ink:#eceefb;
    --mut:#868cb2;--gain:#38e0b0;--loss:#ff7a8a;--accent:#7c6cff;--gold:#f5c451}
  *{box-sizing:border-box}
  body{margin:0 auto;background:var(--bg);color:var(--ink);font-family:Archivo,system-ui,sans-serif;
    padding:18px 14px 60px;max-width:680px;-webkit-font-smoothing:antialiased}
  .mono{font-family:"JetBrains Mono",monospace;font-variant-numeric:tabular-nums}
  h1{font-size:19px;margin:0 0 4px}
  .sub{color:var(--mut);font-size:12px;margin-bottom:18px}
  .up{color:var(--gain)}.down{color:var(--loss)}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:16px;
    padding:16px;margin-bottom:12px}
  .card.win{border-color:var(--gold);box-shadow:0 0 0 1px var(--gold) inset}
  .top{display:flex;align-items:baseline;gap:10px}
  .medal{font-size:18px}
  .name{font-size:16px;font-weight:700}
  .big{margin-left:auto;font-family:"JetBrains Mono",monospace;font-weight:700;font-size:22px}
  .kv{display:flex;flex-wrap:wrap;gap:14px;margin-top:10px;font-size:12.5px;color:var(--mut)}
  .kv b{color:var(--ink);font-weight:600}
  .genes{display:flex;flex-wrap:wrap;gap:6px;margin-top:11px}
  .gene{font-family:"JetBrains Mono",monospace;font-size:11.5px;background:var(--bg);
    border:1px solid var(--line);border-radius:8px;padding:4px 8px;color:#a99bff}
  .err{color:var(--loss);font-size:12px;margin-top:8px}
  .empty{color:var(--mut);font-size:13px;padding:14px 2px}
  .foot{color:var(--mut);font-size:11px;margin-top:20px;line-height:1.5}
</style></head>
<body>
<h1>🏁 Bot league</h1>
<div class="sub" id="sub">which brain is winning · simulated cash</div>
<div id="rows"><div class="empty">Fetching the bots…</div></div>
<div class="foot">Each bot runs the same evolutionary engine on a different strategy.
Ranked by total realized P&amp;L. Numbers are simulated — a leader here is a
hypothesis, not proof it works with real money.</div>
<script>
const K=new URLSearchParams(location.search).get('k')||'';
const Q=K?('?k='+encodeURIComponent(K)):'';
const MODES={snipe:"🎯 Sniper",smart:"🛡️ Smart sniper",hunt:"📈 Hunter"};
const GLABEL={tp:'take',sl:'stop',hold:'hold',slip:'slip',dev_max:'dev ≤',
  dev_min:'dev ≥',top_hold_max:'top ≤',mom_pct:'pump ≥',mom_window:'window'};
function gval(k,v){if(k==='sl')return Math.round(v*100)+'%';if(k==='tp')return v+'×';
  if(k==='hold'||k==='mom_window')return v+'s';if(k==='dev_max'||k==='dev_min')return v+'◎';return v+'%';}
function sgn(n){return (n>=0?"+":"")+Number(n).toFixed(2)}
const MEDAL=["🥇","🥈","🥉"];
function render(s){
  document.getElementById("sub").textContent=
    s.peers+" bots · which brain is winning · simulated cash";
  if(!s.rows.length){document.getElementById("rows").innerHTML=
    '<div class="empty">No bots configured. Set the PEERS variable.</div>';return;}
  document.getElementById("rows").innerHTML=s.rows.map((r,i)=>{
    if(!r.ok) return `<div class="card"><div class="top"><span class="name">${r.label}</span></div>
      <div class="err">can't reach this bot: ${r.err||''}</div></div>`;
    const pnl=r.total_realized||0, cl=pnl>=0?"up":"down";
    const g=r.genome||{};
    const chips=Object.keys(g).map(k=>`<span class="gene">${GLABEL[k]||k} ${gval(k,g[k])}</span>`).join("");
    return `<div class="card ${i===0?'win':''}">
      <div class="top"><span class="medal">${MEDAL[i]||''}</span>
        <span class="name">${r.label} · ${MODES[r.mode]||r.mode||''}</span>
        <span class="big ${cl}">${sgn(pnl)}€</span></div>
      <div class="kv"><span>champion equity <b class="mono">€${(r.champ_eq||0).toFixed(2)}</b></span>
        <span>best combo <b class="mono ${(r.best_cum||0)>=0?'up':'down'}">${r.best_cum!=null?sgn(r.best_cum)+'€':'—'}</b></span>
        <span>day <b class="mono">${r.day||0}</b></span>
        <span>pool <b class="mono">${r.pool||0}</b></span></div>
      <div class="genes">${chips}</div></div>`}).join("");
}
function connect(){
  const proto=location.protocol==="https:"?"wss:":"ws:";
  const ws=new WebSocket(proto+"//"+location.host+"/ws"+Q);
  ws.onmessage=e=>render(JSON.parse(e.data));
  ws.onclose=()=>setTimeout(connect,1500);
}
connect();
</script>
</body></html>"""


if __name__ == "__main__":
    print(f"HTTP {RPC_HTTP}\nWSS  {RPC_WSS}")
    _kt = RPC_HTTP.split("api-key=")[-1][-4:] if "api-key=" in RPC_HTTP else "n/a"
    print(f"BOT_MODE={BOT_MODE}  data={DATA_SOURCE}  rpc key ...{_kt}  pool={POOL_SIZE}  "
          f"evolve every {EVOLVE_INTERVAL_SEC}s  trade=€{TRADE_EUR}  "
          f"dashboard -> http://localhost:{PORT}")
    print(f"state file: {STATE_PATH!r}   trades: {CSV_PATH!r}")
    print("SIMULATED CASH ONLY — no wallet, no real trades\n")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)
