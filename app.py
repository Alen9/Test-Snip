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

START_CASH_EUR = float(os.environ.get("START_CASH_EUR", "1000"))
TRADE_EUR = float(os.environ.get("TRADE_EUR", "10"))
SIM_LATENCY_MS = int(os.environ.get("SIM_LATENCY_MS", "600"))
FEE_BPS = 100

POOL_SIZE = int(os.environ.get("POOL_SIZE", "12"))
EVOLVE_INTERVAL_SEC = int(os.environ.get("EVOLVE_INTERVAL_SEC", "3600"))  # 1h; 86400 = per day
MAX_POS_PER_STRAT = int(os.environ.get("MAX_POS_PER_STRAT", "15"))
POLL_INTERVAL_SEC = float(os.environ.get("POLL_INTERVAL_SEC", "3"))  # raise to 6-8 on free RPC

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

# ---------------------------------------------------------------------------
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_DISC = bytes([24, 30, 200, 40, 5, 28, 7, 119])
LAMPORTS = 1_000_000_000
TOKEN_UNITS = 1_000_000
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# Strategy "genes" and their allowed ranges.
GENE_BOUNDS = {
    "tp":      (1.2, 5.0),    # take profit multiple (x cost)
    "sl":      (0.20, 0.80),  # stop loss (fraction of cost lost)
    "hold":    (30, 300),     # max hold seconds
    "slip":    (8.0, 50.0),   # skip launch if it ran up more than this % while landing
    "dev_max": (0.3, 25.0),   # skip launch if dev bought more than this many SOL
}


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


# ---------------------------------------------------------------------------
# Genes
# ---------------------------------------------------------------------------
def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def rand_genome():
    return {
        "tp": round(random.uniform(*GENE_BOUNDS["tp"]), 2),
        "sl": round(random.uniform(*GENE_BOUNDS["sl"]), 2),
        "hold": random.randint(*GENE_BOUNDS["hold"]),
        "slip": round(random.uniform(*GENE_BOUNDS["slip"]), 1),
        "dev_max": round(random.uniform(*GENE_BOUNDS["dev_max"]), 1),
    }


def mutate(g):
    ng = dict(g)
    for k, (lo, hi) in GENE_BOUNDS.items():
        if random.random() < 0.5:                 # mutate ~half the genes
            v = clamp(ng[k] * random.uniform(0.7, 1.3), lo, hi)
            ng[k] = int(round(v)) if k == "hold" else round(v, 1 if k == "slip" else 2)
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
        self.clients = set()
        for _ in range(POOL_SIZE):
            self._new_strategy(rand_genome())

    def _new_strategy(self, genome):
        st = Strategy(id=self.next_id, genome=genome, cash=START_CASH_EUR,
                      window_start_equity=START_CASH_EUR)
        self.next_id += 1
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

        c0 = await self.get_curve(info["curve"])
        p0 = spot_price(c0)
        elapsed = time.monotonic() - detected_at
        await asyncio.sleep(max(SIM_LATENCY_MS / 1000 - elapsed, 0))
        c1 = await self.get_curve(info["curve"])
        if not isinstance(c1, dict) or c1["complete"]:
            return
        p1 = spot_price(c1)
        for st in self.strategies:
            self.try_enter(st, info, p0, p1, c1)
        await self.broadcast()

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

    def try_enter(self, st, info, p0, p1, c1):
        mint = info["mint"]
        g = st.genome
        if mint in st.positions or st.cash < TRADE_EUR:
            return
        if len(st.positions) >= MAX_POS_PER_STRAT:
            return
        if info["dev_buy_sol"] is not None and info["dev_buy_sol"] > g["dev_max"]:
            return                                      # filter: dev grabbed too much
        if p0 > 0 and p1 > 0 and (p1 / p0 - 1) > g["slip"] / 100:
            return                                      # too much run-up = would miss
        sol_in = int((TRADE_EUR / self.sol_eur) * LAMPORTS)
        tokens = buy_quote(c1, sol_in)
        if tokens <= 0:
            return
        st.cash -= TRADE_EUR
        st.positions[mint] = {
            "mint": mint, "curve": info["curve"], "tokens": tokens,
            "cost_eur": TRADE_EUR, "entry_ts": time.time(), "value_eur": TRADE_EUR,
        }

    def close(self, st, mint, proceeds, reason):
        pos = st.positions.pop(mint)
        pnl = proceeds - pos["cost_eur"]
        st.cash += proceeds
        st.realized += pnl
        st.trades += 1
        if pnl > 0:
            st.wins += 1
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
                    if not isinstance(c, dict):           # transient read failure
                        pos["fails"] = pos.get("fails", 0) + 1
                        if pos["fails"] >= 4:             # unreadable too long = dead
                            self.close(st, mint, 0.0, "dead")
                        continue
                    pos["fails"] = 0
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
        ranked = sorted(self.strategies,
                        key=lambda st: self.equity(st) - st.window_start_equity,
                        reverse=True)
        keep = ranked[:max(1, len(ranked) // 2)]         # elites survive
        drop = ranked[len(keep):]
        parents = [st.genome for st in keep]
        for st in drop:                                   # rebirth losers from winners
            st.genome = mutate(random.choice(parents))
            st.cash = START_CASH_EUR
            st.positions = {}
            st.realized = 0.0
            st.trades = 0
            st.wins = 0
            st.id = self.next_id
            self.next_id += 1
        for st in self.strategies:
            st.window_start_equity = self.equity(st)
        self.generation += 1
        self.last_evolve = time.time()
        print(f"[evolve] gen {self.generation}: kept {len(keep)}, rebred {len(drop)}",
              flush=True)

    # ---- state / io ----
    def snapshot(self):
        board = []
        for st in self.strategies:
            eq = self.equity(st)
            board.append({
                "id": st.id, "genome": st.genome, "equity": round(eq, 2),
                "score": round(eq - st.window_start_equity, 2),
                "realized": round(st.realized, 2), "trades": st.trades,
                "winrate": round(st.wins / st.trades * 100) if st.trades else 0,
                "open": len(st.positions),
            })
        board.sort(key=lambda x: x["equity"], reverse=True)
        t = time.time()
        return {
            "generation": self.generation, "pool": len(self.strategies),
            "sol_eur": round(self.sol_eur, 2), "trade_eur": TRADE_EUR,
            "start_cash": START_CASH_EUR,
            "evolve_in": max(0, int(EVOLVE_INTERVAL_SEC - (t - self.last_evolve))),
            "champion": board[0] if board else None, "board": board,
            "total_realized": round(sum(st.realized for st in self.strategies), 2),
            "launches": [{"mint": l["mint"], "dev_buy": l["dev_buy_sol"],
                          "age_sec": int(t - l["age"])} for l in self.launches],
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
                "next_id": self.next_id, "strategies": [
                    {"id": st.id, "genome": st.genome, "cash": st.cash,
                     "positions": st.positions, "realized": st.realized,
                     "trades": st.trades, "wins": st.wins,
                     "window_start_equity": st.window_start_equity}
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
            self.strategies = [
                Strategy(id=d["id"], genome=d["genome"], cash=d["cash"],
                         positions=d.get("positions", {}), realized=d.get("realized", 0.0),
                         trades=d.get("trades", 0), wins=d.get("wins", 0),
                         window_start_equity=d.get("window_start_equity", START_CASH_EUR))
                for d in data.get("strategies", [])]
            print(f"[load] resumed gen {self.generation}, {len(self.strategies)} strategies",
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
        self.generation = 1
        self.last_evolve = time.time()
        self.pv.clear()
        for _ in range(POOL_SIZE):
            self._new_strategy(rand_genome())
        self.save()
        await self.broadcast()


# ---------------------------------------------------------------------------
# Solana subscription
# ---------------------------------------------------------------------------
async def solana_stream(pool):
    sub = {"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
           "params": [{"mentions": [PUMP_PROGRAM]}, {"commitment": "processed"}]}
    while True:
        try:
            async with pool.s.ws_connect(RPC_WSS, max_msg_size=0) as ws:
                await ws.send_str(json.dumps(sub))
                await ws.receive()
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
            print(f"[reconnect] {e}", flush=True)
            await asyncio.sleep(2)


# ---------------------------------------------------------------------------
# Web
# ---------------------------------------------------------------------------
def authed(request):
    if not ACCESS_TOKEN:
        return True
    return (request.query.get("k") == ACCESS_TOKEN
            or request.headers.get("X-Access-Token") == ACCESS_TOKEN)


_DENY = "Unauthorized — add ?k=YOUR_TOKEN to the URL"


async def h_index(request):
    if not authed(request):
        return web.Response(status=401, text=_DENY)
    return web.Response(text=PAGE, content_type="text/html")


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
    pool = Pool(app["session"])
    pool.load()
    app["pool"] = pool
    app["tasks"] = [
        asyncio.create_task(solana_stream(pool)),
        asyncio.create_task(pool.tracker()),
        asyncio.create_task(pool.price_feed()),
    ]


async def on_cleanup(app):
    for t in app["tasks"]:
        t.cancel()
    await app["session"].close()


def make_app():
    app = web.Application()
    app.add_routes([
        web.get("/", h_index),
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
  .genes{display:flex;flex-wrap:wrap;gap:6px;margin-top:6px}
  .gene{font-family:"JetBrains Mono",monospace;font-size:12px;background:var(--bg);
    border:1px solid var(--line);border-radius:8px;padding:4px 8px;color:var(--accent2)}
  .subrow{display:flex;gap:16px;margin-top:12px;font-size:13px;flex-wrap:wrap}
  .subrow b{font-weight:600}
  .status{display:flex;gap:14px;font-size:12px;color:var(--mut);margin-top:14px;
    align-items:center;flex-wrap:wrap}
  .reset{background:transparent;border:1px solid var(--line);color:var(--mut);
    padding:6px 11px;border-radius:9px;font-family:Archivo;font-weight:600;font-size:12px;cursor:pointer}
  canvas{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:8px;margin-top:6px}
  table{width:100%;border-collapse:collapse;font-size:12.5px}
  th{text-align:left;color:var(--mut);font-weight:600;padding:6px 4px;font-size:11px}
  td{padding:9px 4px;border-top:1px solid var(--line)}
  td.mono,th.r{text-align:right}
  td.mono{font-family:"JetBrains Mono",monospace}
  tr.top td{background:rgba(124,108,255,.08)}
  .rank{color:var(--mut);width:20px}
  .empty{color:var(--mut);font-size:13px;padding:12px 2px}
  .foot{color:var(--mut);font-size:11px;margin-top:22px;line-height:1.5}
  .launch{display:inline-block;font-family:"JetBrains Mono",monospace;font-size:11px;
    color:var(--mut);margin:2px 8px 2px 0}
</style></head>
<body>
<div class="sim">simulated cash · evolving strategy pool · no real money</div>

<div class="hero">
  <div class="hlabel"><span class="crown">♛</span> Best strategy right now</div>
  <div class="big" id="champEq">€—</div>
  <div class="genes" id="champGenes"></div>
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

<h2>Leaderboard — every strategy, best first</h2>
<div id="board"><div class="empty">Warming up…</div></div>

<h2>Live launches</h2>
<div id="launches"><div class="empty">Waiting for the next mint…</div></div>

<div class="foot" id="foot"></div>

<script>
const K=new URLSearchParams(location.search).get('k')||'';
const Q=K?('?k='+encodeURIComponent(K)):'';
let chart;
function eur(n){return "€"+n.toFixed(2)}
function sgn(n){return (n>=0?"+":"")+n.toFixed(2)}
function genes(g){return `<span class="gene">TP ${g.tp}×</span>
  <span class="gene">SL ${Math.round(g.sl*100)}%</span>
  <span class="gene">≤${g.hold}s</span>
  <span class="gene">dev≤${g.dev_max}◎</span>
  <span class="gene">slip ${g.slip}%</span>`}

function render(s){
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

  let rows=s.board.map((b,i)=>{
    const cl=b.realized>=0?"up":"down";
    return `<tr class="${i===0?'top':''}">
      <td class="rank">${i+1}</td>
      <td>${genes(b.genome)}</td>
      <td class="mono">${eur(b.equity)}</td>
      <td class="mono ${cl}">${sgn(b.realized)}</td>
      <td class="mono">${b.trades}</td>
      <td class="mono">${b.winrate}%</td></tr>`}).join("");
  document.getElementById("board").innerHTML=
    `<table><tr><th class="rank"></th><th>strategy (its settings)</th>
     <th class="r">equity</th><th class="r">P&amp;L</th><th class="r">trades</th>
     <th class="r">win</th></tr>${rows}</table>`;

  document.getElementById("launches").innerHTML = s.launches.length?
    s.launches.map(l=>`<span class="launch">${l.mint.slice(0,4)}…${l.mint.slice(-4)}
      ${l.dev_buy!=null?"("+l.dev_buy.toFixed(2)+"◎)":""} ${l.age_sec}s</span>`).join("")
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


if __name__ == "__main__":
    print(f"HTTP {RPC_HTTP}\nWSS  {RPC_WSS}")
    print(f"pool={POOL_SIZE}  evolve every {EVOLVE_INTERVAL_SEC}s  "
          f"trade=€{TRADE_EUR}  dashboard -> http://localhost:{PORT}")
    print(f"state file: {STATE_PATH!r}   trades: {CSV_PATH!r}")
    print("SIMULATED CASH ONLY — no wallet, no real trades\n")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)
