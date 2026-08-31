"""
pump.fun paper-trading desk
===========================
Detects pump.fun launches, SIMULATES buying/selling them with realistic latency
and curve math, and serves a live phone-friendly dashboard over HTTP.

SIMULATION ONLY. No wallet, no private key, no real transactions anywhere.
It reads Solana state and moves imaginary euros. You cannot lose real money
with this file, and you cannot make any either — it's a measurement tool.

Run
---
    pip install aiohttp
    export RPC_HTTP="https://your-endpoint"      # Helius / Triton / QuickNode
    export RPC_WSS="wss://your-endpoint"
    python app.py
    # open http://localhost:8080  (or the forwarded HTTPS URL in Codespaces)

See README.md for running it from your phone via GitHub Codespaces.
"""

import asyncio
import base64
import json
import os
import struct
import time
from collections import deque
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

# ---------------------------------------------------------------------------
# Config (env-overridable)
# ---------------------------------------------------------------------------
RPC_HTTP = os.environ.get("RPC_HTTP", "https://mainnet.helius-rpc.com/?api-key=b842bf4c-c718-48ca-92d0-dbdc408e0b0c")
RPC_WSS = os.environ.get("RPC_WSS", "wss://mainnet.helius-rpc.com/?api-key=b842bf4c-c718-48ca-92d0-dbdc408e0b0c")
PORT = int(os.environ.get("PORT", "8080"))

START_CASH_EUR = float(os.environ.get("START_CASH_EUR", "1000"))
TRADE_EUR = float(os.environ.get("TRADE_EUR", "10"))          # size per trade
SIM_LATENCY_MS = int(os.environ.get("SIM_LATENCY_MS", "600")) # detect -> land
SLIPPAGE_TOL_PCT = float(os.environ.get("SLIPPAGE_TOL_PCT", "25"))  # miss if run-up exceeds this
FEE_BPS = 100                                                 # pump.fun 1% fee

TAKE_PROFIT_X = float(os.environ.get("TAKE_PROFIT_X", "2.0"))
STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0.5"))
MAX_HOLD_SEC = int(os.environ.get("MAX_HOLD_SEC", "120"))
POLL_INTERVAL_SEC = float(os.environ.get("POLL_INTERVAL_SEC", "3"))
MAX_POSITIONS = int(os.environ.get("MAX_POSITIONS", "25"))

# ---------------------------------------------------------------------------
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_DISC = bytes([24, 30, 200, 40, 5, 28, 7, 119])
LAMPORTS = 1_000_000_000
TOKEN_UNITS = 1_000_000
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(s):
    n = 0
    for ch in s:
        n = n * 58 + _B58.index(ch)
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\x00" * (len(s) - len(s.lstrip("1"))) + body


def now_hms():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Curve math (constant product with virtual reserves)
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
    if c["vt"] == 0:
        return 0.0
    return (c["vs"] / LAMPORTS) / (c["vt"] / TOKEN_UNITS)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
class Engine:
    def __init__(self, session):
        self.s = session
        self.cash = START_CASH_EUR
        self.sol_eur = 150.0
        self.trade_eur = TRADE_EUR
        self.auto = False
        self.realized = 0.0
        self.positions = {}          # mint -> dict
        self.closed = deque(maxlen=60)
        self.launches = deque(maxlen=40)
        self.pv = deque(maxlen=240)   # portfolio value history
        self.missed = 0
        self.clients = set()

    # ---- RPC ----
    async def rpc(self, method, params):
        try:
            async with self.s.post(RPC_HTTP, json={
                    "jsonrpc": "2.0", "id": 1, "method": method,
                    "params": params}, timeout=aiohttp.ClientTimeout(total=8)) as r:
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
        if not res or not res.get("value"):
            return None
        return parse_curve(base64.b64decode(res["value"]["data"][0]))

    # ---- detection ----
    async def on_signature(self, sig, detected_at):
        tx = await self.get_tx(sig)
        if not tx:
            return
        info = self.parse_create(tx)
        if not info:
            return
        info["age"] = time.time()
        info["held"] = False
        self.launches.appendleft(info)
        await self.broadcast()
        if self.auto:
            await self.simulate_buy(info, detected_at, manual=False)

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

    # ---- trading (simulated) ----
    async def simulate_buy(self, info, detected_at=None, manual=True):
        mint = info["mint"]
        if mint in self.positions or self.cash < self.trade_eur:
            return
        if len(self.positions) >= MAX_POSITIONS:
            return

        price_at_detect = None
        if not manual:
            c0 = await self.get_curve(info["curve"])
            price_at_detect = spot_price(c0) if c0 else None
            # model the tx landing SIM_LATENCY_MS after detection
            elapsed = time.monotonic() - detected_at if detected_at else 0
            wait = max(SIM_LATENCY_MS / 1000 - elapsed, 0)
            await asyncio.sleep(wait)

        c = await self.get_curve(info["curve"])
        if not c or c["complete"]:
            return

        sol_in = int((self.trade_eur / self.sol_eur) * LAMPORTS)
        tokens = buy_quote(c, sol_in)
        if tokens <= 0:
            return
        entry_price = (sol_in / LAMPORTS) / (tokens / TOKEN_UNITS)

        # slippage guard: if it ran up past tolerance while we were landing, miss it
        if price_at_detect and price_at_detect > 0:
            runup = entry_price / price_at_detect - 1
            if runup > SLIPPAGE_TOL_PCT / 100:
                self.missed += 1
                await self.broadcast()
                return

        self.cash -= self.trade_eur
        info["held"] = True
        self.positions[mint] = {
            "mint": mint, "curve": info["curve"], "tokens": tokens,
            "cost_eur": self.trade_eur, "entry_price": entry_price,
            "entry_ts": time.monotonic(), "value_eur": self.trade_eur,
            "pnl_pct": 0.0, "dev_buy_sol": info.get("dev_buy_sol"),
        }
        await self.broadcast()

    async def simulate_sell(self, mint, reason):
        pos = self.positions.get(mint)
        if not pos:
            return
        c = await self.get_curve(pos["curve"])
        proceeds_eur = pos["value_eur"]
        if c:
            proceeds_eur = (sell_quote(c, pos["tokens"]) / LAMPORTS) * self.sol_eur
        pnl = proceeds_eur - pos["cost_eur"]
        self.cash += proceeds_eur
        self.realized += pnl
        self.closed.appendleft({
            "mint": mint, "reason": reason, "pnl_eur": round(pnl, 3),
            "pnl_pct": round((proceeds_eur / pos["cost_eur"] - 1) * 100, 1),
            "hold_sec": round(time.monotonic() - pos["entry_ts"]),
            "time": now_hms(),
        })
        del self.positions[mint]
        for l in self.launches:
            if l["mint"] == mint:
                l["held"] = False
        await self.broadcast()

    # ---- background loops ----
    async def price_feed(self):
        while True:
            r = await self.rpc_price()
            if r:
                self.sol_eur = r
            await asyncio.sleep(60)

    async def rpc_price(self):
        try:
            url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=eur"
            async with self.s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                return (await r.json())["solana"]["eur"]
        except Exception:
            return None

    async def tracker(self):
        while True:
            for mint in list(self.positions.keys()):
                pos = self.positions.get(mint)
                if not pos:
                    continue
                c = await self.get_curve(pos["curve"])
                if not c:
                    continue
                val = (sell_quote(c, pos["tokens"]) / LAMPORTS) * self.sol_eur
                pos["value_eur"] = val
                pos["pnl_pct"] = (val / pos["cost_eur"] - 1) * 100
                held = time.monotonic() - pos["entry_ts"]
                ratio = val / pos["cost_eur"]
                reason = ("graduated" if c["complete"]
                          else "take_profit" if ratio >= TAKE_PROFIT_X
                          else "stop_loss" if ratio <= (1 - STOP_LOSS_PCT)
                          else "timeout" if held >= MAX_HOLD_SEC else None)
                if reason:
                    await self.simulate_sell(mint, reason)
            self.pv.append({"t": int(time.time()), "v": round(self.portfolio(), 2)})
            await self.broadcast()
            await asyncio.sleep(POLL_INTERVAL_SEC)

    # ---- state ----
    def portfolio(self):
        return self.cash + sum(p["value_eur"] for p in self.positions.values())

    def snapshot(self):
        t = time.time()
        return {
            "cash": round(self.cash, 2), "sol_eur": round(self.sol_eur, 2),
            "trade_eur": self.trade_eur, "auto": self.auto,
            "realized": round(self.realized, 2),
            "portfolio": round(self.portfolio(), 2),
            "roi": round((self.portfolio() / START_CASH_EUR - 1) * 100, 1),
            "missed": self.missed, "start_cash": START_CASH_EUR,
            "positions": [{
                "mint": p["mint"], "value": round(p["value_eur"], 2),
                "pnl_pct": round(p["pnl_pct"], 1), "cost": p["cost_eur"],
                "age": int(t - (time.time() - (time.monotonic() - p["entry_ts"]))),
                "age_sec": int(time.monotonic() - p["entry_ts"]),
                "dev_buy": p["dev_buy_sol"],
            } for p in self.positions.values()],
            "launches": [{
                "mint": l["mint"], "creator": l["creator"],
                "dev_buy": l["dev_buy_sol"], "held": l["held"],
                "age_sec": int(t - l["age"]),
            } for l in self.launches],
            "closed": list(self.closed)[:30],
            "pv": list(self.pv),
        }

    async def broadcast(self):
        if not self.clients:
            return
        msg = json.dumps(self.snapshot())
        dead = []
        for ws in self.clients:
            try:
                await ws.send_str(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


# ---------------------------------------------------------------------------
# Solana subscription
# ---------------------------------------------------------------------------
async def solana_stream(engine):
    sub = {"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
           "params": [{"mentions": [PUMP_PROGRAM]}, {"commitment": "processed"}]}
    while True:
        try:
            async with engine.s.ws_connect(RPC_WSS, max_msg_size=0) as ws:
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
                        asyncio.create_task(engine.on_signature(v["signature"], detected_at))
        except Exception as e:
            print(f"[reconnect] {e}", flush=True)
            await asyncio.sleep(2)


# ---------------------------------------------------------------------------
# Web
# ---------------------------------------------------------------------------
async def h_index(request):
    return web.Response(text=PAGE, content_type="text/html")


async def h_state(request):
    return web.json_response(request.app["engine"].snapshot())


async def h_buy(request):
    body = await request.json()
    eng = request.app["engine"]
    info = next((l for l in eng.launches if l["mint"] == body.get("mint")), None)
    if info:
        await eng.simulate_buy(info, manual=True)
    return web.json_response({"ok": bool(info)})


async def h_sell(request):
    body = await request.json()
    await request.app["engine"].simulate_sell(body.get("mint"), "manual")
    return web.json_response({"ok": True})


async def h_config(request):
    body = await request.json()
    eng = request.app["engine"]
    if "auto" in body:
        eng.auto = bool(body["auto"])
    if "trade_eur" in body:
        try:
            eng.trade_eur = max(1.0, float(body["trade_eur"]))
        except (TypeError, ValueError):
            pass
    await eng.broadcast()
    return web.json_response({"ok": True})


async def h_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    eng = request.app["engine"]
    eng.clients.add(ws)
    await ws.send_str(json.dumps(eng.snapshot()))
    try:
        async for _ in ws:
            pass
    finally:
        eng.clients.discard(ws)
    return ws


async def on_startup(app):
    app["session"] = aiohttp.ClientSession()
    eng = Engine(app["session"])
    app["engine"] = eng
    app["tasks"] = [
        asyncio.create_task(solana_stream(eng)),
        asyncio.create_task(eng.tracker()),
        asyncio.create_task(eng.price_feed()),
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
        web.post("/api/buy", h_buy),
        web.post("/api/sell", h_sell),
        web.post("/api/config", h_config),
        web.get("/ws", h_ws),
    ])
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


# ---------------------------------------------------------------------------
# Dashboard (embedded)
# ---------------------------------------------------------------------------
PAGE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>paper desk</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root{
    --bg:#0d0f1a; --panel:#151932; --panel2:#1a1f3d; --line:#262c4a;
    --ink:#eceefb; --mut:#868cb2; --gain:#38e0b0; --loss:#ff7a8a;
    --accent:#7c6cff; --accent2:#a99bff;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);
    font-family:Archivo,system-ui,sans-serif;padding:16px 14px 60px;
    max-width:640px;margin:0 auto;-webkit-font-smoothing:antialiased}
  .mono{font-family:"JetBrains Mono",monospace;font-variant-numeric:tabular-nums}
  h2{font-size:13px;font-weight:600;color:var(--mut);margin:26px 0 10px;
    letter-spacing:.01em}
  .sim{display:inline-block;font-size:11px;color:var(--accent2);
    border:1px solid var(--line);border-radius:999px;padding:2px 9px;margin-bottom:14px}
  /* hero */
  .hero{background:linear-gradient(160deg,var(--panel),var(--panel2));
    border:1px solid var(--line);border-radius:18px;padding:20px 18px}
  .hlabel{font-size:12px;color:var(--mut)}
  .cash{font-family:"JetBrains Mono",monospace;font-weight:700;
    font-size:44px;line-height:1.05;margin:2px 0 4px;letter-spacing:-.02em}
  .subrow{display:flex;gap:18px;margin-top:8px;font-size:13px}
  .subrow .mono{font-weight:600}
  .up{color:var(--gain)} .down{color:var(--loss)}
  /* controls */
  .ctl{display:flex;align-items:center;gap:12px;margin-top:16px;flex-wrap:wrap}
  .toggle{display:flex;align-items:center;gap:9px;font-size:14px}
  .sw{width:44px;height:26px;border-radius:999px;background:var(--line);
    position:relative;transition:.15s;cursor:pointer;flex:none}
  .sw.on{background:var(--accent)}
  .sw i{position:absolute;top:3px;left:3px;width:20px;height:20px;border-radius:50%;
    background:#fff;transition:.15s}
  .sw.on i{left:21px}
  .size{display:flex;align-items:center;gap:6px;font-size:14px;color:var(--mut)}
  .size input{width:64px;background:var(--bg);border:1px solid var(--line);
    color:var(--ink);border-radius:9px;padding:6px 8px;font-family:"JetBrains Mono",monospace}
  /* cards */
  .card{background:var(--panel);border:1px solid var(--line);border-radius:14px;
    padding:12px 13px;margin-bottom:9px;display:flex;align-items:center;gap:11px}
  .addr{font-size:13px;font-weight:600}
  .meta{font-size:11px;color:var(--mut);margin-top:2px}
  .grow{flex:1;min-width:0}
  .pill{font-size:12px;padding:5px 9px;border-radius:9px;font-weight:600;
    font-family:"JetBrains Mono",monospace;flex:none}
  button{font-family:Archivo,sans-serif;font-weight:600;font-size:13px;
    border:none;border-radius:10px;padding:9px 15px;cursor:pointer;flex:none}
  .buy{background:var(--accent);color:#fff}
  .buy.done{background:transparent;border:1px solid var(--line);color:var(--mut)}
  .sell{background:transparent;border:1px solid var(--loss);color:var(--loss)}
  canvas{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:8px}
  .empty{color:var(--mut);font-size:13px;padding:14px 2px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  td{padding:8px 4px;border-bottom:1px solid var(--line)}
  td.mono{font-family:"JetBrains Mono",monospace;text-align:right}
  .flash{animation:fl .8s ease-out}
  @keyframes fl{from{background:#232a52}to{background:var(--panel)}}
  .foot{color:var(--mut);font-size:11px;margin-top:24px;line-height:1.5}
</style></head>
<body>
<div class="sim">simulated cash · no real money</div>

<div class="hero">
  <div class="hlabel">Cash balance</div>
  <div class="cash" id="cash">€—</div>
  <div class="subrow">
    <span>P&amp;L <span class="mono" id="pnl">—</span></span>
    <span>ROI <span class="mono" id="roi">—</span></span>
    <span>SOL <span class="mono" id="sol">—</span></span>
  </div>
  <div class="ctl">
    <div class="toggle"><div class="sw" id="sw" onclick="toggleAuto()"><i></i></div>
      Auto-snipe every launch</div>
    <div class="size">€<input id="size" type="number" min="1" value="10"
      onchange="setSize(this.value)"> per trade</div>
  </div>
</div>

<h2>Portfolio value</h2>
<canvas id="chart" height="150"></canvas>

<h2>Open positions</h2>
<div id="positions"><div class="empty">Nothing held.</div></div>

<h2>Live launches</h2>
<div id="launches"><div class="empty">Waiting for the next mint…</div></div>

<h2>Closed trades</h2>
<div id="closed"><div class="empty">No trades closed yet.</div></div>

<div class="foot" id="foot"></div>

<script>
let S={}, chart, seen=new Set();
function short(a){return a.slice(0,4)+"…"+a.slice(-4)}
function eur(n){return "€"+n.toFixed(2)}
function sign(n){return (n>=0?"+":"")+n}
function pumpUrl(m){return "https://pump.fun/coin/"+m}

function render(s){
  S=s;
  document.getElementById("cash").textContent=eur(s.cash);
  const pnl=document.getElementById("pnl");
  pnl.textContent=sign(s.realized.toFixed(2))+"€";
  pnl.className="mono "+(s.realized>=0?"up":"down");
  const roi=document.getElementById("roi");
  roi.textContent=sign(s.roi)+"%"; roi.className="mono "+(s.roi>=0?"up":"down");
  document.getElementById("sol").textContent=eur(s.sol_eur);
  document.getElementById("sw").classList.toggle("on",s.auto);

  // positions
  const pc=document.getElementById("positions");
  pc.innerHTML = s.positions.length? "" : '<div class="empty">Nothing held.</div>';
  s.positions.forEach(p=>{
    const cl=p.pnl_pct>=0?"up":"down";
    const el=document.createElement("div"); el.className="card";
    el.innerHTML=`<div class="grow"><a class="addr" style="color:inherit;text-decoration:none"
      href="${pumpUrl(p.mint)}" target="_blank">${short(p.mint)}</a>
      <div class="meta">${eur(p.value)} · held ${p.age_sec}s${p.dev_buy!=null?" · dev "+p.dev_buy.toFixed(2)+"◎":""}</div></div>
      <div class="pill ${cl}">${sign(p.pnl_pct)}%</div>
      <button class="sell" onclick="sell('${p.mint}')">Sell</button>`;
    pc.appendChild(el);
  });

  // launches
  const lc=document.getElementById("launches");
  lc.innerHTML = s.launches.length? "" : '<div class="empty">Waiting for the next mint…</div>';
  s.launches.forEach(l=>{
    const el=document.createElement("div");
    el.className="card"+(seen.has(l.mint)?"":" flash"); seen.add(l.mint);
    el.innerHTML=`<div class="grow"><a class="addr" style="color:inherit;text-decoration:none"
      href="${pumpUrl(l.mint)}" target="_blank">${short(l.mint)}</a>
      <div class="meta">${l.age_sec}s ago${l.dev_buy!=null?" · dev bought "+l.dev_buy.toFixed(2)+"◎":""}</div></div>
      ${l.held?'<button class="buy done" disabled>Held</button>'
        :`<button class="buy" onclick="buy('${l.mint}')">Buy €${s.trade_eur}</button>`}`;
    lc.appendChild(el);
  });

  // closed
  const cc=document.getElementById("closed");
  if(!s.closed.length){cc.innerHTML='<div class="empty">No trades closed yet.</div>';}
  else{
    let rows=s.closed.map(c=>{const cl=c.pnl_eur>=0?"up":"down";
      return `<tr><td>${short(c.mint)}</td><td style="color:var(--mut)">${c.reason}</td>
        <td class="mono">${c.hold_sec}s</td>
        <td class="mono ${cl}">${sign(c.pnl_eur.toFixed(2))}€</td>
        <td class="mono ${cl}">${sign(c.pnl_pct)}%</td></tr>`}).join("");
    cc.innerHTML=`<table>${rows}</table>`;
  }

  document.getElementById("foot").textContent=
    `${s.missed} launches missed on slippage · sim latency baked in · read-only sniper, no wallet connected`;
  drawChart(s.pv);
}

function drawChart(pv){
  const labels=pv.map(p=>""), data=pv.map(p=>p.v);
  if(!chart){
    chart=new Chart(document.getElementById("chart"),{type:"line",
      data:{labels,datasets:[{data,borderColor:"#7c6cff",borderWidth:2,
        fill:true,backgroundColor:"rgba(124,108,255,.12)",tension:.25,
        pointRadius:0}]},
      options:{animation:false,plugins:{legend:{display:false}},
        scales:{x:{display:false},y:{ticks:{color:"#868cb2",font:{family:"JetBrains Mono"}},
          grid:{color:"#262c4a"}}}}});
  }else{chart.data.labels=labels;chart.data.datasets[0].data=data;chart.update();}
}

async function buy(m){await fetch("/api/buy",{method:"POST",
  headers:{"content-type":"application/json"},body:JSON.stringify({mint:m})});}
async function sell(m){await fetch("/api/sell",{method:"POST",
  headers:{"content-type":"application/json"},body:JSON.stringify({mint:m})});}
async function toggleAuto(){await fetch("/api/config",{method:"POST",
  headers:{"content-type":"application/json"},body:JSON.stringify({auto:!S.auto})});}
async function setSize(v){await fetch("/api/config",{method:"POST",
  headers:{"content-type":"application/json"},body:JSON.stringify({trade_eur:v})});}

function connect(){
  const proto=location.protocol==="https:"?"wss:":"ws:";
  const ws=new WebSocket(proto+"//"+location.host+"/ws");
  ws.onmessage=e=>render(JSON.parse(e.data));
  ws.onclose=()=>setTimeout(connect,1500);
}
connect();
</script>
</body></html>"""


if __name__ == "__main__":
    print(f"HTTP {RPC_HTTP}\nWSS  {RPC_WSS}\ndashboard -> http://localhost:{PORT}\n"
          f"SIMULATED CASH ONLY — no wallet, no real trades\n")
    web.run_app(make_app(), port=PORT)
