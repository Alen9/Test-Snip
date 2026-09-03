# One-off: print 3 raw PumpPortal launch events, then exit.
# Run:  ./venv/bin/python3 peek.py
import asyncio, aiohttp, json

async def go():
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect("wss://pumpportal.fun/api/data",
                                headers={"User-Agent": "Mozilla/5.0"}) as ws:
            await ws.send_str(json.dumps({"method": "subscribeNewToken"}))
            n = 0
            while n < 3:
                m = await ws.receive()
                if m.type == aiohttp.WSMsgType.TEXT:
                    d = json.loads(m.data)
                    if d.get("mint"):
                        print("KEYS:", sorted(d.keys()))
                        print("EVENT:", json.dumps(d, indent=2))
                        print("-" * 40)
                        n += 1

asyncio.run(go())
