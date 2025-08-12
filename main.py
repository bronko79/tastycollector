"""
Tastytrade WebSocket → Persist → Fan-out to Clients
==================================================

Was dieses Programm macht
-------------------------
- Verbindet sich zu einer (Tastytrade-) WebSocket‑Quelle und authentifiziert sich.
- Abonniert gewünschte Symbole/Feeds, speichert eingehende Messages in SQLite.
- Startet einen eigenen WebSocket‑Server (FastAPI), an den sich Clients anschließen können.
- Beim Client‑Connect: zuerst Persistenzdaten (Backlog) senden, danach Echtzeit pushen.
- Robuste Wiederverbindung mit Exponential Backoff.

💡 Hinweise
- Die echten Endpunkte, Header und Subscriptions der Tastytrade API variieren.
  Passen Sie die Stellen mit TODO entsprechend Ihrer API‑Doku an.
- Konfiguration per Umgebungsvariablen (siehe unten).
- Abhängigkeiten: `fastapi`, `uvicorn`, `httpx`, `websockets`, `aiosqlite`, `pydantic` (optional), `python-dotenv` (optional).

Starten
-------
# 1) Abhängigkeiten installieren
pip install fastapi uvicorn httpx websockets aiosqlite pydantic python-dotenv

# 2) ENV anlegen (Beispiel)
export TASTY_BASE_URL="https://api.tastytrade.com"
export TASTY_WS_URL="wss://streamer.tastytrade.com"
export TASTY_USERNAME="dein.user@mail"
export TASTY_PASSWORD="deinPasswort"
export FEED_SYMBOLS="SPY"
export DB_PATH="./ticks.db"
export SERVER_HOST="0.0.0.0"
export SERVER_PORT="8000"

# 3) Server starten
python tastytrade_ws_server_and_client.py

Test-Client (siehe weiter unten im selben File) kann separat kopiert werden.

"""
from __future__ import annotations

import os
import json
import asyncio
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

import aiosqlite
import httpx
import websockets
from websockets.exceptions import ConnectionClosed
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse
import uvicorn

# ------------------------------
# Konfiguration
# ------------------------------

@dataclass
class Settings:
    tasty_base_url: str = os.getenv("TASTY_BASE_URL", "https://api.tastytrade.com")
    tasty_ws_url: str = os.getenv("TASTY_WS_URL", "wss://streamer.tastytrade.com")
    tasty_username: str = os.getenv("TASTY_USERNAME", "")
    tasty_password: str = os.getenv("TASTY_PASSWORD", "")
    feed_symbols: List[str] = tuple(s.strip() for s in os.getenv("FEED_SYMBOLS", "SPY").split(","))
    db_path: str = os.getenv("DB_PATH", "./xticks.db")
    server_host: str = os.getenv("SERVER_HOST", "0.0.0.0")
    server_port: int = int(os.getenv("SERVER_PORT", "8000"))
    backlog_seconds: int = int(os.getenv("BACKLOG_SECONDS", "3600"))  # Beim Client-Connect zurückliegende Zeitspanne
    auth_refresh_minutes: int = int(os.getenv("AUTH_REFRESH_MINUTES", "55"))

SETTINGS = Settings()

# ------------------------------
# Utils
# ------------------------------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ------------------------------
# Persistenz
#
# ------------------------------

CREATE_TABLE_SQL = """
DROP TABLE IF EXISTS ticks;

CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    eventSymbol TEXT NOT NULL,
    eventType TEXT NOT NULL,
    ts_ms INTEGER NOT NULL, 
    expiry TEXT,
    payload TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ticks_uni
    ON ticks(symbol, eventSymbol, eventType, ts_ms);
"""

class DataStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def open(self):
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.executescript(CREATE_TABLE_SQL)
        await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def insert_tick(self, symbol: str, event_type: str, ts_ms: int, payload: dict, expiry: Optional[str] = None):
        assert self._db is not None
        async with self._lock:
            await self._db.execute(
                "INSERT OR IGNORE INTO ticks (symbol, eventType, ts_ms, expiry, payload) VALUES (?, ?, ?, ?, ?)",
                (symbol, event_type, int(ts_ms), expiry, json.dumps(payload)),
            )
            await self._db.commit()

    async def insert_ticks_bulk(self, buffer):
        assert self._db is not None

        async with self._lock:
            #buffer = []
            #for item in fullData:
            #    buffer.append((item.get("eventSymbol"), item.get("eventType"), underlyingSymbol, int(item.get("time")), json.dumps(item), expireData))

            await self._db.executemany("INSERT OR IGNORE INTO ticks (eventSymbol, eventType, symbol, ts_ms, payload, expiry) VALUES (?, ?, ?, ?, ?, ?)", buffer)

            await self._db.commit()

    async def iter_ticks(self, symbol: str, since_ms: Optional[int] = None) -> AsyncIterator[dict]:
        assert self._db is not None
        if since_ms is None:
            since_ms = int((datetime.now(timezone.utc).timestamp() - SETTINGS.backlog_seconds) * 1000)
        async with self._db.execute(
            "SELECT eventType, ts_ms, expiry, payload FROM ticks WHERE symbol = ? AND ts_ms >= ? ORDER BY ts_ms ASC",
            (symbol, int(since_ms)),
        ) as cursor:
            async for row in cursor:
                event_type, ts_ms, expiry, payload = row
                try:
                    #print(str(ts_ms))
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    data = {"raw": payload}

                yield data

# ------------------------------
# Broadcast an Clients
# ------------------------------

class Broadcaster:
    def __init__(self):
        self._clients: List[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    async def register(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        async with self._lock:
            self._clients.append(q)
        return q

    async def unregister(self, q: asyncio.Queue):
        async with self._lock:
            if q in self._clients:
                self._clients.remove(q)

    async def publish(self, message: dict):
        # Fan-out an alle Queues, ohne zu blockieren
        dead: List[asyncio.Queue] = []
        for q in list(self._clients):
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                # Client ist überfordert → entkoppeln
                dead.append(q)
        for q in dead:
            await self.unregister(q)

BROADCASTER = Broadcaster()

# ------------------------------
# Upstream: Tastytrade WebSocket
# ------------------------------

class TastytradeIngestor:
    def __init__(self, settings: Settings, store: DataStore, broadcaster: Broadcaster):

        self.eventFields = {
            "TimeAndSale": ["eventType","eventSymbol","price", "bidPrice", "askPrice", "aggressorSide","size", "spreadLeg", "eventTime", "type", "buyer", "seller", "time", "tradeThroughExempt", "exchangeCode", "eventFlags", "exchangeSaleConditions", "extendedTradingHours", "sequence", "index", "validTick"],
            "Greeks" : ["eventType", "eventSymbol", "time", "eventTime", "price", "volatility", "delta", "gamma", "theta", "rho", "vega", "index", "sequence"]
        }


        self.settings = settings
        self.store = store
        self.broadcaster = broadcaster
        self._stop = asyncio.Event()
        self._session_token: Optional[str] = None
        self._last_auth: float = 0.0

    async def stop(self):
        self._stop.set()

    async def run(self):
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self.ensure_auth()
                await self.consume_stream()
                backoff = 1.0  # Erfolg → Backoff zurücksetzen
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[INGEST] Fehler: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def ensure_auth(self):
        # Refresh in Intervallen
        now = time.time()
        if self._session_token and (now - self._last_auth) < self.settings.auth_refresh_minutes * 60:
            return
        self._session_token = await self.authenticate()
        self._last_auth = now

    async def getOptionChain(self, underlyingSymbol, expirationDate):
        # expiration-date': '2027-01-15'
        async with httpx.AsyncClient(timeout=15) as client:
            optionChain_url = f"{self.settings.tasty_base_url}/option-chains/" + underlyingSymbol
            token = self._session_context["data"]["session-token"]
            headers = {"Authorization": f"{token}"}
            r = await client.get(optionChain_url, headers=headers)
            r.raise_for_status()
            optionChainResult = r.json()
            optionChain = optionChainResult["data"]["items"]
            availableOprionChainItems = []
            for optionChainItem in optionChain:
                if optionChainItem["expiration-date"] == expirationDate:
                    availableOprionChainItems.append(optionChainItem)

        return availableOprionChainItems

    async def authenticate(self) -> str:
        """
        Authentifiziert sich bei der Tastytrade API, holt Session-Token und dann den Streamer-Context.
        """
        if not self.settings.tasty_username or not self.settings.tasty_password:
            raise RuntimeError("TASTY_USERNAME/PASSWORD fehlen für Auth")

        async with httpx.AsyncClient(timeout=15) as client:
            url = f"{self.settings.tasty_base_url}/sessions"
            payload = {"login": self.settings.tasty_username, "password": self.settings.tasty_password}
            r = await client.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
            session_context = r.json() or {}

            token = session_context["data"]["session-token"]    #data.get("data", {}).get("session-token") or data.get("session_token") or data.get("token")
            if not token:
                raise RuntimeError("Konnte Session-Token aus Response nicht lesen – bitte Doku prüfen")

            self._session_context = session_context
            print("[AUTH] Session erhalten")

            headers = {"Authorization": f"{token}"}
            streamer_url = f"{self.settings.tasty_base_url}/quote-streamer-tokens"
            r2 = await client.get(streamer_url, headers=headers)
            r2.raise_for_status()
            streamer_context = r2.json() or {}
            streamer_context['websocket-url'] = "wss://tasty-live-ws.dxfeed.com/realtime"

            self.settings.tasty_ws_url = streamer_context.get('websocket-url', self.settings.tasty_ws_url)
            self._streamer_context = streamer_context
            print(f"[AUTH] Streamer-Context erhalten: WS URL = {self.settings.tasty_ws_url}")
            return token

    def onCompactMessage(self, compact_data, received_time):
        values_type = compact_data[0]
        fields = self.eventFields[values_type]
        values = compact_data[1]
        field_count = len(fields)
        items = []
        for i in range(0, len(values), field_count):
            if i + field_count > len(values):
                break
            item = {fields[j]: values[i + j] for j in range(field_count)}
            item["receivedTime"] = received_time
            items.append(item)
        return items


    async def consume_stream(self):

        underlyingSymbol = "SPY"
        expireData = "2025-08-15"


        url = self.settings.tasty_ws_url
        print(f"[WS-UPSTREAM] Verbinde zu {url} …")
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            await ws.send('{"type":"SETUP","channel":0,"keepaliveTimeout":60,"acceptKeepaliveTimeout":60,"version":"0.1-js/1.0.0"}')
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    if msg['type'] == "AUTH_STATE" and msg["state"] == "UNAUTHORIZED":
                        await ws.send('{"type":"AUTH","channel":0,"token":"' + self._streamer_context["data"]["token"] + '"}')

                    if msg['type'] == "AUTH_STATE" and msg["state"] == "AUTHORIZED":
                        await ws.send('{"type":"CHANNEL_REQUEST","channel":1,"service":"FEED","parameters":{"contract":"AUTO"}}')

                    if msg['type'] == "CHANNEL_OPENED":
                        await ws.send('{"type":"FEED_SETUP","channel":1,"acceptAggregationPeriod":0.1,"acceptDataFormat":"FULL", "acceptEventFields":' + json.dumps(self.eventFields) + ' }')

                    if msg['type'] == "FEED_CONFIG":
                        print("[WS-UPSTREAM] Subscribing...")

                        await ws.send('{"type":"FEED_SUBSCRIPTION","channel":1,"add":[{"type": "TimeAndSale", "symbol": "' + underlyingSymbol + '"}] }')
                        availableOptionChainItems = await self.getOptionChain(underlyingSymbol, expireData)
                        for availableOptionChainItem in availableOptionChainItems:
                            streamerSymbol = availableOptionChainItem["streamer-symbol"]
                            await ws.send('{"type":"FEED_SUBSCRIPTION","channel":1,"add":[{"type": "TimeAndSale", "symbol": "' + streamerSymbol + '"}, {"type": "Greeks", "symbol": "' + streamerSymbol + '"}] }')

                    if msg['type'] == "FEED_DATA":
                        compactData = msg["data"]
                        received_ms = int(time.time() * 1000)
                        fullData = self.onCompactMessage(compactData, received_ms)
                        
                        buffer = []
                        for item in fullData:
                            ts_ms = int(item.get("time", received_ms))
                            event_symbol = item.get("eventSymbol")
                            event_type = item.get("eventType")
                            # Broadcast: data als dict, nicht als String
                            out = {
                                "streamtype": "tick",
                                "symbol": underlyingSymbol,
                                "eventSymbol": event_symbol,
                                "eventType": event_type,
                                "ts_ms": ts_ms,
                                "expiry": expireData,
                                "data": item,
                            }
                            await self.broadcaster.publish(out)
                        
                            buffer.append((
                                underlyingSymbol,            # symbol
                                event_symbol,                # eventSymbol
                                event_type,                  # eventType
                                ts_ms,                       # ts_ms
                                json.dumps(item),            # payload
                                expireData                   # expiry
                            ))
                        await self.store.insert_ticks_bulk(buffer)
                        #compactData = msg["data"]
                        #received_ms = int(time.time() * 1000)
                        #fullData = self.onCompactMessage(compactData, received_ms)
                        #buffer = []
                        #for item in fullData:
                        #    payloadString = json.dumps(item)
                        #    buffer.append( (item.get("eventSymbol"), item.get("eventType"), underlyingSymbol, int(item.get("time")), payloadString, expireData) )
                        #    await self.broadcaster.publish({
                        #        "streamtype": "tick",
                        #        "symbol": underlyingSymbol,
                        #        "eventSymbol": item.get("eventSymbol"),
                        #        "eventType": item.get("eventType"),
                        #        "ts_ms": int(item.get("time")),
                        #        "expiry": expireData,
                        #        "data": payloadString,
                        #    })
                        #await self.store.insert_ticks_bulk(buffer)

                    if msg['type'] == "KEEPALIVE":
                        await ws.send('{"type":"KEEPALIVE","channel":0}')

                except json.JSONDecodeError:
                    msg = {"raw": raw}


# ------------------------------
# FastAPI App (HTTP + WebSocket)
# ------------------------------

app = FastAPI(title="Tastytrade Fan-out")
STORE = DataStore(SETTINGS.db_path)
INGESTOR = TastytradeIngestor(SETTINGS, STORE, BROADCASTER)

@app.on_event("startup")
async def _on_startup():
    await STORE.open()
    # Ingestor Task
    app.state.ingestor_task = asyncio.create_task(INGESTOR.run())
    print("[APP] Startup komplett")

@app.on_event("shutdown")
async def _on_shutdown():
    print("[APP] Shutting down")
    await INGESTOR.stop()
    task: asyncio.Task = app.state.ingestor_task
    task.cancel()
    try:
        await task
    except Exception:
        pass
    await STORE.close()

@app.get("/health")
async def health():
    return {"status": "ok", "time": utcnow_iso(), "symbols": list(SETTINGS.feed_symbols)}

@app.get("/ticks")
async def get_ticks(symbol: str = Query(...), since: Optional[str] = Query(None)):
    since_ms = since
    async def gen():
        async for row in STORE.iter_ticks(symbol, since_ms):
            #yield json.dumps(row) + "\n"
            yield row + "\n"
    return StreamingResponse(gen(), media_type="application/x-ndjson")
  
#async def get_ticks(symbol: str = Query(...), since: Optional[str] = Query(None)):
#    async def gen():
#        async for row in STORE.iter_ticks(symbol, since):
#            yield json.dumps(row) + "\n"
#    return JSONResponse(content=[row async for row in STORE.iter_ticks(symbol, since)])

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, symbol: str, since: Optional[str] = None):
    await ws.accept()

    # 1) Backlog senden
    try:
        async for row in STORE.iter_ticks(symbol, since):
            await ws.send_text(json.dumps({"streamtype": "history", **row}))
    except WebSocketDisconnect:
        return

    # 2) Echtzeit abonnieren
    q = await BROADCASTER.register()
    try:
        while True:
            msg = await q.get()
            if msg.get("symbol") == symbol and msg.get("streamtype") == "tick":
                await ws.send_text(json.dumps(msg["data"]))
    except (WebSocketDisconnect, ConnectionClosed):
        pass
    finally:
        await BROADCASTER.unregister(q)

# ------------------------------
# Graceful Run
# ------------------------------
"""
def main():
    loop = asyncio.get_event_loop()
    stop = asyncio.Event()

    def _signal(*_):
        loop.create_task(_shutdown())

    async def _shutdown():
        await INGESTOR.stop()
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal)
        except NotImplementedError:
            pass

    uvicorn.run(app, host=SETTINGS.server_host, port=SETTINGS.server_port, log_level="info")

if __name__ == "__main__":
    main()
    

"""










