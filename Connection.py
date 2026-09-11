"""
Hyperliquid Continuous Data Agent
==================================

An async agent that connects to Hyperliquid's public WebSocket feed and
continuously streams market data (mid prices, L2 order books, trades) to
disk as newline-delimited JSON, with automatic reconnect and heartbeat
handling.

Usage:
    python Connection.py --coins BTC ETH SOL --feeds allMids l2Book trades

Docs reference: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
"""

import argparse
import asyncio
import json
import logging
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from flask import Flask ,Request, jsonify
# import bcrypt , numpy as np
from flask_restful import Resource
import websockets
from websockets.exceptions import WebSocketException
from pymongo import MongoClient

MAINNET_WS = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS = "wss://api.hyperliquid-testnet.xyz/ws"

client = MongoClient("mongo://db:27017")
db = client.SimilarityDB
users = db["Users"]

def UsersExist(username):
    if users.find({"Username":username}).count() == 0:
        return False
    else:
        return True
    
class Register(Resource):
    def post(self):
        postedData = Request.get_json()
        uname = postedData["username"]
        pswd = postedData["password"]

        if UsersExist(uname):
            ret = {
                "status" : 301,
                "message": "Error Here"
            }
        return json(ret)


# Server disconnects any socket that's been silent for 60s -> ping well before that.
HEARTBEAT_INTERVAL_S = 20
# Reconnect backoff bounds.
BACKOFF_START_S = 1
BACKOFF_MAX_S = 30
# Handshake can hang or return 502 under load — fail fast and retry.
CONNECT_OPEN_TIMEOUT_S = 20
CONNECT_CLOSE_TIMEOUT_S = 5

# Transient WS failures that should reconnect (includes HTTP 502 InvalidStatus).
_RECONNECT_ERRORS = (WebSocketException, OSError, asyncio.TimeoutError, TimeoutError)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hl-agent")


@dataclass
class AgentConfig:
    coins: list[str]
    feeds: list[str]                       # subset of {"allMids", "l2Book", "trades"}
    out_dir: Path = Path("./hl_data")
    testnet: bool = False
    print_to_console: bool = True

    def ws_url(self) -> str:
        return TESTNET_WS if self.testnet else MAINNET_WS


class HyperliquidAgent:
    """
    Maintains a single WebSocket connection, subscribes to the requested
    feeds/coins, and hands every incoming message to `_handle_message`.
    Runs forever until `stop()` is called (e.g. on SIGINT/SIGTERM).
    """

    def __init__(self, config: AgentConfig):
        self.config = config
        self._stop_event = asyncio.Event()
        self._ws: Any | None = None
        self._latest_mids: dict[str, str] = {}
        self._msg_count = 0
        self._last_msg_ts = 0.0
        self.config.out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- public control ----------

    def stop(self):
        log.info("Stop requested — shutting down after current cycle.")
        self._stop_event.set()

    async def run(self):
        backoff = BACKOFF_START_S
        while not self._stop_event.is_set():
            try:
                await self._connect_and_stream()
                backoff = BACKOFF_START_S  # reset after a clean session
            except _RECONNECT_ERRORS as e:
                if self._stop_event.is_set():
                    break
                log.warning(f"Connection lost ({e!r}); reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX_S)
            except Exception:
                log.exception("Unexpected error — reconnecting after backoff")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX_S)
        log.info("Agent stopped.")

    # ---------- internals ----------

    async def _connect_and_stream(self):
        url = self.config.ws_url()
        log.info(f"Connecting to {url}")
        async with websockets.connect(
            url,
            ping_interval=None,  # Hyperliquid expects app-level {"method":"ping"}
            open_timeout=CONNECT_OPEN_TIMEOUT_S,
            close_timeout=CONNECT_CLOSE_TIMEOUT_S,
            max_size=8 * 1024 * 1024,
        ) as ws:
            self._ws = ws
            await self._subscribe_all()
            log.info("Stream live — waiting for market data")

            heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            try:
                async for raw in ws:
                    if self._stop_event.is_set():
                        break
                    self._msg_count += 1
                    self._last_msg_ts = time.time()
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        log.warning(f"Non-JSON message dropped: {raw[:200]!r}")
                        continue
                    self._handle_message(msg)
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
                self._ws = None

    async def _subscribe_all(self):
        for feed in self.config.feeds:
            if feed == "allMids":
                await self._send({"method": "subscribe", "subscription": {"type": "allMids"}})
            elif feed in ("l2Book", "trades"):
                for coin in self.config.coins:
                    await self._send({
                        "method": "subscribe",
                        "subscription": {"type": feed, "coin": coin},
                    })
            else:
                log.warning(f"Unknown feed '{feed}' — skipping")

    async def _send(self, payload: dict[str, Any]):
        assert self._ws is not None
        await self._ws.send(json.dumps(payload))

    async def _heartbeat_loop(self):
        """Keep the connection alive; server drops sockets idle >60s."""
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL_S)
                await self._send({"method": "ping"})
        except asyncio.CancelledError:
            pass

    def _handle_message(self, msg: dict[str, Any]):
        channel = msg.get("channel")

        if channel == "subscriptionResponse":
            log.info(f"Subscribed: {msg['data']['subscription']}")
            return
        if channel == "pong":
            return  # heartbeat ack, nothing to persist
        if channel == "allMids":
            self._latest_mids = msg["data"]["mids"]
            self._persist("all_mids.jsonl", {"ts": time.time(), **msg["data"]})
            if self.config.print_to_console:
                sample = dict(list(self._latest_mids.items())[:5])
                log.info(f"allMids update — sample: {sample}")
            return
        if channel == "l2Book":
            data = msg["data"]
            coin = data.get("coin", "unknown")
            self._persist(f"l2book_{coin}.jsonl", {"ts": time.time(), **data})
            if self.config.print_to_console:
                bids, asks = data["levels"]
                best_bid = bids[0]["px"] if bids else None
                best_ask = asks[0]["px"] if asks else None
                log.info(f"[{coin}] book  bid={best_bid}  ask={best_ask}")
            return
        if channel == "trades":
            for trade in msg["data"]:
                coin = trade.get("coin", "unknown")
                self._persist(f"trades_{coin}.jsonl", trade)
                if self.config.print_to_console:
                    log.info(
                        f"[{coin}] trade  side={trade.get('side')} "
                        f"px={trade.get('px')} sz={trade.get('sz')}"
                    )
            return

        # Fallback for any other channel type (notifications, errors, etc.)
        log.debug(f"Unhandled channel '{channel}': {msg}")

    def _persist(self, filename: str, record: dict[str, Any]):
        path = self.config.out_dir / filename
        with path.open("a") as f:
            f.write(json.dumps(record) + "\n")


def parse_args() -> AgentConfig:
    parser = argparse.ArgumentParser(description="Continuous Hyperliquid market data agent")
    parser.add_argument("--coins", nargs="+", default=["BTC", "ETH", "SOL"],
                         help="Coins to track for l2Book/trades feeds")
    parser.add_argument("--feeds", nargs="+", default=["allMids", "l2Book", "trades"],
                         choices=["allMids", "l2Book", "trades"])
    parser.add_argument("--out-dir", type=Path, default=Path("./hl_data"))
    parser.add_argument("--testnet", action="store_true")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-message console logs")
    args = parser.parse_args()
    return AgentConfig(
        coins=args.coins,
        feeds=args.feeds,
        out_dir=args.out_dir,
        testnet=args.testnet,
        print_to_console=not args.quiet,
    )


async def main():
    config = parse_args()
    agent = HyperliquidAgent(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, agent.stop)
        except NotImplementedError:
            pass  # signal handlers unsupported on some platforms (e.g. Windows)

    log.info(
        f"Starting agent | coins={config.coins} feeds={config.feeds} "
        f"testnet={config.testnet} out_dir={config.out_dir}"
    )
    await agent.run()


if __name__ == "__main__":
    asyncio.run(main())
