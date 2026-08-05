"""
blockchain_monitor.py — Solana WebSocket + HTTP RPC client.

Uses logsSubscribe (one subscription per address) for real-time
monitoring. Reconnects automatically with exponential back-off and
re-subscribes all addresses after a reconnect.
"""

import asyncio
import json
import logging
from typing import Callable, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

logger = logging.getLogger(__name__)

LAMPORTS_PER_SOL = 1_000_000_000

# Solana native program addresses
SYSTEM_PROGRAM = "11111111111111111111111111111111"
SPL_TOKEN      = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


class BlockchainMonitor:
    """
    Maintains a single persistent WebSocket connection to a Solana node.
    Callers subscribe to log events for specific addresses via
    subscribe_address(address, callback).  The callback receives:
        (signature: str, address: str, logs: list[str])
    """

    def __init__(self, ws_endpoint: str, rpc_endpoint: str, max_concurrent_rpc: int = 10):
        self.ws_endpoint = ws_endpoint
        self.rpc_endpoint = rpc_endpoint
        self._sem = asyncio.Semaphore(max_concurrent_rpc)

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False

        # address → async callback(signature, address, logs)
        self._subscribed: dict[str, Callable] = {}
        # subscription_id → address
        self._sub_id_map: dict[int, str] = {}
        # pending RPC responses: request_id → Future
        self._pending: dict[int, asyncio.Future] = {}
        self._req_id = 0
        self._id_lock = asyncio.Lock()

        # Reconnect state
        self._reconnect_delay = 2
        self._max_reconnect_delay = 60

        self._http: Optional[aiohttp.ClientSession] = None

    # ── Lifecycle ─────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        asyncio.create_task(self._ws_loop(), name="ws-loop")
        logger.info("BlockchainMonitor started.")

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._http:
            await self._http.close()
        logger.info("BlockchainMonitor stopped.")

    # ── Subscription management ───────────────────────────────

    async def subscribe_address(self, address: str, callback: Callable) -> None:
        """Register a callback for all confirmed transactions mentioning `address`."""
        self._subscribed[address] = callback
        if self._ws:
            await self._do_subscribe(address)

    async def unsubscribe_address(self, address: str) -> None:
        """Remove a subscription for `address`."""
        self._subscribed.pop(address, None)
        sub_id = next(
            (sid for sid, addr in self._sub_id_map.items() if addr == address), None
        )
        if sub_id is not None and self._ws:
            self._sub_id_map.pop(sub_id, None)
            try:
                await self._send_ws("logsUnsubscribe", [sub_id])
            except Exception:
                pass

    # ── HTTP RPC helpers ──────────────────────────────────────

    async def get_transaction(self, signature: str) -> Optional[dict]:
        return await self._rpc(
            "getTransaction",
            [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        )

    async def get_signatures_for_address(self, address: str, limit: int = 50) -> list:
        result = await self._rpc("getSignaturesForAddress", [address, {"limit": limit}])
        return result or []

    async def get_account_info(self, address: str) -> Optional[dict]:
        return await self._rpc("getAccountInfo", [address, {"encoding": "base64"}])

    # ── WebSocket loop ────────────────────────────────────────

    async def _ws_loop(self) -> None:
        while self._running:
            try:
                logger.info(f"Connecting to Solana WebSocket …")
                async with websockets.connect(
                    self.ws_endpoint,
                    ping_interval=20,
                    ping_timeout=15,
                    max_size=16 * 1024 * 1024,
                    open_timeout=30,
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = 2  # reset on success
                    logger.info("WebSocket connected. Re-subscribing …")
                    # IMPORTANT: recv_loop must run concurrently with resubscribe
                    # so that subscription-confirmation replies can be received
                    # while we're still sending subscription requests.
                    recv_task = asyncio.create_task(
                        self._recv_loop(ws), name="ws-recv"
                    )
                    try:
                        await self._resubscribe_all()
                        await recv_task  # block until socket closes
                    except Exception:
                        recv_task.cancel()
                        raise

            except (ConnectionClosed, WebSocketException, OSError, asyncio.TimeoutError) as e:
                logger.warning(f"WebSocket disconnected ({e}). Retry in {self._reconnect_delay}s …")
            except Exception as e:
                logger.error(f"Unexpected WebSocket error: {e}")
            finally:
                self._ws = None
                self._sub_id_map.clear()
                # Fail any pending futures
                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(ConnectionError("WebSocket disconnected"))
                self._pending.clear()

            if self._running:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

    async def _recv_loop(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
                await self._handle_message(msg)
            except json.JSONDecodeError:
                pass
            except Exception as e:
                logger.debug(f"Message handling error: {e}")

    async def _handle_message(self, msg: dict) -> None:
        # Subscription confirmations and RPC replies
        if "id" in msg and ("result" in msg or "error" in msg):
            req_id = msg["id"]
            fut = self._pending.pop(req_id, None)
            if fut and not fut.done():
                if "error" in msg:
                    fut.set_exception(RuntimeError(msg["error"]))
                else:
                    fut.set_result(msg.get("result"))
            return

        method = msg.get("method")
        if method == "logsNotification":
            params = msg.get("params", {})
            sub_id = params.get("subscription")
            value  = params.get("result", {}).get("value", {})
            sig    = value.get("signature")
            err    = value.get("err")
            logs   = value.get("logs", [])

            if err or not sig:
                return

            address = self._sub_id_map.get(sub_id)
            if address and address in self._subscribed:
                cb = self._subscribed[address]
                asyncio.create_task(cb(sig, address, logs))

    # ── Internal WebSocket helpers ────────────────────────────

    async def _resubscribe_all(self) -> None:
        for address in list(self._subscribed.keys()):
            await self._do_subscribe(address)
            await asyncio.sleep(0.05)   # avoid flooding

    async def _do_subscribe(self, address: str) -> None:
        try:
            sub_id = await self._send_ws(
                "logsSubscribe",
                [{"mentions": [address]}, {"commitment": "confirmed"}],
            )
            if sub_id is not None:
                self._sub_id_map[sub_id] = address
                logger.debug(f"Subscribed {address[:8]}… (sub_id={sub_id})")
        except Exception as e:
            logger.error(f"Subscribe failed for {address[:8]}…: {e}")

    async def _send_ws(self, method: str, params: list) -> Optional[int]:
        """Send a JSON-RPC request over WebSocket and await the reply."""
        if not self._ws:
            raise ConnectionError("WebSocket not connected")

        async with self._id_lock:
            self._req_id += 1
            req_id = self._req_id

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[req_id] = fut

        try:
            await self._ws.send(json.dumps({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": method,
                "params": params,
            }))
            return await asyncio.wait_for(asyncio.shield(fut), timeout=15)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            logger.warning(f"WS request timeout: {method}")
            return None
        except Exception:
            self._pending.pop(req_id, None)
            raise

    # ── HTTP RPC ──────────────────────────────────────────────

    async def _rpc(self, method: str, params: list) -> Optional[dict | list | int]:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        async with self._sem:
            try:
                async with self._http.post(self.rpc_endpoint, json=payload) as resp:
                    data = await resp.json(content_type=None)
                if "error" in data:
                    logger.warning(f"RPC {method} error: {data['error']}")
                    return None
                return data.get("result")
            except asyncio.TimeoutError:
                logger.warning(f"RPC timeout: {method}")
                return None
            except Exception as e:
                logger.error(f"RPC {method} failed: {e}")
                return None
