# streaming.py
from __future__ import annotations

import asyncio
import json
import time
import sys
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple

from clients import KalshiWebSocketClient
import requests


# -----------------------------
# OrderBook (in-memory state)
# -----------------------------
@dataclass
class OrderBook:
    market_ticker: str
    yes_levels: Dict[int, int] = field(default_factory=dict)
    no_levels: Dict[int, int] = field(default_factory=dict)

    best_yes: Optional[int] = None
    best_no: Optional[int] = None

    sid: Optional[int] = None
    last_seq: Optional[int] = None

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def apply_snapshot(self, *, yes: List[List[int]], no: List[List[int]], sid: int, seq: int) -> None:
        async with self.lock:
            self.sid = sid
            self.last_seq = seq

            self.yes_levels.clear()
            self.no_levels.clear()

            by = None
            bn = None

            for price, qty in yes:
                qty = int(qty)
                if qty > 0:
                    p = int(price)
                    self.yes_levels[p] = qty
                    if by is None or p > by:
                        by = p

            for price, qty in no:
                qty = int(qty)
                if qty > 0:
                    p = int(price)
                    self.no_levels[p] = qty
                    if bn is None or p > bn:
                        bn = p

            self.best_yes = by
            self.best_no = bn

    async def apply_delta(self, *, side: str, price: int, delta: int, sid: int, seq: int) -> bool:
        """
        Returns False if we detect a gap and must resync.
        """
        async with self.lock:
            # If Kalshi switches sid (new subscription), reset sequencing expectations
            if self.sid is None or self.sid != sid:
                self.sid = sid
                self.last_seq = None

            # If we have sequencing and this isn't the next message -> gap
            if self.last_seq is not None and seq != self.last_seq + 1:
                return False

            self.last_seq = seq

            book = self.yes_levels if side == "yes" else self.no_levels
            best = self.best_yes if side == "yes" else self.best_no

            new_qty = book.get(price, 0) + delta

            if new_qty <= 0:
                if price in book:
                    del book[price]
                if best == price:
                    best = max(book.keys()) if book else None
            else:
                book[price] = new_qty
                if best is None or price > best:
                    best = price

            if side == "yes":
                self.best_yes = best
            else:
                self.best_no = best

            return True

    def best_yes_bid(self) -> Optional[int]:
        return self.best_yes

    def best_no_bid(self) -> Optional[int]:
        return self.best_no

    def best_yes_ask_implied(self) -> Optional[int]:
        # YES ask = 100 - best NO bid
        if self.best_no is None:
            return None
        return 100 - self.best_no

    def best_no_ask_implied(self) -> Optional[int]:
        # NO ask = 100 - best YES bid
        if self.best_yes is None:
            return None
        return 100 - self.best_yes


# -----------------------------
# Websocket Streamer
# -----------------------------
class MarketDataStreamer(KalshiWebSocketClient):
    def __init__(self, key_id: str, private_key, market_ticker: str):
        super().__init__(key_id=key_id, private_key=private_key)
        self.market_ticker = market_ticker
        self.book = OrderBook(market_ticker=market_ticker)

        self._cmd_id = 1
        self._orderbook_sid: Optional[int] = None  # IMPORTANT: subscription id for orderbook_delta

    def _next_cmd_id(self) -> int:
        cid = self._cmd_id
        self._cmd_id += 1
        return cid

    async def on_open(self):
        await self._subscribe_clean()

    async def _subscribe_clean(self):
        cmd = {
            "id": self._next_cmd_id(),
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_ticker": self.market_ticker,
            },
        }
        await self.ws.send(json.dumps(cmd))

    async def _unsubscribe_orderbook(self):
        if self._orderbook_sid is None:
            return
        cmd = {
            "id": self._next_cmd_id(),
            "cmd": "unsubscribe",
            "params": {"sids": [self._orderbook_sid]},
        }
        await self.ws.send(json.dumps(cmd))
        self._orderbook_sid = None

    async def _resync(self):
        """
        Full clean resync:
        - unsubscribe old orderbook sid
        - subscribe again
        """
        sys.stdout.write("\n[RESYNC] Unsubscribing + resubscribing orderbook...\n")
        sys.stdout.flush()
        await self._unsubscribe_orderbook()
        await self._subscribe_clean()

    async def on_message(self, message: str):
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        msg_type = data.get("type")

        # Subscription confirmation includes sids per channel
        if msg_type == "ok":
            subs = data.get("subscriptions", [])
            for s in subs:
                if s.get("channel") == "orderbook_delta":
                    self._orderbook_sid = s.get("sid")
            return

        if msg_type in {"subscribed"}:
            return

        if msg_type == "error":
            sys.stdout.write("\n[WS ERROR] " + json.dumps(data) + "\n")
            sys.stdout.flush()
            return

        if msg_type == "orderbook_snapshot":
            sid = data.get("sid")
            seq = data.get("seq")
            msg = data.get("msg", {})

            if msg.get("market_ticker") != self.market_ticker:
                return

            await self.book.apply_snapshot(
                yes=msg.get("yes", []),
                no=msg.get("no", []),
                sid=sid,
                seq=seq,
            )
            return

        if msg_type == "orderbook_delta":
            sid = data.get("sid")
            seq = data.get("seq")
            msg = data.get("msg", {})

            if msg.get("market_ticker") != self.market_ticker:
                return

            ok = await self.book.apply_delta(
                side=msg["side"],
                price=int(msg["price"]),
                delta=int(msg["delta"]),
                sid=sid,
                seq=seq,
            )

            if not ok:
                # IMPORTANT: gap -> clean resync
                await self._resync()
            return


# -----------------------------
# 1ms NEWLINE Tape Printer
# -----------------------------
def fmt_px(x: Optional[int]) -> str:
    return "---" if x is None else f"{x:02d}"


async def print_tape_every_1ms(book: OrderBook):
    """
    Print a NEW line every 1ms with YES/NO bid+ask.
    NOTE: 1000 lines/sec is very heavy on your terminal.
    """
    interval_ns = 1_000_000  # 1ms
    next_tick = time.perf_counter_ns()

    buf = []
    flush_every = 25  # flush more often so it *looks* realtime

    while True:
        now = time.perf_counter_ns()
        if now < next_tick:
            await asyncio.sleep((next_tick - now) / 1_000_000_000)
            continue

        ts_ns = time.time_ns()
        sec = ts_ns // 1_000_000_000
        micros = (ts_ns % 1_000_000_000) // 1_000
        tstr = time.strftime("%H:%M:%S", time.localtime(sec)) + f".{micros:06d}"

        async with book.lock:
            yb = book.best_yes_bid()
            ya = book.best_yes_ask_implied()
            nb = book.best_no_bid()
            na = book.best_no_ask_implied()

        line = f"{tstr} | YES {fmt_px(yb)}/{fmt_px(ya)} || NO {fmt_px(nb)}/{fmt_px(na)}"
        buf.append(line)

        if len(buf) >= flush_every:
            sys.stdout.write("\n".join(buf) + "\n")
            sys.stdout.flush()
            buf.clear()

        next_tick += interval_ns

        # drift correction
        if now - next_tick > 10 * interval_ns:
            next_tick = now + interval_ns


# -----------------------------
# Startup validation (REST snapshot)
# -----------------------------
def seed_book_with_rest_snapshot(market_ticker: str) -> Tuple[List[List[int]], List[List[int]]]:
    """
    Pull REST orderbook once to confirm the market has activity
    and to seed state before WS updates arrive.
    REST orderbook is public.
    """
    url = f"https://api.elections.kalshi.com/trade-api/v2/markets/{market_ticker}/orderbook"
    r = requests.get(url, timeout=5)
    r.raise_for_status()
    data = r.json()
    ob = data.get("orderbook", {}) or {}
    yes = ob.get("yes") or []
    no = ob.get("no") or []
    # Ensure we return lists, not None
    return (yes if isinstance(yes, list) else []), (no if isinstance(no, list) else [])


async def run_stream(key_id: str, private_key, market_ticker: str):
    streamer = MarketDataStreamer(key_id=key_id, private_key=private_key, market_ticker=market_ticker)

    # Seed from REST once (fast + confirms real liquidity)
    try:
        yes, no = seed_book_with_rest_snapshot(market_ticker)
        await streamer.book.apply_snapshot(yes=yes, no=no, sid=0, seq=0)
        sys.stdout.write(f"[REST SEED] yes_levels={len(yes)} no_levels={len(no)}\n")
        sys.stdout.flush()
    except Exception as e:
        sys.stdout.write(f"[REST SEED WARN] couldn't seed orderbook: {e}\n")
        sys.stdout.flush()

    await asyncio.gather(
        streamer.connect(),
        print_tape_every_1ms(streamer.book),
    )


# -----------------------------
# main
# -----------------------------
if __name__ == "__main__":
    from cryptography.hazmat.primitives import serialization
    import os
    from pathlib import Path
    from dotenv import load_dotenv

    load_dotenv()

    key_id = os.getenv("KALSHI_KEY_ID")
    if not key_id:
        raise ValueError("Missing KALSHI_KEY_ID in .env")

    private_key_path = os.getenv("PRIVATE_KEY_PATH") or str(Path.home() / "Downloads" / "bot.txt")
    private_key_file = Path(private_key_path)
    if not private_key_file.exists():
        raise FileNotFoundError(f"Private key file not found: {private_key_file}")

    with open(private_key_file, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    market_ticker = os.getenv("KALSHI_MARKET_TICKER") or "KXBTC15M-26JAN170515-15"

    sys.stdout.write(f"[STARTING] Streaming {market_ticker}...\n")
    sys.stdout.flush()

    # Run unbuffered if you want the fastest display:
    # python -u streaming.py
    asyncio.run(run_stream(key_id, private_key, market_ticker))
