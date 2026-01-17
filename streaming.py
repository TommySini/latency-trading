# streaming.py
from __future__ import annotations

import asyncio
import json
import time
import sys
import select
import termios
import tty
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple
from collections import defaultdict

from clients import KalshiWebSocketClient, KalshiHttpClient
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

    def best_yes_bid_direct(self) -> Optional[int]:
        """Returns the best YES bid (for selling YES)."""
        return self.best_yes

    def best_no_bid_direct(self) -> Optional[int]:
        """Returns the best NO bid (for selling NO)."""
        return self.best_no


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


async def print_tape_every_1ms(book: OrderBook, http_client: KalshiHttpClient):
    """
    Print two updating lines every 1ms:
    - Line 1: Price data with YES/NO bid+ask
    - Line 2: Portfolio value
    Uses ANSI escape codes to update both lines in place.
    """
    interval_ns = 1_000_000  # 1ms
    next_tick = time.perf_counter_ns()
    last_price_line_length = 0
    last_portfolio_line_length = 0
    
    # Portfolio value cache - update every 500ms to avoid rate limiting
    portfolio_update_interval_ns = 500_000_000  # 500ms
    next_portfolio_update = time.perf_counter_ns()
    portfolio_value = "Loading..."
    portfolio_line = "Portfolio: Loading..."

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

        price_line = f"{tstr} | YES {fmt_px(yb)}/{fmt_px(ya)} || NO {fmt_px(nb)}/{fmt_px(na)}"
        
        # Update portfolio value periodically (every 500ms)
        if now >= next_portfolio_update:
            try:
                balance_data = http_client.get_balance()
                # Extract balance - API returns value in cents, so divide by 100
                balance = balance_data.get("balance", balance_data.get("portfolio_balance", "N/A"))
                if isinstance(balance, (int, float)):
                    balance_dollars = balance / 100.0
                    portfolio_value = f"${balance_dollars:,.2f}"
                else:
                    portfolio_value = str(balance)
                portfolio_line = f"Portfolio: {portfolio_value}"
            except Exception as e:
                portfolio_line = f"Portfolio: Error ({str(e)[:30]})"
            next_portfolio_update = now + portfolio_update_interval_ns
        
        # Pad lines to clear any leftover characters
        if len(price_line) < last_price_line_length:
            price_line = price_line.ljust(last_price_line_length)
        last_price_line_length = len(price_line)
        
        if len(portfolio_line) < last_portfolio_line_length:
            portfolio_line = portfolio_line.ljust(last_portfolio_line_length)
        last_portfolio_line_length = len(portfolio_line)
        
        # Use ANSI escape codes to update both lines:
        # \033[2K = clear entire line
        # \033[A = move cursor up one line
        # \r = return to start of line
        # Print both lines, then move up 1 to return to price line for next iteration
        output = f"\033[2K\r{price_line}\n\033[2K\r{portfolio_line}\033[A"
        sys.stdout.write(output)
        sys.stdout.flush()

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


# -----------------------------
# Position Tracker
# -----------------------------
@dataclass
class PositionEntry:
    side: str  # "yes" or "no"
    quantity: int
    entry_time: float
    buy_order_id: Optional[str] = None
    sell_order_id: Optional[str] = None
    liquidation_start_time: Optional[float] = None
    current_sell_price: Optional[int] = None
    last_bid_check_time: float = 0.0
    escalated: bool = False


class PositionTracker:
    def __init__(self):
        self.positions: Dict[str, PositionEntry] = {}  # key: side, value: PositionEntry
        self.lock = asyncio.Lock()

    async def register_buy_order(self, side: str, quantity: int, order_id: str):
        """Register a buy order - will start liquidation 4s after entry."""
        async with self.lock:
            entry_time = time.time()
            self.positions[side] = PositionEntry(
                side=side,
                quantity=quantity,
                entry_time=entry_time,
                buy_order_id=order_id,
                last_bid_check_time=0.0,
                escalated=False
            )

    async def get_position(self, side: str) -> Optional[PositionEntry]:
        async with self.lock:
            return self.positions.get(side)

    async def clear_position(self, side: str):
        async with self.lock:
            if side in self.positions:
                del self.positions[side]

    async def update_sell_order(self, side: str, sell_order_id: str, sell_price: int):
        async with self.lock:
            if side in self.positions:
                self.positions[side].sell_order_id = sell_order_id
                self.positions[side].current_sell_price = sell_price
                self.positions[side].last_bid_check_time = time.time()


async def keyboard_input_handler(
    book: OrderBook, 
    http_client: KalshiHttpClient, 
    market_ticker: str,
    position_tracker: PositionTracker
):
    """
    Handle keyboard input asynchronously:
    - 'q': Buy YES at ask price (10 shares)
    - 'w': Buy NO at ask price (10 shares)
    """
    # Set stdin to non-blocking mode
    old_settings = termios.tcgetattr(sys.stdin)
    tty.setraw(sys.stdin.fileno())
    
    try:
        loop = asyncio.get_event_loop()
        while True:
            # Check if input is available (non-blocking)
            if select.select([sys.stdin], [], [], 0.01)[0]:
                char = sys.stdin.read(1)
                
                if char == 'q':
                    # Check if YES position already exists - prevent new order if not liquidated
                    existing_pos = await position_tracker.get_position("yes")
                    if existing_pos is not None:
                        sys.stdout.write(f"\n[ORDER ERROR] YES position already exists - cannot place new buy until liquidated\n")
                        sys.stdout.flush()
                        continue
                    
                    # Buy YES at ask price
                    async with book.lock:
                        ya = book.best_yes_ask_implied()
                    
                    if ya is not None:
                        try:
                            result = http_client.create_order(
                                ticker=market_ticker,
                                action="buy",
                                side="yes",
                                count=5,
                                yes_price=ya,
                                order_type="limit"
                            )
                            order_id = result.get("order", {}).get("order_id", "unknown")
                            await position_tracker.register_buy_order("yes", 5, order_id)
                            sys.stdout.write(f"\n[ORDER] YES BUY @ {ya} - 5 shares - Order ID: {order_id}\n")
                            sys.stdout.flush()
                        except Exception as e:
                            sys.stdout.write(f"\n[ORDER ERROR] YES BUY failed: {e}\n")
                            sys.stdout.flush()
                    else:
                        sys.stdout.write(f"\n[ORDER ERROR] YES ask price not available\n")
                        sys.stdout.flush()
                
                elif char == 'w':
                    # Check if NO position already exists - prevent new order if not liquidated
                    existing_pos = await position_tracker.get_position("no")
                    if existing_pos is not None:
                        sys.stdout.write(f"\n[ORDER ERROR] NO position already exists - cannot place new buy until liquidated\n")
                        sys.stdout.flush()
                        continue
                    
                    # Buy NO at ask price
                    async with book.lock:
                        na = book.best_no_ask_implied()
                    
                    if na is not None:
                        try:
                            result = http_client.create_order(
                                ticker=market_ticker,
                                action="buy",
                                side="no",
                                count=5,
                                no_price=na,
                                order_type="limit"
                            )
                            order_id = result.get("order", {}).get("order_id", "unknown")
                            await position_tracker.register_buy_order("no", 5, order_id)
                            sys.stdout.write(f"\n[ORDER] NO BUY @ {na} - 5 shares - Order ID: {order_id}\n")
                            sys.stdout.flush()
                        except Exception as e:
                            sys.stdout.write(f"\n[ORDER ERROR] NO BUY failed: {e}\n")
                            sys.stdout.flush()
                    else:
                        sys.stdout.write(f"\n[ORDER ERROR] NO ask price not available\n")
                        sys.stdout.flush()
                
                elif char == '\x03':  # Ctrl+C
                    break
            
            await asyncio.sleep(0.01)
    finally:
        # Restore terminal settings
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)


async def liquidation_manager(
    book: OrderBook,
    http_client: KalshiHttpClient,
    market_ticker: str,
    position_tracker: PositionTracker
):
    """
    Manages liquidation of positions:
    - Starts 4 seconds after entry
    - Places sell order at best bid
    - Every 1s, checks if bid changed and cancels/replaces
    - After 10s, cancels and places more aggressive order
    """
    while True:
        await asyncio.sleep(0.1)  # Check every 100ms
        
        current_time = time.time()
        
        # Check each position
        for side in ["yes", "no"]:
            pos = await position_tracker.get_position(side)
            if pos is None:
                continue
            
            # Check if position is filled (simplified: assume filled after order placed)
            # In production, you'd check order status via API
            time_since_entry = current_time - pos.entry_time
            
            # Start liquidation 4 seconds after entry
            if time_since_entry >= 4.0 and pos.liquidation_start_time is None:
                pos.liquidation_start_time = current_time
                
                # Get best bid
                async with book.lock:
                    if side == "yes":
                        bid = book.best_yes_bid_direct()
                    else:
                        bid = book.best_no_bid_direct()
                
                if bid is None:
                    sys.stdout.write(f"\n[LIQUIDATION ERROR] {side.upper()} bid missing - panic stop\n")
                    sys.stdout.flush()
                    # Cancel all orders and exit
                    if pos.sell_order_id:
                        try:
                            http_client.cancel_order(pos.sell_order_id)
                        except:
                            pass
                    await position_tracker.clear_position(side)
                    continue
                
                # Place initial sell order
                try:
                    result = http_client.create_order(
                        ticker=market_ticker,
                        action="sell",
                        side=side,
                        count=pos.quantity,
                        yes_price=bid if side == "yes" else None,
                        no_price=bid if side == "no" else None,
                        order_type="limit"
                    )
                    sell_order_id = result.get("order", {}).get("order_id", "unknown")
                    await position_tracker.update_sell_order(side, sell_order_id, bid)
                    sys.stdout.write(f"\n[LIQUIDATION] {side.upper()} SELL @ {bid} - {pos.quantity} shares - Order ID: {sell_order_id}\n")
                    sys.stdout.flush()
                except Exception as e:
                    sys.stdout.write(f"\n[LIQUIDATION ERROR] {side.upper()} SELL failed: {e}\n")
                    sys.stdout.flush()
            
            # If liquidation has started, manage it
            if pos.liquidation_start_time is not None:
                liquidation_age = current_time - pos.liquidation_start_time
                
                # Check if position is flat by querying API (check every 2 seconds to avoid rate limiting)
                if int(liquidation_age * 5) % 10 == 0:  # Every 2 seconds
                    try:
                        positions_data = http_client.get_positions(ticker=market_ticker)
                        market_positions = positions_data.get("market_positions", [])
                        current_position = 0
                        for mp in market_positions:
                            if mp.get("ticker") == market_ticker:
                                # Position field: positive means long, negative means short
                                # For YES side: we check if we have a YES position
                                # For NO side: we check if we have a NO position
                                # The API returns position as a single number, but we need to check
                                # the actual position value. For simplicity, assume if position is 0, we're flat.
                                current_position = mp.get("position", 0)
                                break
                        
                        # If position is flat, exit
                        if current_position == 0:
                            sys.stdout.write(f"\n[LIQUIDATION] {side.upper()} position flat - liquidation complete\n")
                            sys.stdout.flush()
                            if pos.sell_order_id:
                                try:
                                    http_client.cancel_order(pos.sell_order_id)
                                except:
                                    pass
                            await position_tracker.clear_position(side)
                            continue
                    except Exception as e:
                        # If we can't check position, continue with liquidation logic
                        pass
                
                # Bid adjustment walkdown loop - check every 1 second
                if current_time - pos.last_bid_check_time >= 1.0:
                    pos.last_bid_check_time = current_time
                    
                    # Get current bid
                    async with book.lock:
                        if side == "yes":
                            bid_new = book.best_yes_bid_direct()
                        else:
                            bid_new = book.best_no_bid_direct()
                    
                    if bid_new is None:
                        sys.stdout.write(f"\n[LIQUIDATION ERROR] {side.upper()} bid missing - panic stop\n")
                        sys.stdout.flush()
                        if pos.sell_order_id:
                            try:
                                http_client.cancel_order(pos.sell_order_id)
                            except:
                                pass
                        await position_tracker.clear_position(side)
                        continue
                    
                    # If bid changed, cancel and replace
                    if bid_new != pos.current_sell_price:
                        if pos.sell_order_id:
                            try:
                                http_client.cancel_order(pos.sell_order_id)
                            except:
                                pass
                        
                        # Place new sell order at new bid
                        try:
                            result = http_client.create_order(
                                ticker=market_ticker,
                                action="sell",
                                side=side,
                                count=pos.quantity,
                                yes_price=bid_new if side == "yes" else None,
                                no_price=bid_new if side == "no" else None,
                                order_type="limit"
                            )
                            sell_order_id = result.get("order", {}).get("order_id", "unknown")
                            await position_tracker.update_sell_order(side, sell_order_id, bid_new)
                            sys.stdout.write(f"\n[LIQUIDATION] {side.upper()} REPRICE @ {bid_new} - Order ID: {sell_order_id}\n")
                            sys.stdout.flush()
                        except Exception as e:
                            sys.stdout.write(f"\n[LIQUIDATION ERROR] {side.upper()} REPRICE failed: {e}\n")
                            sys.stdout.flush()
                
                # After 10 seconds, escalate with more aggressive pricing (only once)
                if liquidation_age >= 10.0 and not pos.escalated and pos.sell_order_id:
                    pos.escalated = True
                    
                    # Cancel current sell order
                    try:
                        http_client.cancel_order(pos.sell_order_id)
                    except:
                        pass
                    
                    # Get current bid and subtract 0.02 (2 cents)
                    async with book.lock:
                        if side == "yes":
                            bid_aggressive = book.best_yes_bid_direct()
                        else:
                            bid_aggressive = book.best_no_bid_direct()
                    
                    if bid_aggressive is not None:
                        aggressive_price = max(1, bid_aggressive - 2)  # Subtract 2 cents, min 1
                        
                        try:
                            result = http_client.create_order(
                                ticker=market_ticker,
                                action="sell",
                                side=side,
                                count=pos.quantity,
                                yes_price=aggressive_price if side == "yes" else None,
                                no_price=aggressive_price if side == "no" else None,
                                order_type="limit"
                            )
                            sell_order_id = result.get("order", {}).get("order_id", "unknown")
                            await position_tracker.update_sell_order(side, sell_order_id, aggressive_price)
                            sys.stdout.write(f"\n[LIQUIDATION ESCALATE] {side.upper()} SELL @ {aggressive_price} (bid-2) - Order ID: {sell_order_id}\n")
                            sys.stdout.flush()
                        except Exception as e:
                            sys.stdout.write(f"\n[LIQUIDATION ERROR] {side.upper()} ESCALATE failed: {e}\n")
                            sys.stdout.flush()


async def run_stream(key_id: str, private_key, market_ticker: str):
    streamer = MarketDataStreamer(key_id=key_id, private_key=private_key, market_ticker=market_ticker)
    http_client = KalshiHttpClient(key_id=key_id, private_key=private_key)
    position_tracker = PositionTracker()

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
        print_tape_every_1ms(streamer.book, http_client),
        keyboard_input_handler(streamer.book, http_client, market_ticker, position_tracker),
        liquidation_manager(streamer.book, http_client, market_ticker, position_tracker),
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
