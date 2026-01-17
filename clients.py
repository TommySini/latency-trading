import requests
import base64
import time
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
import json

from requests.exceptions import HTTPError

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature

import ssl
import websockets
import certifi

class KalshiBaseClient:
    """Base client class for interacting with the Kalshi API."""
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
    ):
        """Initializes the client with the provided API key and private key.

        Args:
            key_id (str): Your Kalshi API key ID.
            private_key (rsa.RSAPrivateKey): Your RSA private key.
        """
        self.key_id = key_id
        self.private_key = private_key
        self.last_api_call = datetime.now()
        self.HTTP_BASE_URL = "https://api.elections.kalshi.com"
        self.WS_BASE_URL = "wss://api.elections.kalshi.com"

    def request_headers(self, method: str, path: str) -> Dict[str, Any]:
        """Generates the required authentication headers for API requests."""
        current_time_milliseconds = int(time.time() * 1000)
        timestamp_str = str(current_time_milliseconds)

        # Remove query params from path
        path_parts = path.split('?')

        msg_string = timestamp_str + method + path_parts[0]
        signature = self.sign_pss_text(msg_string)

        headers = {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
        }
        return headers

    def sign_pss_text(self, text: str) -> str:
        """Signs the text using RSA-PSS and returns the base64 encoded signature."""
        message = text.encode('utf-8')
        try:
            signature = self.private_key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH
                ),
                hashes.SHA256()
            )
            return base64.b64encode(signature).decode('utf-8')
        except InvalidSignature as e:
            raise ValueError("RSA sign PSS failed") from e

class KalshiHttpClient(KalshiBaseClient):
    """Client for handling HTTP connections to the Kalshi API."""
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
    ):
        super().__init__(key_id, private_key)
        self.host = self.HTTP_BASE_URL
        self.exchange_url = "/trade-api/v2/exchange"
        self.markets_url = "/trade-api/v2/markets"
        self.portfolio_url = "/trade-api/v2/portfolio"

    def rate_limit(self) -> None:
        """Built-in rate limiter to prevent exceeding API rate limits."""
        THRESHOLD_IN_MILLISECONDS = 100
        now = datetime.now()
        threshold_in_microseconds = 1000 * THRESHOLD_IN_MILLISECONDS
        threshold_in_seconds = THRESHOLD_IN_MILLISECONDS / 1000
        if now - self.last_api_call < timedelta(microseconds=threshold_in_microseconds):
            time.sleep(threshold_in_seconds)
        self.last_api_call = datetime.now()

    def raise_if_bad_response(self, response: requests.Response) -> None:
        """Raises an HTTPError if the response status code indicates an error."""
        if response.status_code not in range(200, 299):
            response.raise_for_status()

    def post(self, path: str, body: dict) -> Any:
        """Performs an authenticated POST request to the Kalshi API."""
        self.rate_limit()
        response = requests.post(
            self.host + path,
            json=body,
            headers=self.request_headers("POST", path)
        )
        self.raise_if_bad_response(response)
        return response.json()

    def get(self, path: str, params: Dict[str, Any] = {}) -> Any:
        """Performs an authenticated GET request to the Kalshi API."""
        self.rate_limit()
        full_url = self.host + path
        # #region agent log
        with open('/Users/tommasosini/kalshi-starter-code-python/.cursor/debug.log', 'a') as f: f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"B","location":"clients.py:114","message":"GET request","data":{"full_url":full_url,"path":path,"params":params},"timestamp":int(time.time()*1000)})+"\n")
        # #endregion
        response = requests.get(
            full_url,
            headers=self.request_headers("GET", path),
            params=params
        )
        self.raise_if_bad_response(response)
        return response.json()

    def delete(self, path: str, params: Dict[str, Any] = {}) -> Any:
        """Performs an authenticated DELETE request to the Kalshi API."""
        self.rate_limit()
        response = requests.delete(
            self.host + path,
            headers=self.request_headers("DELETE", path),
            params=params
        )
        self.raise_if_bad_response(response)
        return response.json()

    def get_balance(self) -> Dict[str, Any]:
        """Retrieves the account balance."""
        return self.get(self.portfolio_url + '/balance')

    def get_exchange_status(self) -> Dict[str, Any]:
        """Retrieves the exchange status."""
        return self.get(self.exchange_url + "/status")

    def get_trades(
        self,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        max_ts: Optional[int] = None,
        min_ts: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Retrieves trades based on provided filters."""
        params = {
            'ticker': ticker,
            'limit': limit,
            'cursor': cursor,
            'max_ts': max_ts,
            'min_ts': min_ts,
        }
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}
        return self.get(self.markets_url + '/trades', params=params)


class KalshiWebSocketClient(KalshiBaseClient):
    """Client for handling WebSocket connections to the Kalshi API."""
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
    ):
        super().__init__(key_id, private_key)
        self.ws = None
        self.url_suffix = "/trade-api/ws/v2"
        self.market_ticker = None

    async def connect(self, market_ticker: Optional[str] = None):
        """Establishes a WebSocket connection using authentication.
        
        Args:
            market_ticker: Optional market ticker to subscribe to orderbook on connection.
        """
        self.market_ticker = market_ticker
        host = self.WS_BASE_URL + self.url_suffix
        auth_headers = self.request_headers("GET", self.url_suffix)
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with websockets.connect(host, additional_headers=auth_headers, ssl=ssl_context) as websocket:
            self.ws = websocket
            await self.on_open()
            if self.market_ticker:
                await self.subscribe_orderbook(self.market_ticker)
            await self.handler()

    async def subscribe_orderbook(self, market_ticker: str):
        """Subscribe to orderbook updates for a specific market ticker."""
        if not self.ws:
            raise RuntimeError("WebSocket not connected. Call connect() first.")
        
        subscribe_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_snapshot", "orderbook_delta"],
                "market_tickers": [market_ticker]
            }
        }
        await self.ws.send(json.dumps(subscribe_msg))
        print(f"Subscribed to orderbook for {market_ticker}")

    async def on_open(self):
        """Callback when WebSocket connection is opened."""
        print("WebSocket connection opened.")

    async def handler(self):
        """Handle incoming messages."""
        try:
            async for message in self.ws:
                await self.on_message(message)
        except websockets.ConnectionClosed as e:
            await self.on_close(e.code, e.reason)
        except Exception as e:
            await self.on_error(e)

    async def on_message(self, message):
        """Callback for handling incoming messages."""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "orderbook_snapshot":
                await self.on_orderbook_snapshot(data)
            elif msg_type == "orderbook_delta":
                await self.on_orderbook_delta(data)
            else:
                print("Received message:", message)
        except json.JSONDecodeError:
            print("Received non-JSON message:", message)
        except Exception as e:
            print(f"Error processing message: {e}")
            print("Raw message:", message)

    async def on_orderbook_snapshot(self, data: Dict[str, Any]):
        """Handle orderbook snapshot message."""
        msg = data.get("msg", {})
        market_ticker = msg.get("market_ticker")
        yes_levels = msg.get("yes", [])
        no_levels = msg.get("no", [])
        yes_dollars = msg.get("yes_dollars", [])
        no_dollars = msg.get("no_dollars", [])
        
        print(f"\n=== Orderbook Snapshot: {market_ticker} ===")
        print("YES side:")
        for i, (price, size) in enumerate(yes_levels):
            price_dollar = yes_dollars[i][0] if i < len(yes_dollars) else f"{price/100:.3f}"
            print(f"  Price: {price}¢ (${price_dollar}), Size: {size}")
        print("NO side:")
        for i, (price, size) in enumerate(no_levels):
            price_dollar = no_dollars[i][0] if i < len(no_dollars) else f"{price/100:.3f}"
            print(f"  Price: {price}¢ (${price_dollar}), Size: {size}")
        print("=" * 50)

    async def on_orderbook_delta(self, data: Dict[str, Any]):
        """Handle orderbook delta message."""
        msg = data.get("msg", {})
        market_ticker = msg.get("market_ticker")
        price = msg.get("price")
        price_dollars = msg.get("price_dollars")
        delta = msg.get("delta")
        side = msg.get("side")
        
        print(f"\n[Delta] {market_ticker} | Side: {side} | Price: {price} ({price_dollars}) | Delta: {delta:+d}")

    async def on_error(self, error):
        """Callback for handling errors."""
        print("WebSocket error:", error)

    async def on_close(self, close_status_code, close_msg):
        """Callback when WebSocket connection is closed."""
        print("WebSocket connection closed with code:", close_status_code, "and message:", close_msg)