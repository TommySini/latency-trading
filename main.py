from cryptography.hazmat.primitives import serialization
import os
import asyncio
from dotenv import load_dotenv

from clients import KalshiWebSocketClient

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
KEYID = os.getenv("KALSHI_KEY_ID")
PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH", "private_key.pem")

if not KEYID:
    raise ValueError("KALSHI_KEY_ID not found in environment variables")

if not os.path.exists(PRIVATE_KEY_PATH):
    raise FileNotFoundError(f"Private key file not found: {PRIVATE_KEY_PATH}")

# Load private key from .pem file
with open(PRIVATE_KEY_PATH, "rb") as key_file:
    private_key_pem = key_file.read()

private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
)

async def stream_orderbook():
    """Connect to WebSocket and stream orderbook data for a market ticker."""
    # Ask for market ticker
    market_ticker = input("Enter market ticker: ").strip()
    
    if not market_ticker:
        print("Error: Market ticker cannot be empty")
        return
    
    # Initialize the WebSocket client
    client = KalshiWebSocketClient(
        key_id=KEYID,
        private_key=private_key
    )
    
    # Connect and stream
    try:
        await client.connect(market_ticker=market_ticker)
    except KeyboardInterrupt:
        print("\nStreaming stopped by user")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(stream_orderbook())