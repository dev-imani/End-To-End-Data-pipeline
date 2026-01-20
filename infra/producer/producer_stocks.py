# Import requirements
import time
import json
import requests
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define variables for API
API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = os.getenv("FINNHUB_BASE_URL", "https://finnhub.io/api/v1/quote")
SYMBOLS = os.getenv("FINNHUB_SYMBOLS", "AAPL,MSFT,GOOGL").split(",")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(",")

# Validate API Key
if not API_KEY:
    raise ValueError("FINNHUB_API_KEY is not set in .env file")

# Initial Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Retrieve Data


def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None


print("Stock quotes producer started. Sending to topic 'stock-quotes'...")

# Looping and Pushing to Stream
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {symbol} - ${quote.get('c', 0)}")
            producer.send("stock-quotes", value=quote)
    time.sleep(6)
