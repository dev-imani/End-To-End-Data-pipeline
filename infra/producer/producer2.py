# Import requirements
import time
import json
import requests
from kafka import KafkaProducer

# Define variables for API
API_KEY = "F3FRYOWDEVYHD7EC"   # replace with your real key
BASE_URL = "https://www.alphavantage.co/query"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

# Initial Producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Retrieve Data
def fetch_quote(symbol):
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        raw = response.json().get("Global Quote", {})

        # Map Alpha Vantage fields to Finnhub-style keys
        return {
            "c": float(raw.get("05. price", 0)),                 # current price
            "d": float(raw.get("09. change", 0)),                # change
            "dp": float(raw.get("10. change percent", "0%").strip('%')) / 100,
            "h": float(raw.get("03. high", 0)),                  # high
            "l": float(raw.get("04. low", 0)),                   # low
            "o": float(raw.get("02. open", 0)),                  # open
            "pc": float(raw.get("08. previous close", 0)),       # previous close
            "t": int(time.time()),                               # timestamp
            "symbol": symbol,
            "fetched_at": int(time.time())
        }
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

# Looping and Pushing to Stream
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {quote}")
            producer.send("stock-quotes2", value=quote)
    time.sleep(6)
