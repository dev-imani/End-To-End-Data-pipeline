import time
import json
import requests
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY")
CRYPTO_SYMBOLS = os.getenv("FINNHUB_CRYPTO_SYMBOLS",
                           "BINANCE:BTCUSDT,BINANCE:ETHUSDT").split(",")
BASE_URL = "https://finnhub.io/api/v1/crypto/candle"
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(",")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_crypto(symbol):
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching crypto {symbol}: {e}")
        return None


print("Crypto producer started. Sending to topic 'crypto-quotes'...")

while True:
    for symbol in CRYPTO_SYMBOLS:
        quote = fetch_crypto(symbol)
        if quote:
            print(f"Producing: {quote}")
            producer.send("crypto-quotes", value=quote)
    time.sleep(10)
