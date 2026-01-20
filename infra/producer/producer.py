# Import requirements
import time
import json
import requests
import os
import threading
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(",")

# Initialize Kafka Producer (shared across all threads)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ============================================
# NEWS PRODUCER
# ============================================


def produce_news():
    NEWS_API_KEY = os.getenv("NEWS_API_KEY")
    NEWS_URL = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={NEWS_API_KEY}"

    print("News producer started...")
    while True:
        try:
            response = requests.get(NEWS_URL)
            response.raise_for_status()
            data = response.json()
            data["fetched_at"] = int(time.time())
            print(f"Producing news: {len(data.get('articles', []))} articles")
            producer.send("news-headlines", value=data)
        except Exception as e:
            print(f"Error fetching news: {e}")
        time.sleep(60)

# ============================================
# CRYPTO PRODUCER
# ============================================


def produce_crypto():
    API_KEY = os.getenv("FINNHUB_API_KEY")
    CRYPTO_SYMBOLS = os.getenv(
        "FINNHUB_CRYPTO_SYMBOLS", "BINANCE:BTCUSDT,BINANCE:ETHUSDT").split(",")

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

    print("Crypto producer started...")
    while True:
        for symbol in CRYPTO_SYMBOLS:
            quote = fetch_crypto(symbol)
            if quote:
                print(f"Producing crypto: {symbol} - ${quote.get('c', 0)}")
                producer.send("crypto-quotes", value=quote)
        time.sleep(10)

# ============================================
# STOCK QUOTES PRODUCER
# ============================================


def produce_stocks():
    API_KEY = os.getenv("FINNHUB_API_KEY")
    BASE_URL = os.getenv("FINNHUB_BASE_URL", "https://finnhub.io/api/v1/quote")
    SYMBOLS = os.getenv("FINNHUB_SYMBOLS", "AAPL,MSFT,GOOGL").split(",")

    # Validate API Key
    if not API_KEY:
        raise ValueError("FINNHUB_API_KEY is not set in .env file")

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

    print("Stock quotes producer started...")
    while True:
        for symbol in SYMBOLS:
            quote = fetch_quote(symbol)
            if quote:
                print(f"Producing stock: {symbol} - ${quote.get('c', 0)}")
                producer.send("stock-quotes", value=quote)
        time.sleep(6)


# ============================================
# MAIN: Start all producers in separate threads
# ============================================
if __name__ == "__main__":
    print("Starting multi-producer system...")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")

    # Create threads for each producer
    news_thread = threading.Thread(target=produce_news, daemon=True)
    crypto_thread = threading.Thread(target=produce_crypto, daemon=True)
    stocks_thread = threading.Thread(target=produce_stocks, daemon=True)

    # Start all threads
    news_thread.start()
    crypto_thread.start()
    stocks_thread.start()

    print("\n✓ All producers are running!")
    print("Topics: news-headlines, crypto-quotes, stock-quotes")
    print("Press Ctrl+C to stop...\n")

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down producers...")
        producer.close()
