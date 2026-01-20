import time
import json
import requests
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(",")

if not FINNHUB_API_KEY:
    raise ValueError("FINNHUB_API_KEY is not set in .env file")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

NEWS_URL = "https://finnhub.io/api/v1/news"
NEWS_CATEGORY = os.getenv("FINNHUB_NEWS_CATEGORY", "general")


def fetch_news():
    try:
        response = requests.get(
            NEWS_URL,
            params={"category": NEWS_CATEGORY, "token": FINNHUB_API_KEY},
            timeout=15,
        )
        response.raise_for_status()
        articles = response.json()
        return articles
    except Exception as e:
        print(f"Error fetching news: {e}")
        return []


print("News producer started. Sending to topic 'company-news'...")

while True:
    articles = fetch_news()
    for article in articles:
        article["fetched_at"] = int(time.time())
        print(f"Producing: {article}")
        producer.send("company-news", value=article)
    time.sleep(60)
