# # Import requirements
# import time
# import json
# import requests
# from kafka import KafkaProducer

# # Define variables for API
# API_KEY = "d5gdb49r01qsbeejs4pgd5gdb49r01qsbeejs4q0"
# BASE_URL = "https://finnhub.io/api/v1/quote"
# SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

# # Kafka Configuration
# KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092']
# KAFKA_TOPIC = 'stock-quotes'

# # Initialize Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     key_serializer=lambda k: k.encode('utf-8') if k else None
# )

# print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
# print(f"Publishing to topic: {KAFKA_TOPIC}")

# # Retrieve Data


# def fetch_quote(symbol):
#     url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         data = response.json()
#         data["symbol"] = symbol
#         data["fetched_at"] = int(time.time())
#         return data
#     except Exception as e:
#         print(f"Error fetching {symbol}: {e}")
#         return None


# # Looping and sending to Kafka
# try:
#     while True:
#         for symbol in SYMBOLS:
#             quote = fetch_quote(symbol)
#             if quote:
#                 # Send to Kafka
#                 producer.send(KAFKA_TOPIC, key=symbol, value=quote)
#                 print(f"Sent {symbol}: {quote['c']} (current price)")

#         # Flush to ensure all messages are sent
#         producer.flush()
#         time.sleep(6)
# except KeyboardInterrupt:
#     print("\nShutting down producer...")
# finally:
#     producer.close()
#     print("Producer closed.")