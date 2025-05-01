import websocket
import requests
import json
import yaml
import logging
import logging.config
from pathlib import Path
from confluent_kafka import Producer
import time

with open("config/logging_config.yaml", "r") as f:
    logging_config = yaml.safe_load(f)
logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)

class FinnhubClient:
    def __init__(self, config_path="config/finnhub_config.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)["finnhub"]
        self.api_key = self.config["api_key"]
        self.rest_endpoint = self.config["rest_endpoint"]
        self.ws_endpoint = self.config["websocket_endpoint"]
        self.symbols = self.config.get("symbols", [])
        self.ws = None
        self.reconnect_delay = 5
        self.max_reconnect_attempts = 5
        # config kafka ... (pour producer)
        self.kafka_producer = Producer({
            "bootstrap.servers": self.config.get("kafka_bootstrap_servers", "localhost:29092")
        })

    def get_symbols(self, exchange="US"):
        try:
            url = f"{self.rest_endpoint}/stock/symbol?exchange={exchange}&token={self.api_key}"
            response = requests.get(url)
            response.raise_for_status()
            symbols = response.json()
            logger.info(f"Retrieved {len(symbols)} symbols from exchange {exchange}")
            return symbols
        except requests.RequestException as e:
            logger.error(f"Failed to fetch symbols: {e}")
            raise

    def on_message(self, ws, message):
        logger.info(f"Received message: {message}")
        try:
            data = json.loads(message)
            if data.get("type") == "trade":
                self.kafka_producer.produce(
                    topic="raw_financial_data",
                    value=message.encode("utf-8"),
                    callback=self.delivery_report
                )
                self.kafka_producer.flush()
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON message: {e}")
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}", exc_info=True)

    def on_close(self, ws, close_status_code, close_msg):
        logger.info(f"WebSocket connection closed: status={close_status_code}, message={close_msg}")
        self.kafka_producer.flush()

    def on_open(self, ws):
        logger.info("WebSocket connection opened")
        for symbol in self.symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            logger.info(f"Subscribed to {symbol}")

    def connect_websocket(self):
        attempt = 0
        while attempt < self.max_reconnect_attempts:
            try:
                websocket.enableTrace(self.config.get("websocket_trace", False))
                self.ws = websocket.WebSocketApp(
                    f"{self.ws_endpoint}?token={self.api_key}",
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open
                )
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
                logger.info("WebSocket stopped, attempting to reconnect...")
                attempt += 1
                time.sleep(self.reconnect_delay)
            except Exception as e:
                logger.error(f"WebSocket connection failed: {e}")
                attempt += 1
                time.sleep(self.reconnect_delay)
        logger.error("Max reconnect attempts reached, stopping...")

if __name__ == "__main__":
    client = FinnhubClient()
    symbols = client.get_symbols()
    print(f"First 5 symbols: {symbols[:5]}")
    client.connect_websocket()