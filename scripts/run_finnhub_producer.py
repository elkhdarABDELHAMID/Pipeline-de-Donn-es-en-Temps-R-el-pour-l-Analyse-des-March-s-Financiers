from src.data_source.finnhub_client import FinnhubClient

if __name__ == "__main__":
    client = FinnhubClient()
    client.connect_websocket()