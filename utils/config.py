import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    BASE_URL = os.getenv("BASE_URL")
    MATCHED_ORDER_URL = "https://orderbook-v2-staging.hashira.io/id/{create_id}/matched"
    TOKEN = os.getenv("TOKEN")
    API_TOKEN = f"Bearer {TOKEN}" if TOKEN else None
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }
    
    DEFAULT_LIMIT = 5000
    MAX_LOOKBACK = 2595600
    API_TIMEOUT = 10  # seconds
    EVM_RELAY_CONTAINER = "/staging-evm-relay"
    BIT_PONDER_CONTAINER = "/stage-bit-ponder"
    COBI_V2_CONTAINER = "/staging-cobi-v2"