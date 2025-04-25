import requests
from urllib.parse import quote_plus
from requests.exceptions import RequestException
from utils.config import Config
from utils.logging_setup import setup_logging

logger, console = setup_logging()

def fetch_logs(create_id: str, start_time: int, end_time: int, container: str, limit: int = Config.DEFAULT_LIMIT) -> dict:
    if not Config.API_TOKEN:
        logger.error("Missing API_TOKEN. Ensure .env is configured correctly.")
        raise ValueError("Missing API_TOKEN. Ensure .env is configured correctly.")
    
    if start_time > end_time:
        start_time, end_time = end_time, start_time
    if end_time - start_time > Config.MAX_LOOKBACK:
        start_time = end_time - Config.MAX_LOOKBACK
    
    query = quote_plus(f'{{container="{container}"}}')
    url = f"{Config.BASE_URL}?query={query}&start={start_time}&end={end_time}&limit={limit}"
    
    try:
        logger.info(f"Fetching logs from {url}")
        response = requests.get(url, headers={
            "Authorization": Config.API_TOKEN,
            "Content-Type": "application/json"
        }, timeout=Config.API_TIMEOUT)
        response.raise_for_status()
        logs = response.json()
        log_entries = logs.get("data", {}).get("result", [])
        raw_logs = [msg for entry in log_entries for _, msg in entry.get("values", [])]
        log_result = "\n".join(raw_logs) if raw_logs else "No logs found."
        logger.info(f"Fetched {len(raw_logs)} logs from container: {container}")
        return {
            "raw_logs": log_result,
            "raw_log_list": raw_logs
        }
    except RequestException as e:
        logger.error(f"Request failed for container '{container}': {e}")
        raise RuntimeError(f"Request failed for container '{container}': {e}")

def check_matched_order(create_id: str) -> dict:
    try:
        url = Config.MATCHED_ORDER_URL.format(create_id=create_id)
        logger.info(f"Checking matched order at {url}")
        response = requests.get(url, timeout=Config.API_TIMEOUT)
        response.raise_for_status()
        logger.info(f"Matched order API call successful for create_id: {create_id}")
        return response.json()
    except RequestException as e:
        logger.error(f"Matched order API request failed for create_id '{create_id}': {e}")
        return {"error": f"Matched order API request failed for create_id '{create_id}': {e}"}