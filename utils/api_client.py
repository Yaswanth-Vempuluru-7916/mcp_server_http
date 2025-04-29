# api_client.py
import time
from typing import Optional
import requests
from urllib.parse import quote_plus
from requests.exceptions import RequestException
from utils.config import Config
from utils.logging_setup import setup_logging

logger, console = setup_logging()

def fetch_logs(
    create_id: str,
    start_time: int,
    container: str,
    source_swap_id: Optional[str] = None,
    destination_swap_id: Optional[str] = None,
    secret_hash: Optional[str] = None,
    limit: int = Config.DEFAULT_LIMIT
) -> dict:
    if not Config.API_TOKEN:
        logger.error("Missing API_TOKEN. Ensure .env is configured correctly.")
        raise ValueError("Missing API_TOKEN. Ensure .env is configured correctly.")
    
    # List of identifiers to query separately
    identifiers = [(create_id, "create_id")]
    if source_swap_id:
        identifiers.append((source_swap_id, "source_swap_id"))
    if destination_swap_id:
        identifiers.append((destination_swap_id, "destination_swap_id"))
    if secret_hash:
        identifiers.append((secret_hash, "secret_hash"))
    
    raw_logs = []
    
    # Fetch logs for each identifier separately
    for identifier, identifier_name in identifiers:
        current_start = start_time
        logger.info(f"Fetching logs for {identifier_name}: {identifier}")
        
        while True:
            # Construct the query for the single identifier using |~
            query = quote_plus(f'{{container="{container}"}} |~ "{identifier}"')
            url = f"{Config.BASE_URL}?query={query}&start={current_start}&limit={limit}&direction=forward"
            try:
                logger.info(f"Fetching logs from {url}")
                response = requests.get(url, headers={
                    "Authorization": Config.API_TOKEN,
                    "Content-Type": "application/json"
                }, timeout=Config.API_TIMEOUT)
                response.raise_for_status()
                logs = response.json()
                log_entries = logs.get("data", {}).get("result", [])
                oldest_timestamp = current_start  # Fallback to current_start if no logs
                
                # Extract logs and find the oldest timestamp
                current_fetch_logs = []
                for entry in log_entries:
                    for ts, msg in entry.get("values", []):
                        current_fetch_logs.append(msg)
                        # Loki timestamps are in nanoseconds, convert to seconds
                        ts_seconds = int(ts) // 1_000_000_000
                        oldest_timestamp = min(oldest_timestamp, ts_seconds)
                
                raw_logs.extend(current_fetch_logs)
                logger.info(f"Fetched {len(current_fetch_logs)} logs for {identifier_name} from start time {current_start}")
                
                # If fewer than limit logs were returned, no more logs to fetch for this identifier
                if len(current_fetch_logs) < limit:
                    break
                
                # If limit (5000) was hit, fetch again from the oldest timestamp
                if oldest_timestamp <= current_start:
                    logger.warning(f"Oldest timestamp {oldest_timestamp} is not newer than current start {current_start} for {identifier_name}. Stopping to avoid infinite loop.")
                    break
                current_start = oldest_timestamp
                logger.info(f"Hit limit of {limit} logs for {identifier_name}. Fetching again from {current_start}")
            
            except RequestException as e:
                logger.error(f"Request failed for {url}: {e}")
                raise RuntimeError(f"Request failed for container '{container}' with {identifier_name}: {e}")
    
    # Remove duplicates while preserving order
    unique_logs = list(dict.fromkeys(raw_logs))
    logger.info(f"Total fetched {len(unique_logs)} unique logs from container: {container}")
    
    # Process results
    log_result = "\n".join(unique_logs) if unique_logs else "No logs found."
    return {
        "raw_logs": log_result,
        "raw_log_list": unique_logs
    }

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