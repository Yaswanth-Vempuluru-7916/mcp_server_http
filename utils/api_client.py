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
    
    # List of identifiers to query
    identifiers = [create_id]
    if source_swap_id:
        identifiers.append(source_swap_id)
    if destination_swap_id:
        identifiers.append(destination_swap_id)
    if secret_hash:
        identifiers.append(secret_hash)
    
    raw_logs = []
    current_start = start_time
    fetch_limit = 5000  
    iteration = 0
    recent_logs_fetched = False
    logger.info(f"Fetching logs for identifiers: {identifiers}, container: {container}")
    
    # Construct the query using |~ with regex pattern for all identifiers
    regex_pattern = "|".join(map(str, identifiers))
    query = quote_plus(f'{{container="{container}"}} |~ "{regex_pattern}"')
    
    while True:
        iteration += 1
        # After 5 iterations, fetch recent logs without start/end/direction
        if iteration > 5 and not recent_logs_fetched:
            url = f"{Config.BASE_URL}?query={query}&limit={fetch_limit}"
            recent_logs_fetched = True
        else:
            url = f"{Config.BASE_URL}?query={query}&start={current_start}&limit={fetch_limit}&direction=forward"
        
        try:
            logger.info(f"Fetching logs from {url}")
            response = requests.get(url, headers={
                "Authorization": Config.API_TOKEN,
                "Content-Type": "application/json"
            }, timeout=Config.API_TIMEOUT)
            response.raise_for_status()
            logs = response.json()
            log_entries = logs.get("data", {}).get("result", [])
            oldest_timestamp = float('inf')
            newest_timestamp = float('-inf')
            
            # Extract logs and find timestamps
            current_fetch_logs = []
            for entry in log_entries:
                for ts, msg in entry.get("values", []):
                    current_fetch_logs.append(msg)
                    ts_seconds = int(ts) // 1_000_000_000
                    oldest_timestamp = min(oldest_timestamp, ts_seconds)
                    newest_timestamp = max(newest_timestamp, ts_seconds)
            
            raw_logs.extend(current_fetch_logs)
            logger.info(f"Iteration {iteration}: Fetched {len(current_fetch_logs)} logs from start time {current_start if not recent_logs_fetched else 'recent'}")
            if current_fetch_logs:
                logger.info(f"Timestamp range: min={oldest_timestamp}, max={newest_timestamp}")
            else:
                logger.info("No timestamps available (empty response)")
            
            # Stop if no logs or fewer than limit (except for recent fetch)
            if not current_fetch_logs or (len(current_fetch_logs) < fetch_limit and not recent_logs_fetched):
                logger.info(f"Stopping: Fetched {len(current_fetch_logs)} logs, less than limit {fetch_limit}")
                break
            
            # For recent logs fetch, stop after one request
            if recent_logs_fetched:
                logger.info("Completed recent logs fetch. Stopping.")
                break
            
            # If no valid timestamps, stop
            if newest_timestamp == float('-inf'):
                logger.warning(f"No valid timestamps found. Stopping.")
                break
            
            # Update start to newest timestamp for next iteration
            current_start = newest_timestamp
            logger.info(f"Hit limit of {fetch_limit} logs. Newest timestamp: {newest_timestamp}, next start: {current_start}")
            
            # Avoid rate limiting
            time.sleep(0.5)  # 0.5-second delay
        
        except RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise RuntimeError(f"Request failed for container '{container}' with identifiers: {e}")
    
    logger.info(f"Total fetched {len(raw_logs)} logs from container: {container}")
    
    # Process results
    log_result = "\n".join(raw_logs) if raw_logs else "No logs found."
    return {
        "raw_logs": log_result,
        "raw_log_list": raw_logs
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