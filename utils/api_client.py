import time
import requests
from typing import Optional
from urllib.parse import quote_plus
from requests.exceptions import RequestException
from utils.config import Config
from utils.logging_setup import setup_logging
from math import floor

logger, console = setup_logging()

def fetch_logs(create_id: str, start_time: int, container: str, end_time: Optional[int] = None, limit: int = Config.DEFAULT_LIMIT) -> dict:
    if not Config.API_TOKEN:
        logger.error("Missing API_TOKEN. Ensure .env is configured correctly.")
        raise ValueError("Missing API_TOKEN. Ensure .env is configured correctly.")
    
    # Set end_time to current time if not provided
    end_time = int(time.time()) if end_time is None else end_time
    
    # Ensure start_time <= end_time
    if start_time > end_time:
        start_time, end_time = end_time, start_time
    
    # Respect MAX_LOOKBACK limit (e.g., 30d1h = 2,595,600 seconds)
    max_end_time = start_time + Config.MAX_LOOKBACK
    end_time = min(end_time, max_end_time)
    # 1988a35d6c0cf1693ed59a2abff1f1879c443800362e3fa49cc7b883e3c97ade
    # Define 1-week window size (7 days = 604,800 seconds)
    # WINDOW_SECONDS = 604_800
    # WINDOW_SECONDS = 302_400 #1/2 week 
    WINDOW_SECONDS = 172_800 #2 days
    # Minimum window size to avoid redundant queries (e.g., 60 seconds)
    MIN_WINDOW_SECONDS = 60
    query = quote_plus(f'{{container="{container}"}}')
    current_start = start_time
    raw_logs = []
    iteration = 0
    seen_ranges = set()  # Track queried (start, end) ranges to avoid duplicates
    max_iterations = floor(Config.MAX_LOOKBACK / WINDOW_SECONDS)  # e.g., floor(2,595,600 / 172,800) = 15
    # max_iterations = 5  # Limit iterations to prevent excessive queries (If 1 week)
    # max_iterations = 15  # Limit iterations to prevent excessive queries (if 0.5 week)
    
    while current_start < end_time and iteration < max_iterations:
        # Calculate end of the current 1-week window
        current_end = min(current_start + WINDOW_SECONDS, end_time)

        # Skip if the time range is too small (avoid redundant queries)
        if current_end - current_start < MIN_WINDOW_SECONDS:
            logger.info(f"Skipping iteration {iteration + 1}: time range {current_start} to {current_end} is too small (< {MIN_WINDOW_SECONDS} seconds)")
            current_start = current_end
            iteration += 1
            continue

        current_range = (current_start, current_end)
        if current_range in seen_ranges:
            logger.info(f"Skipping iteration {iteration + 1}: time range {current_start} to {current_end} already queried")
            current_start = current_end
            iteration += 1
            continue
        seen_ranges.add(current_range)

        url = f"{Config.BASE_URL}?query={query}&start={current_start}&end={current_end}&limit={limit}"
        
        try:
            logger.info(f"Fetching logs for iteration {iteration + 1} from {url}")
            response = requests.get(url, headers={
                "Authorization": Config.API_TOKEN,
                "Content-Type": "application/json"
            }, timeout=Config.API_TIMEOUT)
            response.raise_for_status()
            logs = response.json()
            log_entries = logs.get("data", {}).get("result", [])
            window_logs = []
            latest_timestamp = current_start  # Fallback to current_start if no logs
            
            # Extract logs and find the latest timestamp
            for entry in log_entries:
                for ts, msg in entry.get("values", []):
                    window_logs.append(msg)
                    # Loki timestamps are in nanoseconds, convert to seconds
                    ts_seconds = int(ts) // 1_000_000_000
                    latest_timestamp = max(latest_timestamp, ts_seconds)
            
            raw_logs.extend(window_logs)
            logger.info(f"Fetched {len(window_logs)} logs for iteration {iteration + 1} ({current_start} to {current_end})")
            
            # Update current_start to the latest timestamp (or current_end if no logs)
            current_start = latest_timestamp if window_logs else current_end
            iteration += 1
            
        except RequestException as e:
            logger.error(f"Request failed for iteration {iteration + 1} ({current_start} to {current_end}): {e}")
            raise RuntimeError(f"Request failed for container '{container}': {e}")
    
    # Process results
    log_result = "\n".join(raw_logs) if raw_logs else "No logs found."
    logger.info(f"Total fetched {len(raw_logs)} logs from container: {container}")
    return {
        "raw_logs": log_result,
        "raw_log_list": raw_logs,
        "end_time": end_time  # Return end_time for transaction_utils
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