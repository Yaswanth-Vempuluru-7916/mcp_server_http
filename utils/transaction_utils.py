import os
import requests
import json
from typing import Dict, Any
from urllib.parse import quote_plus
from dotenv import load_dotenv
import psycopg2
from dateutil import parser
from requests.exceptions import RequestException
from google import genai
from google.genai import types

# Load environment variables
load_dotenv()

# Configuration
BASE_URL = os.getenv("BASE_URL")
MATCHED_ORDER_URL = "https://orderbook-v2-staging.hashira.io/id/{create_id}/matched"
TOKEN = os.getenv("TOKEN")
API_TOKEN = f"Bearer {TOKEN}"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}
LOG_TIME_WINDOW = 300  # ±300 seconds
DEFAULT_LIMIT = 100
MAX_LOOKBACK = 30 * 24 * 3600  # 30 days
API_TIMEOUT = 10  # seconds
CONTAINER = "/staging-evm-relay"

# Initialize Gemini client
if not GEMINI_API_KEY:
    raise ValueError("Missing GEMINI_API_KEY in .env file.")
try:
    genai_client = genai.Client(api_key=GEMINI_API_KEY)
except ValueError as e:
    raise ValueError(f"Failed to initialize Gemini client: {e}")

def fetch_db_info(initiator_source_address: str) -> Dict[str, Any]:
    """Fetch the latest create_orders record for the initiator source address."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql_query = "SELECT * FROM create_orders WHERE initiator_source_address = %s ORDER BY created_at ASC LIMIT 1"
        cursor.execute(sql_query, (initiator_source_address,))
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchone()
        if result:
            return dict(zip(columns, result))
        return {}
    except Exception as e:
        raise RuntimeError(f"Database query failed: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_logs(create_id: str, start_time: int, end_time: int, limit: int = DEFAULT_LIMIT) -> Dict[str, Any]:
    """Fetch logs and use Gemini to determine if the order was created."""
    if not API_TOKEN:
        raise ValueError("Missing API_TOKEN. Ensure .env is configured correctly.")
    
    if start_time > end_time:
        start_time, end_time = end_time, start_time
    if end_time - start_time > MAX_LOOKBACK:
        start_time = end_time - MAX_LOOKBACK
    
    query = quote_plus(f'{{container="{CONTAINER}"}}')
    url = f"{BASE_URL}?query={query}&start={start_time}&end={end_time}&limit={limit}"
    
    try:
        response = requests.get(url, headers={
            "Authorization": API_TOKEN,
            "Content-Type": "application/json"
        }, timeout=API_TIMEOUT)
        response.raise_for_status()
        logs = response.json()
        log_entries = logs.get("data", {}).get("result", [])
        raw_logs = [msg for entry in log_entries for _, msg in entry.get("values", [])]
        log_result = "\n".join(raw_logs) if raw_logs else "No logs found."

        # Use Gemini to analyze logs
        prompt = (
            f"Analyze the following logs and determine if the order with create_id '{create_id}' was created. "
            "Return only 'Yes' if the create_id is found in the logs, or 'No' if it is not found.\n\n"
            f"Logs:\n{log_result}"
        )
        try:
            gemini_response = genai_client.models.generate_content(
                model="gemini-2.5-pro-exp-03-25",
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0)
            )
            gemini_output = gemini_response.text.strip() if gemini_response.text else "No"
            is_order_created = gemini_output == "Yes"
        except Exception as e:
            # Fallback to manual check if Gemini fails
            is_order_created = any(create_id in msg for msg in raw_logs)
            log_result += f"\nGemini API error: {str(e)}. Falling back to manual check."

        return {
            "is_order_created": is_order_created,
            "raw_logs": log_result
        }
    except RequestException as e:
        raise RuntimeError(f"Request failed for container '{CONTAINER}': {e}") from e

def check_matched_order(create_id: str) -> Dict[str, Any]:
    """Check the matched order status for the given create_id."""
    try:
        url = MATCHED_ORDER_URL.format(create_id=create_id)
        response = requests.get(url, timeout=API_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        return {"error": f"Matched order API request failed for create_id '{create_id}': {str(e)}"}

def transaction_status(initiator_source_address: str) -> str:
    """Process the transaction status for the given initiator source address."""
    result_str = f"Transaction status for initiator_source_address '{initiator_source_address}':\n"
    create_order_success = False
    create_id = None
    unix_timestamp = None
    
    # Step 1: Fetch database info
    try:
        db_result = fetch_db_info(initiator_source_address)
        if db_result:
            result_str += "Database results from create_orders:\n"
            result_str += "\n".join([f"{k}: {v}" for k, v in db_result.items()]) + "\n"
            create_id = db_result.get("create_id")
            timestamp_str = db_result.get("created_at")
            if timestamp_str:
                try:
                    dt = parser.isoparse(str(timestamp_str))
                    unix_timestamp = int(dt.timestamp())
                except Exception as e:
                    result_str += f"Failed to parse timestamp '{timestamp_str}': {e}\n"
        else:
            result_str += f"No data found for initiator_source_address '{initiator_source_address}' in create_orders.\n"
            return result_str
    except Exception as e:
        result_str += f"Database query error: {str(e)}\n"
        return result_str
    
    # Step 2: Fetch and analyze logs using Gemini
    if create_id and unix_timestamp:
        start_time = unix_timestamp - LOG_TIME_WINDOW
        end_time = unix_timestamp + LOG_TIME_WINDOW
        try:
            log_result = fetch_logs(create_id, start_time, end_time)
            create_order_success = log_result["is_order_created"]
            raw_logs = log_result["raw_logs"]
            
            if create_order_success:
                result_str += f"\nOrder created successfully: create_id '{create_id}' found in /staging-evm-relay logs.\n"
            else:
                result_str += f"\nOrder not confirmed: create_id '{create_id}' not found in /staging-evm-relay logs.\n"
            result_str += f"Logs from /staging-evm-relay (±{LOG_TIME_WINDOW}s around {unix_timestamp}):\n{raw_logs}\n"
        except Exception as e:
            result_str += f"\nLogs from /staging-evm-relay: Error fetching logs: {str(e)}\n"
            return result_str
    
    # Step 3: Check matched order using Gemini
    if create_id and create_order_success:
        matched_order_result = check_matched_order(create_id)
        result_str += f"\nMatched order API response for create_id '{create_id}':\n{str(matched_order_result)}\n"
        
        is_matched = False
        user_initiated = False
        cobi_initiated = False
        user_redeemed = False
        cobi_redeemed = False
        
        # Use Gemini to analyze matched order result
        prompt = (
            f"Analyze the following JSON response from a matched order API for create_id '{create_id}'. "
            "Determine the transaction status and return a JSON object with the following fields:\n"
            "- is_matched: boolean (true if source_swap or destination_swap exists)\n"
            "- user_initiated: boolean (true if source_swap.initiate_tx_hash exists and current_confirmations >= required_confirmations)\n"
            "- cobi_initiated: boolean (true if destination_swap.initiate_tx_hash exists and current_confirmations >= required_confirmations)\n"
            "- user_redeemed: boolean (true if source_swap.redeem_tx_hash exists)\n"
            "- cobi_redeemed: boolean (true if destination_swap.redeem_tx_hash exists)\n"
            "- messages: array of strings (descriptive messages for each true status, e.g., 'Order matched successfully for create_id ...')\n"
            "Only include messages for statuses that are true. If status is not 'Ok' or result is empty, return all flags as false and an empty messages array.\n\n"
            f"JSON Response:\n{json.dumps(matched_order_result, indent=2)}"
        )
        
        try:
            gemini_response = genai_client.models.generate_content(
                model="gemini-2.5-pro-exp-03-25",
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0)
            )
            gemini_output = json.loads(gemini_response.text.strip()) if gemini_response.text else {}
            
            # Validate Gemini output
            if not isinstance(gemini_output, dict):
                raise ValueError("Invalid Gemini output: Expected JSON object")
            
            is_matched = gemini_output.get("is_matched", False)
            user_initiated = gemini_output.get("user_initiated", False)
            cobi_initiated = gemini_output.get("cobi_initiated", False)
            user_redeemed = gemini_output.get("user_redeemed", False)
            cobi_redeemed = gemini_output.get("cobi_redeemed", False)
            messages = gemini_output.get("messages", [])
            
            # Append Gemini-generated messages
            for message in messages:
                result_str += f"\n{message}\n"
                
        except Exception as e:
            print(f"Error in Gemini :  {str(e)}")

            # Fallback to manual logic
            # result_str += f"\nGemini API error for matched order analysis: {str(e)}. Falling back to manual processing.\n"
            # if matched_order_result.get("status") == "Ok" and matched_order_result.get("result"):
            #     result_data = matched_order_result.get("result", {})
            #     if result_data.get("source_swap") or result_data.get("destination_swap"):
            #         is_matched = True
            #         result_str += f"\nOrder matched successfully for create_id '{create_id}'.\n"
                
            #     # Check user initiation
            #     if result_data.get("source_swap"):
            #         source_swap = result_data["source_swap"]
            #         initiate_tx_hash = source_swap.get("initiate_tx_hash", "")
            #         current_confirmations = source_swap.get("current_confirmations", 0)
            #         required_confirmations = source_swap.get("required_confirmations", 1)
            #         if initiate_tx_hash and current_confirmations >= required_confirmations:
            #             user_initiated = True
            #             result_str += f"User has initiated the transaction for create_id '{create_id}'.\n"
                
            #     # Check Cobi initiation
            #     if result_data.get("destination_swap"):
            #         destination_swap = result_data["destination_swap"]
            #         initiate_tx_hash = destination_swap.get("initiate_tx_hash", "")
            #         current_confirmations = destination_swap.get("current_confirmations", 0)
            #         required_confirmations = destination_swap.get("required_confirmations", 1)
            #         if initiate_tx_hash and current_confirmations >= required_confirmations:
            #             cobi_initiated = True
            #             result_str += f"Cobi has initiated the transaction for create_id '{create_id}'.\n"
                
            #     # Check user redeem
            #     if result_data.get("source_swap"):
            #         source_swap = result_data["source_swap"]
            #         redeem_tx_hash = source_swap.get("redeem_tx_hash", "")
            #         if redeem_tx_hash:
            #             user_redeemed = True
            #             result_str += f"User has redeemed the transaction for create_id '{create_id}'.\n"
                
            #     # Check Cobi redeem
            #     if result_data.get("destination_swap"):
            #         destination_swap = result_data["destination_swap"]
            #         redeem_tx_hash = destination_swap.get("redeem_tx_hash", "")
            #         if redeem_tx_hash:
            #             cobi_redeemed = True
            #             result_str += f"Cobi has redeemed the transaction for create_id '{create_id}'.\n"
        
        # Final summary
        result_str += "\nFinal Transaction Status Summary:\n"
        result_str += f"- Order Created: {'Yes' if create_order_success else 'No'}\n"
        result_str += f"- Order Matched: {'Yes' if is_matched else 'No'}\n"
        result_str += f"- User Initiated: {'Yes' if user_initiated else 'No'}\n"
        result_str += f"- Cobi Initiated: {'Yes' if cobi_initiated else 'No'}\n"
        result_str += f"- User Redeemed: {'Yes' if user_redeemed else 'No'}\n"
        result_str += f"- Cobi Redeemed: {'Yes' if cobi_redeemed else 'No'}\n"
    
    return result_str