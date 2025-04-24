import os
import requests
import time
import logging
from typing import Dict, Any
from urllib.parse import quote_plus
from dotenv import load_dotenv
import psycopg2
from dateutil import parser
from requests.exceptions import RequestException
import google.generativeai as genai
from rich.console import Console

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize rich console
console = Console()

# Load environment variables
load_dotenv()

# Configuration
BASE_URL = os.getenv("BASE_URL")
MATCHED_ORDER_URL = "https://orderbook-v2-staging.hashira.io/id/{create_id}/matched"
TOKEN = os.getenv("TOKEN")
API_TOKEN = f"Bearer {TOKEN}" if TOKEN else None
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# starknet_sepolia -->  arbitrum_sepolia
# CREATE_ID = "2def3eb62b4b546defab9a658eb1024305d67c7607d83ec531ce47896bdf8fce"
# CREATE_ID = "c66d88df73f94fc84c8c3446e10526da22c4761489673a69f09489d96c73338a"

# CREATE_ID = "8c0692efc8b0f1ffc0554c46b63f9fbe5a00b28d064b0ebb27a2acdf10453993"
# arbitrum_sepolia  --> bitcoin_testnet 
# CREATE_ID = "401844f413bd36eea9d30b5487f7e2bfd0df5c9b8fa6d29fbc6ca3ac0c3beae1"
# bitcoin_testnet  --> arbitrum_sepolia
# CREATE_ID = "ed5c8f040cf59f6f04d46ce37f712bd86c1dccdb283766e0dfa8ec8f10764143"
# arbitrum_sepolia  --> starknet_sepolia 
# CREATE_ID = "9f6c73239f7cd9e1bed9aa2d8f07c8c24bbd724b481020a2a1483296a15a77f6"
#Filtered logs check for staging-cobi-v2
CREATE_ID = "9c2684b55c78afea86b8cb2d565bed6344e923d82a053e8ba9355da15cfea8ff"


DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}
LOG_TIME_WINDOW = 432000  # 5 days in seconds
BIT_PONDER_TIME_WINDOW = 7200  # 2 hours (kept for compatibility, not used)
DEFAULT_LIMIT = 5000
MAX_LOOKBACK = 30 * 24 * 3600  # 30 days
API_TIMEOUT = 10  # seconds
EVM_RELAY_CONTAINER = "/staging-evm-relay"
BIT_PONDER_CONTAINER = "/stage-bit-ponder"
COBI_V2_CONTAINER = "/staging-cobi-v2"

# Initialize Gemini client
if not GEMINI_API_KEY:
    logger.error("Missing GEMINI_API_KEY in .env file.")
    raise ValueError("Missing GEMINI_API_KEY in .env file.")
try:
    genai.configure(api_key=GEMINI_API_KEY)
    logger.info("Gemini client initialized successfully.")
except ValueError as e:
    logger.error(f"Failed to initialize Gemini client: {e}")
    raise ValueError(f"Failed to initialize Gemini client: {e}")

def fetch_db_info(initiator_source_address: str) -> Dict[str, Any]:
    """Fetch the latest create_orders record for the initiator source address."""
    conn = None
    cursor = None
    try:
        logger.info(f"Fetching create_orders for CREATE_ID: {initiator_source_address}")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql_query = """
            SELECT create_id, source_chain, destination_chain, created_at, secret_hash 
            FROM create_orders 
            WHERE create_id = %s
            """
        # cursor.execute(sql_query, (CREATE_ID,))
        # sql_query = """
        #     SELECT create_id, source_chain, destination_chain, created_at 
        #     FROM create_orders 
        #     WHERE initiator_source_address = %s 
        #     AND (source_chain = 'bitcoin_testnet' OR destination_chain = 'bitcoin_testnet') 
        #     ORDER BY created_at ASC 
        #     LIMIT 1
        #     """
        cursor.execute(sql_query, (initiator_source_address,))
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchone()
        if result:
            logger.info(f"Found create_orders record for CREATE_ID: {initiator_source_address}")
            return dict(zip(columns, result))
        logger.warning(f"No create_orders record found for CREATE_ID: {initiator_source_address}")
        return {}
    except psycopg2.Error as e:
        logger.error(f"Database query failed: {e}")
        raise RuntimeError(f"Database query failed: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_matched_order_ids(create_id: str) -> Dict[str, Any]:
    """Fetch source_swap_id and destination_swap_id from matched_orders using create_order_id."""
    conn = None
    cursor = None
    try:
        logger.info(f"Fetching matched_orders for create_id: {create_id}")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql_query = """
            SELECT source_swap_id, destination_swap_id 
            FROM matched_orders 
            WHERE create_order_id = %s
        """
        cursor.execute(sql_query, (create_id,))
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchone()
        if result:
            logger.info(f"Found matched_orders record for create_id: {create_id}")
            return dict(zip(columns, result))
        logger.warning(f"No matched_orders record found for create_id: {create_id}")
        return {}
    except psycopg2.Error as e:
        logger.error(f"Matched orders query failed: {e}")
        raise RuntimeError(f"Matched orders query failed: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_logs(create_id: str, start_time: int, end_time: int, container: str, limit: int = DEFAULT_LIMIT) -> Dict[str, Any]:
    """Fetch logs for the given container."""
    if not API_TOKEN:
        logger.error("Missing API_TOKEN. Ensure .env is configured correctly.")
        raise ValueError("Missing API_TOKEN. Ensure .env is configured correctly.")
    
    if start_time > end_time:
        start_time, end_time = end_time, start_time
    if end_time - start_time > MAX_LOOKBACK:
        start_time = end_time - MAX_LOOKBACK
    
    query = quote_plus(f'{{container="{container}"}}')
    url = f"{BASE_URL}?query={query}&start={start_time}&end={end_time}&limit={limit}"
    
    try:
        logger.info(f"Fetching logs from {url}")
        response = requests.get(url, headers={
            "Authorization": API_TOKEN,
            "Content-Type": "application/json"
        }, timeout=API_TIMEOUT)
        response.raise_for_status()
        logs = response.json()
        log_entries = logs.get("data", {}).get("result", [])
        raw_logs = [msg for entry in log_entries for _, msg in entry.get("values", [])]
        log_result = "\n".join(raw_logs) if raw_logs else "No logs found."
        logger.info(f"Fetched {len(raw_logs)} logs from container: {container}")

        # Only check for order creation in /staging-evm-relay
        is_order_created = False
        if container == EVM_RELAY_CONTAINER:
            prompt = (
                f"Analyze the following logs and determine if the order with create_id '{create_id}' was created. "
                "Return only 'Yes' if the create_id is found in the logs, or 'No' if it is not found.\n\n"
                f"Logs:\n{log_result}"
            )
            try:
                model = genai.GenerativeModel("gemini-1.5-flash")
                gemini_response = model.generate_content(
                    contents=prompt,
                    generation_config=genai.types.GenerationConfig(temperature=0)
                )
                gemini_output = gemini_response.text.strip() if gemini_response.text else "No"
                is_order_created = gemini_output == "Yes"
                logger.info(f"Gemini analysis for create_id '{create_id}' in {container}: {gemini_output}")
            except Exception as e:
                logger.warning(f"Gemini API error: {str(e)}. Falling back to manual check.")
                is_order_created = any(create_id in msg for msg in raw_logs)
                log_result += f"\nGemini API error: {str(e)}. Falling back to manual check."

        return {
            "is_order_created": is_order_created,
            "raw_logs": log_result,
            "raw_log_list": raw_logs
        }
    except RequestException as e:
        logger.error(f"Request failed for container '{container}': {e}")
        raise RuntimeError(f"Request failed for container '{container}': {e}")

def analyze_bit_ponder_logs(
    logs: list,
    source_swap_id: str,
    destination_swap_id: str,
    secret_hash: str,
    create_id: str,
    source_chain: str,
    destination_chain: str,
    container: str
) -> str:
    """Analyze filtered logs using Gemini for a narrative summary and include filtered logs."""
    logger.info(f"Analyzing logs for create_id: {create_id}, container: {container}")
    # Filter logs containing create_id, source_swap_id, destination_swap_id, or secret_hash
    filtered_logs = [
        log for log in logs
        if (create_id and create_id in log) or 
           (source_swap_id and source_swap_id in log) or 
           (destination_swap_id and destination_swap_id in log) or 
           (secret_hash and secret_hash in log)
    ]
    filtered_log_result = "\n".join(filtered_logs) if filtered_logs else "No relevant logs found."
    logger.info(f"Filtered {len(filtered_logs)} logs for create_id: {create_id}")

    # Log the identifiers used for filtering
    logger.info(f"Filtering identifiers: create_id='{create_id}', source_swap_id='{source_swap_id}', "
                f"destination_swap_id='{destination_swap_id}', secret_hash='{secret_hash}'")

    # Gemini prompt for narrative summary
    prompt = (
        f"Thoroughly analyze the following logs related to create_id '{create_id}', which may contain "
        f"create_id '{create_id}', source_swap_id '{source_swap_id}', destination_swap_id '{destination_swap_id}', "
        f"or secret_hash '{secret_hash}'. "
        f"The source chain is '{source_chain}' and the destination chain is '{destination_chain}'. "
        f"The logs are from the '{container}' container. "
        "Provide a detailed narrative summary of the transaction's progress, including any order creation, initiation, redemption, refund, or errors. "
        "Use the following rules to interpret the logs based on the chain and container:\n"
        "- For order creation: Only check for 'order created' in '/staging-evm-relay' logs if source_chain is 'arbitrum_sepolia'. "
        "Look for create_id or secret_hash in these logs to identify order creation events.\n"
        "- If source_chain is 'bitcoin_testnet' and container is '/stage-bit-ponder': 'HTLC initiated' indicates user initiation, "
        "'Redeemed' indicates Cobi redeem. Look for source_swap_id or secret_hash.\n"
        "- If destination_chain is 'bitcoin_testnet' and container is '/stage-bit-ponder': 'HTLC initiated' indicates Cobi initiation, "
        "'Redeemed' indicates user redeem. Look for destination_swap_id or secret_hash.\n"
        "- If source_chain is 'arbitrum_sepolia' and container is '/staging-evm-relay': 'order initiated' indicates user initiation. "
        "Look for create_id, source_swap_id, or secret_hash in these logs.\n"
        "- If destination_chain is 'arbitrum_sepolia' and container is '/staging-evm-relay': 'order redeemed' indicates user redeem. "
        "Look for create_id, destination_swap_id, or secret_hash in these logs.\n"
        "- For '/staging-cobi-v2' logs: Analyze for any transaction-related events (e.g., initiation, redemption, refund, errors) "
        "using create_id, source_swap_id, destination_swap_id, or secret_hash. These logs are not chain-specific.\n"
        "Focus only on the information present in the logs. Do not generate or assume any information not explicitly stated. "
        "If no logs are provided, state that no relevant logs were found and do not proceed with analysis.\n\n"
        f"Logs:\n{filtered_log_result}"
    )

    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        gemini_response = model.generate_content(
            contents=prompt,
            generation_config=genai.types.GenerationConfig(temperature=0)
        )
        gemini_output = gemini_response.text.strip() if gemini_response.text else "No analysis available."
        logger.info(f"Gemini analysis completed for create_id: {create_id}, container: {container}")
        # Combine filtered logs and Gemini analysis
        return (
            f"Filtered Logs:\n{filtered_log_result}\n\n"
            f"Gemini Analysis:\n{gemini_output}"
        )
    except Exception as e:
        logger.error(f"Gemini API error during log analysis for create_id '{create_id}': {str(e)}")
        return (
            f"Filtered Logs:\n{filtered_log_result}\n\n"
            f"Gemini Analysis:\nGemini API error during log analysis: {str(e)}"
        )

def check_matched_order(create_id: str) -> Dict[str, Any]:
    """Check the matched order status for the given create_id."""
    try:
        url = MATCHED_ORDER_URL.format(create_id=create_id)
        logger.info(f"Checking matched order at {url}")
        response = requests.get(url, timeout=API_TIMEOUT)
        response.raise_for_status()
        logger.info(f"Matched order API call successful for create_id: {create_id}")
        return response.json()
    except RequestException as e:
        logger.error(f"Matched order API request failed for create_id '{create_id}': {str(e)}")
        return {"error": f"Matched order API request failed for create_id '{create_id}': {str(e)}"}

def transaction_status(initiator_source_address: str) -> str:
    """Process the transaction status for the given initiator source address."""
    result_str = f"Transaction status for initiator_source_address '{initiator_source_address}':\n"
    create_order_success = False
    create_id = None
    source_chain = None
    destination_chain = None
    unix_timestamp = None
    secret_hash = None
    
    try:
        # Step 1: Fetch database info
        logger.info(f"Starting transaction status check for initiator_source_address: {initiator_source_address}")
        try:
            db_result = fetch_db_info(initiator_source_address)
            if db_result:
                result_str += "Database results from create_orders:\n"
                result_str += "\n".join([f"{k}: {v}" for k, v in db_result.items()]) + "\n"
                create_id = db_result.get("create_id")
                source_chain = db_result.get("source_chain")
                destination_chain = db_result.get("destination_chain")
                secret_hash = db_result.get("secret_hash")
                timestamp_str = db_result.get("created_at")
                if timestamp_str:
                    try:
                        dt = parser.isoparse(str(timestamp_str))
                        unix_timestamp = int(dt.timestamp())
                        logger.info(f"Parsed timestamp: {timestamp_str} -> {unix_timestamp}")
                    except Exception as e:
                        logger.error(f"Failed to parse timestamp '{timestamp_str}': {e}")
                        result_str += f"Failed to parse timestamp '{timestamp_str}': {e}\n"
            else:
                logger.warning(f"No data found for initiator_source_address '{initiator_source_address}' in create_orders.")
                result_str += f"No data found for initiator_source_address '{initiator_source_address}' in create_orders.\n"
                return result_str
        except Exception as e:
            logger.error(f"Database query error: {str(e)}")
            result_str += f"Database query error: {str(e)}\n"
            return result_str
        
        # Step 2: Fetch matched order IDs
        source_swap_id = None
        destination_swap_id = None
        if create_id:
            try:
                matched_order_result = fetch_matched_order_ids(create_id)
                if matched_order_result:
                    source_swap_id = matched_order_result.get("source_swap_id")
                    destination_swap_id = matched_order_result.get("destination_swap_id")
                    result_str += f"\nMatched order IDs for create_id '{create_id}':\n"
                    result_str += f"- Source Swap ID: {source_swap_id or 'Not found'}\n"
                    result_str += f"- Destination Swap ID: {destination_swap_id or 'Not found'}\n"
                else:
                    result_str += f"No matched orders found for create_id '{create_id}'.\n"
            except Exception as e:
                logger.error(f"Matched orders query error: {str(e)}")
                result_str += f"Matched orders query error: {str(e)}\n"
        
        # Step 3: Fetch and analyze logs based on chain type
        if create_id and unix_timestamp:
            start_time = unix_timestamp
            
            # Containers to fetch
            containers_to_fetch = []  
            
            # Add containers based on source_chain
            if source_chain == 'arbitrum_sepolia':
                containers_to_fetch.append(EVM_RELAY_CONTAINER)
            elif source_chain == 'bitcoin_testnet':
                containers_to_fetch.append(BIT_PONDER_CONTAINER)
            elif source_chain == 'starknet_sepolia':
                logger.info("Skipping container fetch for source_chain starknet_sepolia")
            
            # Add containers based on destination_chain
            if destination_chain == 'arbitrum_sepolia':
                containers_to_fetch.append(EVM_RELAY_CONTAINER)
            elif destination_chain == 'bitcoin_testnet':
                containers_to_fetch.append(BIT_PONDER_CONTAINER)
            elif destination_chain == 'starknet_sepolia':
                logger.info("Skipping container fetch for destination_chain starknet_sepolia")
            
            # Remove duplicates while preserving order
            containers_to_fetch = list(dict.fromkeys(containers_to_fetch))
            
            # Always fetch /staging-cobi-v2 last
            containers_to_fetch.append(COBI_V2_CONTAINER)
            
            logger.info(f"Fetching logs from containers: {containers_to_fetch}")
            
            # Fetch and analyze logs for each container
            for container in containers_to_fetch:
                try:
                    # Set end_time based on container
                    if container == COBI_V2_CONTAINER:
                        end_time = int(time.time())  # Use current time for /staging-cobi-v2
                    else:
                        end_time = unix_timestamp + LOG_TIME_WINDOW  # Use 5-day window for others
                    
                    log_result = fetch_logs(create_id, start_time, end_time, container)
                    # Only update create_order_success for /staging-evm-relay
                    if container == EVM_RELAY_CONTAINER:
                        create_order_success = log_result["is_order_created"]
                        if create_order_success:
                            result_str += f"\nOrder created successfully: create_id '{create_id}' found in {container} logs.\n"
                        else:
                            result_str += f"\nOrder not confirmed: create_id '{create_id}' not found in {container} logs.\n"
                    
                    # Print logs (full raw logs for /staging-evm-relay and /stage-bit-ponder, filtered logs for /staging-cobi-v2)
                    if container == COBI_V2_CONTAINER:
                        if source_swap_id or destination_swap_id or secret_hash or create_id:
                            analysis = analyze_bit_ponder_logs(
                                log_result["raw_log_list"], 
                                source_swap_id, 
                                destination_swap_id, 
                                secret_hash,
                                create_id, 
                                source_chain, 
                                destination_chain, 
                                container
                            )
                            result_str += f"\nFiltered logs and analysis from {container} (from {start_time} to {end_time}):\n{analysis}\n"
                        else:
                            result_str += f"\nNo identifiers available for filtering {container} logs.\n"
                    else:
                        # result_str += f"Logs from {container} (from {start_time} to {end_time}):\n{log_result['raw_logs']}\n"
                        if source_swap_id or destination_swap_id or secret_hash or create_id:
                            analysis = analyze_bit_ponder_logs(
                                log_result["raw_log_list"], 
                                source_swap_id, 
                                destination_swap_id, 
                                secret_hash,
                                create_id, 
                                source_chain, 
                                destination_chain, 
                                container
                            )
                            result_str += f"\nAnalysis of filtered {container} logs:\n{analysis}\n"
                except Exception as e:
                    logger.error(f"Error fetching logs from {container}: {str(e)}")
                    result_str += f"\nLogs from {container}: Error fetching logs: {str(e)}\n"
        
        # Step 4: Check matched order
        if create_id :
            try:
                matched_order_result = check_matched_order(create_id)
                result_str += f"\nMatched order API response for create_id '{create_id}':\n{str(matched_order_result)}\n"
                
                is_matched = False
                user_initiated = False
                cobi_initiated = False
                user_redeemed = False
                cobi_redeemed = False
                user_refunded = False
                cobi_refunded = False
                
                if matched_order_result.get("status") == "Ok" and matched_order_result.get("result"):
                    result_data = matched_order_result.get("result", {})
                    if result_data.get("source_swap") or result_data.get("destination_swap"):
                        is_matched = True
                        result_str += f"\nOrder matched successfully for create_id '{create_id}'.\n"
                    
                    # Check user initiation
                    if result_data.get("source_swap"):
                        source_swap = result_data["source_swap"]
                        initiate_tx_hash = source_swap.get("initiate_tx_hash", "")
                        current_confirmations = source_swap.get("current_confirmations", 0)
                        required_confirmations = source_swap.get("required_confirmations", 1)
                        if initiate_tx_hash and current_confirmations >= required_confirmations:
                            user_initiated = True
                            result_str += f"User has initiated the transaction for create_id '{create_id}'.\n"
                    
                    # Check Cobi initiation
                    if result_data.get("destination_swap"):
                        destination_swap = result_data["destination_swap"]
                        initiate_tx_hash = destination_swap.get("initiate_tx_hash", "")
                        current_confirmations = destination_swap.get("current_confirmations", 0)
                        required_confirmations = destination_swap.get("required_confirmations", 1)
                        if initiate_tx_hash and current_confirmations >= required_confirmations:
                            cobi_initiated = True
                            result_str += f"Cobi has initiated the transaction for create_id '{create_id}'.\n"
                    
                    # Check user redeem
                    if result_data.get("source_swap"):
                        source_swap = result_data["source_swap"]
                        redeem_tx_hash = source_swap.get("redeem_tx_hash", "")
                        if redeem_tx_hash:
                            user_redeemed = True
                            result_str += f"User has redeemed the transaction for create_id '{create_id}'.\n"
                    
                    # Check Cobi redeem
                    if result_data.get("destination_swap"):
                        destination_swap = result_data["destination_swap"]
                        redeem_tx_hash = destination_swap.get("redeem_tx_hash", "")
                        if redeem_tx_hash:
                            cobi_redeemed = True
                            result_str += f"Cobi has redeemed the transaction for create_id '{create_id}'.\n"
                    
                    # Check user refund
                    if result_data.get("source_swap"):
                        source_swap = result_data["source_swap"]
                        refund_tx_hash = source_swap.get("refund_tx_hash", "")
                        if refund_tx_hash:
                            user_refunded = True
                            result_str += f"User has been refunded for create_id '{create_id}'.\n"
                    
                    # Check Cobi refund
                    if result_data.get("destination_swap"):
                        destination_swap = result_data["destination_swap"]
                        refund_tx_hash = destination_swap.get("refund_tx_hash", "")
                        if refund_tx_hash:
                            cobi_refunded = True
                            result_str += f"Cobi has been refunded for create_id '{create_id}'.\n"
                
                # Final summary
                result_str += "\nFinal Transaction Status Summary:\n"
                result_str += f"- Source Chain: {source_chain or 'Unknown'}\n"
                result_str += f"- Destination Chain: {destination_chain or 'Unknown'}\n"
                result_str += f"- Source Swap ID: {source_swap_id or 'Not found'}\n"
                result_str += f"- Destination Swap ID: {destination_swap_id or 'Not found'}\n"
                result_str += f"- Secret Hash: {secret_hash or 'Not found'}\n"
                result_str += f"- Order Matched: {'Yes' if is_matched else 'No'}\n"
                result_str += f"- User Initiated: {'Yes' if user_initiated else 'No'}\n"
                result_str += f"- Cobi Initiated: {'Yes' if cobi_initiated else 'No'}\n"
                result_str += f"- User Redeemed: {'Yes' if user_redeemed else 'No'}\n"
                result_str += f"- Cobi Redeemed: {'Yes' if cobi_redeemed else 'No'}\n"
                result_str += f"- User Refunded: {'Yes' if user_refunded else 'No'}\n"
                result_str += f"- Cobi Refunded: {'Yes' if cobi_refunded else 'No'}\n"
            except Exception as e:
                logger.error(f"Error checking matched order for create_id '{create_id}': {str(e)}")
                result_str += f"\nError checking matched order: {str(e)}\n"
        
        logger.info(f"Transaction status check completed for initiator_source_address: {initiator_source_address}")
        return result_str
    
    except Exception as e:
        logger.error(f"Unexpected error in transaction_status: {str(e)}")
        result_str += f"\nUnexpected error: {str(e)}\n"
        return result_str