import time
from dateutil import parser
import google.generativeai as genai
from utils.config import Config
from utils.database import fetch_db_info, fetch_matched_order_ids
from utils.api_client import fetch_logs, check_matched_order
from utils.logging_setup import setup_logging

logger, console = setup_logging()

# Initialize Gemini client
if not Config.GEMINI_API_KEY:
    logger.error("Missing GEMINI_API_KEY in .env file.")
    raise ValueError("Missing GEMINI_API_KEY in .env file.")
try:
    genai.configure(api_key=Config.GEMINI_API_KEY)
    logger.info("Gemini client initialized successfully.")
except ValueError as e:
    logger.error(f"Failed to initialize Gemini client: {e}")
    raise ValueError(f"Failed to initialize Gemini client: {e}")

def analyze_evm_relay_logs(create_id: str, logs: list) -> bool:
    prompt = (
        f"Analyze the following logs and determine if the order with create_id '{create_id}' was created. "
        "Return only 'Yes' if the create_id is found in the logs, or 'No' if it is not found.\n\n"
        f"Logs:\n{'\n'.join(logs)}"
    )
    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        gemini_response = model.generate_content(
            contents=prompt,
            generation_config=genai.types.GenerationConfig(temperature=0)
        )
        gemini_output = gemini_response.text.strip() if gemini_response.text else "No"
        logger.info(f"Gemini analysis for create_id '{create_id}' in {Config.EVM_RELAY_CONTAINER}: {gemini_output}")
        return gemini_output == "Yes"
    except Exception as e:
        logger.warning(f"Gemini API error: {str(e)}. Falling back to manual check.")
        return any(create_id in msg for msg in logs)

def filter_logs(
    logs: list,
    source_swap_id: str,
    destination_swap_id: str,
    secret_hash: str,
    create_id: str,
    source_chain: str,
    destination_chain: str,
    container: str
) -> str:
    logger.info(f"Analyzing logs for create_id: {create_id}, container: {container}")
    filtered_logs = [
        log for log in logs
        if (create_id and create_id in log) or 
           (source_swap_id and source_swap_id in log) or 
           (destination_swap_id and destination_swap_id in log) or 
           (secret_hash and secret_hash in log)
    ]
    filtered_log_result = "\n".join(filtered_logs) if filtered_logs else "No relevant logs found."
    logger.info(f"Filtered {len(filtered_logs)} logs for create_id: {create_id}")

    logger.info(f"Filtering identifiers: create_id='{create_id}', source_swap_id='{source_swap_id}', "
                f"destination_swap_id='{destination_swap_id}', secret_hash='{secret_hash}'")

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

def transaction_status(initiator_source_address: str) -> str:
    result_str = f"Transaction status for initiator_source_address '{initiator_source_address}':\n"
    create_order_success = False
    create_id = None
    source_chain = None
    destination_chain = None
    unix_timestamp = None
    secret_hash = None
    
    try:
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
        
        if create_id and unix_timestamp:
            start_time = unix_timestamp
            containers_to_fetch = []  
            
            if source_chain == 'arbitrum_sepolia':
                containers_to_fetch.append(Config.EVM_RELAY_CONTAINER)
            elif source_chain == 'bitcoin_testnet':
                containers_to_fetch.append(Config.BIT_PONDER_CONTAINER)
            elif source_chain == 'starknet_sepolia':
                logger.info("Skipping container fetch for source_chain starknet_sepolia")
            
            if destination_chain == 'arbitrum_sepolia':
                containers_to_fetch.append(Config.EVM_RELAY_CONTAINER)
            elif destination_chain == 'bitcoin_testnet':
                containers_to_fetch.append(Config.BIT_PONDER_CONTAINER)
            elif destination_chain == 'starknet_sepolia':
                logger.info("Skipping container fetch for destination_chain starknet_sepolia")
            
            containers_to_fetch = list(dict.fromkeys(containers_to_fetch))
            containers_to_fetch.append(Config.COBI_V2_CONTAINER)
            
            logger.info(f"Fetching logs from containers: {containers_to_fetch}")
            
            for container in containers_to_fetch:
                try:
                    if container == Config.COBI_V2_CONTAINER:
                        end_time = int(time.time())
                    else:
                        end_time = unix_timestamp + Config.LOG_TIME_WINDOW
                    
                    log_result = fetch_logs(create_id, start_time, end_time, container)
                    if container == Config.EVM_RELAY_CONTAINER:
                        create_order_success = analyze_evm_relay_logs(create_id, log_result["raw_log_list"])
                        if create_order_success:
                            result_str += f"\nOrder created successfully: create_id '{create_id}' found in {container} logs.\n"
                        else:
                            result_str += f"\nOrder not confirmed: create_id '{create_id}' not found in {container} logs.\n"
                    
                    if container == Config.COBI_V2_CONTAINER:
                        if source_swap_id or destination_swap_id or secret_hash or create_id:
                            analysis = filter_logs(
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
                        if source_swap_id or destination_swap_id or secret_hash or create_id:
                            analysis = filter_logs(
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
        
        if create_id:
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
                    
                    if result_data.get("source_swap"):
                        source_swap = result_data["source_swap"]
                        initiate_tx_hash = source_swap.get("initiate_tx_hash", "")
                        current_confirmations = source_swap.get("current_confirmations", 0)
                        required_confirmations = source_swap.get("required_confirmations", 1)
                        if initiate_tx_hash and current_confirmations >= required_confirmations:
                            user_initiated = True
                            result_str += f"User has initiated the transaction for create_id '{create_id}'.\n"
                    
                    if result_data.get("destination_swap"):
                        destination_swap = result_data["destination_swap"]
                        initiate_tx_hash = destination_swap.get("initiate_tx_hash", "")
                        current_confirmations = destination_swap.get("current_confirmations", 0)
                        required_confirmations = destination_swap.get("required_confirmations", 1)
                        if initiate_tx_hash and current_confirmations >= required_confirmations:
                            cobi_initiated = True
                            result_str += f"Cobi has initiated the transaction for create_id '{create_id}'.\n"
                    
                    if result_data.get("source_swap"):
                        source_swap = result_data["source_swap"]
                        redeem_tx_hash = source_swap.get("redeem_tx_hash", "")
                        if redeem_tx_hash:
                            user_redeemed = True
                            result_str += f"User has redeemed the transaction for create_id '{create_id}'.\n"
                    
                    if result_data.get("destination_swap"):
                        destination_swap = result_data["destination_swap"]
                        redeem_tx_hash = destination_swap.get("redeem_tx_hash", "")
                        if redeem_tx_hash:
                            cobi_redeemed = True
                            result_str += f"Cobi has redeemed the transaction for create_id '{create_id}'.\n"
                    
                    if result_data.get("source_swap"):
                        source_swap = result_data["source_swap"]
                        refund_tx_hash = source_swap.get("refund_tx_hash", "")
                        if refund_tx_hash:
                            user_refunded = True
                            result_str += f"User has been refunded for create_id '{create_id}'.\n"
                    
                    if result_data.get("destination_swap"):
                        destination_swap = result_data["destination_swap"]
                        refund_tx_hash = destination_swap.get("refund_tx_hash", "")
                        if refund_tx_hash:
                            cobi_refunded = True
                            result_str += f"Cobi has been refunded for create_id '{create_id}'.\n"
                
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