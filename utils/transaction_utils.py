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
    formatted_logs = '\n'.join(logs)
    prompt = (
        f"Analyze the following logs and determine if the order with create_id '{create_id}' was created. "
        "Return only 'Yes' if the create_id is found in the logs, or 'No' if it is not found.\n\n"
        f"Logs:\n{formatted_logs}"
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
) -> dict:
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

    # Use a regular string with .format() to avoid backslash issues in f-string expressions
    prompt = (
        "Thoroughly analyze the following logs related to create_id '{create_id}', which may contain "
        "create_id '{create_id}', source_swap_id '{source_swap_id}', destination_swap_id '{destination_swap_id}', "
        "or secret_hash '{secret_hash}'. "
        "The source chain is '{source_chain}' and the destination chain is '{destination_chain}'. "
        "The logs are from the '{container}' container. "
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
        "Logs:\n{filtered_log_result}"
    ).format(
        create_id=create_id,
        source_swap_id=source_swap_id,
        destination_swap_id=destination_swap_id,
        secret_hash=secret_hash,
        source_chain=source_chain,
        destination_chain=destination_chain,
        container=container,
        filtered_log_result=filtered_log_result
    )

    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        gemini_response = model.generate_content(
            contents=prompt,
            generation_config=genai.types.GenerationConfig(temperature=0)
        )
        gemini_output = gemini_response.text.strip() if gemini_response.text else "No analysis available."
        logger.info(f"Gemini analysis completed for create_id: {create_id}, container: {container}")
        return {
            "filtered_logs": filtered_logs,
            "analysis": gemini_output
        }
    except Exception as e:
        logger.error(f"Gemini API error during log analysis for create_id '{create_id}': {str(e)}")
        return {
            "filtered_logs": filtered_logs,
            "analysis": f"Gemini API error during log analysis: {str(e)}"
        }

def transaction_status(initiator_source_address: str = None, create_id: str = None) -> dict:
    input_identifier = f"create_id '{create_id}'" if create_id else f"initiator_source_address '{initiator_source_address}'"
    result = {
        "database": {},
        "matched_orders": {},
        "logs": {},
        "status": {},
        "errors": []
    }
    
    try:
        logger.info(f"Starting transaction status check for {input_identifier}")
        try:
            db_result = fetch_db_info(initiator_source_address, create_id)
            if db_result:
                result["database"] = db_result
                order_id = db_result.get("create_id")
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
                        result["errors"].append(f"Failed to parse timestamp '{timestamp_str}': {e}")
            else:
                logger.warning(f"No data found for {input_identifier} in create_orders.")
                result["errors"].append(f"No data found for {input_identifier} in create_orders.")
                return result
        except Exception as e:
            logger.error(f"Database query error: {str(e)}")
            result["errors"].append(f"Database query error: {str(e)}")
            return result
        
        source_swap_id = None
        destination_swap_id = None
        if order_id:
            try:
                matched_order_result = fetch_matched_order_ids(order_id)
                if matched_order_result:
                    source_swap_id = matched_order_result.get("source_swap_id")
                    destination_swap_id = matched_order_result.get("destination_swap_id")
                    result["matched_orders"]["ids"] = {
                        "source_swap_id": source_swap_id or "Not found",
                        "destination_swap_id": destination_swap_id or "Not found"
                    }
                else:
                    result["matched_orders"]["ids"] = {"error": f"No matched orders found for create_id '{order_id}'"}
            except Exception as e:
                logger.error(f"Matched orders query error: {str(e)}")
                result["matched_orders"]["ids"] = {"error": f"Matched orders query error: {str(e)}"}
        
        if order_id and unix_timestamp:
            start_time = unix_timestamp
            containers_to_fetch = []  
            
            if source_chain == 'arbitrum_sepolia' or source_chain == 'ethereum_sepolia' or source_chain == 'citrea_testnet':
                containers_to_fetch.append(Config.EVM_RELAY_CONTAINER)
            elif source_chain == 'bitcoin_testnet':
                containers_to_fetch.append(Config.BIT_PONDER_CONTAINER)
            elif source_chain == 'starknet_sepolia':
                containers_to_fetch.append(Config.STARKNET_CONTAINER)
                
            
            if destination_chain == 'arbitrum_sepolia' or destination_chain == 'ethereum_sepolia' or destination_chain == 'citrea_testnet':
                containers_to_fetch.append(Config.EVM_RELAY_CONTAINER)
            elif destination_chain == 'bitcoin_testnet':
                containers_to_fetch.append(Config.BIT_PONDER_CONTAINER)
            elif destination_chain == 'starknet_sepolia':
                containers_to_fetch.append(Config.STARKNET_CONTAINER)
            
            containers_to_fetch = list(dict.fromkeys(containers_to_fetch))
            containers_to_fetch.append(Config.COBI_V2_CONTAINER)
            
            logger.info(f"Fetching logs from containers: {containers_to_fetch}")
            
            for container in containers_to_fetch:
                try:
                    log_result = fetch_logs(order_id, start_time, container)
                    log_key = container.lstrip('/')
                    result["logs"][log_key] = {
                        "raw_logs": log_result["raw_log_list"],
                        "start_time": start_time,
                        "end_time": log_result["end_time"]  # Updated to use end_time from fetch_logs
                    }
                    
                    if container == Config.EVM_RELAY_CONTAINER:
                        create_order_success = analyze_evm_relay_logs(order_id, log_result["raw_log_list"])
                        result["logs"][log_key]["create_order_success"] = create_order_success
                    
                    if source_swap_id or destination_swap_id or secret_hash or order_id:
                        analysis = filter_logs(
                            log_result["raw_log_list"], 
                            source_swap_id, 
                            destination_swap_id, 
                            secret_hash,
                            order_id, 
                            source_chain, 
                            destination_chain, 
                            container
                        )
                        result["logs"][log_key]["filtered_logs"] = analysis["filtered_logs"]
                        result["logs"][log_key]["analysis"] = analysis["analysis"]
                except Exception as e:
                    logger.error(f"Error fetching logs from {container}: {str(e)}")
                    result["logs"][container.lstrip('/')] = {"error": f"Error fetching logs: {str(e)}"}
        
        if order_id:
            try:
                matched_order_result = check_matched_order(order_id)
                result["matched_orders"]["api_response"] = matched_order_result
                
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
                    
                    if result_data.get("source_swap"):
                        source_swap = result_data["source_swap"]
                        initiate_tx_hash = source_swap.get("initiate_tx_hash", "")
                        current_confirmations = source_swap.get("current_confirmations", 0)
                        required_confirmations = source_swap.get("required_confirmations", 1)
                        if initiate_tx_hash and current_confirmations >= required_confirmations:
                            user_initiated = True
                    
                    if result_data.get("destination_swap"):
                        destination_swap = result_data["destination_swap"]
                        initiate_tx_hash = destination_swap.get("initiate_tx_hash", "")
                        current_confirmations = destination_swap.get("current_confirmations", 0)
                        required_confirmations = destination_swap.get("required_confirmations", 1)
                        if initiate_tx_hash and current_confirmations >= required_confirmations:
                            cobi_initiated = True
                    
                    if result_data.get("source_swap"):
                        source_swap = result_data["source_swap"]
                        redeem_tx_hash = source_swap.get("redeem_tx_hash", "")
                        if redeem_tx_hash:
                            user_redeemed = True
                    
                    if result_data.get("destination_swap"):
                        destination_swap = result_data["destination_swap"]
                        redeem_tx_hash = destination_swap.get("redeem_tx_hash", "")
                        if redeem_tx_hash:
                            cobi_redeemed = True
                    
                    if result_data.get("source_swap"):
                        source_swap = result_data["source_swap"]
                        refund_tx_hash = source_swap.get("refund_tx_hash", "")
                        if refund_tx_hash:
                            user_refunded = True
                    
                    if result_data.get("destination_swap"):
                        destination_swap = result_data["destination_swap"]
                        refund_tx_hash = destination_swap.get("refund_tx_hash", "")
                        if refund_tx_hash:
                            cobi_refunded = True
                
                result["status"] = {
                    "source_chain": source_chain or "Unknown",
                    "destination_chain": destination_chain or "Unknown",
                    "source_swap_id": source_swap_id or "Not found",
                    "destination_swap_id": destination_swap_id or "Not found",
                    "secret_hash": secret_hash or "Not found",
                    "is_matched": is_matched,
                    "user_initiated": user_initiated,
                    "cobi_initiated": cobi_initiated,
                    "user_redeemed": user_redeemed,
                    "cobi_redeemed": cobi_redeemed,
                    "user_refunded": user_refunded,
                    "cobi_refunded": cobi_refunded
                }
            except Exception as e:
                logger.error(f"Error checking matched order for create_id (order_id) '{order_id}': {str(e)}")
                result["matched_orders"]["api_response"] = {"error": f"Error checking matched order: {str(e)}"}
        
        logger.info(f"Transaction status check completed for {input_identifier}")
        return result
    
    except Exception as e:
        logger.error(f"Unexpected error in transaction_status: {str(e)}")
        result["errors"].append(f"Unexpected error: {str(e)}")
        return result
