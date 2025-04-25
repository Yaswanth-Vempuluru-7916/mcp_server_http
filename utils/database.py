import psycopg2
from utils.config import Config
from utils.logging_setup import setup_logging

logger, console = setup_logging()

def fetch_db_info(initiator_source_address: str) -> dict:
    conn = None
    cursor = None
    try:
        logger.info(f"Fetching create_orders for CREATE_ID: {initiator_source_address}")
        conn = psycopg2.connect(**Config.DB_CONFIG)
        cursor = conn.cursor()
        sql_query = """
            SELECT create_id, source_chain, destination_chain, created_at, secret_hash 
            FROM create_orders 
            WHERE create_id = %s
        """
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

def fetch_matched_order_ids(create_id: str) -> dict:
    conn = None
    cursor = None
    try:
        logger.info(f"Fetching matched_orders for create_id: {create_id}")
        conn = psycopg2.connect(**Config.DB_CONFIG)
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