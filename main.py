# from fastapi import FastAPI, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel
# from typing import Optional
# from utils.transaction_utils import transaction_status
# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["http://localhost:5173"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# class TransactionStatusRequest(BaseModel):
#     arguments: dict

# @app.post("/tools/check_transaction_status")
# async def check_transaction_status(request: TransactionStatusRequest):
#     try:
#         arguments = request.arguments
#         if not arguments:
#             raise HTTPException(status_code=400, detail="Invalid payload. Expected 'arguments' key.")
        
#         initiator_source_address = arguments.get("initiator_source_address")
#         create_id = arguments.get("create_id")
        
#         if not initiator_source_address and not create_id:
#             raise HTTPException(status_code=400, detail="Either initiator_source_address or create_id must be provided")
        
#         result = transaction_status(initiator_source_address, create_id)
#         logger.info(f"Response sent: {result}")
        
#         # Convert the string result into the expected dictionary structure
#         response = {
#             "identifier": f"{'initiator_source_address' if initiator_source_address else 'create_id'} '{initiator_source_address or create_id}'",
#             "database_results": {},  # Could parse result_str for database data if needed
#             "matched_order_ids": {},  # Could parse for swap IDs if needed
#             "logs": {},  # Could parse for log analysis if needed
#             "matched_order_api": {},  # Could parse for API response if needed
#             "status_summary": {},  # Could parse for summary if needed
#             "errors": [],
#             "status_text": result  # Store the original string for display
#         }
#         return response
#     except Exception as e:
#         logger.error(f"Error in check_transaction_status: {str(e)}", exc_info=True)
#         error_response = {
#             "identifier": "",
#             "database_results": {},
#             "matched_order_ids": {},
#             "logs": {},
#             "matched_order_api": {},
#             "status_summary": {},
#             "errors": [f"Server error: {str(e)}"],
#             "status_text": f"Server error: {str(e)}"
#         }
#         return error_response
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from utils.transaction_utils import transaction_status
from utils.logging_setup import setup_logging
import uvicorn
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
logger, console = setup_logging()

origins = os.getenv("ALLOW_ORIGINS", "http://localhost:5173").split(",")
# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Update with your frontend port (e.g., 5173 for Vite)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/transaction_status")
async def get_transaction_status(create_id: str = None, initiator_source_address: str = None):
    try:
        result = transaction_status(initiator_source_address, create_id)
        return result
    except Exception as e:
        logger.error(f"Error processing transaction status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))  # Default to 8000 for local dev
    uvicorn.run(app, host="0.0.0.0", port=port)