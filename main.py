from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from utils.transaction_utils import transaction_status
import uvicorn
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI(title="Transaction Status Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Adjust for your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class ToolRequest(BaseModel):
    arguments: dict

@app.get("/tools")
async def list_tools():
    return {
        "tools": [
            {
                "name": "check_transaction_status",
                "description": "Check the status of a transaction by initiator source address or create_id.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "initiator_source_address": {"type": "string"},
                        "create_id": {"type": "string"}
                    },
                    "required": []  # No required fields, either one must be provided
                }
            }
        ]
    }

@app.post("/tools/check_transaction_status")
async def call_check_transaction_status(request: ToolRequest):
    try:
        initiator_source_address = request.arguments.get("initiator_source_address")
        create_id = request.arguments.get("create_id")
        if not initiator_source_address and not create_id:
            raise HTTPException(status_code=400, detail="Either initiator_source_address or create_id must be provided")
        if initiator_source_address and create_id:
            raise HTTPException(status_code=400, detail="Provide only one of initiator_source_address or create_id")
        
        result = transaction_status(initiator_source_address, create_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 10000))
    print(f"Starting MCP server with HTTP transport on {host}:{port}...")
    uvicorn.run(app, host=host, port=port)