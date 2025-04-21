# server_http.py
# Alternative MCP server using FastAPI for HTTP transport
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from utils.transaction_utils import transaction_status
import uvicorn
import os
from dotenv import load_dotenv

load_dotenv()  # Loads .env file for local development

app = FastAPI(title="Transaction Status Server")

class ToolRequest(BaseModel):
    arguments: dict

@app.get("/tools")
async def list_tools():
    return {
        "tools": [
            {
                "name": "check_transaction_status",
                "description": "Check the status of a transaction by initiator source address.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "initiator_source_address": {"type": "string"}
                    },
                    "required": ["initiator_source_address"]
                }
            }
        ]
    }

@app.post("/tools/check_transaction_status")
async def call_check_transaction_status(request: ToolRequest):
    try:
        initiator_source_address = request.arguments.get("initiator_source_address")
        if not initiator_source_address:
            raise HTTPException(status_code=400, detail="Missing initiator_source_address")
        result = transaction_status(initiator_source_address)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    print(f"Starting MCP server with HTTP transport on {host}:{port}...")
    uvicorn.run(app, host=host, port=port)
