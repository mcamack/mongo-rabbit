import asyncio
import consul
import json
import os
import sys
import uvicorn

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

# Get env vars
CONSUL_PORT =        os.getenv('CONSUL_PORT', 8003)
CONSUL_WORKERS =     os.getenv('CONSUL_WORKERS', 2)
CONSUL_TOKEN =       os.getenv('CONSUL_TOKEN')
if not CONSUL_TOKEN:
    sys.exit("Missing CONSUL_TOKEN environment variable")

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Create Consul connection during startup
        app.state.client = consul.Consul(token=CONSUL_TOKEN)
        print("Consul setup complete.")
        yield

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

############################### KV Routes ###############################

@app.get("/kv/value/{item:path}")
async def get_kv_pair(item: str):
    try:
        data = app.state.client.kv.get(item)[1]
        if data and 'Value' in data:
            # Try to parse as JSON
            value_str = data['Value'].decode('utf-8')
            return json.loads(value_str)  # If it's valid JSON, return it as a Python object
    except json.JSONDecodeError:
        if data and 'Value' in data:
            return value_str  # If not valid JSON, return the raw string
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    raise HTTPException(status_code=404, detail=f"Key {item} not found.")

@app.get("/kv/keys/{item:path}")
async def get_keys(item: str):
    try:
        data = app.state.client.kv.get(item, keys=True)[1]
        if data:
            return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    raise HTTPException(status_code=404, detail=f"Key {item} not found.")

@app.put("/kv/{item:path}")
async def get_kv(item: str, body: dict):
    try:
        result = app.state.client.kv.put(item, json.dumps(body))
        if result:
            return True
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=CONSUL_PORT, reload=True, workers=CONSUL_WORKERS)

if __name__ == "__main__":
    asyncio.run(main())
