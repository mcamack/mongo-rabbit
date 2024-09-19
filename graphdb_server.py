from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from neo4j import GraphDatabase, AsyncGraphDatabase
import asyncio
import uvicorn
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # neo4j Connection
        neo4j_user = os.getenv('NEO4J_USER')
        neo4j_password = os.getenv('NEO4J_PASSWORD')
        neo4j_host = os.getenv('NEO4J_HOST', 'localhost')  # Default to localhost if not set
        neo4j_port = os.getenv('NEO4J_PORT', 7687)  # Default to 27017 if not set
        neo4j_uri = f"bolt://{neo4j_host}:{neo4j_port}"

        # Initialize Neo4j driver
        driver = AsyncGraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        print("NEO4J connection established.")
        
        # Store connection in app.state
        app.state.neo4j_driver = driver

        yield

        # # Yield control back to FastAPI with a RabbitMQ channel open for use
        # async with rabbitmq_connection.channel() as channel:
        #     app.state.rabbitmq_channel = channel
        #     yield

        # # Close RabbitMQ connection during shutdown
        # await rabbitmq_connection.close()
        # print("RabbitMQ connection closed.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await driver.close()
        print("Closed neo4j connection.")

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

# Route to query nodes by label
@app.get("/nodes/{label}")
async def get_nodes_by_label(label: str):
    driver = app.state.neo4j_driver

    try:
        async with driver.session() as session:
            result = await session.run(f"MATCH (n:{label}) RETURN n LIMIT 10")
            nodes = [record["n"] for record in await result.data()]
            return nodes
            # nodes = [record["n"] for record in result]
            # return {"nodes": [dict(node) for node in nodes]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Route to create a node
@app.post("/nodes/{label}")
async def create_node(label: str, properties: dict):
    driver = app.state.neo4j_driver
     
    try:
        async with driver.session() as session:
            # Check if a node with the given properties already exists
            result = await session.run(
                f"""
                MATCH (n:{label})
                RETURN n
                """, 
            )
            existing_node = await result.single()
            
            # If node already exists, return it
            if existing_node:
                node = existing_node["n"]
                return {"message": "Node already exists", "node": dict(node)}
            
            # If node does not exist, create it
            result = await session.run(
                f"CREATE (n:{label} $properties) RETURN n", 
                properties=properties
            )
            record = await result.single()
            node = record["n"]
            return {"message": "Node created", "node": dict(node)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=8002, reload=True, workers=2)

if __name__ == "__main__":
    asyncio.run(main())
