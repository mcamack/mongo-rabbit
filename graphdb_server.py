from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from neo4j import GraphDatabase, AsyncGraphDatabase
from pydantic import BaseModel
import asyncio
import uvicorn
import os

NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
NEO4J_HOST = os.getenv('NEO4J_HOST', 'localhost')  # Default to localhost if not set
NEO4J_PORT = os.getenv('NEO4J_PORT', 7687)  # Default to 27017 if not set
NEO4J_URI = f"bolt://{NEO4J_HOST}:{NEO4J_PORT}"

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Initialize Neo4j driver
        driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        print("NEO4J connection established.")
        
        # Store driver in app.state
        app.state.neo4j_driver = driver

        # Yield control back to FastAPI
        yield

        # Close NEO4J connection on shutdown
        await driver.close()
        print("NEO4J connection closed.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

labels = ["Requirement", "Analysis", 'Drawing']

# Route to query nodes by label
@app.get("/graph/node/{label}")
async def get_nodes_by_label(label: str):
    driver = app.state.neo4j_driver

    try:
        async with driver.session() as session:
            result = await session.run(f"MATCH (n:{label}) RETURN n LIMIT 10")
            nodes = [record["n"] for record in await result.data()]
            return nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Route to create a node
@app.post("/graph/node/{name}")
async def create_node(name: str, properties: dict):
    driver = app.state.neo4j_driver
     
    try:
        async with driver.session() as session:
            # Check if a node with the given properties already exists
            label = "Requirement"
            result = await session.run(f"""
                MERGE (p:{label} {{name: $name}})
                RETURN p.name
                """, {"name": name}
            )

            record = await result.single()
            print(record)
            
            node = record["p.name"]
            return {"message": "Node created", "node": node}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Define a Pydantic model to validate the incoming request body
class GraphRelationship(BaseModel):
    label1: str
    prop1: dict
    label2: str
    prop2: dict
    rel_type: str

async def create_relationship(label1, prop1, label2, prop2, rel_type):
        """
        Creates a relationship between two nodes in the graph.
        
        :param label1: Label of the first node
        :param prop1: Dictionary of properties for the first node (must have at least one property)
        :param label2: Label of the second node
        :param prop2: Dictionary of properties for the second node (must have at least one property)
        :param rel_type: Type of the relationship (string)
        """
        driver = app.state.neo4j_driver

        query = f"""
        MERGE (a:{label1} {{ {', '.join(f'{key}: $prop1.{key}' for key in prop1)} }})
        MERGE (b:{label2} {{ {', '.join(f'{key}: $prop2.{key}' for key in prop2)} }})
        MERGE (a)-[r:{rel_type}]->(b)
        RETURN a, r, b
        """
        print(query)
        try:
            async with driver.session() as session:
                result = await session.run(query, prop1=prop1, prop2=prop2)
                # return [record for record in result]
                return True
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        
@app.post("/graph/relationship/")
async def add_relationship(request: GraphRelationship):
    try:
        result = await create_relationship(
            request.label1, request.prop1, request.label2, request.prop2, request.rel_type
        )

        if result:
            return {"message": "Relationship created successfully", "result": result}
        else:
            raise HTTPException(status_code=400, detail="Failed to create relationship")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=8002, reload=True, workers=1)

if __name__ == "__main__":
    asyncio.run(main())
