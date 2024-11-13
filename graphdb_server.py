from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from neo4j import AsyncGraphDatabase
from pydantic import BaseModel, field_validator
import asyncio
import uvicorn
import os
from typing import List, Dict, Optional

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
        print("Opening NEO4J connection...")
        
        # Store driver in app.state
        app.state.neo4j_driver = driver

        # Test Connection
        async with driver.session() as session:
            # Run a simple query
            result = session.run("RETURN 'Connection successful!' AS message")
            # Fetch the result
            async for record in await result:
                message = record["message"]
                print(message)  # Should print "Connection successful" if connection works
                break  # We only need the first result

        # Yield control back to FastAPI
        yield

        # Close NEO4J connection on shutdown
        await driver.close()
        print("NEO4J connection closed.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # List the allowed origins, can be "*" for all
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

# Predefined labels for validation
ALLOWED_LABELS = ["Requirement", "Specification", "Component"]
    
@app.get("/graph/node/{label}/{name}")
async def get_node(label: str, name: str):
    if label not in ALLOWED_LABELS:
        raise HTTPException(status_code=400, detail=f"Label must be from the list: {ALLOWED_LABELS}")

    driver = app.state.neo4j_driver
    async with driver.session() as session:
        try:
            query = "MATCH (n:{label} {{name: $name}}) RETURN n".format(label=label)
            result = await session.run(query, {"name": name})
            record = await result.single()
            if not record:
                raise HTTPException(status_code=404, detail="Node not found")
            
            return record["n"]._properties
        except HTTPException as e:
            if e.status_code < 500:
                raise e
            raise HTTPException(status_code=500, detail="Internal server error")
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/graph/node/{label}/{name}")
async def create_node(label: str, name: str, properties: Dict[str, str]):
    if label not in ALLOWED_LABELS:
        raise HTTPException(status_code=400, detail=f"Label must be from the list: {ALLOWED_LABELS}")

    driver = app.state.neo4j_driver
    async with driver.session() as session:
        try:
            query = "MATCH (n:{label} {{name: $name}}) RETURN n LIMIT 1".format(label=label)
            result = await session.run(query, {"name": name})
            if (await result.single()) is not None:
                raise HTTPException(status_code=400, detail="A node with the same label and name already exists.")

            # Create the node
            query = "CREATE (n:{label} {{name: $name}}) SET n += $properties RETURN n".format(label=label)
            await session.run(query, {"name": name, "properties": properties})

            return {"message": "Node created successfully"}
        except HTTPException as e:
            if e.status_code < 500:
                raise e
            raise HTTPException(status_code=500, detail="Internal server error")
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal server error")
    
@app.patch("/graph/node/{label}/{name}")
async def update_node(label: str, name: str, node_update: Dict[str, Optional[str]]):
    if label not in ALLOWED_LABELS:
        raise HTTPException(status_code=400, detail=f"Label must be from the list: {ALLOWED_LABELS}")

    driver = app.state.neo4j_driver
    async with driver.session() as session:
        try:
            # Update the properties of the existing node
            query = "MATCH (n:{label} {{name: $name}}) SET ".format(label=label)
            updates = ", ".join([f"n.{key} = ${key}" for key in node_update.keys()])
            query += updates + " RETURN n"

            params = {"name": name, **node_update}
            result = await session.run(query, params)
            if not (await result.single()):
                raise HTTPException(status_code=404, detail="Node not found or no properties updated")

            return {"message": "Node updated successfully"}
        except HTTPException as e:
            if e.status_code < 500:
                raise e
            raise HTTPException(status_code=500, detail="Internal server error")
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal server error")

@app.delete("/graph/node/{label}/{name}")
async def delete_node(label: str, name: str):
    if label not in ALLOWED_LABELS:
        raise HTTPException(status_code=400, detail=f"Label must be from the list: {ALLOWED_LABELS}")

    driver = app.state.neo4j_driver
    async with driver.session() as session:
        try:
            query = "MATCH (n:{label} {{name: $name}}) DETACH DELETE n".format(label=label)
            result = await session.run(query, {"name": name})
            if (await result.consume()).counters.nodes_deleted == 0:
                raise HTTPException(status_code=404, detail="Node not found")

            return {"message": "Node deleted successfully"}
        except HTTPException as e:
            if e.status_code < 500:
                raise e
            raise HTTPException(status_code=500, detail="Internal server error")
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal server error")






# # Define a Pydantic model to validate the incoming request body
# class GraphRelationship(BaseModel):
#     label1: str
#     prop1: dict
#     label2: str
#     prop2: dict
#     rel_type: str

# async def create_relationship(label1, prop1, label2, prop2, rel_type):
#         """
#         Creates a relationship between two nodes in the graph.
        
#         :param label1: Label of the first node
#         :param prop1: Dictionary of properties for the first node (must have at least one property)
#         :param label2: Label of the second node
#         :param prop2: Dictionary of properties for the second node (must have at least one property)
#         :param rel_type: Type of the relationship (string)
#         """
#         driver = app.state.neo4j_driver

#         query = f"""
#         MERGE (a:{label1} {{ {', '.join(f'{key}: $prop1.{key}' for key in prop1)} }})
#         MERGE (b:{label2} {{ {', '.join(f'{key}: $prop2.{key}' for key in prop2)} }})
#         MERGE (a)-[r:{rel_type}]->(b)
#         RETURN a, r, b
#         """
#         print(query)
#         try:
#             async with driver.session() as session:
#                 result = await session.run(query, prop1=prop1, prop2=prop2)
#                 # return [record for record in result]
#                 return True
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=str(e))
        
# @app.post("/graph/relationship/")
# async def add_relationship(request: GraphRelationship):
#     try:
#         result = await create_relationship(
#             request.label1, request.prop1, request.label2, request.prop2, request.rel_type
#         )

#         if result:
#             return {"message": "Relationship created successfully", "result": result}
#         else:
#             raise HTTPException(status_code=400, detail="Failed to create relationship")

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=8002, reload=True, workers=1)

if __name__ == "__main__":
    asyncio.run(main())
