from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from neo4j import AsyncGraphDatabase
from pydantic import BaseModel, field_validator, model_validator
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
ALLOWED_LABELS_AND_TYPES = {
    "Requirement": {
        "allowed_properties": [],
        "allowed_relationships": {
            "Requirement": ["child", "parent"],
            "Requirement Specification": ["part_of"]
        }
    },
    "Requirement_Specification": {
        "allowed_properties": [],
        "allowed_relationships": {
            "Requirement Specification": ["child", "parent"],
            "Requirement": ["contains"]
        }
    },
}

ALLOWED_LABELS = list(ALLOWED_LABELS_AND_TYPES.keys())


######################################################################################
# Rules Routes
######################################################################################

@app.get("/graph/rules")
async def get_rules():
    return ALLOWED_LABELS_AND_TYPES

######################################################################################
# Node Routes
######################################################################################

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

@app.get("/graph/nodes/{label}")
async def get_nodes_by_label(label: str):
    if label not in ALLOWED_LABELS:
        raise HTTPException(status_code=400, detail=f"Label must be from the list: {ALLOWED_LABELS}")

    driver = app.state.neo4j_driver
    async with driver.session() as session:
        try:
            query = "MATCH (n:{label}) RETURN n".format(label=label)
            result = await session.run(query)
            nodes = []
            async for record in result:
                nodes.append(record["n"]._properties)
            
            if not nodes:
                raise HTTPException(status_code=404, detail="No nodes found with the specified label")
            
            return nodes
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


######################################################################################
# Relationship Routes
######################################################################################

class Relationship(BaseModel):
    source_node_label: str
    source_node_name: str
    target_node_label: str
    target_node_name: str
    relationship_type: str
    properties: Optional[Dict] = {}

    # @field_validator('source_node_label', 'target_node_label')
    # def validate_labels(cls, label):
    #     if label not in ALLOWED_LABELS:
    #         raise ValueError(f"Label '{label}' is not allowed. Allowed labels: {ALLOWED_LABELS}")
    #     return label

    @model_validator(mode="after")
    def validate_relationship_type(cls, values):
        source_node_label = values.source_node_label
        relationship_type = values.relationship_type
        target_node_label = values.target_node_label
        target_node_name = values.target_node_name
        relationship_type = values.relationship_type
        
        # Check if the labels are allowed
        if source_node_label not in ALLOWED_LABELS:
            raise ValueError(f"Label '{source_node_label}' is not allowed. Allowed labels: {ALLOWED_LABELS}")
        if target_node_label not in ALLOWED_LABELS:
            raise ValueError(f"Label '{target_node_label}' is not allowed. Allowed labels: {ALLOWED_LABELS}")
        
        # Check if the relationship Type is allowed based on the source and target Node Labels
        if source_node_label in ALLOWED_LABELS_AND_TYPES:
            ALLOWED_RELATIONSHIPS_BY_LABEL = ALLOWED_LABELS_AND_TYPES[source_node_label]["allowed_relationships"]

            if target_node_label not in ALLOWED_RELATIONSHIPS_BY_LABEL:
                raise ValueError(f"Relationship types cannot be found for Node Label: {target_node_label}")
            
            ALLOWED_RELATIONSHIPS = ALLOWED_RELATIONSHIPS_BY_LABEL[target_node_label]

            if relationship_type not in ALLOWED_RELATIONSHIPS:
                raise ValueError(f"Relationship type '{relationship_type}' is not allowed. Allowed types: {ALLOWED_RELATIONSHIPS}")
        else:
            raise ValueError(f"Relationship types cannot be found for Node Label: {source_node_label}")
        
        return values


# CREATE relationship
@app.post("/graph/relationship/")
async def create_relationship(relationship: Relationship):
    driver = app.state.neo4j_driver
    async with driver.session() as session:
        # Check if both nodes exist
        check_query = f"""
        MATCH (source:{relationship.source_node_label} {{name: $source_name}})
        MATCH (target:{relationship.target_node_label} {{name: $target_name}})
        RETURN source, target
        """
        result = await session.run(check_query, {
            "source_name": relationship.source_node_name, 
            "target_name": relationship.target_node_name
        })
        
        if await result.single() is None:
            raise HTTPException(status_code=404, detail="One or both nodes not found.")
        
        # Create the relationship
        create_query = f"""
        MATCH (source:{relationship.source_node_label} {{name: $source_name}})
        MATCH (target:{relationship.target_node_label} {{name: $target_name}})
        MERGE (source)-[r:{relationship.relationship_type}]->(target)
        SET r += $properties
        RETURN r
        """
        await session.run(create_query, {
            "source_name": relationship.source_node_name,
            "target_name": relationship.target_node_name,
            "properties": relationship.properties
        })
        
    return {"message": "Relationship created successfully"}

# READ relationships for a node
@app.get("/graph/relationship/{label}/{name}")
async def get_node_relationships(label: str, name: str):
    if label not in ALLOWED_LABELS:
        raise HTTPException(status_code=400, detail=f"Label must be from the list: {ALLOWED_LABELS}")

    driver = app.state.neo4j_driver
    async with driver.session() as session:
        # Get all relationships from and to a node
        query = f"""
        MATCH (source:{label} {{name: $name}})-[r]->(target)
        RETURN r, type(r) AS relationship_type, labels(target) AS target_labels, target.name AS target_name, NULL AS labels, NULL AS source_name
        UNION
        MATCH (source)-[r]->(target:{label} {{name: $name}})
        RETURN r, type(r) AS relationship_type, NULL AS target_labels, NULL AS target_name, labels(source) AS labels, source.name AS source_name
        """
        result = await session.run(query, {
            "name": name
        })
        relationships = []
        async for record in result:
            if record["target_name"]:
                relationships.append({
                    "properties": record["r"]._properties,
                    "type": record["relationship_type"],
                    "target": {
                        "name": record["target_name"],
                        "labels": record["target_labels"]
                    }
                })
            elif record["source_name"]:
                relationships.append({
                    "properties": record["r"]._properties,
                    "type": record["relationship_type"],
                    "source": {
                        "name": record["source_name"],
                        "labels": record["labels"]
                    }
                })
        
        if not relationships:
            raise HTTPException(status_code=404, detail="No relationships found for the specified node.")
        
        return relationships



async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=8002, reload=True, workers=1)

if __name__ == "__main__":
    asyncio.run(main())
