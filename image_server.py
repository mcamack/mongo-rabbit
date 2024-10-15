import asyncio
import io
import os
import uvicorn

from bson import ObjectId
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from gridfs.errors import NoFile
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket
from pymongo.database import Database

# Get env vars
IMAGE_PORT =                os.getenv('IMAGE_PORT', 8000)
IMAGE_WORKERS =             os.getenv('IMAGE_WORKERS', 1)

MONGODB_USER =              os.getenv('MONGODB_USER')
MONGODB_PASSWORD =          os.getenv('MONGODB_PASSWORD')
MONGODB_HOST =              os.getenv('MONGODB_HOST', 'localhost')  # Default to localhost if not set
MONGODB_PORT =              os.getenv('MONGODB_PORT', 27017)  # Default to 27017 if not set

MONGODB_DATABASE =          os.getenv('MONGODB_DATABASE', 'images')

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # MongoDB Connection
        app.state.mongo_client = AsyncIOMotorClient(
            # f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin&ssl=true",
            f"mongodb://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}",
            # read_preference='secondaryPreferred',
            # write_concern={'w': 'majority'},
            tls=True,
            # tlsCAFile='./generated-cert.pem',  # Path to the CA certificate
            # tlsCAFile='/tmp/mongotest/mongodb.pem',  # Path to the CA certificate
            tlsCAFile='/tmp/mongotest2/ca.crt',  # Path to the CA certificate
            # tlsCertificateKeyFile='./generated-key2.pem',  # Path to the client certificate (optional)
            # tlsCertificateKeyFile='/tmp/mongotest/mongodb-client.pem',  # Path to the client certificate (optional)
            tlsCertificateKeyFile='client.pem',  # Path to the client certificate (optional)
            tlsAllowInvalidCertificates=False,  # Enforce strict certificate validation   
            tlsAllowInvalidHostnames=True,         
            maxPoolSize=10,  # Max connections in the pool
            minPoolSize=5   # Min connections in the pool
        )        

        # Store mongo stuff in app.state
        app.state.db: Database = app.state.mongo_client[MONGODB_DATABASE]
        app.state.fs = AsyncIOMotorGridFSBucket(app.state.db)  # GridFS bucket for storing files

        # Yield control back to FastAPI 
        yield

        # Close MongoDB connection during shutdown
        app.state.mongo_client.close()
        print("MongoDB connection closed.")
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

############################### Image Routes ###############################

@app.get("/image/{file_id}")
async def get_image(file_id: str):
    try:
        # Convert the file_id string to ObjectId
        object_id = ObjectId(file_id)


        # Retrieve the image from GridFS by its file_id
        file_data = await app.state.fs.open_download_stream(object_id)

        if file_data:
            # Create a streaming response with the file data
            return StreamingResponse(
                io.BytesIO(await file_data.read()),
                media_type=file_data.metadata.get('content_type', 'application/octet-stream')
            )        
    except NoFile as e:
        raise HTTPException(status_code=404, detail="Image not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve image: {str(e)}")
    

@app.post("/image/")
async def upload_image(file: UploadFile = File(...)):
    try:
        # Read the file content
        file_contents = await file.read()

        # Check if the file is an image
        if file.content_type.startswith("image/"):
            # Store the image in GridFS asynchronously
            image_stream = io.BytesIO(file_contents)
            file_id = await app.state.fs.upload_from_stream(
                file.filename,
                image_stream,
                metadata={"content_type": file.content_type}
            )

            # Return the ID of the file stored in GridFS
            return {"message": "Image uploaded successfully", "file_id": str(file_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload image: {str(e)}")

    raise HTTPException(status_code=400, detail="Invalid file type. Please upload an image.")
    
@app.delete("/image/{file_id}")
async def delete_image(file_id: str):
    try:
        # Convert the file_id string to ObjectId
        object_id = ObjectId(file_id)

        # Check if the file exists in GridFS
        file_exists = await app.state.db.fs.files.find_one({"_id": object_id})

        if file_exists:
            # Delete the file from GridFS
            await app.state.fs.delete(object_id)
            return {"message": f"Image with ID {file_id} has been deleted successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete image: {str(e)}")

    raise HTTPException(status_code=404, detail="Image not found")

async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=IMAGE_PORT, reload=True, workers=IMAGE_WORKERS)

if __name__ == "__main__":
    asyncio.run(main())
