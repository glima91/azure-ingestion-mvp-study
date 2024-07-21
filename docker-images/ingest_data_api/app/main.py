import os
import json
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = FastAPI()

# Azure Blob Storage configuration
container_name = os.getenv("AZURE_CONTAINER_NAME")
folder_name = os.getenv("AZURE_FOLDER_NAME")
storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
storage_account_url = f"https://{storage_account_name}.blob.core.windows.net"

if not container_name or not folder_name or not storage_account_name:
    raise ValueError("One or more environment variables are missing")

# Use DefaultAzureCredential for authentication
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(
    account_url=storage_account_url, credential=credential
)


# Pydantic model for input validation
class Item(BaseModel):
    name: str
    value: str


@app.post("/upload")
async def upload_file(item: Item):
    try:
        # Generate a unique filename using UUID
        unique_filename = f"{uuid.uuid4()}.json"
        blob_path = f"{folder_name}/{unique_filename}"

        # Convert the request data to JSON
        item_json = item.json()

        # Get the blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )

        # Upload the JSON data to Azure Blob Storage
        blob_client.upload_blob(item_json, blob_type="BlockBlob")
        return {
            "message": f"Data uploaded successfully with filename {unique_filename} in folder {folder_name}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/read")
async def list_blobs():
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=folder_name + "/")

        # Read all JSON files in the specified folder
        all_data = []
        for blob in blob_list:
            if blob.name.endswith(".json"):
                blob_client = container_client.get_blob_client(blob)
                download_stream = blob_client.download_blob()
                item_json = download_stream.readall().decode("utf-8")
                item_data = json.loads(item_json)
                all_data.append(item_data)

        return all_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
async def main_page():
    return "Hello to Demo App!!! See more info in /docs"


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
