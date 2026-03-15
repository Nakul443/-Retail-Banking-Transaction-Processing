# door (endpoint) for ingesting transaction data from a JSON file
# this file defines the API route for uploading transaction data, and calls the service function to process the file.

from fastapi import APIRouter, UploadFile, File
from app.services.ingest import process_transaction_json

# Define the router
router = APIRouter(prefix="/ingest", tags=["Ingestion"])

@router.post("/upload-json")
async def upload_transactions(file: UploadFile = File(...)):
    
    # call the ingestion service to process the uploaded file and save transactions to the database
    count = await process_transaction_json(file)
    
    return {
        "message": "Upload successful",
        "records_processed": count
    }


