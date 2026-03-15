# endpoint for analytics related to transaction batches
# this file defines the API route for fetching analytics summaries for a given batch of transactions, and
# only for analytics routing

from fastapi import APIRouter, HTTPException
from app.services.analytics import get_batch_summary

router = APIRouter(prefix="/analytics", tags=["Analytics"])

@router.get("/summary/{batch_id}")
async def read_batch_summary(batch_id: str):
    summary = await get_batch_summary(batch_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Batch not found")
    return summary