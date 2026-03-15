# entry point for FAST API application
# heart of the API
# initializes FastAPI and handles connection lifecycle
from app.services.analytics import get_batch_summary  # Import your new logic
from fastapi import HTTPException

from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.prisma_db import connect_db, disconnect_db
from app.api.v1 import ingest_router  # Import the router for transaction ingestion
from app.api.v1.analytics_router import router as analytics_router # Import new router for analytics endpoints

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to the database
    await connect_db()
    yield
    # Shutdown: Clean up connection
    await disconnect_db()

app = FastAPI(title="Retail Banking Transaction Processor", lifespan=lifespan)

app.include_router(ingest_router.router)  # Include the ingest router for transaction data upload

@app.get("/")
def read_root():
    return {"status": "Backend is running", "database": "Connected"}

app.include_router(analytics_router) # Include the analytics router to expose the analytics endpoint