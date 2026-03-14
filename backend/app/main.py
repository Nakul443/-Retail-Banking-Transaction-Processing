# entry point for FAST API application
# heart of the API
# initializes FastAPI and handles connection lifecycle

from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.prisma_db import connect_db, disconnect_db

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to the database
    await connect_db()
    yield
    # Shutdown: Clean up connection
    await disconnect_db()

app = FastAPI(title="Retail Banking Transaction Processor", lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "Backend is running", "database": "Connected"}