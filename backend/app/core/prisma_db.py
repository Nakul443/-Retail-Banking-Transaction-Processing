# this file ensures that we have a single instance of the Prisma client throughout the application,
# and provides functions to connect and disconnect from the database.

from prisma import Prisma

# global database instance
db = Prisma()

async def connect_db():
    await db.connect()

async def disconnect_db():
    if db.is_connected():
        await db.disconnect()