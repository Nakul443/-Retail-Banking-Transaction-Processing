import asyncio
from app.core.prisma_db import db

async def reset():
    await db.connect()
    
    print("Deleting child records...")
    # 1. Delete the child tables first
    await db.amount.delete_many()
    await db.transactionstatus.delete_many()
    await db.channelinformation.delete_many()
    # Add any other relation tables you've populated here (Security, Beneficiary, etc.)

    print("Deleting parent records...")
    # 2. Now it's safe to delete the Transactions
    await db.transaction.delete_many()
    
    # 3. Finally, delete the Accounts
    await db.account.delete_many()
    
    print("✅ Database Cleared Successfully!")
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(reset())