# transform raw data into summary and trends for analytics
# this file will contain functions to query the database and perform analytics on the transaction data
# allows to write complex queries (like joining the transaction and amount tables) in one place
# analytics.py: Takes the "PostgreSQL Rows" -> Turns them into Financial Summaries.

from app.core.prisma_db import db

async def get_batch_summary(batch_id: str):
    # fetch all transactions for this batch including their amounts
    transactions = await db.transaction.find_many(
        where={"batch_id": batch_id},
        include={"amount": True}
    )

    if not transactions:
        return None

    total_records = len(transactions)
    total_volume = 0.0
    categories = {}
    currencies = set()

    for txn in transactions:
        # Sum up the amounts
        amt = txn.amount.transaction_amount if txn.amount else 0
        total_volume += amt

# {
#   "transaction_id": "TXN123",
#   "category": "Salary",
#   "amount": {                  // This is txn.amount
#     "transaction_amount": 401.07,
#     "currency_code": "QAR",
#     "transaction_id": "TXN123"
#   }
# }
        # Track currencies present in the file
        if txn.amount:
            currencies.add(txn.amount.currency_code)

        # Count by category
        cat = txn.category or "Uncategorized"
        categories[cat] = categories.get(cat, 0) + 1

    return {
        "batch_id": batch_id,
        "total_records": total_records,
        "total_transaction_volume": round(total_volume, 2),
        "unique_currencies": list(currencies),
        "category_breakdown": categories,
        # Get the date range of the file
        "start_date": min(t.transaction_datetime for t in transactions),
        "end_date": max(t.transaction_datetime for t in transactions),
    }