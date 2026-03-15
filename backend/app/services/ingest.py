# this file is the brain of data pipeline
# acts as a translator, taking raw messy JSON data from a bank export and turns it into structured records in our database
# 1.
# reads the raw bytes from the UploadFile sent via FastAPI -> converts those bytes into a UTF-8 string so Python can read the text ->
# json.loads turns that text into a Python Dictionary or List ->
# since bank files can vary, the code checks if the data is a single object or a list. It specifically looks for a "transactions" key to find the actual data array.

# 2.
# The code starts a for loop to process each transaction one by one.
# Type Check: ensures the item is actually a dictionary (not a random string or empty space).
# Identity Check: looks for the transaction_identification block. If it’s missing, the code assumes this is "junk" metadata (like a file header) and skips it.

# 3. The "Account First" Logic (The Upsert)
# This is critical for your relational database. A Transaction must belong to an Account.
# Upsert: The code tells Prisma: "Look for this account number. If it exists, update its status.
    # If it doesn't exist, create it using the customer details in the JSON."
# This prevents "Foreign Key Errors" where the database would otherwise complain that a transaction
    # is trying to link to an account that doesn't exist yet.

# 4. The Nested Write (The Transaction)
# Instead of running 5 different database commands, the code uses Prisma's Nested Write feature.
# It creates the main Transaction record and, in the same command, creates the linked rows in:
# Amount Table (Money values)
# Status Table (Dispute info, settlement status)
# Channel Table (Merchant names, location)

# 5. The Error Shield (Try/Except)
# If one transaction in a file of 1,000 is corrupted or missing a field (like a KeyError), the try/except block catches it.
# Instead of the whole upload crashing, it prints the error to your terminal and moves to the next transaction.


# ------------------------------------------------------------------------------------------------------------------------------------ #

# Your ingest.py performed a "Relational Dance":
# Validated that the JSON was a bank export.
# Upserted the Account (created it if it was new, ignored it if it already existed).
# Linked the transaction to that account.
# Populated the sub-tables (Amount, Status, Channel) automatically.

# ------------------------------------------------------------------------------------------------------------------------------------ #

import json
from datetime import datetime
from fastapi import UploadFile
from app.core.prisma_db import db  # Import our database connection

async def process_transaction_json(file: UploadFile):
    # read the uploaded file content
    content = await file.read()
    
    # decode the bytes into a string
    string_data = content.decode("utf-8")

    # convert string to python list
    data = json.loads(string_data)

    # We look for the "batch" key at the top level of the JSON.
    # If it's not there, we'll fall back to "UNKNOWN_BATCH".
    global_batch_id = data.get("batch", "UNKNOWN_BATCH")  # Get batch ID from the file, or use a default if not present

    # SAFETY CHECK: If the JSON is just one object, put it in a list
    # ensure data is a list of transactions, even if the user uploaded a single transaction as a JSON object
    # If the JSON is a dictionary and has a "transactions" key, 
    # we want to loop through THAT list instead of the whole file.
    if isinstance(data, dict) and "transactions" in data:
        transactions_list = data["transactions"]
    elif isinstance(data, list):
        transactions_list = data
    else:
        # If it's just a single transaction object
        transactions_list = [data]


    # keep track of how many records we save
    saved_count = 0

    # loop through every transaction in the JSON list
    for item in transactions_list:
        # If 'item' is a string here, the loop is broken. 
        # This check ensures we are looking at a dictionary.
        if not isinstance(item, dict):
            continue
        
        # Check if the required nested key exists before proceeding
        if "transaction_identification" not in item:
            print(f"Skipping: record missing 'transaction_identification'. Keys present: {list(item.keys())}")
            continue

        try:
            # Extracting nested objects for easier mapping
            ident = item["transaction_identification"]
            acc_info = item["account_information"]
            details = item["transaction_details"]
            amt_curr = item["amount_and_currency"]
            status_info = item["transaction_status"]
            channel_info = item["channel_information"]

            # Ensure the Account exists before creating the Transaction (Relational requirement)
            await db.account.upsert(
                where={"account_number": acc_info["account_number"]},
                data={
                    "create": {
                        "account_number": acc_info["account_number"],
                        "customer_id": acc_info["customer_id"],
                        "account_type": acc_info["account_type"],
                        "account_holder_name": acc_info["account_holder_name"],
                        "branch_code": acc_info["branch_code"],
                        "account_status": acc_info["account_status"],
                        "email_id": acc_info.get("email_id"),
                    },
                    "update": {
                        "account_status": acc_info["account_status"]
                    }
                }
            )
                
            # using prisma to create a record in the Transaction table
            # We use nested writes to populate related models (Amount, Status, etc.)
            await db.transaction.create(
                data={
                    # map JSON fields to Database columns
                    "transaction_id": ident["transaction_id"],
                    "reference_number": ident["reference_number"],
                    "account_number": acc_info["account_number"],
                    "batch_id": global_batch_id,
                    
                    "transaction_datetime": datetime.fromisoformat(details["transaction_datetime"]),
                    "transaction_type": details["transaction_type"],
                    "transaction_code": details["transaction_code"],
                    "description": details.get("transaction_description"),
                    "category": details.get("transaction_category"),
                    "priority": details.get("priority"),
                    "mode": details.get("transaction_mode"),
                    "purpose": details.get("transaction_purpose"),

                    "amount": {
                        "create": {
                            "transaction_amount": float(amt_curr["transaction_amount"]),
                            "currency_code": amt_curr["currency_code"],
                            "exchange_rate": float(amt_curr["exchange_rate"]),
                            "converted_amount": float(amt_curr["converted_amount"]),
                            "net_amount": float(amt_curr["net_amount"]),
                        }
                    },
                    "status": {
                        "create": {
                            "status": status_info["status"],
                            "status_code": status_info["status_code"],
                            "settlement_status": status_info.get("settlement_status"),
                        }
                    },
                    "channel": {
                        "create": {
                            "channel_type": channel_info["channel_type"],
                            "merchant_name": channel_info.get("merchant_name"),
                            "location": channel_info.get("location"),
                        }
                    }
                }
            )
            saved_count += 1
        except KeyError as e:
            # This catches the exact missing field and tells you which one it is
            print(f"Error: Missing field in JSON: {e}")
            continue

    # return the total count so the user knows it worked
    return saved_count