import os
import sys
import requests
import pandas as pd
import certifi
from pymongo import MongoClient, ReplaceOne
from datetime import datetime

# ==========================================
# CONFIG FROM ENV (JENKINS SAFE)
# ==========================================
URL = os.getenv("TIRE_EXPORT_URL")
PHPSESSID = os.getenv("MENA_SESSION")
MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "atms"
COLLECTION_NAME = "tire"
BATCH_SIZE = 1000


def main():
    if not all([URL, PHPSESSID, MONGO_URI]):
        print("❌ Missing environment variables")
        sys.exit(1)

    print("🚀 Start ETL:", datetime.utcnow())

    # ======================================
    # FETCH
    # ======================================
    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    response = session.get(URL, verify=False)
    response.raise_for_status()

    response.encoding = "utf-8"

    tables = pd.read_html(response.text)
    if not tables:
        print("❌ No table found")
        sys.exit(1)

    df = tables[0]
    print("Rows fetched:", len(df))

    # ======================================
    # CLEAN
    # ======================================
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    df = df.dropna(how="all")
    df["etl_loaded_at"] = datetime.utcnow()

    if "garage_entry_at" in df.columns:
        df["garage_entry_at"] = pd.to_datetime(
            df["garage_entry_at"], errors="coerce"
        )

    # ======================================
    # MONGO CONNECT
    # ======================================
    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME][COLLECTION_NAME]

    collection.create_index(
        [
            ("receipt_no", 1),
            ("truck_no", 1),
            ("garage_entry_at", 1),
        ],
        name="uniq_tire_composite"
    )

    # ======================================
    # BULK UPSERT
    # ======================================
    operations = []
    total_upserts = 0

    for _, row in df.iterrows():
        record = row.to_dict()

        filter_key = {
            "receipt_no": record.get("receipt_no"),
            "truck_no": record.get("truck_no"),
            "garage_entry_at": record.get("garage_entry_at"),
        }

        operations.append(
            ReplaceOne(filter_key, record, upsert=True)
        )

        if len(operations) >= BATCH_SIZE:
            result = collection.bulk_write(operations)
            total_upserts += result.upserted_count
            operations = []

    if operations:
        result = collection.bulk_write(operations)
        total_upserts += result.upserted_count

    print("✅ ETL Completed")
    print("Upserted:", total_upserts)

    client.close()
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ ERROR:", str(e))
        sys.exit(1)