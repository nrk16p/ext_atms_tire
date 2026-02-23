import os
import sys
import traceback
import requests
import pandas as pd
import certifi
from pymongo import MongoClient, UpdateOne
from datetime import datetime
from io import StringIO

# ==========================================
# CONFIG FROM ENV (JENKINS SAFE)
# ==========================================
URL = os.getenv("TIRE_EXPORT_URL")
PHPSESSID = os.getenv("MENA_SESSION")
MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "atms"
COLLECTION_NAME = "tire"
BATCH_SIZE = 1000


# ==========================================
# FETCH HTML
# ==========================================
def fetch_html():
    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    response = session.get(
        URL,
        verify=False,
        timeout=60
    )
    response.raise_for_status()

    # detect login redirect
    if "login" in response.text.lower():
        raise Exception("Session expired or redirected to login page")

    response.encoding = "utf-8"
    return response.text


# ==========================================
# PARSE TABLE
# ==========================================
def parse_table(html):
    tables = pd.read_html(StringIO(html), flavor="lxml")

    if not tables:
        raise Exception("No table found in HTML")

    df = tables[0]

    # normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    df = df.dropna(how="all")

    df["etl_loaded_at"] = datetime.utcnow()

    # convert date column safely
    if "เปลี่ยนเข้า" in df.columns:
        df["เปลี่ยนเข้า"] = pd.to_datetime(
            df["เปลี่ยนเข้า"], errors="coerce"
        )

    return df


# ==========================================
# UPSERT TO MONGO
# ==========================================
def upsert_mongo(df):
    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME][COLLECTION_NAME]

    # Composite index
    collection.create_index(
        [
            ("receipt_no", 1),
            ("truck_no", 1),
            ("garage_entry_at", 1),
        ],
        name="uniq_tire_composite"
    )

    operations = []
    total_modified = 0
    total_upserted = 0

    for _, row in df.iterrows():
        record = row.to_dict()

        # composite key validation
        receipt_no = record.get("receipt_no")
        truck_no = record.get("truck_no")
        garage_entry_at = record.get("garage_entry_at")

        if not all([receipt_no, truck_no, garage_entry_at]):
            continue

        operations.append(
            UpdateOne(
                {
                    "receipt_no": receipt_no,
                    "truck_no": truck_no,
                    "garage_entry_at": garage_entry_at,
                },
                {"$set": record},
                upsert=True
            )
        )

        if len(operations) >= BATCH_SIZE:
            result = collection.bulk_write(
                operations,
                ordered=False
            )
            total_modified += result.modified_count
            total_upserted += result.upserted_count
            operations = []

    if operations:
        result = collection.bulk_write(
            operations,
            ordered=False
        )
        total_modified += result.modified_count
        total_upserted += result.upserted_count

    client.close()

    print("Modified:", total_modified)
    print("Upserted:", total_upserted)


# ==========================================
# MAIN
# ==========================================
def main():
    if not all([URL, PHPSESSID, MONGO_URI]):
        raise Exception("Missing environment variables")

    print("🚀 Start ETL:", datetime.utcnow())

    html = fetch_html()
    df = parse_table(html)

    print("Rows fetched:", len(df))

    if df.empty:
        print("⚠️ DataFrame empty. Exit safely.")
        return

    upsert_mongo(df)

    print("✅ ETL Completed Successfully")


# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("❌ ETL FAILED:", str(e))
        traceback.print_exc()
        sys.exit(1)