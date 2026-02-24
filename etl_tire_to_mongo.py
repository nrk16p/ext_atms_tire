import os
import sys
import traceback
import requests
import pandas as pd
import urllib3

from pymongo import MongoClient, InsertOne
from datetime import datetime, timezone
from io import StringIO

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL = os.getenv("TIRE_EXPORT_URL")
PHPSESSID = os.getenv("MENA_SESSION")
MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "atms"
COLLECTION_NAME = "tire_raw"
BATCH_SIZE = 500   # 🔥 lower = smoother CPU


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def main():

    print("🚀 Start ETL:", utcnow())

    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    response = session.get(URL, verify=False)
    response.raise_for_status()

    print("Status:", response.status_code)

    # 🔥 Read with pandas (optimized)
    tables = pd.read_html(
        StringIO(response.text),
        flavor="lxml",
        thousands=None
    )

    if not tables:
        raise Exception("No table found")

    df = tables[0]
    df = df.astype("string")

    print("Rows fetched:", len(df))

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    total_sent = 0

    # 🔥 Process in DataFrame batches
    for start in range(0, len(df), BATCH_SIZE):

        end = start + BATCH_SIZE
        df_batch = df.iloc[start:end]

        records = df_batch.to_dict("records")

        ops = []

        for r in records:
            r["etl_loaded_at"] = utcnow()
            ops.append(InsertOne(r))

        if ops:
            col.bulk_write(ops, ordered=False)
            total_sent += len(ops)

        print(f"Sent batch {start} - {end}")

    client.close()

    print("🔥 Total Sent:", total_sent)
    print("✅ Completed Successfully")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("❌ ETL FAILED:", e)
        traceback.print_exc()
        sys.exit(1)