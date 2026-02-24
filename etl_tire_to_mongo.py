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
BATCH_SIZE = 1000

HEADERS = {"User-Agent": "Mozilla/5.0"}


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def main():

    print("🚀 Start ETL:", utcnow())

    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    response = session.get(URL, headers=HEADERS, verify=False)
    response.raise_for_status()

    print("Status:", response.status_code)
    print("Content-Type:", response.headers.get("Content-Type"))

    # 🔥 IMPORTANT FIX
    tables = pd.read_html(StringIO(response.text))

    print("Tables found:", len(tables))

    if not tables:
        raise Exception("No table found")

    df = tables[0]

    print("Rows fetched:", len(df))

    # convert everything to string
    df = df.astype(str)

    records = df.to_dict("records")

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    ops = []
    total = 0

    for r in records:
        r["etl_loaded_at"] = utcnow()
        ops.append(InsertOne(r))
        total += 1

        if len(ops) >= BATCH_SIZE:
            col.bulk_write(ops, ordered=False)
            ops = []

    if ops:
        col.bulk_write(ops, ordered=False)

    client.close()

    print("🔥 Sent to Mongo:", total)
    print("✅ Completed Successfully")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("❌ ETL FAILED:", e)
        traceback.print_exc()
        sys.exit(1)