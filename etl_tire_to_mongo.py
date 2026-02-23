import os
import sys
import traceback
import requests
import pandas as pd
import certifi
import time
from pymongo import MongoClient, ReplaceOne
from datetime import datetime
from io import StringIO

URL = os.getenv("TIRE_EXPORT_URL")
PHPSESSID = os.getenv("MENA_SESSION")
MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "atms"
COLLECTION_NAME = "tire"

BATCH_SIZE = 500   # 🔥 small batch to reduce spike
SLEEP_SEC = 0.05   # 🔥 small delay to smooth CPU


def fetch_html():
    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    r = session.get(URL, verify=certifi.where(), timeout=180)
    r.raise_for_status()

    if "login" in r.text.lower():
        raise Exception("Session expired")

    r.encoding = "utf-8"
    return r.text


def parse_table(html):
    df = pd.read_html(StringIO(html), flavor="lxml")[0]

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    df = df.dropna(how="all")
    df["etl_loaded_at"] = datetime.utcnow()

    return df


def upsert_stream(df):

    client = MongoClient(
        MONGO_URI,
        maxPoolSize=20,
        socketTimeoutMS=600000
    )

    col = client[DB_NAME][COLLECTION_NAME]

    total = len(df)
    print("Total rows:", total)

    for start in range(0, total, BATCH_SIZE):

        chunk_df = df.iloc[start:start+BATCH_SIZE]

        ops = []

        for r in chunk_df.to_dict("records"):

            receipt_no = r.get("receipt_no")
            truck_no = r.get("truck_no")
            garage_entry_at = r.get("garage_entry_at")

            if not (receipt_no and truck_no and garage_entry_at):
                continue

            ops.append(
                ReplaceOne(
                    {
                        "receipt_no": receipt_no,
                        "truck_no": truck_no,
                        "garage_entry_at": garage_entry_at,
                    },
                    r,
                    upsert=True
                )
            )

        if ops:
            col.bulk_write(ops, ordered=False)

        print(f"Processed {min(start+BATCH_SIZE, total)} / {total}")

        time.sleep(SLEEP_SEC)   # 🔥 smooth CPU usage

    client.close()


def main():

    if not all([URL, PHPSESSID, MONGO_URI]):
        raise Exception("Missing ENV")

    start_time = datetime.utcnow()
    print("🚀 Start:", start_time)

    html = fetch_html()
    df = parse_table(html)

    if df.empty:
        print("Empty data")
        return

    upsert_stream(df)

    print("✅ Done in", datetime.utcnow() - start_time)


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("❌ FAILED:", str(e))
        traceback.print_exc()
        sys.exit(1)