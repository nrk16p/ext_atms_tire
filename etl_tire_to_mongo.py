import os
import sys
import time
import traceback
import requests
import pandas as pd
import urllib3

from pymongo import MongoClient, ReplaceOne
from datetime import datetime, timezone
from io import StringIO

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL = os.getenv("TIRE_EXPORT_URL")
PHPSESSID = os.getenv("MENA_SESSION")
MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "atms"
COLLECTION_NAME = "tire"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
REQUEST_TIMEOUT = 60


def utcnow():
    return datetime.now(timezone.utc)


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )
    return df


def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    colmap = {
        "ยานพาหนะ": "vehicle",
        "แจ้งซ่อม_/_ขอเปลี่ยนยาง": "receipt_no",
        "เปลี่ยนเข้า": "garage_entry_at",
        "เปลี่ยนออก": "garage_exit_at",
        "แก้ไขเมื่อ": "updated_at",
    }

    rename_dict = {c: colmap[c] for c in df.columns if c in colmap}
    df = df.rename(columns=rename_dict)

    if "truck_no" not in df.columns:
        df["truck_no"] = df.get("vehicle")

    return df


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["garage_entry_at", "garage_exit_at", "updated_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
    return df


def fetch_html():
    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    r = session.get(URL, verify=False, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


def extract_first_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html), flavor="lxml")
    if not tables:
        raise Exception("No table found")
    return tables[0].dropna(how="all")


def upsert_mongo(df: pd.DataFrame):

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    col.create_index(
        [("receipt_no", 1), ("truck_no", 1), ("garage_entry_at", 1)],
        name="uniq_tire_composite",
        background=True,
    )

    ops = []
    start = time.time()

    for _, row in df.iterrows():

        record = {}

        for k, v in row.items():

            # 🔥 GUARANTEED NaT / NaN FIX
            if pd.isna(v):
                record[k] = None
                continue

            if isinstance(v, pd.Timestamp):
                record[k] = v.to_pydatetime()
                continue

            record[k] = v

        # key validation
        if not record.get("receipt_no") or not record.get("truck_no") or not record.get("garage_entry_at"):
            continue

        record["etl_loaded_at"] = utcnow()

        ops.append(
            ReplaceOne(
                {
                    "receipt_no": record["receipt_no"],
                    "truck_no": record["truck_no"],
                    "garage_entry_at": record["garage_entry_at"],
                },
                record,
                upsert=True,
            )
        )

        if len(ops) >= BATCH_SIZE:
            col.bulk_write(ops, ordered=False)
            ops = []

    if ops:
        col.bulk_write(ops, ordered=False)

    client.close()

    print(f"✅ Completed in {time.time()-start:.2f}s")


def main():

    print("🚀 Start ETL:", utcnow().isoformat())

    html = fetch_html()
    df = extract_first_table(html)

    print("Rows fetched:", len(df))

    df = normalize_cols(df)
    df = map_columns(df)
    df = parse_dates(df)

    # remove rows with invalid key
    df = df[df["garage_entry_at"].notna()]

    df = df.drop_duplicates(
        subset=["receipt_no", "truck_no", "garage_entry_at"],
        keep="last",
    )

    upsert_mongo(df)


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("❌ ETL FAILED:", str(e))
        traceback.print_exc()
        sys.exit(1)