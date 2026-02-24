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

# Silence HTTPS warning (because verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# CONFIG FROM ENV (JENKINS SAFE)
# ==========================================
URL = os.getenv("TIRE_EXPORT_URL")
PHPSESSID = os.getenv("MENA_SESSION")
MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "atms"
COLLECTION_NAME = "tire"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
REQUEST_TIMEOUT = 60


# ==========================================
# HELPERS
# ==========================================
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
        "ตำแหน่งยาง": "tire_position",
        "สินค้า": "sku_name",
        "serial_no": "serial_no",
        "มม.": "millimeter",
        "เลขไมล์เริ่มต้น": "mile_in",
        "เลขไมล์สิ้นสุด": "mile_out",
        "ล่าสุด": "is_latest",
        "ส่ง_ขาย_/_ซ่อม": "flag",
        "แก้ไขเมื่อ": "updated_at",
    }

    rename_dict = {c: colmap[c] for c in df.columns if c in colmap}
    df = df.rename(columns=rename_dict)

    if "truck_no" not in df.columns:
        df["truck_no"] = df.get("vehicle")

    return df


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["garage_entry_at", "garage_exit_at", "updated_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
    return df


# ==========================================
# FETCH HTML
# ==========================================
def fetch_html():
    if not URL or not PHPSESSID:
        raise Exception("Missing TIRE_EXPORT_URL or MENA_SESSION")

    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)

    r = session.get(URL, verify=False, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

    text = r.text
    low = text.lower()

    if "login" in low or ("password" in low and "phpsessid" in low):
        raise Exception("Session expired / redirected to login page")

    return text


# ==========================================
# PARSE TABLE
# ==========================================
def extract_first_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html), flavor="lxml")
    if not tables:
        raise Exception("No table found in HTML")

    df = tables[0]
    df = df.dropna(how="all")
    return df


# ==========================================
# MONGO UPSERT
# ==========================================
def upsert_mongo(df: pd.DataFrame):

    client = MongoClient(
        MONGO_URI,
        connectTimeoutMS=20000,
        serverSelectionTimeoutMS=20000,
    )

    col = client[DB_NAME][COLLECTION_NAME]

    col.create_index(
        [("receipt_no", 1), ("truck_no", 1), ("garage_entry_at", 1)],
        name="uniq_tire_composite",
        background=True,
    )

    total_rows = len(df)
    sent_ops = 0
    matched = 0
    modified = 0
    upserted = 0

    ops = []
    start = time.time()

    for _, row in df.iterrows():
        record = row.to_dict()
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
            res = col.bulk_write(ops, ordered=False)
            sent_ops += len(ops)
            matched += res.matched_count
            modified += res.modified_count
            upserted += res.upserted_count
            ops = []

    if ops:
        res = col.bulk_write(ops, ordered=False)
        sent_ops += len(ops)
        matched += res.matched_count
        modified += res.modified_count
        upserted += res.upserted_count

    elapsed = time.time() - start
    client.close()

    print("---- MONGO RESULT ----")
    print("Total rows:", total_rows)
    print("Ops sent:", sent_ops)
    print("Matched:", matched)
    print("Modified:", modified)
    print("Upserted:", upserted)
    print(f"Elapsed: {elapsed:.2f}s")


# ==========================================
# MAIN
# ==========================================
def main():

    if not all([URL, PHPSESSID, MONGO_URI]):
        raise Exception("Missing environment variables")

    print("🚀 Start ETL:", utcnow().isoformat())

    html = fetch_html()
    df = extract_first_table(html)

    print("Rows fetched:", len(df))

    df = normalize_cols(df)
    df = map_columns(df)
    df = parse_dates(df)

    # 🔥 Remove rows with invalid key
    df = df[df["garage_entry_at"].notna()]

    # 🔥 Convert ALL remaining NaT/NaN → None (critical fix)
    df = df.where(pd.notnull(df), None)

    df = df.drop_duplicates(
        subset=["receipt_no", "truck_no", "garage_entry_at"],
        keep="last",
    )

    upsert_mongo(df)

    print("✅ ETL Completed Successfully")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("❌ ETL FAILED:", str(e))
        traceback.print_exc()
        sys.exit(1)