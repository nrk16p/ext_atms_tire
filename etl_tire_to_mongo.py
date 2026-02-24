import os
import sys
import time
import traceback
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, ReplaceOne
from datetime import datetime, timezone

# ==========================================
# CONFIG
# ==========================================
URL_TEMPLATE = os.getenv("TIRE_EXPORT_URL_TEMPLATE")  # must be like "...index?page={}"
PHPSESSID = os.getenv("MENA_SESSION")
MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "atms"
COLLECTION_NAME = "tire"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
REQUEST_TIMEOUT = 60

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ==========================================
# HELPERS
# ==========================================
def utcnow():
    return datetime.now(timezone.utc).isoformat()


def fetch_html(url):
    session = requests.Session()
    session.cookies.set("PHPSESSID", PHPSESSID)
    resp = session.get(url, headers=HEADERS, verify=False, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def parse_table(html: str):
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    rows = []
    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    for tr in table.find_all("tr"):
        cells = tr.find_all(["td"])
        if not cells:
            continue
        vals = [c.get_text(strip=True) for c in cells]
        # if row length matches headers, map
        if len(vals) == len(headers):
            rows.append(dict(zip(headers, vals)))
    return rows


# ==========================================
# MONGO UPSERT
# ==========================================
def upsert_mongo(rows: list[dict]):
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]
    col.create_index(
        [("receipt_no", 1), ("truck_no", 1), ("garage_entry_at", 1)],
        name="uniq_tire_composite",
        background=True,
    )

    ops = []
    for r in rows:
        # use text values for keys
        key = {
            "receipt_no": r.get("แจ้งซ่อม_/_ขอเปลี่ยนยาง") or "",
            "truck_no": r.get("ยานพาหนะ") or "",
            "garage_entry_at": r.get("เปลี่ยนเข้า") or "",
        }
        if not key["receipt_no"] or not key["truck_no"] or not key["garage_entry_at"]:
            continue

        rec = r.copy()
        rec["etl_loaded_at"] = utcnow()
        ops.append(ReplaceOne(key, rec, upsert=True))

        if len(ops) >= BATCH_SIZE:
            col.bulk_write(ops, ordered=False)
            ops = []

    if ops:
        col.bulk_write(ops, ordered=False)

    client.close()


# ==========================================
# MAIN
# ==========================================
def main():
    if not all([URL_TEMPLATE, PHPSESSID, MONGO_URI]):
        raise Exception("Missing environment variables: TIRE_EXPORT_URL_TEMPLATE, MENA_SESSION, MONGO_URI")

    all_rows = []

    # Example: iterate page numbers until no data
    for page in range(1, 50):  # adjust max pages if needed
        url = URL_TEMPLATE.format(page=page)
        html = fetch_html(url)
        rows = parse_table(html)
        if not rows:
            break
        print(f"Page {page} rows: {len(rows)}")
        all_rows.extend(rows)

    print("Total rows collected:", len(all_rows))

    if all_rows:
        upsert_mongo(all_rows)
        print("Done")
    else:
        print("No rows found")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("❌ ETL FAILED:", e)
        traceback.print_exc()
        sys.exit(1)