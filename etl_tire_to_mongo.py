import os
import sys
import time
import traceback
import requests
import urllib3

from bs4 import BeautifulSoup
from pymongo import MongoClient, ReplaceOne
from datetime import datetime, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# ENV CONFIG
# ==========================================
URL_TEMPLATE = os.getenv("TIRE_EXPORT_URL_TEMPLATE")
PHPSESSID = os.getenv("MENA_SESSION")
MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "atms"
COLLECTION_NAME = "tire"
BATCH_SIZE = 1000
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

    r = session.get(url, headers=HEADERS, verify=False, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


def parse_table(html):
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table")
    if not table:
        return []

    headers = [th.get_text(strip=True) for th in table.find_all("th")]

    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue

        values = [c.get_text(strip=True) for c in cells]

        if len(values) == len(headers):
            rows.append(dict(zip(headers, values)))

    return rows


# ==========================================
# MONGO UPSERT
# ==========================================
def upsert_mongo(rows):

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    col.create_index(
        [("receipt_no", 1), ("truck_no", 1), ("garage_entry_at", 1)],
        name="uniq_tire_composite",
        background=True,
    )

    ops = []
    total = 0

    for r in rows:

        receipt = r.get("แจ้งซ่อม_/_ขอเปลี่ยนยาง", "")
        truck = r.get("ยานพาหนะ", "")
        entry = r.get("เปลี่ยนเข้า", "")

        if not receipt or not truck or not entry:
            continue

        record = r.copy()
        record["receipt_no"] = receipt
        record["truck_no"] = truck
        record["garage_entry_at"] = entry
        record["etl_loaded_at"] = utcnow()

        ops.append(
            ReplaceOne(
                {
                    "receipt_no": receipt,
                    "truck_no": truck,
                    "garage_entry_at": entry,
                },
                record,
                upsert=True,
            )
        )

        total += 1

        if len(ops) >= BATCH_SIZE:
            col.bulk_write(ops, ordered=False)
            ops = []

    if ops:
        col.bulk_write(ops, ordered=False)

    client.close()
    print("🔥 Sent to Mongo:", total)


# ==========================================
# MAIN
# ==========================================
def main():

    if not all([URL_TEMPLATE, PHPSESSID, MONGO_URI]):
        raise Exception(
            "Missing environment variables: "
            "TIRE_EXPORT_URL_TEMPLATE, MENA_SESSION, MONGO_URI"
        )

    print("🚀 Start ETL:", utcnow())

    all_rows = []

    # iterate pages
    for page in range(1, 200):  # adjust max pages if needed
        url = URL_TEMPLATE.format(page=page)
        html = fetch_html(url)
        rows = parse_table(html)

        if not rows:
            break

        print(f"Page {page}: {len(rows)} rows")
        all_rows.extend(rows)

    print("Total rows collected:", len(all_rows))

    if all_rows:
        upsert_mongo(all_rows)
        print("✅ Completed successfully")
    else:
        print("⚠️ No rows found")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("❌ ETL FAILED:", e)
        traceback.print_exc()
        sys.exit(1)