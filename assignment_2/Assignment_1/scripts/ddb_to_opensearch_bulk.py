import os
import json
import time
import requests
import boto3
from requests.auth import HTTPBasicAuth
from botocore.exceptions import NoRegionError

# =========================
# Config
# =========================
def resolve_aws_region() -> str:
    """
    Resolve region from common sources so both service client and
    credential-refresh sub-clients have a concrete region.
    """
    region = (
        os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or boto3.Session().region_name
        or "us-east-1"
    )
    # Keep both vars in sync for libraries/providers that only read one.
    os.environ.setdefault("AWS_REGION", region)
    os.environ.setdefault("AWS_DEFAULT_REGION", region)
    return region


AWS_REGION = resolve_aws_region()
DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME", "yelp-restaurants")
OPENSEARCH_ENDPOINT = os.getenv("OPENSEARCH_ENDPOINT", "")  # e.g. https://search-xxx.us-east-1.es.amazonaws.com
OPENSEARCH_INDEX = os.getenv("OPENSEARCH_INDEX", "restaurants")
OS_USERNAME = os.getenv("OS_USERNAME", "")  # master username
OS_PASSWORD = os.getenv("OS_PASSWORD", "")  # master password

# Batch size
BULK_SIZE = int(os.getenv("BULK_SIZE", "200"))

if not OPENSEARCH_ENDPOINT:
    raise ValueError("Missing env var OPENSEARCH_ENDPOINT")
if not OS_USERNAME or not OS_PASSWORD:
    raise ValueError("Missing env vars OS_USERNAME / OS_PASSWORD")

aws_session = boto3.Session(region_name=AWS_REGION)
ddb = aws_session.resource("dynamodb", region_name=AWS_REGION)
table = ddb.Table(DDB_TABLE_NAME)

session = requests.Session()
session.auth = HTTPBasicAuth(OS_USERNAME, OS_PASSWORD)
session.headers.update({"Content-Type": "application/x-ndjson"})


def normalise_cuisine(item: dict) -> str | None:
    """Read cuisine from Cuisine or CuisineTerm and normalise（规范化）"""
    c = item.get("Cuisine")
    if not c:
        c = item.get("CuisineTerm")
    if not c:
        return None
    return str(c).strip().lower()


def scan_all_items():
    """Full table scan with pagination（分页扫描）"""
    scan_kwargs = {}
    total = 0
    while True:
        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])
        total += len(items)
        print(f"[scan] fetched {len(items)} items (running total={total})")
        for item in items:
            yield item

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key


def build_bulk_payload(docs: list[dict]) -> str:
    """
    NDJSON format（换行 JSON 格式）for _bulk API:
    { "index": { "_index": "restaurants", "_id": "BusinessId" } }
    { "RestaurantID": "...", "Cuisine": "..." }
    """
    lines = []
    for d in docs:
        meta = {"index": {"_index": OPENSEARCH_INDEX, "_id": d["RestaurantID"]}}
        lines.append(json.dumps(meta))
        lines.append(json.dumps(d, ensure_ascii=False))
    return "\n".join(lines) + "\n"


def bulk_index(docs: list[dict]):
    if not docs:
        return 0, 0

    payload = build_bulk_payload(docs)
    url = f"{OPENSEARCH_ENDPOINT}/_bulk"

    r = session.post(url, data=payload.encode("utf-8"), timeout=60)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        print("[bulk] HTTP error:", r.status_code, r.text[:1000])
        raise

    data = r.json()
    errors = data.get("errors", False)
    items = data.get("items", [])

    ok_count = 0
    err_count = 0

    for it in items:
        idx = it.get("index", {})
        status = idx.get("status", 0)
        if 200 <= status < 300:
            ok_count += 1
        else:
            err_count += 1
            print("[bulk-item-error]", json.dumps(idx, ensure_ascii=False)[:1000])

    if errors:
        print(f"[bulk] partial failures detected: ok={ok_count}, err={err_count}")
    else:
        print(f"[bulk] success: ok={ok_count}")

    return ok_count, err_count


def main():
    print("=== DDB -> OpenSearch Bulk Ingestion (MVP) ===")
    print("AWS_REGION =", AWS_REGION)
    print("DDB_TABLE_NAME =", DDB_TABLE_NAME)
    print("OPENSEARCH_ENDPOINT =", OPENSEARCH_ENDPOINT)
    print("OPENSEARCH_INDEX =", OPENSEARCH_INDEX)

    batch = []
    scanned = 0
    prepared = 0
    skipped = 0
    total_ok = 0
    total_err = 0

    try:
        for item in scan_all_items():
            scanned += 1

            business_id = item.get("BusinessId")
            cuisine = normalise_cuisine(item)

            if not business_id or not cuisine:
                skipped += 1
                continue

            doc = {
                "RestaurantID": str(business_id),
                "Cuisine": cuisine
            }
            batch.append(doc)
            prepared += 1

            if len(batch) >= BULK_SIZE:
                ok, err = bulk_index(batch)
                total_ok += ok
                total_err += err
                batch = []
                time.sleep(0.2)
    except NoRegionError as e:
        raise RuntimeError(
            "AWS region is not configured. Set AWS_REGION or AWS_DEFAULT_REGION, "
            "or configure your AWS profile region."
        ) from e

    if batch:
        ok, err = bulk_index(batch)
        total_ok += ok
        total_err += err

    print("=== Done ===")
    print(f"scanned={scanned}, prepared={prepared}, skipped={skipped}, indexed_ok={total_ok}, indexed_err={total_err}")


if __name__ == "__main__":
    main()
