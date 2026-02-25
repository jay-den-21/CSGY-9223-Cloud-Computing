import os
import time
import random
from decimal import Decimal
from datetime import datetime, timezone

import requests
import boto3
from botocore.exceptions import ClientError

# =========================
# Config（配置）
# =========================
YELP_API_KEY = os.environ["YELP_API_KEY"]
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
TABLE_NAME = "yelp-restaurants"

# at least 5 cuisines
CUISINES = [
    {"name": "chinese", "term": "Chinese restaurants", "categories": "chinese"},
    {"name": "japanese", "term": "Japanese restaurants", "categories": "japanese"},
    {"name": "italian", "term": "Italian restaurants", "categories": "italian"},
    {"name": "mexican", "term": "Mexican restaurants", "categories": "mexican"},
    # American restaurants are spread across traditional + new american
    {"name": "american", "term": "American restaurants", "categories": "tradamerican,newamerican"},
]

TARGET_PER_CUISINE = int(os.getenv("TARGET_PER_CUISINE", "220"))
PAGE_SIZE = 50             # Yelp search 常见 page size
MAX_OFFSET = 400           # （作为 slack）

# 实际观测到 offset >= 200 会 400，所以做一个“有效上限”（effective cap）
# range(0, 200, 50) => [0, 50, 100, 150]
YELP_EFFECTIVE_OFFSET_EXCLUSIVE = 200

# Use multiple Manhattan sub-areas to avoid exhausting the same top results.
SEARCH_BUCKETS = [
    {"name": "fidi", "latitude": 40.7075, "longitude": -74.0113, "radius": 2600},
    {"name": "chinatown_soho", "latitude": 40.7191, "longitude": -73.9973, "radius": 2600},
    {"name": "east_village", "latitude": 40.7265, "longitude": -73.9815, "radius": 2600},
    {"name": "chelsea", "latitude": 40.7465, "longitude": -73.9964, "radius": 2600},
    {"name": "midtown", "latitude": 40.7580, "longitude": -73.9855, "radius": 2600},
    {"name": "uws", "latitude": 40.7870, "longitude": -73.9754, "radius": 2600},
    {"name": "ues", "latitude": 40.7736, "longitude": -73.9566, "radius": 2600},
    {"name": "harlem", "latitude": 40.8116, "longitude": -73.9465, "radius": 2600},
    {"name": "washington_heights", "latitude": 40.8401, "longitude": -73.9397, "radius": 2600},
]

SORT_MODES = ["best_match", "rating", "review_count"]

YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"
HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)


# =========================
# Helpers（辅助函数）
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat()


def to_decimal(x):
    if x is None:
        return None
    return Decimal(str(x))


def get_effective_offsets():
    """
    根据你的配置生成“有效 offsets”（effective offsets）
    你仍然保留 MAX_OFFSET=400，但实际会被 Yelp 限制 cap 到 < 200
    """
    effective_end = min(MAX_OFFSET, YELP_EFFECTIVE_OFFSET_EXCLUSIVE)

    # 保证 PAGE_SIZE 合理（sanity check）
    if PAGE_SIZE <= 0:
        raise ValueError("PAGE_SIZE must be > 0")

    offsets = list(range(0, effective_end, PAGE_SIZE))
    return offsets


def build_search_plan(offsets):
    plan = []
    for bucket in SEARCH_BUCKETS:
        for sort_by in SORT_MODES:
            for offset in offsets:
                plan.append(
                    {
                        "bucket": bucket,
                        "sort_by": sort_by,
                        "offset": offset,
                    }
                )
    return plan


def count_table_items():
    """
    统计 DynamoDB table 总记录数（total item count）
    使用 scan + Select='COUNT' 并处理 pagination（分页）
    """
    total = 0
    scan_kwargs = {"Select": "COUNT"}

    while True:
        resp = table.scan(**scan_kwargs)
        total += resp.get("Count", 0)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    return total


def count_table_items_by_cuisine():
    """
    统计 DynamoDB 中各 cuisine 的累计数量（cumulative counts）
    为了避免读取太多字段，仅投影（ProjectionExpression）必要字段
    """
    cuisine_counts = {}
    total = 0

    scan_kwargs = {
        "ProjectionExpression": "#c",
        "ExpressionAttributeNames": {"#c": "Cuisine"},
    }

    while True:
        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])
        for it in items:
            cuisine = it.get("Cuisine", "unknown")
            cuisine_counts[cuisine] = cuisine_counts.get(cuisine, 0) + 1
            total += 1

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    return total, cuisine_counts


def normalise_business(business, cuisine_name, cuisine_term):
    """
    生成 DynamoDB item（记录）
    required fields（必需字段）
    """
    loc = business.get("location", {}) or {}
    coords = business.get("coordinates", {}) or {}

    display_address = loc.get("display_address") or []
    address = ", ".join(display_address) if display_address else None

    item = {
        # partition key（分区键）
        "BusinessId": business.get("id"),

        # required fields（必需字段）
        "Name": business.get("name"),
        "Address": address,
        "Coordinates": {
            "latitude": to_decimal(coords.get("latitude")),
            "longitude": to_decimal(coords.get("longitude")),
        },
        "NumberOfReviews": int(business.get("review_count", 0)),
        "Rating": to_decimal(business.get("rating")),
        "ZipCode": loc.get("zip_code"),

        # insertedAtTimestamp（插入时间戳）
        "insertedAtTimestamp": now_iso(),

        # for further steps（后续步骤用）
        "Cuisine": cuisine_name,
        "CuisineTerm": cuisine_term,
    }
    return item


def yelp_search(term, categories, offset, sort_by, bucket, limit=50):
    params = {
        "term": term,
        "categories": categories,
        "latitude": bucket["latitude"],
        "longitude": bucket["longitude"],
        "radius": bucket["radius"],
        "sort_by": sort_by,
        "limit": limit,
        "offset": offset,
    }
    resp = requests.get(YELP_SEARCH_URL, headers=HEADERS, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def put_if_new(item):
    """
    条件写入（conditional write）防止重复（duplicate）
    DynamoDB 以 BusinessId 为唯一主键（primary key）
    """
    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(BusinessId)"
        )
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ConditionalCheckFailedException":
            return False
        raise


# =========================
# Main Logic（主逻辑）
# =========================
def main():
    # ---- 总体统计：运行前（before-run stats）----
    table_count_before = count_table_items()
    _, cuisine_counts_before = count_table_items_by_cuisine()

    effective_offsets = get_effective_offsets()
    search_plan = build_search_plan(effective_offsets)
    max_raw_per_cuisine = len(search_plan) * PAGE_SIZE

    print("=== CONFIG / EFFECTIVE PLAN ===")
    print(f"TARGET_PER_CUISINE (configured): {TARGET_PER_CUISINE}")
    print(f"PAGE_SIZE: {PAGE_SIZE}")
    print(f"MAX_OFFSET (configured): {MAX_OFFSET}")
    print(f"YELP_EFFECTIVE_OFFSET_EXCLUSIVE (observed): {YELP_EFFECTIVE_OFFSET_EXCLUSIVE}")
    print(f"Effective offsets used: {effective_offsets}")
    print(f"SEARCH_BUCKETS: {len(SEARCH_BUCKETS)}")
    print(f"SORT_MODES: {SORT_MODES}")
    print(f"Search tasks per cuisine: {len(search_plan)}")
    print(f"Max raw results per cuisine (theoretical): {max_raw_per_cuisine}")
    if TARGET_PER_CUISINE > max_raw_per_cuisine:
        print(
            f"[NOTE] TARGET_PER_CUISINE={TARGET_PER_CUISINE} > theoretical raw cap={max_raw_per_cuisine}. "
            "This is okay as slack, but may not be reachable per cuisine."
        )
    print(f"Table count BEFORE run: {table_count_before}")
    print()

    # ---- 运行内统计（in-run stats）----
    total_inserted = 0
    seen_in_this_run = set()
    per_cuisine_inserted = {}
    per_cuisine_needed = {}
    per_cuisine_queries_used = {}

    # 新增：详细统计（detailed metrics）
    metrics = {
        "api_requests_attempted": 0,
        "api_requests_success": 0,
        "http_400_count": 0,
        "http_429_count": 0,
        "other_http_error_count": 0,
        "other_exception_count": 0,
        "businesses_returned_total": 0,
        "skipped_missing_id_or_name": 0,
        "skipped_duplicate_in_run": 0,
        "skipped_duplicate_in_db": 0,
        "search_tasks_exhausted_before_target": 0,
    }

    for cuisine_cfg in CUISINES:
        cuisine_name = cuisine_cfg["name"]
        cuisine_term = cuisine_cfg["term"]
        cuisine_categories = cuisine_cfg["categories"]
        existing_count = cuisine_counts_before.get(cuisine_name, 0)
        needed = max(0, TARGET_PER_CUISINE - existing_count)
        per_cuisine_needed[cuisine_name] = needed

        if needed == 0:
            per_cuisine_inserted[cuisine_name] = 0
            per_cuisine_queries_used[cuisine_name] = 0
            print(f"[SKIP] {cuisine_name}: existing={existing_count} already >= target={TARGET_PER_CUISINE}")
            continue

        print(f"[START] {cuisine_name}: existing={existing_count}, need_insert={needed}")

        inserted_this_cuisine = 0
        queries_used_this_cuisine = 0
        cuisine_search_plan = search_plan[:]  # copy
        random.shuffle(cuisine_search_plan)

        for plan_item in cuisine_search_plan:
            if inserted_this_cuisine >= needed:
                break

            offset = plan_item["offset"]
            sort_by = plan_item["sort_by"]
            bucket = plan_item["bucket"]
            queries_used_this_cuisine += 1
            metrics["api_requests_attempted"] += 1

            try:
                data = yelp_search(
                    term=cuisine_term,
                    categories=cuisine_categories,
                    offset=offset,
                    sort_by=sort_by,
                    bucket=bucket,
                    limit=PAGE_SIZE,
                )
                metrics["api_requests_success"] += 1

                businesses = data.get("businesses", []) or []
                metrics["businesses_returned_total"] += len(businesses)

                if not businesses:
                    continue

                random.shuffle(businesses)

                for b in businesses:
                    bid = b.get("id")
                    name = b.get("name")

                    if not bid or not name:
                        metrics["skipped_missing_id_or_name"] += 1
                        continue

                    # 本次运行内去重（in-memory dedupe）
                    if bid in seen_in_this_run:
                        metrics["skipped_duplicate_in_run"] += 1
                        continue

                    item = normalise_business(b, cuisine_name, cuisine_term)

                    inserted = put_if_new(item)
                    seen_in_this_run.add(bid)

                    if inserted:
                        inserted_this_cuisine += 1
                        total_inserted += 1
                    else:
                        metrics["skipped_duplicate_in_db"] += 1

                    if inserted_this_cuisine >= needed:
                        break

                # 限流（rate limiting）
                time.sleep(0.2)

            except requests.HTTPError as e:
                status_code = None
                if getattr(e, "response", None) is not None:
                    status_code = e.response.status_code

                if status_code == 400:
                    metrics["http_400_count"] += 1
                elif status_code == 429:
                    metrics["http_429_count"] += 1
                else:
                    metrics["other_http_error_count"] += 1

                print(
                    "[HTTPError] "
                    f"cuisine={cuisine_name}, bucket={bucket['name']}, sort={sort_by}, "
                    f"offset={offset}, status={status_code}, err={e}"
                )

                # 429 限流时稍等久一点（backoff）
                if status_code == 429:
                    time.sleep(1.5)
                else:
                    time.sleep(0.5)

            except Exception as e:
                metrics["other_exception_count"] += 1
                print(
                    "[Error] "
                    f"cuisine={cuisine_name}, bucket={bucket['name']}, sort={sort_by}, "
                    f"offset={offset}, err={e}"
                )
                time.sleep(1.0)

        if inserted_this_cuisine < needed:
            metrics["search_tasks_exhausted_before_target"] += 1

        per_cuisine_inserted[cuisine_name] = inserted_this_cuisine
        per_cuisine_queries_used[cuisine_name] = queries_used_this_cuisine
        print(
            f"[DONE] {cuisine_name}: inserted={inserted_this_cuisine}, "
            f"needed={needed}, queries_used={queries_used_this_cuisine}/{len(search_plan)}"
        )

    # ---- 总体统计：运行后（after-run stats）----
    table_count_after = count_table_items()
    net_growth = table_count_after - table_count_before
    to_1000_gap = max(0, 1000 - table_count_after)

    # 可选：累计 cuisine 分布统计（cumulative cuisine distribution）
    total_count_scan, cuisine_counts_all = count_table_items_by_cuisine()

    print("\n=== RUN SUMMARY (THIS RUN) ===")
    for cuisine_cfg in CUISINES:
        cuisine_name = cuisine_cfg["name"]
        inserted = per_cuisine_inserted.get(cuisine_name, 0)
        needed = per_cuisine_needed.get(cuisine_name, 0)
        queries_used = per_cuisine_queries_used.get(cuisine_name, 0)
        print(f"{cuisine_name}: needed={needed}, inserted={inserted}, queries_used={queries_used}")
    print(f"TOTAL_INSERTED_THIS_RUN: {total_inserted}")

    print("\n=== OVERALL STATS (TABLE LEVEL) ===")
    print(f"TABLE_COUNT_BEFORE: {table_count_before}")
    print(f"TABLE_COUNT_AFTER : {table_count_after}")
    print(f"NET_GROWTH        : {net_growth}")
    print(f"GAP_TO_1000       : {to_1000_gap}")

    print("\n=== PIPELINE / API METRICS ===")
    for k, v in metrics.items():
        print(f"{k}: {v}")

    print("\n=== CUMULATIVE CUISINE COUNTS IN DYNAMODB ===")
    print(f"total_count_scan_check: {total_count_scan}")
    for cuisine_name in sorted(cuisine_counts_all.keys()):
        print(f"{cuisine_name}: {cuisine_counts_all[cuisine_name]}")

    # 额外提示（helpful note）
    if to_1000_gap > 0:
        print(
            "\n[NOTE] Still below 1000. Easiest top-up strategy: add new cuisines "
            "(e.g., Indian / Thai / Korean) and rerun."
        )
    else:
        print("\n[SUCCESS] Table has reached 1000+ records!!! Finally!!!")


if __name__ == "__main__":
    main()
