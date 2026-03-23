import os
import json
import random
import base64
import logging
from datetime import datetime, timezone
from urllib import request as urlrequest
from urllib import error as urlerror

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
DDB_TABLE_NAME = os.environ["DDB_TABLE_NAME"]
OPENSEARCH_ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"].rstrip("/")
OPENSEARCH_INDEX = os.environ.get("OPENSEARCH_INDEX", "restaurants")
OS_USERNAME = os.environ["OS_USERNAME"]
OS_PASSWORD = os.environ["OS_PASSWORD"]
SES_SOURCE_EMAIL = os.environ["SES_SOURCE_EMAIL"]
SES_REGION = os.environ.get("SES_REGION", AWS_REGION)
MAX_MESSAGES_PER_RUN = int(os.environ.get("MAX_MESSAGES_PER_RUN", "5"))

sqs = boto3.client("sqs", region_name=AWS_REGION)
ddb = boto3.client("dynamodb", region_name=AWS_REGION)
ses = boto3.client("ses", region_name=SES_REGION)


def os_request(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{OPENSEARCH_ENDPOINT}{path}"
    headers = {"Content-Type": "application/json"}

    creds = f"{OS_USERNAME}:{OS_PASSWORD}".encode("utf-8")
    headers["Authorization"] = "Basic " + base64.b64encode(creds).decode("ascii")

    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")

    req = urlrequest.Request(url=url, data=data, headers=headers, method=method)

    try:
        with urlrequest.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urlerror.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        logger.error("OpenSearch HTTPError %s %s body=%s", e.code, e.reason, err_body)
        raise
    except Exception:
        logger.exception("OpenSearch request failed")
        raise


def parse_sqs_body(body_raw: str) -> dict:
    """匹配 LF1 当前小写 keys（lowercase keys）"""
    req = json.loads(body_raw)

    # 兼容不同命名风格（lowercase / camel / Capitalized）
    key_aliases = {
        "cuisine": ["cuisine", "Cuisine"],
        "email": ["email", "Email"],
        "location": ["location", "Location"],
        "date": ["date", "Date", "dining_date", "DiningDate"],
        "time": ["time", "Time", "dining_time", "DiningTime"],
        "people": ["people", "People", "numberOfPeople", "NumberOfPeople", "people_count"],
    }
    norm = {}
    for target_key, aliases in key_aliases.items():
        val = None
        for k in aliases:
            if req.get(k) is not None and str(req.get(k)).strip() != "":
                val = req.get(k)
                break
        norm[target_key] = val

    # required fields（必填字段）
    required = ["cuisine", "email"]
    missing = [k for k in required if not norm.get(k)]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    # normalise（规范化）
    norm["cuisine"] = str(norm["cuisine"]).strip().lower()
    norm["email"] = str(norm["email"]).strip()
    norm["location"] = str(norm.get("location") or "manhattan").strip().lower()
    norm["date"] = str(norm.get("date") or "")
    norm["time"] = str(norm.get("time") or "")
    norm["people"] = str(norm.get("people") or "")

    return norm


def search_restaurant_ids_by_cuisine(cuisine: str, size: int = 20) -> list[str]:
    cuisine = cuisine.strip().lower()
    if not cuisine:
        return []

    # 兼容 Step 6 mapping（keyword）+ fallback
    query = {
        "size": size,
        "_source": ["RestaurantID", "Cuisine"],
        "query": {
            "bool": {
                "should": [
                    {"term": {"Cuisine": cuisine}},
                    {"term": {"Cuisine.keyword": cuisine}},
                    {"match": {"Cuisine": cuisine}}
                ],
                "minimum_should_match": 1
            }
        }
    }

    resp = os_request("POST", f"/{OPENSEARCH_INDEX}/_search", query)
    hits = resp.get("hits", {}).get("hits", [])
    ids = []
    for h in hits:
        src = h.get("_source", {})
        rid = src.get("RestaurantID")
        if rid and rid not in ids:
            ids.append(rid)
    return ids


def ddb_batch_get_restaurants(business_ids: list[str]) -> list[dict]:
    if not business_ids:
        return []

    keys = [{"BusinessId": {"S": bid}} for bid in business_ids[:100]]
    resp = ddb.batch_get_item(
        RequestItems={
            DDB_TABLE_NAME: {
                "Keys": keys
            }
        }
    )
    items = resp.get("Responses", {}).get(DDB_TABLE_NAME, [])

    def parse_attr_map(attr_map):
        out = {}
        for k, v in attr_map.items():
            if "S" in v:
                out[k] = v["S"]
            elif "N" in v:
                n = v["N"]
                out[k] = float(n) if "." in n else int(n)
            elif "BOOL" in v:
                out[k] = v["BOOL"]
            elif "M" in v:
                out[k] = v["M"]
            elif "L" in v:
                out[k] = v["L"]
            else:
                out[k] = v
        return out

    return [parse_attr_map(i) for i in items]


def format_email(req: dict, restaurants: list[dict]) -> tuple[str, str]:
    cuisine = req.get("cuisine", "")
    location = req.get("location", "manhattan")
    date_ = req.get("date", "")
    time_ = req.get("time", "")
    people = req.get("people", "")

    subject = "Your Dining Concierge Recommendations"

    if not restaurants:
        body = f"""Hello!

Here are your dining request details:
- Cuisine: {cuisine}
- Location: {location}
- Date: {date_}
- Time: {time_}
- Number of people: {people}

Sorry, we could not find matching restaurants at the moment.
Please try another cuisine or try again later.

Best,
Dining Concierge Bot
"""
        return subject, body

    lines = []
    for idx, r in enumerate(restaurants, start=1):
        name = r.get("Name", "Unknown")
        address = r.get("Address", "N/A")
        rating = r.get("Rating", "N/A")
        reviews = r.get("NumberOfReviews", "N/A")
        lines.append(
            f"{idx}. {name}\n"
            f"   Address: {address}\n"
            f"   Rating: {rating} ({reviews} reviews)\n"
        )

    body = f"""Hello!

Here are my {cuisine} restaurant suggestions for {people} people, on {date_} at {time_} in {location}:

{chr(10).join(lines)}

Enjoy your meal!

Best,
Dining Concierge Bot
"""
    return subject, body


def send_email(to_email: str, subject: str, body_text: str) -> str:
    resp = ses.send_email(
        Source=SES_SOURCE_EMAIL,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body_text}}
        }
    )
    return resp.get("MessageId", "")


def delete_polled_message(receipt_handle: str):
    sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)


def extract_sqs_message_fields(msg: dict) -> tuple[str, str | None]:
    """Support both poll format and Lambda SQS-trigger format."""
    body_raw = msg.get("Body") if "Body" in msg else msg.get("body")
    receipt_handle = msg.get("ReceiptHandle") if "ReceiptHandle" in msg else msg.get("receiptHandle")
    return body_raw or "{}", receipt_handle


def process_one_message(msg: dict, should_delete_from_queue: bool) -> bool:
    body_raw, receipt_handle = extract_sqs_message_fields(msg)

    try:
        req = parse_sqs_body(body_raw)
    except Exception as e:
        logger.error("Invalid SQS body, deleting poison message. err=%s body=%s", str(e), body_raw)
        if should_delete_from_queue and receipt_handle:
            delete_polled_message(receipt_handle)
        return True

    cuisine = req["cuisine"]
    to_email = req["email"]

    ids = search_restaurant_ids_by_cuisine(cuisine, size=20)
    logger.info("OpenSearch matched %d RestaurantIDs for cuisine=%s", len(ids), cuisine)

    chosen_ids = random.sample(ids, k=min(3, len(ids))) if ids else []
    restaurants = ddb_batch_get_restaurants(chosen_ids)

    subject, body_text = format_email(req, restaurants)
    try:
        ses_msg_id = send_email(to_email, subject, body_text)
        logger.info("SES send_email success MessageId=%s to=%s", ses_msg_id, to_email)
    except ClientError as e:
        err = e.response.get("Error", {})
        logger.error(
            "SES send_email failed code=%s message=%s to=%s source=%s ses_region=%s",
            err.get("Code"), err.get("Message"), to_email, SES_SOURCE_EMAIL, SES_REGION
        )
        raise

    if should_delete_from_queue and receipt_handle:
        delete_polled_message(receipt_handle)
        logger.info("Deleted polled SQS message after successful processing")
    return True


def lambda_handler(event, context):
    logger.info("LF2 invoked at %s event=%s",
                datetime.now(timezone.utc).isoformat(),
                json.dumps(event)[:1000])

    # Mode A: Lambda is wired directly to SQS trigger (event.Records)
    trigger_records = event.get("Records") if isinstance(event, dict) else None
    if trigger_records and isinstance(trigger_records, list):
        messages = [
            r for r in trigger_records
            if (r.get("eventSource") == "aws:sqs" or r.get("eventSourceARN"))
        ]
        should_delete_from_queue = False
        logger.info("Processing %d messages from SQS trigger event", len(messages))
    else:
        # Mode B: EventBridge/scheduled poller
        resp = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=min(MAX_MESSAGES_PER_RUN, 10),
            WaitTimeSeconds=10,
            VisibilityTimeout=120
        )
        messages = resp.get("Messages", [])
        should_delete_from_queue = True
        logger.info("Polled %d SQS messages", len(messages))

    processed = 0
    failed = 0

    for msg in messages:
        try:
            ok = process_one_message(msg, should_delete_from_queue=should_delete_from_queue)
            if ok:
                processed += 1
            else:
                failed += 1
        except Exception:
            failed += 1
            logger.exception("Failed processing one SQS message (will be retried)")

    return {
        "statusCode": 200,
        "queue": "hw1-dining-q1-jayden",
        "received": len(messages),
        "processed": processed,
        "failed": failed
    }
