import base64
import json
import logging
import os
import random
from urllib import error as urlerror
from urllib import request as urlrequest

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
USER_STATE_TABLE = os.environ["USER_STATE_TABLE"]
DDB_TABLE_NAME = os.environ.get("DDB_TABLE_NAME", "yelp-restaurants")
OPENSEARCH_ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"].rstrip("/")
OPENSEARCH_INDEX = os.environ.get("OPENSEARCH_INDEX", "restaurants")
OS_USERNAME = os.environ["OS_USERNAME"]
OS_PASSWORD = os.environ["OS_PASSWORD"]
MAX_RECOMMENDATIONS = int(os.environ.get("MAX_RECOMMENDATIONS", "3"))
SEARCH_POOL_SIZE = int(os.environ.get("SEARCH_POOL_SIZE", "20"))

ddb_resource = boto3.resource("dynamodb", region_name=AWS_REGION)
ddb_client = boto3.client("dynamodb", region_name=AWS_REGION)
state_table = ddb_resource.Table(USER_STATE_TABLE)


def os_request(method, path, body=None):
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
        logger.error("LF3 OpenSearch HTTPError code=%s reason=%s body=%s", e.code, e.reason, e.read().decode("utf-8", errors="replace"))
        raise


def extract_user_id(event):
    if isinstance(event, dict):
        if event.get("userId"):
            return str(event["userId"]).strip()
        body = event.get("body")
        if isinstance(body, str):
            try:
                parsed = json.loads(body)
            except Exception:
                parsed = {}
            uid = parsed.get("userId")
            if uid:
                return str(uid).strip()
        elif isinstance(body, dict) and body.get("userId"):
            return str(body["userId"]).strip()
    return None


def get_last_search(user_id):
    resp = state_table.get_item(Key={"UserId": user_id})
    return resp.get("Item")


def search_restaurant_ids_by_cuisine(cuisine, size):
    cuisine = str(cuisine or "").strip().lower()
    if not cuisine:
        return []

    query = {
        "size": size,
        "_source": ["RestaurantID", "Cuisine"],
        "query": {
            "bool": {
                "should": [
                    {"term": {"Cuisine": cuisine}},
                    {"term": {"Cuisine.keyword": cuisine}},
                    {"match": {"Cuisine": cuisine}},
                ],
                "minimum_should_match": 1,
            }
        },
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


def ddb_batch_get_restaurants(business_ids):
    if not business_ids:
        return []

    keys = [{"BusinessId": {"S": bid}} for bid in business_ids[:100]]
    resp = ddb_client.batch_get_item(
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


def format_returning_user_message(location, cuisine, restaurants):
    if not restaurants:
        return (
            f"Welcome back! Last time you searched for {cuisine} food in {location}. "
            "I couldn't find matches right now, but tell me a cuisine and I will search again."
        )

    lines = []
    for idx, r in enumerate(restaurants, start=1):
        name = r.get("Name", "Unknown")
        address = r.get("Address", "N/A")
        lines.append(f"{idx}. {name}, located at {address}")

    return (
        f"Welcome back! Based on your last search for {cuisine} food in {location}, "
        f"here are some recommendations: {'; '.join(lines)}"
    )


def lambda_handler(event, context):
    user_id = extract_user_id(event)
    if not user_id:
        return {
            "hasRecommendation": False,
            "message": "",
            "reason": "missing_user_id",
        }

    last_state = get_last_search(user_id)
    if not last_state:
        return {
            "hasRecommendation": False,
            "message": "",
            "reason": "no_last_search",
        }

    location = str(last_state.get("LastLocation") or "manhattan").strip().lower()
    cuisine = str(last_state.get("LastCuisine") or "").strip().lower()
    if not cuisine:
        return {
            "hasRecommendation": False,
            "message": "",
            "reason": "missing_last_cuisine",
        }

    ids = search_restaurant_ids_by_cuisine(cuisine, size=max(SEARCH_POOL_SIZE, MAX_RECOMMENDATIONS))
    chosen_ids = random.sample(ids, k=min(MAX_RECOMMENDATIONS, len(ids))) if ids else []
    restaurants = ddb_batch_get_restaurants(chosen_ids)
    message = format_returning_user_message(location, cuisine, restaurants)

    return {
        "hasRecommendation": True,
        "message": message,
        "lastSearch": {
            "location": location,
            "cuisine": cuisine,
        },
        "restaurantCount": len(restaurants),
    }
