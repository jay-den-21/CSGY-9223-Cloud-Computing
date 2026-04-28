import json
import os
import re
import base64
import urllib.parse
import urllib.request
from uuid import uuid4

import boto3

print("hw3 backend pipeline test!")

lex = boto3.client("lexv2-runtime")
s3 = boto3.client("s3")

OPENSEARCH_ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"].rstrip("/")
OPENSEARCH_USERNAME = os.environ["OPENSEARCH_USERNAME"]
OPENSEARCH_PASSWORD = os.environ["OPENSEARCH_PASSWORD"]
INDEX_NAME = os.environ.get("INDEX_NAME", "photos")

BOT_ID = os.environ["BOT_ID"]
BOT_ALIAS_ID = os.environ["BOT_ALIAS_ID"]
LOCALE_ID = os.environ.get("LOCALE_ID", "en_US")
PHOTO_BUCKET = os.environ.get("PHOTO_BUCKET", "")


STOPWORDS = {
    "show", "me", "photos", "photo", "pictures", "picture",
    "with", "of", "the", "a", "an", "and", "or", "in", "them",
    "for", "find", "search", "please", "i", "want", "to", "see"
}


def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,X-Api-Key,x-amz-meta-customLabels",
        "Access-Control-Allow-Methods": "GET,OPTIONS"
    }


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": cors_headers(),
        "body": json.dumps(body)
    }


def normalise_word(word):
    word = word.strip().lower()
    word = re.sub(r"[^a-z0-9_-]", "", word)

    candidates = [word]

    # Simple singular fallback: cats -> cat, dogs -> dog.
    if len(word) > 3 and word.endswith("s"):
        candidates.append(word[:-1])

    return [w for w in candidates if w]


def get_query_from_event(event):
    params = event.get("queryStringParameters") or {}
    q = params.get("q")

    if q:
        return q.strip()

    # Useful for manual Lambda test events.
    if "q" in event:
        return str(event["q"]).strip()

    return ""


def extract_slot_values(slots):
    keywords = []

    if not slots:
        return keywords

    for slot_name, slot in slots.items():
        if not slot:
            continue

        # Scalar slot
        value_obj = slot.get("value")
        if value_obj:
            raw_value = (
                value_obj.get("interpretedValue")
                or value_obj.get("originalValue")
            )
            if raw_value:
                keywords.append(raw_value)

        # List slot, if Lex returns multiple values
        for item in slot.get("values", []) or []:
            item_value = item.get("value", {})
            raw_value = (
                item_value.get("interpretedValue")
                or item_value.get("originalValue")
            )
            if raw_value:
                keywords.append(raw_value)

    return keywords


def lex_extract_keywords(query_text):
    """
    Send user query to Lex and extract slot values.
    If Lex returns no useful slot, fallback to simple keyword parsing.
    """
    try:
        lex_response = lex.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=str(uuid4()),
            text=query_text
        )

        print("Lex response:")
        print(json.dumps(lex_response))

        intent = lex_response.get("sessionState", {}).get("intent", {})
        intent_name = intent.get("name")

        if intent_name != "SearchIntent":
            print(f"Lex did not return SearchIntent. Got: {intent_name}")
            return []

        slot_keywords = extract_slot_values(intent.get("slots", {}))

    except Exception as exc:
        print(f"Lex error: {exc}")
        slot_keywords = []

    # Fallback: parse original query text.
    if not slot_keywords:
        slot_keywords = [query_text]

    keywords = []
    for phrase in slot_keywords:
        for token in re.split(r"[\s,]+", phrase.lower()):
            token = token.strip()
            if token and token not in STOPWORDS:
                keywords.extend(normalise_word(token))

    # Remove duplicates while preserving order.
    seen = set()
    final_keywords = []
    for kw in keywords:
        if kw and kw not in seen:
            final_keywords.append(kw)
            seen.add(kw)

    return final_keywords


def opensearch_request(path, payload):
    url = f"{OPENSEARCH_ENDPOINT}{path}"
    data = json.dumps(payload).encode("utf-8")

    auth_text = f"{OPENSEARCH_USERNAME}:{OPENSEARCH_PASSWORD}"
    auth_header = base64.b64encode(auth_text.encode("utf-8")).decode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth_header}"
        }
    )

    with urllib.request.urlopen(request, timeout=10) as res:
        return json.loads(res.read().decode("utf-8"))


def search_photos_by_labels(keywords):
    if not keywords:
        return []

    query = {
        "query": {
            "terms": {
                "labels": keywords
            }
        }
    }

    result = opensearch_request(f"/{INDEX_NAME}/_search", query)
    hits = result.get("hits", {}).get("hits", [])

    photos = []
    seen_keys = set()

    for hit in hits:
        source = hit.get("_source", {})
        bucket = source.get("bucket")
        object_key = source.get("objectKey")

        if not bucket or not object_key:
            continue

        unique_key = f"{bucket}/{object_key}"
        if unique_key in seen_keys:
            continue
        seen_keys.add(unique_key)

        # This format works well for frontend display.
        # Later, API Gateway can expose the S3 object path.
        photos.append({
            "objectKey": object_key,
            "bucket": bucket,
            "createdTimestamp": source.get("createdTimestamp"),
            "labels": source.get("labels", []),
            "url": s3.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": bucket,
            "Key": object_key
        },
        ExpiresIn=3600
    )

        })

    return photos


def lambda_handler(event, context):
    print("Received event:")
    print(json.dumps(event))

    http_method = (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method")
    )

    if http_method == "OPTIONS":
        return response(200, {})

    query_text = get_query_from_event(event)

    if not query_text:
        return response(200, {
            "results": [],
            "message": "Missing query parameter q"
        })

    keywords = lex_extract_keywords(query_text)
    print(f"Extracted keywords: {keywords}")

    photos = search_photos_by_labels(keywords)

    return response(200, {
        "results": photos,
        "keywords": keywords
    })