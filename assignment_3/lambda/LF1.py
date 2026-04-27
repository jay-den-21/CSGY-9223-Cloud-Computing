import json
import os
import base64
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3


s3 = boto3.client("s3")
rekognition = boto3.client("rekognition")

OPENSEARCH_ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"].rstrip("/")
OPENSEARCH_USERNAME = os.environ["OPENSEARCH_USERNAME"]
OPENSEARCH_PASSWORD = os.environ["OPENSEARCH_PASSWORD"]
INDEX_NAME = os.environ.get("INDEX_NAME", "photos")


def normalise_label(label: str) -> str:
    return label.strip().lower()


def get_custom_labels(bucket: str, key: str):
    """
    Read x-amz-meta-customLabels from S3 object metadata.

    In boto3 head_object response, user metadata keys are lowercased.
    So x-amz-meta-customLabels becomes customlabels.
    """
    response = s3.head_object(Bucket=bucket, Key=key)
    metadata = response.get("Metadata", {})

    raw_custom_labels = metadata.get("customlabels", "")
    if not raw_custom_labels:
        return []

    labels = []
    for item in raw_custom_labels.split(","):
        item = normalise_label(item)
        if item:
            labels.append(item)

    return labels


def detect_rekognition_labels(bucket: str, key: str):
    response = rekognition.detect_labels(
        Image={
            "S3Object": {
                "Bucket": bucket,
                "Name": key
            }
        },
        MaxLabels=20,
        MinConfidence=70
    )

    labels = []
    for item in response.get("Labels", []):
        name = item.get("Name")
        if name:
            labels.append(normalise_label(name))

    return labels


def index_document(document_id: str, document: dict):
    url = f"{OPENSEARCH_ENDPOINT}/{INDEX_NAME}/_doc/{urllib.parse.quote(document_id)}"

    body = json.dumps(document).encode("utf-8")

    auth_text = f"{OPENSEARCH_USERNAME}:{OPENSEARCH_PASSWORD}"
    auth_header = base64.b64encode(auth_text.encode("utf-8")).decode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=body,
        method="PUT",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth_header}"
        }
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        response_body = response.read().decode("utf-8")
        return json.loads(response_body)


def lambda_handler(event, context):
    print("Received event:")
    print(json.dumps(event))

    indexed_results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        print(f"Processing image: s3://{bucket}/{key}")

        rekognition_labels = detect_rekognition_labels(bucket, key)
        custom_labels = get_custom_labels(bucket, key)

        all_labels = sorted(set(rekognition_labels + custom_labels))

        created_timestamp = datetime.now(timezone.utc).isoformat()

        document = {
            "objectKey": key,
            "bucket": bucket,
            "createdTimestamp": created_timestamp,
            "labels": all_labels
        }

        document_id = f"{bucket}-{key}".replace("/", "-")

        print("Document to index:")
        print(json.dumps(document))

        result = index_document(document_id, document)

        print("OpenSearch index result:")
        print(json.dumps(result))

        indexed_results.append({
            "bucket": bucket,
            "objectKey": key,
            "labels": all_labels,
            "opensearchResult": result
        })

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Indexing completed",
            "results": indexed_results
        })
    }