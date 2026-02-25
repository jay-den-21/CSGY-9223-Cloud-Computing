import json
import os
import uuid
from datetime import datetime, timezone

import boto3

LEX_REGION = os.getenv("LEX_REGION", "us-east-1")
LEX_BOT_ID = os.environ["LEX_BOT_ID"]
LEX_BOT_ALIAS_ID = os.environ["LEX_BOT_ALIAS_ID"]
LEX_LOCALE_ID = os.getenv("LEX_LOCALE_ID", "en_US")

lex = boto3.client("lexv2-runtime", region_name=LEX_REGION)

def _response(status_code, payload):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",   # CORS
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        "body": json.dumps(payload)
    }

def lambda_handler(event, context):
    try:
        # 1) parse body
        body = event.get("body", event)
        if isinstance(body, str):
            body = json.loads(body)

        # 2) extract text
        # swagger style: messages[0].unstructured.text
        msgs = body.get("messages", [])
        if not msgs:
            return _response(400, {"error": "Missing messages[]"})
        text = (((msgs[0] or {}).get("unstructured") or {}).get("text") or "").strip()
        if not text:
            return _response(400, {"error": "Missing unstructured.text"})

        # sessionId（会话ID）
        body_session_id = body.get("sessionId")
        if body_session_id:
            body_session_id = str(body_session_id).strip()
        if not body_session_id:
            body_session_id = None

        session_id = (
            body_session_id
            or (((msgs[0] or {}).get("unstructured") or {}).get("id"))
            or (event.get("requestContext", {}) or {}).get("requestId")
            or str(uuid.uuid4())
        )

        # 3) call Lex（调用Lex）
        lex_resp = lex.recognize_text(
            botId=LEX_BOT_ID,
            botAliasId=LEX_BOT_ALIAS_ID,
            localeId=LEX_LOCALE_ID,
            sessionId=session_id,
            text=text
        )

        # 4) extract Lex response
        lex_messages = lex_resp.get("messages", [])
        reply_text = "Sorry, I didn't get that."
        if lex_messages:
            # Lex V2 message 格式通常有 content
            reply_text = lex_messages[0].get("content") or reply_text

        # 5) return API response according to our Swagger file
        out = {
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "id": str(uuid.uuid4()),
                        "text": reply_text,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                }
            ]
        }
        return _response(200, out)

    except Exception as e:
        print("LF0 error:", repr(e))
        return _response(500, {"error": str(e)})
