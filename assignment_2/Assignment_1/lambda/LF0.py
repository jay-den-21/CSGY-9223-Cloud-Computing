import json
import os
import uuid
from datetime import datetime, timezone

import boto3

LEX_REGION = os.getenv("LEX_REGION", "us-east-1")
LEX_BOT_ID = os.environ["LEX_BOT_ID"]
LEX_BOT_ALIAS_ID = os.environ["LEX_BOT_ALIAS_ID"]
LEX_LOCALE_ID = os.getenv("LEX_LOCALE_ID", "en_US")
LF3_REGION = os.getenv("LF3_REGION", LEX_REGION)
LAST_SEARCH_LAMBDA_NAME = os.getenv("LAST_SEARCH_LAMBDA_NAME", "")

GREETING_INPUTS = {
    "hi", "hello", "hey", "hi there", "hello there",
    "good morning", "good afternoon", "good evening",
}
RETURNING_USER_PROBE = "__returning_user_check__"

lex = boto3.client("lexv2-runtime", region_name=LEX_REGION)
lambda_client = boto3.client("lambda", region_name=LF3_REGION) if LAST_SEARCH_LAMBDA_NAME else None

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

def _normalise_text(text):
    return " ".join((text or "").strip().lower().split())

def _extract_user_id(body, msgs):
    body_user_id = body.get("userId")
    msg_user_id = (((msgs[0] or {}).get("unstructured") or {}).get("userId"))
    user_id = body_user_id or msg_user_id
    if user_id is None:
        return None
    user_id = str(user_id).strip()
    return user_id or None

def _invoke_last_search_recommendation(user_id):
    if not (LAST_SEARCH_LAMBDA_NAME and lambda_client and user_id):
        return None
    try:
        invoke_resp = lambda_client.invoke(
            FunctionName=LAST_SEARCH_LAMBDA_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps({"userId": user_id}).encode("utf-8"),
        )
        if invoke_resp.get("FunctionError"):
            raw_error = invoke_resp.get("Payload").read().decode("utf-8", errors="replace")
            print("LF0 LF3 function error:", raw_error)
            return None

        payload_stream = invoke_resp.get("Payload")
        raw = payload_stream.read().decode("utf-8", errors="replace") if payload_stream else ""
        if not raw:
            return None

        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("body"), str):
            # compatible with proxy-style lambda responses
            try:
                data = json.loads(data["body"])
            except Exception:
                pass

        if isinstance(data, dict):
            return data
        return None
    except Exception as e:
        print("LF0 failed invoking LF3:", repr(e))
        return None

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
        user_id = _extract_user_id(body, msgs)

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

        # Optional: returning-user shortcut for extra credit.
        normalised_text = _normalise_text(text)
        if normalised_text == RETURNING_USER_PROBE:
            reco = _invoke_last_search_recommendation(user_id)
            if reco and reco.get("hasRecommendation") and reco.get("message"):
                out = {
                    "messages": [
                        {
                            "type": "unstructured",
                            "unstructured": {
                                "id": str(uuid.uuid4()),
                                "text": reco["message"],
                                "timestamp": datetime.now(timezone.utc).isoformat()
                            }
                        }
                    ]
                }
                return _response(200, out)
            return _response(200, {"messages": []})

        if user_id and normalised_text in GREETING_INPUTS:
            reco = _invoke_last_search_recommendation(user_id)
            if reco and reco.get("hasRecommendation") and reco.get("message"):
                out = {
                    "messages": [
                        {
                            "type": "unstructured",
                            "unstructured": {
                                "id": str(uuid.uuid4()),
                                "text": reco["message"],
                                "timestamp": datetime.now(timezone.utc).isoformat()
                            }
                        }
                    ]
                }
                return _response(200, out)

        # 3) call Lex（调用Lex）
        lex_kwargs = {
            "botId": LEX_BOT_ID,
            "botAliasId": LEX_BOT_ALIAS_ID,
            "localeId": LEX_LOCALE_ID,
            "sessionId": session_id,
            "text": text,
        }
        if user_id:
            lex_kwargs["sessionState"] = {
                "sessionAttributes": {
                    "userId": user_id
                }
            }

        lex_resp = lex.recognize_text(
            **lex_kwargs
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
