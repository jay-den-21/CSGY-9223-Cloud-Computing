import json
import os
import re
from datetime import datetime, timezone

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
USER_STATE_TABLE = os.environ.get("USER_STATE_TABLE", "")

sqs = boto3.client("sqs", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION) if USER_STATE_TABLE else None
user_state_table = ddb.Table(USER_STATE_TABLE) if USER_STATE_TABLE else None

# 可接受的 cuisine（菜系）白名单；
ALLOWED_CUISINES = {
    "chinese", "japanese", "italian", "mexican", "american"
}

# 仅支援 Manhattan
ALLOWED_LOCATIONS = {"manhattan", "new york", "nyc", "manhattan, ny"}

def _norm_key(s):
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())

def get_slot_value(slots, slot_name):
    slot = slots.get(slot_name)
    if not slot:
        return None
    try:
        return slot["value"]["interpretedValue"]
    except Exception:
        return None

def get_slot_value_fuzzy(slots, exact_names, keyword_tokens):
    # 1) exact name match (case/format insensitive)
    norm_to_key = {_norm_key(k): k for k in slots.keys()}
    for n in exact_names:
        k = norm_to_key.get(_norm_key(n))
        if k:
            v = get_slot_value(slots, k)
            if v:
                return v

    # 2) fallback by keyword hit in slot key
    for raw_key in slots.keys():
        nk = _norm_key(raw_key)
        if any(tok in nk for tok in keyword_tokens):
            v = get_slot_value(slots, raw_key)
            if v:
                return v
    return None

def close(intent_name, message, state="Fulfilled", slots=None, session_attributes=None):
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {
                "name": intent_name,
                "state": state,
                "slots": slots or {}
            },
            "sessionAttributes": session_attributes or {}
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

def delegate(intent_name, slots, session_attributes=None):
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": {
                "name": intent_name,
                "state": "InProgress",
                "slots": slots
            },
            "sessionAttributes": session_attributes or {}
        }
    }

def elicit_slot(intent_name, slots, slot_to_elicit, message, session_attributes=None):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": slot_to_elicit
            },
            "intent": {
                "name": intent_name,
                "state": "InProgress",
                "slots": slots
            },
            "sessionAttributes": session_attributes or {}
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

def is_valid_email(email):
    if not email:
        return False
    return re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email) is not None


def get_user_id_from_session_attributes(session_attributes):
    if not session_attributes:
        return None
    candidate = (
        session_attributes.get("userId")
        or session_attributes.get("userid")
        or session_attributes.get("user_id")
    )
    if candidate is None:
        return None
    val = str(candidate).strip()
    return val or None


def save_user_last_search(user_id, location, cuisine, email):
    if not (user_state_table and user_id and cuisine):
        return

    item = {
        "UserId": str(user_id).strip(),
        "LastLocation": str(location or "manhattan").strip().lower(),
        "LastCuisine": str(cuisine).strip().lower(),
        "UpdatedAt": datetime.now(timezone.utc).isoformat(),
    }
    if email:
        item["LastEmail"] = str(email).strip().lower()

    user_state_table.put_item(Item=item)
    print("LF1 saved user state:", json.dumps({"UserId": item["UserId"], "LastCuisine": item["LastCuisine"], "LastLocation": item["LastLocation"]}))


def send_sqs_request(location, cuisine, dining_date, dining_time, people, email, user_id=None):
    message_body = {
        "location": location,
        "cuisine": cuisine.lower(),
        "date": dining_date,
        "time": dining_time,
        "people": str(people),
        "email": email
    }
    if user_id:
        message_body["userId"] = str(user_id)
    resp = sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(message_body)
    )
    return resp.get("MessageId")

def lambda_handler(event, context):
    print("Lex event:", json.dumps(event))

    intent = event["sessionState"]["intent"]
    intent_name = intent["name"]
    slots = intent.get("slots") or {}
    session_attributes = (event.get("sessionState", {}) or {}).get("sessionAttributes") or {}
    user_id = get_user_id_from_session_attributes(session_attributes)
    invocation_source = event.get("invocationSource")  # DialogCodeHook / FulfillmentCodeHook

    # --- 1) GreetingIntent ---
    if intent_name == "GreetingIntent":
        return close(intent_name, "Hi there, how can I help?", slots=slots)

    # --- 2) ThankYouIntent ---
    if intent_name == "ThankYouIntent":
        return close(intent_name, "You’re welcome.", slots=slots)

    # --- 3) DiningSuggestionsIntent ---
    if intent_name == "DiningSuggestionsIntent":
        # 尽量容错：支持不同 slot 命名
        location = get_slot_value_fuzzy(
            slots, ["Location", "DiningLocation", "City", "Area"], ["location", "city", "area"]
        )
        cuisine = get_slot_value_fuzzy(
            slots, ["Cuisine", "DiningCuisine", "FoodType"], ["cuisine", "food"]
        )
        dining_date = get_slot_value_fuzzy(
            slots, ["DiningDate", "Date"], ["date"]
        )
        dining_time = get_slot_value_fuzzy(
            slots, ["DiningTime", "Time"], ["time"]
        )
        people = get_slot_value_fuzzy(
            slots, ["NumberOfPeople", "PeopleCount", "PartySize"], ["people", "party", "count", "number"]
        )
        email = get_slot_value_fuzzy(
            slots, ["Email", "email", "EmailAddress"], ["email", "mail"]
        )

        print(
            "LF1 extracted slots:",
            json.dumps(
                {
                    "invocationSource": invocation_source,
                    "slotKeys": list(slots.keys()),
                    "location": location,
                    "cuisine": cuisine,
                    "dining_date": dining_date,
                    "dining_time": dining_time,
                    "people": people,
                    "email": email,
                    "user_id": user_id,
                },
                ensure_ascii=False,
            ),
        )

        # ---------- DialogCodeHook: 参数校验（validation） ----------
        if invocation_source == "DialogCodeHook":
            # location 校验（只接受 Manhattan）
            if location and location.strip().lower() not in ALLOWED_LOCATIONS:
                return elicit_slot(
                    intent_name,
                    slots,
                    "Location" if "Location" in slots else "DiningLocation",
                    f"Sorry, I can't fulfill requests for {location}. Please enter a valid location in Manhattan.",
                    session_attributes
                )

            # cuisine 校验
            if cuisine and cuisine.strip().lower() not in ALLOWED_CUISINES:
                return elicit_slot(
                    intent_name,
                    slots,
                    "Cuisine" if "Cuisine" in slots else "DiningCuisine",
                    "Sorry, I currently support cuisines like chinese, japanese, italian, mexican, american. What cuisine would you like?",
                    session_attributes
                )

            # email 校验
            if email and not is_valid_email(email):
                return elicit_slot(
                    intent_name,
                    slots,
                    "Email" if "Email" in slots else "email",
                    "That email address looks invalid. Please provide a valid email address.",
                    session_attributes
                )

            # 未填满时交回给 Lex 自己继续问（Delegate）
            required_values = [location, cuisine, dining_date, dining_time, people, email]
            if not all(required_values):
                return delegate(intent_name, slots, session_attributes)

            # 参数已填满：无论 FulfillmentCodeHook 是否配置，直接写 SQS，避免“队列一直为空”
            # 用 session attribute 防止重复入队。
            if session_attributes.get("requestEnqueued") != "1":
                msg_id = send_sqs_request(location, cuisine, dining_date, dining_time, people, email, user_id=user_id)
                session_attributes["requestEnqueued"] = "1"
                print("LF1 SQS enqueue success (DialogCodeHook), MessageId=", msg_id)
                save_user_last_search(user_id, location, cuisine, email)

            return {
                "sessionState": {
                    "dialogAction": {"type": "Close"},
                    "intent": {
                        "name": intent_name,
                        "state": "Fulfilled",
                        "slots": slots
                    },
                    "sessionAttributes": session_attributes
                },
                "messages": [
                    {
                        "contentType": "PlainText",
                        "content": "You’re all set. Expect my suggestions shortly! I will notify you by email."
                    }
                ]
            }

        # ---------- FulfillmentCodeHook: 写 SQS + 确认回复 ----------
        # Lex 有时直接在 Fulfillment 阶段调用，所以这里也做一次兜底校验
        if not all([location, cuisine, dining_date, dining_time, people, email]):
            return close(
                intent_name,
                "I am missing some details for your dining request. Please try again.",
                state="Failed",
                slots=slots,
                session_attributes=session_attributes
            )

        if session_attributes.get("requestEnqueued") != "1":
            msg_id = send_sqs_request(location, cuisine, dining_date, dining_time, people, email, user_id=user_id)
            session_attributes["requestEnqueued"] = "1"
            print("LF1 SQS enqueue success (FulfillmentCodeHook), MessageId=", msg_id)
            save_user_last_search(user_id, location, cuisine, email)

        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {
                    "name": intent_name,
                    "state": "Fulfilled",
                    "slots": slots
                },
                "sessionAttributes": session_attributes
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "You’re all set. Expect my suggestions shortly! I will notify you by email."
                }
            ]
        }

    # --- fallback ---
    return close(intent_name, "Sorry, I couldn't understand that.", state="Failed", slots=slots)
