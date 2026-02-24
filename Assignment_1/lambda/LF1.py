import json
import os
import re
import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

sqs = boto3.client("sqs", region_name=REGION)

# 可接受的 cuisine（菜系）白名单；
ALLOWED_CUISINES = {
    "chinese", "japanese", "italian", "mexican", "american"
}

# 仅支援 Manhattan
ALLOWED_LOCATIONS = {"manhattan", "new york", "nyc", "manhattan, ny"}

def get_slot_value(slots, slot_name):
    slot = slots.get(slot_name)
    if not slot:
        return None
    try:
        return slot["value"]["interpretedValue"]
    except Exception:
        return None

def close(intent_name, message, state="Fulfilled", slots=None):
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {
                "name": intent_name,
                "state": state,
                "slots": slots or {}
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

def delegate(intent_name, slots):
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": {
                "name": intent_name,
                "state": "InProgress",
                "slots": slots
            }
        }
    }

def elicit_slot(intent_name, slots, slot_to_elicit, message):
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
            }
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

def lambda_handler(event, context):
    print("Lex event:", json.dumps(event))

    intent = event["sessionState"]["intent"]
    intent_name = intent["name"]
    slots = intent.get("slots") or {}
    invocation_source = event.get("invocationSource")  # DialogCodeHook / FulfillmentCodeHook

    # --- 1) GreetingIntent ---
    if intent_name == "GreetingIntent":
        return close(intent_name, "Hi there, how can I help?", slots=slots)

    # --- 2) ThankYouIntent ---
    if intent_name == "ThankYouIntent":
        return close(intent_name, "You’re welcome.", slots=slots)

    # --- 3) DiningSuggestionsIntent ---
    if intent_name == "DiningSuggestionsIntent":
        # 这些 slot 名与在 Lex console 中建立的名字保持一致
        location = (
            get_slot_value(slots, "Location")
            or get_slot_value(slots, "DiningLocation")
        )
        cuisine = (
            get_slot_value(slots, "Cuisine")
            or get_slot_value(slots, "DiningCuisine")
        )
        dining_date = (
            get_slot_value(slots, "DiningDate")
            or get_slot_value(slots, "Date")
        )
        dining_time = (
            get_slot_value(slots, "DiningTime")
            or get_slot_value(slots, "Time")
        )
        people = (
            get_slot_value(slots, "NumberOfPeople")
            or get_slot_value(slots, "PeopleCount")
        )
        email = (
            get_slot_value(slots, "Email")
            or get_slot_value(slots, "email")
        )

        # ---------- DialogCodeHook: 参数校验（validation） ----------
        if invocation_source == "DialogCodeHook":
            # location 校验（只接受 Manhattan）
            if location and location.strip().lower() not in ALLOWED_LOCATIONS:
                return elicit_slot(
                    intent_name,
                    slots,
                    "Location" if "Location" in slots else "DiningLocation",
                    f"Sorry, I can't fulfill requests for {location}. Please enter a valid location in Manhattan."
                )

            # cuisine 校验
            if cuisine and cuisine.strip().lower() not in ALLOWED_CUISINES:
                return elicit_slot(
                    intent_name,
                    slots,
                    "Cuisine" if "Cuisine" in slots else "DiningCuisine",
                    "Sorry, I currently support cuisines like chinese, japanese, italian, mexican, american. What cuisine would you like?"
                )

            # email 校验
            if email and not is_valid_email(email):
                return elicit_slot(
                    intent_name,
                    slots,
                    "Email" if "Email" in slots else "email",
                    "That email address looks invalid. Please provide a valid email address."
                )

            # 未填满时交回给 Lex 自己继续问（Delegate）
            required_values = [location, cuisine, dining_date, dining_time, people, email]
            if not all(required_values):
                return delegate(intent_name, slots)

            # 如果已经填满，通常 Lex 会继续走 FulfillmentCodeHook（视你配置）
            return delegate(intent_name, slots)

        # ---------- FulfillmentCodeHook: 写 SQS + 确认回复 ----------
        # Lex 有时直接在 Fulfillment 阶段调用，所以这里也做一次兜底校验
        if not all([location, cuisine, dining_date, dining_time, people, email]):
            return close(
                intent_name,
                "I am missing some details for your dining request. Please try again.",
                state="Failed",
                slots=slots
            )

        message_body = {
            "location": location,
            "cuisine": cuisine.lower(),
            "date": dining_date,
            "time": dining_time,
            "people": str(people),
            "email": email
        }

        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )

        return close(
            intent_name,
            "You’re all set. Expect my suggestions shortly! I will notify you by email.",
            slots=slots
        )

    # --- fallback ---
    return close(intent_name, "Sorry, I couldn't understand that.", state="Failed", slots=slots)