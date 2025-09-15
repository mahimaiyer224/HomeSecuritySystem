import json
import boto3
from datetime import datetime
import uuid
import os

dynamodb = boto3.client('dynamodb')
eventbridge = boto3.client('events')

table_name = os.environ.get("DOORBELL_TABLE_NAME", "DoorbellEvents")
event_bus_name = os.environ.get("EVENT_BUS_NAME", "DoorbellEventBus")

def lambda_handler(event, context):
    print("[DoorbellLambda] Raw Event Body:", event.get("body"))
    
    try:
        body = json.loads(event.get("body", "{}"))
        house_id = body.get("houseId", "Unknown House")
        print(f"[DoorbellLambda] Parsed houseId: {house_id}")
    except json.JSONDecodeError:
        print("[DoorbellLambda] JSON decoding failed.")
        return {
            "statusCode": 400,
            "body": json.dumps({
                "status": "failure",
                "message": "Invalid JSON format"
            })
        }

    valid_houses = ["H1", "H2", "H3", "H4"]
    if house_id not in valid_houses:
        print(f"[DoorbellLambda] Invalid houseId received: {house_id}")
        return {
            "statusCode": 400,
            "body": json.dumps({
                "status": "failure",
                "message": f"Invalid houseId: {house_id}"
            })
        }

    event_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()

    print(f"[DoorbellLambda] Generated EventID: {event_id}, Timestamp: {timestamp}")

    doorbell_event = {
        "EventID": {"S": event_id},
        "Timestamp": {"S": timestamp},
        "HouseID": {"S": house_id},
        "source": {"S": "UI"}
    }

    try:
        # ✅ Store event in DynamoDB
        dynamodb.put_item(TableName=table_name, Item=doorbell_event)
        print(f"[DoorbellLambda] Event stored in DynamoDB table: {table_name}")

        # ✅ Trigger UploadAPI asynchronously via EventBridge
        result = eventbridge.put_events(
            Entries=[
                {
                    "Source": "doorbell.lambda",
                    "DetailType": "DoorbellTriggered",
                    "Detail": json.dumps({
                        "eventId": event_id,
                        "houseId": house_id
                    }),
                    "EventBusName": event_bus_name
                }
            ]
        )
        print(f"[DoorbellLambda] EventBridge triggered. Result: {result}")

        response = {
            "status": "success",
            "message": f"Doorbell event logged for House {house_id} and UploadAPI triggered"
        }

    except Exception as e:
        print(f"[DoorbellLambda] Error logging event or triggering EventBridge: {str(e)}")
        response = {
            "status": "failure",
            "message": f"Failed to log event: {str(e)}"
        }

    return {
        "statusCode": 200 if response["status"] == "success" else 500,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response)
    }
