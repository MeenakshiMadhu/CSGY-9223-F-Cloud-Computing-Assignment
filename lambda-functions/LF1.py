import json, boto3
from datetime import datetime

sqs = boto3.client('sqs')
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/637695533064/DiningConciergeSuggestions"

def lambda_handler(event, context):
    intent_name = event['sessionState']['intent']['name']

    if intent_name == "GreetingIntent":
        return close_intent(event, "Hi there! How can I help you today?")

    elif intent_name == "ThankYouIntent":
        return close_intent(event, "You’re welcome!!")

    elif intent_name == "DiningSuggestionsIntent":
        return handle_dining_suggestion(event)
    else:
        return close_intent(event, "Sorry, I didn’t get that.")

def handle_dining_suggestion(event):
    intent = event['sessionState']['intent']
    slots = intent['slots']

    print(f"--- DiningSuggestionsIntent STATE: {intent['state']} ---")

    # if all slots are filled   
    if intent['state'] == 'ReadyForFulfillment':
        print("--- FULFILLED BLOCK REACHED ---")

        location = slots['Location']['value']['interpretedValue']
        cuisine = slots['Cuisine']['value']['interpretedValue']
        dine_time = slots['DiningTime']['value']['interpretedValue']
        num_people = slots['NumberOfPeople']['value']['interpretedValue']
        email = slots['Email']['value']['interpretedValue']

        # message to add to SQS queue
        message = {
            "Location": location,
            "Cuisine": cuisine,
            "DiningTime": dine_time,
            "NumberOfPeople": num_people,
            "Email": email
        }

        # push to SQS
        try:
            print("--- ATTEMPTING TO SEND TO SQS ---")
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message)
            )
            print("--- MESSAGE SENT TO SQS ---", message)
        except Exception as e:
            print(f"--- SQS SEND ERROR: {str(e)} ---")
            return close_intent(event, "Sorry, I couldn't process your request. Please try again.")

        return close_intent(event, f"You're all set! Expect my {cuisine} restaurant suggestions for {num_people} in {location} at {dine_time} shortly.")

    # if not all slots are filled, continue to delegate to Lex
    else:
        return delegate(event)

def delegate(event):
    print("--- DELEGATE BLOCK REACHED ---")
    print("--- SLOTS---- : ", event["sessionState"]["intent"]["slots"])
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": {
                "name": event["sessionState"]["intent"]["name"],
                "slots": event["sessionState"]["intent"]["slots"]
            }
        }
    }

def close_intent(event, message):
    print("--- CLOSE INTENT CALLED with message: '{}' ---".format(message))
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {
                "name": event["sessionState"]["intent"]["name"],
                "state": "Fulfilled"
                }
        },
        "messages": [{"contentType": "PlainText", "content": message}]
    }