import json
import boto3
from datetime import datetime
import uuid

lex_client = boto3.client('lexv2-runtime')

BOT_ID = 'II2QWHDEAQ'
BOT_ALIAS_ID = 'HK6LD0H6HS'
LOCALE_ID = 'en_US'


def lambda_handler(event, context):
    
    print("--- EVENT --- :", json.dumps(event))

    if 'body' in event:
        try:
            request_data = json.loads(event['body'])
        except:
            request_data = event['body']
    else: 
        request_data = event
    
    # process the request
    messages = request_data.get('messages', [])   
    session_id = request_data.get('sessionId') or str(uuid.uuid4())
    
    print("--- MESSAGES --- :", json.dumps(messages))
    if not messages:
        return build_response("No messages provided", 400, session_id)
    
    # extract the user message
    user_message = ""
    if len(messages) > 0:
        user_message = messages[0].get('unstructured', {}).get('text', '')

    if not user_message:
        return build_response("Empty user message", 400, session_id)

    # # temporary response message
    # response_message = "I'm still under development. Please come back later."

    try:
        # send the user message to Lex
        lex_response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text=user_message
        )

        print("--- LEX RESPONSE --- :", json.dumps(lex_response, default=str))

        # extract the response message from Lex
        bot_message = ""
        if "messages" in lex_response and len(lex_response["messages"]) > 0:
            bot_message = lex_response["messages"][0]["content"]
        else:
            bot_message = "Sorry I didn't understand that"

        return build_response(bot_message, 200, lex_response.get('sessionId'))

    except Exception as e:
        print("Error calling Lex:", str(e))
        return build_response("Error processing request", 500, session_id)

def build_response(message, status_code, session_id):
    if status_code == 200:
        response = {
            'messages': [{'type': 'unstructured','unstructured': {'id': str(uuid.uuid4()),'text': message}}],
            'sessionId': session_id
        }
    else:
        response = {'error': message,'sessionId': session_id}

    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'
        },
        'body': json.dumps(response)
    }

def error_response(message):
    return {
        'statusCode': 400,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'
        },
        'body': json.dumps({'error': message})
    }

