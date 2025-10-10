import json, boto3, requests, os, random
# from elasticsearch import Elasticsearch, RequestsHttpConnection
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key

SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
# OPENSEARCH_ENDPOINT = 'search-resto-domain3-vkc3rgjiapciufieswp37tqcgm.us-east-1.es.amazonaws.com'
ES_HOST = os.environ['ES_HOST']
FROM_EMAIL = os.environ['FROM_EMAIL']

AWS_REGION = "us-east-1"

# --- AWS Authentication & Clients ---
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    AWS_REGION,
    'es',
    session_token=credentials.token
)

# AWS clients
sqs = boto3.client('sqs', region_name=AWS_REGION)
ses = boto3.client('ses', region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Opensearch client
es = OpenSearch(
    hosts=[{'host': ES_HOST, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def lambda_handler(event, context):
    
    print("--- LF2 triggered----")
    
    #  Get the message from SQS
    try:
        # print("----Receiving messages from SQS----")
        # response = sqs.receive_message(
        #     QueueUrl=SQS_QUEUE_URL,
        #     MaxNumberOfMessages=5,
        #     WaitTimeSeconds=10
        # )
        # print(f"----Received messages from SQS: {response}----")
        # messages = response.get('Messages', [])
        # print(f"Received {len(messages)} message(s) from SQS: {messages}")

        # if not messages:
        #     print("No messages in queue")
        #     return {'statusCode': 200, 'body': 'No messages'}
        
        # for message in messages:
        #     process_message(message)
        # return {'statusCode': 200, 'body': f'Processed {len(messages)} messages'}

        for record in event['Records']:
            message = json.loads(record['body'])
            print(f"Processing message: {message}")
            process_message(message)
            print(f"Processed message: {message}")

    except Exception as e:
        print(f"Error receiving messages from SQS: {e}")
        return {'statusCode': 500, 'body': str(e)}


##### -------------- FETCH RESTOS FROM ES --------------- ########

def get_restaurant_suggestions(cuisine):
    ## fetches 5 restaurants from elasticsearch
    print(f"Fetching 5 restaurants from Elasticsearch for cuisine: {cuisine}")
    query = {
        "size": 5,
        "query": {
            "function_score": {
                "query": { "match": { "Cuisine": cuisine } },
                "random_score": {}
            }
        }
    }
    try:
        response = es.search(index="restaurants", body=query)
        print(f"ES Response: {response}")
        hits = response['hits']['hits']
        if hits:
            return [hit['_source']['RestaurantID'] for hit in hits]
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
    print("No restaurants found in Elasticsearch")
    return []


##### ---------- FETCH DETAILS OF RESTOS FROM DYNAMODB ------- ########

def get_restaurants_details_batch(restaurant_ids):
    # fetch full details of the 5 restos from dynamoDB
    try:
        response = dynamodb.batch_get_item(
            RequestItems={
                DYNAMODB_TABLE_NAME: {
                    'Keys': [{'BusinessID': id} for id in restaurant_ids]
                }
            }
        )
        return response.get('Responses', {}).get(DYNAMODB_TABLE_NAME, [])
    except Exception as e:
        print(f"Error batch fetching from DynamoDB: {e}")
    return []


##### --------------- FORMAT EMAIL ----------------------- ########

def format_email_body(message_data, restaurants):

    num_people = message_data.get('NumberOfPeople', 'two')
    dining_time = message_data.get('DiningTime', 'tonight')
    cuisine = message_data.get('Cuisine', 'delicious')

    body = (
        f"Hello!\n\n"
        f"Here are my {cuisine} restaurant suggestions for {num_people} people for {dining_time}:\n\n"
    )

    for i, restaurant in enumerate(restaurants, 1):
        name = restaurant.get('Name', 'N/A')
        address = restaurant.get('Address', 'N/A')
        body += f"{i}. {name}, located at {address}\n"
    
    body += "\nEnjoy your meal!"
    return body

##### ---------------------- SEND EMAIL -----------------------##########

def send_email(user_email, subject, body):

    try:
        ses.send_email(
            Source=FROM_EMAIL, # IMPORTANT: Replace with your verified SES email
            Destination={'ToAddresses': [user_email]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        print(f"Successfully sent email to {user_email}")
        return True
    except Exception as e:
        print(f"Failed to send email to {user_email}: {e}")
        return False

##### --------------------- PROCESS MESSAGE ---------------------##########
def process_message(message):
    print("--- Message Processor triggered----")
    try:

        body = message
        print(f"Processing request: {body}")
      
        cuisine = body['Cuisine']
        location = body['Location']
        dining_time = body['DiningTime']
        num_people = body['NumberOfPeople']
        user_email = body['Email']

        if not all([cuisine, user_email, location]):
            print(f"Message missing required slots.")
            # sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        print(f"Processing request for {cuisine} food in {location} for {user_email}")

        # Search OpenSearch for restaurants with this cuisine
        restaurant_ids = get_restaurant_suggestions(cuisine)
        if not restaurant_ids:
            print(f"Could not find restaurants for cuisine: {cuisine}.")
            # sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        restaurants_details = get_restaurants_details_batch(restaurant_ids)
        if not restaurants_details:
            print(f"Could not fetch details from DynamoDB.")
            # sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        # Format and send the email
        email_subject = "Your Dining Concierge Restaurant Suggestions!"
        email_body = format_email_body(message, restaurants_details)
        email_sent = send_email(user_email, email_subject, email_body)
        
        if email_sent:
            # sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            print(f"Successfully processed and deleted message for {user_email}.")
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")