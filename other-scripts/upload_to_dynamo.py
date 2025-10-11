import boto3
import json
from decimal import Decimal
from datetime import datetime

# --- Configuration ---
TABLE_NAME = 'yelp-restaurants2'
JSON_FILE_PATH = 'yelp_manhattan_restaurants.json'

# Initialize the DynamoDB resource using boto3
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)

def convert_floats_to_decimal(obj):
    # recursively converts all float values to Decimal
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = convert_floats_to_decimal(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = convert_floats_to_decimal(v)
        return obj
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj


def upload_data():
    # Open and load the JSON file
    try:
        with open(JSON_FILE_PATH, 'r') as json_file:
            restaurants = json.load(json_file)
    except FileNotFoundError:
        print(f"Error: The file at {JSON_FILE_PATH} was not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from the file. Please check its format.")
        return

    print(f"Found {len(restaurants)} restaurants to upload.")
    processed_ids = set()

    # batch write to DynamoDB
    with table.batch_writer() as batch:
        for i, restaurant in enumerate(restaurants):
            business_id = restaurant.get('BusinessID')
            
            # Check 1: Ensure the business ID exists
            if not business_id:
                print(f"WARNING: Skipping record #{i+1} because it is missing an 'id'.")
                continue
            
            # Check 2: Ensure the business ID is not a duplicate
            if business_id in processed_ids:
                print(f"WARNING: Skipping duplicate BusinessID '{business_id}' for restaurant '{restaurant.get('name')}'.")
                continue

            item = {
                'BusinessID': business_id,
                'Name': restaurant.get('Name'),
                'Address': restaurant.get('Address'),
                'Coordinates': restaurant.get('Coordinates'),
                'NumberOfReviews': restaurant.get('NumReviews'),
                'Rating': restaurant.get('Rating'),
                'ZipCode': restaurant.get('ZipCode'),
                'Cuisine': restaurant.get('Cuisine'),
                'insertedAtTimestamp': datetime.utcnow().isoformat()
            }
            
            # Remove keys with None or empty values to avoid DynamoDB errors
            item_cleaned = {k: v for k, v in item.items() if v is not None and v != ''}
            
            # Convert all floats in the item to Decimals
            item_decimal = convert_floats_to_decimal(item_cleaned)
            
            # Add the item to the batch
            batch.put_item(Item=item_decimal)
            processed_ids.add(business_id)
            print(f"Adding {item.get('Name')} to the batch...")

    print(f"\nUpload complete! Successfully processed and uploaded {len(processed_ids)} unique restaurants.")

if __name__ == '__main__':
    upload_data()