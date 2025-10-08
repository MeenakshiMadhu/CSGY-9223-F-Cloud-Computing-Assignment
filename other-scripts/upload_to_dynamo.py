import boto3
import json
from decimal import Decimal
from datetime import datetime

# --- Configuration ---
TABLE_NAME = 'yelp-restaurants'
JSON_FILE_PATH = 'path/to/your/restaurants.json'

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

   # batch write to DynamoDB
    with table.batch_writer() as batch:
        for restaurant in restaurants:

            item = {
                'BusinessID': restaurant.get('id'),
                'Name': restaurant.get('name'),
                'Address': ' '.join(restaurant.get('location', {}).get('display_address', [])),
                'Coordinates': restaurant.get('coordinates'),
                'NumberOfReviews': restaurant.get('review_count'),
                'Rating': restaurant.get('rating'),
                'ZipCode': restaurant.get('location', {}).get('zip_code'),
                'insertedAtTimestamp': datetime.utcnow().isoformat()
            }
            
            # Remove keys with None or empty values to avoid DynamoDB errors
            item_cleaned = {k: v for k, v in item.items() if v is not None and v != ''}
            
            # Convert all floats in the item to Decimals
            item_decimal = convert_floats_to_decimal(item_cleaned)
            
            # Add the item to the batch
            batch.put_item(Item=item_decimal)
            print(f"Adding {item.get('Name')} to the batch...")

    print("\nUpload complete!")

if __name__ == '__main__':
    upload_data()