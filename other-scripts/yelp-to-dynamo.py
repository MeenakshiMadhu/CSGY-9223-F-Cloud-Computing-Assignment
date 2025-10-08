import requests
import boto3
import time
from datetime import datetime

# --- Configuration ---
YELP_API_KEY = 'YOUR_YELP_API_KEY'
YELP_API_HOST = 'https://api.yelp.com/v3/businesses/search'
DYNAMODB_TABLE_NAME = 'yelp-restaurants'

# Define at least 5 cuisines to search for [cite: 64]
CUISINES = ['japanese', 'chinese', 'italian', 'mexican', 'indian']
LOCATION = 'Manhattan'
RESTAURANTS_PER_CUISINE = 200 # Target ~200 restaurants per cuisine [cite: 66]
YELP_LIMIT_PER_REQUEST = 50 # Yelp API allows a max of 50 results per call

# Initialize Boto3 client for DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# --- Main Script Logic ---
def scrape_and_store():
    scraped_business_ids = set()

    for cuisine in CUISINES:
        print(f"--- Scraping restaurants for cuisine: {cuisine} ---")
        offset = 0
        while offset < RESTAURANTS_PER_CUISINE:
            try:
                # Set up parameters for the Yelp API request
                params = {
                    'term': f'{cuisine} food',
                    'location': LOCATION,
                    'limit': YELP_LIMIT_PER_REQUEST,
                    'offset': offset
                }
                headers = {
                    'Authorization': f'Bearer {YELP_API_KEY}'
                }

                # Make the API call
                response = requests.get(YELP_API_HOST, headers=headers, params=params)
                response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
                
                businesses = response.json().get('businesses', [])
                if not businesses:
                    print("No more businesses found for this cuisine.")
                    break

                for biz in businesses:
                    # Avoid duplicates [cite: 67]
                    if biz['id'] in scraped_business_ids:
                        continue
                    
                    scraped_business_ids.add(biz['id'])
                    
                    # Prepare the item for DynamoDB 
                    item = {
                        'BusinessID': biz.get('id'),
                        'Name': biz.get('name'),
                        'Cuisine': cuisine, # Add cuisine for easier searching later
                        'Address': ' '.join(biz.get('location', {}).get('display_address', [])),
                        'Coordinates': biz.get('coordinates'),
                        'NumberOfReviews': biz.get('review_count'),
                        'Rating': str(biz.get('rating', 'N/A')), # DynamoDB prefers strings for numbers
                        'ZipCode': biz.get('location', {}).get('zip_code'),
                        'insertedAtTimestamp': datetime.utcnow().isoformat() # Add timestamp [cite: 71]
                    }
                    
                    # Store the restaurant in DynamoDB [cite: 70]
                    table.put_item(Item=item)
                    print(f"Stored: {item['Name']} ({item['BusinessID']})")

                offset += len(businesses)
                time.sleep(0.2) # Be polite to the API and avoid rate limiting

            except requests.exceptions.RequestException as e:
                print(f"Error calling Yelp API: {e}")
                break
            except Exception as e:
                print(f"An error occurred: {e}")
                break

if __name__ == '__main__':
    scrape_and_store()
    print("\n--- Data scraping and storing complete! ---")