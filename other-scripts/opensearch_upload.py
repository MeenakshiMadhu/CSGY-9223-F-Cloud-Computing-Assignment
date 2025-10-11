import json
from opensearchpy import OpenSearch

# Connect as above...
# ---------- CONFIG ----------
OPENSEARCH_ENDPOINT = "https://search-resto-domain4-dpfpo5upneffztgu4sw6xom5ke.us-east-1.es.amazonaws.com"
INDEX_NAME = "restaurants2"
# ----------------------------

# Connect to OpenSearch
client = OpenSearch(
    hosts=[{'host': OPENSEARCH_ENDPOINT.replace("https://", "").replace("http://", ""), 'port': 443}],
    http_auth=('master-resto', 'Resto123!'),  # use your master user credentials
    use_ssl=True,
    verify_certs=True
)
# Load data
with open("yelp_manhattan_restaurants.json", "r") as f:
    restaurants = json.load(f)

# Upload partial info (RestaurantID + Cuisine)
for r in restaurants:
    doc = {
        "RestaurantID": r["BusinessID"],
        "Cuisine": r["Cuisine"]
    }
    client.index(index=INDEX_NAME, id=r["BusinessID"], body=doc)

print("Uploaded all restaurant IDs and cuisines to OpenSearch.")