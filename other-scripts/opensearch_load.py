from opensearchpy import OpenSearch

# ---------- CONFIG ----------
OPENSEARCH_ENDPOINT = "https://search-restaurant-domain-y5iqwponuujjazqlce2t67qaue.us-east-1.es.amazonaws.com"
INDEX_NAME = "restaurants"
# ----------------------------

# Connect to OpenSearch
client = OpenSearch(
    hosts=[{'host': OPENSEARCH_ENDPOINT.replace("https://", "").replace("http://", ""), 'port': 443}],
    http_auth=('admin', 'AdminPass123!'),  # use your master user credentials
    use_ssl=True,
    verify_certs=True
)

# Create index if it doesn't exist
if not client.indices.exists(index=INDEX_NAME):
    client.indices.create(
        index=INDEX_NAME,
        body={
            "settings": {"number_of_shards": 1},
            "mappings": {
                "properties": {
                    "RestaurantID": {"type": "keyword"},
                    "Cuisine": {"type": "keyword"}
                }
            }
        }
    )
    print(f"✅ Index '{INDEX_NAME}' created.")
else:
    print(f"Index '{INDEX_NAME}' already exists.")