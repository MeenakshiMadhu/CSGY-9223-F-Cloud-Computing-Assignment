from opensearchpy import OpenSearch

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
    print(f"Index '{INDEX_NAME}' created.")
else:
    print(f"Index '{INDEX_NAME}' already exists.")