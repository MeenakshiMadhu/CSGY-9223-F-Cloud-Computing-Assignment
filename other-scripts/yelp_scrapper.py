import requests
import time
import json

# ---------- CONFIGURATION ----------
YELP_API_KEY = "7lFsN42TdO5Nl9T43uGqhIDCEVzGKJGvydylVcNKE2kWwZ6JF5zLEBqpgHmNbSnJplKhgNbZ6AOeUHN-XkKhyJkNx39Q4cZe826q74LgpJWkXMcYbDsZZKqLM07kaHYx"
HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}
LOCATION = "Manhattan, NY"
CUISINES = ["Japanese", "Mediterranean", "Indian", "American", "Mexican", "Korean", "Chinese", "Italian", "Thai", "Caribbean"]
RESTAURANTS_PER_CUISINE = 200
OUTPUT_FILE = "yelp_manhattan_restaurants.json"
# -----------------------------------

def fetch_yelp_restaurants(cuisine, offset=0, limit=50):
    """Fetch businesses from Yelp API"""
    url = "https://api.yelp.com/v3/businesses/search"
    params = {
        "term": f"{cuisine} restaurants",
        "location": LOCATION,
        "limit": limit,
        "offset": offset
    }
    response = requests.get(url, headers=HEADERS, params=params)
    print(f"Status code: {response.status_code}")
    if response.status_code != 200:
        print(f"Error fetching {cuisine} (offset={offset}): {response.text}")
        return []
    return response.json().get("businesses", [])
    print(f"Fetched {len(data)} businesses for {cuisine} (offset={offset})")  # <--- debug
    return data

def main():
    all_restaurants = []

    for cuisine in CUISINES:
        print(f"\nFetching {cuisine} restaurants...")
        seen_ids = set()
        offset = 0
        count = 0

        while count < RESTAURANTS_PER_CUISINE:
            remaining = RESTAURANTS_PER_CUISINE - count
            limit = min(50, remaining)  # Yelp max limit per request is 50
            results = fetch_yelp_restaurants(cuisine, offset=offset, limit=limit)

            if not results:
                print(f"No more results for {cuisine}.")
                break

            for biz in results:
                if biz["id"] in seen_ids:
                    continue
                full_address = ", ".join(biz["location"].get("display_address", []))
                if "New York, NY" not in full_address:
                    continue  # Skip if not Manhattan

                restaurant_data = {
                    "BusinessID": biz["id"],
                    "Name": biz.get("name", ""),
                    "Address": ", ".join(biz["location"].get("display_address", [])),
                    "Coordinates": {
                        "lat": biz["coordinates"].get("latitude"),
                        "lon": biz["coordinates"].get("longitude")
                    },
                    "NumReviews": biz.get("review_count", 0),
                    "Rating": biz.get("rating", 0),
                    "ZipCode": biz["location"].get("zip_code", ""),
                    "Cuisine": cuisine
                }
                all_restaurants.append(restaurant_data)
                seen_ids.add(biz["id"])
                count += 1
                if count >= RESTAURANTS_PER_CUISINE:
                    break

            offset += limit
            time.sleep(1)  # avoid hitting rate limit

        print(f"Collected {count} {cuisine} restaurants.")

    # Write output to JSON file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_restaurants, f, ensure_ascii=False, indent=2)

    print(f"\nAll data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()