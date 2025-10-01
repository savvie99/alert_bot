import requests
import datetime
import time

# --- CONFIGURATION --- 
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN") 
SHOP_NAME = "doggyvers-f-c" 
API_VERSION = "2025-10" 
# Map Location ID to Slack webhook (fetched from ENV) 
LOCATION_SLACK_MAP = { 
    68029743337: os.getenv("SLACK_WEBHOOK_WILLIAM"), 
    104345567569: os.getenv("SLACK_WEBHOOK_ZENVENTORY"), }


headers = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# --- DATE RANGE: from 31 to 2 days ago ---
today = datetime.datetime.utcnow()
created_at_min = (today - datetime.timedelta(days=31)).isoformat() + "Z"
created_at_max = (today - datetime.timedelta(days=2)).isoformat() + "Z"

# --- STEP 1: Get All Locations ---
locations_url = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/locations.json"
response = requests.get(locations_url, headers=headers)
if response.status_code != 200:
    print(f"❌ Failed to fetch locations: {response.status_code}, {response.text}")
    exit()

locations = response.json().get("locations", [])

# --- STEP 2: Only keep William and Zenventory ---
target_location_ids = set(LOCATION_SLACK_MAP.keys())
locations = [loc for loc in locations if loc["id"] in target_location_ids]

# --- STEP 3: Loop through selected locations and check unfulfilled orders ---
for loc in locations:
    location_id = loc["id"]
    location_name = loc["name"]

    print(f"\n🔎 Checking location: {location_name} (ID: {location_id})")

    # Fetch orders for this location
    orders_url = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/orders.json"
    params = {
        "status": "open",
        "fulfillment_status": "unfulfilled",
        "reference_location_id": location_id,
        "created_at_min": created_at_min,
        "created_at_max": created_at_max,
        "limit": 250,
        "fields": "id,name,created_at"
    }

    all_orders = []
    url = orders_url

    while url:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            print("⚠️ Rate limit hit. Sleeping 2 seconds...")
            time.sleep(2)
            continue
        elif response.status_code != 200:
            print(f"❌ Failed to fetch orders: {response.status_code}, {response.text}")
            break

        data = response.json()
        all_orders.extend(data.get("orders", []))

        # Check pagination
        link_header = response.headers.get("Link", "")
        if 'rel="next"' in link_header:
            next_url = link_header.split(";")[0].strip("<> ")
            url = next_url
            params = {}  # Clear params for paginated request
        else:
            url = None

        time.sleep(0.6)  # To respect Shopify rate limits

    # --- STEP 4: Build message ---
    if all_orders:
        order_names = [o["name"] for o in all_orders]
        order_list = "\n".join(order_names)
        message = f"🚨 *Unfulfilled orders* (from {created_at_min[:10]} to {created_at_max[:10]}) for *{location_name}*:\n{order_list}"
    else:
        message = f"✅ No unfulfilled orders between {created_at_min[:10]} and {created_at_max[:10]} for *{location_name}*."

    # --- STEP 5: Send to Slack ---
    slack_url = LOCATION_SLACK_MAP.get(location_id)
    if slack_url:
        slack_payload = {"text": message}
        slack_response = requests.post(slack_url, json=slack_payload)

        if slack_response.status_code == 200:
            print(f"✅ Slack message sent to {location_name}")
        else:
            print(f"❌ Failed to send Slack message to {location_name}: {slack_response.status_code}, {slack_response.text}")
    else:
        print(f"⚠️ No Slack webhook configured for location '{location_name}' (ID: {location_id})")
        print("--- Message Preview ---")
        print(message)
