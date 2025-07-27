import requests
import datetime
import time
from collections import defaultdict

# --- CONFIGURATION ---
SHOPIFY_ACCESS_TOKEN = "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"
SHOP_NAME = "doggyvers-f-c"
API_VERSION = "2024-07"

LOCATION_NAMES = {
    68029743337: "William",
    104345567569: "Zenventory"
}
FULFILLMENT_DELAY_DAYS = {
    68029743337: 10,
    104345567569: 5
}

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

headers = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# --- DATE RANGE: last 3 weeks ---
today = datetime.datetime.now(datetime.timezone.utc)
created_after = (today - datetime.timedelta(days=21)).isoformat()

# --- STORAGE ---
delayed_orders = defaultdict(list)

def parse_iso(dt):
    return datetime.datetime.fromisoformat(dt.replace("Z", "+00:00"))

print("🔍 Fetching fulfilled orders from the last 3 weeks...\n")

orders_url = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/orders.json"
params = {
    "status": "any",
    "fulfillment_status": "fulfilled",
    "created_at_min": created_after,
    "limit": 250,
    "fields": "id,name,fulfillments"
}

url = orders_url
while url:
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 429:
        print("⚠️ Rate limited. Sleeping 2s...")
        time.sleep(2)
        continue
    elif resp.status_code != 200:
        print(f"❌ API Error: {resp.status_code} — {resp.text}")
        break

    orders = resp.json().get("orders", [])
    for order in orders:
        order_name = order["name"]
        for f in order.get("fulfillments", []):
            loc_id = f.get("location_id")
            fulfilled_at = f.get("created_at")
            shipment_status = f.get("shipment_status")

            if not fulfilled_at or loc_id not in LOCATION_NAMES:
                continue
            if shipment_status == "delivered":
                continue

            days_since_fulfilled = (today - parse_iso(fulfilled_at)).days
            if days_since_fulfilled > FULFILLMENT_DELAY_DAYS[loc_id]:
                loc_name = LOCATION_NAMES[loc_id]
                delayed_orders[loc_name].append((order_name, days_since_fulfilled))

    # Pagination
    link = resp.headers.get("Link", "")
    if 'rel="next"' in link:
        url = link.split(";")[0].strip("<> ")
        params = {}
    else:
        url = None

    time.sleep(0.2)

# --- Compile Slack Message ---
message_lines = ["📦 *Delayed Undelivered Orders (Last 3 Weeks)*\n"]
for loc_id, loc_name in LOCATION_NAMES.items():
    orders = delayed_orders.get(loc_name, [])
    message_lines.append(f"*📍 {loc_name}*")
    if orders:
        for name, days in orders:
            message_lines.append(f"   🔴 Order {name} — Fulfilled {days} days ago, still not delivered")
    else:
        message_lines.append("   ✅ No delayed undelivered orders")
    message_lines.append("")  # Blank line between sections

message = "\n".join(message_lines)

# --- Send to Slack ---
slack_response = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
if slack_response.status_code == 200:
    print("✅ Slack message sent successfully")
else:
    print(f"❌ Failed to send Slack message: {slack_response.status_code}, {slack_response.text}")
