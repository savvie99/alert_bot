{\rtf1\ansi\ansicpg1252\cocoartf2822
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fmodern\fcharset0 Courier;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;}
{\*\expandedcolortbl;;\cssrgb\c0\c0\c0;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs30 \cf0 \expnd0\expndtw0\kerning0
import requests\
import datetime\
import time\
import os\
\
# --- CONFIGURATION ---\
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")\
SHOP_NAME = "doggyvers-f-c"\
API_VERSION = "2024-07"\
\
# Map Location ID to Slack webhook (fetched from ENV)\
LOCATION_SLACK_MAP = \{\
    68029743337: os.getenv("SLACK_WEBHOOK_WILLIAM"),\
    104345567569: os.getenv("SLACK_WEBHOOK_ZENVENTORY"),\
\}\
\
headers = \{\
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,\
    "Content-Type": "application/json"\
\}\
\
# --- DATE RANGE: from 31 to 2 days ago ---\
today = datetime.datetime.utcnow()\
created_at_min = (today - datetime.timedelta(days=31)).isoformat() + "Z"\
created_at_max = (today - datetime.timedelta(days=2)).isoformat() + "Z"\
\
# --- STEP 1: Get Locations ---\
locations_url = f"https://\{SHOP_NAME\}.myshopify.com/admin/api/\{API_VERSION\}/locations.json"\
response = requests.get(locations_url, headers=headers)\
if response.status_code != 200:\
    print(f"\uc0\u10060  Failed to fetch locations: \{response.status_code\}, \{response.text\}")\
    exit()\
\
locations = response.json().get("locations", [])\
target_location_ids = set(LOCATION_SLACK_MAP.keys())\
locations = [loc for loc in locations if loc["id"] in target_location_ids]\
\
# --- STEP 2: Check orders per location ---\
for loc in locations:\
    location_id = loc["id"]\
    location_name = loc["name"]\
\
    print(f"\\n\uc0\u55357 \u56590  Checking location: \{location_name\} (ID: \{location_id\})")\
\
    orders_url = f"https://\{SHOP_NAME\}.myshopify.com/admin/api/\{API_VERSION\}/orders.json"\
    params = \{\
        "status": "open",\
        "fulfillment_status": "unfulfilled",\
        "reference_location_id": location_id,\
        "created_at_min": created_at_min,\
        "created_at_max": created_at_max,\
        "limit": 250,\
        "fields": "id,name,created_at"\
    \}\
\
    all_orders = []\
    url = orders_url\
\
    while url:\
        response = requests.get(url, headers=headers, params=params)\
        if response.status_code == 429:\
            print("\uc0\u9888 \u65039  Rate limit hit. Sleeping...")\
            time.sleep(2)\
            continue\
        elif response.status_code != 200:\
            print(f"\uc0\u10060  Failed to fetch orders: \{response.status_code\}, \{response.text\}")\
            break\
\
        all_orders.extend(response.json().get("orders", []))\
\
        # Pagination\
        link = response.headers.get("Link", "")\
        if 'rel="next"' in link:\
            url = link.split(";")[0].strip("<> ")\
            params = \{\}\
        else:\
            url = None\
\
        time.sleep(0.6)\
\
    # --- STEP 3: Build message ---\
    if all_orders:\
        order_list = "\\n".join([o["name"] for o in all_orders])\
        message = f"\uc0\u55357 \u57000  *Unfulfilled orders* (from \{created_at_min[:10]\} to \{created_at_max[:10]\}) for *\{location_name\}*:\\n\{order_list\}"\
    else:\
        message = f"\uc0\u9989  No unfulfilled orders between \{created_at_min[:10]\} and \{created_at_max[:10]\} for *\{location_name\}*."\
\
    # --- STEP 4: Send to Slack ---\
    slack_url = LOCATION_SLACK_MAP.get(location_id)\
    if slack_url:\
        slack_response = requests.post(slack_url, json=\{"text": message\})\
        if slack_response.status_code == 200:\
            print(f"\uc0\u9989  Sent to Slack (\{location_name\})")\
        else:\
            print(f"\uc0\u10060  Slack error: \{slack_response.status_code\} - \{slack_response.text\}")\
    else:\
        print(f"\uc0\u9888 \u65039  No webhook for location \{location_name\}")\
        print(message)}