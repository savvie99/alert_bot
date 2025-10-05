import os
import time
import datetime
import requests
from collections import defaultdict

# ========= CONFIG =========
SHOP_NAME = "doggyvers-f-c"
API_VERSION = "2025-10"  # change to a supported version if needed (e.g., 2025-07)
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_REPORT")  # <-- add this secret

# Windows (days)
REFUND_WINDOW_DAYS = int(os.getenv("REFUND_WINDOW_DAYS", "90"))   # for refund rate
LTV_WINDOW_DAYS    = int(os.getenv("LTV_WINDOW_DAYS", "365"))     # for LTV

# Filters
EXCLUDE_CANCELLED = True
INCLUDE_TEST_ORDERS = False

# ========= SAFETY CHECKS =========
missing = [k for k, v in {
    "SHOPIFY_ACCESS_TOKEN": ACCESS_TOKEN,
    "SLACK_WEBHOOK_REPORT": SLACK_WEBHOOK,
}.items() if not v]
if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

# ========= HTTP =========
S = requests.Session()
S.headers.update({
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
})
BASE = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}"

def backoff_get(url, params=None, max_retries=6):
    wait = 1.0
    for i in range(max_retries):
        r = S.get(url, params=params if i == 0 else None)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(wait, 6))
            wait *= 1.7
            continue
        return r
    return r

def parse_next_link(link_header: str):
    if not link_header:
        return None
    for part in link_header.split(","):
        part = part.strip()
        if 'rel="next"' in part:
            if part.startswith("<") and ">" in part:
                return part[1:part.index(">")]
    return None

def iso(dt):
    return dt.replace(microsecond=0).isoformat() + "Z"

# ========= DATES =========
now = datetime.datetime.utcnow()
refund_min = iso(now - datetime.timedelta(days=REFUND_WINDOW_DAYS))
refund_max = iso(now)
ltv_min    = iso(now - datetime.timedelta(days=LTV_WINDOW_DAYS))
ltv_max    = iso(now)

# ========= HELPERS =========
def valid_order(o):
    if EXCLUDE_CANCELLED and o.get("cancelled_at"):
        return False
    if not INCLUDE_TEST_ORDERS and o.get("test"):
        return False
    fs = o.get("financial_status")
    return fs in {"paid", "partially_refunded", "refunded"}  # treat these as “real” sales

def fetch_orders(created_at_min, created_at_max):
    url = f"{BASE}/orders.json"
    params = {
        "status": "any",
        "limit": 250,
        "created_at_min": created_at_min,
        "created_at_max": created_at_max,
    }
    out, first = [], True
    while url:
        r = backoff_get(url, params=params if first else None)
        first = False
        if r.status_code != 200:
            raise RuntimeError(f"Orders fetch failed: {r.status_code} {r.text[:400]}")
        payload = r.json()
        out.extend(payload.get("orders", []))
        url = parse_next_link(r.headers.get("Link", ""))
        time.sleep(0.35)
    return out

def order_refund_units_by_product(order):
    m = defaultdict(int)
    for ref in order.get("refunds", []) or []:
        for rli in ref.get("refund_line_items", []) or []:
            li = rli.get("line_item") or {}
            pid = li.get("product_id")
            if pid is None:
                continue
            m[pid] += int(rli.get("quantity") or 0)
    return m

def order_net_refund_amount(order):
    total = 0.0
    for ref in order.get("refunds", []) or []:
        for rli in ref.get("refund_line_items", []) or []:
            li  = (rli.get("line_item") or {})
            qty = int(rli.get("quantity") or 0)
            try:
                price = float(li.get("price") or 0)
            except:
                price = 0.0
            total += qty * price
    return total

# ========= 1) Refund rate per product (units) =========
orders_refund_window = fetch_orders(refund_min, refund_max)

sold_units_by_product      = defaultdict(int)
refunded_units_by_product  = defaultdict(int)
product_title_lookup       = {}  # product_id -> title

for o in orders_refund_window:
    if not valid_order(o):
        continue

    for li in o.get("line_items", []) or []:
        pid = li.get("product_id")
        if pid is None:
            continue
        sold_units_by_product[pid] += int(li.get("quantity") or 0)
        product_title_lookup.setdefault(pid, li.get("title") or f"Product {pid}")

    # add refunds
    m = order_refund_units_by_product(o)
    for pid, units in m.items():
        refunded_units_by_product[pid] += units
        product_title_lookup.setdefault(pid, f"Product {pid}")

refund_rows = []
for pid, sold in sold_units_by_product.items():
    refunded = refunded_units_by_product.get(pid, 0)
    rate = (refunded / sold) * 100 if sold else 0.0
    refund_rows.append({
        "product": product_title_lookup.get(pid, str(pid)),
        "sold_units": sold,
        "refunded_units": refunded,
        "refund_rate_pct": round(rate, 2),
    })
refund_rows.sort(key=lambda r: (r["refund_rate_pct"], r["refunded_units"]), reverse=True)

# ========= 2) LTV per product =========
# Define cohort: customers who bought each product in REFUND window.
customers_by_product = defaultdict(set)
for o in orders_refund_window:
    if not valid_order(o):
        continue
    cid = (o.get("customer") or {}).get("id")
    if not cid:
        continue
    for li in o.get("line_items", []) or []:
        pid = li.get("product_id")
        if pid is not None:
            customers_by_product[pid].add(cid)

# get all orders in LTV window and compute each customer's net spend (sum of totals - refunds)
orders_ltv_window = fetch_orders(ltv_min, ltv_max)
customer_net_spend = defaultdict(float)
for o in orders_ltv_window:
    if not valid_order(o):
        continue
    cid = (o.get("customer") or {}).get("id")
    if not cid:
        continue
    try:
        total_price = float(o.get("total_price") or 0.0)
    except:
        total_price = 0.0
    net = max(total_price - order_net_refund_amount(o), 0.0)
    customer_net_spend[cid] += net

ltv_rows = []
for pid, cohort in customers_by_product.items():
    if not cohort:
        continue
    spends = [customer_net_spend.get(cid, 0.0) for cid in cohort]
    avg = (sum(spends) / len(spends)) if spends else 0.0
    ltv_rows.append({
        "product": product_title_lookup.get(pid, str(pid)),
        "buyers_count": len(cohort),
        "avg_ltv_per_buyer": round(avg, 2),
    })
ltv_rows.sort(key=lambda r: (r["avg_ltv_per_buyer"], r["buyers_count"]), reverse=True)

# ========= Slack formatting =========
def format_table(rows, headers, max_rows=15):
    if not rows:
        return "_No data_"
    widths = [len(h) for h in headers]
    for row in rows[:max_rows]:
        for i, h in enumerate(headers):
            widths[i] = max(widths[i], len(str(row[h])))
    def fmt_row(vals):
        cells = []
        for i, v in enumerate(vals):
            s = str(v)
            cells.append(s + " " * (widths[i] - len(s)))
        return " | ".join(cells)
    lines = [fmt_row(headers),
             "-+-".join("-" * w for w in widths)]
    for r in rows[:max_rows]:
        lines.append(fmt_row([r[h] for h in headers]))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more)")
    return "```" + "\n".join(lines) + "```"

refund_table = format_table(
    refund_rows,
    headers=["product", "sold_units", "refunded_units", "refund_rate_pct"]
)
ltv_table = format_table(
    ltv_rows,
    headers=["product", "buyers_count", "avg_ltv_per_buyer"]
)

msg = (
    f"*Shopify Product Report*\n"
    f"• Refund window: {refund_min[:10]} → {refund_max[:10]}\n"
    f"• LTV window: {ltv_min[:10]} → {ltv_max[:10]}\n\n"
    f"*1) Refund rate per product (units)*\n{refund_table}\n\n"
    f"*2) LTV per product (avg net spend of buyers)*\n{ltv_table}\n"
    f"_Notes:_ Refund rate = refunded units ÷ sold units in refund window. "
    f"LTV(product) = average customer net spend across LTV window for buyers of that product."
)

r = requests.post(SLACK_WEBHOOK, json={"text": msg})
if r.status_code == 200:
    print("✅ Slack report sent.")
else:
    print(f"❌ Slack post failed: {r.status_code} {r.text[:300]}")
2) Add a workflow step (same job or a new one)
In your existing workflow, add a second step (or create a new job) to run the report. Example:

yaml
Copy code
- name: Product Refund & LTV Report
  env:
    SHOPIFY_ACCESS_TOKEN: ${{ secrets.SHOPIFY_ACCESS_TOKEN }}
    SLACK_WEBHOOK_REPORT: ${{ secrets.SLACK_WEBHOOK_REPORT }}   # add this in repo Secrets
    REFUND_WINDOW_DAYS: "90"   # optional overrides
    LTV_WINDOW_DAYS: "365"
  run: |
    set -euo pipefail
    python product_report.py
