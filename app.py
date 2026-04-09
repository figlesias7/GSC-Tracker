import os
import json
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import date, timedelta
from pathlib import Path

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
SITE_URL = os.environ.get("GSC_SITE")

# Pull up to the last 180 days of page-by-day data
END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=179)

credentials = service_account.Credentials.from_service_account_info(
    json.loads(os.environ.get("GOOGLE_CREDS")),
    scopes=SCOPES
)

service = build('searchconsole', 'v1', credentials=credentials)

request = {
    'startDate': START_DATE.isoformat(),
    'endDate': END_DATE.isoformat(),
    'dimensions': ['date', 'page'],
    'rowLimit': 25000
}

response = service.searchanalytics().query(
    siteUrl=SITE_URL,
    body=request
).execute()

rows = response.get('rows', [])

data = []
for row in rows:
    keys = row.get('keys', [])
    row_date = keys[0] if len(keys) > 0 else None
    row_page = keys[1] if len(keys) > 1 else None

    data.append({
        "date": row_date,
        "page": row_page,
        "clicks": row.get("clicks", 0),
        "impressions": row.get("impressions", 0),
        "ctr": row.get("ctr", 0),
        "position": row.get("position", 0)
    })

df = pd.DataFrame(data)

if df.empty:
    df = pd.DataFrame(columns=["date", "page", "clicks", "impressions", "ctr", "position"])

Path("data").mkdir(exist_ok=True)
Path("docs").mkdir(exist_ok=True)

# Save raw daily data
df.to_csv("data/gsc_daily_data.csv", index=False)

# Save JSON for dashboard
df.to_json("docs/daily_data.json", orient="records", indent=2)

def get_sitemap_pages_from_xml(xml_text):
    root = ET.fromstring(xml_text)

    # Regular sitemap
    if root.tag.endswith("urlset"):
        pages = []
        for child in root:
            for sub in child:
                if sub.tag.endswith("loc") and sub.text:
                    pages.append(sub.text.strip())
        return pages

    # Sitemap index
    if root.tag.endswith("sitemapindex"):
        sitemaps = []
        for child in root:
            for sub in child:
                if sub.tag.endswith("loc") and sub.text:
                    sitemaps.append(sub.text.strip())
        return sitemaps

    return []

def fetch_sitemap_urls(sitemap_url, visited=None):
    if visited is None:
        visited = set()

    if sitemap_url in visited:
        return []

    visited.add(sitemap_url)

    try:
        response = requests.get(sitemap_url, timeout=20)
        response.raise_for_status()
        items = get_sitemap_pages_from_xml(response.text)

        # If this is a sitemap index, recursively fetch child sitemaps
        if items and items[0].endswith(".xml"):
            all_pages = []
            for child_sitemap in items:
                all_pages.extend(fetch_sitemap_urls(child_sitemap, visited))
            return all_pages

        return items
    except Exception as e:
        print(f"Sitemap fetch failed for {sitemap_url}: {e}")
        return []

# Pull pages from sitemap
sitemap_url = SITE_URL.rstrip("/") + "/sitemap.xml"
all_pages = fetch_sitemap_urls(sitemap_url)

# Normalize both sets by trimming trailing slash for comparison
pages_with_data = set(
    str(page).rstrip("/")
    for page in df["page"].dropna().unique()
)

zero_pages = []
for page in all_pages:
    normalized = str(page).rstrip("/")
    if normalized not in pages_with_data:
        zero_pages.append(page)

with open("docs/zero_pages.json", "w", encoding="utf-8") as f:
    json.dump(sorted(set(zero_pages)), f, indent=2)

print("Daily page-level GSC data and zero-impression page list exported successfully")
