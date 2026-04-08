import os
import json
import pandas as pd
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

print("Daily page-level GSC data exported successfully")
