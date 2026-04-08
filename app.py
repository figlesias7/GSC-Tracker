import os
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import date, timedelta
from pathlib import Path

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
SITE_URL = os.environ.get("GSC_SITE")

# Pull last 30 days ending today
END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=30)

credentials = service_account.Credentials.from_service_account_info(
    json.loads(os.environ.get("GOOGLE_CREDS")),
    scopes=SCOPES
)

service = build('searchconsole', 'v1', credentials=credentials)

request = {
    'startDate': START_DATE.isoformat(),
    'endDate': END_DATE.isoformat(),
    'dimensions': ['page'],
    'rowLimit': 25000
}

response = service.searchanalytics().query(
    siteUrl=SITE_URL,
    body=request
).execute()

rows = response.get('rows', [])

data = []
for row in rows:
    data.append({
        "snapshot_date": END_DATE.isoformat(),
        "start_date": START_DATE.isoformat(),
        "end_date": END_DATE.isoformat(),
        "page": row['keys'][0],
        "clicks": row['clicks'],
        "impressions": row['impressions'],
        "ctr": row['ctr'],
        "position": row['position']
    })

df = pd.DataFrame(data)

# Create folders
Path("data").mkdir(exist_ok=True)
Path("docs").mkdir(exist_ok=True)

# Save latest snapshot
latest_csv = "data/gsc_latest.csv"
df.to_csv(latest_csv, index=False)

# Save dated snapshot
dated_csv = f"data/gsc_snapshot_{END_DATE.isoformat()}.csv"
df.to_csv(dated_csv, index=False)

# Append to history
history_csv = "data/gsc_history.csv"
if os.path.exists(history_csv):
    old_df = pd.read_csv(history_csv)
    combined = pd.concat([old_df, df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["snapshot_date", "page"], keep="last")
else:
    combined = df.copy()

combined.to_csv(history_csv, index=False)

# Create summary JSON for dashboard
summary = {
    "site": SITE_URL,
    "snapshot_date": END_DATE.isoformat(),
    "total_pages": int(df["page"].nunique()) if not df.empty else 0,
    "total_clicks": float(df["clicks"].sum()) if not df.empty else 0,
    "total_impressions": float(df["impressions"].sum()) if not df.empty else 0,
    "avg_position": float(df["position"].mean()) if not df.empty else 0
}

with open("docs/summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)

# Export top pages JSON
top_pages = df.sort_values(["clicks", "impressions"], ascending=[False, False]).head(100)
top_pages.to_json("docs/top_pages.json", orient="records", indent=2)

print("Daily snapshot, history, and dashboard data exported successfully")
