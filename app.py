import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import date, timedelta

# CONFIG
SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
SITE_URL = os.environ.get("GSC_SITE")
START_DATE = (date.today() - timedelta(days=30)).isoformat()
END_DATE = date.today().isoformat()

# AUTH
credentials = service_account.Credentials.from_service_account_info(
    eval(os.environ.get("GOOGLE_CREDS")),
    scopes=SCOPES
)

service = build('searchconsole', 'v1', credentials=credentials)

# QUERY
request = {
    'startDate': START_DATE,
    'endDate': END_DATE,
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
        "page": row['keys'][0],
        "clicks": row['clicks'],
        "impressions": row['impressions'],
        "ctr": row['ctr'],
        "position": row['position']
    })

df = pd.DataFrame(data)

# SAVE CSV
df.to_csv("gsc_data.csv", index=False)

print("Data exported successfully")
