import gspread
from google.oauth2.service_account import Credentials
from pymongo import MongoClient
import pandas as pd

# Define the scope (read-only access to Sheets)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Load the service account credentials
creds = Credentials.from_service_account_file(r'C:\Users\ENDUSER\OneDrive\FOR CHRISTINA\Python\ETLs\credentials.json', scopes=SCOPES)

# Authorize the client with the credentials
client = gspread.authorize(creds)

# Open the Google Sheet by name or by URL
sheet = client.open_by_key("1UAtfmU1LSsIvfFBDdS0cpUrrhsW9ZDC0mDKF1kj8Ato").worksheet("Leads")

# Get all data in the sheet
data = sheet.get_all_records()

mongo_client = MongoClient()
db = mongo_client['Christina']
collection = db['app_leads_backup']

def delete_all():
    result = collection.delete_many({})
    print(f"Deleted {result.deleted_count} documents from {collection.name}")

delete_all()

if data:
    collection.insert_many(data)

print(f"{sheet} Backup to Mongodb completed successfully!")

