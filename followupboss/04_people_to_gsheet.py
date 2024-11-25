from datetime import datetime as dt
import requests
import json
import pandas as pd
import base64
from tqdm import tqdm
import pytz
from pymongo import MongoClient
import gspread
# from google.oauth2.service_account import Credentials
import uuid
import numpy as np
import os
from followupboss.scripts import backup_script_collection_input, mongodb_logging

api_key = os.getenv("FOLLOWUPBOSS_APIKEY")
X_System_Key = os.getenv("FOLLOWUPBOSS_XSYSTEMKEY")
X_System = os.getenv("FOLLOWUPBOSS_XSYSTEM")
mongopass = os.getenv("MONGODB_PASSWORD")
gsheetid = os.getenv("GSHEET_ID")
creds = os.getenv("GOOGLE_CREDENTIALS")
MONGO_URI = os.getenv("MONGO_URI")

working_directory = os.getcwd()
# r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'
logfile = os.path.join(working_directory, "followupboss", "logs.txt")

hoover_tz = pytz.timezone('America/Chicago')

current_time_initial = dt.now(hoover_tz)
current_time_ph_initial = dt.now()

mongodb_logging(event_var=f'People Extract Start time in USA: {current_time_initial}', old_doc=None, new_doc=None)
mongodb_logging(event_var=f'People Extract Start time in PH: {current_time_ph_initial}', old_doc=None, new_doc=None)

with open(logfile, 'a') as file:
    file.write(f'\nPeople Extract Start time in USA: {current_time_initial}')
    file.write(f'\nPeople Extract Start time in PH: {current_time_ph_initial}\n')

# Encode API key in Base64
encoded_api_key = base64.b64encode(api_key.encode('utf-8')).decode('utf-8')

# API endpoint and query parameters
url = "https://api.followupboss.com/v1/people?sort=lastActivity&limit=100&offset=0&includeTrash=false&includeUnclaimed=false"

r = requests.get(url, headers={'accept': "application/json",
                               'Authorization': "Basic " + encoded_api_key,
                                'X-System-Key': X_System_Key,
                                'X-System': X_System
})
data = r.json()
total = data["_metadata"]["total"]

client = MongoClient(MONGO_URI)
db = client['Christina']
collection = db['followupboss_people']

def delete_all():
    result = collection.delete_many({})
    print(f"Deleted {result.deleted_count} documents from {collection.name}")

def insert_one_document(data):
    collection.insert_one(data)

def count_of_all_collections():
    return collection.count_documents({})    

initial_count = count_of_all_collections()
print(f"There are {initial_count} documents now in {collection.name}")

# backup to mongodb
backup_script_collection_input(backup_type="followupboss people", 
                               collection_source=collection, 
                               collection_output=db['followupboss_people_backups'])

# -------delete all for replacement
delete_all()

list_of_offsets = list(range(0, round(total, -2) + 1, 100))
date_now = dt.today().strftime('%m/%d/%Y')

for i in tqdm(list_of_offsets, total=len(list_of_offsets), desc="Processing People from Followup boss", leave=True):
    url = f'https://api.followupboss.com/v1/people?sort=lastActivity&limit=100&offset={i}&includeTrash=false&includeUnclaimed=false'
    r = requests.get(url, headers={'accept': "application/json",
                                'Authorization': "Basic " + encoded_api_key,
                                    'X-System-Key': X_System_Key,
                                    'X-System': X_System
    })
    data = r.json()
    for x in data['people']:
        if isinstance(x, dict):
            insert_one_document(x)
