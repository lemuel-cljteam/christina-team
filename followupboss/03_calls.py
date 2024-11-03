import requests
import json
import pandas as pd
import base64
from tqdm import tqdm
import pytz
from datetime import datetime as dt
import pytz
import os

api_key = os.getenv("FOLLOWUPBOSS_APIKEY")
X_System_Key = os.getenv("FOLLOWUPBOSS_XSYSTEMKEY")
X_System = os.getenv("FOLLOWUPBOSS_XSYSTEM")
mongopass = os.getenv("MONGODB_PASSWORD")
gsheetid = os.getenv("GSHEET_ID")

working_directory = os.getcwd()
# r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'
logfile = os.path.join(working_directory, "followupboss", "logs.txt")

hoover_tz = pytz.timezone('America/Chicago')

current_time_initial = dt.now(hoover_tz)
current_time_ph_initial = dt.now()

with open(logfile, 'a') as file:
    file.write(f'\nCalls Extract Start time in USA: {current_time_initial}')
    file.write(f'\nCalls Extract Start time in PH: {current_time_ph_initial}\n')

# Your API key
api_key = "fka_0fEZ6mLXysLcr5c3wVKxUUnKgRTHQwftdg"
X_System_Key = "ad593739c6d8eb43684c90ef2d98d08f"
X_System = "Christina_James"

# Encode API key in Base64
encoded_api_key = base64.b64encode(api_key.encode('utf-8')).decode('utf-8')

from pymongo import MongoClient
client = MongoClient(f"mongodb+srv://christina:{mongopass}@clusterchristina.57107.mongodb.net/test?retryWrites=true&w=majority&ssl=true")
db = client['Christina']
collection = db['followupboss_calls']

initial_count_of_collection = collection.count_documents({})

def delete_all():
    result = collection.delete_many({})
    print(f"Deleted {result.deleted_count} documents from {collection.name}")

def insert_one_document(data):
    collection.insert_one(data)

def count_of_all_documents():
    # print(f"There are {collection.count_documents({})} documents now in {collection.name}")
    return collection.count_documents({})

delete_all()

# API endpoint and query parameters
url = "https://api.followupboss.com/v1/calls?limit=100&offset=0"

r = requests.get(url, headers={'accept': "application/json",
                               'Authorization': "Basic " + encoded_api_key,
                                'X-System-Key': X_System_Key,
                                'X-System': X_System
})
data = r.json()   
total = round(data['_metadata']['total'] / 100, None)
nextLink = data['_metadata'].get('nextLink')
for x in data['calls']:
    if isinstance(x, dict):
        insert_one_document(x)

with tqdm(total, total=total, desc="Extracting calls", leave=True) as pbar:
    while nextLink:
        r = requests.get(nextLink, headers={'accept': "application/json",
                                    'Authorization': "Basic " + encoded_api_key,
                                        'X-System-Key': X_System_Key,
                                        'X-System': X_System
        })
        if r.status_code == 200:
            data = r.json()
            nextLink = data['_metadata'].get('nextLink')
            for x in data.get('calls', []):
                if isinstance(x, dict):
                    insert_one_document(x)
            pbar.update(1)
        else:
            print(f'Failed to fetch data. Status code: {r.status_code}')
            nextLink = None

final_count_of_collection = count_of_all_documents() - initial_count_of_collection
print(f'Added {final_count_of_collection} in the collection {collection.name}')
print(f'Total Number of documents: {count_of_all_documents()} in the collection {collection.name}')

hoover_tz = pytz.timezone('America/Chicago')

current_time = dt.now(hoover_tz)
current_time_ph = dt.now()
total_running_time = current_time_ph - current_time_ph_initial
logfile = r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'

with open(logfile, 'a') as file:
    file.write(f'\nTotal Number of documents: {count_of_all_documents()} in the collection {collection.name}')
    file.write(f'\nCalls Extract End time in USA: {current_time}')
    file.write(f'\nCalls Extract End time in PH: {current_time_ph}\n')
    file.write(f'\nCalls Total Running time: {total_running_time}')
