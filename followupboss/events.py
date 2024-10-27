import requests
import json
import pandas as pd
import base64
from tqdm import tqdm
import pytz
from datetime import datetime as dt
from pymongo import MongoClient, UpdateOne

# Timezones
hoover_tz = pytz.timezone('America/Chicago')
current_time_initial = dt.now(hoover_tz)
current_time_ph_initial = dt.now()

# Logging
logfile = r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'
with open(logfile, 'a') as file:
    file.write(f'\nEvents Extract Start time in USA: {current_time_initial}')
    file.write(f'\nEvents Extract Start time in PH: {current_time_ph_initial}\n')

# API keys
api_key = "fka_0fEZ6mLXysLcr5c3wVKxUUnKgRTHQwftdg"
X_System_Key = "ad593739c6d8eb43684c90ef2d98d08f"
X_System = "Christina_James"
encoded_api_key = base64.b64encode(api_key.encode('utf-8')).decode('utf-8')

# MongoDB client and database collection
client = MongoClient()
db = client['Christina']
collection = db['followupboss_events']

# Clear collection
initial_count_of_collection = collection.count_documents({})
# collection.delete_many({})

# Define the bulk upsert function
def bulk_upsert_documents(events):
    operations = []
    for event in events:
        if 'id' in event:
            event_id = event['id']
            # Create an upsert operation
            operations.append(
                UpdateOne(
                    {'events.id': event_id},
                    {'$set': event},
                    upsert=True
                )
            )
    # Execute bulk upsert if operations exist
    if operations:
        result = collection.bulk_write(operations)
        # print(f"Upserted {result.upserted_count} documents, Modified {result.modified_count} documents")
created_after = '2024-09-30'
# API endpoint and data extraction
url = f"https://api.followupboss.com/v1/events?limit=100&offset=0&createdAfter={created_after}T11:00:00Z"
r = requests.get(url, headers={
    'accept': "application/json",
    'Authorization': "Basic " + encoded_api_key,
    'X-System-Key': X_System_Key,
    'X-System': X_System
})
data = r.json()
nextLink = data['_metadata'].get('nextLink')
events = data.get('events', [])
bulk_upsert_documents(events)
total = data['_metadata']['total']
print(f'found total items: {total}')
list_of_offsets = list(range(0, total + 100, 100))

# Process data in batches and upsert
with tqdm(list_of_offsets, total=len(list_of_offsets), desc="Processing Events from Followup boss", leave=True) as pbar:
    while nextLink:
        r = requests.get(nextLink, headers={
            'accept': "application/json",
            'Authorization': "Basic " + encoded_api_key,
            'X-System-Key': X_System_Key,
            'X-System': X_System
        })
        data = r.json()
        nextLink = data['_metadata'].get('nextLink')
        events = data.get('events', [])
        bulk_upsert_documents(events)
        pbar.update(1)

# Final counts and logging
final_count_of_collection = collection.count_documents({}) - initial_count_of_collection
print(f'Added {final_count_of_collection} in the collection {collection.name}')
print(f'Total Number of documents: {collection.count_documents({})} in the collection {collection.name}')

# Log end times
current_time = dt.now(hoover_tz)
current_time_ph = dt.now()
total_running_time = current_time_ph - current_time_ph_initial

with open(logfile, 'a') as file:
    file.write(f'\nTotal Number of documents: {collection.count_documents({})} in the collection {collection.name}')
    file.write(f'\nEvents Extract End time in USA: {current_time}')
    file.write(f'\nEvents Extract End time in PH: {current_time_ph}\n')
    file.write(f'\nEvents Total Running time: {total_running_time}')
