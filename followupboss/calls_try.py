import requests
import json
import pandas as pd
import base64
from tqdm import tqdm
import pytz
from datetime import datetime as dt
import os
from pymongo import MongoClient

# Set up working directory and log file
working_directory = os.getcwd()
logfile = os.path.join(working_directory, "followupboss", "logs.txt")

# Set timezone
hoover_tz = pytz.timezone('America/Chicago')

# Log start time
current_time_initial = dt.now(hoover_tz)
current_time_ph_initial = dt.now()
with open(logfile, 'a') as file:
    file.write(f'\nCalls Extract Start time in USA: {current_time_initial}')
    file.write(f'\nCalls Extract Start time in PH: {current_time_ph_initial}\n')

# Your API credentials
api_key = "fka_0fEZ6mLXysLcr5c3wVKxUUnKgRTHQwftdg"
X_System_Key = "ad593739c6d8eb43684c90ef2d98d08f"
X_System = "Christina_James"

# Encode API key in Base64
encoded_api_key = base64.b64encode(api_key.encode('utf-8')).decode('utf-8')

# MongoDB connection
try:
    client = MongoClient("mongodb+srv://christina:akodcXC3gIB2qhYf@clusterchristina.57107.mongodb.net/test?connectTimeoutMS=30000&socketTimeoutMS=30000&ssl=true")
    print("Connected to MongoDB")
    db = client['Christina']
    collection = db['followupboss_calls']
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")

# Count initial documents
try:
    initial_count_of_collection = collection.count_documents({})
    print(f"Initial count of documents: {initial_count_of_collection}")
except Exception as e:
    print(f"Error counting documents: {e}")

def delete_all():
    result = collection.delete_many({})
    print(f"Deleted {result.deleted_count} documents from {collection.name}")

def insert_one_document(data):
    try:
        collection.insert_one(data)
    except Exception as e:
        print(f"Error inserting document: {e}")

def count_of_all_documents():
    return collection.count_documents({})

# Clear collection
delete_all()

# API endpoint and initial request
url = "https://api.followupboss.com/v1/calls?limit=100&offset=0"
r = requests.get(url, headers={
    'accept': "application/json",
    'Authorization': "Basic " + encoded_api_key,
    'X-System-Key': X_System_Key,
    'X-System': X_System
})

# Log and save initial data
if r.status_code == 200:
    data = r.json()
    with open(r'C:\Users\ENDUSER\OneDrive\FOR CHRISTINA\Python\calls.json', 'w') as file:
        json.dump(data, file, indent=4)

    total = data['_metadata']['total']
    total_pages = (total // 100) + (1 if total % 100 > 0 else 0)
    nextLink = data['_metadata'].get('nextLink')

    # Insert initial calls into MongoDB
    for x in data['calls']:
        if isinstance(x, dict):
            insert_one_document(x)

    # Progress bar for additional pages
    with tqdm(total=total_pages, desc="Extracting calls", leave=True) as pbar:
        while nextLink:
            r = requests.get(nextLink, headers={
                'accept': "application/json",
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
                break  # Exit loop if the request fails
else:
    print(f'Initial API request failed. Status code: {r.status_code}')

# Log final counts
final_count_of_collection = count_of_all_documents() - initial_count_of_collection
print(f'Added {final_count_of_collection} in the collection {collection.name}')
print(f'Total Number of documents: {count_of_all_documents()} in the collection {collection.name}')

# Log end time
current_time = dt.now(hoover_tz)
current_time_ph = dt.now()
total_running_time = current_time_ph - current_time_ph_initial

with open(logfile, 'a') as file:
    file.write(f'\nTotal Number of documents: {count_of_all_documents()} in the collection {collection.name}')
    file.write(f'\nCalls Extract End time in USA: {current_time}')
    file.write(f'\nCalls Extract End time in PH: {current_time_ph}\n')
    file.write(f'\nCalls Total Running time: {total_running_time}')
