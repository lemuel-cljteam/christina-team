import requests
import json
import pandas as pd
import base64
from tqdm import tqdm
import pytz
from datetime import datetime as dt

hoover_tz = pytz.timezone('America/Chicago')

current_time_initial = dt.now(hoover_tz)
current_time_ph_initial = dt.now()
logfile = r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'

with open(logfile, 'a') as file:
    file.write(f'\nPeople Relationships Extract Start time in USA: {current_time_initial}')
    file.write(f'\nPeople Relationships Extract Start time in PH: {current_time_ph_initial}\n')

# Your API key
api_key = "fka_0fEZ6mLXysLcr5c3wVKxUUnKgRTHQwftdg"
X_System_Key = "ad593739c6d8eb43684c90ef2d98d08f"
X_System = "Christina_James"

# Encode API key in Base64
encoded_api_key = base64.b64encode(api_key.encode('utf-8')).decode('utf-8')

# API endpoint and query parameters
url = "https://api.followupboss.com/v1/peopleRelationships?limit=100&offset=0"

r = requests.get(url, headers={'accept': "application/json",
                               'Authorization': "Basic " + encoded_api_key,
                                'X-System-Key': X_System_Key,
                                'X-System': X_System
})
data = r.json()
with open(r'C:\Users\ENDUSER\OneDrive\FOR CHRISTINA\Python\people_relationships.json', 'w') as file:
    json.dump(data, file, indent=4)
    
total = data['_metadata']['total']
list_of_offsets = list(range(0, round(total, -1) + 1, 100))

df_list = []

from pymongo import MongoClient
client = MongoClient()
db = client['Christina']
collection = db['followupboss_people_relationships']

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
for i in tqdm(list_of_offsets, total=len(list_of_offsets), desc="Processing People from Followup boss", leave=True):
    url = f"https://api.followupboss.com/v1/peopleRelationships?limit=100&offset={i}"

    r = requests.get(url, headers={'accept': "application/json",
                                'Authorization': "Basic " + encoded_api_key,
                                    'X-System-Key': X_System_Key,
                                    'X-System': X_System
    })
    data = r.json()
    for x in data['peoplerelationships']:
        if isinstance(x, dict):
            insert_one_document(x)

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
    file.write(f'\nPeople Relationships Extract End time in USA: {current_time}')
    file.write(f'\nPeople Relationships Extract End time in PH: {current_time_ph}\n')
    file.write(f'\nPeople Relationships Total Running time: {total_running_time}')