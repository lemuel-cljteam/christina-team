from datetime import datetime as dt
import requests
import json
import pandas as pd
import base64
from tqdm import tqdm
import pytz
from pymongo import MongoClient
import gspread
from google.oauth2.service_account import Credentials
import uuid
import numpy as np
import os

working_directory = os.getcwd()
# r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'
logfile = os.path.join(working_directory, "followupboss", "logs.txt")

hoover_tz = pytz.timezone('America/Chicago')

current_time_initial = dt.now(hoover_tz)
current_time_ph_initial = dt.now()

with open(logfile, 'a') as file:
    file.write(f'\nPeople Extract Start time in USA: {current_time_initial}')
    file.write(f'\nPeople Extract Start time in PH: {current_time_ph_initial}\n')

# Your API key
api_key = "fka_0fEZ6mLXysLcr5c3wVKxUUnKgRTHQwftdg"
X_System_Key = "ad593739c6d8eb43684c90ef2d98d08f"
X_System = "Christina_James"

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

client = MongoClient()
db = client['Christina']
collection = db['followupboss_people_backup']

def delete_all():
    result = collection.delete_many({})
    print(f"Deleted {result.deleted_count} documents from {collection.name}")

def insert_one_document(data):
    collection.insert_one(data)

def count_of_all_collections():
    return collection.count_documents({})    

initial_count = count_of_all_collections()
print(f"There are {initial_count} documents now in {collection.name}")

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

hoover_tz = pytz.timezone('America/Chicago')

current_time = dt.now(hoover_tz)
current_time_ph = dt.now()
total_running_time = current_time_ph - current_time_ph_initial
logfile = r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'

final_count = count_of_all_collections() - initial_count
print(f"There are {final_count} documents now in {collection.name}")

with open(logfile, 'a') as file:
    file.write(f"There are {count_of_all_collections()} documents now in {collection.name}")
    file.write(f'\nAdded {final_count} in the collection {collection.name}')
    file.write(f'\nPeople Extract End time in USA: {current_time}')
    file.write(f'\nPeople Extract End time in PH: {current_time_ph}\n')
    file.write(f'\nPeople Total Running time: {total_running_time}')

def get_all_data_as_dataframe():
    # Retrieve all documents in the collection
    documents = list(collection.find({}))  # Convert the cursor to a list of dictionaries

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(documents)

    return df

df = get_all_data_as_dataframe()

# Limit the number of phones to 6
df['phones'] = df['phones'].apply(lambda x: x[:6] if x is not None and len(x) > 0 else [])
df['emails'] = df['emails'].apply(lambda x: x[:6] if x is not None and len(x) > 0 else [])
df['addresses'] = df['addresses'].apply(lambda x: x[:6] if x is not None and len(x) > 0 else [])

# Create separate columns for the first 6 phones
for i in range(6):
    df[f'phone_{i+1}'] = df['phones'].apply(lambda x: x[i]['value'] if len(x) > i else None)
    df[f'phone_type_{i+1}'] = df['phones'].apply(lambda x: x[i]['type'] if len(x) > i else None)
    df[f'email_{i+1}'] = df['emails'].apply(lambda x: x[i]['value'] if len(x) > i else None)
    df[f'email_type_{i+1}'] = df['emails'].apply(lambda x: x[i]['type'] if len(x) > i else None)
    df[f'address_street_{i+1}'] = df['addresses'].apply(lambda x: x[i]['street'] if len(x) > i else None)
    df[f'address_city_{i+1}'] = df['addresses'].apply(lambda x: x[i]['city'] if len(x) > i else None)
    df[f'address_state_{i+1}'] = df['addresses'].apply(lambda x: x[i]['state'] if len(x) > i else None)
    df[f'address_code_{i+1}'] = df['addresses'].apply(lambda x: x[i]['code'] if len(x) > i else None)
    df[f'address_country_{i+1}'] = df['addresses'].apply(lambda x: x[i]['country'] if len(x) > i else None)
    df[f'address_type_{i+1}'] = df['addresses'].apply(lambda x: x[i]['type'] if len(x) > i else None)

# Drop the original 'phones' column if not needed
df.drop(columns=['phones'], inplace=True)
df.drop(columns=['emails'], inplace=True)
df.drop(columns=['addresses'], inplace=True)

# Display the DataFrame
df['Update Source'] = "Followup boss"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
creds = Credentials.from_service_account_file(r'C:\Users\ENDUSER\OneDrive\FOR CHRISTINA\Python\ETLs\credentials.json', scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open_by_key("1UAtfmU1LSsIvfFBDdS0cpUrrhsW9ZDC0mDKF1kj8Ato").worksheet("Leads")
data = sheet.get_all_values()
df_leads = pd.DataFrame(data[1:], columns=data[0])

unique_people_id = df_leads[['Lead ID', 'ID']]
unique_people_id['ID'].replace(r'^\s*$', float('NaN'), regex=True, inplace=True)
unique_people_id.dropna(subset=['ID'], inplace=True)

df['id'] = df['id'].astype(int)
unique_people_id['ID'] = unique_people_id['ID'].astype(int)

print(f'length of rows of people from followup boss before merge: {len(df)}')
df_new = pd.merge(df, unique_people_id, 'left', left_on='id', right_on='ID')
print(f'length of rows of people from followup boss after merge: {len(df)}')

def generate_lead_id():
    return str(uuid.uuid4())[:8]

df_new ['Lead ID']= df_new['Lead ID'].apply(lambda x: generate_lead_id() if pd.isna(x) else x)
df_new.replace([np.inf, -np.inf, np.nan], None, inplace=True)

df_new['Budget'] = df_new['price']

df_app_only = df_leads[df_leads['Update Source'] == 'App']

df_fub_only = df_new[~df_new['Lead ID'].isin(df_app_only['Lead ID'])]
df_fub_only['Update Source'] = 'Followup boss'
df_fub_only