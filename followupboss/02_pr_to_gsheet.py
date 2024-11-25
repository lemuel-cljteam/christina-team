import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from pymongo import MongoClient
from pymongoarrow.api import find_pandas_all
import pytz
import os
import json
import base64
import subprocess
from followupboss.scripts import mongodb_logging, backup_script_collection_input, backup_script_df_input

api_key = os.getenv("FOLLOWUPBOSS_APIKEY")
X_System_Key = os.getenv("FOLLOWUPBOSS_XSYSTEMKEY")
X_System = os.getenv("FOLLOWUPBOSS_XSYSTEM")
mongopass = os.getenv("MONGODB_PASSWORD")
gsheetid = os.getenv("GSHEET_ID")
creds = os.getenv("GOOGLE_CREDENTIALS")

working_directory = os.getcwd()

hoover_tz = pytz.timezone('America/Chicago')

current_time_initial = datetime.now(hoover_tz)
current_time_ph_initial = datetime.now()

# r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'
logfile = os.path.join(working_directory, "followupboss", "logs.txt")

mongodb_logging(event_var=f"Update People Relationships Gsheet Start time in USA: {current_time_initial}",
        old_doc=None,
        new_doc=None)
mongodb_logging(event_var=f"Update People Relationships Gsheet Start time in PH: {current_time_ph_initial}",
        old_doc=None,
        new_doc=None)

with open(logfile, 'a') as file:
    file.write(f'\nUpdate People Relationships Gsheet Start time in USA: {current_time_initial}')
    file.write(f'\nUpdate People Relationships Gsheet Start time in PH: {current_time_ph_initial}\n')

#run first the peoplerelationships.py

# Connect to MongoDB (adjust the connection string as needed)
client = MongoClient(f"mongodb+srv://christina:{mongopass}@clusterchristina.57107.mongodb.net/test?retryWrites=true&w=majority&ssl=true")
db = client['Christina']
collection = db['followupboss_people_relationships']

def get_all_data_as_dataframe():
    # Retrieve all documents in the collection
    documents = list(collection.find({}))  # Convert the cursor to a list of dictionaries

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(documents)

    return df

# Define the fields you want to extract
projection = {
    'id': 1,
    'firstName': 1,
    'lastName': 1,
    'type': 1,
    'phones.value': 1,
    'phones.type': 1,
    'emails.value': 1,
    'emails.type': 1,
    'addresses.street': 1,
    'addresses.city': 1,
    'addresses.state': 1,
    'addresses.code': 1,
    'addresses.country': 1,
    'addresses.type': 1
}

# Use find_pandas_all to query MongoDB and get a DataFrame
df = find_pandas_all(collection, {}, projection=projection)

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
df.columns = [
    "Mongodb ID",
"People ID From FB",
"Relationship First Name",
"Relationship Last Name",
"Relationship Type",
"Relationship Phone 1",
"Relationship Phone 1 - Type",
"Relationship Phone 2",
"Relationship Phone 2 - Type",
"Relationship Phone 3",
"Relationship Phone 3 - Type",
"Relationship Phone 4",
"Relationship Phone 4 - Type",
"Relationship Phone 5",
"Relationship Phone 5 - Type",
"Relationship Phone 6",
"Relationship Phone 6 - Type",
"Relationship Email 1",
"Relationship Email 1 - Type",
"Relationship Email 2",
"Relationship Email 2 - Type",
"Relationship Email 3",
"Relationship Email 3 - Type",
"Relationship Email 4",
"Relationship Email 4 - Type",
"Relationship Email 5",
"Relationship Email 5 - Type",
"Relationship Email 6",
"Relationship Email 6 - Type",
"Relationship Address 1 - Street",
"Relationship Address 1 - City",
"Relationship Address 1 - State",
"Relationship Address 1 - Zip",
"Relationship Address 1 - Country",
"Relationship Address 1 - Type",
"Relationship Address 2 - Street",
"Relationship Address 2 - City",
"Relationship Address 2 - State",
"Relationship Address 2 - Zip",
"Relationship Address 2 - Country",
"Relationship Address 2 - Type",
"Relationship Address 3 - Street",
"Relationship Address 3 - City",
"Relationship Address 3 - State",
"Relationship Address 3 - Zip",
"Relationship Address 3 - Country",
"Relationship Address 3 - Type",
"Relationship Address 4 - Street",
"Relationship Address 4 - City",
"Relationship Address 4 - State",
"Relationship Address 4 - Zip",
"Relationship Address 4 - Country",
"Relationship Address 4 - Type",
"Relationship Address 5 - Street",
"Relationship Address 5 - City",
"Relationship Address 5 - State",
"Relationship Address 5 - Zip",
"Relationship Address 5 - Country",
"Relationship Address 5 - Type",
"Relationship Address 6 - Street",
"Relationship Address 6 - City",
"Relationship Address 6 - State",
"Relationship Address 6 - Zip",
"Relationship Address 6 - Country",
"Relationship Address 6 - Type",
"Update Source"
]

# Define the scope (read-only access to Sheets)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Load the service account credentials
# for local testing
# creds = Credentials.from_service_account_file(os.path.join(working_directory, "credentials.json"), scopes=SCOPES)
if creds and os.path.exists(creds):
    credentials = Credentials.from_service_account_file(creds, scopes=SCOPES)
else:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not set or file does not exist!")

client = gspread.authorize(credentials)

# Open the Google Sheet by name or by URL
sheet = client.open_by_key(gsheetid).worksheet("Leads")

# Get all data in the sheet
data = sheet.get_all_values()

# Convert the data into a pandas DataFrame
df_1 = pd.DataFrame(data[1:], columns=data[0])

unique_people_id = df_1[['Lead ID', 'ID']].copy()
# unique_people_id.replace({'ID': r'^\s*$'}, float('NaN'), regex=True, inplace=True)
unique_people_id.loc[:,'ID'] = unique_people_id['ID'].replace(r'^\s*$', float('NaN'), regex=True)
unique_people_id.dropna(subset=['ID'], inplace=True)
unique_people_id.loc[:, 'ID'] = unique_people_id['ID'].astype(int)

df.loc[:, 'People ID From FB'] = df['People ID From FB'].astype(int)

print(f'length of rows of people relationships from followup boss before merge: {len(df)}')
df_new = pd.merge(df, unique_people_id, 'left', left_on='People ID From FB', right_on='ID')
print(f'length of rows of people relationships from followup boss after merge: {len(df)}')

def generate_lead_id():
    return str(uuid.uuid4())[:8]

df_new ['Lead ID']= df_new['Lead ID'].apply(lambda x: generate_lead_id() if pd.isna(x) else x)
df_new = df_new[[
    "Lead ID",
    "Relationship First Name",
    "Relationship Last Name",
    "Relationship Type",
    "Relationship Phone 1",
    "Relationship Phone 1 - Type",
    "Relationship Phone 2",
    "Relationship Phone 2 - Type",
    "Relationship Phone 3",
    "Relationship Phone 3 - Type",
    "Relationship Phone 4",
    "Relationship Phone 4 - Type",
    "Relationship Phone 5",
    "Relationship Phone 5 - Type",
    "Relationship Phone 6",
    "Relationship Phone 6 - Type",
    "Relationship Email 1",
    "Relationship Email 1 - Type",
    "Relationship Email 2",
    "Relationship Email 2 - Type",
    "Relationship Email 3",
    "Relationship Email 3 - Type",
    "Relationship Email 4",
    "Relationship Email 4 - Type",
    "Relationship Email 5",
    "Relationship Email 5 - Type",
    "Relationship Email 6",
    "Relationship Email 6 - Type",
    "Relationship Address 1 - Street",
    "Relationship Address 1 - City",
    "Relationship Address 1 - State",
    "Relationship Address 1 - Zip",
    "Relationship Address 1 - Country",
    "Relationship Address 1 - Type",
    "Relationship Address 2 - Street",
    "Relationship Address 2 - City",
    "Relationship Address 2 - State",
    "Relationship Address 2 - Zip",
    "Relationship Address 2 - Country",
    "Relationship Address 2 - Type",
    "Relationship Address 3 - Street",
    "Relationship Address 3 - City",
    "Relationship Address 3 - State",
    "Relationship Address 3 - Zip",
    "Relationship Address 3 - Country",
    "Relationship Address 3 - Type",
    "Relationship Address 4 - Street",
    "Relationship Address 4 - City",
    "Relationship Address 4 - State",
    "Relationship Address 4 - Zip",
    "Relationship Address 4 - Country",
    "Relationship Address 4 - Type",
    "Relationship Address 5 - Street",
    "Relationship Address 5 - City",
    "Relationship Address 5 - State",
    "Relationship Address 5 - Zip",
    "Relationship Address 5 - Country",
    "Relationship Address 5 - Type",
    "Relationship Address 6 - Street",
    "Relationship Address 6 - City",
    "Relationship Address 6 - State",
    "Relationship Address 6 - Zip",
    "Relationship Address 6 - Country",
    "Relationship Address 6 - Type",
    "People ID From FB",
    "Update Source"
]]

# Step 1: Group by 'Lead ID' and create a row number within each group
df_new['Row Number'] = df_new.groupby('Lead ID').cumcount() + 1

# Step 2: Concatenate 'Lead ID' with the row number to create 'Relationship ID'
df_new.loc[:, 'Relationship ID'] = df_new['Lead ID'] + '-' + df_new['Row Number'].astype(str)
df_new.replace([np.inf, -np.inf, np.nan], '', inplace=True)

# Resulting DataFrame
df_from_fub = df_new[[
    "Lead ID",
    "Relationship ID",
    "Relationship First Name",
    "Relationship Last Name",
    "Relationship Type",
    "Relationship Phone 1",
    "Relationship Phone 1 - Type",
    "Relationship Phone 2",
    "Relationship Phone 2 - Type",
    "Relationship Phone 3",
    "Relationship Phone 3 - Type",
    "Relationship Phone 4",
    "Relationship Phone 4 - Type",
    "Relationship Phone 5",
    "Relationship Phone 5 - Type",
    "Relationship Phone 6",
    "Relationship Phone 6 - Type",
    "Relationship Email 1",
    "Relationship Email 1 - Type",
    "Relationship Email 2",
    "Relationship Email 2 - Type",
    "Relationship Email 3",
    "Relationship Email 3 - Type",
    "Relationship Email 4",
    "Relationship Email 4 - Type",
    "Relationship Email 5",
    "Relationship Email 5 - Type",
    "Relationship Email 6",
    "Relationship Email 6 - Type",
    "Relationship Address 1 - Street",
    "Relationship Address 1 - City",
    "Relationship Address 1 - State",
    "Relationship Address 1 - Zip",
    "Relationship Address 1 - Country",
    "Relationship Address 1 - Type",
    "Relationship Address 2 - Street",
    "Relationship Address 2 - City",
    "Relationship Address 2 - State",
    "Relationship Address 2 - Zip",
    "Relationship Address 2 - Country",
    "Relationship Address 2 - Type",
    "Relationship Address 3 - Street",
    "Relationship Address 3 - City",
    "Relationship Address 3 - State",
    "Relationship Address 3 - Zip",
    "Relationship Address 3 - Country",
    "Relationship Address 3 - Type",
    "Relationship Address 4 - Street",
    "Relationship Address 4 - City",
    "Relationship Address 4 - State",
    "Relationship Address 4 - Zip",
    "Relationship Address 4 - Country",
    "Relationship Address 4 - Type",
    "Relationship Address 5 - Street",
    "Relationship Address 5 - City",
    "Relationship Address 5 - State",
    "Relationship Address 5 - Zip",
    "Relationship Address 5 - Country",
    "Relationship Address 5 - Type",
    "Relationship Address 6 - Street",
    "Relationship Address 6 - City",
    "Relationship Address 6 - State",
    "Relationship Address 6 - Zip",
    "Relationship Address 6 - Country",
    "Relationship Address 6 - Type",
    "People ID From FB",
    "Update Source"
]]

# client = MongoClient()
client = MongoClient(f"mongodb+srv://christina:{mongopass}@clusterchristina.57107.mongodb.net/test?retryWrites=true&w=majority&ssl=true")
db = client['Christina']
collection = db['app_people_relationships']

initial_count_of_collection = collection.count_documents({})

def delete_all(collection):
    result = collection.delete_many({})
    print(f"Deleted {result.deleted_count} documents from {collection.name}")

def insert_one_document(data):
    collection.insert_one(data)

def count_of_all_documents():
    # print(f"There are {collection.count_documents({})} documents now in {collection.name}")
    return collection.count_documents({})

# backup to mongodb
backup_script_collection_input(backup_type="app people relationships", 
                               collection_source=collection, 
                               collection_output=db['app_people_relationships_backups'])

# --------------- delete all of the contents of the db to update
delete_all(collection=collection)

creds = os.getenv("GOOGLE_CREDENTIALS")
if creds and os.path.exists(creds):
    credentials = Credentials.from_service_account_file(creds, scopes=SCOPES)
else:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not set or file does not exist!")

client = gspread.authorize(credentials)

# Open the Google Sheet by name or by URL
sheet = client.open_by_key(gsheetid).worksheet("People Relationships")

# Get all data in the sheet
data = sheet.get_all_values()

headers = data[0]
rows = data[1:]

documents = [dict(zip(headers, row)) for row in rows]

if documents:
    collection.insert_many(documents)
    print("Data successfully inserted to Mongodb")
else:
    print("No data to insert.")

df_app = get_all_data_as_dataframe()
df_app = df_app[df_app['Update Source'] == "App"]

df_final = pd.concat([df_from_fub, df_app], ignore_index=True)

# Open the Google Sheet by name or by URL
sheet = client.open_by_key(gsheetid).worksheet("People Relationships")

data = sheet.get_all_values()

df = pd.DataFrame(data[1:], columns=data[0])

backup_file_path = 'people_relationships_backup.csv'
df.to_csv(backup_file_path, index=False)

subprocess.run(['git', 'add', backup_file_path])
subprocess.run(['git', 'commit', '-m', 'Backup people relationships old to CSV'])
subprocess.run(['git', 'push'])

sheet.clear()

df_final = df_final.replace('', None)
# df_final = df_final.replace([np.inf, -np.inf], np.nan).fillna(np.nan)
df_final.drop('_id', axis=1, inplace=True)
df_final_copy = df_final.copy()

data = [df_final.columns.values.tolist()] + df_final.values.tolist()

try:
    sheet.update(data)
    print("Overwritten People Relationships")
except Exception as e:
    print(e)
    df_final_copy.to_csv('people_relationships_new.csv', index=False)
    subprocess.run(['git', 'add', backup_file_path])
    subprocess.run(['git', 'commit', '-m', 'Backup people relationships new to CSV'])
    subprocess.run(['git', 'push'])
    print("People relationships to csv instead")

final_count_of_collection = count_of_all_documents() - initial_count_of_collection
print(f'Added {final_count_of_collection} in the collection {collection.name}')
print(f'Total Number of documents: {count_of_all_documents()} in the collection {collection.name}')

hoover_tz = pytz.timezone('America/Chicago')

current_time = datetime.now(hoover_tz)
current_time_ph = datetime.now()
total_running_time = current_time_ph - current_time_ph_initial
logfile = r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'

with open(logfile, 'a') as file:
    file.write(f'\nTotal Number of documents: {count_of_all_documents()} in the collection {collection.name}')
    file.write(f'\nUpdate People Relationships Gsheet End time in USA: {current_time}')
    file.write(f'\nUpdate People Relationships Gsheet End time in PH: {current_time_ph}\n')
    file.write(f'\nUpdate People Relationships Gsheet Total Running time: {total_running_time}')


mongodb_logging(event_var=f'Total Number of documents: {count_of_all_documents()} in the collection {collection.name}', old_doc=None, new_doc=None)
mongodb_logging(event_var=f'Update People Relationships Gsheet End time in USA: {current_time}', old_doc=None, new_doc=None)
mongodb_logging(event_var=f'Update People Relationships Gsheet End time in PH: {current_time_ph}', old_doc=None, new_doc=None)
mongodb_logging(event_var=f'Update People Relationships Gsheet Total Running time: {total_running_time}', old_doc=None, new_doc=None)