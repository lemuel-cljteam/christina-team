import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import uuid
from datetime import datetime as dt
from pymongo import MongoClient
from pymongoarrow.api import find_pandas_all
import pytz
import os

working_directory = os.getcwd()

hoover_tz = pytz.timezone('America/Chicago')

current_time_initial = dt.now(hoover_tz)
current_time_ph_initial = dt.now()

# r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'
logfile = os.path.join(working_directory, "followupboss", "logs.txt")

with open(logfile, 'a') as file:
    file.write(f'\nUpdate People Relationships Gsheet Start time in USA: {current_time_initial}')
    file.write(f'\nUpdate People Relationships Gsheet Start time in PH: {current_time_ph_initial}\n')

#run first the peoplerelationships.py

# Connect to MongoDB (adjust the connection string as needed)
client = MongoClient("mongodb+srv://christina:akodcXC3gIB2qhYf@clusterchristina.57107.mongodb.net/test?retryWrites=true&w=majority&ssl=true")
db = client['Christina']
collection = db['followupboss_people_relationships']

def get_all_data_as_dataframe():
    # Retrieve all documents in the collection
    documents = list(collection.find({}))  # Convert the cursor to a list of dictionaries

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(documents)

    return df

# Example usage
df = get_all_data_as_dataframe()

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
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Load the service account credentials
creds = Credentials.from_service_account_file(r'C:\Users\ENDUSER\OneDrive\FOR CHRISTINA\Python\ETLs\credentials.json', scopes=SCOPES)

# Authorize the client with the credentials
client = gspread.authorize(creds)

# Open the Google Sheet by name or by URL
sheet = client.open_by_key("1UAtfmU1LSsIvfFBDdS0cpUrrhsW9ZDC0mDKF1kj8Ato").worksheet("Leads")

# Get all data in the sheet
data = sheet.get_all_values()

# Convert the data into a pandas DataFrame
df_1 = pd.DataFrame(data[1:], columns=data[0])

unique_people_id = df_1[['Lead ID', 'ID']]
unique_people_id['ID'].replace(r'^\s*$', float('NaN'), regex=True, inplace=True)
unique_people_id.dropna(subset=['ID'], inplace=True)

df['People ID From FB'] = df['People ID From FB'].astype(int)
unique_people_id['ID'] = unique_people_id['ID'].astype(int)

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
df_new['Relationship ID'] = df_new['Lead ID'] + '-' + df_new['Row Number'].astype(str)
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

client = MongoClient()
db = client['Christina']
collection = db['app_people_relationships']

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

# Define the scope (read-only access to Sheets)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Load the service account credentials
creds = Credentials.from_service_account_file(r'C:\Users\ENDUSER\OneDrive\FOR CHRISTINA\Python\ETLs\credentials.json', scopes=SCOPES)

# Authorize the client with the credentials
client = gspread.authorize(creds)

# Open the Google Sheet by name or by URL
sheet = client.open_by_key("1UAtfmU1LSsIvfFBDdS0cpUrrhsW9ZDC0mDKF1kj8Ato").worksheet("People Relationships")

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

def get_all_data_as_dataframe():
    # Retrieve all documents in the collection
    documents = list(collection.find({}))  # Convert the cursor to a list of dictionaries

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(documents)

    return df

df_app = get_all_data_as_dataframe()
df_app = df_app[df_app['Update Source'] == "App"]

df_final = pd.concat([df_from_fub, df_app], ignore_index=True)

# Define the scope (read-only access to Sheets)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Load the service account credentials
creds = Credentials.from_service_account_file(r'C:\Users\ENDUSER\OneDrive\FOR CHRISTINA\Python\ETLs\credentials.json', scopes=SCOPES)

# Authorize the client with the credentials
client = gspread.authorize(creds)

# Open the Google Sheet by name or by URL
sheet = client.open_by_key("1UAtfmU1LSsIvfFBDdS0cpUrrhsW9ZDC0mDKF1kj8Ato").worksheet("People Relationships")

sheet.clear()

df_final = df_final.replace('', None)
# df_final = df_final.replace([np.inf, -np.inf], np.nan).fillna(np.nan)
df_final.drop('_id', axis=1, inplace=True)

data = [df_final.columns.values.tolist()] + df_final.values.tolist()

sheet.update(data)
print("Overwritten People Relationships")

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
    file.write(f'\nUpdate People Relationships Gsheet End time in USA: {current_time}')
    file.write(f'\nUpdate People Relationships Gsheet End time in PH: {current_time_ph}\n')
    file.write(f'\nUpdate People Relationships Gsheet Total Running time: {total_running_time}')
