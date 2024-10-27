import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import uuid
from datetime import datetime as dt
from pymongo import MongoClient
from pymongoarrow.api import find_pandas_all
import pytz

hoover_tz = pytz.timezone('America/Chicago')

current_time_initial = dt.now(hoover_tz)
current_time_ph_initial = dt.now()
logfile = r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'

with open(logfile, 'a') as file:
    file.write(f'\nUpdate People Gsheet Start time in USA: {current_time_initial}')
    file.write(f'\nUpdate People Gsheet Start time in PH: {current_time_ph_initial}\n')

#run first the peoplerelationships.py

# Connect to MongoDB (adjust the connection string as needed)
client = MongoClient()
db = client['Christina']
collection = db['followupboss_people_backup']

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