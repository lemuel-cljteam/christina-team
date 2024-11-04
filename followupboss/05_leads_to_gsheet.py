from datetime import datetime
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
import subprocess

working_directory = os.getcwd()
# r'c:\\Users\\ENDUSER\\OneDrive\\FOR CHRISTINA\\Python\\ETLs\\followupboss\\logs.txt'
logfile = os.path.join(working_directory, "followupboss", "logs.txt")

hoover_tz = pytz.timezone('America/Chicago')

current_time_initial = datetime.now(hoover_tz)
current_time_ph_initial = datetime.now()

api_key = os.getenv("FOLLOWUPBOSS_APIKEY")
X_System_Key = os.getenv("FOLLOWUPBOSS_XSYSTEMKEY")
X_System = os.getenv("FOLLOWUPBOSS_XSYSTEM")
mongopass = os.getenv("MONGODB_PASSWORD")
gsheetid = os.getenv("GSHEET_ID")

# Encode API key in Base64
encoded_api_key = base64.b64encode(api_key.encode('utf-8')).decode('utf-8')

client = MongoClient(F"mongodb+srv://christina:{mongopass}@clusterchristina.57107.mongodb.net/test?retryWrites=true&w=majority&ssl=true")
db = client['Christina']
collection = db['followupboss_people']

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

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# creds = Credentials.from_service_account_file(os.path.join(working_directory, "credentials.json"), scopes=SCOPES)
creds = os.getenv("GOOGLE_CREDENTIALS")
if creds and os.path.exists(creds):
    credentials = Credentials.from_service_account_file(creds, scopes=SCOPES)
else:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not set or file does not exist!")

client = gspread.authorize(credentials)

sheet = client.open_by_key(gsheetid).worksheet("Leads")
    
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
df_app_only['Year'] = df_app_only['Year'].astype('int')
df_app_only['Date Added'] = pd.to_datetime(df_app_only['Date Added']).dt.date
df_app_only['Last Assigned'] = pd.to_datetime(df_app_only['Last Assigned'])
df_app_only['Deal Close Date'] = pd.to_datetime(df_app_only['Deal Close Date'])

df_fub_only = df_new[~df_new['Lead ID'].isin(df_app_only['Lead ID'])]
df_fub_only['Update Source'] = 'Followup boss'
# df_fub_only2 = df_fub_only.copy()
# df_fub_only = df_fub_only2.copy()

df_fub_only['Lead ID'] = df_fub_only['Lead ID']
df_fub_only['Year'] = pd.to_datetime(df_fub_only['created']).dt.year
df_fub_only['Month'] = pd.to_datetime(df_fub_only['created']).dt.strftime('%b')
df_fub_only['Agent ID'] = df_fub_only['assignedUserId']
df_fub_only['Date Added'] = pd.to_datetime(df_fub_only['created']).dt.date
df_fub_only['Name'] = df_fub_only['firstName'] + ' ' + df_fub_only['lastName']
df_fub_only['First Name'] = df_fub_only['firstName']
df_fub_only['Last Name'] = df_fub_only['lastName']
df_fub_only['Stage'] = df_fub_only['stage']
df_fub_only['Lead Source'] = df_fub_only['source']
df_fub_only['Assigned To'] = df_fub_only['assignedTo']
df_fub_only['Last Assigned'] = pd.to_datetime(df_fub_only['updated'])
df_fub_only['Is Contacted'] = df_fub_only['contacted']
df_fub_only['Listing Price'] = df_fub_only['price']
df_fub_only['Tags'] = df_fub_only['tags']
df_fub_only['Email 1'] = df_fub_only['email_1']
df_fub_only['Email 1 - Type'] = df_fub_only['email_type_1']
df_fub_only['Email 2'] = df_fub_only['email_2']
df_fub_only['Email 2 - Type'] = df_fub_only['email_type_2']
df_fub_only['Email 3'] = df_fub_only['email_3']
df_fub_only['Email 3 - Type'] = df_fub_only['email_type_3']
df_fub_only['Email 4'] = df_fub_only['email_4']
df_fub_only['Email 4 - Type'] = df_fub_only['email_type_4']
df_fub_only['Email 5'] = df_fub_only['email_5']
df_fub_only['Email 5 - Type'] = df_fub_only['email_type_5']
df_fub_only['Email 6'] = df_fub_only['email_6']
df_fub_only['Email 6 - Type'] = df_fub_only['email_type_6']
df_fub_only['Phone 1'] = df_fub_only['phone_1']
df_fub_only['Phone 1 - Type'] = df_fub_only['phone_type_1']
df_fub_only['Phone 2'] = df_fub_only['phone_2']
df_fub_only['Phone 2 - Type'] = df_fub_only['phone_type_2']
df_fub_only['Phone 3'] = df_fub_only['phone_3']
df_fub_only['Phone 3 - Type'] = df_fub_only['phone_type_3']
df_fub_only['Phone 4'] = df_fub_only['phone_4']
df_fub_only['Phone 4 - Type'] = df_fub_only['phone_type_4']
df_fub_only['Phone 5'] = df_fub_only['phone_5']
df_fub_only['Phone 5 - Type'] = df_fub_only['phone_type_5']
df_fub_only['Phone 6'] = df_fub_only['phone_6']
df_fub_only['Phone 6 - Type'] = df_fub_only['phone_type_6']
df_fub_only['Address 1 - Street'] = df_fub_only['address_street_1']
df_fub_only['Address 1 - City'] = df_fub_only['address_city_1']
df_fub_only['Address 1 - State'] = df_fub_only['address_state_1']
df_fub_only['Address 1 - Zip'] = df_fub_only['address_code_1']
df_fub_only['Address 1 - Country'] = df_fub_only['address_country_1']
df_fub_only['Address 1 - Type'] = df_fub_only['address_type_1']
df_fub_only['Address 2 - Street'] = df_fub_only['address_street_2']
df_fub_only['Address 2 - City'] = df_fub_only['address_city_2']
df_fub_only['Address 2 - State'] = df_fub_only['address_state_2']
df_fub_only['Address 2 - Zip'] = df_fub_only['address_code_2']
df_fub_only['Address 2 - Country'] = df_fub_only['address_country_2']
df_fub_only['Address 2 - Type'] = df_fub_only['address_type_2']
df_fub_only['Address 3 - Street'] = df_fub_only['address_street_3']
df_fub_only['Address 3 - City'] = df_fub_only['address_city_3']
df_fub_only['Address 3 - State'] = df_fub_only['address_state_3']
df_fub_only['Address 3 - Zip'] = df_fub_only['address_code_3']
df_fub_only['Address 3 - Country'] = df_fub_only['address_country_3']
df_fub_only['Address 3 - Type'] = df_fub_only['address_type_3']
df_fub_only['Address 4 - Street'] = df_fub_only['address_street_4']
df_fub_only['Address 4 - City'] = df_fub_only['address_city_4']
df_fub_only['Address 4 - State'] = df_fub_only['address_state_4']
df_fub_only['Address 4 - Zip'] = df_fub_only['address_code_4']
df_fub_only['Address 4 - Country'] = df_fub_only['address_country_4']
df_fub_only['Address 4 - Type'] = df_fub_only['address_type_4']
df_fub_only['Address 5 - Street'] = df_fub_only['address_street_5']
df_fub_only['Address 5 - City'] = df_fub_only['address_city_5']
df_fub_only['Address 5 - State'] = df_fub_only['address_state_5']
df_fub_only['Address 5 - Zip'] = df_fub_only['address_code_5']
df_fub_only['Address 5 - Country'] = df_fub_only['address_country_5']
df_fub_only['Address 5 - Type'] = df_fub_only['address_type_5']
df_fub_only['Address 6 - Street'] = df_fub_only['address_street_6']
df_fub_only['Address 6 - City'] = df_fub_only['address_city_6']
df_fub_only['Address 6 - State'] = df_fub_only['address_state_6']
df_fub_only['Address 6 - Zip'] = df_fub_only['address_code_6']
df_fub_only['Address 6 - Country'] = df_fub_only['address_country_6']
df_fub_only['Address 6 - Type'] = df_fub_only['address_type_6']
df_fub_only['Property Address'] = None
df_fub_only['Property City'] = None
df_fub_only['Property State'] = None
df_fub_only['Property Postal Code'] = None
df_fub_only['Property MLS Number'] = None
df_fub_only['Property Price'] = None
df_fub_only['Property Beds'] = None
df_fub_only['Property Baths'] = None
df_fub_only['Property Area'] = None
df_fub_only['Property Lot'] = None
df_fub_only['Message'] = None
df_fub_only['Description'] = None
df_fub_only['Notes'] = None
df_fub_only['Calls'] = None
df_fub_only['Texts'] = None
df_fub_only['Background'] = None
df_fub_only['Campaign Source'] = None
df_fub_only['Campaign Medium'] = None
df_fub_only['Campaign Term'] = None
df_fub_only['Campaign Content'] = None
df_fub_only['Campaign Name'] = None
df_fub_only['Deal Stage'] = df_fub_only['dealStage']
df_fub_only['Deal Close Date'] = pd.to_datetime(df_fub_only['dealCloseDate'])
df_fub_only['Deal Price'] = df_fub_only['dealPrice']
df_fub_only['ID'] = df_fub_only['id']
df_fub_only['Birthday'] = None
df_fub_only['Closing Anniversary'] = None
df_fub_only['Date Reassigned'] = None
df_fub_only['Home Anniversary'] = None
df_fub_only['Huddle URL'] = None
df_fub_only['RealGeeks URL'] = None
df_fub_only['Timeframe'] = None
df_fub_only['Website'] = None
df_fub_only['Update Source'] = df_fub_only['Update Source']
df_fub_only['Budget'] = df_fub_only['price']

df_fub_only = df_fub_only[[
    'Lead ID',
    'Year',
    'Month',
    'Agent ID',
    'Date Added',
    'Name',
    'First Name',
    'Last Name',
    'Stage',
    'Lead Source',
    'Assigned To',
    'Last Assigned',
    'Is Contacted',
    'Listing Price',
    'Tags',
    'Email 1',
    'Email 1 - Type',
    'Email 2',
    'Email 2 - Type',
    'Email 3',
    'Email 3 - Type',
    'Email 4',
    'Email 4 - Type',
    'Email 5',
    'Email 5 - Type',
    'Email 6',
    'Email 6 - Type',
    'Phone 1',
    'Phone 1 - Type',
    'Phone 2',
    'Phone 2 - Type',
    'Phone 3',
    'Phone 3 - Type',
    'Phone 4',
    'Phone 4 - Type',
    'Phone 5',
    'Phone 5 - Type',
    'Phone 6',
    'Phone 6 - Type',
    'Address 1 - Street',
    'Address 1 - City',
    'Address 1 - State',
    'Address 1 - Zip',
    'Address 1 - Country',
    'Address 1 - Type',
    'Address 2 - Street',
    'Address 2 - City',
    'Address 2 - State',
    'Address 2 - Zip',
    'Address 2 - Country',
    'Address 2 - Type',
    'Address 3 - Street',
    'Address 3 - City',
    'Address 3 - State',
    'Address 3 - Zip',
    'Address 3 - Country',
    'Address 3 - Type',
    'Address 4 - Street',
    'Address 4 - City',
    'Address 4 - State',
    'Address 4 - Zip',
    'Address 4 - Country',
    'Address 4 - Type',
    'Address 5 - Street',
    'Address 5 - City',
    'Address 5 - State',
    'Address 5 - Zip',
    'Address 5 - Country',
    'Address 5 - Type',
    'Address 6 - Street',
    'Address 6 - City',
    'Address 6 - State',
    'Address 6 - Zip',
    'Address 6 - Country',
    'Address 6 - Type',
    'Property Address',
    'Property City',
    'Property State',
    'Property Postal Code',
    'Property MLS Number',
    'Property Price',
    'Property Beds',
    'Property Baths',
    'Property Area',
    'Property Lot',
    'Message',
    'Description',
    'Notes',
    'Calls',
    'Texts',
    'Background',
    'Campaign Source',
    'Campaign Medium',
    'Campaign Term',
    'Campaign Content',
    'Campaign Name',
    'Deal Stage',
    'Deal Close Date',
    'Deal Price',
    'ID',
    'Birthday',
    'Closing Anniversary',
    'Date Reassigned',
    'Home Anniversary',
    'Huddle URL',
    'RealGeeks URL',
    'Timeframe',
    'Website',
    'Update Source',
    'Budget'
]]

# df_app_only.reset_index(inplace=True)

df_final = pd.concat([df_fub_only, df_app_only], ignore_index=True)
# df_final.drop(['index'], axis=1, inplace=True)

sheet = client.open_by_key(gsheetid).worksheet("Leads")

data = sheet.get_all_values()

df = pd.DataFrame(data[1:], columns=data[0])

backup_file_path = 'leads_backup.csv'
df.to_csv(backup_file_path, index=False)

sheet.clear()

# df_final = df_final.replace('', None)
# df_final.replace('NaN', '', inplace=True)
# df_final = df_final.replace([np.inf, -np.inf], None)
# df_final.fillna('', inplace=True)
df_final.replace([np.nan, None, 'NaN'], '', inplace=True)
# df_final = df_final.astype(str).replace('nan', '')  # Replace string 'nan' with empty string
# df_final.drop('_id', axis=1, inplace=True)
# df_final.drop('level_0', axis=1, inplace=True)

df_final_snap = df_final.copy()
df_final['Tags'] = df_final['Tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
df_final['Tags'].value_counts()

# for col in df_final.select_dtypes(include=['object']).columns:
#     df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
# data = [df_final.columns.values.tolist()] + df_final.values.tolist()
# data = [df_final.columns.values.tolist()] + df_final.astype(str).values.tolist()

data = df_final.values.tolist()
data.insert(0, df_final.columns.tolist())  # Add header

# Update the Google Sheet with the data
# try:
#     sheet.update(data)
#     print("Overwritten Leads")
# except Exception as e:
#     print("Error updating the sheet:", e)

try:
    sheet.update(data)
    print("Overwritten Leads")
except Exception as e:
    print(e)
    df_final_snap.to_csv('leads.csv', index=False)
    subprocess.run(['git', 'add', backup_file_path])
    subprocess.run(['git', 'commit', '-m', 'Backup leads to CSV'])
    subprocess.run(['git', 'push'])
    print("Leads to csv instead")

hoover_tz = pytz.timezone('America/Chicago')

current_time = datetime.now(hoover_tz)
current_time_ph = datetime.now()
total_running_time = current_time_ph - current_time_ph_initial

with open(logfile, 'a') as file:
    file.write(f'\nTotal Number of leads: {len(df_final)} in the collection {collection.name}')
    file.write(f'\nUpdate Leads Gsheet End time in USA: {current_time}')
    file.write(f'\nUpdate Leads Gsheet End time in PH: {current_time_ph}\n')
    file.write(f'\nUpdate Leads Gsheet Total Running time: {total_running_time}')