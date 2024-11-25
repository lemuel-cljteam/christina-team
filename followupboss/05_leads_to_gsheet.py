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
from followupboss.scripts import mongodb_logging, backup_script_collection_input, backup_script_df_input, error_logging

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

current_time_initial = datetime.now(hoover_tz)
current_time_ph_initial = datetime.now()

api_key = os.getenv("FOLLOWUPBOSS_APIKEY")
X_System_Key = os.getenv("FOLLOWUPBOSS_XSYSTEMKEY")
X_System = os.getenv("FOLLOWUPBOSS_XSYSTEM")
mongopass = os.getenv("MONGODB_PASSWORD")
gsheetid = os.getenv("GSHEET_ID")
creds = os.getenv("GOOGLE_CREDENTIALS")

# Encode API key in Base64
encoded_api_key = base64.b64encode(api_key.encode('utf-8')).decode('utf-8')

client = MongoClient(MONGO_URI)
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
df.drop(columns=['phones', 'emails', 'addresses'], inplace=True)

# Display the DataFrame
df['Update Source'] = "Followup boss"

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

if creds and os.path.exists(creds):
    credentials = Credentials.from_service_account_file(creds, scopes=SCOPES)
else:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not set or file does not exist!")

client = gspread.authorize(credentials)

sheet = client.open_by_key(gsheetid).worksheet("Leads")
    
data = sheet.get_all_values()
df_leads = pd.DataFrame(data[1:], columns=data[0])

unique_people_id = df_leads[['Lead ID', 'ID']].copy()
# unique_people_id.replace({'ID': r'^\s*$'}, float('NaN'), regex=True, inplace=True)
unique_people_id.loc[:,'ID'] = unique_people_id['ID'].replace(r'^\s*$', float('NaN'), regex=True)
unique_people_id.dropna(subset=['ID'], inplace=True)
unique_people_id.loc[:, 'ID'] = unique_people_id['ID'].astype(int)

df.loc[:,'id'] = df['id'].astype(int)
df['realGeeksUrl'] = df['sourceUrl'].str.contains('realgeeks', case=False, na=False)

def realgeeksurl(row):
    if 'realgeeks' in row.lower():
        return row
    else:
        return None
    
df['realGeeksUrl'] = df['sourceUrl'].apply(realgeeksurl)
df.rename(
    {'id': 'ID', 'realGeeksUrl': 'RealGeeks URL'}, axis=1, inplace=True
)

print(f'length of rows of people from followup boss before merge: {len(df)}')

df_new = pd.merge(df, unique_people_id, 'left', on='ID')

print(f'length of rows of people from followup boss after merge: {len(df)}')

def generate_lead_id():
    return str(uuid.uuid4())[:8]

df_new ['Lead ID']= df_new['Lead ID'].apply(lambda x: generate_lead_id() if pd.isna(x) else x)
df_new.replace([np.inf, -np.inf, np.nan], None, inplace=True)

df_new['Budget'] = df_new['price']

df_app_only = df_leads[df_leads['Update Source'] == 'App'].copy()
df_app_only.loc[:, 'Year'] = df_app_only['Year'].astype('int')
# df_app_only['Date Added'] = pd.to_datetime(df_app_only['Date Added']).dt.date

def convert_date(row):
    try:
        # Try parsing the date without specifying the format
        return pd.to_datetime(row).strftime('%Y-%m-%d')
    except Exception:
        try:
            # Fallback to a specific format if the first attempt fails
            return pd.to_datetime(row, format='%m/%d/%Y').strftime('%Y-%m-%d')
        except Exception as e:
            error_logging(error_type="convert date in df_app_only error", error_doc=e)
            # If both fail, return NaT or another default value
            return None  # or pd.NaT for a datetime-compatible value

# Apply the function to each row in the 'Date Added' column
df_app_only['Date Added'] = df_app_only['Date Added'].apply(convert_date)
df_app_only['Last Assigned'] = df_app_only['Last Assigned'].apply(convert_date)
df_app_only['Deal Close Date'] = df_app_only['Deal Close Date'].apply(convert_date)
df_app_only['Birthday'] = df_app_only['Birthday'].apply(convert_date)
df_app_only['Closing Anniversary'] = df_app_only['Closing Anniversary'].apply(convert_date)
df_app_only['Date Reassigned'] = df_app_only['Date Reassigned'].apply(convert_date)
df_app_only['Home Anniversary'] = df_app_only['Home Anniversary'].apply(convert_date)

df_fub_only = df_new[~df_new['Lead ID'].isin(df_app_only['Lead ID'])].copy()
# df_fub_only2 = df_fub_only.copy()
# df_fub_only = df_fub_only2.copy()

# Define the new columns in a dictionary
new_columns = {
    'Year': pd.to_datetime(df_fub_only['created']).dt.year.astype('int'),
    'Month': pd.to_datetime(df_fub_only['created']).dt.strftime('%b'),
    'Agent ID': df_fub_only['assignedUserId'],
    'Date Added': pd.to_datetime(df_fub_only['created']).dt.strftime('%Y-%m-%d'),
    'Name': df_fub_only['firstName'] + ' ' + df_fub_only['lastName'],
    'First Name': df_fub_only['firstName'],
    'Last Name': df_fub_only['lastName'],
    'Stage': df_fub_only['stage'],
    'Lead Source': df_fub_only['source'],
    'Assigned To': df_fub_only['assignedTo'],
    'Last Assigned': pd.to_datetime(df_fub_only['updated']).dt.strftime('%Y-%m-%d'),
    'Is Contacted': df_fub_only['contacted'],
    'Listing Price': df_fub_only['price'],
    'Tags': df_fub_only['tags'],
    'Email 1': df_fub_only['email_1'],
    'Email 1 - Type': df_fub_only['email_type_1'],
    'Email 2': df_fub_only['email_2'],
    'Email 2 - Type': df_fub_only['email_type_2'],
    'Email 3': df_fub_only['email_3'],
    'Email 3 - Type': df_fub_only['email_type_3'],
    'Email 4': df_fub_only['email_4'],
    'Email 4 - Type': df_fub_only['email_type_4'],
    'Email 5': df_fub_only['email_5'],
    'Email 5 - Type': df_fub_only['email_type_5'],
    'Email 6': df_fub_only['email_6'],
    'Email 6 - Type': df_fub_only['email_type_6'],
    'Phone 1': df_fub_only['phone_1'],
    'Phone 1 - Type': df_fub_only['phone_type_1'],
    'Phone 2': df_fub_only['phone_2'],
    'Phone 2 - Type': df_fub_only['phone_type_2'],
    'Phone 3': df_fub_only['phone_3'],
    'Phone 3 - Type': df_fub_only['phone_type_3'],
    'Phone 4': df_fub_only['phone_4'],
    'Phone 4 - Type': df_fub_only['phone_type_4'],
    'Phone 5': df_fub_only['phone_5'],
    'Phone 5 - Type': df_fub_only['phone_type_5'],
    'Phone 6': df_fub_only['phone_6'],
    'Phone 6 - Type': df_fub_only['phone_type_6'],
    'Address 1 - Street': df_fub_only['address_street_1'],
    'Address 1 - City': df_fub_only['address_city_1'],
    'Address 1 - State': df_fub_only['address_state_1'],
    'Address 1 - Zip': df_fub_only['address_code_1'],
    'Address 1 - Country': df_fub_only['address_country_1'],
    'Address 1 - Type': df_fub_only['address_type_1'],
    'Address 2 - Street': df_fub_only['address_street_2'],
    'Address 2 - City': df_fub_only['address_city_2'],
    'Address 2 - State': df_fub_only['address_state_2'],
    'Address 2 - Zip': df_fub_only['address_code_2'],
    'Address 2 - Country': df_fub_only['address_country_2'],
    'Address 2 - Type': df_fub_only['address_type_2'],
    'Address 3 - Street': df_fub_only['address_street_3'],
    'Address 3 - City': df_fub_only['address_city_3'],
    'Address 3 - State': df_fub_only['address_state_3'],
    'Address 3 - Zip': df_fub_only['address_code_3'],
    'Address 3 - Country': df_fub_only['address_country_3'],
    'Address 3 - Type': df_fub_only['address_type_3'],
    'Address 4 - Street': df_fub_only['address_street_4'],
    'Address 4 - City': df_fub_only['address_city_4'],
    'Address 4 - State': df_fub_only['address_state_4'],
    'Address 4 - Zip': df_fub_only['address_code_4'],
    'Address 4 - Country': df_fub_only['address_country_4'],
    'Address 4 - Type': df_fub_only['address_type_4'],
    'Address 5 - Street': df_fub_only['address_street_5'],
    'Address 5 - City': df_fub_only['address_city_5'],
    'Address 5 - State': df_fub_only['address_state_5'],
    'Address 5 - Zip': df_fub_only['address_code_5'],
    'Address 5 - Country': df_fub_only['address_country_5'],
    'Address 5 - Type': df_fub_only['address_type_5'],
    'Address 6 - Street': df_fub_only['address_street_6'],
    'Address 6 - City': df_fub_only['address_city_6'],
    'Address 6 - State': df_fub_only['address_state_6'],
    'Address 6 - Zip': df_fub_only['address_code_6'],
    'Address 6 - Country': df_fub_only['address_country_6'],
    'Address 6 - Type': df_fub_only['address_type_6'],
    'Property Address': None,
    'Property City': None,
    'Property State': None,
    'Property Postal Code': None,
    'Property MLS Number': None,
    'Property Price': None,
    'Property Beds': None,
    'Property Baths': None,
    'Property Area': None,
    'Property Lot': None,
    'Message': None,
    'Description': None,
    'Notes': None,
    'Calls': None,
    'Texts': None,
    'Background': None,
    'Campaign Source': None,
    'Campaign Medium': None,
    'Campaign Term': None,
    'Campaign Content': None,
    'Campaign Name': None,
    'Deal Stage': df_fub_only['dealStage'],
    'Deal Close Date': pd.to_datetime(df_fub_only['dealCloseDate']).dt.strftime('%Y-%m-%d'),
    'Deal Price': df_fub_only['dealPrice'],
    'ID': df_fub_only['ID'],
    'Birthday': None,
    'Closing Anniversary': None,
    'Date Reassigned': None,
    'Home Anniversary': None,
    'Huddle URL': None,
    'RealGeeks URL': df_fub_only['RealGeeks URL'],
    'Timeframe': None,
    'Website': None,
    'Update Source': df_fub_only['Update Source'],
    'Budget': df_fub_only['price']
}

# Concatenate with the original DataFrame
df_fub_only = pd.concat([df_fub_only, pd.DataFrame(new_columns)], axis=1)

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

df_fub_only.reset_index(drop=True, inplace=True)
df_app_only.reset_index(drop=True, inplace=True)

# there are resulting duplicated columns here that needs troubleshooting
# print("df_fub_only columns:", df_fub_only.columns[df_fub_only.columns.duplicated()])
# print("df_app_only columns:", df_app_only.columns[df_app_only.columns.duplicated()])
# df_fub_only columns: Index(['ID', 'Update Source', 'Budget'], dtype='object')
# df_app_only columns: Index([], dtype='object')

# Drop duplicated columns by suffix pattern if they are created by merge (e.g., `_x`, `_y`)
df_fub_only = df_fub_only.loc[:, ~df_fub_only.columns.duplicated()]


# ----------
# solution to drop the second columns
df_fub_only = df_fub_only.loc[:, ~df_fub_only.columns.duplicated()]

# map agent ids from followupboss to agent ids from app

sheet = client.open_by_key(gsheetid).worksheet("Agents")

data = sheet.get_all_values()

dfAgents = pd.DataFrame(data[1:], columns=data[0])
dfAgents = dfAgents[['Agent ID', 'ID from Followupboss']]
dfAgents['ID from Followupboss'] = pd.to_numeric(dfAgents['ID from Followupboss']) 

df_fub_only = df_fub_only.merge(dfAgents, how='left', left_on='Agent ID', right_on='ID from Followupboss')
df_fub_only = df_fub_only.drop(columns=['Agent ID_x', 'ID from Followupboss'])
df_fub_only = df_fub_only.rename(
    {
        'Agent ID_y': 'Agent ID'
    }, axis=1)

df_final = pd.concat([df_fub_only, df_app_only], ignore_index=True)
# df_final.drop(['index'], axis=1, inplace=True)

df_final = df_final[[
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

sheet = client.open_by_key(gsheetid).worksheet("Leads")

data = sheet.get_all_values()

df = pd.DataFrame(data[1:], columns=data[0])

# backup to mongodb
backup_script_df_input(backup_type="app leads",
                       df_original=df,
                       collection=db['app_people_backups'])

backup_file_path = 'leads_backup.csv'
# df.to_csv(backup_file_path, index=False)
# subprocess.run(['git', 'add', backup_file_path])
# subprocess.run(['git', 'commit', '-m', 'Backup leads old to CSV'])
# subprocess.run(['git', 'push'])

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
    error_logging(error_type="cannot write leads to gsheet error", error_doc=e)
    print(e)
    # df_final_snap.to_csv('leads_new.csv', index=False)
    # subprocess.run(['git', 'add', backup_file_path])
    # subprocess.run(['git', 'commit', '-m', 'Backup leads new to CSV'])
    # subprocess.run(['git', 'push'])
    # print("Leads new to csv instead")
    # df = pd.read_csv('leads_new.csv')
    # sheet.update([df.columns.values.tolist()] + df.values.tolist())
    # print("Leads new copied to gsheet")

hoover_tz = pytz.timezone('America/Chicago')

current_time = datetime.now(hoover_tz)
current_time_ph = datetime.now()
total_running_time = current_time_ph - current_time_ph_initial

with open(logfile, 'a') as file:
    file.write(f'\nTotal Number of leads: {len(df_final)} in the collection {collection.name}')
    file.write(f'\nUpdate Leads Gsheet End time in USA: {current_time}')
    file.write(f'\nUpdate Leads Gsheet End time in PH: {current_time_ph}\n')
    file.write(f'\nUpdate Leads Gsheet Total Running time: {total_running_time}')