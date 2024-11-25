import base64
import json

def convert_to_base64(json_file, output_file):
    # 1. Read the JSON file (ensure the file path is correct)
    with open(json_file, "rb") as file:  # Open the file in binary mode
        # 2. Read the content of the file
        file_content = file.read()

    # 3. Encode the file content to Base64
    encoded_content = base64.b64encode(file_content).decode("utf-8")  # Decode to string for readability

    with open(output_file, 'w') as f:
        f.write(encoded_content)

import os
from pymongo import MongoClient
from datetime import datetime

api_key = os.getenv("FOLLOWUPBOSS_APIKEY")
X_System_Key = os.getenv("FOLLOWUPBOSS_XSYSTEMKEY")
X_System = os.getenv("FOLLOWUPBOSS_XSYSTEM")
MONGO_URI = os.getenv("MONGO_URI")
gsheetid = os.getenv("GSHEET_ID")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client['Christina']
collection = db['event_logs']

def mongodb_logging(event_var, old_doc, new_doc):
    collection.insert_one({
        "date_inserted": datetime.now(),
        "event": event_var,
        "old_doc": old_doc,
        "new_doc": new_doc
    })


def backup_script_df_input(backup_type, df_original, collection):
    # backup to mongodb
    df_backup = df_original.copy()
    df_backup['date_inserted'] = datetime.now()
    collection_backup = collection

    df_dict = df_backup.to_dict(orient="records")
    record_backup = {
        "backup_type": backup_type,
        "date_inserted": datetime.now(), 
        "data": df_dict
    }
    collection_backup.insert_one(record_backup)

def backup_script_collection_input(backup_type, collection_source, collection_output):
    documents = list(collection_source.find())

    backup_document = {
        'backup_type': backup_type,
        'date_inserted': datetime.now(),
        'data': documents
    }

    collection_output.insert_one(backup_document)