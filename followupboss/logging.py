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

def logging(event_var, old_doc, new_doc):
    collection.insert_one({
        "date_inserted": datetime.now(),
        "event": event_var,
        "old_doc": old_doc,
        "new_doc": new_doc
    })