#!/usr/bin/env python
# coding: utf-8

# In[164]:


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
import threading
from bson import ObjectId
import json
import logging
from urllib.parse import quote_plus
from bson import DBRef
import datetime


# In[165]:


username = "AdminRoot"
password = quote_plus("m@ster!11")

# Connection to sahwi instance
MONGO_URI = (
    f"mongodb://{username}:{password}@10.250.20.215:27017/?authSource=admin&readPreference=primary&ssl=false"
)


# In[166]:


MYSQL_CONFIG = {
    "host": "nc01-ntss-dw-db01.ci3sbsdosha7.eu-central-1.rds.amazonaws.com",
    "user": "admin",
    "password": "p=.81E9o?(UWy5Izt417:808",
    "database": "digital_platforms"
}


# In[167]:


# === LOGGING ===
logging.basicConfig(filename='mongo_sync.log', level=logging.INFO)


# In[168]:


# === CONNECT TO DATABASES ===
mongo_client = MongoClient(MONGO_URI)


# In[169]:


mysql_conn = mysql.connector.connect(**MYSQL_CONFIG, autocommit=True, connection_timeout=60)
mysql_cursor = mysql_conn.cursor()


# In[170]:


# === HELPER TO SAFELY EXTRACT DATETIME ===
def extract_datetime(val):
    if isinstance(val, dict) and "$date" in val:
        return val["$date"]
    elif isinstance(val, datetime.datetime):
        return val
    else:
        return None


# In[171]:


COLLECTIONS = {
    "whatsapp_sessions": {
        "mongo_db": "bots_db",
        "collection": "whatsapp_sessions",
        "table": "whatsapp_sessions",
        "fields": {
            "_id": lambda d: str(d.get("_id")),
            "option": lambda d: d.get("option"),
            "cartOption": lambda d: d.get("cartOption"),
            "globalOption": lambda d: d.get("globalOption"),
            "phoneNumber": lambda d: d.get("phoneNumber"),
            "regPolicyNumber": lambda d: d.get("regPolicyNumber"),
            "dateModified": lambda d: d.get("dateModified"),
            "dateCreated": lambda d: d.get("dateCreated"),
            "name": lambda d: d.get("name"),
            "paymentMethod": lambda d: d.get("paymentMethod"),
            "policyNumber": lambda d: d.get("policyNumber"),
            "timestamp": lambda d: datetime.datetime.fromtimestamp(d.get("timestamp") / 1000) if d.get("timestamp") else None
            # "agent_displayName": lambda d: d.get("agent", {}).get("firstName"),
            # "agent_displayName": lambda d: d.get("agent", {}).get("lastName")
        }
    },
      "ussd_sessions": {
        "mongo_db": "customer_db",
        "collection": "ussd_sessions",  # first collection
        "table": "ussd_sessions",
        "fields": {
            "_id": lambda d: str(d.get("_id")),
            "phoneNumber": lambda d: d.get("phoneNumber"),
            "transactionID": lambda d: d.get("transactionID"),
            "sourceNumber": lambda d: d.get("sourceNumber"),
            "mainMemberId": lambda d: d.get("mainMemberId"),
            "stage": lambda d: d.get("stage"),
            "transactionType": lambda d: d.get("transactionType"),
            "policyNumber": lambda d: d.get("policyNumber"),
            "idNumber": lambda d: d.get("idNumber"),
            "claimPrompt": lambda d: d.get("claimPrompt"),
            "dateCreated": lambda d: extract_datetime(d.get("dateCreated")),
            "dateModified": lambda d: extract_datetime(d.get("dateModified")),
            "service_provider": lambda d: "econet"
        }
    },
     "netone_ussd_sessions": {
         "mongo_db": "customer_db",
         "collection": "netone_ussd_sessions",
         "table": "ussd_sessions_netone",
         "fields": {
            "_id": lambda d: f"netone_{str(d.get('_id'))}",
            "phoneNumber": lambda d: d.get("phoneNumber"),
            "dateCreated": lambda d: extract_datetime(d.get("dateCreated")),
            "active": lambda d: d.get("active"),
            "deleted": lambda d: d.get("deleted"),
            "version": lambda d: d.get("version"),
            "transactionID": lambda d: None,
            "sourceNumber": lambda d: None,
            "stage": lambda d: None,
            "transactionType": lambda d: None,
            "policyNumber": lambda d: None,
            "idNumber": lambda d: None,
            "claimPrompt": lambda d: None,
            "dateModified": lambda d: None,
            "service_provider": lambda d: "netone"
     }   
    },
      "mobile_app_clients": {
        "mongo_db": "customer_db",
        "collection": "mobile_app_clients",
        "table": "mobile_app_clients",
        "fields": {
            "_id": lambda d: str(d.get("_id")),
            "clientID": lambda d: d.get("clientID"),
            "firstName": lambda d: d.get("firstName"),
            "lastName": lambda d: d.get("lastName"),
            "idNumber": lambda d: d.get("idNumber"),
            "isPolicyHolder": lambda d: d.get("isPolicyHolder"),
            "mobileNumber": lambda d: d.get("mobileNumber"),
            "customerType": lambda d: d.get("customerType"),
            "policyNumber": lambda d: d.get("policyNumber"),
            "dateCreated": lambda d: extract_datetime(d.get("dateCreated"))
        }
    },
      "main_member_alteration": {
        "mongo_db": "easipol_service",
        "collection": "main_member_alteration",
        "table": "all_alterations",  # unified target
        "fields": {
            "_id": lambda d: str(d.get("_id")),
            "policyNumber": lambda d: d.get("policyNumber"),
            "alterationType": lambda d: d.get("alterationType"),
            "alterationSource": lambda d: d.get("alterationSource"),
            "alterationStatus": lambda d: d.get("alterationStatus"),
            "isVerified": lambda d: d.get("isVerified"),
            "dateCreated": lambda d: extract_datetime(d.get("dateCreated"))
        }
    },
    "child_alteration": {
        "mongo_db": "easipol_service",
        "collection": "child_alteration",
        "table": "all_alterations",
        "fields": {
            "_id": lambda d: str(d.get("_id")),
            "policyNumber": lambda d: d.get("policyNumber"),
            "alterationType": lambda d: d.get("alterationType"),
            "alterationSource": lambda d: d.get("alterationSource"),
            "alterationStatus": lambda d: d.get("alterationStatus"),
            "isVerified": lambda d: d.get("isVerified"),
            "dateCreated": lambda d: extract_datetime(d.get("dateCreated"))
        }
    },
    "extended_member_alteration": {
        "mongo_db": "easipol_service",
        "collection": "extended_member_alteration",
        "table": "all_alterations",
        "fields": {
            "_id": lambda d: str(d.get("_id")),
            "policyNumber": lambda d: d.get("policyNumber"),
            "alterationType": lambda d: d.get("alterationType"),
            "alterationSource": lambda d: d.get("alterationSource"),
            "alterationStatus": lambda d: d.get("alterationStatus"),
            "isVerified": lambda d: d.get("isVerified"),
            "dateCreated": lambda d: extract_datetime(d.get("dateCreated"))
        }
    },
    "spouse_alteration": {
        "mongo_db": "easipol_service",
        "collection": "spouse_alteration",
        "table": "all_alterations",
        "fields": {
            "_id": lambda d: str(d.get("_id")),
            "policyNumber": lambda d: d.get("policyNumber"),
            "alterationType": lambda d: d.get("alterationType"),
            "alterationSource": lambda d: d.get("alterationSource"),
            "alterationStatus": lambda d: d.get("alterationStatus"),
            "isVerified": lambda d: d.get("isVerified"),
            "dateCreated": lambda d: extract_datetime(d.get("dateCreated"))
        }
    },
    "upgrade_downgrade_alteration": {
        "mongo_db": "easipol_service",
        "collection": "upgrade_downgrade_alteration",
        "table": "upgrade_downgrades",
        "fields": {
            "_id": lambda d: str(d.get("_id")),
            "policyNumber": lambda d: d.get("policyNumber"),
            "oldPlan": lambda d: d.get("oldPlan"),
            "newPlan": lambda d: d.get("newPlan"),
            "isVerified": lambda d: d.get("isVerified"),
            "subPlanType": lambda d: d.get("subPlanType"),
            "memberType": lambda d: d.get("memberType"),
            "firstName": lambda d: d.get("firstName"),
            "lastName": lambda d: d.get("lastName"),
            "alterationStatus": lambda d: d.get("alterationStatus"),
            "alterationSource": lambda d: d.get("alterationSource"),
            "dateCreated": lambda d: extract_datetime(d.get("dateCreated"))
        }
    },
}   


# In[172]:


# === UTILITY FUNCTIONS ===

def sanitize_for_json(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, DBRef):
        return {
            "$ref": obj.collection,
            "$id": str(obj.id)
        }
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    else:
        return obj

def infer_sql_type(value):
    if isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "DOUBLE"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, (dict, list)):
        return "TEXT"
    elif isinstance(value, datetime.datetime):
        return "DATETIME"
    else:
        return "VARCHAR(255)"

def create_table_from_document(document, table_name, fields):
    columns = []
    for key in fields:
        value = fields[key](document)
        sql_type = infer_sql_type(value)
        column_def = f"`{key}` {sql_type}"
        if key == "_id":
            column_def += " PRIMARY KEY"
        columns.append(column_def)

    table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(columns)}
    );
    """
    mysql_cursor.execute(table_query)
    mysql_conn.commit()

def upsert_mysql_batch(documents, table_name, fields):
    if not documents:
        return

    keys = list(fields.keys())
    columns = ", ".join(f"`{k}`" for k in keys)
    placeholders = ", ".join(["%s"] * len(keys))
    updates = ", ".join(f"`{k}`=VALUES(`{k}`)" for k in keys)

    sql = f"""
    INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {updates};
    """

    batch_values = []
    for doc in documents:
        values = []
        for key in keys:
            raw_value = fields[key](doc)
            sanitized = sanitize_for_json(raw_value)
            if isinstance(sanitized, (dict, list)):
                values.append(json.dumps(sanitized))
            elif isinstance(sanitized, datetime.datetime):
                values.append(sanitized.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                values.append(sanitized)
        batch_values.append(tuple(values))

    try:
        mysql_cursor.executemany(sql, batch_values)
        mysql_conn.commit()
    except mysql.connector.Error as e:
        logging.error(f"MySQL batch insert failed: {e}")
        mysql_conn.rollback()


def delete_mysql_row(_id, table_name):
    mysql_cursor.execute(f"DELETE FROM `{table_name}` WHERE `_id` = %s", (str(_id),))
    mysql_conn.commit()

def migrate_existing_documents(mongo_collection, table_name, fields, batch_size=10000):
    print(f"📦 Migrating from MongoDB collection '{mongo_collection.name}' to MySQL table '{table_name}'...")

    sample_doc = mongo_collection.find_one()
    if not sample_doc:
        logging.warning(f"⚠️ Collection '{mongo_collection.name}' is empty. Skipping.")
        return

    create_table_from_document(sample_doc, table_name, fields)

    cursor = mongo_collection.find(no_cursor_timeout=True)
    batch = []
    count = 0

    try:
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                upsert_mysql_batch(batch, table_name, fields)
                count += len(batch)
                logging.info(f"→ Inserted {count} records into {table_name}")
                batch.clear()

        if batch:
            upsert_mysql_batch(batch, table_name, fields)
            count += len(batch)
            logging.info(f"→ Inserted final {len(batch)} records into {table_name}")
    finally:
        cursor.close()

    print(f"✅ Finished migrating {count} records to '{table_name}'.")



# In[173]:


# for real time updates, we can use MongoDB change streams
# def listen_to_changes(mongo_collection, table_name, fields):
#     print(f"📡 Listening to MongoDB changes on '{mongo_collection.name}'...")
#     with mongo_collection.watch() as stream:
#         for change in stream:
#             try:
#                 op = change["operationType"]
#                 if op == "insert":
#                     doc = change["fullDocument"]
#                     upsert_mysql_row(doc, table_name, fields)
#                     logging.info(f"Inserted: {doc.get('_id')} → {table_name}")
#                 elif op == "update":
#                     _id = change["documentKey"]["_id"]
#                     updated = change["updateDescription"]["updatedFields"]
#                     updated["_id"] = _id
#                     upsert_mysql_row(updated, table_name, fields)
#                     logging.info(f"Updated: {_id} → {table_name}")
#                 elif op == "delete":
#                     _id = change["documentKey"]["_id"]
#                     delete_mysql_row(_id, table_name)
#                     logging.info(f"Deleted: {_id} → {table_name}")
#             except Exception as e:
#                 logging.error(f"Error processing change on {table_name}: {e}")


# In[174]:


# if __name__ == "__main__":
#     for coll_name, config in COLLECTIONS.items():
#         db_name = config.get("mongo_db", MONGO_DB_NAME)
#         mongo_collection = mongo_client[db_name][coll_name]
#         table_name = config["table"]
#         fields = config["fields"]

#         migrate_existing_documents(mongo_collection, table_name, fields)

#         threading.Thread(
#             target=listen_to_changes,
#             args=(mongo_collection, table_name, fields),
#             daemon=True
#         ).start()

#     # Keep main thread alive
#     while True:
#         pass

def main():
    for key, config in COLLECTIONS.items():
        if "mongo_db" not in config or "collection" not in config:
            print(f"⚠️ Skipping {key} — missing 'mongo_db' or 'collection'")
            continue

        db_name = config["mongo_db"]
        collection_name = config["collection"]
        table_name = config["table"]
        fields = config["fields"]

        mongo_collection = mongo_client[db_name][collection_name]
        print(f"🔄 Starting sync: {collection_name} → {table_name}")
        migrate_existing_documents(mongo_collection, table_name, fields)

if __name__ == "__main__":
    main()

