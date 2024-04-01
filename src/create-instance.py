from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import GoogleCredentials
import sys
import json
import time




num_args = len(sys.argv)
epoch_time = int(time.time()) 


project_id = 'hardy-device-327220'  # Replace with your Google Cloud Project ID

'''
instance_name = 'testabcdfgh'  # Replace with your instance name
user_name = 'hybrid-SQL-BM'  # Replace with your desired username
password = 'earhbrdfafwwrgfsdv'  # Replace with your desired password
database_name = 'hybrid-SQL-BM'  # Replace with your desired database name
machine_type = 'db-perf-optimized-N-2'  # Replace with your desired machine type
'''

if num_args != 7:
    print("Need in order: instance_name, user_name, password, database_name, machine_type")
    sys.exit()
else:
    instance_name = sys.argv[1]  # Replace with your instance name
    user_name = sys.argv[2]  # Replace with your desired username
    password = sys.argv[3]  # Replace with your desired password
    database_name = sys.argv[4]  # Replace with your desired database name
    machine_type = sys.argv[5]  # Replace with your desired machine type
    postgress_password = sys.argv[6]  # Replace with your desired postgress password
    

# Replace with your service account credentials file path
# service_account_file = '../../google-cloud-key/hardy-device-327220-d8a75eb3649b.json'

# Define the required scopes
scopes = ['https://www.googleapis.com/auth/sqlservice.admin']

# Create credentials
credentials = GoogleCredentials.get_application_default()

# Create a Cloud SQL service object
sql_service = build('sqladmin', 'v1', credentials=credentials)

output_file = "../log/" + str(epoch_time) + "_instance_log.txt"

sys.stdout = open(output_file, 'w')
sys.stderr = sys.stdout

instance_body = {
    "name": instance_name,
    "databaseVersion": "POSTGRES_15",
    "settings": {
        "ip_configuration": {
            "ipv4_enabled": "true",
            "authorizedNetworks": [
                    {
                        "name": "allow-all",
                        "value": "0.0.0.0/0"
                    }
            ]
        },
        "tier": machine_type,
        "edition": 'ENTERPRISE_PLUS'
    }
}

#resp = sql_service.instances().list(project=project_id).execute()

# Create the Cloud SQL instance
operation = sql_service.instances().insert(project=project_id, body=instance_body).execute()
print(f"Creating instance operation: {operation}")

for i in range (0, 8):
    print("waiting...\n")
    time.sleep(60)

# # Create a user for the database
user_op = sql_service.users().insert(
    project=project_id,
    instance=instance_name,
    body={"name": user_name, "password": password}
).execute()
print(f"Creating user operation: {user_op}")


# Create the database
database_op = sql_service.databases().insert(
    project=project_id,
    instance=instance_name,
    body={"name": database_name}
).execute()
print(f"Creating database operation: {database_op}")

dbinfo = sql_service.instances().get(project = project_id, instance = instance_name).execute()


config = {
    "project_id" : project_id,
    "instance_name" : instance_name,
    "user_name" : user_name,
    "password" : password,
    "database_name" : database_name,
    "machine_type" : machine_type,
    "postgress_password" : postgress_password,
    "ip_address" : dbinfo['ipAddresses'][0]["ipAddress"]
 }

user_pgpass_update = sql_service.users().update(project = project_id, instance = instance_name, host = dbinfo['ipAddresses'][0]["ipAddress"], name = "postgres", body = {"password" : postgress_password}).execute()
print(f"Updating postgres user with password")


file_path = "../runs/" + str(epoch_time) + "_instance_config.json"

with open(file_path, "w") as json_file:
    json.dump(config, json_file, indent=4)
    print(f"JSON data written to {file_path}")

sys.stdout = sys.__stdout__
sys.stderr = sys.__stderr__