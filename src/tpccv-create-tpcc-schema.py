from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import GoogleCredentials
import sys
import json
import time
import os
import subprocess
import psycopg2


'''

output file: 

input: original config.json
output: time took to load schema

'''



# overall project id
project_id = 'hardy-device-327220'
credentials = GoogleCredentials.get_application_default()
sql_service = build('sqladmin', 'v1', credentials=credentials)


# executes a command in the command line
def execute_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, errors = process.communicate()
    if output:
        print("Output:")
        print(output)

    if errors:
        print("Errors:")
        print(errors)


def get_db_size(tpcc_pass, ip_add):
    size_sql_command = '''SELECT
    sum(total_size) AS total_size
FROM (
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        I.indexrelid AS indexid,
        pg_total_relation_size(C.oid) AS total_size
    FROM
        pg_class C
    LEFT JOIN pg_index I ON (C.oid = I.indrelid)
    LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    LEFT JOIN pg_roles R ON (C.relowner = R.oid)
    WHERE
        R.rolname = 'tpcc'  -- Replace 'specific_username' with the desired username
        AND N.nspname NOT IN ('pg_catalog', 'information_schema') -- Exclude system schemas
) sizes;'''
    try:
        conn = psycopg2.connect(
            dbname="tpcc",
            user="tpcc",
            password=tpcc_pass,
            host=ip_add
        )

        # Create a cursor object using the connection
        cursor = conn.cursor()

        # print(size_sql_command)
        cursor.execute(size_sql_command)

        # Commit changes to the database
        conn.commit()
        return cursor.fetchone()[0]

    except psycopg2.Error as e:
        print("Error occurred while querying the size:", e)

    finally:
        # Close the cursor and connection
        if cursor:
           cursor.close()
        if conn:
            conn.close()



# take in input from the command line, get the epoch time of the database initialization
num_args = len(sys.argv)
if num_args != 4:
    print("Need in order: config.json file, number of warehouses, number of virtual users")
    sys.exit()
else:
    config_json_file_path = sys.argv[1]
    num_warehouses = sys.argv[2]
    num_virtual_users = sys.argv[3]
epoch_time = config_json_file_path.split("_")[0]



# combine output and error to a log file
output_file = "../log/" + str(epoch_time) + "_tpcc_schema_log.txt"
sys.stdout = open(output_file, 'w')
sys.stderr = sys.stdout

# get json data
with open("../runs/" + config_json_file_path, 'r') as file:
        json_data = json.load(file)


# update root password
user_pgpass_update = sql_service.users().update(project = project_id, instance = json_data["instance_name"], host = json_data["ip_address"], name = "postgres", body = {"password" : json_data["postgress_password"]}).execute()


# switch directory and execute hammerdb methods
orig_dir = os.getcwd()
os.chdir("../HammerDB-4.9")
start_time = time.time()
command = "./hammerdbcli <<.\ndbset db pg\ndbset bm TPC-C\ndiset tpcc pg_superuser postgres\ndiset tpcc pg_superuserpass " + json_data["postgress_password"] + "\ndiset connection pg_host " + json_data["ip_address"] + "\ndiset tpcc pg_count_ware " + str(num_warehouses) + "\ndiset tpcc pg_num_vu " + str(num_virtual_users) + "\nbuildschema\nquit\n.\n"
print(command)
execute_command(command)
end_time = time.time()
dbsize = get_db_size("tpcc", json_data["ip_address"])
elapsed_time = end_time - start_time
os.chdir(orig_dir)
print("size: " + str(dbsize) + "\n")
print("time elapsed: " + str(elapsed_time) + " seconds\n")

conn = psycopg2.connect(
            dbname="tpcc",
            user="postgres",
            password=json_data["postgress_password"],
            host=json_data["ip_address"]
        )
cursor = conn.cursor()
cursor.execute("ALTER USER tpcc WITH PASSWORD '" + json_data["postgress_password"] + "';")
conn.commit()
print("changed password")
cursor.execute("CREATE EXTENSION vector SCHEMA public;")
print("created pgvector extension")
conn.commit()
conn.close()

# create the output json
output_json = {
    "config" : json_data,
    "num_warehouses" : num_warehouses,
    "num_virtual_users" : num_virtual_users,
    "loading_time" : str(elapsed_time),
    "size" : str(dbsize)
 }

output_json_path = "../runs/" + str(epoch_time) + "_tpcc_schema_output.json"
with open(output_json_path, "w") as json_file:
    json.dump(output_json, json_file, indent=4)
    print(f"JSON data written to {json_file}")

sys.stdout = sys.__stdout__
sys.stderr = sys.__stderr__
