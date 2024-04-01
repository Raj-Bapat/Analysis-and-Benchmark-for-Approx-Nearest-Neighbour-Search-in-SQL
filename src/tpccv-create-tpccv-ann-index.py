from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import GoogleCredentials
from collections import defaultdict
import psycopg2
import sys
import json
import time
import os
import subprocess
import numpy as np




# Read input, jsons, create variables
num_args = len(sys.argv)
if num_args != 2:
    print("Need in order: output.json file")
    sys.exit()
else:
    config_json_file_path = sys.argv[1]
epoch_time = config_json_file_path.split("_")[0]

with open("../runs/" + config_json_file_path, 'r') as file:
        json_data = json.load(file)

num_warehouses = json_data["tpccv-output"]["num_warehouses"]
ip_address = json_data["tpccv-output"]["config"]["ip_address"]


json_output = json_data
json_output["create-ann-index"] = {}
conn = psycopg2.connect(
        dbname="tpcc",  
        user="tpcc",
        password=json_data["tpccv-output"]["config"]["postgress_password"],
        host=ip_address
    )
# Create a cursor object using the connection
cursor = conn.cursor()


cursor.execute("set maintenance_work_mem to '1GB'")
timeBefore = time.time()
cursor.execute("CREATE INDEX ON item_vector USING hnsw (iv_v vector_l2_ops)")
conn.commit()
timeAfter = time.time()
time_elapsed = timeAfter-timeBefore
json_output["create-ann-index"]["create-HNSW-index"] = {}
json_output["create-ann-index"]["create-HNSW-index"]["time_elapsed"] = str(time_elapsed)


cursor.execute("CREATE TABLE item_vector2 (iv_id integer primary key, iv_v vector(960))")
conn.commit()
cursor.execute("set maintenance_work_mem to '1GB'")
cursor.execute("CREATE INDEX ON item_vector2 USING hnsw (iv_v vector_l2_ops)")
conn.commit()
timeBefore = time.time()
cursor.execute("INSERT into item_vector2 (SELECT * from item_vector)")
conn.commit()
timeAfter = time.time()
time_elapsed = timeAfter-timeBefore
json_output["create-ann-index"]["insert-with-HNSW"] = {}
json_output["create-ann-index"]["insert-with-HNSW"]["time_elapsed"] = str(time_elapsed)


timeBefore = time.time()
cursor.execute("Delete FROM item_vector2")
conn.commit()
timeAfter = time.time()
time_elapsed = timeAfter-timeBefore
json_output["create-ann-index"]["delete-with-HNSW"] = {}
json_output["create-ann-index"]["delete-with-HNSW"]["time_elapsed"] = str(time_elapsed)


cursor.execute("CREATE TABLE item_vector_noidx (iv_id integer primary key, iv_v vector(960))")
conn.commit()
timeBefore = time.time()
cursor.execute("INSERT into item_vector_noidx (SELECT * from item_vector)")
conn.commit()
timeAfter = time.time()
time_elapsed = timeAfter-timeBefore
json_output["create-ann-index"]["insert-without-HNSW"] = {}
json_output["create-ann-index"]["insert-without-HNSW"]["time_elapsed"] = str(time_elapsed)

# timeBefore = time.time()
# cursor.execute("Delete FROM item_vector_noidx")
# conn.commit()
# timeAfter = time.time()
# time_elapsed = timeAfter-timeBefore
# json_output["create-ann-index"]["delete-without-HNSW"] = {}
# json_output["create-ann-index"]["delete-without-HNSW"]["time_elapsed"] = str(time_elapsed)

cursor.execute("CREATE TABLE item_vector_novector (iv_id integer primary key, iv_text text)")
conn.commit()
timeBefore = time.time()
cursor.execute("insert into item_vector_novector select i_id, 'foo' from item")
conn.commit()
timeAfter = time.time()
time_elapsed = timeAfter-timeBefore
json_output["create-ann-index"]["insert-without-vector"] = {}
json_output["create-ann-index"]["insert-without-vector"]["time_elapsed"] = str(time_elapsed)

if cursor:
    cursor.close()
if conn:
    conn.close()



output_json_path = "../runs/" + str(epoch_time) + "_ann_index_output.json"
with open(output_json_path, "w") as json_file:
    json.dump(json_output, json_file, indent=4)
    print(f"JSON data written to {json_file}")