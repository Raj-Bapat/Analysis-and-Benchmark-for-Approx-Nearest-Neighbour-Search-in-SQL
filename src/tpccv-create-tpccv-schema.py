from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import GoogleCredentials
import psycopg2
import sys
import json
import time
import os
import subprocess
import numpy as np


# execute terminal command
def execute_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, errors = process.communicate()
    if output:
        print("Output:")
        print(output)

    if errors:
        print("Errors:")
        print(errors)
    return output




# take in input from the command line, get the epoch time of the database initialization
num_args = len(sys.argv)
if num_args != 2:
    print("Need in order: output.json file")
    sys.exit()
else:
    config_json_file_path = sys.argv[1]
epoch_time = config_json_file_path.split("_")[0]

output_file = "../log/" + str(epoch_time) + "_create-tpccv-schema_log.txt"

sys.stdout = open(output_file, 'w')
sys.stderr = sys.stdout

with open("../runs/" + config_json_file_path, 'r') as file:
        json_data = json.load(file)

num_warehouses = json_data["num_warehouses"]
ip_address = json_data["config"]["ip_address"]


orig_dir = os.getcwd()
os.chdir("/tmp")
cmd_output = execute_command('[ -d raj-tmp ] && echo "Yes" || echo "No"')
print(cmd_output)

if cmd_output == "No\n":
    print(execute_command('mkdir raj-tmp'))
    os.chdir("/tmp/raj-tmp")
    print(execute_command('wget ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz'))
    print(execute_command('tar -xvzf gist.tar.gz'))
os.chdir(orig_dir)
    



'''
- item_vector table has 1-1 correspondance with the item table
- item table has 100000*warehouse number of rows
- we will take the nearly 1m vectors from gist dataset and either subset them or repeat them to generate the desired number of rows in the item_vector table
- we will repeatedly iterate through the gist dataset if more vectors are needed for the item vector table
- check if sql table is 1 or 0 based (i_id)
'''





# get database size
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










# reads fvecs files
def fvecs_read(filename, c_contiguous=True):
    fv = np.fromfile(filename, dtype=np.float32)
    if fv.size == 0:
        return np.zeros((0, 0))
    print(fv.size)
    dim = fv.view(np.int32)[0]
    assert dim > 0
    fv = fv.reshape(-1, 1 + dim)
    if not all(fv.view(np.int32)[:, 0] == dim):
        raise IOError("Non-uniform vector sizes in " + filename)
    fv = fv[:, 1:]
    if c_contiguous:
        fv = fv.copy()
    return fv








# use the create_table_query query on the database
create_table_query = '''
    CREATE TABLE IF NOT EXISTS item_vector (
        iv_id INTEGER PRIMARY KEY,
        iv_v vector(960)
    )
'''

conn = psycopg2.connect(
        dbname="tpcc",
        user="tpcc",
        password=json_data["config"]["postgress_password"],
        host=ip_address
    )

    # Create a cursor object using the connection
cursor = conn.cursor()
cursor.execute(create_table_query)
conn.commit()

if cursor:
    cursor.close()
if conn:
    conn.close()










fvtry = fvecs_read("/tmp/raj-tmp/gist/gist_base.fvecs")
# print(fvtry)
size_before = get_db_size(json_data["config"]["postgress_password"], ip_address)
try:
    conn = psycopg2.connect(
        dbname="tpcc",
        user="tpcc",
        password=json_data["config"]["postgress_password"],
        host=ip_address
    )

    # Create a cursor object using the connection
    amnt_of_rows = 100000*int(num_warehouses)
    cursor = conn.cursor()
    batch_size = 10000
    curr = 0
    start_time = time.time()
    coeff = 1
    for k in range(0, amnt_of_rows, batch_size):
        values = ""
        for p in range(0, batch_size):
            if curr == len(fvtry):
                curr = 0
                coeff = coeff*1.001
            embedding = fvtry[curr]
            for i in range(0, len(embedding)):
                embedding[i] = embedding[i]*coeff
            embedding_str = ','.join(map(str, embedding.tolist()))
            values+=f"({str(k+p+1)}, '[{embedding_str}]')"
            curr = curr + 1
            if p != batch_size-1:
                values+=", "
        query = f"INSERT INTO item_vector (iv_id, iv_v) VALUES {values}"
        cursor.execute(query)
        conn.commit()
    end_time = time.time()
    elapsed_time = end_time-start_time

    # Commit changes to the database

    # print("Embedding added successfully!")

except psycopg2.Error as e:
    print("Error occurred while adding the embedding:", e)

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
size_after = get_db_size(json_data["config"]["postgress_password"], ip_address)
size_difference = size_after-size_before







# input config, time_elapsed, size_before, size_after, size_difference
# create output json
output_json = {
    "config" : json_data["config"],
    "num_warehouses" : num_warehouses,
    "num_virtual_users" : json_data["num_virtual_users"],
    "loading_time" : str(elapsed_time),
    "size_before" : str(size_before),
    "size_after" : str(size_after),
    "size_difference" : str(size_difference)
    
 }

output_json_path = "../runs/" + str(epoch_time) + "_tpccv_schema_output.json"
with open(output_json_path, "w") as json_file:
    json.dump(output_json, json_file, indent=4)
    print(f"JSON data written to {json_file}")







create_table_query = '''
    CREATE TABLE IF NOT EXISTS item_queries (
        iv_id INTEGER PRIMARY KEY,
        iv_v vector(960)
    )
'''

# put queries in the database
fvtry = fvecs_read("/tmp/raj-tmp/gist/gist_query.fvecs") 
try:
    conn = psycopg2.connect(
        dbname="tpcc",
        user="tpcc",
        password=json_data["config"]["postgress_password"],
        host=ip_address
    )
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    for i in range (0, len(fvtry), batch_size):
        # embedding = fvtry[0]
        # embedding_str = ','.join(map(str, embedding.tolist()))
        # query = f"INSERT INTO item_vector (iv_id, iv_v) VALUES (0, '[{embedding_str}]')"
        values = ""
        for j in range (i, i+batch_size):
            embedding = fvtry[j]
            embedding_str = ','.join(map(str, embedding.tolist()))
            values+=f"({str(j)}, '[{embedding_str}]')"
            if j == len(fvtry)-1:
                break
            if j != i+batch_size-1:
                values+=", "
        # embedding = fvtry[i: i+batch_size]
        # embedding_str = ','.join(map(str, embedding.tolist()))
        # print(embedding_str)
        query = f"INSERT INTO item_queries (iv_id, iv_v) VALUES {values}"
        # print(query)
        cursor.execute(query)
        conn.commit()


    # Commit changes to the database

except psycopg2.Error as e:
    print("Error occurred while adding the embedding:", e)

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()