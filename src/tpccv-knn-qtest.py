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

output_file = "../log/" + str(epoch_time) + "_knn_qtest_log.txt"

sys.stdout = open(output_file, 'w')
sys.stderr = sys.stdout

with open("../runs/" + config_json_file_path, 'r') as file:
        json_data = json.load(file)

num_warehouses = json_data["num_warehouses"]
ip_address = json_data["config"]["ip_address"]














curr_table = "item_vector"

# Run exact one-table filtered similarity search for each selectivity s = 1, 10, 100, 1000, 10K, 100K, 1M, no filter: “iv_id % (n/s) == s/2”; also vary K = 10, 30, 100, 300, 1000
# if s = 10, then we want s rows selected, which means  there are n/s blocks, which means we want id%(n/s) = (n/s)/
# s > k
json_output = {}
s_list = [1, 10, 100, 1000, 10000, 100000, 1000000]
k_list = [1, 10, 20, 40, 80, 160, 183, 184, 320, 640, 1000]

conn = psycopg2.connect(
        dbname="tpcc",  
        user="tpcc",
        password=json_data["config"]["postgress_password"],
        host=ip_address
    )
# Create a cursor object using the connection
cursor = conn.cursor()
cursor.execute("select iv_v from item_queries where iv_id = 1;")
search_vector = cursor.fetchone()[0]

# # find the nearest neighbors
# cursor.execute(f"SELECT iv_id, iv_v <-> '{search_vector}' from item_vector where MOD(iv_id, {int(json_data['num_warehouses'])*100000}/{s_list[2]}) = {s_list[2]/2} ORDER BY iv_v <-> '{search_vector}' LIMIT {k_list[1]};")
# print(', '.join(map(str, cursor.fetchall())))


# # find the time it took
# cursor.execute(f"Explain Analyze SELECT iv_id, iv_v <-> '{search_vector}' from item_vector where MOD(iv_id, {int(json_data['num_warehouses'])*100000}/{s_list[2]}) = {s_list[2]/2} ORDER BY iv_v <-> '{search_vector}' LIMIT {k_list[1]};")
# ret_obj = cursor.fetchall()
# total_time = (float(ret_obj[7][0].split()[2])+float(ret_obj[8][0].split()[2]))
# print(total_time)

json_output["knn-q1"] = {}
for s in s_list:
    json_output["knn-q1"][str(s)] = {}
    for k in k_list:
        print(1, s, k)
        if k > s or s > int(json_data['num_warehouses'])*100000:
            continue
        print(s, k)
        cursor.execute(f"SELECT iv_id, iv_v <-> '{search_vector}' from {curr_table} where MOD(iv_id, DIV({int(json_data['num_warehouses'])*100000}, {s})) = DIV(DIV({int(json_data['num_warehouses'])*100000},  {s}), 2) ORDER BY iv_v <-> '{search_vector}' LIMIT {k};")
        ret_obj = cursor.fetchall()
        print(ret_obj)
        json_output["knn-q1"][str(s)][str(k)] = {} 
        json_output["knn-q1"][str(s)][str(k)]["nearest_neighbors"] = ', '.join(map(str, ret_obj))
        cursor.execute(f"Explain Analyze SELECT iv_id, iv_v <-> '{search_vector}' from {curr_table} where MOD(iv_id, DIV({int(json_data['num_warehouses'])*100000}, {s})) = DIV(DIV({int(json_data['num_warehouses'])*100000},  {s}), 2) ORDER BY iv_v <-> '{search_vector}' LIMIT {k};")
        ret_obj = cursor.fetchall()
        print(ret_obj)
        # print('\n'.join(map(str, ret_obj)))
        total_time = (float(ret_obj[len(ret_obj)-2][0].split()[2])+float(ret_obj[len(ret_obj)-1][0].split()[2]))
        json_output["knn-q1"][str(s)][str(k)]["total_time"] = str(total_time)
          









c_id = -1
d_id = -1
w_id = -1
for c in range (1, 30000*int(json_data['num_warehouses'])+1):
    cursor.execute(f"select count(*) from item i, orders o, order_line ol, customer c where i_id = ol.ol_i_id and (ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id) and o.o_c_id = {c}")
    if (cursor.fetchall()[0][0] < 100):
         continue
    found = False
    for w in range (1, int(json_data['num_warehouses'])+1):
        for d in range (1, 10*int(json_data['num_warehouses'])+1):
            cursor.execute(f"select count(*) from item i, orders o, order_line ol, customer c where i_id = ol.ol_i_id and (ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id) and (o.o_w_id = {w} and o.o_d_id = {d} and o.o_c_id = {c})")
            ret_obj = cursor.fetchall()
            # print(ret_obj)
            if (ret_obj[0][0] >= 100):
                c_id = c
                d_id = d
                w_id = w
                found = True
                break
        if found == True:
            break
    if found == True:
        break








json_output["knn-q2"] = {}
for k in k_list:
    print(2, k)
    json_output["knn-q2"][str(k)] = {}
    # print(s, k)
    cursor.execute(f"SELECT iv_id, iv_v <-> '{search_vector}' FROM {curr_table} iv, order_line ol, orders o WHERE iv_id = ol.ol_i_id and (ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id) and  (o.o_w_id = {w_id} and o.o_d_id = {d_id} and o.o_c_id = {c_id}) ORDER BY iv_v <-> '{search_vector}' LIMIT {k}")
    ret_obj = cursor.fetchall()
    print(ret_obj)
    json_output["knn-q2"][str(k)]["nearest_neighbors"] = ', '.join(map(str, ret_obj))
    cursor.execute(f"Explain Analyze SELECT iv_id, iv_v <-> '{search_vector}' FROM {curr_table} iv, order_line ol, orders o WHERE iv_id = ol.ol_i_id and (ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id) and  (o.o_w_id = {w_id} and o.o_d_id = {d_id} and o.o_c_id = {c_id}) ORDER BY iv_v <-> '{search_vector}' LIMIT {k}")
    ret_obj = cursor.fetchall()
    # print('\n'.join(map(str, ret_obj)))
    total_time = (float(ret_obj[len(ret_obj)-2][0].split()[2])+float(ret_obj[len(ret_obj)-1][0].split()[2]))
    json_output["knn-q2"][str(k)]["total_time"] = str(total_time)






json_output["knn-q3"] = {}
for k in k_list:
    print(3, k)
    json_output["knn-q3"][str(k)] = {}
    print(s, k)
    cursor.execute(f"SELECT iv_id, iv_v <-> (select iv_v from item_queries where iv_id = 1) FROM {curr_table} iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 10 ORDER BY iv_v <-> (select iv_v from item_queries where iv_id = 1) LIMIT {k}")
    ret_obj = cursor.fetchall()
    print(ret_obj)
    json_output["knn-q3"][str(k)]["nearest_neighbors"] = ', '.join(map(str, ret_obj))
    cursor.execute(f"Explain Analyze SELECT iv_id, iv_v <-> (select iv_v from item_queries where iv_id = 1) FROM {curr_table} iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 10 ORDER BY iv_v <-> (select iv_v from item_queries where iv_id = 1) LIMIT {k}")
    ret_obj = cursor.fetchall()
    print(ret_obj)
    # print('\n'.join(map(str, ret_obj)))
    total_time = (float(ret_obj[len(ret_obj)-2][0].split()[2])+float(ret_obj[len(ret_obj)-1][0].split()[2]))
    json_output["knn-q3"][str(k)]["total_time"] = str(total_time)


cursor.execute(f"SELECT count(*) FROM {curr_table} iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 10")
ret_obj = cursor.fetchall()
json_output["knn-q3"]["selectivity"] = ret_obj[0][0]


json_output["knn-q4"] = {}
for k in k_list:
    print(4, k)
    json_output["knn-q4"][str(k)] = {}
    print(s, k)
    cursor.execute(f"SELECT iv_id, iv_v <-> (select iv_v from item_queries where iv_id = 1) FROM {curr_table} iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 90 ORDER BY iv_v <-> (select iv_v from item_queries where iv_id = 1) LIMIT {k}")
    ret_obj = cursor.fetchall()
    print(ret_obj)
    json_output["knn-q4"][str(k)]["nearest_neighbors"] = ', '.join(map(str, ret_obj))
    cursor.execute(f"Explain Analyze SELECT iv_id, iv_v <-> (select iv_v from item_queries where iv_id = 1) FROM {curr_table} iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 90 ORDER BY iv_v <-> (select iv_v from item_queries where iv_id = 1) LIMIT {k}")
    ret_obj = cursor.fetchall()
    print(ret_obj)
    print('\n'.join(map(str, ret_obj)))
    total_time = (float(ret_obj[len(ret_obj)-2][0].split()[2])+float(ret_obj[len(ret_obj)-1][0].split()[2]))
    json_output["knn-q4"][str(k)]["total_time"] = str(total_time)


cursor.execute(f"SELECT count(*) FROM {curr_table} iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 90")
ret_obj = cursor.fetchall()
json_output["knn-q4"]["selectivity"] = ret_obj[0][0]

if cursor:
    cursor.close()
if conn:
    conn.close()



json_output["tpccv-output"] = json_data
output_json_path = "../runs/" + str(epoch_time) + "_knn_qtest_output.json"
with open(output_json_path, "w") as json_file:
    json.dump(json_output, json_file, indent=4)
    print(f"JSON data written to {json_file}")