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

output_file = "../log/" + str(epoch_time) + "_ann_qtest_log.txt"

sys.stdout = open(output_file, 'w')
sys.stderr = sys.stdout

with open("../runs/" + config_json_file_path, 'r') as file:
        json_data = json.load(file)

num_warehouses = int(json_data["tpccv-output"]["num_warehouses"])
ip_address = json_data["tpccv-output"]["config"]["ip_address"]


json_output = json_data
json_output["ann-qtest"] = {}
s_list = [1, 10, 100, 1000, 10000, 100000, 1000000]
k_list = [1, 10, 20, 40, 80, 160, 183, 184, 320, 640, 1000]
conn = psycopg2.connect(
        dbname="tpcc",  
        user="tpcc",
        password=json_data["tpccv-output"]["config"]["postgress_password"],
        host=ip_address
    )
# Create a cursor object using the connection
cursor = conn.cursor()
cursor.execute("select iv_v from item_queries where iv_id = 1;")
search_vector = cursor.fetchone()[0]

json_output["ann-qtest"]["ann-q1"] = {}
ef_list = []

for i in range(20, 320, 20):
    ef_list.append(i)



for ef in ef_list:
    json_output["ann-qtest"]["ann-q1"][str(ef)] = {}
    for s in s_list:
        json_output["ann-qtest"]["ann-q1"][str(ef)][str(s)] = {}
        for k in k_list:
            if k > s or s > num_warehouses*100000:
                continue
            print(ef, s, k)
            cursor.execute(f"SET hnsw.ef_search = {ef}")
            cursor.execute(f"SELECT iv_id, iv_v <-> '{search_vector}' from item_vector where MOD(iv_id, DIV({num_warehouses*100000}, {s})) = DIV(DIV({num_warehouses*100000},  {s}), 2) ORDER BY iv_v <-> '{search_vector}' LIMIT {k};")
            conn.commit()
            ret_obj = cursor.fetchall()
            # print(ret_obj)
            json_output["ann-qtest"]["ann-q1"][str(ef)][str(s)][str(k)] = {} 
            json_output["ann-qtest"]["ann-q1"][str(ef)][str(s)][str(k)]["nearest_neighbors"] = ', '.join(map(str, ret_obj))
            cursor.execute(f"Explain Analyze SELECT iv_id, iv_v <-> '{search_vector}' from item_vector where MOD(iv_id, DIV({num_warehouses*100000}, {s})) = DIV(DIV({num_warehouses*100000},  {s}), 2) ORDER BY iv_v <-> '{search_vector}' LIMIT {k};")
            ret_obj = cursor.fetchall()
            print(ret_obj)
            has_idx = False
            for ob in ret_obj:
                if ob[0].find("idx") != -1:
                    has_idx = True
                    break
            conn.commit()
            json_output["ann-qtest"]["ann-q1"][str(ef)][str(s)][str(k)]["Uses_Index"] = str(has_idx)
            # print('\n'.join(map(str, ret_obj)))
            total_time = (float(ret_obj[len(ret_obj)-2][0].split()[2])+float(ret_obj[len(ret_obj)-1][0].split()[2]))
            json_output["ann-qtest"]["ann-q1"][str(ef)][str(s)][str(k)]["total_time"] = str(total_time)
          



c_id = -1
d_id = -1
w_id = -1
for c in range (1, 30000*num_warehouses+1):
    cursor.execute(f"select count(*) from item i, orders o, order_line ol, customer c where i_id = ol.ol_i_id and (ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id) and o.o_c_id = {c}")
    if (cursor.fetchall()[0][0] < 100):
         continue
    found = False
    for w in range (1, num_warehouses+1):
        for d in range (1, 10*num_warehouses+1):
            cursor.execute(f"select count(*) from item i, orders o, order_line ol, customer c where i_id = ol.ol_i_id and (ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id) and (o.o_w_id = {w} and o.o_d_id = {d} and o.o_c_id = {c})")
            conn.commit()
            if (cursor.fetchall()[0][0] >= 100):
                c_id = c
                d_id = d
                w_id = w
                found = True
                break
        if found == True:
            break
    if found == True:
        break



json_output["ann-qtest"]["ann-q2"] = {}
for ef in ef_list:
    json_output["ann-qtest"]["ann-q2"][str(ef)] = {}
    for k in k_list:
        print(ef, k)
        cursor.execute(f"SET hnsw.ef_search = {ef}")
        cursor.execute(f"SELECT iv_id, iv_v <-> '{search_vector}' FROM item_vector iv, order_line ol, orders o WHERE iv_id = ol.ol_i_id and (ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id) and  (o.o_w_id = {w_id} and o.o_d_id = {d_id} and o.o_c_id = {c_id}) ORDER BY iv_v <-> '{search_vector}' LIMIT {k}")
        conn.commit()
        json_output["ann-qtest"]["ann-q2"][str(ef)][str(k)] = {} 
        ret_obj = cursor.fetchall()
        # print(ret_obj)
        json_output["ann-qtest"]["ann-q2"][str(ef)][str(k)]["nearest_neighbors"] = ', '.join(map(str, ret_obj))
        cursor.execute(f"Explain Analyze SELECT iv_id, iv_v <-> '{search_vector}' FROM item_vector iv, order_line ol, orders o WHERE iv_id = ol.ol_i_id and (ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id) and  (o.o_w_id = {w_id} and o.o_d_id = {d_id} and o.o_c_id = {c_id}) ORDER BY iv_v <-> '{search_vector}' LIMIT {k}")
        ret_obj = cursor.fetchall()
        print(ret_obj)
        has_idx = False
        for ob in ret_obj:
            if ob[0].find("idx") != -1:
                has_idx = True
                break
        conn.commit()
        json_output["ann-qtest"]["ann-q2"][str(ef)][str(k)]["Uses_Index"] = str(has_idx)
        # print('\n'.join(map(str, ret_obj)))
        total_time = (float(ret_obj[len(ret_obj)-2][0].split()[2])+float(ret_obj[len(ret_obj)-1][0].split()[2]))
        json_output["ann-qtest"]["ann-q2"][str(ef)][str(k)]["total_time"] = str(total_time)

json_output["ann-qtest"]["ann-q3"] = {}
for ef in ef_list:
    json_output["ann-qtest"]["ann-q3"][str(ef)] = {}
    for k in k_list:
        print(ef, s, k)
        cursor.execute(f"SET hnsw.ef_search = {ef}")
        cursor.execute(f"SELECT iv_id, iv_v <-> (select iv_v from item_queries where iv_id = 1) FROM item_vector iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 10 ORDER BY iv_v <-> (select iv_v from item_queries where iv_id = 1) LIMIT {k}")
        conn.commit()
        json_output["ann-qtest"]["ann-q3"][str(ef)][str(k)] = {} 
        ret_obj = cursor.fetchall()
        # print(ret_obj)
        json_output["ann-qtest"]["ann-q3"][str(ef)][str(k)]["nearest_neighbors"] = ', '.join(map(str, ret_obj))
        cursor.execute(f"Explain Analyze SELECT iv_id, iv_v <-> (select iv_v from item_queries where iv_id = 1) FROM item_vector iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 10 ORDER BY iv_v <-> (select iv_v from item_queries where iv_id = 1) LIMIT {k}")
        ret_obj = cursor.fetchall()
        print(ret_obj)
        conn.commit()
        has_idx = False
        for ob in ret_obj:
            if ob[0].find("idx") != -1:
                has_idx = True
                break
        json_output["ann-qtest"]["ann-q3"][str(ef)][str(k)]["Uses_Index"] = str(has_idx)
        # print('\n'.join(map(str, ret_obj)))
        total_time = (float(ret_obj[len(ret_obj)-2][0].split()[2])+float(ret_obj[len(ret_obj)-1][0].split()[2]))
        json_output["ann-qtest"]["ann-q3"][str(ef)][str(k)]["total_time"] = str(total_time)

cursor.execute(f"SELECT count(*) FROM item_vector iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 10")
conn.commit()
ret_obj = cursor.fetchall()
json_output["ann-qtest"]["ann-q3"]["selectivity"] = ret_obj[0][0]

json_output["ann-qtest"]["ann-q4"] = {}
for ef in ef_list:
    json_output["ann-qtest"]["ann-q4"][str(ef)] = {}
    for k in k_list:
        print(ef, s, k)
        cursor.execute(f"SET hnsw.ef_search = {ef}")
        cursor.execute(f"SELECT iv_id, iv_v <-> (select iv_v from item_queries where iv_id = 1) FROM item_vector iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 90 ORDER BY iv_v <-> (select iv_v from item_queries where iv_id = 1) LIMIT {k}")
        conn.commit()
        ret_obj = cursor.fetchall()
        # print(ret_obj)
        json_output["ann-qtest"]["ann-q4"][str(ef)][str(k)] = {} 
        json_output["ann-qtest"]["ann-q4"][str(ef)][str(k)]["nearest_neighbors"] = ', '.join(map(str, ret_obj))
        cursor.execute(f"Explain Analyze SELECT iv_id, iv_v <-> (select iv_v from item_queries where iv_id = 1) FROM item_vector iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 90 ORDER BY iv_v <-> (select iv_v from item_queries where iv_id = 1) LIMIT {k}")
        ret_obj = cursor.fetchall()
        print(ret_obj)
        conn.commit()
        has_idx = False
        for ob in ret_obj:
            if ob[0].find("idx") != -1:
                has_idx = True
                break
        json_output["ann-qtest"]["ann-q4"][str(ef)][str(k)]["Uses_Index"] = str(has_idx)
        # print('\n'.join(map(str, ret_obj)))
        total_time = (float(ret_obj[len(ret_obj)-2][0].split()[2])+float(ret_obj[len(ret_obj)-1][0].split()[2]))
        json_output["ann-qtest"]["ann-q4"][str(ef)][str(k)]["total_time"] = str(total_time)

cursor.execute(f"SELECT count(*) FROM item_vector iv, stock s WHERE iv.iv_id = s.s_i_id and s.s_quantity > 90")
ret_obj = cursor.fetchall()
conn.commit()
json_output["ann-qtest"]["ann-q4"]["selectivity"] = ret_obj[0][0]

output_json_path = "../runs/" + str(epoch_time) + "_kann_qtest_output.json"
with open(output_json_path, "w") as json_file:
    json.dump(json_output, json_file, indent=4)
    print(f"JSON data written to {json_file}")