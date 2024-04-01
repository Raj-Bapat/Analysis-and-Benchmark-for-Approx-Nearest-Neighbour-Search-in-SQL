import matplotlib.pyplot as plt
import numpy as np
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


def open_json(filename):
    with open("../runs/" + filename, 'r') as file:
        json_data = json.load(file)
    return json_data

num_args = len(sys.argv)
epoch_times = []
if num_args == 0:
    print("need a list of output files")
    sys.exit()
else:
    for i in range(1, len(sys.argv)):
        epoch_times.append(sys.argv[i])

create_tpcc_schema_x = []
create_tpcc_schema_y = []
create_tpcc_schema_lv_x = []
create_tpcc_schema_lv_y = []
create_tpcc_schema_lv_ci_x = []
create_tpcc_schema_lv_ci_y = []
insert_x_novec = [] # insert_x_novec, insert_y_novec
insert_y_novec = []
insert_x_vec = [] # insert_x_vec, insert_y_vec
insert_y_vec = []
insert_x_index = [] # insert_x_index, insert_y_index
insert_y_index = []
delete_x_noidx = [] # delete_x_noidx, delete_y_noidx
delete_y_noidx = []
delete_x_idx = [] # delete_x_idx, delete_y_idx
delete_y_idx = []


for et in epoch_times: 
    instance_config = open_json(et+"_instance_config.json")
    tpcc_schema_output = open_json(et+"_tpcc_schema_output.json")
    tpccv_schema_output = open_json(et+"_tpccv_schema_output.json")
    knn_qtest_output = open_json(et+"_knn_qtest_output.json")
    ann_index_output = open_json(et+"_ann_index_output.json")
    kann_qtest_output = open_json(et+"_kann_qtest_output.json")
    
    # create plot
    create_tpcc_schema_x.append(int(tpcc_schema_output["num_warehouses"]))
    create_tpcc_schema_lv_x.append(int(tpcc_schema_output["num_warehouses"]))
    create_tpcc_schema_lv_ci_x.append(int(tpcc_schema_output["num_warehouses"]))
    create_tpcc_schema_y.append(float(tpcc_schema_output["loading_time"]))
    create_tpcc_schema_lv_y.append(float(tpcc_schema_output["loading_time"])+float(tpccv_schema_output["loading_time"]))
    create_tpcc_schema_lv_ci_y.append(float(tpcc_schema_output["loading_time"])+float(tpccv_schema_output["loading_time"])+float(ann_index_output["create-ann-index"]["create-HNSW-index"]["time_elapsed"]))
    
    # insert plot
    insert_x_novec.append(int(tpcc_schema_output["num_warehouses"]))
    insert_x_vec.append(int(tpcc_schema_output["num_warehouses"]))
    insert_x_index.append(int(tpcc_schema_output["num_warehouses"]))
    insert_y_novec.append(float(ann_index_output["create-ann-index"]["insert-without-vector"]["time_elapsed"]))
    insert_y_vec.append(float(ann_index_output["create-ann-index"]["insert-without-HNSW"]["time_elapsed"]))
    insert_y_index.append(float(ann_index_output["create-ann-index"]["insert-with-HNSW"]["time_elapsed"]))
    
    # delete plot
    delete_x_noidx.append(int(tpcc_schema_output["num_warehouses"]))
    delete_y_noidx.append(float(ann_index_output["create-ann-index"]["delete-with-HNSW"]["time_elapsed"]))
    delete_x_idx.append(int(tpcc_schema_output["num_warehouses"]))
    delete_y_idx.append(float(ann_index_output["create-ann-index"]["delete-without-HNSW"]["time_elapsed"]))


# create plot
fig=plt.figure()
fig.show()
ax=fig.add_subplot(111)
ax.plot(create_tpcc_schema_x,create_tpcc_schema_y,c='b',marker="^",ls='--',label='schema',fillstyle='none')
ax.plot(create_tpcc_schema_lv_x,create_tpcc_schema_lv_y,c='g',marker=(8,2,0),ls='--',label='vectors')
ax.plot(create_tpcc_schema_lv_ci_x,create_tpcc_schema_lv_ci_y, c='r',marker="v",ls='-',label='index')
# ax.plot(create_tpcc_schema_lv_ci_x,create_tpcc_schema_lv_ci_y,c='k',ls='-',label='create schema, load vectors, create index')
# ax.plot(x,x**2-1,c='m',marker="o",ls='--',label='BSwap',fillstyle='none')
# ax.plot(x,x-1,c='k',marker="+",ls=':',label='MSD')
plt.legend(loc='best')
plt.savefig("create_plot.png")



# insert plot
fig=plt.figure()
fig.show()
ax=fig.add_subplot(111)
ax.plot(insert_x_novec, insert_y_novec,c='b',marker="^",ls='--',label='schema',fillstyle='none')
ax.plot(insert_x_vec, insert_y_vec,c='g',marker=(8,2,0),ls='--',label='vectors')
ax.plot(insert_x_index, insert_y_index, c='r',marker="v",ls='-',label='index')
# ax.plot(create_tpcc_schema_lv_ci_x,create_tpcc_schema_lv_ci_y,c='k',ls='-',label='create schema, load vectors, create index')
# ax.plot(x,x**2-1,c='m',marker="o",ls='--',label='BSwap',fillstyle='none')
# ax.plot(x,x-1,c='k',marker="+",ls=':',label='MSD')
plt.legend(loc='best')
plt.savefig("insert_plot.png")


# delete plot
fig=plt.figure()
fig.show()
ax=fig.add_subplot(111)
ax.plot(delete_x_noidx, delete_y_noidx,c='b',marker="^",ls='--',label='schema',fillstyle='none')
ax.plot(delete_x_idx, delete_y_idx,c='g',marker=(8,2,0),ls='--',label='vectors')
# ax.plot(create_tpcc_schema_lv_ci_x,create_tpcc_schema_lv_ci_y,c='k',ls='-',label='create schema, load vectors, create index')
# ax.plot(x,x**2-1,c='m',marker="o",ls='--',label='BSwap',fillstyle='none')
# ax.plot(x,x-1,c='k',marker="+",ls=':',label='MSD')
plt.legend(loc='best')
plt.savefig("delete_plot.png")