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
import pandas as pd
import plotly.express as px
import re
from plotly.validators.scatter.marker import SymbolValidator

def open_json(filename):
    with open("../runs/" + filename, 'r') as file:
        json_data = json.load(file)
    return json_data

et = "1711934271"
instance_config = open_json(et+"_instance_config.json")
tpcc_schema_output = open_json(et+"_tpcc_schema_output.json")
tpccv_schema_output = open_json(et+"_tpccv_schema_output.json")
knn_qtest_output = open_json(et+"_knn_qtest_output.json")
ann_index_output = open_json(et+"_ann_index_output.json")
kann_qtest_output = open_json(et+"_kann_qtest_output.json")

s_list = [1, 10, 100, 1000, 10000, 100000, 1000000]
k_list = [1, 10, 20, 40, 80, 183, 184, 320, 640]
symbols = ['circle', 'square', 'diamond', 'cross', 'x', 'triangle-up', 'triangle-down', 'star', 'hexagram']
ef_list = []

for i in range(20, 320, 20):
    ef_list.append(i)

def joinRecall():
    d1l1 = []
    d1l2 = []
    d1l3 = []

    for k in k_list:
        d1l1.append(float(k))
        d1l2.append(1.0)         


    d1table = {'Recall': d1l2, 'k': d1l1}
    d1tablef = pd.DataFrame(data=d1table)  
    # d1tablef = d1tablef.sort_values(by="Time")

    # print(d1tablef)


    fig = px.line(d1tablef, x='k', y = "Recall", markers = True)
    fig.update_layout(font=dict(size=24))
    fig.update_layout(xaxis_type='log')
    return fig

def joinTime():
    d1l1 = []
    d1l2 = []
    d1l3 = []

    for k in k_list:
        for ef in ef_list:
            d1l1.append(float(k))
            d1l2.append(float(kann_qtest_output["ann-qtest"]["ann-q2"][str(ef)][f"{k}"]["total_time"]))
            d1l3.append(f"ef_search: {ef}")
                


    d1table = {'Time (ms)': d1l2, 'k': d1l1, 'ef_search': d1l3}
    d1tablef = pd.DataFrame(data=d1table)  
    # d1tablef = d1tablef.sort_values(by="Time")

    # print(d1tablef)


    fig = px.line(d1tablef, x='k', y = "Time (ms)", color='ef_search', markers = True)
    # fig.update_layout(font=dict(size=24))
    # fig.update_layout(yaxis_type='log')
    # fig.update_layout(xaxis_type='log')
    return fig
  

def TimeVSelectivityFixedEF(ef_sch):
    d1l1 = []
    d1l2 = []
    d1l3 = []

    for k in k_list:
        for s in s_list:
            if (f"{k}" in kann_qtest_output["ann-qtest"]["ann-q2"][str(ef_sch)][f"{s}"]):
                d1l1.append(float(s))
                d1l2.append(float(kann_qtest_output["ann-qtest"]["ann-q2"][str(ef_sch)][f"{s}"][f"{k}"]["total_time"]))
                d1l3.append(f"k: {k}")
                


    d1table = {'Time (seconds)': d1l2, 'Selectivity': d1l1, 'k': d1l3}
    d1tablef = pd.DataFrame(data=d1table)  
    # d1tablef = d1tablef.sort_values(by="Time")

    # print(d1tablef)


    fig = px.line(d1tablef, x='Selectivity', y = "Time (ms)", color='k', markers = True)
    fig.update_layout(font=dict(size=24))
    fig.update_layout(yaxis_type='log')
    fig.update_layout(xaxis_type='log')
    return fig
  
# chart 1: x is selectivity, y is time, lines = lines for each different k value, ef search = 40, ef search = 80
    # chart 2: instead of y axis time, y axis is recall, ef search = 40, ef search = 80
    # chart 3: chart 1 except instead of lines representing individual k values, they represent different ef-search values, fix k = 100 and k = 300
    # chart 4: exact same as chart 2, but lines represent different ef values,  k = 100, 300
    # for each, try   

def RecallVSelectivityFixedEF(ef_sch):
    d2l1 = []
    d2l2 = []
    d2l3 = []
    for k in k_list:
        for s in s_list:
            if (f"{k}" in knn_qtest_output["knn-q2"][f"{s}"]):
                string_with_coordinates1 = knn_qtest_output["knn-q2"][f"{s}"][f"{k}"]["nearest_neighbors"]
                # Extract x coordinates using regular expressions
                x_coordinates1 = re.findall(r"\(([0-9]+),", string_with_coordinates1)
                x_coordinates_list1 = [int(x) for x in x_coordinates1]
                # print(s, k)
                # print(x_coordinates_list1)
                string_with_coordinates2 = kann_qtest_output["ann-qtest"]["ann-q2"][str(ef_sch)][f"{s}"][f"{k}"]["nearest_neighbors"]
                # print(string_with_coordinates1)
                # print(string_with_coordinates2)
                x_coordinates2 = re.findall(r"\(([0-9]+),", string_with_coordinates2)
                x_coordinates_list2 = [int(x) for x in x_coordinates2]
                # print(x_coordinates_list2)
                cnt = 0.0
                for xc in x_coordinates_list2:
                    if xc in x_coordinates_list1:
                        cnt+=1.0
                if len(x_coordinates_list1) == 0:
                    continue
                else:
                    d2l1.append(cnt/float(len(x_coordinates_list1)))
                    # print (d2l1[len(d2l1)-1])
                d2l2.append(s)
                d2l3.append(f"k: {k}")
                
                


    d1table = {'Recall': d2l1, 'Selectivity': d2l2, 'k': d2l3}
    d1tablef = pd.DataFrame(data=d1table)  
    # d1tablef = d1tablef.sort_values(by="Time")

    # print(d1tablef)


    fig = px.line(d1tablef, x = 'Selectivity', y = "Recall", color='k', markers=True)
    fig.update_layout(font=dict(size=24))
    # fig.update_layout(yaxis_type='log')
    fig.update_layout(xaxis_type='log')
    return fig


def TimeVefFixedSelectivity(selectivity):
    d1l1 = []
    d1l2 = []
    d1l3 = []

    for k in k_list:
        for ef in ef_list:
            if (f"{k}" in kann_qtest_output["ann-qtest"]["ann-q2"][f"{ef}"][f"{selectivity}"]):
                d1l1.append(float(ef))
                d1l2.append(float(kann_qtest_output["ann-qtest"]["ann-q2"][f"{ef}"][f"{selectivity}"][f"{k}"]["total_time"]))
                d1l3.append(f"k: {k}")
                


    d1table = {'Time (ms)': d1l2, 'ef_search': d1l1, 'k': d1l3}
    d1tablef = pd.DataFrame(data=d1table)  
    # d1tablef = d1tablef.sort_values(by="Time")

    # print(d1tablef)


    fig = px.line(d1tablef, x='ef_search', y = "Time (ms)", color='k', markers=True)
    fig.update_layout(font=dict(size=24))
    fig.update_layout(yaxis_type='log')
    return fig
  
def RecallVefFixedSelectivity(selectivity):
    d2l1 = []
    d2l2 = []
    d2l3 = []
    for k in k_list:
        for ef in ef_list:
            if (f"{k}" in knn_qtest_output["knn-q2"][f"{selectivity}"]):
                string_with_coordinates1 = knn_qtest_output["knn-q2"][f"{selectivity}"][f"{k}"]["nearest_neighbors"]
                # Extract x coordinates using regular expressions
                x_coordinates1 = re.findall(r"\(([0-9]+),", string_with_coordinates1)
                x_coordinates_list1 = [int(x) for x in x_coordinates1]
                # print(s, k)
                # print(x_coordinates_list1)
                string_with_coordinates2 = kann_qtest_output["ann-qtest"]["ann-q2"][str(ef)][f"{selectivity}"][f"{k}"]["nearest_neighbors"]
                # print(string_with_coordinates1)
                # print(string_with_coordinates2)
                x_coordinates2 = re.findall(r"\(([0-9]+),", string_with_coordinates2)
                x_coordinates_list2 = [int(x) for x in x_coordinates2]
                # print(x_coordinates_list2)
                cnt = 0.0
                for xc in x_coordinates_list2:
                    if xc in x_coordinates_list1:
                        cnt+=1.0
                if len(x_coordinates_list1) == 0:
                    continue
                else:
                    d2l1.append(cnt/float(len(x_coordinates_list1)))
                    # print (d2l1[len(d2l1)-1])
                d2l2.append(ef)
                d2l3.append(f"k: {k}")
                
                


    d1table = {'Recall': d2l1, 'ef_search': d2l2, 'k': d2l3}
    d1tablef = pd.DataFrame(data=d1table)  
    # d1tablef = d1tablef.sort_values(by="Time")

    # print(d1tablef)


    fig = px.line(d1tablef, x = 'ef_search', y = "Recall", color='k', markers=True)
    fig.update_layout(font=dict(size=24))
    # fig.update_layout(yaxis_type='log')
    # fig.update_layout(xaxis_type='log')
    return fig



# if not os.path.exists("/tmp/rbapat"):
#     os.mkdir("/tmp/rbapat")
    
# if not os.path.exists("/tmp/rbapat/images"):
#     os.mkdir("/tmp/rbapat/images")

# for ef in ef_list:
#     fig = TimeVSelectivityFixedEF(ef)
#     fig.write_image("/tmp/rbapat/images/TimeVSelectivityFixedEF=" + f"{ef}" + ".png")
#     fig = RecallVSelectivityFixedEF(ef)
#     fig.write_image("/tmp/rbapat/images/RecallVSelectivityFixedEF=" + f"{ef}" + ".png")
    
# for s in s_list:
#     fig = TimeVefFixedSelectivity(s)
#     fig.write_image("/tmp/rbapat/images/TimeVefFixedSelectivity=" + f"{s}" + ".png")
#     fig = RecallVefFixedSelectivity(s)
#     fig.write_image("/tmp/rbapat/images/RecallVefFixedSelectivity=" + f"{s}" + ".png")
    
    
    
if not os.path.exists("images"):
    os.mkdir("images")

fig = joinRecall()
filename = "images/joinRecall.png"
# fig.update_layout(yaxis = dict(tickfont = dict(size=15)))
fig.write_image(filename)

fig = joinTime()
fig.update_layout(yaxis_range=[-2,2])
filename = "images/joinTime.png"
# fig.update_layout(yaxis = dict(tickfont = dict(size=15)))
fig.write_image(filename)
    
# for ef in ef_list:
#     fig = TimeVSelectivityFixedEF(ef)
#     filename = "images/TimeVSelectivityFixedEF=" + f"{ef}" + ".png"
#     fig.update_layout(yaxis = dict(tickfont = dict(size=15)))
#     fig.write_image(filename)
#     print(filename)
#     fig = RecallVSelectivityFixedEF(ef)
#     filename = "images/RecallVSelectivityFixedEF=" + f"{ef}" + ".png"
#     fig.write_image(filename)
#     print(filename)
    
# for s in s_list:
#     fig = TimeVefFixedSelectivity(s)
#     filename = "images/TimeVefFixedSelectivity=" + f"{s}" + ".png"
#     fig.update_layout(yaxis = dict(tickfont = dict(size=15)))
#     fig.write_image(filename)
#     print(filename)
#     fig = RecallVefFixedSelectivity(s)
#     filename = "images/RecallVefFixedSelectivity=" + f"{s}" + ".png"
#     fig.write_image(filename)
#     print(filename)