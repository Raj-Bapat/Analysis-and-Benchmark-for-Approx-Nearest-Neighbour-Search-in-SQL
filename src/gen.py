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

'''

We will not take any arguments. We will generate preset number of instances with a preset number of warehouses
We print every single file call to sql
We print a map from warehouses size to epoch time

'''

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

num_args = len(sys.argv)
if num_args != 2:
    print("Need in order: prefix for instance name")
    sys.exit()
else:
    pref = sys.argv[1]

warehouse_amounts = [1, 3, 10, 30, 100, 300, 1000]
epoch_times = []
for whs in warehouse_amounts:
    print("warehouse amount " + whs)
    instance_name = pref + "_" + whs
    execute_command("python3 create-instance.py "+ instance_name +" hybrid-SQL-BM pass hybrid-SQL-BM db-perf-optimized-N-32 pass")
    