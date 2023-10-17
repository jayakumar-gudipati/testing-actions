import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime
import os
from pytz import timezone 
import pkg_resources
import sys
import subprocess as su
import wget

for i in range(1,20):
    print("hello")

print(dir(pd))

print(dir(np))

def print_libs():
    # for i in ['webdriver-manager']:
    #     subprocess.check_call([sys.executable, '-m', 'pip', 'install', i])
    # os.system("export PATH=$PATH:/opt/airflow/dags/chrome_headless/chrome-headless-shell'")
    # os.environ['PATH'] += os.pathsep + r"/opt/airflow/dags/chrome_headless/chrome-headless-shell"
    print(os.environ['PATH'])
    command = ["wget", "https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb"]
    # print(su.run(command, capture_output=True))
    command = ["apt", "install", "./google-chrome-stable_current_amd64.deb"]
    # print(su.run(command, capture_output=True))
    # os.system("python3 -m pip install ")
    # subprocess.check_call([sys.executable, '-m', 'pip', 'install', i])
    # subprocess.Popen("wget https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/118.0.5993.70/linux64/chrome-linux64.zip -o chrome_driver.zip")
    # import wget
    # # url = 'https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/118.0.5993.70/linux64/chrome-linux64.zip'
    # url = 'https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/118.0.5993.70/linux64/chrome-headless-shell-linux64.zip'
    # out_dir = '/opt/airflow/dags/chrome_driver.zip'
    # os.remove("/opt/airflow/dags/chrome_driver.zip")
    # filename = wget.download(url,out=out_dir)
    # print(filename)
    # unzip()
    import selenium
    # subprocess.check_call([sys.executable, '/opt/airflow/dags/pyunzip.py /opt/airflow/dags/chrome_driver.zip'])

    env = dict(tuple(str(ws).split()) for ws in pkg_resources.working_set)
    print(env)

print_libs()

print("adadasdasdasdasdasdasda")