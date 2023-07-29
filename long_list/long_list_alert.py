from slack_alert import SlackAlert

from slack_long_list_alert import *
from FTC_long_list_ariflow_DAG_0525 import *
import pandas as pd
import numpy as np
import requests
import warnings
import os
from bs4 import BeautifulSoup
from tqdm import tqdm, trange
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum

warnings.simplefilter(action='ignore', category=FutureWarning) # Ignore FutureWarnings
pd.set_option('display.max_columns', None) # Show all columns

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")



# 기본 args 생성
default_args = {
    'owner' : 'jhkim',
    'email' : ['jhkim@winkstone.com'],
    'email_on_failure': False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 1)
}

# DAG 생성 
with DAG(
    dag_id='FTC_long_list_extract',
    default_args=default_args,
    start_date=datetime(2023, 4,27, tzinfo=kst),
    description='Extract long list',
    schedule_interval='30 14 * * 2',
    tags=['FTC','longlist','slack alert', 'create xlsx'],
    catchup = False
) as dag:
    
    t1 = PythonOperator(
        task_id = "extract",
        python_callable = main_create_long_list)
        
    t2 = PythonOperator(
        task_id = "alertslack",
        python_callable = generate_and_send_file_to_slack)
    
    t1 >> t2
