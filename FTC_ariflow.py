from utils.slack_alert import SlackAlert
from utils.npc_html_etl2 import *
from utils.main_index_etl2 import *
from utils.ncp_html_downloader2 import *
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


slack = SlackAlert('# ds-ai-팀', "###############################")

def print_result(**kwargs):
    r = kwargs["task_instance"].xcom_pull(key = 'result_msg')
    print("message : ", r)



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
    dag_id='Final_FTC',
    default_args=default_args,
    start_date=datetime(2023, 4,27, tzinfo=kst),
    description='Crawling, downloading html and ETL',
    schedule_interval='30 11 * * *',
    tags=['FTC','crawling','ETL','HTML'],
    on_success_callback = slack.success_msg,
    on_failure_callback = slack.fail_msg,
    catchup = False
) as dag:
    
    t1 = PythonOperator(
        task_id = "crawling",
        python_callable = main_etl)
    
    t2 = PythonOperator(
        task_id = "Html-download",
        python_callable = main_html_download)
        
    t3 = PythonOperator(
    	task_id = "ETL",
    	python_callable = main_html_etl)
    
    t1 >> t2 >> t3
