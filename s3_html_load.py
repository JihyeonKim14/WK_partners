
## Library and Settings
import pandas as pd
import numpy as np
import requests
import warnings
import time
import os
import boto3
import shutil

from bs4 import BeautifulSoup
from tqdm import tqdm, trange
from datetime import datetime, timedelta
from sqlalchemy import create_engine

warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('display.max_columns', None)




def db_connection():

    id = "###########"
    pw = "###########"
    host = "##############"
    port = "############"
    db = "##########"

    engine = create_engine(f"mysql+pymysql://{id}:{pw}@{host}:{port}/{db}")
    
    return engine




def get_index_df(engine, date):
    
    index_query = f"""
        SELECT *
        FROM ftc.FTC_MAIN_INDEX_HIST_TB AS A
        WHERE 1=1
            AND A.search_dt = "{date}"
        """
    index_df = pd.read_sql(index_query, engine)

    return index_df





def get_main_df(engine):
    
    main_query = f"""
        SELECT *
        FROM ftc.FTC_HEAD_INFO_HIST AS A
        """
    main_df = pd.read_sql(main_query, engine)

    return main_df





def get_new_df(index_df, main_df):
    
    new_df = index_df[~index_df["page_id"].isin(main_df["page_id"])]
    new_df = new_df.reset_index(drop = True)

    return new_df





def to_ncp(s3, date, file_name):

    s3.put_object(Bucket = "winkstone-data-lake", Key = f"crawler/01_FTC/")

    local_file_path = f"./{date}/{file_name}.html"

    try:
        s3.upload_file(local_file_path, "winkstone-data-lake", f"crawler/01_FTC/{date}/{file_name}.html")

    except Exception as e:
        print(e)





def crawler(page_id):

    url = f"https://franchise.ftc.go.kr/mnu/00013/program/userRqst/view.do?firMstSn={page_id}"

    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        html_list = soup.select("table")
        table_list = []
        for i in range(len(html_list)):
            table_html = str(html_list[i])
            table_df = pd.read_html(table_html)[0]
            table_list.append(table_df)
        head = table_list[0]['상호'][0]
        head = head.replace("상호  ", "")
        brand = table_list[0]['영업표지'][0]
        brand = brand.replace("영업표지  ", "")

        if head != '상호':
            # print(f"{timestemp} Saved - {page_id}, {head} - {brand}")
            return soup
        
        else:
            # print(f"{timestemp} Pass - {page_id}, {head} - {brand}")
            pass
    else:
        # print(f"{timestemp} Retry - {page_id}, {head} - {brand}")
        time.sleep(1)
        crawler(page_id)





def save_html_file(folder_path, file_name, soup):

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    with open(f"{folder_path}/{file_name}.html", "w") as file:
        file.write(str(soup))




def get_s3_client():

    service_name = '##'
    endpoint_url = 'https://kr.object.fin-ncloudstorage.com'
    region_name = 'kr-standard'
    access_key = '#################'
    secret_key = '#####################'

    s3 = boto3.client(service_name, endpoint_url = endpoint_url, aws_access_key_id = access_key, aws_secret_access_key = secret_key)
    
    return s3




def main_html_download():

    engine = db_connection()

    date = datetime.today().strftime("%Y-%m-%d")

    print("Getting new data from URL ... ")
    index_df = get_index_df(engine, date)
    main_df = get_main_df(engine)
    new_df = get_new_df(index_df, main_df)


    print("Downloading HTML files in local and cloud ... ")
    s3 = get_s3_client()

    for page_id in tqdm(new_df["page_id"]):

        soup = crawler(page_id)

        folder_path = f"./{date}"
        file_name = str(page_id)

        save_html_file(folder_path, file_name, soup)
        to_ncp(s3, date, page_id)


    print("All done ! ")


if __name__ == '__main__':
    main_html_download()






