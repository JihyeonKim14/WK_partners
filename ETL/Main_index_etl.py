

## Library and Settings
import pandas as pd
import numpy as np
import requests
import warnings
# import csv
# import multiprocessing
# import time
import os

# from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from tqdm import tqdm, trange
from datetime import datetime, timedelta
from sqlalchemy import create_engine

warnings.simplefilter(action='ignore', category=FutureWarning) # Ignore FutureWarnings
pd.set_option('display.max_columns', None) # Show all columns



def get_data():

    url = 'https://franchise.ftc.go.kr/mnu/00013/program/userRqst/list.do?searchCondition=&searchKeyword=&column=brd&selUpjong=&selIndus=&pageUnit=999999'

    print("URL connecting ... ")
    response = requests.get(url)

    if response.status_code == 200:
        print("HTML parsing ... ")
        soup = BeautifulSoup(response.text, 'html.parser')

        table = soup.select('table')[0]

        headers = [header.text.strip() for header in table.select('th')]
        rows = []

        print("Data processing ... ")
        for row in tqdm(table.select('tr')[1:]):
            cells = [cell.text.strip() for cell in row.select('td')]
            page_id = row.select('a')[0]['onclick'].split('=')[1][:-3]
            rows.append(cells + [page_id])

        new_df = pd.DataFrame(rows, columns=headers + ['page_id'])
        new_df.columns = ["index_no", "head_nm", "brand_nm", "ceo", "brand_sn", "first_reg_dt", "category", "page_id"]

        new_df["first_reg_dt"] = new_df["first_reg_dt"].str.replace(".", "-")
        new_df["search_dt"] = datetime.today().strftime("%Y-%m-%d")
        # new_df["search_tm"] = datetime.today().strftime("%H%M")

        # new_df["first_reg_dt"] = pd.to_datetime(new_df["first_reg_dt"], errors = "coerce")
        # new_df["search_dt"] = pd.to_datetime(new_df["search_dt"], errors = "coerce")

        new_df = new_df.astype({
            "index_no": int
            , "head_nm": 'string'
            , "brand_nm": 'string'
            , "ceo": 'string'
            , "brand_sn": 'string'
            , "first_reg_dt": 'string'
            , "category": 'string'
            , "page_id": 'string'
            , "search_dt": 'string'
            # , "search_tm": 'string'
        })

        # print("done ! ")

    else:
        print("connecting error ... ")
        print("retry ... ")

        get_data()

    return new_df


def main_etl():
    
    ## DB connection info.
    id = "#####"
    pw = "########"
    host = "########"
    port = "#####"
    db = "####"

    engine = create_engine(f"mysql+pymysql://{id}:{pw}@{host}:{port}/{db}")

    new_df = get_data()
    # db_df = get_db(engine)

    # new_data = new_df[~new_df["page_id"].isin(db_df["page_id"])] # 기존에 있는, 중복된 데이터 제외

    ## DB inserting
    try:
        print("DB inserting ... ")
        new_df.to_sql("FTC_MAIN_INDEX_HIST_TB", engine, if_exists = 'append', index = False)
        print("All done !")

    except Exception as e:
        print("DB inserting failed ... ")
        print(e)






if __name__ == '__main__':
    main_etl()






