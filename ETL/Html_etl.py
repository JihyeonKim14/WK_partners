

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




def html_parser(table_list):

    ### head_info
    ## 0 
    df1 = pd.read_html(str(table_list[0]))[0].loc[:0]

    df2 = pd.read_html(str(table_list[0]))[0].loc[1:2]
    df2 = df2.reset_index(drop = True)
    df2.columns = df2.iloc[0]
    df2 = df2[1:]
    df2 = df2.reset_index(drop = True)

    df3 = pd.read_html(str(table_list[0]))[0].loc[3:4]
    df3 = df3.reset_index(drop = True)
    df3.columns = df3.iloc[0]
    df3 = df3[1:]
    df3 = df3.reset_index(drop = True)

    final_df1 = pd.concat([df1, df2, df3], axis = 1)
    final_df1

    ## 1
    df = pd.read_html(str(table_list[1]))[0]
    cols = [df[0][0], df[0][1], df[2][1], df[4][1]]
    vals = [df[1][0], df[1][1], df[3][1], df[5][1]]

    final_df2 = pd.DataFrame([vals], columns=cols)
    final_df2

    ## 3
    df1 = pd.read_html(str(table_list[3]))[0]
    df1

    ## 4
    df2 = pd.read_html(str(table_list[4]))[0]
    df2

    head_info_df = pd.concat([final_df1, final_df2, df1, df2], axis = 1)

    head_info_df["상호"] = head_info_df["상호"].str.replace("상호  ", "")
    head_info_df["영업표지"] = head_info_df["영업표지"].str.replace("영업표지  ", "")
    head_info_df["대표자"] = head_info_df["대표자"].str.replace("대표자  ", "")
    head_info_df["법인설립등기일"] = head_info_df["법인설립등기일"].str.replace(".", "-")
    head_info_df["사업자등록일"] = head_info_df["사업자등록일"].str.replace(".", "-")
    head_info_df["대표번호"] = head_info_df["대표번호"].str.replace(" ", "")
    head_info_df["대표팩스 번호"] = head_info_df["대표팩스 번호"].str.replace(" ", "")
    head_info_df["최초등록일"] = head_info_df["최초등록일"].str.replace(".", "-")
    head_info_df["최종등록일"] = head_info_df["최종등록일"].str.replace(".", "-")
    head_info_df["주소"] = head_info_df["주소"].str.replace("우 : ", "")
    head_info_df["법인등록번호"] = head_info_df["법인등록번호"].str.replace(" ", "")
    head_info_df["사업자등록번호"] = head_info_df["사업자등록번호"].str.replace(" ", "")

    head_info_df

    ### head_fin
    ## 2 
    head_fin = pd.read_html(str(table_list[2]))[0]
    head_fin

    ## 5 - franchise_info
    df1 = pd.read_html(str(table_list[5]))[0]
    df1 = df1.T
    df1.columns = df1.iloc[0]
    df1 = df1[1:]
    df1 = df1.reset_index(drop = True)
    df1

    ## 9
    df2 = pd.read_html(str(table_list[9]))[0]
    df2 = df2.T
    df2.columns = df2.iloc[0]
    df2 = df2[1:]
    df2 = df2.reset_index(drop = True)
    df2

    ## 10
    df3 = pd.read_html(str(table_list[10]))[0]
    df3

    ## 11
    df4 = pd.read_html(str(table_list[11]))[0]
    df4 = df4.T
    df4.columns = df4.iloc[0]
    df4 = df4[1:]
    df4 = df4.reset_index(drop = True)
    df4

    franchise_info = pd.concat([df1, df2, df3, df4], axis = 1)

    franchise_info["가맹사업 개시일"] = franchise_info["가맹사업 개시일"].str.replace(" ", "")
    franchise_info["가맹사업 개시일"] = franchise_info["가맹사업 개시일"].str.replace(".", "-")

    ### franchise_cnt
    ## 6 
    df = pd.read_html(str(table_list[6]))[0]
    df = df.T
    df = df.reset_index(drop = False)
    df.columns = df.iloc[0]
    df = df[1:]
    df.columns = ["YEAR", "TYPE", "전체", "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종", "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]
    df = df.reset_index(drop = True)

    franchise_cnt = df

    ### franchise_hist
    ## 7
    df = pd.read_html(str(table_list[7]))[0]

    franchise_hist = df

    ### franchise_sales
    ## 8
    df = pd.read_html(str(table_list[8]))[0]
    df = df.T
    df = df.reset_index(drop = False)
    df.columns = df.iloc[0]
    df = df[1:]
    df.columns = ["YEAR", "TYPE", "전체", "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종", "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]
    df = df.reset_index(drop = True)

    franchise_sales = df

    ### violating
    ## 12
    df = pd.read_html(str(table_list[12]))[0]

    violating = df

    ### franchise_fee
    ## 13
    df1 = pd.read_html(str(table_list[13]))[0]
    df1

    ## 14
    df2 = pd.read_html(str(table_list[14]))[0]
    df2

    franchise_fee = pd.concat([df1, df2], axis = 1)

    ### contract_info
    ## 15
    df = pd.read_html(str(table_list[15]))[0]
    df.columns = [f"{level[0]}_{level[1]}" for level in df.columns]

    contract_info = df

    # display(head_info_df, head_fin, franchise_info, franchise_cnt, franchise_hist, franchise_sales, violating, franchise_fee, contract_info)

    return head_info_df, head_fin, franchise_info, franchise_cnt, franchise_hist, franchise_sales, violating, franchise_fee, contract_info



def main_html_etl():

    date = datetime.today().strftime("%Y-%m-%d")

    ## MariaDB connection info.
    id = "########################"
    pw = "######################"
    host = "####################"
    port = "########"
    db = "#####"
    engine = create_engine(f"mysql+pymysql://{id}:{pw}@{host}:{port}/{db}")

    print("Data reading & parsing ... ")
    directory = f'./{date}/'
    for file_name in tqdm(os.listdir(directory)):
        file_path = os.path.join(directory, file_name)

        with open(file_path) as fp:
            page_id = os.path.basename(file_path).split('.')[0]
            soup = BeautifulSoup(fp, "html.parser")

            table_list = soup.select("table")

            try:

                head_info_df, head_fin_df, franchise_info_df, franchise_cnt_df, franchise_hist_df, franchise_sales_df, violating_df, franchise_fee_df, contract_info_df = html_parser(table_list)

                head_info_df["page_id"] = page_id
                head_info_df["search_dt"] = date

                head_fin_df["page_id"] = page_id
                head_fin_df["search_dt"] = date

                franchise_info_df["page_id"] = page_id
                franchise_info_df["search_dt"] = date

                franchise_cnt_df["page_id"] = page_id
                franchise_cnt_df["search_dt"] = date

                franchise_hist_df["page_id"] = page_id
                franchise_hist_df["search_dt"] = date

                franchise_sales_df["page_id"] = page_id
                franchise_sales_df["search_dt"] = date

                violating_df["page_id"] = page_id
                violating_df["search_dt"] = date

                franchise_fee_df["page_id"] = page_id
                franchise_fee_df["search_dt"] = date

                contract_info_df["page_id"] = page_id
                contract_info_df["search_dt"] = date

            
                head_info_df.to_sql("FTC_HEAD_INFO_HIST", engine, if_exists = 'append', index = False)
                head_fin_df.to_sql("FTC_HEAD_FIN_HIST", engine, if_exists = 'append', index = False)
                franchise_info_df.to_sql("FTC_FRANCHISE_INFO_HIST", engine, if_exists = 'append', index = False)
                franchise_cnt_df.to_sql("FTC_FRANCHISE_CNT_HIST", engine, if_exists = 'append', index = False)
                franchise_hist_df.to_sql("FTC_FRANCHISE_HIST", engine, if_exists = 'append', index = False)
                franchise_sales_df.to_sql("FTC_FRANCHISE_SALES", engine, if_exists = 'append', index = False)
                violating_df.to_sql("FTC_VIOLATION_HIST", engine, if_exists = 'append', index = False)
                franchise_fee_df.to_sql("FTC_FRANCHISE_FEE_INFO_HIST", engine, if_exists = 'append', index = False)
                contract_info_df.to_sql("FTC_CONTRACT_INFO_HIST", engine, if_exists = 'append', index = False)
            
            except Exception as e:
                print(f"Failed to parse ... {page_id}")
                print(e)

    print("Local file deleting ... ")

    shutil.rmtree(f"{date}")

    print("All done ! ")



if __name__ == '__main__':
    main_html_etl()





