import re
import pandas as pd
import numpy as np
from tqdm import tqdm
import mysql.connector
from sqlalchemy import create_engine

import warnings
warnings.filterwarnings('ignore')
from datetime import datetime

import requests
from io import BytesIO
import PyPDF2

pd.set_option('display.max_columns', None)
pd.options.display.float_format = '{:.2f}'.format


# ## 데이터 테이블 가져오는 함수

def db_connection():

    id = "#########"
    pw = "############"
    host = "##########"
    port = "########"
    db = "#############"

    engine = create_engine(f"mysql+pymysql://{id}:{pw}@{host}:{port}/{db}")
    
    return engine



def get_cnt_df(engine):
    
    index_query = f"""
        SELECT *
        FROM ftc.FTC_FRANCHISE_CNT_HIST
        """
    df_franchise_cnt = pd.read_sql(index_query, engine)

    return df_franchise_cnt


def get_hist_df(engine):
    
    index_query = f"""
        SELECT *
        FROM ftc.FTC_FRANCHISE_HIST
        WHERE 1=1
        """
    df_franchise_hist = pd.read_sql(index_query, engine)

    return df_franchise_hist



def get_fin_df(engine):
    
    index_query = f"""
        SELECT *
        FROM ftc.FTC_HEAD_FIN_HIST
        WHERE 1=1
        """
    df_head_fin = pd.read_sql(index_query, engine)

    return df_head_fin





def get_head_df(engine):
    
    index_query = f"""
        SELECT *
        FROM ftc.FTC_HEAD_INFO_HIST
        WHERE 1=1
        """
    df_head_info = pd.read_sql(index_query, engine)

    return df_head_info




def get_main_df(engine):
    
    index_query = f"""
        SELECT *
        FROM ftc.FTC_MAIN_INDEX_HIST_TB
        WHERE 1=1
        """
    df_main_index = pd.read_sql(index_query, engine)

    return df_main_index


# ## 전처리 함수




def long_list(dff_result,missing_pgid_list):
    # 3년 데이터만

    # 3년 이상 데이터가 있는 가맹점 list
    up_3_pg = dff_result['page_id'].value_counts()
    up_3_pg = up_3_pg[up_3_pg.values>= 3]
    up_3_pg = list(up_3_pg.index)

    # page_id 별로 group
    df_group = dff_result.groupby(['page_id', '연도'])[['신규개점', '계약종료','계약해지', '전체']].mean()
    df_group.rename(columns={'전체':'가맹점수'}, inplace=True)
    df_group = df_group.loc[up_3_pg]

    updownpercent = []
    uppercent = []
    stoppercent = []

    for i in range(len(up_3_pg)):

        # 최근년도의 "전체"가 0인 경우에 최근년도-1 데이터로 채운 list에 해당하는 page_id
        if up_3_pg[i] in missing_pgid_list:
            df = df_group.loc[up_3_pg[i]]
            df['가맹점수_s'] = df['가맹점수'].shift()
            df['가맹점수_s'] = df['가맹점수_s'].fillna(0)

            # 개점_증감율 계산
            n1 = round(((df['신규개점'].iloc[1] / int(df['가맹점수_s'].iloc[1])) * 100),2)
            n_lst = [np.NaN, n1, n1]
            uppercent.append(n_lst)

            # 폐점율 계산
            s1 = round((((df['계약종료'].iloc[1]+df['계약해지'].iloc[1]) / abs(int(df['가맹점수'].iloc[1]))) * 100),2)
            s_lst = [np.NaN, s1, s1]
            stoppercent.append(s_lst)

            # 가맹점_증감율 계산
            f1 = round(((df['가맹점수'].iloc[1] - df['가맹점수'].iloc[0]) / df['가맹점수'].iloc[0] * 100), 2)
            f_lst = [np.NaN, f1, f1]
            updownpercent.append(f_lst)
        
        else:
            df = df_group.loc[up_3_pg[i]]
            df['가맹점수_s'] = df['가맹점수'].shift()
            df['가맹점수_s'] = df['가맹점수_s'].fillna(0)

            # 개점_증감율 계산
            n1 = round(((df['신규개점'].iloc[1] / int(df['가맹점수_s'].iloc[1])) * 100),2)
            n2 = round(((df['신규개점'].iloc[2] / int(df['가맹점수_s'].iloc[2])) * 100),2)
            n_lst = [np.NaN, n1, n2]
            uppercent.append(n_lst)
            
            # 폐점율 계산
            s1 = round((((df['계약종료'].iloc[1]+df['계약해지'].iloc[1]) / abs(int(df['가맹점수'].iloc[1]))) * 100),2)
            s2 = round((((df['계약종료'].iloc[2]+df['계약해지'].iloc[2]) / abs(int(df['가맹점수'].iloc[2]))) * 100),2)
            s_lst = [np.NaN, s1, s2]
            stoppercent.append(s_lst)

            # 가맹점_증감율 계산
            f1 = round(((df['가맹점수'].iloc[1] - df['가맹점수'].iloc[0]) / df['가맹점수'].iloc[0] * 100), 2)
            f2 = round(((df['가맹점수'].iloc[2] - df['가맹점수'].iloc[1]) / df['가맹점수'].iloc[1] * 100), 2)
            f_lst = [np.NaN, f1, f2]
            updownpercent.append(f_lst)

    df_group['가맹점_증감율'] = sum(updownpercent, [])
    df_group['개점_증가율'] = sum(uppercent, [])
    df_group['폐점율'] = sum(stoppercent, [])
    

    return df_group

# ## 가설1 함수




# 가설 1을 통과한 page id들을 반환 

def condition1(df_group):
    con1_pg_id_list = []
    df_group.reset_index(inplace=True)
    ind_list = list(range(2, df_group.shape[0], 3)) # 가장 최근년도

    for i in range(df_group.shape[0]):
        if i in ind_list:
            if df_group['연도'].iloc[i] == 2022:
                # 2020->2021년도, 2021->2022년도 15%
                if (float(df_group['가맹점_증감율'].iloc[i]) > 15) & (float(df_group['가맹점_증감율'].iloc[i-1]) > 15):
                    pg_id = df_group['page_id'].iloc[i]
                    con1_pg_id_list.append(pg_id)
                else:
                    pass
            elif df_group['연도'].iloc[i] == 2021:
                # 2019->2020년도는 -15% / 2020->2021년도는 15%
                if (float(df_group['가맹점_증감율'].iloc[i]) > 15) & (float(df_group['가맹점_증감율'].iloc[i-1]) > -15):
                    pg_id = df_group['page_id'].iloc[i]
                    con1_pg_id_list.append(pg_id)

    return con1_pg_id_list


# ## 가설2 함수




def condition2(df_group):
    con2_pg_id_list = []
    ind_list = list(range(2, df_group.shape[0], 3)) # 가장 최근년도
    for i in range(df_group.shape[0]):
        if i in ind_list:
            if (float(df_group['폐점율'].iloc[i]) <= 10):
                pg_id = df_group['page_id'].iloc[i]
                con2_pg_id_list.append(pg_id)
    return con2_pg_id_list


# ## 가설3 함수
# 



def condition3(df_fin_group):
    df_fin_group = df_fin_group.reset_index()
    con3_pg_id_list = []
    for i in range(2,df_fin_group.shape[0], 3):
        if (float(df_fin_group['영업이익'].iloc[i]) >= 0) & (float(df_fin_group['영업이익'].iloc[i]) < 10000000):
            pg_id = df_fin_group['page_id'].iloc[i]
            con3_pg_id_list.append(pg_id)
    return con3_pg_id_list


# ## 가설4 함수



def condition4(df_group):
    con4_pg_id_list = []
    for i in range(2, df_group.shape[0], 3):
        if (df_group['가맹점수'].iloc[i] > 8) & (df_group['가맹점수'].iloc[i] < 100):
            pg_id = df_group['page_id'].iloc[i]
            con4_pg_id_list.append(pg_id)
    return con4_pg_id_list


# ## Main 함수

def main_create_long_list():

    engine = db_connection()

    date = datetime.today().strftime("%Y-%m-%d")

    print("Load Data . . .")
    
    df_franchise_cnt = get_cnt_df(engine)
    df_franchise_hist = get_hist_df(engine)
    df_head_fin = get_fin_df(engine)
    df_head_info = get_head_df(engine)
    df_main_index = get_main_df(engine)

    dff_main_index = df_main_index.copy()
    dff_main_index = dff_main_index[dff_main_index['search_dt'] == date].drop_duplicates('page_id')

    print('단일한 프랜차이즈 개수:', len(dff_main_index['page_id'].unique()))
    df_head_info['page_id'] = df_head_info['page_id'].astype(int) # merge 하려고 데이터 타입 일치 시킨 것
    dff_main_index['page_id'] = dff_main_index['page_id'].astype(int)
    df_head_fin['page_id'] = df_head_fin['page_id'].astype(int)
    
    
    df_total = pd.merge(dff_main_index, df_head_info, on='page_id', how='left')

    # 상호가 null이 아닌 데이터만
    df_total = df_total[df_total['상호'].notnull()]
    df_total = df_total[['index_no', 'head_nm', 'brand_nm', 'ceo', 
        'category', 'page_id', '연도','브랜드 수']]
    df_total = df_total.drop_duplicates()


    df_franchise_cnt_1 = df_franchise_cnt[df_franchise_cnt.iloc[:,2:-2].isnull().sum(axis=1) != 18].fillna(0)
    df_franchise_cnt_2 = df_franchise_cnt[df_franchise_cnt.iloc[:,2:-2].isnull().sum(axis=1) == 18]
    df_franchise_cnt = pd.concat([df_franchise_cnt_1, df_franchise_cnt_2], axis=0)
    df_franchise_hist_1 = df_franchise_hist[df_franchise_hist.iloc[:,1:-2].isnull().sum(axis=1) != 4].fillna(0)
    df_franchise_hist_2 = df_franchise_hist[df_franchise_hist.iloc[:,1:-2].isnull().sum(axis=1) == 4]
    df_franchise_hist = pd.concat([df_franchise_hist_1, df_franchise_hist_2], axis=0)


    ## 데이터프레임 정제
    df_franchise_cnt['YEAR'] = df_franchise_cnt['YEAR'].str[:4] # 뒤에 "년" 제거
    df_franchise_cnt.rename(columns={"YEAR":"연도"}, inplace=True)

    df_franchise_hist['연도'] = df_franchise_hist['연도'].astype(int)
    df_franchise_cnt['연도'] = df_franchise_cnt['연도'].astype(int)

    df_franchise_hist['page_id'] = df_franchise_hist['page_id'].astype(int)
    df_franchise_cnt['page_id'] = df_franchise_cnt['page_id'].astype(int)

    df_franchise_hist = df_franchise_hist.drop_duplicates()
    df_franchise_cnt = df_franchise_cnt.drop_duplicates()

    # 전체 가맹점 수와 연도별 신규 가맹점 수 merge
    df_newftc = pd.merge(df_franchise_hist, df_franchise_cnt, on=["page_id","연도"])
    df_newftc = df_newftc.iloc[:,:9]
    df_newftc = df_newftc[df_newftc['TYPE'] =='전체']
    
    # 2019년 이후만
    df_total['page_id'] = df_total['page_id'].astype(int)

    df_total_ = pd.merge(df_total, df_newftc, on='page_id', how='left')


    df_total_ = df_total_[['head_nm', 'brand_nm', 'ceo', 'page_id','연도_y', '신규개점', '계약종료', '계약해지', '전체']]
    df_total_ = df_total_[df_total_['연도_y'] >= 2019] # 2019년 이후의 데이터들만
    df_total_.rename(columns={'연도_y':'연도'}, inplace=True)


    df_fin = pd.merge(df_total_, df_head_fin, how='left', on=['page_id','연도'])
    df_fin.loc[df_fin['head_nm'] == '주시회사뚠뚠푸드', 'head_nm'] = '주식회사뚠뚠푸드' # 오타 수정
    df_fin = df_fin.drop_duplicates()
    df_fin = df_fin.reset_index(drop=True)

    # 전체 가맹점 리스트
    pg_list = list(df_fin['page_id'].unique())
    print('전체 가맹점 리스트 개수1:', len(pg_list))

    # 결과 DF
    df_fin_result = pd.DataFrame(columns=df_fin.columns)

    for pg in pg_list:
        df = df_fin[df_fin['page_id'] == pg]
        year_list = list(df['연도'].unique())
        year_list = sorted(year_list, reverse=True)[:3]
        for i in range(3):
            try:
                df_fin_result = pd.concat([df_fin_result, df[df['연도'] == year_list[i]]], axis=0)
            except:
                continue

    fin_up_3_pg = df_fin_result['page_id'].value_counts()
    fin_up_3_pg = fin_up_3_pg[fin_up_3_pg.values>= 3] # 3개년도 이상인 가맹점들만
    fin_up_3_pg = list(fin_up_3_pg.index)

    # page_id 별로 group
    df_fin_result['영업이익'] = df_fin_result['영업이익'].fillna(0)
    df_fin_result['영업이익'] = df_fin_result['영업이익'].astype(int)
    df_fin_group = df_fin_result.groupby(['page_id', '연도'])[['영업이익']].mean()
    df_fin_group = df_fin_group.loc[fin_up_3_pg]



    # 전체 가맹점 리스트
    pg_list = list(df_total_['page_id'].unique())
    print('전체 가맹점 리스트 개수2:', len(pg_list))

    # 결과 DF
    df_result = pd.DataFrame(columns=df_total_.columns)

    for pg in pg_list:
        df = df_total_[df_total_['page_id'] == pg]
        year_list = list(df['연도'].unique())
        year_list = sorted(year_list, reverse=True)[:3] # 각 가맹점별 최근 3년 데이터들만 출력
        for i in range(3):
            try:
                df_result = pd.concat([df_result, df[df['연도'] == year_list[i]]], axis=0)
            except:
                continue
    

    print("line 398")

    # 최종데이터 프레임 
    dff_result = df_result.copy()
    dff_result.reset_index(drop=True,inplace=True)

    dff_result['전체'] = dff_result['전체'].fillna(0)
    dff_result['전체'] = dff_result['전체'].astype('int')

    # 최근년도-1 데이터는 있으나, 최근년도 데이터 중 "전체"의 값이 0인 경우 있음
    # 최근년도의 "전체" 값이 0인 경우, 최근년도-1 데이터로 채움



    print("가설적용 진행")

    missing_pgid_list = []
    for i in range(0, dff_result.shape[0]-1, 3):
        cnt = dff_result.iloc[i]['전체']
        if cnt == 0:
            dff_result.loc[i, '전체'] = dff_result.iloc[i+1]['전체']
            missing_pgid_list.append(dff_result.iloc[i]['page_id'])

    df_group = long_list(dff_result,missing_pgid_list)

    con1_pg_id_list = condition1(df_group)
    con2_pg_id_list = condition2(df_group)
    con3_pg_id_list = condition3(df_fin_group)
    con4_pg_id_list = condition4(df_group)

    set1 = set(con1_pg_id_list)
    set2 = set(con2_pg_id_list)
    set3 = set(con3_pg_id_list)
    set4 = set(con4_pg_id_list)

    result_list = list(set1 & set2 & set3 & set4)

    result_nm=[]
    for pg in result_list:
        result = df_total_[df_total_['page_id'] == pg]['brand_nm'].values[0]
        result_nm.append(result)
    #print(len(result_nm))

    
    df_final_2022 = pd.DataFrame(columns=['상호', '영업표지', '업종', '브랜드 수', '2020년 가맹점 수', '2021년 가맹점 수', '2022년 가맹점 수', '가맹점증가율_2021(%)',  '가맹점증가율_2122(%)', '폐점율_2122(%)', '2022년 영업이익(천원)', 'page_id'])
    df_final_n2022 = pd.DataFrame(columns=['상호', '영업표지', '업종', '브랜드 수', '2019년 가맹점 수', '2020년 가맹점 수',  '2021년 가맹점 수', '가맹점증가율_1920(%)', '가맹점증가율_2021(%)', '폐점율_2021(%)', '2021년 영업이익(천원)', 'page_id'])
    
    

    for i in range(len(result_nm)):
        pg_id = result_list[i]
        nm = result_nm[i]
        result_head_info = df_total[df_total['brand_nm'] == nm][['head_nm', 'brand_nm', 'category', '브랜드 수', 'page_id']].reset_index(drop=True)
        result_head_info.columns = ['상호', '영업표지', '업종', '브랜드 수', 'page_id']
        result_franc_up = df_group[df_group['page_id'] == pg_id][['연도', '가맹점수', '가맹점_증감율', '폐점율']]

        if result_franc_up['연도'].iloc[2] == 2022:
            result_franc_up = result_franc_up[['가맹점수', '가맹점_증감율', '폐점율']]
            result_franc_up = result_franc_up.fillna(0)
            result_franc_up = result_franc_up.stack().to_frame().T.droplevel(level=0,axis=1)
            result_franc_fin = df_fin_result[df_fin_result['page_id'] == pg_id][['영업이익']].iloc[0]
            result_franc_fin = pd.DataFrame(result_franc_fin).T.reset_index(drop=True)
            df = pd.concat([result_head_info, result_franc_up], axis=1)
            df = pd.concat([df, result_franc_fin], axis=1)
            df.columns = ['상호', '영업표지', '업종', '브랜드 수', 'page_id', '2020년 가맹점 수', '가맹점증가율(%)', '폐점율(%)', '2021년 가맹점 수', '가맹점증가율_2021(%)', '폐점율_2021(%)', '2022년 가맹점 수', '가맹점증가율_2122(%)',  '폐점율_2122(%)', '2022년 영업이익(천원)']
            df = df[['상호', '영업표지', '업종', '브랜드 수', '2020년 가맹점 수', '2021년 가맹점 수', '2022년 가맹점 수', '가맹점증가율_2021(%)', '가맹점증가율_2122(%)', '폐점율_2122(%)', '2022년 영업이익(천원)', 'page_id']]
            df_final_2022 = pd.concat([df_final_2022, df], axis=0, ignore_index=True)
            df_final_n2022['브랜드 수'] = df_final_n2022['브랜드 수'].fillna(0)
            df_final_2022['브랜드 수'] = df_final_2022['브랜드 수'].astype(int)

        elif result_franc_up['연도'].iloc[2] == 2021:
            result_franc_up = result_franc_up[['가맹점수', '가맹점_증감율', '폐점율']]
            result_franc_up = result_franc_up.fillna(0)
            result_franc_up = result_franc_up.stack().to_frame().T.droplevel(level=0,axis=1)
            result_franc_fin = df_fin_result[df_fin_result['page_id'] == pg_id][['영업이익']].iloc[0]
            result_franc_fin = pd.DataFrame(result_franc_fin).T.reset_index(drop=True)
            df = pd.concat([result_head_info, result_franc_up], axis=1)
            df = pd.concat([df, result_franc_fin], axis=1)
            df.columns = ['상호', '영업표지', '업종', '브랜드 수', 'page_id', '2019년 가맹점 수', '가맹점증가율(%)', '폐점율(%)', '2020년 가맹점 수', '가맹점증가율_1920(%)', '폐점율_1920(%)', '2021년 가맹점 수', '가맹점증가율_2021(%)',  '폐점율_2021(%)', '2021년 영업이익(천원)']
            df = df[['상호', '영업표지', '업종', '브랜드 수', '2019년 가맹점 수', '2020년 가맹점 수', '2021년 가맹점 수', '가맹점증가율_1920(%)',  '가맹점증가율_2021(%)', '폐점율_2021(%)', '2021년 영업이익(천원)', 'page_id']]
            df_final_n2022 = pd.concat([df_final_n2022, df], axis=0, ignore_index=True)
            df_final_n2022['브랜드 수'] = df_final_n2022['브랜드 수'].fillna(0)
            df_final_n2022['브랜드 수'] = df_final_n2022['브랜드 수'].astype(int)
        else:
            print('ERROR')

    pdf_url_list_2022 = []
    df_final_2022_pg_list = df_final_2022['page_id']

    print(f"df_final_2022_pg_list 길이 = {len(df_final_2022_pg_list)}개")
    
    for i in range(len(df_final_2022_pg_list)):
        pg_id = df_final_2022_pg_list.iloc[i]
        url = f"https://franchise.ftc.go.kr/cmm/fms/firOpenPdfView.do?firMstSn={pg_id}"
        # Download PDF file using requests
        res = requests.get(url)
        # Open PDF file
        pdf_file = BytesIO(res.content)
        try:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            pdf_url_list_2022.append(url)
        except:
            pdf_url_list_2022.append("pdf 존재하지 않음")

    newList = [x for x in pdf_url_list_2022 if x != "pdf 존재하지 않음"]

    print(f"pdf_url_list_2022 데이터의 개수 = {len(pdf_url_list_2022)}개")
    print(f"pdf가 있는 데이터의 개수 = {len(newList)}개")

    pdf_url_list_n2022 = []
    df_final_n2022_pg_list = df_final_n2022['page_id']
    for i in range(len(df_final_n2022_pg_list)):
        pg_id = df_final_n2022_pg_list.iloc[i]
        url = f"https://franchise.ftc.go.kr/cmm/fms/firOpenPdfView.do?firMstSn={pg_id}"
        # Download PDF file using requests
        res = requests.get(url)
        # Open PDF file
        pdf_file = BytesIO(res.content)
        try:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            pdf_url_list_n2022.append(url)
        except:
            pdf_url_list_n2022.append("pdf 존재하지 않음")

    newList = [x for x in pdf_url_list_n2022 if x != "pdf 존재하지 않음"]
    
    print(f"pdf_url_list_2022 데이터의 개수 = {len(pdf_url_list_n2022)}개")
    print(f"pdf가 있는 데이터의 개수 = {len(newList)}개")
    

    df_final_2022 = df_final_2022.replace(np.inf, '계산불가')
    df_final_n2022 = df_final_n2022.replace(np.inf, '계산불가')

    df_final_2022 = df_final_2022.reset_index(drop=True)
    df_final_n2022 = df_final_n2022.reset_index(drop=True)

    
    df_final_2022['search_dt'] = date
    df_final_n2022['search_dt'] = date

    print(f"df_final_2022의 길이 = {len(df_final_2022)}개")
    print(f"df_final_n2022의 길이 = {len(df_final_n2022)}개")


    df_final_2022['pdf_url'] = pdf_url_list_2022
    df_final_n2022['pdf_url'] = pdf_url_list_n2022
    
    df_final_2022.dropna(subset=['2020년 가맹점 수', '2021년 가맹점 수', '2022년 가맹점 수', '가맹점증가율_2021(%)',  '가맹점증가율_2122(%)', '폐점율_2122(%)', '2022년 영업이익(천원)'], inplace=True)
    df_final_n2022.dropna(subset=['2019년 가맹점 수', '2020년 가맹점 수',  '2021년 가맹점 수', '가맹점증가율_1920(%)', '가맹점증가율_2021(%)', '폐점율_2021(%)', '2021년 영업이익(천원)'], inplace=True)


    df_final_2022.to_sql("FTC_LONGLIST_RESULT_2022", engine, if_exists = 'append', index = False)
    df_final_n2022.to_sql("FTC_LONGLIST_RESULT_2021", engine, if_exists = 'append', index = False)

    
    print("All done!")


if __name__ == '__main__':
    main_create_long_list()





