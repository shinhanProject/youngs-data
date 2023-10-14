# 종목별 재무제표 정보를 가져온다.
import pandas as pd
import requests
import mysql.connector
from mysql.connector import Error
from xml.etree.ElementTree import parse
from datetime import datetime
from dotenv import load_dotenv
import os
from tqdm import tqdm
from sqlalchemy import create_engine

# 환경변수 세팅
load_dotenv()

dart_key = os.environ.get('dart_key')
krx_key = os.environ.get('krx_key')

db_host = os.environ.get('db_host')
db_user = os.environ.get('db_user')
db_password = os.environ.get('db_password')
db_database = os.environ.get('db_database')

engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_database}')

# 상위 종목들의 코드, 이름 가져오기
query = "SELECT corp_code, corp_name FROM stockCode"

stock_df = pd.read_sql_query(query, engine)

for i in range(len(stock_df)):
    code = stock_df.iloc[i, 0]
    name = stock_df.iloc[i, 1]
    # 사업보고서 가져오기
    dart_response=requests.get(f"https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?crtfc_key={dart_key}&corp_code={code}&bsns_year=2018&reprt_code=11011&fs_div=CFS")
    dart_response.json()
    dict_dart=dart_response.json()
    financial_stat=pd.DataFrame(dict_dart["list"])
    financial_stat[["sj_nm","account_nm","thstrm_nm","thstrm_amount","frmtrm_nm","frmtrm_amount","bfefrmtrm_nm","bfefrmtrm_amount"]]
engine.dispose()
