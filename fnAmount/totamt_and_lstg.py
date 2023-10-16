# KRX를 통해 종목별 시총, 상장주식수를 가져온다.
import json
import time
import boto3
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

load_dotenv()

krx_key = os.environ.get('krx_key')

# mysql 연결 정보
db_host = os.environ.get('db_host')
db_user = os.environ.get('db_user')
db_password = os.environ.get('db_password')
db_database = os.environ.get('db_database')

# AWS S3를 위한 정보
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION")

# db
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_database}')

# 상위 종목들의 코드, 이름 가져오는 쿼리
query = "SELECT corp_code, stock_code, corp_name FROM stockCode"

# 종목 목록
stock_df = pd.read_sql_query(query, engine)
# s3
s3 = boto3.client('s3', region_name=aws_region,
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)
# bucket_name
bucket_name = 'youngs-financial-statements'
# 현재 년도
target_year = datetime.today().year
# 현재 데이터 저장을 위한 월
cur_month = datetime.today().month

with tqdm(total=len(stock_df)) as pbar:
    for i in range(len(stock_df)):
        code = stock_df.iloc[i, 0]
        stock_code = stock_df.iloc[i, 1]
        name = stock_df.iloc[i, 2]
        # 재시도 횟수를 설정합니다.
        max_retries = 3
        retries = 0

        while retries < max_retries:
            krx_response = requests.get(
                f"http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo?serviceKey={krx_key}&likeSrtnCd={stock_code}&numOfRows=1&resultType=json")

            if krx_response.status_code == 200:
                try:
                    file_key = f'{code}/totaldata/{target_year}-{cur_month}.json'

                    data = krx_response.json()
                    stock_data_all = data['response']['body']['items']['item'][0]
                    selected_data = {
                        '시가총액': stock_data_all['mrktTotAmt'],
                        '상장주식수': stock_data_all['lstgStCnt']
                    }
                    print(name, selected_data)
                    s3.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(selected_data))
                    break  # 성공하면 루프 탈출
                except Exception as e:
                    print(f'JSON 디코딩 오류: {e}')
            else:
                print(f'요청 실패. 상태 코드: {krx_response.status_code}')
                retries += 1
                if retries < max_retries:
                    time.sleep(3)  # 재시도 전 3초 대기
                    continue
                else:
                    print(f'재시도 횟수 초과. 종료.')
                    break

        time.sleep(3)
        pbar.update(1)