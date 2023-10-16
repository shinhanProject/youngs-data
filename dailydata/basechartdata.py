# 현재 년도 1월 1일 ~ 현재 날짜 ohlc 데이터 수집 및 저장 -> 최초 1회 실행
# corp_code/chartdata/year.json
import json
import os
from dotenv import load_dotenv
import pandas as pd
import mojito
import pprint
from datetime import datetime, timedelta
import time
import boto3
from sqlalchemy import create_engine

load_dotenv()

# mysql 연결 정보
db_host = os.environ.get('db_host')
db_user = os.environ.get('db_user')
db_password = os.environ.get('db_password')
db_database = os.environ.get('db_database')

# KIS API 사용을 위한 정보
key = os.environ.get('kis_key')
secret = os.environ.get('kis_sec')
acc_no = os.environ.get('kis_no')

# AWS S3를 위한 정보
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION")

# db연산을 위한 engine
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_database}')

# 상위 종목들의 코드, 이름 가져오는 쿼리
query = "SELECT corp_code, corp_name, stock_code FROM stockCode"
stock_df = pd.read_sql_query(query, engine)

broker = mojito.KoreaInvestment(
    api_key=key,
    api_secret=secret,
    acc_no=acc_no
)

# s3
s3 = boto3.client('s3', region_name=aws_region,
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)
# bucket_name
bucket_name = 'youngs-financial-statements'

# 저장 년도
year = datetime.today().year

# 현재 날짜 구하기
end_date = datetime.now()

# 올해 1월 1일 날짜 구하기
start_date = datetime(end_date.year, 1, 1)

# 데이터를 가져올 날짜 범위 설정 (100일씩 나누어 요청)
date_range = [start_date]
while date_range[-1] < end_date:
    date_range.append(date_range[-1] + timedelta(days=100))

# 종목별 차트 데이터 저장
for j in range(len(stock_df)):
    code = stock_df.iloc[j, 0] # 회사 코드
    name = stock_df.iloc[j, 1] # 회사 이름
    stock_code = stock_df.iloc[j, 2] # 종목 코드

    file_key = f"{code}/chart/{year}.json" # s3에 저장할 키 파일 이름

    json_data = {}
    for i in range(len(date_range) - 1):
        resp = broker.fetch_ohlcv(
            symbol=stock_code,
            timeframe='D',
            start_day=date_range[i].strftime('%Y%m%d'),
            end_day=date_range[i + 1].strftime('%Y%m%d'),
            adj_price=True
        )

        df = pd.DataFrame(resp['output2'])
        dt = pd.to_datetime(df['stck_bsop_date'], format="%Y%m%d")
        df.set_index(dt, inplace=True)
        df = df[['stck_oprc', 'stck_hgpr', 'stck_lwpr', 'stck_clpr']]
        df.columns = ['open', 'high', 'low', 'close']
        df.index.name = "date"

        for index, row in df.iterrows():
            date_str = index.strftime('%Y-%m-%d')
            json_data[date_str] = {
                'open': str(row['open']),
                'high': str(row['high']),
                'low': str(row['low']),
                'close': str(row['close'])
            }
        time.sleep(3)
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(json_data))
    print(name, ' 차트데이터 저장 완료')