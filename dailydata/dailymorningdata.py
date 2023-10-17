# 오전에 가져오는 데이터 -> 전일 대비 등락률, 전일 거래량
# corp_code/morning/year.json
import requests
import os
from dotenv import load_dotenv
import pandas as pd
import mojito
from datetime import datetime, timedelta
import boto3
from sqlalchemy import create_engine
import time
import json

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

# KRX key
krx_key = os.environ.get('krx_key')

# AWS S3를 위한 정보
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION")

# db연산을 위한 engine
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_database}')

# 상위 종목들의 코드, 이름 가져오는 쿼리
query = "SELECT corp_code, corp_name, stock_code FROM stock_corp_code"
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
# 오늘 날짜
date_string = datetime.today().strftime('%Y-%m-%d')

for j in range(len(stock_df)):
    code = stock_df.iloc[j, 0] # 회사 코드
    name = stock_df.iloc[j, 1] # 회사 이름
    stock_code = stock_df.iloc[j, 2] # 종목 코드
    json_data = {}

    # 전일 거래량
    resp = broker.fetch_ohlcv(
        symbol=stock_code,
        timeframe='D',
        start_day=end_date.strftime('%Y%m%d'),
        end_day=end_date.strftime('%Y%m%d'),
        adj_price=True
    )
    prdy_vol = resp['output1']['prdy_vol']

    # krx - 등락률
    krx_url = f"http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo?serviceKey={krx_key}&likeSrtnCd={stock_code}&numOfRows=1&resultType=json"
    krx_response = requests.get(krx_url)
    if krx_response.status_code == 200:
        try:
            data = krx_response.json()  # JSON 형식으로 응답 데이터를 파싱
            # 이제 'data' 변수에 파싱된 JSON 데이터가 저장됩니다.
        except Exception as e:
            print(f'JSON 디코딩 오류: {e}')
    else:
        print(f'요청 실패. 상태 코드: {krx_response.status_code}')
    items = data['response']['body']['items']['item'][0]

    json_data = {
        date_string : {
            'prdy_vol' : prdy_vol,
            'fltRt' : items['fltRt']
        }
    }
    print(json_data)
    file_key = f"{code}/morning/{year}.json"
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        s3_data = response['Body'].read().decode('utf-8')
        s3_data = json.loads(s3_data)
        s3_data.update(json_data)
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(s3_data))
        print(name, " 데이터 추가")
    except Exception as e:
        # 없는 경우 새로저장
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(json_data))
        print(name, " 데이터 신규 생성")
    time.sleep(3)