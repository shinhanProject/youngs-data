# 시총 상위 종목에 대한 코드 조회 및 데이터 저장
# 초기 데이터 세팅 -> 초기 1회 실행
import os
import pandas as pd
import requests
import mysql.connector
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 환경변수 세팅
load_dotenv()
krx_key = os.environ.get('krx_key')
db_host = os.environ.get('db_host')
db_user = os.environ.get('db_user')
db_password = os.environ.get('db_password')
db_database = os.environ.get('db_database')

# db 연결

# SQLAlchemy 엔진 생성
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_database}')

# KRX로 기업에 대한 정보 요청
krx_url = f"http://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo?serviceKey={krx_key}&beginMrktTotAmt=20000000000000&resultType=json&numOfRows=15"
krx_response = requests.get(krx_url)

if krx_response.status_code == 200:
    try:
        data = krx_response.json()  # JSON 형식으로 응답 데이터를 파싱
        items = data['response']['body']['items']['item']  # 종목에 대한 값만 가져오기
    except Exception as e:
        print(f'JSON 디코딩 오류: {e}')
else:
    print(f'요청 실패. 상태 코드: {krx_response.status_code}')

# 기업의 이름, 코드
infos = []
for item in items:
    infos.append([item['itmsNm'], item['srtnCd']])

stocks_df = pd.DataFrame(columns=['corp_code', 'stock_code', 'name'])
temp_dfs = []  # 데이터프레임을 저장할 리스트

# db 조회 - 종목 코드로 조회
for i in range(len(infos)):
    cur_stock_code = infos[i][1]
    query_code = f"SELECT * FROM dartCode WHERE dartCode.stock_code = '{cur_stock_code}'" # 종목코드로 조회

    result_df = pd.read_sql_query(query_code, engine)
    if result_df.empty:
        pass
    else:
        temp_df = pd.DataFrame({'corp_code' : result_df['corp_code'], 'stock_code' : result_df['stock_code'], 'corp_name' : result_df['corp_name']})
        temp_dfs.append(temp_df)

# 테이블 저장
if temp_dfs:
    stocks_df = pd.concat(temp_dfs, ignore_index=True)
    stocks_df.to_sql(name='stock_corp_code', con=engine, if_exists='replace', index=False)
    print("저장 완료")

# 연결 종료
engine.dispose()
