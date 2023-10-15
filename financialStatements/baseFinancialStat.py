# 종목별 재무제표 정보를 가져온다.
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv
import os
from tqdm import tqdm
from sqlalchemy import create_engine
import json
import time

# 환경변수 세팅
load_dotenv()

dart_key = os.environ.get('dart_key')
krx_key = os.environ.get('krx_key')

db_host = os.environ.get('db_host')
db_user = os.environ.get('db_user')
db_password = os.environ.get('db_password')
db_database = os.environ.get('db_database')

engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306/{db_database}')

# 상위 종목들의 코드, 이름 가져오는 쿼리
query = "SELECT corp_code, corp_name FROM stockCode"

stock_df = pd.read_sql_query(query, engine)

# 재무제표 년도
target_year = datetime.today().year - 1


def filter_stat(f_df, result):
    # 필요 정보 목록
    target_account_nms = ["당기순이익(손실)", "당기순이익", "자본총계", "자산총계", "부채총계",
                          "유동자산", "유동부채", "재고자산", "영업이익(손실)", "영업이익",
                          "이자의 지급", "이자지급", "이자지급(영업)", "수익(매출액)", "매출액", "배당금지급", "배당금의 지급"]

    for target_account_nm in target_account_nms:
        filtered_data = f_df[f_df['account_nm'] == target_account_nm]

        if not filtered_data.empty:
            result[target_account_nm] = filtered_data.iloc[0].to_dict()
            # result[target_account_nm] = filtered_data.to_dict(orient='records')
    json_data = json.dumps(result, indent=4)
    json_result = json.loads(json_data)
    return json_result


with tqdm(total=len(stock_df)) as pbar:
    for i in range(len(stock_df)):
        code = stock_df.iloc[i, 0]
        name = stock_df.iloc[i, 1]
        # 사업보고서 가져오기
        dart_response=requests.get(f"https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?crtfc_key={dart_key}&corp_code={code}&bsns_year={target_year}&reprt_code=11011&fs_div=OFS")
        dart_response.json()
        dict_dart=dart_response.json()
        if dict_dart['status'] == '013': # 재무제표 데이터가 없는 경우
            pass
        else:
            financial_stat=pd.DataFrame(dict_dart["list"])
            f_df = financial_stat[["sj_nm","account_nm","thstrm_nm","thstrm_amount","frmtrm_nm","frmtrm_amount","bfefrmtrm_nm","bfefrmtrm_amount"]]
            result = {}
            temp = filter_stat(f_df, result)
            print(temp)

            # print(name, ' : ', temp['자본총계']['thstrm_amount'])
            print(name, ' : ', list(temp.keys()))

        time.sleep(3)
        pbar.update(1)

engine.dispose()
