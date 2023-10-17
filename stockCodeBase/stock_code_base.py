# 주식 코드 DB 생성 코드 - DART 기업 코드 정보
# 초기 데이터 세팅 -> 최초 1회만 실행
import pandas as pd
import requests
import zipfile
import mysql.connector
from mysql.connector import Error
from xml.etree.ElementTree import parse
from datetime import datetime
from dotenv import load_dotenv
import os
from tqdm import tqdm

load_dotenv()

dart_key = os.environ.get('dart_key')

db_host = os.environ.get('db_host')
db_user = os.environ.get('db_user')
db_password = os.environ.get('db_password')
db_database = os.environ.get('db_database')

# 설치
def download(url, file_name):
  with open(file_name, "wb") as file:
    response = requests.get(url)
    file.write(response.content)

if __name__ == '__main__':
  url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={dart_key}"
  download(url,"고유번호.zip")

# 압축 해제
zipfile.ZipFile('고유번호.zip').extractall()

tree = parse('CORPCODE.xml')
root = tree.getroot()
raw_list = root.findall('list')

corp_list = []

for i in range(0, len(raw_list)):
  corp_code = raw_list[i].findtext('corp_code')
  corp_name = raw_list[i].findtext('corp_name')
  stock_code_str = raw_list[i].findtext('stock_code')
  stock_code = stock_code_str if stock_code_str != ' ' else ' '  # 빈 값인 경우 None 처리
  modify_date_str = raw_list[i].findtext('modify_date')
  modify_date = datetime.strptime(modify_date_str, "%Y%m%d").date()  # 문자열을 날짜로 변환
  modify_date_formatted = modify_date.strftime("%Y-%m-%d")

  corp_list.append([corp_code, corp_name, stock_code, modify_date_formatted])


print("done")

corp_df = pd.DataFrame(corp_list, columns=[
    'corp_code',
    'corp_name',
    'stock_code',
    'modify_date'
])

# 데이터베이스 연결 설정
connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_database
)

if connection.is_connected():
    print("연결 성공")
    cursor = connection.cursor()

    # 데이터프레임의 데이터를 MySQL 데이터베이스 테이블에 삽입
    with tqdm(total=len(corp_df)) as pbar:
        for i, row in corp_df.iterrows():
            insert_query = "INSERT INTO dart_code (corp_code, corp_name, stock_code, modify_date) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (row['corp_code'], row['corp_name'], row['stock_code'], row['modify_date']))
            pbar.update(1)
    connection.commit()
    print("데이터가 MySQL 데이터베이스의 dartCode 테이블에 성공적으로 삽입되었습니다.")

# 연결 종료
if connection.is_connected():
    cursor.close()
    connection.close()
