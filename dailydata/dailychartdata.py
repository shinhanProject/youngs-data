
import os
from dotenv import load_dotenv
import pandas as pd
import mojito
import pprint
from datetime import datetime, timedelta
import time

load_dotenv()

key = os.environ.get('kis_key')
secret = os.environ.get('kis_sec')
acc_no = os.environ.get('kis_no')

broker = mojito.KoreaInvestment(
    api_key=key,
    api_secret=secret,
    acc_no=acc_no
)

# 현재 날짜 구하기
end_date = datetime.now() - timedelta(days=1)


# # 올해 1월 1일 날짜 구하기
# start_date = datetime(end_date.year, 1, 1)

# 데이터를 가져올 날짜 범위 설정 (100일씩 나누어 요청)
# date_range = [start_date]
# while date_range[-1] < end_date:
#     date_range.append(date_range[-1] + timedelta(days=100))

json_data = {}

resp = broker.fetch_ohlcv(
    symbol="005930",
    timeframe='D',
    start_day=end_date.strftime('%Y%m%d'),
    end_day=end_date.strftime('%Y%m%d'),
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

pprint.pprint(json_data)
