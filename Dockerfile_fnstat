# 베이스 이미지 선택
FROM --platform=linux/amd64 python:3.10

# 작업 디렉토리 설정
WORKDIR /financialStatements

# requirements.txt 파일 복사
COPY requirements.txt /financialStatements/
COPY .env /financialStatements/
# requirements.txt에 기반한 패키지 설치
RUN pip install -r requirements.txt

# 스크립트 파일 복사
COPY financialStatements/baseFinancialStat.py /financialStatements/

# 실행 명령 지정
CMD ["python", "baseFinancialStat.py"]