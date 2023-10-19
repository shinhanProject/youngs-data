# 베이스 이미지 선택
FROM --platform=linux/amd64 python:3.10

# 작업 디렉토리 설정
WORKDIR /fnAmount

# requirements.txt 파일 복사
COPY requirements.txt /fnAmount/
COPY .env /fnAmount/
# requirements.txt에 기반한 패키지 설치
RUN pip install -r requirements.txt

# 스크립트 파일 복사
COPY fnAmount/totamt_and_lstg.py /fnAmount/

# 실행 명령 지정
CMD ["python", "totamt_and_lstg.py"]
