'''
- 원격 PC에서 AWS S3에 데이터를 업로드하는 간단한 DAG 생성
    - 엑세스키가 잘 작동하는지 체크
    - 데이터량에 따른 수행시간 체크 -> 데이터를 S3에 적재하는 방식에 대한 고민 (직접 or 서비스 이용)
- 설치 (host pc, local)
    - pip install apache-airflow-providers-amazon
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 2. 환경변수 설정

# 2-1. 버킷명(iam계정-827913617635-region명-an)
# 827913617635 : root 계정 ID
# Region : ap-northeast-2
BUCKET_NAME = "de-ai-06-827913617635-ap-northeast-2-an" # 글로벌하게 고유한 이름 사용해야 하기 때문에 길다
# 2-2 업로드할 파일명 준비
FILE_NAME = 'hello.txt'
# 2-3. 업로드할 파일의 로컬내 위치 -> 컨테이너 기반
LOCAL_PATH = f'/opt/airflow/dags/data/{FILE_NAME}'
# 3. DAG 정의
with DAG(
    
) as dag:

# 4. Task 정의

# 5. 의존성 