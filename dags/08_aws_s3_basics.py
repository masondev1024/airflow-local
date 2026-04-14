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
from airflow.operators.bash import BashOperator
# 2. 환경변수 설정

# 2-1. 버킷명(iam계정-827913617635-region명-an)
# 827913617635 : root 계정 ID
# Region : ap-northeast-2
BUCKET_NAME = "de-ai-06-827913617635-ap-northeast-2-an" # 글로벌하게 고유한 이름 사용해야 하기 때문에 길다
# 2-2 업로드할 파일명 준비
FILE_NAME = 'hello.txt'
# 2-3. 업로드할 파일의 로컬내 위치 -> 컨테이너 기반
LOCAL_PATH = f'/opt/airflow/dags/data/{FILE_NAME}'

def _check_s3_file(**kwargs):
    # S3Hook을 사용하여 실제로 파일이 올라갔는지 확인 로직
    hook = S3Hook(aws_conn_id='aws_default')
    # exists = hook.check_for_key(FILE_NAME, BUCKET_NAME)
    # if exists:
    #     logging.info(f"성공: {FILE_NAME}이 {BUCKET_NAME}에 존재합니다.")
    # else:
    #     raise FileNotFoundError("S3 업로드 확인 실패!")
    keys = hook.list_keys(bucket_name=BUCKET_NAME)
    if not keys:
        raise ValueError('업로드 실패')
    for key in keys:
        logging.info(f'키 : {key}')
# 3. DAG 정의
with DAG(
    dag_id      = "08_aws_s3_basics", 
    description = "aws 연동, s3 업로드",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['s3', 'aws'],
) as dag:
    # task_create_file = BashOperator(
    #     task_id = "create_file",
    #     bash_command=f'echo "hello airflow & s3" > {LOCAL_PATH}'
    # )

# 4. Task 정의
    task_upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=LOCAL_PATH,       # 로컬에 있는 내 컴퓨터(airflow)의 어디에 있는 파일을 보낼 것인지 입력
        dest_key=FILE_NAME, # S3라는 거대한 저장소 안에서 **어떤 이름(Key)**으로 저장할 것인가
        # S3는 디렉토리 개념이 아니라 Key-Value 구조
        dest_bucket=BUCKET_NAME,    # 버킷 이름
        aws_conn_id="aws_default",  # Airflow UI에서 설정한 AWS 연결 ID
        replace=True,               # 동일 파일 존재 시 덮어쓰기
    )
    task_check_s3 = PythonOperator(
        task_id="check_s3",
        python_callable=_check_s3_file
    )
# 5. 의존성 
    # task_create_file >> 
    task_upload_to_s3 >> task_check_s3