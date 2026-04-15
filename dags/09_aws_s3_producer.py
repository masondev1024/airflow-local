'''
- 목표
    - 데이터 생산(etl등 통해서) -> CSV -> s3 업로드(PUSH) 처리
    - 
'''
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 환경 변수 및 설정
BUCKET_NAME = "de-ai-06-827913617635-ap-northeast-2-an"
AWS_CONN_ID = "aws_default"
# 업로드할 파일명
FILE_NAME = 'sensor_data.csv'
# 버킷내에 특정 폴더 위치에 생성 -> KEY 지정 -> bucket/income/xx.csv
S3_KEY      = f'income/{FILE_NAME}'
# 업로드할 로컬 파일의 위치(컨테이너 -> 리눅스 기반)
LOCAL_PATH = f'/opt/airflow/dags/data/{FILE_NAME}'


with DAG(
    dag_id="09_aws_s3_producer",
    description="aws 연동, s3 업로드",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = None, # Scheduling x -> Trigger Execute
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3'],
) as dag:

    # 1. 데이터 생성 (Pandas/ETL)
    task_create_dummy_data_csv = BashOperator(
        task_id="task_create_dummy_data_csv",
        bash_command = f'echo "id, timestamp,value\n1,$(date),100\n2,$(date),$(date),500" > {LOCAL_PATH}'
    )

    # 2. S3 업로드 (Partitioning 적용된 dest_key 사용)
    task_upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="task_upload_to_s3",
        filename=LOCAL_PATH,
        dest_key=S3_KEY,
        dest_bucket=BUCKET_NAME,
        aws_conn_id= "aws_default"
    )


    task_create_dummy_data_csv >> task_upload_to_s3 