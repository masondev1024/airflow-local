from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 환경 변수 및 설정
BUCKET_NAME = "de-ai-06-827913617635-ap-northeast-2-an"
AWS_CONN_ID = "aws_default"
DATA_DIR = "/opt/airflow/dags/data"

def _generate_data(execution_date, **kwargs):
    """
    ETL 시뮬레이션: 데이터를 생성하고 CSV로 저장
    logical_date를 사용해 데이터 내부에 타임스탬프를 박는 것이 실무의 핵심입니다.
    """
    os.makedirs(DATA_DIR, exist_ok=True) # 해당 위치에 디렉토리 생성 만약 존재하면 넘어가셈
    
    # 1. 추출/생성 로직 (예시: 더미 데이터)
    data = {
        'order_id': [1, 2, 3],
        'product': ['A', 'B', 'C'],
        'created_at': [execution_date] * 3
    }
    df = pd.DataFrame(data)
    
    # 2. 파일명에 날짜를 포함시켜 중복 방지 (멱등성 확보)
    file_name = f"orders_{execution_date.strftime('%Y%m%d_%H')}.csv"
    local_path = os.path.join(DATA_DIR, file_name)
    
    df.to_csv(local_path, index=False)
    
    # 다음 오퍼레이터(S3 전송)에서 사용할 수 있도록 경로 전달 (XCom)
    kwargs['ti'].xcom_push(key='file_name', value=file_name)
    kwargs['ti'].xcom_push(key='local_path', value=local_path)
    logging.info(f"Successfully created: {local_path}")

def _verify_upload(bucket_name, **kwargs):
    """업로드된 파일의 존재 여부와 크기를 검증"""
    ti = kwargs['ti']
    file_name = ti.xcom_pull(key='file_name', task_ids='generate_csv_task')
    
    # S3 내 저장 경로 (Partitioning 적용)
    # logical_date 기반으로 폴더 구조 생성: data/yyyy/mm/dd/file.csv
    execution_date = kwargs['execution_date']
    s3_key = f"raw/orders/{execution_date.strftime('%Y/%m/%d')}/{file_name}"
    
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    if hook.check_for_key(s3_key, bucket_name):
        obj = hook.get_key(s3_key, bucket_name)
        logging.info(f"Verification Success: {s3_key} (Size: {obj.content_length} bytes)")
    else:
        raise ValueError(f"Verification Failed: {s3_key} not found")

with DAG(
    dag_id="s3_producer_optimized",
    description="ETL to S3 with Partitioning and Verification",
    default_args={
        'owner': 'de_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='@hourly', # 실무에선 보통 시간단위 혹은 배치단위
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['production', 's3', 'etl']
) as dag:

    # 1. 데이터 생성 (Pandas/ETL)
    generate_csv = PythonOperator(
        task_id="generate_csv_task",
        python_callable=_generate_data,
        provide_context=True
    )

    # 2. S3 업로드 (Partitioning 적용된 dest_key 사용)
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="{{ ti.xcom_pull(key='local_path', task_ids='generate_csv_task') }}",
        dest_key="raw/orders/{{ execution_date.strftime('%Y/%m/%d') }}/{{ ti.xcom_pull(key='file_name', task_ids='generate_csv_task') }}",
        dest_bucket=BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
        replace=True
    )

    # 3. 검증
    verify_upload = PythonOperator(
        task_id="verify_s3_upload",
        python_callable=_verify_upload,
        op_kwargs={'bucket_name': BUCKET_NAME}
    )

    # 4. 로컬 임시 파일 삭제 (리소스 관리)
    cleanup_local = BashOperator(
        task_id="cleanup_local_file",
        bash_command="rm {{ ti.xcom_pull(key='local_path', task_ids='generate_csv_task') }}"
    )

    generate_csv >> upload_to_s3 >> verify_upload >> cleanup_local