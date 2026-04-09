from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging
import json
import random
import pandas as pd
import os

# 1. 환경 설정
DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)

def _extract(**kwargs):
    data = [
        { 
            "sensor_id" : f"SENSOR_{i+1}",
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": round(random.uniform(20.0, 150.0), 2),
            "status" : "on",
        } for i in range(10)
    ]

    file_path = f"{DATA_PATH}/sensor_data_{kwargs['ds_nodash']}.json"
    with open(file_path, 'w') as f:
        json.dump(data, f)

    logging.info(f'Extract 완료: {file_path}')
    return file_path

def _transform(**kwargs):
    ti = kwargs['ti']
    json_file_path = ti.xcom_pull(task_ids='extract')
    
    # 수정: pd.read_json 사용
    df = pd.read_json(json_file_path)

    # 100도 미만 데이터 필터링 및 화씨 변환
    target_df = df[df['temperature'] < 100].copy()
    target_df['temperature_f'] = (target_df['temperature'] * 1.8) + 32
    target_df.rename(columns={'temperature': 'temperature_c'}, inplace=True)
    
    file_path = f"{DATA_PATH}/preprocessing_data_{kwargs['ds_nodash']}.csv"
    target_df.to_csv(file_path, index=False)
    
    logging.info(f'Transform 완료: {file_path}')
    return file_path

def _load(**kwargs):
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids='transform') # ID 일치 확인
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV 파일 부재: {csv_file_path}")

    df = pd.read_csv(csv_file_path)
    
    # DB 적재 형식 변환 (튜플 리스트)
    load_data = [tuple(x) for x in df[['sensor_id', 'timestamp', 'temperature_c', 'temperature_f']].values]

    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    mysql_hook.insert_rows(
        table='sensor_readings',
        rows=load_data,
        target_fields=['sensor_id', 'timestamp', 'temperature_c', 'temperature_f']
    )
    logging.info(f"Load 완료: {len(load_data)}건 적재")

# 3. DAG 정의
with DAG(
    dag_id = "05_mysql_etl", 
    default_args= {
        'owner' : 'de_2team_manager',        
        'retries' : 1,
        'retry_delay' : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date = datetime(2026,2,25),     
    catchup = False,
    tags = ['mysql', 'etl'],
) as dag:

    task_create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "mysql_default",
        sql = '''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sensor_id VARCHAR(50),
                timestamp DATETIME,
                temperature_c FLOAT,
                temperature_f FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''
    )
    
    task_extract = PythonOperator(
        task_id = "extract",
        python_callable = _extract
    )
    
    task_transform = PythonOperator(
        task_id = "transform", # trasform 오타 수정
        python_callable = _transform
    )
    
    task_load = PythonOperator(
        task_id = "load",
        python_callable = _load
    )

    # 5. 의존성 정의
    task_create_table >> task_extract >> task_transform >> task_load