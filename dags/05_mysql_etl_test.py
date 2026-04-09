from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging 
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import random
import pandas as pd
import os

# 1. 환경변수 및 경로 설정
DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)

# 2. 콜백 함수 정의
def _extract(**kwargs):
    # 수행 날짜 획득 (파일명 구분용)
    ds = kwargs.get('ds')
    data = [
        {
            "sensor_id" : f"SENSOR_{i+1}",
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature" : round(random.uniform(20.0, 150.0), 2),
            "status" : "on", 
        } for i in range(10) 
    ]
    
    file_path = f"{DATA_PATH}/sensor_data_{ds}.json"
    with open(file_path, 'w') as f:
        json.dump(data, f)
    
    # 다음 task를 위해 파일 경로를 XCom에 저장
    kwargs['ti'].xcom_push(key='file_path', value=file_path)
    logging.info(f"Saved to {file_path}")

def _transform(**kwargs):
    ti = kwargs['ti']
    # 파일 경로 가져오기
    file_path = ti.xcom_pull(key='file_path', task_ids='extract')
    
    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"No file found at {file_path}")

    # JSON 읽기 -> Pandas 변환
    with open(file_path, 'r') as f:
        raw_data = json.load(f)
    
    df = pd.DataFrame(raw_data)
    
    # 섭씨를 화씨로 변환 (C * 1.8 + 32)
    df['temperature_f'] = (df['temperature'] * 1.8 + 32).round(2)
    df.rename(columns={'temperature': 'temperature_c'}, inplace=True)
    
    # 변환된 데이터를 다시 JSON 혹은 CSV로 저장하거나 리스트로 전달
    transformed_list = df.to_dict(orient='records')
    ti.xcom_push(key='transformed_result', value=transformed_list)

def _load(**kwargs):
    ti = kwargs['ti']
    final_data = ti.xcom_pull(key='transformed_result', task_ids='transform')
    
    # MySqlHook을 이용한 적재 (Connection ID 확인 필요)
    # mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    # mysql_hook.insert_rows(table='sensor_readings', rows=final_data, ...)
    logging.info(f"Final Data for Load: {final_data}")

# 3. DAG 정의
with DAG(
    dag_id = "05_mysql_etl",
    default_args = {
        'owner' : 'de_2team_manager', 
        'retries' : 1, 
        'retry_delay' : timedelta(minutes=1)
    },      
    schedule_interval = '@daily', 
    start_date = datetime(2026,2,25), 
    catchup = False,           
    tags = ['etl','mysql'],
) as dag:

    task_extract = PythonOperator(
        task_id="extract",
        python_callable=_extract
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=_transform
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=_load
    )

    # 수정된 부분: 존재하지 않는 task_create_table 제거
    task_extract >> task_transform >> task_load