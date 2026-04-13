from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # 핵심
import json
import random
import pandas as pd
import os

DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)

def _transform(**kwargs):
    # _extract에서 추출한 데이터를 다른 Dag 에서 전달한 conf를 활용하여 추출 -> "dag_run" 활용
    # 1. dag_run을 통해서 이전 task에서 전달한 데이터 획득
    dag_run = kwargs['dag_run']
    json_file_path = dag_run.conf.get('json_path')
    # 로그 출력
    logging.info(f'전달받은 데이터 {json_file_path}')

    # 이 데이터를 df(pandas 사용, 소량데이터)로 로드
    df = pd.read_json( json_file_path )
     
    # 섭씨를 화씨로 일괄 처리(1번에 n개의 센서에서 데이터가 전달)    
    # 설정 : 우리 공장에서는 측정온도가 섭씨 100도 미만 정상 데이터로 간주한다.
    #       100도 이상 데이터는 이상탐지로 간주한다. -> 일단 버리는것으로 사용
    # 2. 100도 미만 데이터만 필터링(추출) -> pandas의 블리언 인덱싱 사용
    target_df = df[ df['temperature'] < 100 ].copy()    
    # 3. 파생변수로 화씨 데이터 구성 (temperature_f) = (섭씨 * 9/5) + 32
    target_df['temperature_f'] = (target_df['temperature'] * 9/5) + 32
    
    # 4. 전처리된 내용은 csv로 덤프 (s3로 업로드 고려)
    # 파일명 준비 /opt/airflow/dags/data/preprocessing_data_DAG수행날짜.csv
    file_path = f'{DATA_PATH}/preprocessing_data_{ kwargs['ds_nodash'] }.csv'
    # 저장
    target_df.to_csv( file_path, index=False ) # 인덱스 제외
    logging.info(f'전처리후 csv 저장 완료 {file_path}') # airflow가 aws에서 가동되면 s3로 저장

    # 5. csv 경로 xcom을 통해서 개시
    return file_path

with DAG(
    dag_id      = "06_multi_dag_2step_transform", 
    description = "transform 전용 DAG",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['transform', 'etl'],
) as dag:
    task_transform   = PythonOperator(
        task_id = "transform",
        python_callable = _transform
    )
    # 실습 : TriggerDagRunOperator 반영, 의존성까지 적용
    #       전달할 데이터의 키값 csv_path로 지정
    task_trigger_load_dag_run = TriggerDagRunOperator(
        task_id = "trigger_load",
        trigger_dag_id = "06_multi_dag_3step_load",
        conf    = {
            "csv_path":"{{ task_instance.xcom_pull(task_ids='transform') }}"
        },
        reset_dag_run= True,
        wait_for_completion = False 
    )
    # 의존성
    task_transform >> task_trigger_load_dag_run
