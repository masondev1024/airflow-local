'''
DAG -> DAG 작동시키는(오퍼레이터) 트리거 필요함 -> 핵심
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
import random
import os

# 2. 기본설정
# 프로젝트 내부 폴더를 데이터용으로 (~/dags/data)지정
# task 진행간 생성되는 파일을 동기화하도록 위치 지정 -> 향후 s3(데이터 레이크)로 대체 될수 있음

# 도커 내부에 생성된 컨테이너 상 워커내의 airflow 상의 지정한 데이터 위치
DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)

def _extract(**kwargs):
    # 스마트팩토리에 설치된 오븐 온도 센서에서  데이터가 발생되면 데이터레이크(s3, 어딘가에 존재)
    # 에 쌓이고 있다 (가정) => 추출해서 가져오는 단계로 가정
    
    # 더미 데이터 고려 구성 -> 1회성으로 10건 구성 -> [ {}, {}, ... ]
    data  = [
        { 
            "sensor_id" : f"SENSOR_{i+1}", # 장비 ID
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # YYYY-MM-DD hh:mm:ss
            "temperature": round( random.uniform(20.0, 150.0), 2),
            "status" : "on", # "off"
        } for i in range(10)   ]

    # 더미 데이터를 파일로 저장 (로그파일처럼) -> json 형태
    # /opt/airflow/dags/data/sensor_data_DAG수행날짜.json
    # 실습 -> 위의 데이터를 위의 형식으로 저장하시오 ( json.dump(data, f) )
    file_path = f'{DATA_PATH}/sensor_data_{ kwargs['ds_nodash'] }.json'
    with open(file_path, 'w') as f:
        json.dump(data, f)

    # 로그는 별도의 프로그램에서 지속적으로 발생시켜야 함(시뮬레이션 기준)
    # 현재는 편의상 airflow에 포함시킴 

    # XCOM을 통해서  task_trasform에게 전달 (로그의 경로를 전달, 실 데이터 전달 x(지양))
    logging.info(f'extract 한 로그 데이터 { file_path } ')
    return file_path

# 3. DAG 정의
with DAG(
    dag_id      = "06_multi_dag_1step_extract", 
    description = "extract 전용 DAG",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['etl', 'extract'],
) as dag:

    task_extract    = PythonOperator(
        task_id = "extract",
        python_callable = _extract
    )

    # 신규 추가 오퍼레이터로 만든 New Task
    # 다음 dag를 실행시키는 트리거 역할
    trigger_transform_dag_run = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="06_multi_dag_2step_transform", # 구동시킬 DAG id

        # 전달할 데이터 -> xcom을 통해서 획득 가능(동일 dag에 존재 -> jinja 템플릿 활용)
        conf={
            # f"{DATA_PATH}/sensor_data_{{ ds_nodash }}.json"  # 여기서 전달
            "json_path": "{{ task_instance.xcom_pull(task_ids='extract')}}"
        },

        # dag 수행시간 세팅 => 동일하게 맞추겠다. PythonOperator의 작동시간과 (컨셉)
        # 1개의 DAG에서 task 간 시간차와 유사하게 혹은 동일하게 맞추고자 하는 것.(굳이긴함)
        reset_dag_run= True,

        wait_for_completion=False # 다음 DAG 실행을 기다릴지 여부
    )
    
    task_extract >> trigger_transform_dag_run
