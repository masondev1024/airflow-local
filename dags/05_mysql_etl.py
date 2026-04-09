
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging 
import random

def _extract(**kwargs):
    pass

def _transform(**kwargs):
    '''
    ETL의 transform 담당
    kwargs를 이용하여 airflow context 정보 획득 -> ti
        - 다른 task에서 전될된 데이터 획득 -> t1.xcom_pull() 처리
    '''
    ti = kwargs['ti']
    
    # XCom에서 데이터 가져오기 (pull)
    data = ti.xcom_pull(
        key='extract_data',
        task_ids='extract_task_data' # 특정 task가 기록한 데이터를 획득
    )
    
    logging.info(f"Pulled data: {data}")
    
    # 간단한 변환 작업
    transformed_data = [x * 10 for x in data]
    # 3. Push: 가공된 데이터를 'transformed_result'라는 키로 게시판에 게시 (이게 빠졌습니다!)
    ti.xcom_push(key='transformed_result', value=transformed_data)
    logging.info(f"Transformed data pushed with key 'transformed_result': {transformed_data}")

def _load(**kwargs):
    ti = kwargs['ti']
    # 1. Transform 단계에서 게시판(XCom)에 올린 가공 데이터를 가져옴
    final_data = ti.xcom_pull(key='transformed_result', task_ids='transform_task_data')
    
    # 2. 실제 적재 (예: MySQL에 Insert 하거나 S3에 파일로 저장)
    # mysql_hook.run(f"INSERT INTO table VALUES ({final_data})") 
    logging.info(f"Final Load to Destination: {final_data}")



with DAG(
    dag_id = "05_mysql_etl",
    description="1개의 DAG에서 etl 수행 <-> n개의 DAG에서 수행",
    default_args = {
        'owner'             : 'de_2team_manager' , 
        'retries'           : 1 ,                  
        'retry_delay'       : timedelta(minutes=1)
    },      
    schedule_interval = '@daily', 
    start_date = datetime(2026,2,25), 
    catchup     = False,           
    tags        = ['etl','mysql'],
) as dag:
    t1 = PythonOperator(
        task_id="extract",
        python_callable = _extract
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable = _transform
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable = _load

    )


    t1 >> t2 >> t3