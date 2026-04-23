'''
DAG 에서 OpenSearch 검색 -> 데이터 회득
'''

# 1. 모듈 가져오기
from opensearchpy import OpenSearch
from datetime import datetime, timedelta
import time     
from airflow.operators.python import PythonOperator
from airflow import DAG
import pendulum # 서울시간대 간편하게 설정
from airflow.models import Variable

# 2. 환경변수 설정
# HOST, AUTH, 인덱스(상황에 따라 별도 구성가능) -> 검색어/패턴으로 구성/고정등

HOST = Variable.get('OPENSEARCH_HOST')
AUTH = (Variable.get('AUTH_NAME'), Variable.get('AUTH_PW'))



# 4.1 opensearch 통해 검색 후 결과 획득 콜백함수 (_searching_proc)
def _searching_proc(**kwargs):
    
    pass


# 3. DAG 정의
with DAG(
    dag_id = 'opensearch_to_python_dag',
    description="OpenSearch 검색 결과를 Python으로 획득하는 DAG",
    default_args={
        'owner': 'de_2team_manager',
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval='*/10 * * * *', # 이 표시는 10분마다 실행되는 스케줄링 패턴입니다.
    start_date= pendulum.datetime(2026,1,1, tz='Asia/Seoul'), # pendulum을 사용하여 서울 시간대로 시작 날짜 설정
    catchup=False, # Backfill 방지
    tags=['production', 'opensearch', 'aws']
    ) as dag:
    # 4. task 정의
    task_search_opensearch = PythonOperator(
        task_id='search_opensearch_task',
        python_callable=_searching_proc
    )
    # 5. task 간의 의존성 설정
    task_search_opensearch