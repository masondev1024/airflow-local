
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
# 추가분
#from airflow.providers.mysql.operators.mysql import MysqlOperator
# 범용 sql 오퍼레이터로 대체
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Load 처리시 sql에 전처리된 데이터를 밀어 넣을때 사용
from airflow.providers.mysql.hooks.mysql import MySqlHook
# 데이터
import json
import random
import pandas as pd  # 소량의 데이터(데이터 규모)
import os

# 2. 기본설정
# 프로젝트 내부 폴더를 데이터용으로 (~/dags/data)지정
# task 진행간 생성되는 파일을 동기화하도록 위치 지정 -> 향후 s3(데이터 레이크)로 대체 될수 있음

# 도커 내부에 생성된 컨테이너 상 워커내의 airflow 상의 지정한 데이터 위치
DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)


def _load(**kwargs):
    # csv => df => mysql 적제
    # 1. csv 경로 획득 => XCom을 통해서 이전 task(게시자)의 id를 이용하여 추출
    dag_run = kwargs['dag_run']
    csv_path = dag_run.conf.get('csv_path')

    # 2. csv -> df (도입 근거 => 소규모 데이터라 데이터프레임, 대규모는 스파크 사용)
    df = pd.read_csv(csv_path) # 판다스는 데이터 분석, 전처리에 사용

    # 3. mysql 연결 => MySqlHook 사용
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    conn       = mysql_hook.get_conn() 
    # 커넥션 획득 -> 입출력 연산 영향 있음(예외처리, with문 염두)(with문을 통해 open,close를 활용하지 않았다면 마지막에 무조건 닫는 코드 존재)
    
    # 7. 전체를 try ~ except로 감싸기(I/O)
    try:
        # 4. 커서를 획득하여 
        with conn.cursor() as cursor:        
            # 4-1. insert 구문 사용
            sql = '''
                insert into sensor_readings
                (sensor_id, timestamp,temperature_c, temperature_f)
                values (%s, %s, %s, %s)
            '''

            params = [
                (data['sensor_id'],data['timestamp'],data['temperature'],data['temperature_f'])
                for _ ,data in df.iterrows() # 데이터가 없을때까지 반복 -> 데이터가 한세트씩 추출
                # 위의 iterrows() 출력값 : sensor_id: (0,SENSOR_1), (1,timestamp: ...),(2, temperature: 25.5 )...
            ]
            logging.info(f'입력한 데이터(파라미터) {params}')
            cursor.executemany( sql, params ) # 한번에 데이터 밀어넣기
            # 4-2. 커밋
            conn.commit()
            logging.info(f'mysql에 적재 완료')
            pass        
    except Exception as e:
        logging.info(f'적재 오류 {e}')
        pass
    finally:
        # 5. 연결 종료
        if conn:
            conn.close()
            logging.info(f'mysql 연결 종료')
    pass

# 3. DAG 정의
with DAG(
    dag_id      = "06_multi_dag_3step_load", 
    description = "load 전용 DAG",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['load', 'etl'],
) as dag:
    # 4. task 정의
    task_create_table = SQLExecuteQueryOperator(
        # 테이블 생성, if not exists를 사용하여 무조건 sql이 일단 수행되게 구성 
        # -> 아니라면 fail 발생함(2회차부터)
        # 최초는 생성, 존재하면 pass => if not exists
        task_id = "create_table",
        # 연결정보
        conn_id = "mysql_default", # 대시보드에 admin>connectinos>하위에 사전 등록
        # sql
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
    
    task_load       = PythonOperator(
        task_id = "load",
        python_callable = _load
    )
    task_create_table >> task_load    

