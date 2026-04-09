'''
- etl 간단하게 적용, 스마트팩토리상 온도 센서에 대한 ETL 처리, mysql 사용
'''
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

def _trasform(**kwargs):
    # _extract에서 추출한 데이터를 XCom을 통해서 획득    
    # 1. XCOM을 통해서 이전 task에서 전달한 데이터 획득
    ti = kwargs['ti']
    json_file_path = ti.xcom_pull(task_ids='extract')
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
    pass

def _load(**kwargs):
    # csv => df => mysql 적제
    # 1. csv 경로 획득

    # 2. csv -> df

    # 3. mysql 연결 => MySqlHook 사용
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn       = mysql_hook.get_conn() # 커넥션 획득
    # 7. 전체를 try ~ except로 감싸기(I/O)
    try:
        # 4. 커서를 획득하여 
        with conn.cursor() as cursor:        
            # 4-1. insert 구문 사용
            #sql = ""
            #params = []
            #cursor.executemany( sql, params )
            # 4-2. 커밋
            #conn.commit()
            pass        
    except Exception as e:
        pass
    finally:
        # 5. 연결 종료
        if conn:
            conn.close()
    
    pass

# 3. DAG 정의
with DAG(
    dag_id      = "05_mysql_etl", 
    description = "etl 수행하여 mysql에 온도 센서 데이터 적제",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['mysql', 'etl'],
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
    
    task_extract    = PythonOperator(
        task_id = "extract",
        python_callable = _extract
    )
    task_trasform   = PythonOperator(
        task_id = "trasform",
        python_callable = _trasform
    )
    task_load       = PythonOperator(
        task_id = "load",
        python_callable = _load
    )

    # 5. 의존성 정의 -> 시나리오별 준비 
    task_create_table >> task_extract >> task_trasform >> task_load
    #task_extract >> task_trasform >> task_load