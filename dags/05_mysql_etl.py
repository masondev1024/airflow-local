from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging 

# 추가분
from airflow.providers.mysql.operators.mysql import MySqlOperator
# Load 처리시 sql에 전처리된 데이터를 밀어 넣을때 사용
from airflow.providers.mysql.hooks.mysql import MySqlHook
# 훅을 날려서 DB안에 테이블들로 데이터를 밀어넣는다고 생각해봐?

# 데이터
import json
import random
import pandas as pd # 소량의 데이터(데이터 규모)
import os

# 2. 환경변수
# 프로젝트 내부 폴더에 데이터용으로 (~/dags/data) 지정
# task 진행간 생성되는 파일을 동기화하도록 위치 지정 -> 향후 s3(데이터 레이크)로 대체 될 수 있음
DATA_PATH = '/opt/airflow/dags/data' # 도커 내부에 생성된 컨테이너 상 워커의 airflow에서 지정된 데이터 위치
os.makedirs(DATA_PATH, exist_ok=True)

def _extract(**kwargs):
    # 스마트팩토리에 설치된 오븐 온도 센서에서 데이터가 발생되면 데이터레이크(s3)에 쌓이고 있다
    # => 추출해서 가져오는 단계로 가정

    # 더미 데이터 고려 구성 -> 1회성으로 10건 구성 -> [ {}, {}, ...]
    data = [
        {
            "sensor_id" : f"SENSOR_{i+1}", # 장비 ID
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature" : round(random.uniform(20.0, 150.0), 2),
            "status"    : "on", 

        } for i in range(10) ]
    # 더미 데이터를 파일로 저장 (로그파일처럼) -> json 형태
    # /opt/airflow/dags/data/sensor_data_DAG수행날짜.json
    # 실습 -> 데이터를 위의 형식으로 저장하시오 ( json.dump(data,f))
    file_name = f"sensor_data_{ds}.json"
    file_path = os.path.join(DATA_PATH, file_name) # f'{DATA_PATH}/sensor_data_{kwargs['ds_nodash']}.json

    # 실습에서 배운 JSON 파일 저장
    # with open(file_path,'w') as f:
    #     json.dump(data,f)

    # 로그는 별도의 프로그램에서 지속적으로 발생시켜야 함(시뮬레이션 기준)
    # 현재는 편의상 airflow에 포함시킴
    # XCom을 통해서 task_transform에게 전달 (로그의 경로를 전달)
    # logging.info(f'extract 한 로그 데이터 {file_path}')
    # return file_path

    # 4. JSON 파일로 저장
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4) # indent를 주면 가독성이 좋아집니다.
        
        logging.info(f"데이터 추출 및 저장 완료: {file_path}")
        
        # 5. 다음 Task(transform)에서 파일을 찾을 수 있도록 경로를 XCom으로 전달
        kwargs['ti'].xcom_push(key='extracted_file_path', value=file_path)
        
    except Exception as e:
        logging.error(f"파일 저장 중 에러 발생: {e}")
        raise e
    pass

def _transform(**kwargs):
    # _extract에서 추출한 데이터를 XCom을 통해서 획득
    # 이 데이터를 df로 load -> 섭씨를 화씨로 일괄 처리(1번에 n개의 센서에서 데이터 전달)
    # 전처리된 내용은 csv로 덤프 (s3로 업로드 고려)

    ti = kwargs['ti']
    # 1. extract 단계에서 XCom에 저장한 파일 경로를 획득
    source_file = ti.xcom_pull(key='extracted_file_path', task_ids='extract')
    
    if not source_file or not os.path.exists(source_file):
        raise FileNotFoundError(f"추출된 파일을 찾을 수 없습니다: {source_file}")

    # 2. JSON 파일 로드 및 Pandas DataFrame 변환
    with open(source_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    
    df = pd.DataFrame(raw_data)
    logging.info(f"데이터 로드 성공. 행 수: {len(df)}")

    # 3. 데이터 가공 (섭씨 -> 화씨 변환)
    # 수식: (Celsius * 1.8) + 32
    df['temperature_f'] = (df['temperature'] * 1.8 + 32).round(2)
    df.rename(columns={'temperature': 'temperature_c'}, inplace=True)
    
    # 4. 가공된 데이터를 CSV로 저장 (중간 아티팩트 보존)
    ds = kwargs.get('ds')
    target_file = f"{DATA_PATH}/transformed_data_{ds}.csv"
    df.to_csv(target_file, index=False, encoding='utf-8-sig')
    
    # 5. 다음 단계를 위해 가공된 파일 경로를 XCom으로 전달
    ti.xcom_push(key='transformed_file_path', value=target_file)
    logging.info(f"데이터 변환 및 저장 완료: {target_file}")


def _load(**kwargs):
    ti = kwargs['ti']
    # 1. transform 단계에서 생성된 CSV 파일 경로 획득
    csv_file = ti.xcom_pull(key='transformed_file_path', task_ids='transform')
    
    if not csv_file or not os.path.exists(csv_file):
        raise FileNotFoundError(f"변환된 파일을 찾을 수 없습니다: {csv_file}")

    # 2. CSV 파일을 읽어서 적재 준비
    df = pd.read_csv(csv_file)
    
    # MySQL Hook 호출 (Connection ID: mysql_default)
    # 튜플 리스트 형태로 변환 [(sensor_id, timestamp, temp_c, temp_f), ...]
    # DB 스키마 순서와 정확히 일치해야 함
    load_data = [tuple(x) for x in df[['sensor_id', 'timestamp', 'temperature_c', 'temperature_f']].values]

    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    # 3. 데이터 적재 (Idempotency 보장: 중복 방지를 위한 사전 삭제 로직 권장)
    # mysql_hook.run(f"DELETE FROM sensor_readings WHERE DATE(timestamp) = '{kwargs.get('ds')}'")
    
    mysql_hook.insert_rows(
        table='sensor_readings',
        rows=load_data,
        target_fields=['sensor_id', 'timestamp', 'temperature_c', 'temperature_f']
    )
    
    logging.info(f"최종 MySQL 적재 완료. 적재 건수: {len(load_data)}")
# 3. DAG 정의
# ... (상단 import 부분 동일)

with DAG(
    dag_id = "05_mysql_etl",
    description="1개의 DAG에서 etl 수행",
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

    # 수정: MySqlOperator 클래스명 일치 및 괄호 닫기
    # task_create_table = MySqlOperator(
    #     task_id="create_table",
    #     mysql_conn_id="mysql_default", 
    #     sql='''
    #         CREATE TABLE IF NOT EXISTS sensor_readings (
    #             id INT AUTO_INCREMENT PRIMARY KEY,
    #             sensor_id VARCHAR(50),
    #             timestamp DATETIME,
    #             temperature_c FLOAT,
    #             temperature_f FLOAT,
    #             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #         );
    #     '''
    # )

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

    # 의존성 정의
    task_extract >> task_transform >> task_load