from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import logging

# 1. 전역 설정 (Connection ID는 Airflow UI에서 생성한 것과 일치해야 함)
MYSQL_CONN_ID = 'mysql_default'

def _transform(**kwargs):
    ti = kwargs['ti']
    # SQLExecuteQueryOperator의 결과는 기본적으로 XCom에 저장됨
    raw_data = ti.xcom_pull(task_ids='extract_from_mysql')
    
    if not raw_data:
        raise ValueError("No data found in extract step")

    logging.info(f"Raw data extracted: {raw_data}")
    
    # 예시: 첫 번째 컬럼 값에 10을 곱하는 변환 작업 (데이터 구조에 따라 수정 필요)
    # raw_data는 보통 리스트 안의 튜플 형태 [(val1, val2), (val1, val2)]
    transformed_data = [(row[0] * 10, row[1]) for row in raw_data]
    
    ti.xcom_push(key='transformed_result', value=transformed_data)

def _load(**kwargs):
    ti = kwargs['ti']
    final_data = ti.xcom_pull(key='transformed_result', task_ids='transform_data')
    
    # Hook을 사용하여 DB 연산 수행
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    # 데이터 적재 전 대상 테이블 비우기 (Idempotency 보장)
    mysql_hook.run("DELETE FROM target_table WHERE 1=1")
    
    # Bulk Insert 수행
    mysql_hook.insert_rows(
        table='target_table', 
        rows=final_data,
        target_fields=['value_column', 'name_column'] # 컬럼명 명시
    )
    logging.info(f"Successfully loaded {len(final_data)} rows.")

with DAG(
    dag_id="05_mysql_etl_mixed",
    default_args={
        'owner': 'de_2team_manager',
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 25),
    catchup=False,
    tags=['etl', 'mysql', 'hybrid'],
) as dag:

    # [Extract] 전용 Operator 사용 (SQL 기반)
    t1 = SQLExecuteQueryOperator(
        task_id="extract_from_mysql",
        conn_id=MYSQL_CONN_ID,
        sql="SELECT id_value, name_text FROM source_table", # 실제 테이블명으로 수정
        do_xcom_push=True # 결과를 XCom에 저장
    )

    # [Transform] Python의 유연함 활용
    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=_transform
    )

    # [Load] Hook을 활용한 고성능 적재
    t3 = PythonOperator(
        task_id="load_to_mysql",
        python_callable=_load
    )

    t1 >> t2 >> t3