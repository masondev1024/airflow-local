'''
- ma 에서 silver 단계로 넘어가는 과정에서, 데이터가 kinesis로 잘 들어오는지 확인
- silver level에서 데이터를 계속 증분한다

'''

# 1. 모듈 가져오기
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# 2. 환경 변수
DATABASE_BRONZE = 'de_ai_06_ma_bronze_db'
DATABASE_SILVER = 'de_ai_06_ma_silver_db'
BUCKET_NAME = 'de-ai-06-827913617635-ap-northeast-2-an'
SILVER_S3_PATH = 's3://de-ai-06-827913617635-ap-northeast-2-an/medallion/'
ATHENA_RESULTS = f's3://{BUCKET_NAME}/athena-results/'
SILVER_TBL_NAME = 'sales_silver_tbl_increment_tbl'
# 3. DAG 정의
with DAG(
    dag_id      = "11_medallion_bronze_to_silver_increment", 
    description = "athena increment 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '18 * * * *', # 매시간 10분에 실행
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 'medallion', 'athena', 'silver', 'increment'],
) as dag:
    # 4. TASK 정의 -> athena에 접속해서 필요한 sql을 실행하여 업무를 수행(본질 목표)
    create_silver_table = AthenaOperator(
        task_id='create_silver_table_if_not_exists',
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_silver }}.{{ params.tbl_nm }} (
                event_id string,
                event_timestamp timestamp,
                user_id string,
                item_id string,
                price int,
                qty int,
                total_price int,
                store_id string,
                source_ip string,
                user_agent string
        )
            PARTITIONED BY (dt string, hr string)
            STORED AS PARQUET
            LOCATION '{{ params.silver_path }}'
            TBLPROPERTIES ('parquet.compress'='SNAPPY');
    """,
        params={
            'database_silver': DATABASE_SILVER,
            'silver_path': SILVER_S3_PATH,
            'tbl_nm': SILVER_TBL_NAME
        },
        database=DATABASE_SILVER,
        output_location=ATHENA_RESULTS
    )

# [TASK 2] 특정 시간대의 데이터를 추출하여 Silver 테이블에 삽입 (Incremental Load)
# execution_date를 활용해 딱 해당 시간의 데이터만 골라냄
    insert_silver_data = AthenaOperator(
        task_id='insert_bronze_to_silver',
        query="""
            INSERT INTO {{ params.database_silver }}.{{ params.tbl_nm }}
            SELECT
                event_id,
                event_time as event_timestamp,
                data.user_id,
                data.item_id,
                data.price,
                data.qty,
                (data.price * data.qty) as total_price,
                data.store_id,
                source_ip,
                user_agent,
                -- 파티션 컬럼
                CAST(year || '-' || month || '-' || day AS VARCHAR) as dt,
                hour as hr
            FROM {{ params.database_bronze }}.raw_bronze_tbl
            WHERE year = '{{ execution_date.format("YYYY") }}'
            AND month = '{{ execution_date.format("MM") }}'
            AND day = '{{ execution_date.format("DD") }}'
            AND hour = '{{ execution_date.format("HH") }}';
        """,
        params={
            'database_bronze': DATABASE_BRONZE,
            'database_silver': DATABASE_SILVER,
            'tbl_nm': SILVER_TBL_NAME,
            'silver_path': SILVER_S3_PATH

        },
        database=DATABASE_SILVER,
        output_location=ATHENA_RESULTS
    )

# 태스크 순서 설정
create_silver_table >> insert_silver_data