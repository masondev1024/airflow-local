'''
- ma 
'''

# 1. 모듈 가져오기
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# 2. 환경 변수
DATABASE_GOLD = 'de_ai_06_ma_gold_db'
DATABASE_SILVER = 'de_ai_06_ma_silver_db'
BUCKET_NAME = 'de-ai-06-827913617635-ap-northeast-2-an'

# 데이터 저장용 -> 실제 데이터
SILVER_S3_PATH = f's3://{BUCKET_NAME}/medallion/'
# 쿼리 히스토리등 저장용 -> 메타
ATHENA_RESULTS = f's3://{BUCKET_NAME}/athena-results/'
SILVER_TBL_NAME = 'sales_silver_tbl' # 'sales_silver_increment_tbl'
GOLD_VIEW_NAME = 'daily_sales_summary_view'
# 3. DAG 정의
with DAG(
        dag_id      = "12_medallion_silver_to_gold_view", 
    description = "athena gold view 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    # 모든 매장은 21시에 마감
    schedule_interval = '@daily', 
    start_date  = datetime(2025,2,25),          
    catchup     = False,
    tags        = ['aws', 'medallion', 'athena', 'gold', 'view'],
) as dag:
    # 4. TASK 정의 -> athena에 접속해서 필요한 sql을 실행하여 업무를 수행(본질 목표)
    # 조건 : 어제 데이터를 오늘 수행 -> 조건의 날짜 1일전 과거 조회 -> dt연산 필요
        create_gold_view = AthenaOperator(
        task_id ='create_or_replace_gold_view',
        query='''
            CREATE OR REPLACE VIEW {{ params.database_gold }}.{{ params.view_nm }} AS
            SELECT
                item_id,
                SUM(qty) AS total_qty,
                SUM(total_price) AS total_revenue,
                COUNT(DISTINCT user_id) AS unique_customer,
                dt AS sales_date
            FROM {{ params.database_silver }}.{{ params.table_nm }}
            WHERE dt = '{{ (execution_date - macros.timedelta(days=1)).format('2026-04-22') }}'
            -- macros.timedelta(days=1) -> execution_date에서 1일을 빼는 연산 -> 어제 날짜 조회
            GROUP BY dt, item_id
            ;
            ''',
        params ={
            'database_gold': DATABASE_GOLD,
            'database_silver': DATABASE_SILVER,
            'view_nm': GOLD_VIEW_NAME,
            'table_nm': SILVER_TBL_NAME
        },
        database=DATABASE_GOLD,
        output_location=ATHENA_RESULTS,
        aws_conn_id = 'aws_default'
    )