from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.operators.bash import BashOperator

# 환경변수 (본인 환경에 맞게 수정 필요)
BUCKET_NAME    = 'de-ai-06-827913617635-ap-northeast-2-an'
ATHENA_DB_NAME = 'de-ai-06-an2-glue-db'
S3_QUERY_LOG_LOC = f's3://{BUCKET_NAME}/athena/query_logs/'
TARGET_TABLE   = 'async_report_tbl'

with DAG(
    dag_id      = "11_aws_athena_async_sensor", 
    description = "Athena 비동기 실행 및 Sensor 자원 최적화 실습",
    default_args= {
        'owner'           : 'de_2team_manager',        
        'retries'         : 1,
        'retry_delay'     : timedelta(minutes=1)
    },
    schedule_interval = None, 
    start_date  = datetime(2026, 2, 25),     
    catchup     = False,
    tags        = ['aws', 'athena', 'sensor', 'async'],
) as dag:

    # T1: 쿼리 실행만 하고 대기하지 않음 (비동기 트리거)
    # wait_for_completion=False 가 핵심입니다.
    t1_trigger_query = AthenaOperator(
        task_id = 'trigger_heavy_query',
        query   = f'''
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
            WITH (
                format = 'PARQUET', 
                parquet_compression = 'GZIP',
                external_location = 's3://{BUCKET_NAME}/athena/tbl/{TARGET_TABLE}/'
            )
            AS
            SELECT result, COUNT(*) as cnt
            FROM s3_exam_csv_tbl 
            GROUP BY result
        ''',
        database = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC,
        aws_conn_id = 'aws_default'
    )

    # T2: 쿼리가 도는 동안 수행되는 다른 병렬 작업 시뮬레이션
    t2_do_something_else = BashOperator(
        task_id = 'do_other_work',
        bash_command = 'echo "쿼리가 Athena 엔진에서 돌아가는 동안 Airflow는 워커를 비우고 다른 작업을 할 수 있습니다."; sleep 10'
    )

    # T3: Sensor를 이용한 쿼리 상태 감시 (Reschedule 모드)
    # T1의 XCom 데이터에서 query_execution_id를 가져옵니다.
    t3_wait_for_query = AthenaSensor(
        task_id = 'wait_for_athena_query',
        query_execution_id = "{{ task_instance.xcom_pull(task_ids='trigger_heavy_query') }}",
        max_retries = 10,
        poke_interval = 20,     # 20초마다 상태 확인
        mode = 'reschedule',    # 확인 후 안 끝났으면 워커 슬롯을 반납 (매우 중요)
        aws_conn_id = 'aws_default',
    )

    # T4: 쿼리 완료 후 후속 작업
    t4_finalize = BashOperator(
        task_id = 'finalize_process',
        bash_command = 'echo "Athena 쿼리가 성공적으로 완료됨을 Sensor가 확인했습니다. 다음 파이프라인 진행!"'
    )

    # 의존성 구성 (비동기 분기 후 Sensor에서 합류)
    t1_trigger_query >> [t2_do_something_else, t3_wait_for_query]
    
    [t2_do_something_else, t3_wait_for_query] >> t4_finalize