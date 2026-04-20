'''
- DAG 스케줄은 하루에 한번(00시00분00초) 돌게 지정 -> 테스트는 트리거 발동
- T1 : S3에 특정위치에 적제된 데이터를 기반으로 테이블을 구성
    - ~/csvs/ 하위 데이터를 기반으로 테이블 구성 -> s3_exam_csv_tbl
- T2 : 해당 테이블을 이용하여 분석결과를 담은 테이블 삭제(존재하면)
    - daily_report_tbl 삭제 쿼리 수행(존재하면)
- T3 : T1에서 만들어진 테이블을 기반으로 분석 결과를 도출하여 분석결과를 담은 테이블에 연결 -> 결과레포트용
    - 시험 결과를 기반으로 결과, 카운트, 평균, 최소, 최대 -> 그룹화 수행(기준 result)
    - 테이블명 => daily_report_tbl
        - format = 'PARQUET'
        - external_location = '내 s3 위치/athena/' => 쿼리 결과가 저장되는 곳   
    - output_location = '원하는 s3 위치로 지정' => 테이블의 메타 정보가 저장되는 곳
- 추가 구현해야될 사항 -> T3 데이터를 기반으로 대시보드 구성 -> 원하는 시간에 결과 파악
- 의존성 : T1 >> T2 >> T3
'''


# 1. 모듈가져오기
from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

# 2. 환경변수
BUCKET_NAME    = 'de-ai-06-827913617635-ap-northeast-2-an'
ATHENA_DB_NAME = 'de-ai-06-an2-glue-db'
SRC_TABLE      = 's3_exam_csv_tbl'
TARGET_TABLE   = 'daily_report_tbl'

# 메타 정보, 임시 정보 필요시 저장/삭제 공간으로 활용
S3_TARGET_LOC  = f's3://{BUCKET_NAME}/athena/tbl/{TARGET_TABLE}/'
S3_QUERY_LOG_LOC = f's3://{BUCKET_NAME}/athena/query_logs/'

# 3. DAG 정의
with DAG(
    dag_id      = "10_aws_athena_query_etl", 
    description = "athena query 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '0 0 * * *', # 스케줄 x -> 트리거 작동으로 실행
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3', 'athena', 'query'],
) as dag:
    t1 = AthenaOperator(
        task_id = 'create_src_table', 
        query = f'''
            CREATE EXTERNAL TABLE IF NOT EXISTS {SRC_TABLE} (
                id string,
                name string,
                score int,
                result string,
                created_at string
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            LOCATION 's3://{BUCKET_NAME}/csvs/'
            TBLPROPERTIES ("skip.header.line.count"="1")
        ''',
        database = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC,
        aws_conn_id = 'aws_default'
    )
    t2 = S3DeleteObjectsOperator(
        task_id = 'clean_s3_target', # 작업 ID
        bucket  = BUCKET_NAME,       # 버킷 이름
        prefix  = f'athena/tbl/{TARGET_TABLE}/', # 해당 위치가 대상
        aws_conn_id = 'aws_default'  # 접속 정보
    )
    # 임시로 사용한 테이블 삭제 -> 클린
    t3 = AthenaOperator(
        task_id = 'drop_table',
        query   = f'drop table if exists `{ATHENA_DB_NAME}`.{TARGET_TABLE}', 
        database        = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC, # 쿼리 수행 결과 로그 저장 위치
        aws_conn_id     = 'aws_default'  # 접속 정보
    )
    # csv -> 테이블 매핑 -> 쿼리수행 -> 결과를 저장 (필요시 포멧 변환)
    # 테스트 응시 결과가 90점 이상 학생만 추출 결과를 담는 테이블 => TARGET_TABLE   
    # PARQUET : 압축형태 지원, GZIP등 포멧 사용, 열기반 데이터 관리 
    # 90점 이상 학생들 데이터를 추출 => PARQUET 포멧변환 => GZIP 압축 => S3_TARGET_LOC 저장
    # 해당 소스를 TARGET_TABLE이 참조하여 => Athena를 통해 쿼리 수행 => 결과를 뽑아준다
    query = f'''
        CREATE TABLE {TARGET_TABLE}
        WITH (
            format = 'PARQUET', 
            parquet_compression = 'GZIP',
            external_location = '{S3_TARGET_LOC}'
        )
        AS
        SELECT 
            result,
            COUNT(*) as cnt,
            AVG(score) as avg_score,
            MIN(score) as min_score,
            MAX(score) as max_score
        FROM {SRC_TABLE} 
        GROUP BY result
    '''
    t4 = AthenaOperator(
        task_id = 'create_table_format_parquet',
        query   = query,
        database= ATHENA_DB_NAME, 
        output_location = S3_QUERY_LOG_LOC,
        aws_conn_id     = 'aws_default',
        do_xcom_push    = True , # 테이블 만들어 졌나? 센서 가동으로 조건으로 xcom 활용
        # xCOM 을 통해서 TARGET_TABLE이 생성되었는지 체크=> 확인 = t4내 기타 처리등 활용
 # 이 명령문을 사용하면 해당 task가 끝날때까지 
        # 다음 task를 진행시키지 않기 때문에 아래 task가 필요 x
    )
    # CTAS
    # 10분간 최대 대기, 10초 간격 감시 => create_table_format_parquet 테스크가 완료되었는지 점검
    # athena상에 테이블이 완성되었지 감시
    t5 = AthenaSensor(
        task_id = 'sensor',
        # 앞 테스크를 감시
        query_execution_id = "{{ task_instance.xcom_pull(task_ids='create_table_format_parquet') }}",
        poke_interval = 10, # 10초간격 감시
        timeout = 600,      # 최대 대기 시간, 10분
        aws_conn_id     = 'aws_default',
    )

    # 5. 의존성 구성
    t1 >> t2 >> t3 >> t4 