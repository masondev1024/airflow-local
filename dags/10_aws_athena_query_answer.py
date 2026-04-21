'''
- DAG 스케줄은 하루에 한번(00시00분00초) 지정하뒤 -> 테스트는 트리거 작동
- T1 : S3에 특정위치에 적제된 데이터를 기반으로 테이블 구성
    - ~/csvs/ 하위 데이터를 기반으로 테이블 구성(존재하지 않으면) -> s3_exam_csv_tbl
- T2 : 해당 테이블을 이용하여 분석결과를 담은 테이블 삭제(존재하면)
    - daily_report_tbl 삭제 쿼리 수행(존재하면)
- T3 : T1에서 만들어진 테이블을 기반으로 분석 결과를 도출하여 분석결과를 담은 테이블에 연결 -> 결과레포트용 데이터 구성
    - 시험 결과를 기반으로, 결과, 카운트, 평균, 최소, 최대 -> 그룹화 수행(기준 result) -> 분석에 필요한 데이터
    - 테이블명 => daily_report_tbl
        - format = 'PARQUET'
        - external_location = '원하는 s3 위치로 지정' -> 쿼리 결과가 저장되는 곳
    - output_location = '원하는 s3 위치로 지정', -> 테이블의 메타 정보가 저장되는 곳
- 미구현 -> T3 데이터를 기반으로 대시보드 구성 -> 원하는 시간에 결과 파악
- 의종성 : T1 >> T2 >> T3
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor

# 2. 환경변수
BUCKET_NAME     = 'de-ai-06-827913617635-ap-northeast-2-an'
DATABASE_NAME   = 'de-ai-06-an2-glue-db'
QUERY_RESULT_S3 = f's3://{BUCKET_NAME}/athena-results/'

# 3. DAG 정의 
with DAG(
    dag_id      = "10_aws_athena_query_answer", 
    description = "athena query 연습",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3', 'athena', 'sql'],
) as dag:
    # 목표 raw data(csv, 향후 parquet,..) => 가공 => report data 변환(기존 데이터 유지 x)
    # 4. TASK 정의 -> athena에 접속해서 필요한 sql을 실행하여 업무를 수행(본질 목표)
    t1 = AthenaOperator(
        task_id = "raw_data_tbl_create",
        query   = f'''
            create external table if not exists s3_exam_csv_tbl (
                id int,
                name string,
                score int,
                created_at string,
                result string
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION 's3://de-ai-06-827913617635-ap-northeast-2-an/csvs/'
            TBLPROPERTIES ("skip.header.line.count"="1");
        ''',
        database        = DATABASE_NAME,
        output_location = QUERY_RESULT_S3,
        aws_conn_id     = 'aws_default'
    )
    # 매 스케줄마다 그 시점의 최신데이터로 유지하기 위해서 테이블 삭제
    # 멱등성 -> 매번 주기적으로 반복되는 연산의 결과가 항상 동일하도록 (값의 동일x,절차,형태등등 동일성)
    t2 = AthenaOperator(
        task_id="report_tbl_drop",
        query   = f'''
            drop table if exists daily_report_tbl;
        ''',
        database=DATABASE_NAME,
        output_location = QUERY_RESULT_S3,
        aws_conn_id     = 'aws_default'
    )
    # ctas
    t3 = AthenaOperator(
        task_id="report_tbl_create_with_raw_data_tbl",
        query   = f'''
            create table daily_report_tbl
            with (
                format='PARQUET',
                external_location = 's3://{BUCKET_NAME}/report_data/'
            ) as
            select 
                result,                
                count(*) as count,
                avg(score) as avg_score,
                min(score) as min_score,
                max(score) as max_score
            from s3_exam_csv_tbl
            group by result            
        ''',
        database=DATABASE_NAME,
        output_location = QUERY_RESULT_S3,
        aws_conn_id     = 'aws_default'
    )
    # 5. 의존성
    t1 >> t2  >> t3
    pass