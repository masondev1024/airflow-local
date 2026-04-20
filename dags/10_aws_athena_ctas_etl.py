'''
    - 현재 상황은 database 존재(athena), s3 데이터만 있다 (가정)
    - airflow를 통해서 athena 기본 연동
    - task 1
        - 테이블 생성 -> Location 정보로 특정 csv가 존재하는 버킷을 지정
        - 해당 경로에 있는 데이터를 쿼리를 통해서 엑세스 가능함
    - task 2
        - 작업 진행될때 최신 상태것만 사용 -> 기존에 어떤 내용이 있다면 제거 처리(감안)
    - task 3
        - 획득한 데이터를 통해 -> 분석 -> 결과를 특정 s3에 저장, 저장된 내용은 압축
    - 의존성 
        - task 1 => task 2 => task 3
    - 스케줄 매일 1회 진행
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
SRC_TABLE      = 'athena_s3_data_tbl'
TARGET_TABLE   = 'pass_student'

# 메타 정보, 임시 정보 필요시 저장/삭제 공간으로 활용
S3_TARGET_LOC  = f's3://{BUCKET_NAME}/athena/tbl/{TARGET_TABLE}/'
S3_QUERY_LOG_LOC = f's3://{BUCKET_NAME}/athena/query_logs/'

# 3. DAG 정의
with DAG(
    dag_id      = "10_aws_athena_ctas_etl", 
    description = "athena ctas 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = None, # 스케줄 x -> 트리거 작동으로 실행
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3', 'athena', 'ctas'],
) as dag:
    # 4. task 정의
    # DAG 작동하면, s3내 특정위치에 저장된 내용, 테이블등을 삭제 처리 -> Clean
    # 매번 가동시 깨끗한 초기 상태 유지 전략 -> 먹통(틍/)성 유지 -> 기존 데이터와 꼬이는 문제 해결    
    
    # 임시로 사용한 s3 특정 공간 삭제 -> 클린
    t1 = S3DeleteObjectsOperator(
        task_id = 'clean_s3_target', # 작업 ID
        bucket  = BUCKET_NAME,       # 버킷 이름
        prefix  = f'athena/tbl/{TARGET_TABLE}/', # 해당 위치가 대상
        aws_conn_id = 'aws_default'  # 접속 정보
    )
    # 임시로 사용한 테이블 삭제 -> 클린
    t2 = AthenaOperator(
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

    # 향후 쿼리 업데이트해야 될 사항 => 당일 시험에 응시한 학생대상으로 90점 이상만 추출 (created_at 컬럼 활용 string 타입)
    query = f'''
        create table {TARGET_TABLE}
        with (
            format = 'PARQUET', 
            parquet_compression = 'GZIP',
            external_location = '{S3_TARGET_LOC}'
        )
        as
        select id,name,score,created_at
        from {SRC_TABLE} 
        where score >= 90 
        order by score desc
    '''
    t3 = AthenaOperator(
        task_id = 'create_table_format_parquet',
        query   = query,
        database= ATHENA_DB_NAME, 
        output_location = S3_QUERY_LOG_LOC,
        aws_conn_id     = 'aws_default',
        do_xcom_push    = True  # 테이블 만들어 졌나? 센서 가동으로 조건으로 xcom 활용
        # xCOM 을 통해서 TARGET_TABLE이 생성되었는지 체크=> 확인 = t4내 기타 처리등 활용
    )
    # CTAS
    # 10분간 최대 대기, 10초 간격 감시 => create_table_format_parquet 테스크가 완료되었는지 점검
    # athena상에 테이블이 완성되었지 감시
    t4 = AthenaSensor(
        task_id = 'sensor',
        # 앞 테스크를 감시
        query_execution_id = "{{ task_instance.xcom_pull(task_ids='create_table_format_parquet') }}",
        poke_interval = 10, # 10초간격 감시
        timeout = 600,      # 최대 대기 시간, 10분
        aws_conn_id     = 'aws_default',
    )

    # 5. 의존성 구성
    t1 >> t2 >> t3 >> t4
    pass