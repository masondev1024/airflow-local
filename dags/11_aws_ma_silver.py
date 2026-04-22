'''
- ma 에서 silver 단계로 넘어가는 과정에서, 데이터가 kinesis로 잘 들어오는지 확인
- schedule ( 10 0 * * * )
    - fire hose에서 버퍼 시간을 최대 3분으로 구성 -> 3분 이후부터는 스케줄 가동 가능
    - 보수적으로 10시 5분으로 스케줄 구성 -> 10시 5분에 데이터가 들어오는지 확인
- 처리할 데이터(flatten, 파생변수, 컬럼명변경) 형태로 변환 -> json 직렬화 -> kinesis로 전송
    - event_id: 이벤트 관리 번호(중복 x)- engine에서 고유하게 식별할 수 있는 번호
    - event_time: 현재시간(이벤트 발생시간)
    - source_ip: 클라이언트의 접속 IP(가짜)
    - user_agent: 클라이언트의 접속 브라우저 타입(가짜)
    - data: 상세 데이터 => dict 내에 dict 구성, 객체 직렬화추가
        - user_id: 사용자 id (사용자별로 여러번구매)
        - item_id: 구매 제품
        - price: 단가
        - qty: 수량     
        - (price * qty) => total_price: 총 구매 금액
        - store_id: 매장번호(오프/온라인 포함)
    - source_ip
    - user_agent
    - dt( year, month, day)
    - hour as hr
- 작업 (silver 테이블 삭제 -> ctas로 테이블 생성) -> sensor로 쿼리 상태 감시 -> 완료 후 후속 작업
'''

# 1. 모듈 가져오기
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# 2. 환경 변수
DATABASE_BRONZE = 'de_ai_06_ma_bronze_db'
DATABASE_SILVER = 'de_ai_06_ma_silver_db'
BUCKET_NAME = 'de-ai-06-827913617635-ap-northeast-2-an'
SILVER_S3_PATH = 's3://de-ai-06-827913617635-ap-northeast-2-an/medallion/'
ATHENA_RESULTS = f's3://{BUCKET_NAME}/athena-results/'
SILVER_TBL_NAME = 'sales_silver_tbl'
# 3. DAG 정의
with DAG(
    dag_id      = "11_medallion_bronze_to_silver_ctas", 
    description = "athena ctas 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '18 * * * *', # 매시간 10분에 실행
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 'medallion', 'athena', 'silver', 'ctas'],
) as dag:
    # 4. TASK 정의 -> athena에 접속해서 필요한 sql을 실행하여 업무를 수행(본질 목표)
    drop_silver_task = AthenaOperator(
        task_id = "drop_silver_tbl",
        query   = f'''
            drop table if exists {DATABASE_SILVER}.{SILVER_TBL_NAME};
        ''',
        database = DATABASE_SILVER,
        output_location = ATHENA_RESULTS,
        aws_conn_id = 'aws_default'
        # params = {'database_silver' : DATABASE_SILVER} 
        # -> query문에서 jinjja 템플릿 {{database_silver}}로 사용 가능하게 만듬
        )
    # jinjja 템플릿 -> query문에서 변수처럼 사용 가능하게 만들어주는 문법
    # -> query문에서 {{params.변수명}} 형태로 사용 가능
    ctas_silver_task = AthenaOperator(
        task_id = 'ctas_silver',
        query   = '''
            Create Table if not exists {{ params.database_silver }}.{{ params.tbl_nm }}
            with (
                format              = 'PARQUET',
                parquet_compression = 'SNAPPY',
                external_location   = '{{ params.silver_path }}',
                partitioned_by      = ARRAY['dt','hr']
            ) As 
            Select 
                event_id,
                event_time as event_timestamp,
                data.user_id,
                data.item_id,
                data.price,
                data.qty,
                (data.price * data.qty) as total_price ,
                data.store_id,
                source_ip,
                user_agent,
                cast(year || '-' || month || '-' || day as VARCHAR) as dt,
                hour as hr
            from {{ params.database_bronze }}.raw_bronze_tbl
            where   year = '{{ execution_date.format('YYYY') }}'
                and month= '{{ execution_date.format('MM') }}'
                and day  = '{{ execution_date.format('DD') }}'
                and hour = '10'
            ;

        ''',
        database= DATABASE_SILVER,
        params  = {
            'database_bronze':DATABASE_BRONZE, 
            'database_silver':DATABASE_SILVER, 
            'tbl_nm':SILVER_TBL_NAME,
            'silver_path':SILVER_S3_PATH
        },
        output_location = ATHENA_RESULTS 
    )


    # 의존성 구성 (비동기 분기 후 Sensor에서 합류)
    drop_silver_task >> ctas_silver_task