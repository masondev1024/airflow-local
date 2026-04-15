'''
- 평시 -> 잠복하듯이 센서를 켜고 대기중
- 특정 버킷 혹은 버킷내 공간을 감시(sensor) -> 파일(객체등) 업로드 -> 감지 -> DAG 작동
- 렌터카 반납 => 개인 촬영 => S3 => 트리거 작동 => 데이터 추출 전처리 => AI 모델 전달 => 추론 => 
    - 사용자가 언제 이런 행위를 할지 아무도 모름 -> 의외성 -> Socar 모델 확인 가능 -> 인력 비용 감축 가능
'''
# 1. 모듈가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # s3 키등 읽는 용도
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # 감시용 센서
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator # 특정데이터(객체) 삭제
import logging

# 2. 환경변수 설정
BUCKET_NAME = "de-ai-06-827913617635-ap-northeast-2-an"
FILE_NAME = 'sensor_data.csv'
S3_KEY      = f'income/{FILE_NAME}' # 감시 대상

# 5. 콜백함수 정의
def _reading_data(**kwargs):
    hook = S3Hook(aws_conn_id='aws_default') # S3 사용하여 연결
    data =hook.read_key(key=S3_KEY, bucket_name =BUCKET_NAME)
    logging.info('--- 로그 출력 시작 ---')
    logging.info(data)
    logging.info('--- 로그 출력 완료 ---')
    pass

# 3. DAG 정의
with DAG(
    dag_id="09_aws_s3_consummer",
    description="s3 특정 버킷(사용자별로 섹션 할당-이름구분등등 비즈니스 처리필요)에 대해 데이터 변화를 감지해 읽은 다음 비지니스 처리 후 삭제(특정 위치에 보관(raw 데이터 구축))",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily', # Scheduling x -> Trigger Execute (센서 작동에 스케쥴링이 필요한지 테스트)
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3'],
) as dag:
    # 4. Task 정의
    # 4-1. 감시자(센서,옵저버)
    task_waiting_trigger =S3KeySensor(
        task_id = "waitting_trigger",
        bucket_key= S3_KEY, # 버킷 내 타겟
        bucket_name= BUCKET_NAME,
        aws_conn_id= 'aws_default',
        mode = 'reschedule', # 대기중에 자원 반납 (비용 감축)
        poke_interval = 10, # 10초 간격으로 체크(주기에 따라 자원 사용 차이 발생) (비용감축)
        timeout = 60*10 # service 가동후 10분넘게 감지가 안되면 종료
    )
    # 4-2. 비즈니스 처리
    task_reading_data = PythonOperator(
        task_id = "reading_data",
        python_callable= _reading_data

    )
    # 4-3. 파일삭제 or 키삭제/ 필요시 보관 -> 뒷처리
    task_delete_data_or_backup = S3DeleteObjectsOperator(
        task_id = "delete_data_or_backup",
        bucket  = BUCKET_NAME,
        keys    = [S3_KEY], # 삭제 대상 n개 지정 
        aws_conn_id='aws_default'
    )


    # 5. 의존성 정의
    task_waiting_trigger >> task_reading_data >> task_delete_data_or_backup
