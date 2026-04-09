'''
- PythonOperator 사용
- task 간 통신 => XCom 사용 => task간 상호대화할 수 있는 내부 공간( Airflow의 메타데이터 DB안에 있는 XCom table)
- 통신간 사용할 데이터의 크기는 저장공간(혹은 메모리공간) 고려하여 가급적 raw 데이터가 아닌
- raw 데이터나 상황을 접근, 판단할 수 있는 메타 정보 정도(또는 s3에 저장되어 있는 데이터의 주소)
'''
# 1. 모듈 가져오기
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging # level별로 로그 출력(에러, 경고, 디버깅, 정보..)

# 3-1. 콜백함수 정의
def _extract_cb(**kwargs):
    '''
        - ETL의 Extract 담당 task의 콜백함수(실질적 작업)
        - parameters
            - kwargs : airflow가 작업 실행하기 전에 정보(airflow의 내부에 구성되어 있는 Context(딕셔너리))를 접근할수 있는 내용
    '''
    # 1. airflow가 주입한 airflow context 정보에서 필요한 정보 추출    
    ti = kwargs['ti']  # TaskInstance 객체
    
    data = [1, 2, 3, 4, 5]  # 예시 데이터 (추출 결과)
    logging.info('== Extract 작업 start ==')
    logging.info(f"Extracted data: {data}")
    logging.info('== Extract 작업 end ==')    
    
    # XCom에 데이터 저장 (push)(게시판에 글 등록됨)

    ti.xcom_push(key='extract_data', value=data) # XCom(DB)에 실제 데이터를 저장하고 있기에 메모리/DB 직접 사용 방식

    data = [1,2,3,4,5]

    # # S3 저장 ( 문자열인 주소값만 value로 사용해서 전달하면 가벼움)
    # s3_path = "s3://my-bucket/extract/2026-04-09/data.json"

    # ti.xcom_push(key="extract_data", value=s3_path)

def _transform_cb(**kwargs):
    '''
    ETL의 transform 담당
    kwargs를 이용하여 airflow context 정보 획득 -> ti
        - 다른 task에서 전될된 데이터 획득 -> t1.xcom_pull() 처리
    '''
    ti = kwargs['ti']
    
    # XCom에서 데이터 가져오기 (pull)
    data = ti.xcom_pull(
        key='extract_data',
        task_ids='extract_task_data' # 특정 task가 기록한 데이터를 획득
    )
    
    logging.info(f"Pulled data: {data}")
    
    # 간단한 변환 작업
    transformed_data = [x * 10 for x in data]
    # 3. Push: 가공된 데이터를 'transformed_result'라는 키로 게시판에 게시 (이게 빠졌습니다!)
    ti.xcom_push(key='transformed_result', value=transformed_data)
    logging.info(f"Transformed data pushed with key 'transformed_result': {transformed_data}")

def _load_cb(**kwargs):
    ti = kwargs['ti']
    # 1. Transform 단계에서 게시판(XCom)에 올린 가공 데이터를 가져옴
    final_data = ti.xcom_pull(key='transformed_result', task_ids='transform_task_data')
    
    # 2. 실제 적재 (예: MySQL에 Insert 하거나 S3에 파일로 저장)
    # mysql_hook.run(f"INSERT INTO table VALUES ({final_data})") 
    logging.info(f"Final Load to Destination: {final_data}")

# 2. DAG 정의
with DAG(
    dag_id = "02_basic_python",
    description="파이썬 task 구성, 통신(XCom)",
    default_args = {
        'owner'             : 'de_2team_manager' , 
        'retries'           : 1 ,                  
        'retry_delay'      : timedelta(minutes=1)
    },      
    schedule_interval = '@once',  # 수동으로 딱 한번 수행, 주기x  
    start_date = datetime(2026,2,25), 
    catchup     = False,           
    tags        = ['python','xcom','context'],
) as dag:
    # 3. Task 정의 (PythonOperator, XCom 사용)
    #    ETL을 고려하여 task 정의(간단)
    extract_task = PythonOperator(
        task_id = "extract_task_data",
        # 함수 단위(많은 작업을 하나의 단위로 구성)로 작업 구성 => 콜백함수 형태
        python_callable = _extract_cb
    )
    transform_task = PythonOperator(
        task_id= "transform_task_data",
        python_callable = _transform_cb#
    )
    load_task = PythonOperator(
        task_id = "load_task_data",
        python_callable = _load_cb
    )

    # 4. 의존성 정의
    extract_task >> transform_task >> load_task
    pass