'''
- API 호출 과정 적용. 데이터 처리에 대한 스케줄 구성
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
import logging
import json
import requests # api 호출용, MSA 서비스 호출용
import pandas as pd

# 2. API 서버 주소
#  API_URL = 'http://127.0.0.1:8000/predict' # 현 코드가 작동중인 컨테이너 의미
API_URL = 'http://ai-api-server:8000/predict' # AI 서비스를 

# 4-4. 콜백함수 정의
def _create_dummy_data(**kwargs):
    # 차후 버전은 db 테이블에서 조회 -> 데이터 구성
    # 현재 버전은 더미 데이터를 임시 구성 xcom 전달하여 nex task에서 사용
    users= [
        {"user_id": "C001", "income" : 5000, "loan_amt": 4000},
        {"user_id": "C002", "income" : 40000, "loan_amt": 1000},
        {"user_id": "C003", "income" : 3000, "loan_amt": 5000}
    ]
    # xcom으로 전달
    return users

def _api_service_call(**kwargs):
    # xcom에 게시될 때는 키값이 카멜표기법으로 조정, 추출할때는 다시 스네이크 표기법으로 복원됨
    # 1. 이전 task의 결과물 획득 (차후 -> 데이터레이크(s3), athena, redshift, opensearch(elasticsearch의 아마존 버전) 등 서비스통해서 획득)
    ti = kwargs['ti']
    users_data = ti.xcom_pull(task_ids = 'task_create_dummy_data')
    if not users_data:
        raise ValueError(f"추출된 고객 데이터가 없습니다.")

    # 2. 신용 평가 요청 및 응답 -> api 호출 (차후 LLM모델과 연계 가능) -> 통신 -> I/O -> 예외처리
    try:
        # 3. post 방식 요청, dict 형태 데이터 첨부, json 형태로 전달 (내부적으로는 객체 직렬화 처리됨)
        res =requests.post(API_URL, json=users_data)
        # 실제 서비스에서는 보안 이슈로 인증 정보, 각종 키등을 헤더에 세팅해야 함
        # 4. 요청이 성공하면 다음으로 진행 -> 200 응답코드 => skip
        # if res.raise_fir_status() ==
        # 5. 결과 획득 (객체의 역직렬화: json -> dict or list[dict,...])
        results = res.json()
        # 6. 결과 출력
        logging.info(f'신용 평가 결과 획득 {results}')
        return results

    except Exception as e:
        logging.info(f'API 호출 실패{e}')
        raise
    pass
def _load_users_credit(**kwargs):
    # 1. ti(Task Instance) 객체 획득
    ti = kwargs['ti']
    # 2. XCom을 통해 API 호출 결과 획득
    # PythonOperator의 return 값은 기본적으로 'return_value'라는 키로 저장됩니다.
    uesrs_grade = ti.xcom_pull(task_ids='task_api_service_call', key='return_value')
    if not uesrs_grade:
        logging.error('신용 평가 결과 없음')
        raise ValueError('신용 평가 결과 없음')
    df = pd.DataFrame(uesrs_grade)
    # 3. 테이블이 없으면 생성(임시편성) -> 추후 사전 작업으로 이동
    '''
        CREATE TABLE IF NOT EXISTS customers (
            user_id VARCHAR(50) PRIMARY KEY,
            income INT DEFAULT NULL,
            loan_amt INT DEFAULT NULL,
            credit_score INT DEFAULT NULL,
            grade VARCHAR(10) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''' 
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default', key='return_value')
    
    # conn       = mysql_hook.get_conn()
    with mysql_hook.get_conn() as conn: 
    # 커넥션 획득 -> 입출력 연산 영향 있음(예외처리, with문 염두)
    # (with문을 통해 open,close를 활용하지 않았다면 마지막에 무조건 닫는 코드 존재)
    # 7. 전체를 try ~ except로 감싸기(I/O)
        with conn.cursor() as cursor:        
            # 4-1. insert 구문 사용
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    user_id VARCHAR(50) PRIMARY KEY,
                    income INT DEFAULT NULL,
                    loan_amt INT DEFAULT NULL,
                    credit_score INT DEFAULT NULL,
                    grade VARCHAR(10) DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''')
            # 4. 신용 평가 별과 삽입( 추후 고객 정보 업데이트로 조정)
            sql = '''
                insert into customers
                (user_id, credit_score, grade)
                values
                (%s,%s,%s)
            '''
            params = [
                (data['user_id'],data['credit_score'],data['grade'])
                for data in uesrs_grade
            ]
            cursor.executemany(sql,params)
            conn.commit()

    # 4. 신용평가 결과 삽입(추후 고객 정보 업데이트로 조정)
    #    cursor.executemany()
    # 5. 커밋
    # 6. 연결종료

    # # 3. 데이터 검증 (방어적 코딩)
    # if not credit_results:
    #     logging.warning("적재할 신용 평가 데이터가 없습니다.")
    #     return

    # logging.info(f"최종 적재 단계 진입: 총 {len(credit_results)}건의 데이터를 처리합니다.")

    # # 4. 데이터 순회 및 적재 (실무에서는 여기서 MySqlHook 등을 사용해 DB 업데이트)
    # for res in credit_results:
    #     u_id = res.get('user_id')
    #     score = res.get('credit_score')
    #     grade = res.get('grade')
        
    #     # 로그 출력으로 적재 시뮬레이션
    #     logging.info(f"[DB LOAD] 사용자: {u_id} | 점수: {score} | 등급: {grade}")
        
    # # 성공 시 리턴 (필요시 다음 DAG로 경로 전달 등을 위해 사용)
    # return True

# 3. DAG 정의
with DAG(
    dag_id      = "07_msa_api_server_used", 
    description = "MSA 아키텍처상에 특정 서비스(ai 서빙 컨셉)를 호출하여 신용평가 수행하는 스케줄링",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['msa', 'fastapi'],
) as dag:
    # 4. Task 정의

    # 4-1. 더미 데이터 준비 -> 추후 고객 정보 저장 -> s3 업로드까지 
    task_create_dummy_data = PythonOperator(
        task_id="task_create_dummy_data",
        python_callable= _create_dummy_data
    )

    # 4-2. API 호출(AI 서비스 활용) -> 신용평가획득
    task_api_service_call = PythonOperator(
        task_id="task_api_service_call",
        python_callable= _api_service_call
    )
    # 4-3. 결과저장 -> 추후 고객 정보 업데이트
    task_load_users_credit = PythonOperator(
        task_id="task_load_users_credit",
        python_callable=_load_users_credit
    )


    # 5. 의존성, 각 task는 XCom 통신으로 데이터 공유
    task_create_dummy_data >> task_api_service_call >> task_load_users_credit
