# 1. 모듈 가져오기
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule # 성공/실패/취소 단위 등등 조건 설정
from datetime import datetime, timedelta
import logging 
import random

# 3-1. 콜백함수 정의
def _branching(**kwargs):
    '''
        특정 조건에 따라 분기 처리 -> 특정 task로 다음 수행 지정
    '''
    if random.choice([True,False]):
        logging.info('task process로 실행')
        return "process" # 이동하고 싶은 task_id 값 반환 -> 해당 task 수행
    else:
        logging.info('task skip 실행')
        return "skip" 
    pass
def _process(**kwargs):
    logging.info('task process : 특정 목적 수행')
    pass

# 2. DAG 정의
with DAG(
    dag_id = "04_basics_branching",
    description="분기 처리, 선택적 task 구동",
    default_args = {
        'owner'             : 'de_2team_manager' , 
        'retries'           : 1 ,                  
        'retry_delay'       : timedelta(minutes=1)
    },      
    schedule_interval = '@daily', 
    start_date = datetime(2026,2,25), 
    catchup     = False,           
    tags        = ['branch','trigger_rule'],
) as dag:
    # task 정의
    task_start   = EmptyOperator(
        
        task_id ="start"
    )
    task_branch  = BranchPythonOperator(
        task_id ="branching",
        python_callable = _branching
    )
    task_process = PythonOperator(
        task_id ="process",
        python_callable = _process
    )
    task_skip   =  EmptyOperator(
        task_id ="skip"
    )
    task_end    =  EmptyOperator(
        task_id ="end",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # 4. 의존성 정의 -> 시나리오별 준비
    task_start >> task_branch
    task_branch >> task_process >> task_end
    task_branch >> task_skip >> task_end
    pass