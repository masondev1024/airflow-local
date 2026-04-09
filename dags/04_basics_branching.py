# 1. 모듈 가져오기
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule # 성공/실패/취소 단위 등등 조건 설정
from datetime import datetime, timedelta
import logging 

# 2. DAG 정의
with DAG() as dag:
    # task 정의
    task_start   = EmptyOperator()
    task_branch  = BranchPythonOperator()
    task_process = PythonOperator()
    task_skip   =  EmptyOperator()
    task_end    =  EmptyOperator()

    # 4. 의존성 정의 -> 시나리오별 준비
    task_start >> task_branch
    task_branch >> task_process >> task_end
    task_branch >> task_skip >> task_end
    pass