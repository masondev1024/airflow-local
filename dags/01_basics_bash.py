'''
- 기본 DAG 연습
- DAG의 기본 형태가 갖춰지지 않으면 등록 x
- 구성이 갖춰지면 특정 시간이 지난 후 자동으로 등록되는 구조

- 주제
    - bash 오퍼레이션 테스트, 적용 코드 -> DAG 인식, 작동 확인
    - 작동 확인 : 로그에서 확인 가능함, 대시보드 시각적 확인

- 특징
    - 현재 프로젝트 디렉토리는 docker 상에 특정 컨테이너와 연동되어 있음
    - 작성한 코드들은 airflow 현재 생태계 내부로 공유됨 -> 인식됨
        -> 대시보드에서 보임 : 볼륨 설정
'''

# 1. 필요한 패키지, 모듈 가져오기
## DAG
from airflow import DAG
## Operator 2.x
from airflow.operators.bash import BashOperator
## Operator 3.x
# from airflow.providers.standard.operators.bash import BashOperator
## 시간, 스케줄, 계산 등
from datetime import datetime, timedelta # timedelta는 시간의차이를 계산할 때 사용하는 

# 2-1. DAG 정의에 필요한 파라미터를 외부에서 설정(옵션), DAG 내부에서도 가능
default_args = {
    'owner'             : 'de_2team_manager' , # DAG 소유주
    'depends_on_past'   : False,               # 과거 데이터 소급 처리
    'retries'           : 1 ,                  # 작업 실패시 재시도 1회 자동
    'retry_delay'      : timedelta(minutes=5) # 작업실패 후 5분후에 재시도
    # 시나리오 : 작업 성공 => 완료
    # 시나리오 : 작업 실패 => 5분후 => 1회 재시도 => 성공 => 완료
    # 시나리오 : 작업 실패 => 5분후 => 1회 재시도 => 실패 => 완료(실패) => 향후 일정에서 성공하더라도 실패 데이터 소급 X

}
# 2-2. DAG 정의 -> 첫글자 대문자 -> class로 이해 -> 객체 생성 시작
with DAG(
    dag_id = "first_bash_dag",                    # DAG를 구분하는 용도
    description="DE를 위한 ETL 작업의 핵심 서비스(패키지) airflow 기본 연습용 DAG", # DAG 설명
    default_args = default_args,      # DAG의 기본 인자값
    schedule_interval = '@daily',      # 하루에 한번 00시00분(기본옵션), cron 표현 가능
    start_date = datetime(2026,2,25), 
    # 현재 시점에서 시작일과의 차이를 고려하여 소급 처리 여부 체크
    # 기본 설정에서 처리 x 설정해놓음 (False)
    catchup     = False,            # 과거에 대한 소급 처리 실행 방지
    # 해당 조치가 없었다면 현재일-2026.2.25 차이만큼 소급 처리 수행
    tags        = ['bash','basic'], # DAG가 많으면 찾기 힘들수 있음 -> 검색 -> 관리 
) as dag:
    # 3. DAG 세션 오픈 -> Operator 구성
    # 오퍼레이터 객체를 생성 -> Task가 정의됨 -> 구동이 되면 -> Task Instance 생성
    # t1: 오늘 날짜 출력
    t1 = BashOperator(
        task_id='print_date', # id 구성값 : 영문자, 숫자, 하이픈, 마침표, 언더바만으로 구성
        bash_command='date',  
    )

    t2 = BashOperator(
        task_id = 'sleep',
        bash_command = 'sleep 10' # 5초 대기
    )
    t3 = BashOperator( 
        task_id='print_hello',
        bash_command='echo "hello airflow"',
    )
    # 오퍼레이터에 파라미터를 설정하여 특정값이 들어가 생성되는 순간 t1,t2라는 task가 DAG라는 가방안에 들어간다.


    # 의존성 설정 (순서 정하기)
    t1 >> t2 >> t3