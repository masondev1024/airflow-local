'''
DAG 에서 OpenSearch 검색 -> 데이터 회득
'''

# 1. 모듈 가져오기
from opensearchpy import OpenSearch
from datetime import datetime, timedelta
import time     
from airflow.operators.python import PythonOperator
from airflow import DAG
import pendulum # 서울시간대 간편하게 설정
from airflow.models import Variable
import pandas as pd
# 2. 환경변수 설정
# HOST, AUTH, 인덱스(상황에 따라 별도 구성가능) -> 검색어/패턴으로 구성/고정등
HOST = Variable.get('HOST')
AUTH = (Variable.get('AUTH_NAME'), Variable.get('AUTH_PW'))
index_name = 'factory-45-sensor-v1' # 검색할 인덱스 이름



# 4.1 opensearch 통해 검색 후 결과 획득 콜백함수 (_searching_proc)
def _searching_proc(**kwargs):
    # OpenSearch 클라이언트 연결
    client = OpenSearch(
    hosts         = [{"host": HOST, "port": 443}], # https -> 443
    http_auth     = AUTH,
    http_compress = True,
    use_ssl       = True,
    verify_certs  = True,
    ssl_assert_hostname = False,
    ssl_show_warn = False
    )
    # 4-1-2 opensearch 검색 쿼리 구성
    #   Query DSL 기반으로 검색어/패턴 구성
    #   https://docs.opensearch.org/latest/query-dsl/ 
    query = {
        "size" : 1000, # 검색 결과 최대 1000개 획득
        "query": {
            "range": {
                "timestamp":{
                    "gte": "now-120m" # 최근 120분 동안의 데이터
                    # (gte: greater than or equal, lte: less than or equal)
                }
            } # 모든 문서 검색 (예시)
        }
    }

    # 4-1-3. 검색 요청
    # 인덱스 정보 + 상세 조건
    response = client.search(
        index=index_name,
        body=query
    )
    print( '검색결과', response )
    # 4-1-4 검색 결과 획득
    hits = response['hits']['hits']
    if not hits:
        print("검색 결과가 없습니다.")
        return
    else:
        print(len(hits), "개의 문서가 검색되었습니다.")
    # 4-1-5 분석 -> 요구사항(평균 온도, 최대 진동등 계산), 이상탐지(허용범위 이상인 경우)
    #분석이 가능한 형태의 자료구조로 변환(pandas or pyspark 등 활용- 데이터체급에 따라 적용)
    data = [ hit['_source'] for hit in hits ] # 검색 결과에서 _source 필드만 추출하여 리스트로 저장 ( dict 형태의 리스트 )
    # data = [ {}, {}, ... ] 
    df = pd.DataFrame(data)
    print(df.head(1)) # 데이터프레임 형태로 변환된 결과의 상위 1개 행 출력

    # 요구 사항 => 그룹화(groupby or pivot_table) -> 오븐별 평균 온도, 최대 진동 계산
    # df.groupby('oven_id')['temperature'].mean() # 오븐별 평균 온도 계산
    analysis_result = df.groupby('oven_id').agg({
        'temperature': 'mean', # 오븐별 평균 온도 계산
        'vibration': 'max',     # 오븐별 최대 진동 계산
        'status' : 'count'   # 오븐별 상태별 문서 수 계산
    }).rename(columns={'temperature': 'temp_mean', 'vibration': 'vib_max', 'status': 'status_count'}) # 결과 컬럼명 변경
    print("최근 120분 동안의 오븐별 평균 온도, 최대 진동, 상태별 문서 수")
    print(analysis_result)

    # 이상치 탐지 => 온도가 230도 이상인 데이터 추출 => 블리언(조건식) 인덱싱 사용
    # df => 2차원/metrics, series => 1차원/단일 컬럼, 0차원=> 값/스칼라
    outliers = df[df['temperature'] >= 230] # 파이썬에서는 안되고 판다스 넘파이에서부터 가능
    print("최근 120분 동안의 온도가 230도 이상인 데이터")
    print(outliers)
    # 알람 전송
    if not outliers.empty:
        print("알람: 온도가 230도 이상인 데이터가 존재", len(outliers), "건")
    else:
        print("현재 이상치는 없습니다.")
    # 4-1-6. 클라이언트 연결 종료
    client.transport.close() # 클라이언트 연결 종료 


# 3. DAG 정의
with DAG(
    dag_id = 'opensearch_to_python_dag',
    description="OpenSearch 검색 결과를 Python으로 획득하는 DAG",
    default_args={
        'owner': 'de_2team_manager',
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval='*/10 * * * *', # 이 표시는 10분마다 실행되는 스케줄링 패턴입니다.
    start_date= pendulum.datetime(2026,1,1, tz='Asia/Seoul'), # pendulum을 사용하여 서울 시간대로 시작 날짜 설정
    catchup=False, # Backfill 방지
    tags=['production', 'opensearch', 'aws']
    ) as dag:
    # 4. task 정의
    task_search_opensearch = PythonOperator(
        task_id='search_opensearch_task',
        python_callable=_searching_proc
    )
    # 5. task 간의 의존성 설정
    task_search_opensearch