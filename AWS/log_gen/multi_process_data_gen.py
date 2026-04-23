'''
- 멀티 프로세스로 데이터 발생
    - stor-01 ~ store-(프로세스수) : 점포별로 데이터 발생 구성
- 실행
    airflow/log_gen/multi_process_data_gen.py
'''

# 1. 모듈 가져오기
import json
import os
import uuid
import random
import boto3
import time
from datetime import datetime, UTC
from faker import Faker
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import multiprocessing # 멀티 프로세싱을 위한 모듈
load_dotenv() # .env 내용을 읽어서 환경 변수로 설정


# 2. faker 생성 및 환경변수 세팅
fake = Faker()
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
AWS_REGION = 'ap-northeast-2'
KINESIS_DATA_STREAM_NAME = 'de-ai-06-an2-dks-medallion-bronze-stream'

# 3. aws 연동 => Session 설정 => I/O
try:
    # kinesis 세션(클라이언트) 획득
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key= SECRET_KEY,
        region_name = AWS_REGION
    )
    kinesis_client = session.client('kinesis')
    print('AWS 연동 성공')
except Exception as e:
    print('AWS 연동 실패')

def gen_data(store_id):
    '''
    데이터 더미 생성
    '''
    items = [
        {"item_id": "bread-001", "item_name": "우유식빵", "price": 5500},
        {"item_id": "bread-002", "item_name": "천연발효버터치아바타", "price": 4800},
        {"item_id": "coffee-01", "item_name": "아메리카노", "price": 4000},
        {"item_id": "jam-01", "item_name": "수제 딸기잼", "price": 8500}
    ]

    selected_item = random.choice(items)
    qty = random.randint(1, 3)
    current_utc_time = datetime.now(UTC).isoformat().replace("+00:00", "Z")


    raw_log = {
        "event_id": str(uuid.uuid4()),  # 이벤트 관리 번호(중복 x)
        "event_time": current_utc_time, # 현재시간(이벤트 발생시간)
        "source_ip": fake.ipv4(),       # 클라이언트의 접속 IP(가짜)
        "user_agent": fake.user_agent(),# 클라이언트의 접속 브라우저 타입(가짜)
        "data": {              # 상세 데이터 => dict내에 dict 구성, 객체 직렬화추가
          "user_id" : f"user_{random.randint(100, 999)}", # 사용자 id (사용자별로 여러번구매)
          "item_id" : selected_item["item_id"],           # 구매 제품
          "price"   : selected_item["price"],             # 단가
          "qty"     : qty,                                # 수량
          "store_id": store_id                          # 매장번호(오프/온라인 포함) 
        },
        "ingested_at": current_utc_time                    # 로그 발생시간(event_time과 동일)
    }
    return raw_log
    pass

def send_to_kinesis(log_entry):
    '''
    kinesis로 데이터 전송
    '''
    try:
        # PartitionKey -> .... -> 샤드(전용차선)의 개수에 영향줌
        # -> 용량(가변(온디맨드), 고정(프로비저닝)) -> 운영비용 및 성능 영향
        # 샤드에 대응되는 컬럼(키) 지정 -> log_entry['event_id'] -> 중복되지 않는 가지수 등장
        # 몇개의 샤드가 필요하지? -> log_entry['event_id'] 해싱처리 -> 수치화 -> 구간화 -> 샤드 배치
        # 1개의 샤드에 여러개의 log_entry['event_id']들이 배치가 됨 -> 분산 구조(골고루)
        # 적정 개수를 모르겠다 => 온디멘드 => 테스트 => 적정 샤드수 산출 => 프로비저닝(서비스할 때)
        kinesis_client.put_record(
            StreamName = KINESIS_DATA_STREAM_NAME,
            Data = json.dumps(log_entry),
            PartitionKey = log_entry['event_id']
        )
        return True
    except Exception as e:
        print( 'aws 전송 에러', e )
        return False


def run_producer(i, store_id):
    try:
        print(f'프로세스-{i} {store_id}  가동')
        while True:
            log_entry = gen_data(store_id)
            if send_to_kinesis(log_entry):
                print(f'{log_entry["event_time"]}   전송 성공   {store_id} ')
            time.sleep( random.uniform(0.5, 1.5) )

    except KeyboardInterrupt:
        print('발생 중단')
    print(f'프로세스-{i} {store_id}  종료')

if __name__ == '__main__':
    # 동시에 진행할 프로세스의 수 지정
    NUM_STORES = 4
    
    processes = [] # 프로세스 보관용 -> 조인
    # 로그 출력
    print(f'{NUM_STORES}개의 점포가 데이터발생시켜서 kinesis로 전송')
    for i in range(NUM_STORES):
        # 점포 id 생성
        store_id = f"store-{str(i+1).zfill(2)}" # store-01, store-02, ... 
        # 다른 함수 사용 -> f'{store_id:02d}' -> store-{i+1:02d}
        p = multiprocessing.Process(target=run_producer, args=(i,store_id))
        processes.append(p)
        # 프로세스 가동 => 함수 호출 => 데이터 발생 => kinesis 전송 => 로그 출력
        p.start()

    try:
        # 모든 프로세스가 종료될 때까지 대기
        for p in processes:
            p.join() # join() -> 프로세스가 종료될 때까지 기다리는 함수
    except Exception as e:
        print('프로세스 실행 중 에러 발생')
        print(e)
    except KeyboardInterrupt:
        print('프로세스 실행 중단 요청')
        for p in processes:
            p.terminate() # 프로세스 강제 종료  