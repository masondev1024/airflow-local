'''
- Amazon Data Firehose(ADF)에게 direct로 데이터를 put 샘플
'''

# apache-airflow-providers-amazon 설치하여 자동으로 awd sdk(boto3)가 자동설치되어 있음
# 1. 모듈 가져오기
import boto3
# Boto3는 **AWS(Amazon Web Services)를 파이썬 코드로 제어할 수 있게 해주는 파이썬 전용 SDK(Software Development Kit)**
# AWS 콘솔(웹사이트)에 들어가서 마우스로 버튼을 눌러 처리하던 일들을 파이썬 스크립트로 자동화할 수 있게 해주는 라이브러리
import json
import time

# 2. 환경변수
ACCESS_KEY = ''
SECRET_KEY = ''
# 우리는 이 파일을 firehose stream을 cloudshell로 열어 내부에서 접근할려고 하기때문에 
# boto3는 코드안에 키가 없으면 자동으로 aws 서비스 내부에 숨겨져있는 폴더로 가서 찾아옴
REGION     = 'ap-northeast-2'

# 3. 특정 서비스(ADF) 클라이언트 생성
# is_in_aws 파라미터를 받는 이유는 해당 코드를 실행하는 환경이 aws 내부 shell이 아닌 로컬 pc나, 타사 server를 구분하기 위해
# 그렇기때문에 아래 함수에서 파라미터로 받은 is_in_aws 값을 True로 고정시킨 이유는 이 코드를 로컬 pc에서 실행하는게 아닌 그대로 복사해서
# amazon datastreams firehorse안에 똑같은 파일을 만들어 복사 붙여넣기 할거기때문에 True로 한 것. 
# 만약 현재 로컬에 있는 adf_direct_data_put.py를 실행시키려면 아래에서 객체를 생성할 때 firehose = get_client('firehose',False)
def get_client( service_name='firehose', is_in_aws=True ): 
    if not is_in_aws:
        #    AWS 외부에서 진행
        session   = boto3.Session(
            
            aws_access_key_id     = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY,
            region_name           = REGION
        )
        return session.client(service_name)    
    #   AWS 내부에서 진행
    return boto3.client(service_name, region_name = REGION)

firehose = get_client() # cloudshell로 adf 내부안으로 들어가 객체를 만들거기때문에 파라미터를 따로 변경하지 x

# 4. 로그 생성 및 ADF 발송
from run import make_one_log

# 5. 로그 1개 생성 -> adf 발송 함수
def send_log():
    # 5-1. 로그 1개 생성
    response =firehose.put_record(
        # 어디로? => Firehose 스트림 (본인것)
        DeliveryStreamName ='de-ai-06-an2-kdf-log-to-s3',
        # 데이터
        Record = {
            'Data':make_one_log() + "\n" # 로그 데이터를 한줄씩 적재
        }
    )
    print(f'전송결과 : {response}') # 응답 코드 200 -> ok
    pass



# 6. 10번 로그 생성 발송
for i in range(10):
    send_log()