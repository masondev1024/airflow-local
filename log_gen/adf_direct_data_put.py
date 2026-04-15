'''
- Amazon Data Firehose(ADF)에게 direct로 데이터를 put 샘플
'''
# apache--airflow-providers-amazon 설치해서 import 가능
# 1. 모듈 가져오기
import boto3
import json
import time

# 2. 환경변수
ACCESS_KEY  = '' 
SECRET_KEY  = ''
REGION      = 'ap-north-east-2'


# 3. 특정 서비스(ADF) 클라이언트 생성
    #   AWS 외부에서 진행
def get_client(service_name='firehose',is_in_aws =True):
    if not is_in_aws:
    
        Session = boto3.Session(
            aws_access_key_id       = ACCESS_KEY,
            aws_secret_access_key   = SECRET_KEY,
            region_name             = REGION
        )
        return Session.client(service_name)
    # AWS 내부에서 진행
    firehose = boto3.client(service_name, region_name = REGION)

firehose = get_client()

print(firehose)