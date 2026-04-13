'''
- 요구사항
    - 신용평가(예측만) 담당하는 API 구성
    - AI 모델이 처리하는 것처럼 전체 틀만 구성, 실제는 간단한 공식 처리
    - API
        - 요청 데이터
            - 1 ~ n명 데이터 => [개별 개인 정보(dict),...]
        - 응답 데이터
            - 1 ~ n명 데이터 => [개별 평가 정보(dict),...]
- 로컬 PC
    - 패키지 설치(필요시 가상환경에서 수행)
        - pip install fastapi pydantic uvicorn
'''

# 1. 모듈 가져오기
from fastapi import FastAPI # 앱 자체 의미
from pydantic import BaseModel # 해당 클래스 상속 -> 요청/응답 데이터 구조 정의
from typing import List # 요청 데이터에 대한 형태 정의(n개 데이터에 대한 타입 표현) -> 유효성 점검용
import random # 신용평가 더미 데이터 생성용


# 2. FastAPI 앱(객체) 생성
#    해당 변수명은 uvicorn에서 구동시 `모델명:FastAPI객체명`에서 객체명 해당

app = FastAPI() # dockerfile에 있는 fastapi 객체명 동기화 필수 이름 다르면 안됨

# 3. 요청/응답 데이터의 구조를 정의 + 유효성 검사의 틀을 제공하는 클래스 구성 -> pydantic 사용
#   BaseModel을 상속받고 -> pydantic 사용 가능함 -> 틀/구조 정의
# class의 구성원들중 클래스 멤버 -> 키값 활용 -> 타입 부여 -> 유효성 검사를 위해서
class ReqData(BaseModel):
    # 사용자 아이디, 소득, 대출총량
    user_id:str # 타입 힌트
    income:int
    loan_amt:int
    pass
class ResData(BaseModel):
    # 사용자 아이디, 신용점수, 등급
    user_id:str
    credit_score:int
    grade:str
    pass

# 4. 라우팅 (url 정의, 해당 요청시 처리할 함수 매칭)
# @app => 데커레이터 -> 함수안에 함수가 존재하는 2중 구조 => 특정함수에 공통 기능 부여시 유용
# 웹프로그램에서 자주 보임(요청을 전달하는 기능 공통등...)
# '/' -> 홈페이지 주소를 표현
@app.get('/') # URL 정의, http프로톨의 method를 정의(get 방식)
def home():
    return {'status' : 'AI 신용평가 서비스 API'}

# 신용평가 api 서비스
# post로 전송하는 이유: 보안, 대량의 데이터, http body를 통해서 전달 등등.. (로그인)
# response_model : 응답 데이터는 이런 형태로 보내라는 의미 -> 유효성 검사 자동 수행
@app.post("/predict", response_model=List[ResData])
def predict(users:List[ReqData]): # 요청 데이터 형태를 규정 -> 유효성 검사 자동 수행
    results =list()
    # 1. users 순회하며 정보 획득
    for user in users:
        '''
            가상 공식
            사전식 = (소득//1000)*10
            credit_score = min( random(300,600) + 사전식,900)
            grade = credit_score가 800이상 A, 600이상이면 B, 나머지는 C
        '''
        # 2. 고객 1명당 신용평가 수행( AI x, 간단한 가상 공식사용) => 차후 실제 모델과 교체
        사전식  = (user.income//1000)*10
        credit_score = min(random.randint(300, 600) + 사전식 - (user.loan_amt // 100), 900)
        if credit_score>= 800: grade='A'
        elif credit_score >=600 : grade='B' 
        else: grade = 'C'
        # 3. 평가 결과 담기 -> list
        results.append({
            "user_id":user.user_id,
            "credit_score":credit_score,
            "grade": grade 
        })
    # 4. 결과 반환
        pass
    return results