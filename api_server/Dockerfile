# 도커 파일 -> 커스텀한 이미지 생성
# image 
FROM python:3.12.12-slim

# WORK DIR 
WORKDIR /app

# 필요한 패키지 install -> 가상환경 x -> 본 컨테이너에서 단독 사용(유일한 서비스)
# uvicorn : fastapi 앞단에서 서비스 중계(프록시, 안정적 백엔드 운영 등등, web 서버 역할 담당)
# fastapi : 백엔드 구성, WAS 역할 담당
# pydantic : API를 통해서 전달되는 요청 데이터, 응답하는 데이터 -> 유효성(valid) 체크, 구조전환 등
RUN pip install fastapi pydantic uvicorn # 
# docker 파일안에서 리눅스 명령어 사용하기 위해선 앞에 RUN 붙여야함

# 소스코드 복사 (작업 디렉토리에 복사)
COPY main.py .

# 이미지가 컨테이너로 구성 -> 자동으로 실행시킬 내용 작성
# main -> 파일명(모듈명), app -> 아직 구성전 fastapi 객체명(변수명)
CMD [ "uvicorn", "main:app", "--host", "0,0,0,0", "--port", "8000" ]