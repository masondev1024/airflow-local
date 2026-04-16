# git 주소
    https://github.com/edu-kenshi/airflow-local

# 패키지 
    - 필요한 패키지는 전부 컨테이너상에 설치되어 있음
    - 개발 => IDE를 통해서 호스트 PC에서 작업중임
        - IDE에서 컨테이너로 접속해서 개발
        - 호스트 PC에서 가상환경등 구성하여 필요한 패키지 설치
            ```
                # vscode에서 airflow 인식하기 위해 동일하게 설치
                pip install apache-airflow==2.11.2

                # airflow상에 mysql 인식을 위해서 추가 설치
                pip install apache-airflow-providers-mysql

            ```

# 로그 제너레이터
    - 설치
        ```
            pip install faker
        ```
    - 구조
        ````
            /log_gen
            L log_generator.py : 로그 생성 클레스 모듈 포함
            L run.py           : 생성 코드 샘플
        ````
    - 산업별(금융, 이커머스 ,.. ) 로그 생성 클레스 모듈
    - 실행
        ```
            # 워킹 디렉토리(작업 폴더) -> 엔트리포인트
            cd log_gen
            ~/log_gen> pyton run.py
        ```

# .env git에서 제거
    ```
        # 캐싱 제거 (터미널에서 실행)
        git rm --cached .env

        # aws 브런치에서 진행
        # 스테이징 -> 커밋 -> 푸시
        git add .
        git commit -m "Feat: .env 캐시 제거"
        git push
    ```
