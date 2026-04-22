'''
- ma 에서 silver 단계로 넘어가는 과정에서, 데이터가 kinesis로 잘 들어오는지 확인
- 처리할 데이터(flatten, 파생변수, 컬럼명변경) 형태로 변환 -> json 직렬화 -> kinesis로 전송
    - event_id: 이벤트 관리 번호(중복 x)- engine에서 고유하게 식별할 수 있는 번호
    - event_time: 현재시간(이벤트 발생시간)
    - source_ip: 클라이언트의 접속 IP(가짜)
    - user_agent: 클라이언트의 접속 브라우저 타입(가짜)
    - data: 상세 데이터 => dict 내에 dict 구성, 객체 직렬화추가
        - user_id: 사용자 id (사용자별로 여러번구매)
        - item_id: 구매 제품
        - price: 단가
        - qty: 수량     
        - (price * qty) => total_price: 총 구매 금액
        - store_id: 매장번호(오프/온라인 포함)
    - source_ip
    - user_agent
    - dt( year, month, day)
    - hour as hr
'''