import json
import time
# log_generator.py 파일이 같은 경로에 있어야 합니다.
from log_generator import LogGenerator

def make_log(config, generator_instance):
    # 1. 함수 매핑을 함수 내부에서 처리 (안전성 확보)
    log_gen_map = {
        "finance": generator_instance.finance,
        "factory": generator_instance.factory
    }

    # 2. 매핑 확인 및 함수 추출
    target = config.get('target_industry')
    cur_func = log_gen_map.get(target)

    if not cur_func:
        print(f"오류: {target}은(는) 지원하지 않는 산업군입니다.")
        return

    # 3. f-string 따옴표 수정 (바깥쪽 " , 안쪽 ' )
    print(f"{target} 로그 생성 시작")
    print('-' * 50)

    for i in range(config.get('total_count', 0)):
        log = cur_func()
        log_json = json.dumps(log, ensure_ascii=False)
        print(f'[LOG-{i+1}]{log_json}')

        # 4. 인터벌 계산 및 대기
        wait_time = generator_instance.get_interval_time(config['mode'], config['interval'])
        time.sleep(wait_time)

    print('-' * 50)

log_gen_g = LogGenerator()
def make_one_log():

    return json.dumps( log_gen_g.finance(), ensure_ascii=False ) 
    # dict -> json형태의 str : 객체 직렬화 ( 외부로 던져야 되기 때문에 문자열로 바꿔줘야 함 저번에 post 요청때처럼)
  

if __name__ == '__main__':
    # 인스턴스 생성
    log_gen = LogGenerator()

    # 설정 (중복된 total_count 제거)
    config = {
        "target_industry": "finance",
        "mode": "random",
        "interval": 1,
        "total_count": 50,
        "loop": False
    }

    # 함수 실행

    result = make_one_log()
    print(result)