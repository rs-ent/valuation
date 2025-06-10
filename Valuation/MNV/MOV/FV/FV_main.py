##### Valuation/MNV/MOV/FV/FV_main.py #####
'''
아티스트 식별자와 정보를 환경변수로부터 로드하여 기본 설정을 수행함
Weights 모듈에서 FB, ER, G, FV의 가중치 값을 불러와 연산에 적용함
fb, er, g 모듈을 호출하여 각각 팬 베이스, 소셜 참여율, 팬덤 경제력을 산출함
Firebase 캐시 확인 후, 기존 데이터가 존재하면 이를 반환하여 중복 연산을 방지함
각 모듈로부터 획득한 지표를 콘솔 출력으로 확인하여 디버깅에 활용함
팬 베이스, 참여율, 경제력 값을 지수 연산 방식으로 통합하여 Fan Valuation을 계산함
계산 결과에 FV_WEIGHT를 곱해 최종 Fan Valuation을 보정함
산출된 결과는 각 가중치와 개별 지표, 최종 Fan Valuation을 포함하는 딕셔너리로 구성됨
Firebase에 save_record 함수를 통해 결과를 저장하여 데이터 일관성을 유지함
모듈화된 구조로 확장성과 재사용성을 보장하며 후속 분석에 응용 가능함
'''

import openpyxl
import os
import sys

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

from Valuation.utils.weights import Weights
FB_WEIGHT = float(Weights.FV.FB_WEIGHT)
ER_WEIGHT = float(Weights.FV.ER_WEIGHT)
G_WEIGHT = float(Weights.FV.G_WEIGHT)
FV_WEIGHT = float(Weights.FV.FV_WEIGHT)

from Valuation.MNV.MOV.FV.FB.FB_main import fb
from Valuation.MNV.MOV.FV.ER.ER_main import er
from Valuation.MNV.MOV.FV.G.G_main import g

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='FV'
def fv():
    load_data = check_record(DATA_TARGET)
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    fb_result = fb()
    print(f'FB (Fan Base) : {fb_result['fb']}')

    er_result = er()
    print(f'ER (Engagement Ratio) : {er_result['er']}')

    g_result = g()
    print(f'G (Fandom Economic Power) : {g_result['g']}')

    fv_result = (fb_result['fb'] ** FB_WEIGHT) * (er_result['er'] ** ER_WEIGHT) * (g_result['g'] ** G_WEIGHT)
    print(f'Fan Valuation : {fv_result * FV_WEIGHT}')

    result = {
        'fb_weight': FB_WEIGHT,
        'fb': fb_result['fb'] * FB_WEIGHT,
        'er_weight': ER_WEIGHT,
        'er': er_result['er'] * ER_WEIGHT,
        'g_weight': G_WEIGHT,
        'g': g_result['g'] * G_WEIGHT,
        'fv_weight': FV_WEIGHT,
        'fv': fv_result * FV_WEIGHT
    }
    
    save_record(DATA_TARGET, result)
    return result