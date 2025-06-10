##### Valuation/MNV/MOV/PFV/PFV_main.py #####
'''
PFV_main.py는 앨범 평가(AV)를 기반으로 최종 PFV 지표를 산출함
Weights 모듈에서 AV_WEIGHT와 PFV_WEIGHT 값을 불러와 가중치 적용에 활용함
av() 함수를 호출하여 AV 데이터를 수집함
Firebase의 check_record 함수로 캐시 여부를 확인함
캐시 존재 시 저장된 데이터를 반환하여 연산을 최적화함
캐시 미존재 시 AV 값을 기반으로 pfv_value를 계산함
계산된 pfv_value에 AV_WEIGHT와 PFV_WEIGHT를 차례로 적용함
결과는 앨범 메트릭, 가중치, AV 및 PFV 값으로 구성됨
save_record 함수를 통해 결과 데이터를 Firebase에 저장함
모듈화된 구조로 앨범 가치 분석 및 후속 응용에 효과적으로 활용 가능함
'''

from Valuation.utils.weights import Weights
AV_WEIGHT=float(Weights.PFV.AV_WEIGHT)
PFV_WEIGHT=float(Weights.PFV.PFV_WEIGHT)

from Valuation.MNV.MOV.PFV.AV.AV_main import av
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='PFV'

def pfv():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'av_a')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    av_data = av()
    print(f'{av_data}')
    av_value = av_data.get('av', 0)
    pfv_value = (av_value * AV_WEIGHT)

    result = {
        'av_a': av_data.get('metrics', []),
        'av_weight': AV_WEIGHT,
        'av': av_value,
        'pfv_a': pfv_value,
        'pfv_weight': PFV_WEIGHT,
        'pfv': pfv_value * PFV_WEIGHT
    }

    save_record(DATA_TARGET, result, DATA_TARGET, 'av_a')
    return result