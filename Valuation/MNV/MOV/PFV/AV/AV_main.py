##### Valuation/MNV/MOV/PFV/AV/AV_main.py #####
'''
AV_main.py는 Spotify와 Melon 플랫폼의 앨범 메트릭 데이터를 통합하여 앨범 평가(AV)를 산출하는 기능을 수행함
Weights와 Variables 모듈을 통해 RV, SV, APV 가중치 및 REVENUE_PER_STREAM 값을 설정함
parse_date_any_format 함수는 다양한 날짜 포맷을 지원하여 문자열을 datetime 객체로 파싱함
av() 함수는 Firebase 캐시를 확인 후, UDI_main 모듈을 호출하여 UDI와 결합된 메트릭 데이터를 획득함
각 앨범에 대해 할인된 수익(RV_a), 스트림 기반 수익(SV_a), 인기도 기반 값(APV_a)을 산출함
UDI 값을 기본값 또는 계산값으로 적용하여 최종 AV 값(AV_a)을 가중치와 결합하여 계산함
각 앨범의 메트릭 데이터는 spotify_album_id, melon_album_id, 제목, 발매일 등으로 구성됨
정렬된 앨범 리스트와 총 AV 값은 Firebase에 저장되어 후속 분석에 활용됨
이 구조는 플랫폼별 데이터 통합 및 앨범 평가 모델의 효율적 응용을 가능하게 함
'''

from collections import defaultdict
from datetime import datetime
from Valuation.utils.weights import Weights
RV_WEIGHT = float(Weights.PFV.RV_WEIGHT)
SV_WEIGHT = float(Weights.PFV.SV_WEIGHT)
APV_WEIGHT = float(Weights.PFV.APV_WEIGHT)
UDI_ALPHA = float(Weights.PFV.UDI_ALPHA)

from Valuation.utils.weights import Variables
REVENUE_PER_STREAM = float(Variables.REVENUE_PER_STREAM)

from Valuation.MNV.MOV.PFV.AV.UDI.UDI_main import udi
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='AV'

def parse_date_any_format(date_str):
    """시도 가능한 포맷을 순회하며 날짜 파싱."""
    if not date_str:
        return None
    for fmt in ['%Y-%m-%d', '%Y.%m.%d']:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def av():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'metrics')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    udi_calculated_data = udi()
    combined_metrics = udi_calculated_data.get('combined_metrics', {})
    udi_data = udi_calculated_data.get('udi', {})

    total_AV = 0
    album_AV = []
    for album in combined_metrics:
        album_id = album.get('id', '')
        # 기본값 처리
        RV_a = album.get('discounted_retail_revenue', 0)
        streams_data = album.get('streams_data', [])
        SV_a = sum(streams_data) * REVENUE_PER_STREAM

        PopR_a = album.get('album_popularity', 0)
        popularity_data = album.get('popularity_data', [])
        PopR_k_a = sum(popularity_data) if popularity_data else 0
        APV_a = PopR_k_a * PopR_a

        # UDI 값이 없으면 기본값 0으로
        UDI_a = udi_data.get(album_id, 1)

        AV_a = ((RV_a * RV_WEIGHT) + (SV_a * SV_WEIGHT) + (APV_a * APV_WEIGHT)) * (1 + UDI_ALPHA * UDI_a)

        # release_date 파싱 시도 (문자열 -> datetime)
        raw_date = album.get('release_date')
        parsed_date = parse_date_any_format(raw_date)
        
        album_data = {
            'spotify_album_id': album.get('spotify_album_id'),
            'melon_album_id': album.get('melon_album_id'),
            'album_title': album.get('album_title'),
            'release_date': parsed_date,  # datetime 객체 또는 None
            'rv_weight': RV_WEIGHT,
            'rv': RV_a * RV_WEIGHT if RV_a is not None else 0,
            'sv_weight': SV_WEIGHT,
            'sv': SV_a * SV_WEIGHT,
            'apv_weight': APV_WEIGHT,
            'apv': APV_a * APV_WEIGHT,
            'udi_a': UDI_a,
            'udi_alpha': UDI_ALPHA,
            'udi': UDI_a * UDI_ALPHA,
            'av': AV_a
        }
        album_AV.append(album_data)
        total_AV += AV_a

    # release_date가 None인 경우 datetime.min으로 치환하여 정렬
    sorted_album_AV = sorted(album_AV, key=lambda x: x['release_date'] if x['release_date'] else datetime.min)

    result = {
        'metrics': sorted_album_AV,
        'av': total_AV,
    }

    save_record(DATA_TARGET, result, DATA_TARGET, 'metrics')
    return result