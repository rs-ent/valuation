##### Valuation/MNV/MOV/PCV/MDS/MDS_main.py #####
'''
MDS_main.py는 FV 트렌드 데이터, 소셜 참여율 및 앨범 평가 데이터를 활용하여 공연 경제 가치를 산출함
make_naive 함수는 timezone 정보가 포함된 datetime 객체를 naive 형태로 변환함
decay_factor 함수는 앨범 출시 후 경과 일수에 따른 지수 감쇠율을 계산하여 AIF 요소를 산출함
calculate_discount_factor 함수는 이벤트 발생일과 현재일 사이의 할인율을 연 단위로 산출함
fv_t, er, av 모듈을 호출하여 각각 팬 밸류 트렌드, 소셜 참여율, 앨범 평가 데이터를 불러옴
fv_t 데이터를 DataFrame으로 변환하고 날짜별 이동 평균(FV_t_rolling)을 계산함
앨범 데이터에서 최대 AV 값을 기준으로 각 앨범의 상대적 가치와 감쇠 효과를 적용하여 AIF_t를 산출함
각 날짜별로 최신 앨범의 AV 값과 해당 시점까지 출시된 앨범의 AIF_t, 그리고 er 값을 결합하여 MDS_t를 계산함
계산된 MDS_t 값은 할인 요인을 적용하여 억 단위로 변환 후 출력 및 누적 합산됨
최종 결과는 날짜별 MDS_t 기록과 총 MDS 값으로 구성되어 Firebase에 저장되어 후속 분석에 활용됨
'''

import pandas as pd
import numpy as np
from datetime import datetime

def make_naive(dt):
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

from Valuation.MNV.MOV.FV.FV_t import fv_t
from Valuation.MNV.MOV.FV.ER.ER_main import er
from Valuation.MNV.MOV.PFV.AV.AV_main import av
from Valuation.utils.weights import Weights, Variables
DISCOUNT_RATE=Variables.DISCOUNT_RATE
AIF_WEIGHT=Weights.PCV.MDS_AIF_WEIGHT
AV_WEIGHT=Weights.PCV.MDS_AV_WEIGHT

def decay_factor(release_date, current_date, half_life_days=180, decay_rate=1.5):
    release_date = make_naive(release_date)  # release_date를 naive로 변환
    days_elapsed = (current_date - release_date).days
    if days_elapsed < 0:
        return 1.0  # 출시일 이전에는 최대값
    return np.exp(-decay_rate * np.log(2) * days_elapsed / half_life_days)

def calculate_discount_factor(event_date, current_date=datetime.now()):
    time_difference = (current_date - event_date).days / 365.25  # Convert days to years
    discount_factor = 1 / ((1 + DISCOUNT_RATE) ** time_difference)
    return discount_factor

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='MDS'
def mds():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'records')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    fv_t_data = fv_t()
    er_value = er().get('er')
    av_data = av()

    df_fv = pd.DataFrame(fv_t_data['sub_data'])
    df_fv['date'] = pd.to_datetime(df_fv['date'])
    df_fv = df_fv.sort_values('date').reset_index(drop=True)
    df_fv['FV_t_rolling'] = df_fv['FV_t'].rolling(window=3, min_periods=1).mean()

    max_album_value = max(album['av'] for album in av_data['metrics'])

    for album in av_data['metrics']:
        album['release_date'] = pd.to_datetime(album['release_date'])

    mds_values = []
    total_mds = 0
    for index, row in df_fv.iterrows():
        current_date = make_naive(row['date'])
        FV_t_rolling = row['FV_t_rolling']

        released_albums = [
            album for album in av_data['metrics']
            if make_naive(album['release_date']) <= current_date
        ]

        latest_album = max(
            (album for album in av_data['metrics'] if make_naive(album['release_date']) <= current_date),
            key=lambda x: make_naive(x['release_date']),
            default=None
        )

        av_value = 1
        if latest_album:
            av_value = latest_album.get('av')

        AIF_t = 0.000001
        for album in released_albums:
            normalized_av = album['av'] / max_album_value if max_album_value > 0 else 1
            decay = decay_factor(album['release_date'], current_date)
            AIF_t += normalized_av * decay

        discount_factor = calculate_discount_factor(current_date)

        MDS_t = ((FV_t_rolling * er_value) + (av_value * AIF_t * er_value)) * discount_factor
        MDS_t_eok = MDS_t / 100000000
        print(f"{current_date} MDS : {MDS_t_eok:.4f}억")
        total_mds += MDS_t
        mds_values.append(MDS_t)

    df_fv['MDS_t'] = mds_values
    df_fv_result = df_fv.to_dict(orient='records')
    result = {
        'records': df_fv_result,
        'mds': total_mds
    }
    
    save_record(DATA_TARGET, result, DATA_TARGET, 'records')
    return result