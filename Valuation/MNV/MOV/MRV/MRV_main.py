##### Valuation/MNV/MOV/MRV/MRV_main.py #####

'''
!!! 중요 !!!
1. FV 선행 - FV 값 필요
2. PFV 선행 - PFV, AV 값과 Album Metrics 필요.
3. Firebase Firestore의 <broadcast> 컬렉션에 실제 수익 데이터가 있어야 정상 작동함

===========

MRV_main.py는 FV, PFV, AV 및 실제 방송 수익 데이터를 결합하여 방송 가치(MRV)를 산출함
Firebase Firestore의  컬렉션에 저장된 실제 수익 데이터를 기반으로 이벤트 정보를 불러옴
parse_revenue와 clean_start_period 함수를 통해 문자열 형태의 수익과 시작일을 정제함
find_latest_album 함수는 이벤트 발생 시점 이전의 최신 앨범 데이터를 추출함
fv_t, er, av 모듈을 호출하여 팬 밸류 트렌드, 소셜 참여율, 앨범 평가 데이터를 획득함
fv_t 데이터로부터 날짜별 이동 평균(FV_t_rolling)을 계산하여 시계열 추세를 반영함
각 방송 이벤트의 카테고리에 따라 가중치(CATEGORY_WEIGHT)를 적용함
이벤트별로 해당 시점의 FV_t_rolling과 최신 앨범의 AV 값을 결합하여 개별 방송 가치를 산출함
할인 요인을 적용하여 이벤트 발생 시점까지의 시간 가치를 보정함
최종적으로 모든 방송 이벤트의 가치를 누적하여 총 MRV를 계산
'''
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import re
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")
env_urls = os.getenv("NAVER_BROADCAST_TAB_URLS", "").split("|")

def parse_revenue(revenue_str):
    if revenue_str is None:
        return 0
    try:
        return int(revenue_str.replace(',', '').replace('.', ''))
    except (ValueError, AttributeError) as e:
        print(f"Error parsing revenue '{revenue_str}': {e}")
        return 0
    
def clean_start_period(start_period_str):
    if not start_period_str:
        return None
    try:
        if start_period_str.endswith('.'):
            start_period_str = start_period_str.rstrip('.')
        return datetime.strptime(start_period_str, '%Y.%m.%d')
    except ValueError as e:
        print(f"Error parsing start_period '{start_period_str}': {e}")
        return None
    
def find_latest_album(av_data, event_start_period):
    # event_start_period를 naive로 변환
    if event_start_period is not None and event_start_period.tzinfo is not None:
        event_start_period = event_start_period.replace(tzinfo=None)
    
    relevant_albums = []
    for album in av_data:
        release_date_str = album.get('release_date')
        if not release_date_str:
            continue
        
        release_date = pd.to_datetime(release_date_str, format='%Y.%m.%d', errors='coerce')
        if release_date is None:
            continue
        
        # release_date를 naive로 유지
        if release_date.tzinfo is not None:
            release_date = release_date.replace(tzinfo=None)
        
        if release_date <= event_start_period:
            relevant_albums.append({**album, 'release_date': release_date})
    
    if not relevant_albums:
        return None
    
    latest_album = max(relevant_albums, key=lambda x: x['release_date'])
    return latest_album

from Valuation.firebase.firebase_handler import load_with_filter
def get_exist_events():
    filters = [('artist_id', '==', ARTIST_ID)]
    events_data = load_with_filter('broadcast', filters)
    return events_data

def extract_episodes(summary_str):
    if not summary_str:
        return 1
    match = re.search(r'(\d+)부작', summary_str)
    if match:
        return int(match.group(1))
    else:
        return 1

from Valuation.MNV.MOV.FV.FV_t import fv_t
from Valuation.MNV.MOV.PFV.AV.AV_main import av
from Valuation.MNV.MOV.FV.ER.ER_main import er
from Valuation.utils.weights import Weights, Variables
DISCOUNT_RATE = Variables.DISCOUNT_RATE
AV_WEIGHT = Weights.MRV.BV_AV_WEIGHT
AV_RATIO = Weights.MRV.BV_AV_RATIO
FV_WEIGHT = Weights.MRV.BV_FV_WEIGHT
CATEGORY_WEIGHT = Weights.MRV.BV_CATEGORY_WEIGHT

def calculate_discount_factor(event_date, current_date=datetime.now()):
    time_difference = (current_date - event_date).days / 365.25  # Convert days to years
    discount_factor = 1 / ((1 + DISCOUNT_RATE) ** time_difference)
    return discount_factor

from Valuation.MNV.MOV.MRV.MRV_collector import mrv_collector
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='MRV'

def mrv():
    load_data = check_record(DATA_TARGET)
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    results = {
        'mrv': 0
    }

    if env_urls and len(env_urls) > 0 and env_urls[0] != '':
        print(f"env_urls : {env_urls}")
        fv_t_data = fv_t()
        er_data = er()
        engagement = er_data['er']
        av_data = av()
        mrv_data = mrv_collector()
        events_data = mrv_data['events']

        df_fv = pd.DataFrame(fv_t_data['sub_data'])
        df_fv['date'] = pd.to_datetime(df_fv['date'])
        df_fv = df_fv.sort_values('date').reset_index(drop=True)

        df_fv['FV_t_rolling'] = df_fv['FV_t'].rolling(window=3, min_periods=1).mean()

        total_broadcast_value = 0

        if events_data and len(events_data) > 0 :
            for event in events_data:
                print(f"Event : {event}")
                category = event.get('category')
                w_category = CATEGORY_WEIGHT.get(category, 1.0)

                start_period_str = event.get('start_period')
                start_period = clean_start_period(start_period_str)
                print(f"Start Period : {start_period}")

                if not start_period:
                    print(f"Invalid start period for event: {event.get('title')}")
                    continue
                
                df_fv['date'] = df_fv['date'].dt.tz_localize(None)
                start_period = start_period.replace(tzinfo=None)
                relevant_fv_row = df_fv[df_fv['date'] <= start_period].iloc[-1] if not df_fv[df_fv['date'] <= start_period].empty else None
                FV_t_rolling = relevant_fv_row['FV_t_rolling'] * engagement
                
                latest_album = find_latest_album(av_data['metrics'], start_period)
                AV_a = latest_album['av'] * engagement

                N = event.get('frequency', 1)
                
                discount_factor = calculate_discount_factor(start_period)

                BF_event = (AV_a + FV_t_rolling) * N * w_category * discount_factor
                print(f'w_category : {w_category}')
                print(f'N : {N}')
                print(f'FV_t_rolling : {FV_t_rolling}')
                print(f'AV_a : {AV_a}')
                print(f'추정 가치 : {BF_event}')
                event['BF_event'] = BF_event
                total_broadcast_value += BF_event

                program_name = event.get('title', 'Unknown Program')
                bf_value_eok = BF_event / 100000000  # 억 단위로 변환
                print(f'{program_name} : {bf_value_eok:.4f}억')
        else :
            total_broadcast_value = 0

        results = {
            'mrv': total_broadcast_value
        }

    save_record(DATA_TARGET, results)
    return results