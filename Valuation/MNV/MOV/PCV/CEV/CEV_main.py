##### Valuation/MNV/MOV/PCV/CEV/CEV_main.py #####

'''
!!! 중요 !!!
1. FV 선행 - FV 값 필요
2. PFV 선행 - PFV, AV 값과 Album Metrics 필요.
3. Firebase Firestore의 <performance> 컬렉션에 실제 수익 데이터가 있어야 정상 작동함

===========

아티스트 공연 수익 데이터와 앨범 평가, 팬 밸류 트렌드 데이터를 활용하여 CEV(Concert Economic Value)를 산출함
환경변수를 통해 아티스트 정보와 NAVER_CONCERT_TAB URL 등을 동적으로 로드함
av(), er(), fv_t() 모듈을 호출하여 앨범 메트릭, 소셜 참여율, 팬 밸류 트렌드 데이터를 각각 수집함
parse_revenue, clean_start_period, calculate_discount_factor 함수로 데이터 전처리 및 할인율을 계산함
find_latest_album과 find_latest_fv 함수로 이벤트 발생 시점 이전의 최신 앨범 및 팬 밸류 데이터를 추출함
Firebase Firestore의 performance 컬렉션에서 실제 수익 데이터를 필터링하여 이벤트 정보를 획득함
수익 데이터가 있는 이벤트와 누락된 이벤트를 분리하여 각각 할인 적용 후 누적 수익을 산출함
이벤트별 AV dependency와 CEV 알파 계수를 계산하여 최종 CEV 값을 보정함
계산된 CEV 값과 관련 이벤트 데이터를 Firebase에 저장하고 결과를 반환함
모듈화된 구조로 재사용성과 확장성이 뛰어나 향후 경제 가치 평가에 응용 가능함
'''

import math
import os
import sys
from datetime import datetime
import pandas as pd
import statsmodels.api as sm
import asyncio

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")
env_urls = os.getenv("NAVER_CONCERT_TAB", "").split("|")

from Valuation.firebase.firebase_handler import load_with_filter

from Valuation.MNV.MOV.PFV.AV.AV_main import av
from Valuation.MNV.MOV.FV.ER.ER_main import er
from Valuation.MNV.MOV.FV.FV_t import fv_t
from Valuation.utils.weights import Weights, Variables
DISCOUNT_RATE=Variables.DISCOUNT_RATE
ALPHA_AV_DEPENDENCY=Weights.PCV.CEV_ALPHA_AV_DEPENDENCY
ALPHA_AV_PROPORTION=Weights.PCV.CEV_ALPHA_AV_PROPORTION
BETA_PER_EVENTS = Weights.PCV.CEV_BETA_PER_EVENTS
FV_T_WEIGHT = Weights.PCV.CEV_FV_T_WEIGHT

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
    
def calculate_discount_factor(start_period, current_date=datetime.now()):
    time_difference = (current_date - start_period).days / 365.25
    discount_factor = (1 / (1 + DISCOUNT_RATE)) ** time_difference
    return discount_factor
    
def find_latest_album(av_data, event_start_period):

    # release_date가 없거나 event_start_period보다 큰 경우 제외
    event_start_period_naive = make_naive(event_start_period)
    relevant_albums = [
        album for album in av_data
        if album.get('release_date') and make_naive(album['release_date']) <= event_start_period_naive
    ]

    if not relevant_albums:
        return None
    
    latest_album = max(relevant_albums, key=lambda x: x['release_date'])
    print(f"선택된 latest_album: {latest_album['album_title']} | {latest_album['av']}")
    return latest_album

def make_naive(dt):
    if dt is None:
        return None
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

def find_latest_fv(fv_data, event_start_period):
    event_start_period = make_naive(event_start_period)
    
    relevant_fv_entries = [
        entry for entry in fv_data if entry.get('date') and make_naive(entry['date']) <= event_start_period
    ]
    
    if not relevant_fv_entries:
        return None
    
    latest_fv = max(relevant_fv_entries, key=lambda x: x['date'])
    print(latest_fv)
    return latest_fv.get('FV_t_rolling_mean', 0)
    
def get_exist_events():
    filters = [('artist_id', '==', ARTIST_ID)]
    events_data = load_with_filter('performance', filters)
    return events_data

def load_av():
    portfolio = av()
    metrics_data = portfolio.get('metrics')
    metrics_df = pd.DataFrame(metrics_data)
    metrics_df['release_date'] = pd.to_datetime(metrics_df['release_date'], format='%Y.%m.%d')
    metrics_df = metrics_df.sort_values('release_date').reset_index(drop=True)
    metrics = metrics_df.to_dict(orient='records')
    return metrics

def load_fv():
    fv_t_data = fv_t()
    fanbase = fv_t_data['sub_data']
    df = pd.DataFrame(fanbase)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    df['FV_t_lag1'] = df['FV_t'].shift(1)
    df['FV_t_rolling_mean'] = df['FV_t'].rolling(window=3).mean()
    df.loc[:1, 'FV_t_rolling_mean'] = df.loc[:1, 'FV_t']

    result = df.to_dict(orient='records')
    return result

def cev_without_data():
    metrics = load_av()
    cev_result = 0
    for album in metrics:
        av_value = album.get('av',0)
        temp_cev = av_value * ALPHA_AV_DEPENDENCY
        cev_result += temp_cev

    return cev_result

def calculate_alpha(events_with_cer, av_data, fv_data):
    if not events_with_cer:
        return 0.5

    sum_cer = 0
    sum_av_factor = 0
    latest_fv_value = 0

    for event in events_with_cer:
        start_period_str = event.get('start_period')
        start_period = clean_start_period(start_period_str)
        if not start_period:
            continue
        
        temp_fv_value = find_latest_fv(fv_data, start_period)
        temp_album = find_latest_album(av_data, start_period)
        if not temp_album:
            continue
        
        cer_value = parse_revenue(event.get('revenue'))
        sum_cer += cer_value
        av_a = temp_album.get('av', 0)
        sum_av_factor += av_a
        latest_fv_value = temp_fv_value
    
    if sum_av_factor == 0 or latest_fv_value == 0:
        return 1.0
    
    alpha = sum_cer / (sum_av_factor * latest_fv_value)
    return min(alpha, 0.5)

from Valuation.MNV.MOV.PCV.CEV.CEV_collector import cev_collector, load_performance_data_from_sheet_and_save_to_firestore
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='CEV'

def cev():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'events')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    if env_urls and len(env_urls) > 0 and env_urls[0] != '':
            
        
        cev_collector_data = cev_collector()

        events_data = cev_collector_data.get('events',[])
        cev_value = 0
        cev_value_without_discount = 0
        av_dependency = 0
        cev_alpha = 0
        events = []

        if not events_data:
            av_dependency = 1
            cev_alpha = ALPHA_AV_DEPENDENCY
            cev_value = cev_without_data()

        elif events_data:
            fan_values = load_fv()
            av_data = load_av()
            er_data = er()
            engagement = er_data['er']

            total_events = len(events_data)
            missing_revenue_events = []
            events_with_cer = []
            for event in events_data:
                rev = event.get('revenue', 0)
                if int(rev) <= 0:
                    missing_revenue_events.append(event)
                else :
                    events_with_cer.append(event)

            num_missing_revenue = len(missing_revenue_events)

            av_dependency = num_missing_revenue / total_events if total_events > 0 else 0
            cev_alpha = calculate_alpha(events_with_cer, av_data, fan_values)

            sum_cer = 0
            sum_cer_without_discount = 0
            for event in events_with_cer:
                revenue = parse_revenue(event.get('revenue'))
                start_period_str = event.get('start_period')
                start_period = clean_start_period(start_period_str)

                latest_fv_value = find_latest_fv(fan_values, start_period)
                latest_album = find_latest_album(av_data, start_period)

                av_a = latest_album.get('av', 0)
                if start_period:
                    discount_factor = calculate_discount_factor(start_period)
                    if not discount_factor or discount_factor <= 0 :
                        discount_factor = 1.0
                    revenue_cer = 0
                    if not revenue_cer or revenue_cer <= 0 or math.isnan(revenue_cer):
                        revenue_cer = 0.0
                    revenue_with_cer = revenue + revenue_cer
                    discounted_revenue = revenue_with_cer * discount_factor
                    sum_cer += discounted_revenue 
                    sum_cer_without_discount += revenue

                rc_eok = revenue_cer / 100000000
                value_eok = discounted_revenue / 100000000  # 억 단위로 변환
                print(f"{event.get('title', 'Unknown')}의 가치 (할인 적용됨): {value_eok:.4f}억 (CER: {rc_eok:.4f}억)")
                event_data = {
                    'cer': discounted_revenue,
                    **event
                }
                events.append(event_data)
            
            print(f"수익 데이터가 있는 이벤트의 총 수익 (할인 적용됨): {sum_cer}")

            sum_estimated_cer = 0
            sum_estimated_cer_without_discount = 0

            for event in missing_revenue_events:
                print(f'Event Data : {event}')
                start_period_str = event.get('start_period')
                start_period = clean_start_period(start_period_str)
                if start_period : 
                    latest_fv_value = find_latest_fv(fan_values, start_period)
                    latest_album = find_latest_album(av_data, start_period)

                    av_a = latest_album.get('av', 0)
                    discount_factor = calculate_discount_factor(start_period)
                    estimated_cer_without_discount = (av_a * engagement) + ((latest_fv_value * engagement)) * (1 + cev_alpha)
                    estimated_cer = estimated_cer_without_discount * discount_factor
                    sum_estimated_cer += estimated_cer
                    sum_estimated_cer_without_discount += estimated_cer_without_discount

                    value_eok = estimated_cer / 100000000  # 억 단위로 변환
                    print(f"{event.get('title', 'Unknown')}의 추정 수익 및 가치 (할인 적용됨): {value_eok:.4f}억")

                    event_data = {
                        'cer': estimated_cer,
                        **event
                    }
                    events.append(event_data)

            cev_value = sum_cer + sum_estimated_cer
            cev_value_without_discount = sum_estimated_cer_without_discount + sum_cer_without_discount
            print(f"추정 수익 합산 (할인 적용): {sum_estimated_cer}")
            print(f"추정 수익 합산 (할인 미적용): {sum_estimated_cer_without_discount}")
            print(f"최종 CEV (할인 적용): {cev_value}")
            print(f"최종 CEV (할인 미적용): {cev_value_without_discount}")

    else:
        av_dependency = 1
        cev_alpha = ALPHA_AV_DEPENDENCY
        cev_value = cev_without_data()
        events = []
        cev_value_without_discount = cev_value
        
    result = {
        'events': events,
        'av_dependency' : av_dependency,
        'cev_alpha' : cev_alpha,
        'cev' : cev_value,
        'cev_value_without_discount' : cev_value_without_discount
    }

    print(f'CEV ALPHA : {cev_alpha}')
    print(f'CEV : {cev_value}')
    save_record(DATA_TARGET, result, DATA_TARGET, 'events')
    return result