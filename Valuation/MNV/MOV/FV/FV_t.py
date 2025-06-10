##### Valuation/MNV/MOV/FV/FV_t.py #####

'''
FV_t.py는 아티스트 팬 밸류 트렌드(FV_t)를 시계열 데이터로 산출하기 위한 모듈임함
parse_trend_data 함수는 SERPAPI로부터 수집한 타임시리즈 데이터에서 날짜, 타임스탬프, 쿼리, 추출값을 추출하여 DataFrame으로 정제함
clean_trend_df 함수는 아티스트 한글 및 영문 이름으로 필터링하고, 이상치 문자를 0으로 대체하여 정수형으로 변환함
normalize_date_string 함수는 특수 공백과 대시를 표준 하이픈으로 치환하여 날짜 문자열을 정규화함
convert_date 함수는 다양한 날짜 포맷을 지원하여 정규화된 문자열을 datetime 객체로 변환하고 월말 보정을 적용함
calculate_fv_t 함수는 팬 베이스(FB), 참여율(ER), 팬덤 경제력(G) 지표에 가중치 지수 연산을 적용해 FV_t 값을 산출함
fb, er, g 함수 호출로 각각 소셜 미디어, 참여율, 경제력 데이터를 획득하고 fv_trends 함수를 통해 웹 및 유튜브 관심도 데이터를 수집함
웹과 유튜브 트렌드 DataFrame은 날짜 기준 외부 조인을 통해 통합되어 각 날짜별 트렌드 비율을 산출함
계산된 FV_t 값은 일정 단위(억 단위)로 출력되며, Firebase에 저장되어 후속 분석에 활용됨
모듈은 pandas, numpy, scipy, re, dotenv 등 다양한 라이브러리를 활용하여 데이터 전처리, 정규화, 수치 연산 및 API 통합 처리를 수행함
'''

import pandas as pd
import numpy as np
from datetime import datetime
from scipy import stats
from scipy.optimize import fsolve
import os
import re
from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")
SERPAPI_API_KEY= os.getenv("SERPAPI_API_KEY")

def parse_trend_data(timeline):
    records = []
    
    for entry in timeline:
        date = entry['date']
        timestamp = entry['timestamp']
        for value in entry['values']:
            query = value['query']
            extracted_value = value['extracted_value']
            records.append({
                'date': date,
                'timestamp': timestamp,
                'query': query,
                'extracted_value': extracted_value
            })
    
    df = pd.DataFrame(records)
    return df

def clean_trend_df(df):
    desired_queries = [ARTIST_NAME_KOR, ARTIST_NAME_ENG]
    df_clean = df[df['query'].isin(desired_queries)].copy()
    df_clean['extracted_value'] = df_clean['extracted_value'].apply(lambda x: 0 if isinstance(x, str) and ('<' in x or '>' in x) else x)
    df_clean['extracted_value'] = df_clean['extracted_value'].astype(int)
    
    return df_clean

def normalize_date_string(date_str):
    # 특수 공백(예: U+2009, U+202F 등)을 일반 공백으로 변환
    # \s 에 매칭되지 않는 특수 공백은 명시적으로 치환
    date_str = re.sub(r'[\u2000-\u200F\u202F\u205F\u3000]', ' ', date_str)
    
    # 특수 대시(–, —)를 일반 하이픈(-)으로 변환
    date_str = date_str.replace('–', '-')
    date_str = date_str.replace('—', '-')
    
    return date_str.strip()

def convert_date(df):
    def parse_single_date(d_raw):
        d = normalize_date_string(d_raw)
        
        # 1) '%b %Y'
        try:
            dt = pd.to_datetime(d, format='%b %Y')
            return dt + pd.offsets.MonthEnd(0)
        except ValueError:
            pass
        
        # 2) '%Y-%m-%d'
        try:
            dt = pd.to_datetime(d, format='%Y-%m-%d')
            return dt + pd.offsets.MonthEnd(0)
        except ValueError:
            pass
        
        # 3) 같은 달 내 기간: "Sep 10 - 16, 2023"
        single_month_match = re.match(r'([A-Za-z]{3}) (\d+) - (\d+), (\d{4})', d)
        if single_month_match:
            month = single_month_match.group(1)
            last_day = single_month_match.group(3)
            year = single_month_match.group(4)
            try:
                dt_str = f"{month} {last_day}, {year}"
                dt = pd.to_datetime(dt_str, format='%b %d, %Y')
                return dt + pd.offsets.MonthEnd(0)
            except ValueError:
                pass

        # 4) 두 달에 걸친 기간 (같은 해): "Oct 29 - Nov 4, 2023"
        multi_month_match = re.match(r'([A-Za-z]{3}) (\d+) - ([A-Za-z]{3}) (\d+), (\d{4})', d)
        if multi_month_match:
            month2 = multi_month_match.group(3)
            last_day = multi_month_match.group(4)
            year = multi_month_match.group(5)
            try:
                dt_str = f"{month2} {last_day}, {year}"
                dt = pd.to_datetime(dt_str, format='%b %d, %Y')
                return dt + pd.offsets.MonthEnd(0)
            except ValueError:
                pass

        # 5) 두 날짜 모두 완전한 형태(연도가 다를 수도 있음): "Dec 31, 2023 - Jan 6, 2024"
        full_range_match = re.match(
            r'([A-Za-z]{3} \d{1,2}, \d{4}) - ([A-Za-z]{3} \d{1,2}, \d{4})', d
        )
        if full_range_match:
            # 두 번째 날짜를 최종 날짜로 사용
            end_date_str = full_range_match.group(2)
            try:
                dt = pd.to_datetime(end_date_str, format='%b %d, %Y')
                return dt + pd.offsets.MonthEnd(0)
            except ValueError:
                pass

        # 6) 모두 실패하면 일반 파싱
        dt = pd.to_datetime(d)
        return dt + pd.offsets.MonthEnd(0)

    df['date'] = df['date'].apply(parse_single_date)
    return df

def calculate_fv_t(trends_df, FB_a, ER_a, G_a):
    FB = FB_a ** FB_WEIGHT
    ER = ER_a ** ER_WEIGHT
    G = G_a ** G_WEIGHT
    trends_df['FV_t'] = ((FB * trends_df['trends_ratio_exponential']) * ER * G) * FV_T_WEIGHT
    return trends_df

from Valuation.MNV.MOV.FV.FB.FB_main import fb
from Valuation.MNV.MOV.FV.ER.ER_main import er
from Valuation.MNV.MOV.FV.G.G_main import g
from Valuation.MNV.MOV.FV.FV_trends import fv_trends
from Valuation.utils.weights import Weights
FB_WEIGHT = Weights.FV.FB_WEIGHT
ER_WEIGHT = Weights.FV.ER_WEIGHT
G_WEIGHT = Weights.FV.G_WEIGHT
FV_TREND_WEIGHT = Weights.FV.FV_TREND_WEIGHT
FV_T_WEIGHT = Weights.FV.FV_T_WEIGHT

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='FV_t'
def fv_t():
    load_data = check_record(DATA_TARGET, DATA_TARGET, "sub_data")
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    fb_data = fb()
    er_data = er()
    g_data = g()
    trends_data = fv_trends()
    
    web_trends_df = parse_trend_data(trends_data['web_trends'])
    youtube_trends_df = parse_trend_data(trends_data['youtube_trends'])

    web_trends_clean = clean_trend_df(web_trends_df)
    youtube_trends_clean = clean_trend_df(youtube_trends_df)
    
    web_trends_clean = convert_date(web_trends_clean)
    youtube_trends_clean = convert_date(youtube_trends_clean)

    combined_trends = pd.merge(
        web_trends_clean,
        youtube_trends_clean,
        on=['date', 'query'],
        how='outer',
        suffixes=('_web', '_youtube')
    )

    combined_trends.fillna(0, inplace=True)
    combined_trends['trends_ratio'] = combined_trends['extracted_value_web'] + combined_trends['extracted_value_youtube']
    print(f"Trend Ratio : {combined_trends}")
    trends_per_date = combined_trends.groupby('date')['trends_ratio'].sum().reset_index()
    for index, row in trends_per_date.iterrows():
        print(f"Index: {index}, Date: {row['date']}, Trends Ratio: {row['trends_ratio']}")
    trends_per_date['trends_ratio_exponential'] = 1.0003 ** trends_per_date['trends_ratio']

    result_df = calculate_fv_t(trends_per_date, fb_data.get('fb'), er_data.get('er'), g_data.get('g'))
    
    for index, row in result_df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        fv_value = row['FV_t'] / 100000000
        print(f'{date_str} : {fv_value:.5f}억')
    
    sub_data = result_df.to_dict(orient='records')
    result = {
        "collected_time": datetime.now().strftime("%Y-%m-%d"),
        "sub_data": sub_data
    }
    
    save_record(DATA_TARGET, result, DATA_TARGET, "sub_data")
    return result

