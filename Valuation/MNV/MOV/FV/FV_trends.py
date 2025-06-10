##### Valuation/MNV/MOV/FV/FV_trends.py #####

'''
!!! 중요 !!!
1. FV 선행 - FV 값 필요
2. PFV 선행 - PFV, AV 값과 Album Metrics 필요.

>> FV의 시계열 데이터를 구하고 PFV와 MRV에 적용

==============

FV_trends.py는 Google Trends API를 통해 아티스트 검색 관심도 시계열 데이터를 수집함
본 코드는 FV 및 PFV 산출에 필요한 선행 데이터를 확보하는 역할을 수행함
sv() 함수를 호출하여 앨범 메트릭 데이터를 가져오고 가장 이른 발매일을 기준으로 시작 기간을 설정함
get_interest_over_time() 함수를 통해 웹과 유튜브의 관심도 데이터를 각각 조회함
GoogleSearch 객체를 사용하여 SERPAPI로부터 타임시리즈 데이터를 요청함
조회된 데이터는 ‘timeline_data’ 키를 통해 Firebase 캐시로 저장 및 불러옴
환경변수를 통해 API 키 및 아티스트 관련 정보를 동적으로 로드함
수집된 웹과 유튜브 트렌드 데이터는 이후 PFV와 MRV 계산에 적용됨
시계열 데이터는 FV 트렌드 분석에 활용 가능한 형태로 가공됨
모듈화된 구조로 다양한 플랫폼의 시계열 데이터 수집 및 응용이 가능함
'''

import time
from datetime import datetime
import pandas as pd
import pycountry
from datetime import datetime
from serpapi import GoogleSearch
import re
import random
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests

import os
from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")
SERPAPI_API_KEY= os.getenv("SERPAPI_API_KEY")
CUSTOM_NAME_KOR = '루셈블'
CUSTOM_NAME_ENG = 'Loossemble'

from Valuation.MNV.MOV.FV.FV_main import fv
from Valuation.MNV.MOV.PFV.AV.SV.SV_main import sv
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET_WEB='FV_trends_web'
DATA_TARGET_YOUTUBE='FV_trends_youtube'
def fv_trends():
    sv_data = sv()
    albums = sv_data.get('albums')
    release_dates = [datetime.strptime(album.get('release_date'), '%Y.%m.%d') for album in albums]
    earliest_release_date = min(release_dates)
    start_period = earliest_release_date.strftime('%Y-%m-%d')
    print(f"START PERIOD: {start_period}")

    web_trends = get_interest_over_time(start_period, "WEB")
    print(web_trends)
    youtube_trends = get_interest_over_time(start_period, "YOUTUBE")

    fv_trends_data = {
        'web_trends': web_trends,
        'youtube_trends': youtube_trends
    }
    
    return fv_trends_data

def get_interest_over_time(start_period, target = "WEB"):
    if target == "WEB":
        checker = DATA_TARGET_WEB
    elif target == "YOUTUBE":
        checker = DATA_TARGET_YOUTUBE

    load_data = check_record(checker, checker, 'timeline_data')
    if load_data:
        print(f'{checker} Loaded')
        return load_data.get(checker).get('timeline_data')
    
    data = get_serpapi_data(start_period, target)
    temp = {
        "collected_time": datetime.now().strftime("%Y-%m-%d"),
        "timeline_data" : data.get('timeline_data')
    }

    save_record(checker, temp, checker, 'timeline_data')
    return temp

def get_serpapi_data(start_period, target = 'WEB'):

    today = datetime.today().strftime('%Y-%m-%d')

    query = f"{ARTIST_NAME_KOR}, {ARTIST_NAME_ENG}"
    #query = f"{CUSTOM_NAME_KOR}, {CUSTOM_NAME_ENG}"

    params = {}
    if target == 'WEB':
        params = {
        "engine": "google_trends",
        "q": query,
        "cat": "35",
        "date": f"{start_period} {today}",
        "data_type": "TIMESERIES",
        "include_low_search_volume": "true",
        "api_key": SERPAPI_API_KEY
    }
    elif target == 'YOUTUBE':
        params = {
            "engine": "google_trends",
            "q": query,
            "cat": "35",
            "gprop": "youtube",
            "date": f"{start_period} {today}",
            "data_type": "TIMESERIES",
            "include_low_search_volume": "true",
            "api_key": SERPAPI_API_KEY
        }

    search = GoogleSearch(params)
    results = search.get_dict()
    interest_over_time = results["interest_over_time"]

    return interest_over_time