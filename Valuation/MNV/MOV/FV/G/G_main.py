##### Valuation/MNV/MOV/FV/G/G_main.py #####
'''
G_main.py는 Songstats 기반 청취자 데이터를 활용하여 국가별 팬덤 경제력을 산출함
make_csv 함수는 JSON 파일을 파싱하여 국가, 도시, 월간 청취자 및 최고 기록 데이터를 CSV로 저장함
CSV 데이터는 청취자 통계를 국가별로 집계하여 팬덤 비율을 계산하는 기초 자료로 활용됨
get_country_gdp 함수는 pycountry와 World Bank API를 통해 국가별 1인당 명목 GDP 값을 조회함
calculate_fandom_economic_power 함수는 pandas로 청취자 비율과 GDP를 곱해 팬덤경제력을 산출함
get_usd_to_krw 함수는 환율 API를 호출하여 USD 대비 KRW 환율을 반환함
g 함수는 Firebase 캐시를 확인하고, 팬덤경제력 총합에 환율을 곱해 최종 경제력 지표를 도출함
모듈 간 데이터 흐름과 예외 처리로 안정적 데이터 처리 및 재사용성을 보장함
산출된 지표는 아티스트의 국제적 영향력 및 경제적 가치를 평가하는 데 응용 가능함
'''

import pandas as pd
import requests
import pycountry
from typing import List, Dict, Optional
import time
import copy
from datetime import datetime, timezone, timedelta
import json
import csv
import os
import sys

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

#접속 : https://songstats.com/artist/ji2rm1hs/knk/audience
#데이터 : https://data.songstats.com/api/v1/audience/map_stats?idUnique=rlm7ou49&source=spotify&
def make_csv(csv_path):
    json_path = 'Valuation/MNV/MOV/FV/G_data.json'

    if os.path.getsize(json_path) == 0:
        fallback_data = [
            ['Country', 'City', 'Current Monthly Listeners', 'Peak Monthly Listeners', 'Peak Date'],
            ['South Korea', 'Seoul', 100, 100, 'N/A']
        ]
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(fallback_data)
        print('G_data.json is empty (0 bytes). Fallback: All listeners from South Korea (100%).')
        return

    with open(json_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # "Listeners by City" 섹션 찾아내기
    listeners_by_city = None
    for stat in data['mapStats']:
        if stat['id'] == 'spotify_map_city_listeners':
            listeners_by_city = stat['data']
            break
    
    if listeners_by_city is None:
        print("Listeners by City 데이터를 찾을 수 없습니다.")
        return

    # CSV 컬럼 정의
    fields = ['Country', 'City', 'Current Monthly Listeners', 'Peak Monthly Listeners', 'Peak Date']

    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fields)

        # 국가별 정보 순회
        for row in listeners_by_city['rows']:
            country_info = row.get('countryRow', [])
            if not country_info:
                continue
            country_name = country_info[0].get('displayText', '')

            row_groups = row.get('rowGroup', [])
            for city_data in row_groups:
                city_name = city_data['cityName']
                cells = city_data['cells']
                # cells 인덱스 별 정보:
                # 0: 도시 정보 셀
                # 1: Current Monthly Listeners
                # 2: Peak Monthly Listeners
                # 3: Peak Date

                current_listeners = cells[1]['order'] if cells[1]['order'] is not None else ''
                peak_listeners = cells[2]['order'] if cells[2]['order'] is not None else ''
                peak_date = cells[3]['displayText'] if cells[3]['displayText'] is not None else ''

                writer.writerow([country_name, city_name, current_listeners, peak_listeners, peak_date])

    print(f"CSV 파일 생성 완료: {csv_path}")

def get_country_gdp(country_name: str, max_retries: int = 3, retry_delay: int = 1) -> Optional[float]:
    hardcoded_gdp = {
        'Taiwan': 33233  # 대만의 1인당 명목 GDP (PPP) 값
        # 필요 시 다른 국가들도 추가 가능
    }

    if country_name in hardcoded_gdp:
        return hardcoded_gdp[country_name]
    
    try:
        country = pycountry.countries.lookup(country_name)
        country_code = country.alpha_2
        url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.PCAP.PP.CD?format=json"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        for attempt in range(max_retries):
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                try:
                    data = response.json()

                    if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list) and len(data[1]) > 0:
                        latest_data = data[1][0]
                        gdp_value = latest_data.get('value', None)
                        if gdp_value is not None:
                            return gdp_value
                        else:
                            break
                    else:
                        break
                except ValueError:
                    break
            else:
                time.sleep(retry_delay)
    except (LookupError, KeyError):
        print(f"국가 코드 찾기 실패: {country_name}")


def calculate_fandom_economic_power() -> pd.DataFrame:
    csv_path = f'Valuation/MNV/MOV/FV/G/G_data_{ARTIST_ID}.csv'
    make_csv(csv_path)
    df = pd.read_csv(csv_path)

    df['Current Monthly Listeners'] = pd.to_numeric(df['Current Monthly Listeners'], errors='coerce')

    df_grouped = df.groupby('Country', as_index=False)['Current Monthly Listeners'].sum()

    total_listeners = df_grouped['Current Monthly Listeners'].sum()

    df_grouped['리스너비율'] = df_grouped['Current Monthly Listeners'] / total_listeners

    df_grouped['GDP'] = df_grouped['Country'].apply(get_country_gdp)

    df_grouped['GDP'] = df_grouped['GDP'].fillna(0)

    df_grouped['팬덤경제력'] = df_grouped['리스너비율'] * df_grouped['GDP']

    df_grouped.rename(columns={'Country': '국가', 'Monthly Listeners': '월간 청취자 수'}, inplace=True)

    df_sorted = df_grouped.sort_values(by='팬덤경제력', ascending=False)

    return df_sorted

def get_usd_to_krw():
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        usd_to_krw = data['rates']['KRW']
        print(f"1 USD = {usd_to_krw} KRW")
        return usd_to_krw
    else:
        print(f"Failed to fetch exchange rate: {response.status_code}")
        return None

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='G'

def g():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'fandom_economic_power')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    df = calculate_fandom_economic_power()
    won = get_usd_to_krw()

    g_result = df['팬덤경제력'].sum() * won
    data = df.to_dict(orient='records')
    result = {
        'fandom_economic_power': data,
        'exchange': won,
        'g': g_result
    }

    print(f'Fandom Economic Power : {round(g_result, 0)}원')
    save_record(DATA_TARGET, result, DATA_TARGET, 'fandom_economic_power')
    return result