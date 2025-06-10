##### Valuation/MNV/MOV/PFV/AV/RV/RV_main.py #####

'''
RV_main.py는 CircleChart 웹사이트에서 앨범 판매 데이터를 수집함
환경변수로 ARTIST_ID, ARTIST_NAME_KOR, ARTIST_NAME_ENG, MELON_ID 및 검색 기준을 불러옴
get_sales_record_data 함수는 연도별 페이지를 요청하여 스크립트 내 res_list 데이터를 파싱함
정규표현식을 이용하여 앨범 정보와 아티스트명을 추출 및 구조화함
get_sales_from_list 함수는 추가 API 호출을 통해 상세 판매 정보를 획득함
fetch_sales 함수는 중복 제거 로직을 적용하여 고유 앨범 데이터를 구성함
Variables 모듈의 LAP와 DISCOUNT_RATE를 활용하여 할인된 판매 가격을 계산함
각 앨범의 판매량과 할인된 가격으로 개별 및 누적 수익을 산출함
최종 결과는 판매 데이터, 총 판매량, 최신 앨범 가격, 할인율, 총 수익을 포함함
Firebase의 save_record 함수를 통해 분석 결과를 저장함
'''

import openpyxl
import os
import sys
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import time

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

CIRCLECHART_SEARCH_START_YEAR=os.getenv("CIRCLECHART_SEARCH_START_YEAR")
CIRCLECHART_SEARCH_ARTIST_NAME=os.getenv("CIRCLECHART_SEARCH_ARTIST_NAME")

def get_sales_record_data(start_year, artist_name):
    all_formatted_data = []

    current_year = datetime.now().year
    for year in range(start_year, current_year + 1):
        url = f'https://circlechart.kr/page_chart/search.circle?chartType=Album&serviceGbn=&searchGbn=2&termGbn=month&hitYear={year}&searchStr={artist_name}'
        
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            script_tags = soup.find_all('script')

            for script in script_tags:
                if 'res_list' in script.text:
                    script_text = script.string

                    if script_text:
                        pattern = r'res_list\[(\d+)\]\["(\w+)"\]\s*=\s*\'(.*?)\';'
                        matches = re.findall(pattern, script_text)

                        if matches:
                            res_list = {}
                            for match in matches:
                                index, key, value = match
                                index = int(index)
                                if index not in res_list:
                                    res_list[index] = {}
                                res_list[index][key] = value

                            formatted_data = [res_list[i] for i in sorted(res_list.keys())]

                            for obj in formatted_data:
                                artist_name_cleaned = re.sub(r'<.*?>', '', obj.get('ARTIST_NAME', ''))
                                album_object = {
                                    'service_ranking': obj.get('SERVICE_RANKING'),
                                    'hit_year': obj.get('HIT_YEAR'),
                                    'period_num': obj.get('PERIODNUM'),
                                    'title_name': obj.get('TITLE_NAME'),
                                    'album_name': obj.get('ALBUM_NAME'),
                                    'artist_name': artist_name_cleaned
                                }
                                all_formatted_data.append(album_object)
                        else:
                            print(f"No valid res_list found for year {year}.")
                    else:
                        print(f"No script content found for year {year}.")
                    
                    break
        else:
            print(f'Error fetching data for {year}: {response.status_code}')

        time.sleep(0.1)

    return all_formatted_data

def get_sales_from_list(year,month,index):
    details_url = 'https://circlechart.kr/data/api/chart/album'

    params = {
        'nationGbn': 'T',
        'termGbn': 'month',
        'hitYear': year,
        'targetTime': month,
        'yearTime': '3',
        'curUrl': f'circlechart.kr/page_chart/album.circle?nationGbn=T&targetTime={month}&hitYear={year}&termGbn=month&yearTime=3'
    }

    try:
        response = requests.post(details_url, data=params)
        if response.status_code == 200:
            data = response.json()
            if data.get('ResultStatus') == 'OK':
                data_list = data.get('List', {})

                key = str(int(index) - 1)

                if key in data_list:
                    return data_list[key]
                else:
                    print(f"Index {index} out of range for year {year} and month {month}.")
                    return {}
            else:
                print(f"API returned non-OK status for {year}-{month}: {data.get('ResultStatus')}")
                return {}
        else:
            print(f"Failed to fetch additional data for {year}-{month}: {response.status_code}")
            return {}
    except Exception as e:
        print(f"Exception occurred while fetching additional data for {year}-{month}: {e}")
        return {}

def fetch_sales():
    start_years = CIRCLECHART_SEARCH_START_YEAR.split('||')
    artist_names = CIRCLECHART_SEARCH_ARTIST_NAME.split('||')

    results = []
    seen_titles = set()
    unique_data = []
    for start_year_str, artist_name in zip(start_years, artist_names): 
        start_year = int(start_year_str)
        print(f'Start Year : {start_year} \nArtist Name : {artist_name}')
        recorded_data = get_sales_record_data(start_year, artist_name)
        sorted_data = sorted(
            recorded_data,
            key=lambda x: (int(x['hit_year']), int(x['period_num'])),
            reverse=True
        )
        
        for entry in sorted_data:
            title = entry['album_name']
            if title not in seen_titles:
                unique_data.append(entry)
                seen_titles.add(title)
        
        
        for data in unique_data:
            additional_data = get_sales_from_list(data['hit_year'], data['period_num'], data['service_ranking'])
            
            result = {}
            result['artist_id'] = ARTIST_ID
            result['melon_artist_id'] = MELON_ID
            result['artist_name'] = ARTIST_NAME_KOR
            result['artist_name_eng'] = ARTIST_NAME_ENG
            result['album_name'] = data['album_name']
            result['total_sales'] = int(additional_data['Total_CNT'])
            result['total_sales_year'] = data['hit_year']
            result['total_sales_month'] = data['period_num']
            result['seq_aoa'] = additional_data['seq_aoa']
            results.append(result)

    return results

from Valuation.utils.weights import Variables
LAP=Variables.LAP
DISCOUNT_RATE=Variables.DISCOUNT_RATE

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='RV'

def rv():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'sales_data')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)

    if CIRCLECHART_SEARCH_START_YEAR and CIRCLECHART_SEARCH_ARTIST_NAME:
        data = fetch_sales()
        current_year = datetime.now().year
        total_sales = 0
        total_revenue = 0
        for dt in data:
            sales = dt.get('total_sales', 500)
            total_sales += sales

            sold_year = int(dt.get('total_sales_year'))
            years_ago = current_year - sold_year

            discounted_LAP = LAP * (1 - DISCOUNT_RATE) ** years_ago
            revenue = sales * discounted_LAP
            discounted_revenue = revenue * (1 - DISCOUNT_RATE) ** years_ago

            dt['revenue'] = sales * LAP
            dt['discounted_LAP'] = discounted_LAP
            dt['discounted_revenue'] = discounted_revenue
            total_revenue += discounted_revenue

        result = {
            'sales_data' : data,
            'total_sales': total_sales,
            'latest_album_price': LAP,
            'discount_rate': DISCOUNT_RATE,
            'rv': total_revenue
        }

    else :
        result = {
            'sales_data': [],
            'total_sales': 0,
            'latest_album_price': LAP,
            'discount_rate': DISCOUNT_RATE,
            'rv': 0
        }
    
    save_record(DATA_TARGET, result, DATA_TARGET, 'sales_data')
    return result