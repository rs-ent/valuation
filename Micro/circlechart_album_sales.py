import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import re
import time

from utils.logger import setup_logger
logger = setup_logger(__name__)

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

def fetch_album_sales_record_data():
    start_year = int(os.getenv('CIRCLECHART_SEARCH_START_YEAR'))
    artist_name = os.getenv('CIRCLECHART_SEARCH_ARTIST_NAME')

    all_formatted_data = []

    current_year = datetime.now().year
    for year in range(start_year, current_year + 1):  # start_year ~ 2024
        # URL 생성
        url = f'https://circlechart.kr/page_chart/search.circle?chartType=Album&serviceGbn=&searchGbn=2&termGbn=month&hitYear={year}&searchStr={artist_name}'
        
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            script_tags = soup.find_all('script')

            for script in script_tags:
                if 'res_list' in script.text:
                    # 스크립트 텍스트 가져오기
                    script_text = script.string

                    if script_text:
                        # 정규 표현식으로 res_list[number]["key"] = 'value'; 패턴 찾기
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

                            # 인덱스 순서대로 정렬된 리스트로 변환
                            formatted_data = [res_list[i] for i in sorted(res_list.keys())]

                            # 데이터 정제 및 객체 생성
                            for obj in formatted_data:
                                # HTML 태그 제거
                                artist_name_cleaned = re.sub(r'<.*?>', '', obj.get('ARTIST_NAME', ''))

                                # 원하는 형식의 객체 생성
                                album_object = {
                                    'service_ranking': obj.get('SERVICE_RANKING'),
                                    'hit_year': obj.get('HIT_YEAR'),
                                    'period_num': obj.get('PERIODNUM'),
                                    'title_name': obj.get('TITLE_NAME'),
                                    'album_name': obj.get('ALBUM_NAME'),
                                    'artist_name': artist_name_cleaned
                                }

                                logger.info(f"Fetched Data : {album_object}")
                                all_formatted_data.append(album_object)
                        else:
                            logger.error(f"No valid res_list found for year {year}.")
                    else:
                        logger.error(f"No script content found for year {year}.")
                    
                    break
        else:
            logger.error(f'Error fetching data for {year}: {response.status_code}')

        time.sleep(0.1)

    return all_formatted_data

def fetch_sales_from_list(year,month,index):
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
                    logger.error(f"Index {index} out of range for year {year} and month {month}.")
                    return {}
            else:
                logger.error(f"API returned non-OK status for {year}-{month}: {data.get('ResultStatus')}")
                return {}
        else:
            logger.error(f"Failed to fetch additional data for {year}-{month}: {response.status_code}")
            return {}
    except Exception as e:
        logger.error(f"Exception occurred while fetching additional data for {year}-{month}: {e}")
        return {}

def fetch_sales():
    recorded_data = fetch_album_sales_record_data()
    sorted_data = sorted(
        recorded_data,
        key=lambda x: (int(x['hit_year']), int(x['period_num'])),
        reverse=True
    )
    
    seen_titles = set()
    unique_data = []
    
    for entry in sorted_data:
        title = entry['album_name']
        if title not in seen_titles:
            unique_data.append(entry)
            seen_titles.add(title)
    
    results = []
    for data in unique_data:
        additional_data = fetch_sales_from_list(data['hit_year'], data['period_num'], data['service_ranking'])
        
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
