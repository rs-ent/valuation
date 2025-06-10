##### Valuation/MNV/MOV/FV/FB/FB_main.py #####
'''
아티스트 소셜 미디어 데이터 수집 및 팬 베이스 산출 기능을 수행함
환경변수를 통해 ARTIST_ID, ARTIST_NAME_KOR, ARTIST_NAME_ENG, MELON_ID를 로드함
openpyxl로 Statista 엑셀 파일에서 플랫폼 이름과 사용자 수를 추출함
추출된 데이터를 딕셔너리 형태로 매핑하여 플랫폼별 사용자 수를 구성함
fb_youtube, fb_instagram, fb_twitter 모듈을 통해 각 플랫폼의 팔로워 수를 수집함
전체 사용자 수를 기반으로 각 플랫폼의 영향력을 산출함
플랫폼별 팬 베이스는 팔로워 수와 영향력의 곱으로 계산됨
모든 플랫폼의 팬 베이스를 합산하여 최종 팬 베이스를 도출함
Firebase 캐시를 확인하여 중복 연산을 방지함
최종 결과는 아티스트 정보, 플랫폼별 데이터 및 총 팬 베이스를 포함하여 Firebase에 저장됨
'''

import openpyxl
import os
import sys

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

from Valuation.firebase.firebase_handler import save_record, load_record
from Valuation.MNV.MOV.FV.FB.FB_twitter import fb_twitter
from Valuation.MNV.MOV.FV.FB.FB_youtube import fb_youtube
from Valuation.MNV.MOV.FV.FB.FB_instagram import fb_instagram

def load_platform_users(target_spreadsheet, target_worksheet):
    spreadsheet = openpyxl.load_workbook(target_spreadsheet)
    worksheet = spreadsheet[target_worksheet]

    platform_names = [worksheet[f"B{row}"].value for row in range(6, 21)]
    user_counts = [worksheet[f"C{row}"].value for row in range(6, 21)]

    platform_data = dict(zip(platform_names, user_counts))
    print(platform_data)
    return platform_data

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='FB'
def fb():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'sub_data')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    platform_users = load_platform_users('Valuation/MNV/MOV/FV/FB/statista.xlsx', 'Data')
    
    youtube_user_count = platform_users.get('YouTube', 0.000001)
    twitter_user_count = platform_users.get('X/Twitter', 0.000001)
    instagram_user_count = platform_users.get('Instagram', 0.000001)
    
    total_user_count = sum(filter(None, platform_users.values()))

    youtube_followers = fb_youtube()
    print(f'Youtube Followers : {youtube_followers}')
    youtube_influence = 1 + (youtube_user_count / total_user_count)
    youtube_fanbase = youtube_followers * youtube_influence
    print(f'Youtube Fan Base : {youtube_fanbase}')

    instagram_followers = fb_instagram()
    print(f'Instagram Followers : {instagram_followers}')
    instagram_influence = 1 + (instagram_user_count / total_user_count)
    instagram_fanbase = instagram_followers * instagram_influence
    print(f'Instagram Fan Base : {instagram_fanbase}')
    
    twitter_followers = fb_twitter()
    print(f"Twitter Followers : {twitter_followers}")
    twitter_influence = 1 + (twitter_user_count / total_user_count)
    twitter_fanbase = twitter_followers * twitter_influence
    print(f"Twitter Fan base : {twitter_fanbase}")

    fb = youtube_fanbase + instagram_fanbase + twitter_fanbase
    print(f"Fan Base : {fb}")

    result = {
        'artist_id': ARTIST_ID,
        'melon_artist_id': MELON_ID,
        'artist_name': ARTIST_NAME_KOR,
        'artist_name_eng': ARTIST_NAME_ENG,

        'youtube_followers': youtube_followers,
        'youtube_user_count' : youtube_user_count,
        'youtube_influence': youtube_influence,
        'youtube_fanbase': youtube_fanbase,

        'instagram_followers': instagram_followers,
        'instagram_user_count': instagram_user_count,
        'instagram_influence': instagram_influence,
        'instagram_fanbase': instagram_fanbase,

        'twitter_followers': twitter_followers,
        'twitter_user_count' : twitter_user_count,
        'twitter_influence': twitter_influence,
        'twitter_fanbase': twitter_fanbase,

        'sub_data': [],

        'fb': fb
    }

    result['sub_data'].append({
            'youtube_followers': youtube_followers,
            'youtube_user_count' : youtube_user_count,
            'youtube_influence': youtube_influence,
            'youtube_fanbase': youtube_fanbase,

            'instagram_followers': instagram_followers,
            'instagram_user_count': instagram_user_count,
            'instagram_influence': instagram_influence,
            'instagram_fanbase': instagram_fanbase,

            'twitter_followers': twitter_followers,
            'twitter_user_count' : twitter_user_count,
            'twitter_influence': twitter_influence,
            'twitter_fanbase': twitter_fanbase,
        })
    save_record(DATA_TARGET, result, DATA_TARGET, 'sub_data')
    return result

fb()