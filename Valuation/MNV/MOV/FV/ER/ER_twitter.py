##### Valuation/MNV/MOV/FV/ER/ER_twitter.py #####
'''
트위터 API를 활용하여 지정된 계정의 트윗 데이터를 수집함
환경변수에서 API 키, 토큰, 계정 정보 등을 불러와 설정함
get_user_id() 함수는 Twitter 계정의 사용자 ID를 요청 API를 통해 획득함
er_twitter() 함수는 캐시된 데이터를 check_record로 확인 후, 최신 트윗을 최대 100개 조회함
각 트윗의 공공 통계(public_metrics)에서 좋아요 수를 추출하여 누적함
수집된 트윗 데이터는 결과 딕셔너리로 구성되어 총 트윗 수와 누적 좋아요 수를 포함함
Firebase에 save_record 함수를 통해 결과 데이터를 저장함
오류 발생 시 API 응답 코드를 출력하여 문제를 알림함
'''

import requests

import os
import sys

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

TWITTER_ACCOUNT = os.getenv("TWITTER_ACCOUNT")

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_KEY_SECRET = os.getenv("TWITTER_API_KEY_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
TWITTER_CLIENT_ID = os.getenv("TWITTER_CLIENT_ID")
TWITTER_CLIENT_SECRET = os.getenv("TWITTER_CLIENT_SECRET")

def get_user_id():
    url = f"https://api.twitter.com/2/users/by/username/{TWITTER_ACCOUNT}"
    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('data', {}).get('id')
    else:
        print("Error:", response.status_code, response.json())
        return None

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='ER_twitter'
def er_twitter(max_results=100):
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'tweets')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    result = {
        "tweets": [],
        "total_tweets": 0,
        "total_likes": 0
    }

    if TWITTER_ACCOUNT:

        TWITTER_ID = get_user_id()
        
        url = f"https://api.twitter.com/2/users/{TWITTER_ID}/tweets"
        headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
        params = {"max_results": max_results, "tweet.fields": "public_metrics"}
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            posts = response.json().get('data', [])
            result["total_tweets"] = len(posts)
            
            for post in posts:
                like_count = post["public_metrics"]["like_count"]
                content = post["text"]
                result["tweets"].append({"content": content, "likes": like_count})
                result["total_likes"] += like_count
        else:
            print("Error:", response.status_code, response.json())
    
    save_record(DATA_TARGET, result, DATA_TARGET, 'tweets')
    return result