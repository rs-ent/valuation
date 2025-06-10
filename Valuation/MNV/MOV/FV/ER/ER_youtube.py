##### Valuation/MNV/MOV/FV/ER/ER_youtube.py #####

'''
ER_youtube.py는 YouTube API를 통해 지정된 채널의 최신 동영상 데이터를 수집함
dotenv를 사용하여 환경변수에서 API 키 및 채널 ID를 로드함
firebase_handler의 check_record로 캐시된 데이터를 확인하여 중복 호출을 방지함
캐시가 없으면 YouTube 검색 API를 호출하여 최신 동영상 ID 목록을 확보함
수집된 동영상 ID를 기반으로 영상 API를 호출하여 조회수와 좋아요 통계를 집계함
각 동영상의 통계 데이터를 누적하여 총 동영상 수, 조회수, 좋아요 수를 계산함
결과 데이터를 딕셔너리로 구성하고 save_record로 Firebase에 저장함
모듈화된 구조로 API 호출, 데이터 집계, 캐싱 로직을 통합하여 효율적 데이터 수집을 구현함
수집된 데이터는 후속 분석 및 소셜 미디어 영향력 평가에 활용 가능함
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

YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='ER_youtube'
def er_youtube(max_results=50):
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'videos')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    result = {
        "videos": [],
        "total_videos": 0,
        "total_views": 0,
        "total_likes": 0
    }
    
    if YOUTUBE_CHANNEL_ID and YOUTUBE_API_KEY:
        search_url = "https://www.googleapis.com/youtube/v3/search"
        search_params = {
            "key": YOUTUBE_API_KEY,
            "channelId": YOUTUBE_CHANNEL_ID,
            "part": "id",
            "order": "date",
            "maxResults": max_results
        }
        
        response = requests.get(search_url, params=search_params)
        video_ids = []
        if response.status_code == 200:
            items = response.json().get("items", [])
            for item in items:
                if item["id"]["kind"] == "youtube#video":
                    video_ids.append(item["id"]["videoId"])
        else:
            print("Error:", response.status_code, response.json())
            return None
        
        metrics_url = "https://www.googleapis.com/youtube/v3/videos"
        metrics_params = {
            "key": YOUTUBE_API_KEY,
            "id": ",".join(video_ids),
            "part": "statistics"
        }
        
        response = requests.get(metrics_url, params=metrics_params)
        
        if response.status_code == 200:
            items = response.json().get("items", [])
            result["total_videos"] = len(items)
            
            for item in items:
                video_id = item["id"]
                view_count = int(item["statistics"].get("viewCount", 0))
                like_count = int(item["statistics"].get("likeCount", 0))
                
                result["videos"].append({
                    "id": video_id,
                    "views": view_count,
                    "likes": like_count
                })
                result["total_views"] += view_count
                result["total_likes"] += like_count
        else:
            print("Error:", response.status_code, response.json())
    
    save_record(DATA_TARGET, result, DATA_TARGET, 'videos')
    return result