##### Valuation/MNV/MOV/PCV/MCV/MCV_youtube.py #####

import os
import sys
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import statsmodels.api as sm
import os
import requests
import time
from sklearn.preprocessing import StandardScaler
from scipy.optimize import minimize

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

BASE_URL = "https://www.googleapis.com/youtube/v3/"

from Valuation.firebase.firebase_handler import load_with_filter
from Valuation.utils.weights import Weights, Variables
REVENUE_PER_STREAM=Variables.REVENUE_PER_STREAM
DISCOUNT_RATE=Variables.DISCOUNT_RATE
CURRENT_DATE = datetime.now(timezone.utc)

INIT_WEIGHTS = {
    'w_EG': 0.0001,
    'w_eta': 0.01
}

def get_channel_uploads_playlist(channel_id):
    url = f"{BASE_URL}channels"
    params = {
        "part": "contentDetails",
        "id": channel_id,
        "key": YOUTUBE_API_KEY
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    except requests.exceptions.RequestException as e:
        return None

def get_videos_from_playlist(playlist_id):
    video_ids = []
    url = f"{BASE_URL}playlistItems"
    params = {
        "part": "contentDetails",
        "playlistId": playlist_id,
        "maxResults": 50,
        "key": YOUTUBE_API_KEY
    }
    while True:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [])
            video_ids.extend([item["contentDetails"]["videoId"] for item in items])
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params["pageToken"] = next_page_token
            time.sleep(0.1)
        except requests.exceptions.RequestException as e:
            break
    return video_ids

def get_video_details(video_ids):
    videos_data = []
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        url = f"{BASE_URL}videos"
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(batch_ids),
            "key": YOUTUBE_API_KEY
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            for video in data.get("items", []):
                # 추가할 아티스트 정보
                video['artist_id'] = ARTIST_ID
                video['artist_name_kor'] = ARTIST_NAME_KOR
                video['artist_name_eng'] = ARTIST_NAME_ENG
                video['melon_artist_id'] = MELON_ID
                video['youtube_channel_id'] = YOUTUBE_CHANNEL_ID
                videos_data.append(video)
            time.sleep(0.1)  # Small delay to respect rate limits
        except requests.exceptions.RequestException as e:
            continue
    return videos_data

def get_youtube_videos():
    playlist_id = get_channel_uploads_playlist(YOUTUBE_CHANNEL_ID)
    if not playlist_id:
        return []
    
    video_ids = get_videos_from_playlist(playlist_id)
    if not video_ids:
        return []
    
    videos = get_video_details(video_ids)
    print(f"총 {len(videos)}개의 비디오 데이터를 수집했습니다.")

    return videos

def calculate_discounted_ratio(published_at):
    try:
        published_date = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        delta_years = (CURRENT_DATE - published_date).days / 365.25
        discounted_ratio = (1 - DISCOUNT_RATE) ** delta_years
        return discounted_ratio
    except Exception as e:
        return 1.0

def parse_duration(duration):
    try:
        duration = duration.replace('PT', '')
        hours = 0
        minutes = 0
        seconds = 0

        if 'H' in duration:
            hours_part, duration = duration.split('H')
            hours = int(hours_part)
        if 'M' in duration:
            minutes_part, duration = duration.split('M')
            minutes = int(minutes_part)
        if 'S' in duration:
            seconds_part = duration.replace('S', '')
            seconds = int(seconds_part) if seconds_part else 0

        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds
    except Exception as e:
        print(f"Error parsing duration '{duration}': {e}")
        return 0
    
def preprocess_data(videos):

    df = pd.DataFrame(videos)

    df['viewCount'] = df['statistics'].apply(lambda x: int(x.get('viewCount', '0')))
    df['likeCount'] = df['statistics'].apply(lambda x: int(x.get('likeCount', '0')))
    df['favoriteCount'] = df['statistics'].apply(lambda x: int(x.get('favoriteCount', '0')))
    df['commentCount'] = df['statistics'].apply(lambda x: int(x.get('commentCount', '0')))
    df['duration'] = df['contentDetails'].apply(lambda x: x.get('duration', 'PT0S'))
    df['publishedAt'] = df['snippet'].apply(lambda x: x.get('publishedAt', '2000-01-01T03:00:00Z'))
    
    df['duration_seconds'] = df['duration'].apply(parse_duration)
    
    df['engagement_ratio'] = (df['likeCount'] + df['commentCount']) / df['viewCount'].replace(0, 1)
    
    df['efficiency_ratio'] = df['engagement_ratio'] / df['duration_seconds'].replace(0, 1)
    
    df['discounted_ratio'] = df['publishedAt'].apply(calculate_discounted_ratio)

    # 소수점 6자리로 반올림
    df = df.round({
        'engagement_ratio': 5,
        'efficiency_ratio': 5,
        'discounted_ratio': 5,
        'MCV': 2
    })

    scaler = StandardScaler()
    df[['viewCount_norm', 'engagement_ratio_norm', 'efficiency_ratio_norm']] = scaler.fit_transform(
        df[['viewCount', 'engagement_ratio', 'efficiency_ratio']]
    )
    
    return df

def calculate_mcv(df, weights):
    df['weighted_EG'] = df['engagement_ratio'] ** weights['w_EG']
    df['weighted_eta'] = df['efficiency_ratio'] ** weights['w_eta']

    df['MCV'] = (
        df['viewCount'] * 
        REVENUE_PER_STREAM * 
        (1 + df['weighted_EG']) * 
        (1 + df['weighted_eta']) * 
        df['discounted_ratio']
    )

    for index, row in df.iterrows():
        title = row.get('title', 'Unknown')
        value_eok = row['MCV']  # 억 단위로 변환
        viewCount = row['viewCount']
        er = row['engagement_ratio']
        eta = row['efficiency_ratio']
        print(f"{title}의 가치: {value_eok:.4f}원 (조회수 : {viewCount}회)")
        print(f"(참여도 : {er:.4f} / 효율성 : {eta:.4f})")
        print("=======================")

    return df

def optimize_weights(df):
    scaler = StandardScaler()
    X = df[['viewCount', 'engagement_ratio', 'efficiency_ratio']]
    X_scaled = scaler.fit_transform(X)
    X_scaled = sm.add_constant(X_scaled)

    y = df['MCV']

    model = sm.OLS(y, X_scaled).fit()
    print(model.summary())

    weights = {
        'w_EG': model.params.get('engagement_ratio', 0),
        'w_eta': model.params.get('efficiency_ratio', 0)
    }

    return weights

def optimize_weights_nonlinear(df):
    def cost_function(weights, df):
        w_EG, w_eta = weights
        predicted_mcv = (
            df['viewCount'] * 
            REVENUE_PER_STREAM * 
            (df['engagement_ratio'] * w_EG) * 
            (df['efficiency_ratio'] * w_eta) * 
            df['discounted_ratio']
        )
        return np.sum((df['MCV'] - predicted_mcv) ** 2)
    
    initial_weights = [INIT_WEIGHTS['w_EG'], INIT_WEIGHTS['w_eta']]
    bounds = [(0, None), (0, None)]

    result = minimize(cost_function, initial_weights, args=(df,), bounds=bounds)
    
    if result.success:
        optimized_weights = {
            'w_EG': result.x[0],
            'w_eta': result.x[1]
        }
        print(f"Optimized Weights: {optimized_weights}")
        return optimized_weights
    else:
        print("Optimization failed.")
        return INIT_WEIGHTS


from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='MCV_youtube'

def mcv_youtube():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'details')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    videos = get_youtube_videos()
    
    df = preprocess_data(videos)
    df = calculate_mcv(df, INIT_WEIGHTS)

    total_mcv = df['MCV'].sum()
    print(f"전체 MCV (Optimized): {total_mcv:.2f}원")

    result = {
        'mcv_youtube': total_mcv,
        'details': df[['id', 'viewCount', 'likeCount', 'commentCount', 'favoriteCount', 
                      'duration_seconds', 'engagement_ratio', 'efficiency_ratio', 
                      'discounted_ratio', 'publishedAt', 'MCV']].to_dict(orient='records'),
    }

    print(result)

    save_record(DATA_TARGET, result, DATA_TARGET, 'details')
    return result