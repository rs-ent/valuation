##### Valuation/MNV/MOV/PFV/AV/APV/APV_main.py #####
'''
APV_main.py는 Spotify API를 활용하여 아티스트 앨범 데이터와 APV(Album Popularity Value)를 산출함
환경 변수에서 SPOTIFY_ID 및 아티스트 정보를 동적으로 불러옴
Firebase의 check_record 함수를 통해 기존 데이터 존재 여부를 확인함
SPOTIFY_ID가 존재할 경우 spotify_album_data 함수를 호출하여 아티스트 팔로워 수와 앨범 리스트를 수집함
각 앨범 내 트랙들의 인기도를 합산하여 album[‘track_popularity’]를 산출함
트랙 인기도와 아티스트 팔로워 수의 곱으로 개별 앨범의 APV 값을 계산함
모든 앨범의 APV 값을 총합하여 전체 APV를 도출함
결과 데이터를 딕셔너리로 구성하여 Firebase에 save_record 함수를 통해 저장함
SPOTIFY_ID 미존재 시 빈 앨범 리스트와 0의 APV를 반환함
모듈화와 캐싱 기법으로 효율적 데이터 수집 및 응용 가능성을 제공함
'''

from Valuation.MNV.MOV.PFV.AV.APV.APV_spotify import spotify_album_data
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='APV'

import os
from dotenv import load_dotenv
load_dotenv()
ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")
SPOTIFY_ID = os.getenv("SPOTIFY_ID")

def apv():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'albums')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    print(f"SPOTIFY_ID : {SPOTIFY_ID}")
    
    if SPOTIFY_ID:
        artist_followers, spotify_albums = spotify_album_data()

        total_apv = 0
        for album in spotify_albums:
            track_popularity = [
                track['popularity'] for track in album['tracks']
            ]

            track_popularity = sum(track_popularity)
            album['track_popularity'] = track_popularity

            album_apv = track_popularity * artist_followers
            album['apv'] = album_apv
            total_apv += album_apv
            
        result = {
            'albums' : spotify_albums,
            'apv' : total_apv
        }
    else:
        result = {
            'albums': [],
            'apv': 0
        }

    save_record(DATA_TARGET, result, DATA_TARGET, 'albums')
    return result