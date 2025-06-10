##### Valuation/MNV/MOV/PFV/AV/SV/SV_main.py #####
'''
SV_main.py는 Melon 웹사이트에서 아티스트 앨범 및 곡 데이터를 수집, 집계하여 수익 지표를 산출함
환경변수를 통해 ARTIST_ID, MELON_ID, MELON_IDS를 동적으로 불러옴
get_albums_data와 get_songs_data 함수를 호출하여 앨범 및 곡 세부 정보를 획득함
앨범별로 각 곡의 멜론 수익, 스트림, 좋아요, 청취자 수를 합산함
앨범에 대응하는 곡 데이터를 트랙 리스트에 할당함
중복 앨범 검증 로직을 통해 고유 앨범만 결과에 포함시킴
앨범 수익을 누적하여 전체 멜론 총 수익을 산출함
최종 결과 딕셔너리에 앨범 목록, 멜론 총 수익, SV 값이 포함됨
Firebase의 save_record 함수를 사용하여 데이터를 저장함
모듈화 및 캐싱 전략으로 효율적 데이터 수집 및 응용 가능함
'''

import os
from dotenv import load_dotenv
load_dotenv()
ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_IDS = os.getenv("MELON_IDS")
MELON_ID = os.getenv("MELON_ID")

from collections import defaultdict

from Valuation.MNV.MOV.PFV.AV.SV.SV_melon import get_albums_data, get_songs_data
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='SV'

def sv():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'albums')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    total_albums = []
    melon_total_revenue = 0

    ids = []
    if MELON_IDS:
        ids = MELON_IDS.split('||')
    else:
        ids.append(MELON_ID)
        
    for id in ids:
        albums = get_albums_data(id)
        songs = get_songs_data(id)

        for album in albums:
            album_id = album['album_id']
            album_revenue = 0
            album_streams = 0
            album_likes = 0
            album_listeners = 0
            tracks = []
            for song in songs:
                if song['album_id'] == album_id:
                    album_revenue += song['melon_revenue']
                    album_streams += song['melon_streams']
                    album_listeners += song['melon_listeners']
                    album_likes += song['melon_likes']
                    tracks.append(song)

            album['tracks'] = tracks
            album['melon_album_revenue'] = album_revenue
            album['melon_album_tracks_total_streams'] = album_streams
            album['melon_album_tracks_total_listeners'] = album_listeners
            album['melon_album_tracks_total_likes'] = album_likes
            
            if not any(a['album_id'] == album_id for a in total_albums) and len(album['tracks']) > 0:
                total_albums.append(album)
                melon_total_revenue += album_revenue


    total = melon_total_revenue + 0
    result = {
        'albums': total_albums,
        'melon_total_revenue': melon_total_revenue,
        'sv': total
    }
    
    save_record(DATA_TARGET, result, DATA_TARGET, 'albums')
    return result
