##### Valuation/MNV/MOV/PFV/AV/SV/SV_melon.py ######

'''
SV_main.py는 먼저 Firebase에서 캐시된 SV 데이터를 확인하여, 이미 저장된 결과가 있으면 이를 즉시 반환합니다.
캐시가 없으면, 환경변수에 지정된 MELON_ID 또는 MELON_IDS를 기반으로 Melon에서 앨범과 곡 데이터를 수집합니다.
각 앨범에 대해 해당 앨범의 곡들을 찾아 곡별 수익, 스트림, 리스너, 좋아요 수를 누적하여 앨범 단위의 통계를 산출합니다.
중복되지 않는 앨범 목록을 구성한 후, 전체 Melon 수익을 계산하여 Firebase에 저장하고 최종 결과를 반환합니다.
'''

import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sys
import os
import re
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")

melon_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
}

from Valuation.utils.weights import Variables
REVENUE_PER_STREAM = Variables.REVENUE_PER_STREAM

def string_to_integer(string):
    units = {'K': 10**3, 'M': 10**6, 'B': 10**9}
    for unit in units:
        if unit in string:
            return int(float(string.replace(unit, '').replace(',', '').replace('+', '')) * units[unit])
    return int(string.replace(',', '').replace('+', ''))

def get_albums_data(MELON_ID):
    url = 'https://www.melon.com/artist/albumPaging.htm'
    query = {
        'startIndex': '1',
        'pageSize': '10000',
        'orderBy': 'ISSUE_DATE',
        'artistId': MELON_ID
    }

    album_list_response = requests.get(url, params=query, headers=melon_headers).text
    soup = BeautifulSoup(album_list_response, 'html.parser')

    album_data = []
    albums = soup.select('li.album11_li')

    for album in albums:

        # 앨범 ID (href에서 추출)
        album_href = album.select_one('a.ellipsis')['href']
        album_id_match = re.search(r"goAlbumDetail\('(\d+)'\)", album_href)
        album_id = album_id_match.group(1) if album_id_match else None

        # 앨범 제목
        title = album.select_one('a.ellipsis').text.strip()

        # 앨범 발매일
        release_date = album.select_one('span.cnt_view').text.strip()

        # 총 곡 수
        total_songs = album.select_one('span.tot_song').text.strip()

        # 이미지 URL
        img_url = album.select_one('img')['src']
        
        # 앨범 정보 딕셔너리 생성
        album_info = {
            'album_id': album_id,
            'album_title': title,
            'release_date': release_date,
            'total_songs': int(total_songs.replace('곡','')),
            'img_url': img_url
        }

        # 결과 리스트에 추가
        album_data.append(album_info)
    
    return album_data
    


def get_songs_data(MELON_ID):
    paging_url = 'https://www.melon.com/artist/songPaging.htm'
    paging_query = {
        'startIndex': '1',
        'pageSize': '10000',
        'orderBy': 'ISSUE_DATE',
        'artistId': MELON_ID
    }

    print(f'MELON ID : {MELON_ID}')
    hearts_url = 'https://www.melon.com/commonlike/getSongLike.json'
    stars_url = 'https://www.melon.com/artist/getArtistFanNTemper.json?artistId=' + MELON_ID

    paging_songs = requests.get(paging_url, params=paging_query, headers=melon_headers).text
    paging_response = BeautifulSoup(paging_songs, 'html.parser')

    results = []
    for tr in paging_response.select('div.tb_list table tbody tr'):
        # 곡 ID 추출
        song_id = tr.select('button.btn_icon.like')[0].attrs['data-song-no']
        # 곡 제목 추출
        song_title = tr.select('button.btn_icon.like')[0].attrs['title']
        
        # 뮤직비디오 유무
        try:
            mv_button = tr.select('button.btn_icon.mv')[0]
            if 'disabled' in mv_button.attrs:
                song_mv = 'FALSE'
            else:
                song_mv = 'TRUE'
        except IndexError:
            song_mv = 'FALSE'

        # 앨범 제목 추출
        album_tag = tr.select('a[href*="goAlbumDetail"]')[0]
        album_href = album_tag.attrs['href']
        album_id = album_href.split("goAlbumDetail('")[1].split("');")[0]
        album_title = album_tag.attrs['title'].split(' - 페이지 이동')[0]

        # 타이틀곡 확인
        is_title = bool(tr.select('span.icon_song.title'))
        
        # 이용자수 & 리스너수
        api_request = requests.get(
            f'https://m2.melon.com/m6/chart/streaming/card.json?cpId=AS40&cpKey=14LNC3&appVer=6.0.0&songId={song_id}',
            headers=melon_headers).text
        api_response = json.loads(api_request)['response']
        if api_response['VIEWTYPE'] == "2":
            if api_response['STREAMUSER'] != '':
                listeners = string_to_integer(api_response['STREAMUSER'])
            if api_response['STREAMCOUNT'] != '':
                streams = string_to_integer(api_response['STREAMCOUNT'])

        # 곡 좋아요 수
        song_id_params = {'contsIds': song_id}
        hearts_song = requests.get(hearts_url, params=song_id_params, headers=melon_headers).text
        hearts_song_json = json.loads(hearts_song)
        hearts = hearts_song_json['contsLike'][0]['SUMMCNT']

        results.append({
            'song_id': song_id,
            'track_name': song_title,
            'album_title': album_title,
            'album_id': album_id,
            'mv': song_mv,
            'representative': 'TRUE' if is_title else 'FALSE',
            'melon_likes': hearts,
            'melon_listeners': listeners,
            'melon_streams': streams,
            'melon_revenue': streams * REVENUE_PER_STREAM
        })

    return results