##### Valuation/utils/data_collector.py #####
'''
Melon 웹사이트에서 아티스트 상세 정보를 수집하여 Firebase에 저장하는 것을 목표로 함
캐시 체크를 통해 기존 데이터 중복 수집을 방지하는 로직을 구현함
HTTP GET 요청을 requests 모듈로 전송하여 아티스트 페이지의 HTML을 획득함
BeautifulSoup을 활용하여 HTML 문서를 파싱하고 주요 요소를 추출함
정규표현식으로 데뷔곡, 그룹 멤버 등 세부 데이터의 식별자를 추출함
환경변수를 통해 아티스트 관련 식별자 및 정보를 동적으로 불러옴
추가 API 호출로 JSON 응답을 받아 팬 수 데이터를 보완함
수집된 데이터를 save_data와 save_record 함수를 통해 Firebase에 저장함
pandas와 numpy를 포함한 라이브러리로 데이터 후처리 및 분석 확장 가능함
모듈화된 구조와 명확한 데이터 흐름으로 다양한 아티스트 분석 및 응용에 활용 가능함
'''

import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sys
from datetime import datetime
import os
import re
import asyncio
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from Valuation.firebase.firebase_handler import save_data, check_record, save_record
DATA_TARGET='artist'

def get_artist_data():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'data')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    ARTIST_ID = os.getenv('ARTIST_ID')
    ARTIST_NAME_KOR = os.getenv('ARTIST_NAME_KOR')
    ARTIST_NAME_ENG = os.getenv('ARTIST_NAME_ENG')
    MELON_ARTIST_ID = os.getenv('MELON_ID')

    artist_id = MELON_ARTIST_ID
    artist_url = f'https://www.melon.com/artist/detail.htm'
    melon_headers = {
        'Referer': f'https://www.melon.com/artist/timeline.htm?artistId={artist_id}',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    }
    cookies = {
        '__T_': '1',
        '__T_SECURE': '1',
        'PCID': '17253568021593451713142',
        'PC_PCID': '17253568021593451713142',
        'POC': 'MP10',
        # Add other cookie values as needed
    }

    detail_params = {'artistId': artist_id}
    response = requests.get(artist_url, params=detail_params, cookies=cookies, headers=melon_headers)
    paging_response = BeautifulSoup(response.text, 'html.parser')

    result = {}
    result['id'] = artist_id
    artist_name_tag = paging_response.find('p', class_='title_atist')
    if artist_name_tag:
        artist_name = artist_name_tag.get_text(strip=True)
        result['artist_name'] = artist_name.replace('아티스트명','')

    artist_image_tag = paging_response.find('img', class_='image_typeAll')
    if artist_image_tag:
        artist_image_url = artist_image_tag.get('src', '')
        result['artist_image_url'] = artist_image_url

    artist_follower_tag = paging_response.find('span', class_='cnt')
    if artist_follower_tag:
        follower_text = artist_follower_tag.get_text(strip=True).replace(',', '')
        follower_count = int(follower_text) if follower_text.isdigit() else 0
        result['followers'] = follower_count

    awards_section = paging_response.find('div', class_='section_atistinfo01')
    if awards_section:
        awards_list = awards_section.find_all('dd')
        awards = []
        for award in awards_list:
            award_text = award.get_text(strip=True)
            awards.append(award_text)
        result['awards'] = awards

    activity_section = paging_response.find('div', class_='section_atistinfo03')
    if activity_section:
        # Debut Song
        debut_song_info = activity_section.find('div', class_='debutsong_info')
        if debut_song_info:
            debut_song = {}
            song_link = debut_song_info.find('a', class_='thumb')
            if song_link:
                href = song_link.get('href', '')
                song_id_match = re.search(r"goSongDetail\('(\d+)'\)", href)
                if song_id_match:
                    debut_song['song_id'] = song_id_match.group(1)
            song_title_tag = debut_song_info.find('dt')
            if song_title_tag:
                song_title = song_title_tag.get_text(strip=True)
                debut_song['title'] = song_title
            result['debut_song'] = debut_song

        activity_info_dl = activity_section.find('dl', class_='list_define clfix')
        if activity_info_dl:
            dt_tags = activity_info_dl.find_all('dt')
            dd_tags = activity_info_dl.find_all('dd')
            for dt, dd in zip(dt_tags, dd_tags):
                key = dt.get_text(strip=True)
                value = dd.get_text(strip=True).split('곡재생')[0]
                key_map = {
                    '데뷔': 'debut_date',
                    '활동년대': 'active_years',
                    '유형': 'type',
                    '장르': 'genre',
                    '소속사명': 'agency_name',
                }
                eng_key = key_map.get(key, key)
                result[eng_key] = value

    # Group Members
    group_members_section = paging_response.find('div', class_='wrap_gmem')
    if group_members_section:
        members = []
        member_list = group_members_section.find_all('li')
        for member in member_list:
            member_info = {}
            name_tag = member.find('a', class_='ellipsis')
            if name_tag:
                member_info['name'] = name_tag.get_text(strip=True)
                href = name_tag.get('href', '')
                artist_id_match = re.search(r"goArtistDetail\('(\d+)'\)", href)
                if artist_id_match:
                    member_info['artist_id'] = artist_id_match.group(1)
            profile_img_tag = member.find('a', class_='thumb')
            if profile_img_tag:
                member_info['profile_image'] = profile_img_tag.get('src', '')
            members.append(member_info)
        result['group_members'] = members

    # Personal Information
    personal_info_section = paging_response.find('div', class_='section_atistinfo04')
    if personal_info_section:
        personal_info_dl = personal_info_section.find('dl', class_='list_define clfix')
        if personal_info_dl:
            dt_tags = personal_info_dl.find_all('dt')
            dd_tags = personal_info_dl.find_all('dd')
            for dt, dd in zip(dt_tags, dd_tags):
                key = dt.get_text(strip=True)
                value = dd.get_text(strip=True)
                key_map = {
                    '국적': 'nationality',
                }
                eng_key = key_map.get(key, key)
                result[eng_key] = value

    # Related Information
    related_info_section = paging_response.find('div', class_='section_atistinfo05')
    if related_info_section:
        # SNS Links
        sns_dl = related_info_section.find('dl', class_='list_define_sns clfix')
        if sns_dl:
            sns_links = {}
            sns_buttons = sns_dl.find_all('button', class_='btn_sns02')
            for button in sns_buttons:
                sns_type = button.get('title', '').split(' -')[0]
                onclick_attr = button.get('onclick', '')
                sns_url_match = re.search(r"window\.open\('([^']+)'", onclick_attr)
                if sns_url_match:
                    sns_url = sns_url_match.group(1)
                    sns_links[sns_type] = sns_url
            result['sns_links'] = sns_links

        # Other Links
        other_dl = related_info_section.find('dl', class_='list_define clfix')
        if other_dl:
            dt_tags = other_dl.find_all('dt')
            dd_tags = other_dl.find_all('dd')
            for dt, dd in zip(dt_tags, dd_tags):
                key = dt.get_text(strip=True)
                link_tag = dd.find('a')
                value = link_tag.get('href', '') if link_tag else ''
                key_map = {
                    'YouTube': 'youtube',
                }
                eng_key = key_map.get(key, key)
                result[eng_key] = value

    # Artist Introduction
    artist_intro_section = paging_response.find('div', class_='section_atistinfo02')
    if artist_intro_section:
        intro_div = artist_intro_section.find('div', class_='atist_insdc')
        if intro_div:
            artist_intro = intro_div.get_text(strip=True)
            result['introduction'] = artist_intro

    # 아티스트 팬맺기 수
    print(f'artist_id : {artist_id}')
    followers_url = 'https://www.melon.com/artist/getArtistFanNTemper.json?artistId=' + artist_id
    artist_stars_api = requests.get(followers_url, headers=melon_headers).text
    artist_stars_json = json.loads(artist_stars_api)
    followers = artist_stars_json['fanInfo']['SUMMCNT']
    result['followers'] = followers
        
    save_data('artists', result, artist_id)

    temp_result = {
        'data': [result],
        "collected_time": datetime.now().strftime("%Y-%m-%d")
    }

    save_record(DATA_TARGET, temp_result, DATA_TARGET, 'data')
    return temp_result