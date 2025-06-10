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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.logger import setup_logger

load_dotenv()

logger = setup_logger(__name__)

def string_to_integer(string):
    """단위가 포함된 문자열을 정수로 변환합니다."""
    units = {'K': 10**3, 'M': 10**6, 'B': 10**9}
    for unit in units:
        if unit in string:
            return int(float(string.replace(unit, '').replace(',', '').replace('+', '')) * units[unit])
    return int(string.replace(',', '').replace('+', ''))

def get_album_data(album_id):

    # Album Detail URL API
    detail_url = 'https://www.melon.com/album/detail.htm'
    
    # Hearts URL API
    heart_url = 'https://www.melon.com/commonlike/getAlbumLike.json'

    # Grade URL API
    grades_url = 'https://www.melon.com/album/albumGradeInfo.json'

    # Comments URL API
    cmt_url = 'https://cmt.melon.com/cmt/api/api_loadPgn.json'

    comments = []

    melon_headers = {
        # 'Referer': 'https://www.melon.com/album/detail.htm?albumId=' + album_id,
        'Referer': 'https://www.melon.com/index.htm',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:118.0) Gecko/20100101 Firefox/118.0'
    }

    detail_params = {'albumId': album_id}
    detail_tags = requests.get(detail_url, params=detail_params, headers=melon_headers).text
    paging_response = BeautifulSoup(detail_tags, 'html.parser')

    artist_tag = paging_response.find('div', class_='artist').find('a')
    artist_name = ''
    artist_id = ''
    if artist_tag:
        artist_name = artist_tag.get_text(strip=True)
        artist_href = artist_tag.get('href', '')
        artist_id_match = re.search(r"goArtistDetail\('(\d+)'\)", artist_href)
        if artist_id_match:
            artist_id = artist_id_match.group(1)

    album_type = paging_response.find('span', class_='gubun').get_text(strip=True).strip("[]")
    album_title = paging_response.find('div', class_='song_name').get_text(strip=True).replace('앨범명','')
    song_count = int(paging_response.find('span', class_='sum').get_text(strip=True).strip("()"))

    album_image_url = ''
    image_tag = paging_response.find('a', {'id': 'd_album_org'}).find('img')
    if image_tag:
        album_image_url = image_tag.get('src')

    meta_data = paging_response.find('dl', class_='list')
    release_date = meta_data.find('dd').get_text(strip=True)
    genre_text = meta_data.find_all('dd')[1].get_text(strip=True)
    genre = genre_text.split(", ")
    publisher = meta_data.find_all('dd')[2].get_text(strip=True)
    agency = meta_data.find_all('dd')[3].get_text(strip=True)

    tracklist = []
    track_table = paging_response.find('div', class_='section_contin')
    if track_table:
        tbody = track_table.find('tbody')
        if tbody:
            song_rows = tbody.find_all('tr')
            for row in song_rows:
                song_info = {}
                # Song ID from input checkbox
                input_checkbox = row.find('input', {'class': 'input_check'})
                if input_checkbox:
                    song_id = input_checkbox.get('value')
                    song_info['song_id'] = song_id
                else:
                    logger.error(f"Song ID not found for a song in album {album_id}")
                
                # Song title
                song_title_tag = row.find('div', class_='ellipsis').find('a')
                if song_title_tag:
                    song_title = song_title_tag.get_text(strip=True)
                    song_info['song_title'] = song_title
                else:
                    song_info['song_title'] = ''

                tracklist.append(song_info)

    hearts_params = {'contsIds': album_id}
    hearts_tags = requests.get(heart_url, params=hearts_params, headers=melon_headers).text
    hearts_json = json.loads(hearts_tags)
    hearts = int(hearts_json['contsLike'][0]['SUMMCNT'])

    grades_params = {'albumId': album_id}
    grades_tags = requests.get(grades_url, params=grades_params, headers=melon_headers).text
    grades_json = json.loads(grades_tags)
    grades_people = int(grades_json['infoGrade']['PTCPNMPRCO'])
    grades_rating = float(grades_json['infoGrade']['TOTAVRGSCORE'])
    grades_avg = float(grades_json['infoGrade']['AVRGSCORERATING'])

    cmt_params = {
        'chnlSeq' : '102',
        'contsRefValue' : album_id,
        'startIndex' : '1',
        'pageSize' : '1000',
    }
    cmt_tags = requests.get(cmt_url, params=cmt_params, headers=melon_headers).text
    cmt_json = json.loads(cmt_tags)

    for comment in cmt_json['result']['cmtList']:
        print(f"Comment Data : {comment}")
        cmt_content = comment['cmtInfo']['cmtCont']
        cmt_date = comment['cmtInfo']['dsplyRegDate']
        member_nickname = comment['memberInfo']['memberNickname']
        member_key = comment['memberInfo']['memberKey']
    
        
        # 필요한 정보 저장
        comments.append({
            'user_id': member_key,
            'user_name': member_nickname,
            'comment': cmt_content,
            'createdAt': cmt_date,
            'album_id': album_id,
            'album_title': album_title,
            'released_date': release_date
        })

    result = {
        'id': album_id,
        'album_id': album_id,
        'album_title': album_title,
        'album_image_url': album_image_url,
        'artist_name': artist_name,
        'artist_id': artist_id,
        'released_date': release_date,
        'album_type': album_type,
        'track_count': song_count,
        'tracks': tracklist,
        'genre': genre,
        'production': publisher,
        'distribution': agency,
        'likes': hearts,
        'rating_count': grades_people,
        'rating' : grades_rating,
        'rating_ppp' : grades_avg,
        'comments_count' : len(comments),
        'comments': comments
    }
    
    return result

def get_artist_data(artist_id):
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
                if song_id_match:
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
    followers_url = 'https://www.melon.com/artist/getArtistFanNTemper.json?artistId=' + artist_id
    artist_stars_api = requests.get(followers_url, headers=melon_headers).text
    artist_stars_json = json.loads(artist_stars_api)
    followers = artist_stars_json['fanInfo']['SUMMCNT']
    result['followers'] = followers
        
    return result

def get_target_artist_albums_list(artist_id):
    # MelOn Headers
    melon_headers = {
        'Referer': 'https://www.melon.com/artist/timeline.htm?artistId=' + artist_id,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
    }

    # Paging URL API
    paging_url = 'https://www.melon.com/artist/albumPaging.htm'
    paging_query = {
        'startIndex': '1',
        'pageSize': '2000',
        'orderBy': 'ISSUE_DATE',
        'artistId': artist_id
    }

    paging_albums = requests.get(paging_url, params=paging_query, headers=melon_headers).text
    paging_response = BeautifulSoup(paging_albums, 'html.parser')

    album_ids = [
        re.search(r"goAlbumDetail\('(\d+)'\)", link['href']).group(1)
        for link in paging_response.find_all("a", href=re.compile(r"javascript:melon\.link\.goAlbumDetail\('\d+'\)"))
    ]

    return album_ids

def get_songs_data(tracks, album_data):
    album_id = album_data['album_id']

    song_url = f'https://www.melon.com/song/detail.htm'
    hearts_url = 'https://www.melon.com/commonlike/getSongLike.json'
    

    melon_headers = {
        'Referer': f'https://www.melon.com/album/detail.htm?albumId={album_id}',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.134 Safari/537.36',
    }

    results = []
    for track_data in tracks:
        track = {}
        song_id = track_data['song_id']

        track['id'] = song_id
        track['song_id'] = song_id
        track['album_id'] = album_id
        track['album_title'] = album_data['album_title']
        track['artist_name'] = album_data['artist_name']
        track['artist_id'] = album_data['artist_id']
        track['released_date'] = album_data['released_date']

        detail_params = {'songId': song_id}
        response = requests.get(song_url, params=detail_params, headers=melon_headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        song_info_section = soup.find('div', class_='section_info')
        if not song_info_section:
            logger.error(f"song_info_section not found for song ID {song_id}")
            return {}
        
        song_name_tag = song_info_section.find('div', class_='song_name')
        if song_name_tag:
            song_title = song_name_tag.get_text(strip=True).replace('곡명', '').strip()
            track['song_title'] = song_title

        meta_info = {}
        meta_dl = song_info_section.find('dl', class_='list')
        if meta_dl:
            dt_tags = meta_dl.find_all('dt')
            dd_tags = meta_dl.find_all('dd')
            for dt, dd in zip(dt_tags, dd_tags):
                key = dt.get_text(strip=True)
                value = dd.get_text(strip=True)
                key_map = {
                    '앨범': 'album',
                    '발매일': 'release_date',
                    '장르': 'genre',
                    'FLAC': 'flac_info',
                }
                eng_key = key_map.get(key, key)
                if eng_key == 'genre':
                    value = value.split(", ")  # Convert genre to list
                meta_info[eng_key] = value
        track.update(meta_info)

        lyrics_section = soup.find('div', id='lyricArea')
        if lyrics_section:
            lyrics = lyrics_section.get_text("\n", strip=True)
            track['lyrics'] = lyrics

        creators_section = soup.find('div', class_='section_prdcr')
        if creators_section:
            creators = []
            creator_list = creators_section.find_all('li')
            for creator in creator_list:
                creator_type = creator.find('span', class_='type').get_text(strip=True)
                creator_name_tag = creator.find('a', class_='artist_name')
                if creator_name_tag:
                    creator_name = creator_name_tag.get_text(strip=True)
                    creator_id_match = re.search(r"goArtistDetail\((\d+)\)", creator_name_tag.get('href', ''))
                    creator_id = creator_id_match.group(1) if creator_id_match else None
                    creators.append({
                        'name': creator_name,
                        'type': creator_type,
                        'id': creator_id
                    })
            track['creators'] = creators

        videos_section = soup.find('div', class_='section_movie')
        if videos_section:
            videos = []
            video_list = videos_section.find_all('li')
            for video in video_list:
                video_title_tag = video.find('a', class_='album_name')
                video_artist_tag = video.find('a', class_='artist_name')
                video_duration = video.find('span', class_='time').get_text(strip=True)
                if video_title_tag and video_artist_tag:
                    video_title = video_title_tag.get_text(strip=True)
                    video_artist = video_artist_tag.get_text(strip=True)
                    video_id_match = re.search(r"goMvDetail\('\d+', '(\d+)'\)", video_title_tag.get('href', ''))
                    video_id = video_id_match.group(1) if video_id_match else None
                    videos.append({
                        'title': video_title,
                        'artist': video_artist,
                        'duration': video_duration,
                        'id': video_id
                    })
            track['related_videos'] = videos


        streaming_url = f'https://m2.melon.com/m6/chart/streaming/card.json?cpId=AS40&cpKey=14LNC3&appVer=6.0.0&songId={song_id}'
        api_request = requests.get(streaming_url, headers=melon_headers).text
        api_response = json.loads(api_request)['response']

        if api_response['VIEWTYPE'] == "2":
            if api_response['STREAMUSER'] != '':
                listeners = string_to_integer(api_response['STREAMUSER'])
                track['listeners'] = listeners
            if api_response['STREAMCOUNT'] != '':
                streams = string_to_integer(api_response['STREAMCOUNT'])
                track['streams'] = streams

        hearts_params = {'contsIds': song_id}
        hearts_song = requests.get(hearts_url, params=hearts_params, headers=melon_headers).text
        hearts_song_json = json.loads(hearts_song)
        hearts = hearts_song_json['contsLike'][0]['SUMMCNT']
        track['hearts'] = hearts

        results.append(track)

    return results

