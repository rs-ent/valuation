##### Valuation/MNV/MOV/PFV/AV/UDI/UDI_main.py #####
'''
UDI_main.py의 목표는 Spotify와 Melon에서 수집한 앨범 메트릭스를 통합하여 UDI(Unique Diversity Index)를 산출함
rv, sv, apv 모듈을 통해 판매, 스트리밍, 인기도 데이터를 개별적으로 수집함
normalize_release_date_spotify와 normalize_release_date_melon 함수로 각 플랫폼의 발매일을 표준화함
match_albums_by_release_date 함수로 발매일을 기준으로 Spotify와 Melon 앨범을 매핑함
is_valid_track 함수로 불필요한 인스트루멘탈, 인터루드 등 트랙을 필터링함
combine_metrics 함수에서 각 앨범의 스트림, 청취, 좋아요, 인기도 데이터를 정규화 및 통합함
normalize_values 함수를 활용해 메트릭 데이터의 상대적 크기를 산출함
entropy_ratio와 calculate_normalized_entropy 함수로 각 메트릭의 엔트로피를 계산함
UDI는 네 엔트로피의 평균으로 산출되며 0.5에서 1.0 사이로 제한됨
Firebase에 저장된 결과는 앨범 다양성 분석 및 향후 응용에 활용될 수 있음
'''

from Valuation.MNV.MOV.PFV.AV.RV.RV_main import rv
from Valuation.MNV.MOV.PFV.AV.SV.SV_main import sv
from Valuation.MNV.MOV.PFV.AV.APV.APV_main import apv
import math
import re
import calendar
from datetime import datetime, timedelta
from collections import defaultdict

def normalize_release_date_spotify(release_date_str):
    try:
        return datetime.strptime(release_date_str, '%Y-%m-%d').date()
    except ValueError:
        return None
    
def normalize_release_date_melon(release_date_str):
    try:
        return datetime.strptime(release_date_str, '%Y.%m.%d').date()
    except ValueError:
        return None
    
def parse_release_date(release_date_str, format_str):
    try:
        return datetime.strptime(release_date_str, format_str).date()
    except ValueError:
        return None
    
def get_last_day_of_month(year, month):
    last_day = calendar.monthrange(year, month)[1]
    return last_day
    
def match_albums_by_release_date(spotify_albums, melon_albums):
    mapping = {}

    melon_release_dict = defaultdict(list)
    for album in melon_albums:
        release_date = normalize_release_date_melon(album.get('release_date', ''))
        if release_date:
            melon_release_dict[release_date].append(album)
    
    for sp_album in spotify_albums:
        sp_release_date = normalize_release_date_spotify(sp_album.get('release_date', ''))
        
        if not sp_release_date:
            continue

        melon_albums_on_same_date = melon_release_dict.get(sp_release_date, [])
        if not melon_albums_on_same_date:
            continue

        mapped_melon_album = melon_albums_on_same_date[0]
        mapping[sp_album['album_id']] = mapped_melon_album['album_id']
    
    return mapping

def is_valid_track(track):
    invalid_patterns = [
        r'\binst\.?\b',
        r'\binstrumental\b',
        r'\bintro\b',
        r'\binterlude\b',
        r'\bbonus track\b',
        r'\bversion\b',
        r'\bmix\b',
        r'\bguitar solo\b',
        r'\bdrum solo\b',
        r'\btrack intro\b',
        r'\btrack outro\b',
        r'\bvoiceover\b',
        r'\bkaraoke\b',
        r'\bdub\b',
        r'\bremix\b'
    ]

    title = track.get('track_name', '').lower()
    for pattern in invalid_patterns:
        if re.search(pattern, title):
            return False
    return True

def normalize_values(values):
    max_value = max(values) if values else 0
    return [value / max_value if max_value > 0 else 0 for value in values]
def combine_metrics():
    sv_data = sv()
    apv_data = apv()
    rv_data = rv()

    melon_albums = sv_data.get('albums', [])
    spotify_albums = apv_data.get('albums', [])
    sales_data = rv_data.get('sales_data', [])

    # Melon 앨범 기본값 설정
    for melon_album in melon_albums:
        melon_album['parsed_release_date'] = parse_release_date(melon_album.get('release_date', ''), '%Y.%m.%d')
        melon_album['total_sales'] = 0
        melon_album['discounted_revenue'] = 0
        melon_album['discounted_LAP'] = 0

    # 매출 데이터 기반 Melon 앨범 업데이트
    for sales_entry in sales_data:
        rv_year = sales_entry.get('total_sales_year', '')
        rv_month = sales_entry.get('total_sales_month', '')
        total_sales = sales_entry.get('total_sales', 0)
        discounted_revenue = sales_entry.get('discounted_revenue', 0)
        discounted_LAP = sales_entry.get('discounted_LAP', 0)

        if not rv_year or not rv_month:
            continue

        last_day = get_last_day_of_month(int(rv_year), int(rv_month))
        sales_date_str = f"{rv_year}-{rv_month}-{last_day}"
        sales_date = parse_release_date(sales_date_str, '%Y-%m-%d')

        if not sales_date:
            continue

        max_difference = timedelta(days=110)

        candidate_albums = []
        for album in melon_albums:
            if album['parsed_release_date'] and (0 < (sales_date - album['parsed_release_date']).days <= 110):
                candidate_albums.append(album)

        if candidate_albums:
            closest_album = min(candidate_albums, key=lambda x: (sales_date - x['parsed_release_date']).days)
            closest_album['total_sales'] = total_sales
            closest_album['discounted_revenue'] = discounted_revenue
            closest_album['discounted_LAP'] = discounted_LAP

    album_mapping = match_albums_by_release_date(spotify_albums=spotify_albums, melon_albums=melon_albums)
    combined_metrics = []

    melon_dict = {album['album_id']: album for album in melon_albums}

    # 매핑된 Spotify 앨범 처리
    for sp_album in spotify_albums:
        sp_album_id = sp_album['album_id']
        if sp_album_id in album_mapping:
            ml_album_id = album_mapping[sp_album_id]
            melon_album = melon_dict.get(ml_album_id, {})

            valid_melon_tracks = [track for track in melon_album.get('tracks', []) if is_valid_track(track)]
            valid_spotify_tracks = [track for track in sp_album.get('tracks', []) if is_valid_track(track)]

            streams = [track.get('melon_streams', 0) for track in valid_melon_tracks]
            all_streams = [track.get('melon_streams', 0) for track in melon_album.get('tracks', [])]
            listeners = [track.get('melon_listeners', 0) for track in valid_melon_tracks]
            all_listeners = [track.get('melon_listeners', 0) for track in melon_album.get('tracks', [])]
            likes = [track.get('melon_likes', 0) for track in valid_melon_tracks]
            all_likes = [track.get('melon_likes', 0) for track in melon_album.get('tracks', [])]
            popularity = [track.get('popularity', 0) for track in valid_spotify_tracks]
            all_popularity = [track.get('popularity', 0) for track in sp_album.get('tracks', [])]
            album_popularity = sp_album.get('popularity', 0)

            normalized_streams = normalize_values(streams)
            normalized_listeners = normalize_values(listeners)
            normalized_likes = normalize_values(likes)
            normalized_popularity = normalize_values(popularity)

            total_sales = melon_album.get('total_sales', 0)
            discounted_LAP = melon_album.get('discounted_LAP', 0)
            discounted_revenue = melon_album.get('discounted_revenue', 0)

            total_tracks = (len(valid_melon_tracks) + len(valid_spotify_tracks)) / 2 if (valid_melon_tracks or valid_spotify_tracks) else 0

            metrics = {
                'id':sp_album_id,
                'streams_data': streams,
                'streams': normalized_streams,
                'streams_without_filter': all_streams,
                'listeners_data': listeners,
                'listeners': normalized_listeners,
                'listeners_without_filter': all_listeners,
                'likes_data': likes,
                'likes': normalized_likes,
                'likes_without_filter': all_likes,
                'popularity_data': popularity,
                'popularity': normalized_popularity,
                'popularity_without_filter': all_popularity,
                'album_popularity': album_popularity,
                'total_sales': total_sales,
                'discounted_retail_LAP': discounted_LAP,
                'discounted_retail_revenue': discounted_revenue,
                'spotify_album_id': sp_album_id,
                'melon_album_id': ml_album_id,
                'release_date': melon_album.get('release_date'),
                'album_title': melon_album.get('album_title'),
                'img_url': melon_album.get('img_url'),
                'total_tracks': total_tracks,
            }
        else:
            # Spotify만 있는 앨범 처리
            metrics = {
                'id': sp_album_id,
                'spotify_album_id': sp_album_id,
                'release_date': sp_album.get('release_date'),
                'album_title': sp_album.get('album_title'),
                'popularity_data': [track.get('popularity', 0) for track in sp_album.get('tracks', [])],
                'album_popularity': sp_album.get('popularity', 0),
                # Melon 정보는 없으므로 0이나 빈 값으로 처리
                'streams_data': [],
                'listeners_data': [],
                'likes_data': [],
                'total_sales': 0,
                'discounted_retail_LAP': 0,
                'discounted_retail_revenue': 0,
                'total_tracks': len([track for track in sp_album.get('tracks', []) if is_valid_track(track)])
            }
            
        combined_metrics.append(metrics)

    # Melon만 있는 앨범 처리 (Spotify와 매핑되지 않은 앨범)
    mapped_melon_ids = set(album_mapping.values())
    for ml_album in melon_albums:
        ml_album_id = ml_album['album_id']
        if ml_album_id not in mapped_melon_ids:
            # Melon만 있는 앨범
            valid_melon_tracks = [track for track in ml_album.get('tracks', []) if is_valid_track(track)]
            streams = [track.get('melon_streams', 0) for track in valid_melon_tracks]
            listeners = [track.get('melon_listeners', 0) for track in valid_melon_tracks]
            likes = [track.get('melon_likes', 0) for track in valid_melon_tracks]

            normalized_streams = normalize_values(streams)
            normalized_listeners = normalize_values(listeners)
            normalized_likes = normalize_values(likes)

            metrics = {
                'id': ml_album_id,
                'melon_album_id': ml_album_id,
                'release_date': ml_album.get('release_date'),
                'album_title': ml_album.get('album_title'),
                'img_url': ml_album.get('img_url'),
                'streams_data': streams,
                'streams': normalized_streams,
                'listeners_data': listeners,
                'listeners': normalized_listeners,
                'likes_data': likes,
                'likes': normalized_likes,
                'total_sales': ml_album.get('total_sales', 0),
                'discounted_retail_LAP': ml_album.get('discounted_LAP', 0),
                'discounted_retail_revenue': ml_album.get('discounted_revenue', 0),
                'total_tracks': len(valid_melon_tracks),
                # Spotify 정보 없음 -> popularity 관련은 빈 값 처리
                'popularity_data': [],
                'popularity': [],
                'popularity_without_filter': [],
                'album_popularity': 0
            }

        combined_metrics.append(metrics)

    return combined_metrics

def entropy_ratio(ratios):
    return -sum(r * math.log(r + 1e-9) for r in ratios if r > 0)

def calculate_normalized_entropy(values):
    N = len(values)
    if N < 1:
        return 0.0
    if N == 1:
        return 1.0
    
    total = sum(values)
    if total == 0:
        return 0.0
    
    ratios = [value / total for value in values]

    H = entropy_ratio(ratios)
    H_max = math.log(N)
    H_normalized = H / H_max if H_max > 0 else 0.0
    return H_normalized

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='UDI'

def udi():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'combined_metrics')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    combined_metrics = combine_metrics()
    
    udi = {}
    for metrics in combined_metrics:
        album_id = metrics.get('id', '')
        streams = metrics.get('streams', [])
        listeners = metrics.get('listeners', [])
        likes = metrics.get('likes', [])
        popularity = metrics.get('popularity', [])

        H_streams = calculate_normalized_entropy(streams)
        H_listeners = calculate_normalized_entropy(listeners)
        H_likes = calculate_normalized_entropy(likes)
        H_popularity = calculate_normalized_entropy(popularity)

        UDI = (H_streams + H_listeners + H_likes + H_popularity) / 4
        UDI = max(min(UDI, 1.0), 0.5)

        udi[album_id] = UDI

    result = {
        'combined_metrics': combined_metrics,
        'udi': udi
    }


    save_record(DATA_TARGET, result, DATA_TARGET, 'combined_metrics')
    return result