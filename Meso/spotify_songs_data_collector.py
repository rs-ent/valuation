import os
import asyncio
import logging
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from Firebase.firestore_handler import load_songs, save_to_firestore
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from functools import partial

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
    raise Exception("Spotify Client ID and Secret must be set as environment variables.")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

auth_manager = SpotifyClientCredentials(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)

def search_spotify_track(artist_name: str, song_title: str) -> Optional[Dict[str, Any]]:
    query = f"track:{song_title} artist:{artist_name}"
    try:
        results = sp.search(q=query, type='track', limit=1)
        tracks = results.get('tracks', {}).get('items', [])
        if tracks:
            logger.info(f"Found Spotify track for '{song_title}' by '{artist_name}'.")
            return tracks[0]
        else:
            logger.warning(f"No Spotify track found for '{song_title}' by '{artist_name}'.")
            return None
    except Exception as e:
        logger.error(f"Exception during Spotify track search for '{song_title}' by '{artist_name}': {e}")
        return None
    
def get_audio_features(track_id: str) -> Optional[Dict[str, Any]]:
    try:
        features = sp.audio_features([track_id])[0]
        if features:
            logger.info(f"Retrieved audio features for track ID '{track_id}'.")
            return features
        else:
            logger.warning(f"No audio features found for track ID '{track_id}'.")
            return None
    except Exception as e:
        logger.error(f"Exception during Spotify audio features retrieval for track ID '{track_id}': {e}")
        return None
    
def get_spotify_data(tracks):
    results = []

    for idx, track in enumerate(tracks, start=1):
        artist_name = track.get('artist_name', '').strip()
        song_title = track.get('song_title', '').strip()
        
        if not artist_name or not song_title:
            logger.warning(f"Track {idx} is missing artist_name or song_title. Skipping.")
            continue
        
        try:
            # Spotify 검색 결과 가져오기
            search_result = search_spotify_track(artist_name, song_title)
            
            if not search_result:
                logger.warning(f"No search result for '{song_title}' by '{artist_name}'. Skipping.")
                continue
            
            spotify_id = search_result.get('id', 'N/A')

            # 검색 결과에서 필요한 정보만 추출하여 하나의 result로 정리
            album_info = search_result.get('album', {})
            if not album_info:
                logger.warning(f"No album info for track '{song_title}' by '{artist_name}'.")
            
            # 아티스트 정보 처리
            artists_info = album_info.get('artists', [])
            artists = [
                {
                    'name': artist.get('name', 'N/A'),
                    'spotify_url': artist.get('external_urls', {}).get('spotify', 'N/A'),
                    'spotify_id': artist.get('id', 'N/A')
                }
                for artist in artists_info
            ]
            
            # 이미지 URL 처리
            images = album_info.get('images', [])
            album_image_url = images[0].get('url', 'N/A') if images else 'N/A'
            
            # 트랙 정보
            track_name = search_result.get('name', 'N/A')
            track_number = search_result.get('track_number', 0)
            duration_ms = search_result.get('duration_ms', 0)
            track_spotify_url = search_result.get('external_urls', {}).get('spotify', 'N/A')
            preview_url = search_result.get('preview_url', 'N/A')
            isrc = search_result.get('external_ids', {}).get('isrc', 'N/A')
            popularity = search_result.get('popularity', 0)
            
            # 앨범 정보
            album_name = album_info.get('name', 'N/A')
            release_date = album_info.get('release_date', 'N/A')
            total_tracks = album_info.get('total_tracks', 0)
            album_spotify_url = album_info.get('external_urls', {}).get('spotify', 'N/A')
            
            result = {
                'id': track['id'],
                'spotify_id': spotify_id,
                'melon_id': track['song_id'],

                'album_name': album_name,
                'album_release_date': release_date,
                'album_total_tracks': total_tracks,
                'album_spotify_url': album_spotify_url,
                'album_image_url': album_image_url,
                
                'artists': artists,
                
                'track_name': track_name,
                'track_number': track_number,
                'track_duration_ms': duration_ms,
                'track_spotify_url': track_spotify_url,
                'track_preview_url': preview_url,
                'track_isrc': isrc,
                'track_popularity': popularity
            }

            features = get_audio_features(spotify_id)
            result['features'] = features
            
            results.append(result)
        
        except Exception as e:
            logger.error(f"Error processing track '{song_title}' by '{artist_name}': {e}")
            continue

    return results

def get_spotify_data_with_melon_data(tracks):
    results = []

    for idx, track in enumerate(tracks, start=1):
        artist_name = track.get('artist_name', '').strip()
        song_title = track.get('song_title', '').strip()
        
        if not artist_name or not song_title:
            logger.warning(f"Track {idx} is missing artist_name or song_title. Skipping.")
            continue
        
        try:
            # Spotify 검색 결과 가져오기
            search_result = search_spotify_track(artist_name, song_title)
            
            if not search_result:
                logger.warning(f"No search result for '{song_title}' by '{artist_name}'. Skipping.")
                continue
            
            # Spotify 트랙 ID
            spotify_id = search_result.get('id', 'N/A')
            track['spotify_id'] = spotify_id

            # 검색 결과에서 필요한 정보만 추출하여 Melon 데이터에 추가
            album_info = search_result.get('album', {})
            if not album_info:
                logger.warning(f"No album info for track '{song_title}' by '{artist_name}'.")

            # 아티스트 정보 처리
            artists_info = album_info.get('artists', [])
            spotify_artists = [
                {
                    'spotify_name': artist.get('name', 'N/A'),
                    'spotify_url': artist.get('external_urls', {}).get('spotify', 'N/A'),
                    'spotify_id': artist.get('id', 'N/A')
                }
                for artist in artists_info
            ]
            
            # 이미지 URL 처리
            images = album_info.get('images', [])
            spotify_album_image_url = images[0].get('url', 'N/A') if images else 'N/A'
            
            # 트랙 정보 추가
            spotify_track_name = search_result.get('name', 'N/A')
            spotify_track_number = search_result.get('track_number', 0)
            spotify_duration_ms = search_result.get('duration_ms', 0)
            spotify_track_url = search_result.get('external_urls', {}).get('spotify', 'N/A')
            spotify_preview_url = search_result.get('preview_url', 'N/A')
            spotify_isrc = search_result.get('external_ids', {}).get('isrc', 'N/A')
            spotify_popularity = search_result.get('popularity', 0)
            
            # Melon 데이터 업데이트
            track.update({
                'spotify_album_name': album_info.get('name', 'N/A'),
                'spotify_album_release_date': album_info.get('release_date', 'N/A'),
                'spotify_album_total_tracks': album_info.get('total_tracks', 0),
                'spotify_album_url': album_info.get('external_urls', {}).get('spotify', 'N/A'),
                'spotify_album_image_url': spotify_album_image_url,
                'spotify_artists': spotify_artists,
                'spotify_track_name': spotify_track_name,
                'spotify_track_number': spotify_track_number,
                'spotify_track_duration_ms': spotify_duration_ms,
                'spotify_track_url': spotify_track_url,
                'spotify_preview_url': spotify_preview_url,
                'spotify_isrc': spotify_isrc,
                'spotify_popularity': spotify_popularity
            })

            # 오디오 피처 가져오기
            spotify_features = get_audio_features(spotify_id)
            track['spotify_audio_features'] = spotify_features

            results.append(track)
        
        except Exception as e:
            logger.error(f"Error processing track '{song_title}' by '{artist_name}': {e}")
            continue

    return results