##### Valuation/MNV/MOV/PFV/AV/APV/APV_spotify.py #####

import os
import spotipy
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

SPOTIFY_CLIENT_ID=os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET=os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_ID=os.getenv("SPOTIFY_ID")

from spotipy.oauth2 import SpotifyClientCredentials
auth_manager = SpotifyClientCredentials(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)

def get_artist_data():
    result = sp.artist(SPOTIFY_ID)
    return result

def get_album_ids():
    albums_data = sp.artist_albums(SPOTIFY_ID)
    albums = albums_data.get('items')
    results = []
    for album in albums:
        album_id = album.get('id')
        results.append(album_id)
    
    return results

def get_albums_data(album_ids):
    album_data = sp.albums(album_ids)
    albums = album_data.get('albums')
    results = []
    all_track_ids = []
    for album in albums:
        album_type = album.get('album_type')
        total_tracks = album.get('total_tracks')
        album_id = album.get('id')
        album_image = album.get('images')[0].get('url')
        album_title = album.get('name')
        release_date = album.get('release_date')
        tracks_raw = album.get('tracks').get('items')
        track_ids = []
        for track_raw in tracks_raw:
            track_id = track_raw.get('id')
            track_ids.append(track_id)
            all_track_ids.append(track_id)

        popularity = album.get('popularity')
        result = {
            'album_id' : album_id,
            'album_title' : album_title,
            'album_type' : album_type,
            'total_tracks' : total_tracks,
            'release_date' : release_date,
            'album_image' : album_image,
            'popularity' : popularity,
            'tracks': track_ids
        }
        results.append(result)
    
    return results, all_track_ids

def get_tracks_data(track_ids):
    tracks_data_raw = sp.tracks(track_ids)
    tracks_data = tracks_data_raw.get('tracks')
    results = []
    for track_data in tracks_data:
        track_id = track_data.get('id')
        track_name = track_data.get('name')
        popularity = track_data.get('popularity')
        track_number = track_data.get('track_number')
        duration_ms = track_data.get('duration_ms')
        result = {
            'track_id' : track_id,
            'track_name' : track_name,
            'track_number' : track_number,
            'duration_ms' : duration_ms,
            'popularity' : popularity,
        }
        results.append(result)
    
    return results

def spotify_album_data():
    artist_data = get_artist_data()
    artist_followers = artist_data.get('followers',0).get('total', 0)
    album_ids = get_album_ids()
    albums, track_ids = get_albums_data(album_ids)
    tracks = get_tracks_data(track_ids)

    tracks_dict = {track['track_id']: track for track in tracks}
    for album in albums:
        album['tracks'] = [
            tracks_dict[track_id] if track_id in tracks_dict else track_id
            for track_id in album['tracks']
        ]

    return artist_followers, albums