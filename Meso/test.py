from melon_album_data_collector import get_album_data, get_artist_data, get_songs_data, get_target_artist_albums_list
from spotify_songs_data_collector import get_spotify_data

target = '653211'

album_ids = get_target_artist_albums_list(target)
print(album_ids)