import os
import requests
import time

from utils.logger import setup_logger
logger = setup_logger(__name__)

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")
YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

BASE_URL = "https://www.googleapis.com/youtube/v3/"

def get_channel_uploads_playlist(channel_id):
    """Fetch the upload playlist ID for a channel."""
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
        logger.error(f"Error fetching uploads playlist: {e}")
        return None

def get_videos_from_playlist(playlist_id):
    """Fetch all video IDs from the channel's uploads playlist."""
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
            time.sleep(0.1)  # Small delay to respect rate limits
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching videos from playlist: {e}")
            break
    return video_ids

def get_video_details(video_ids):
    """Fetch detailed statistics and snippets for a list of videos."""
    videos_data = []
    for i in range(0, len(video_ids), 50):  # API max limit per request is 50 videos
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
            logger.error(f"Error fetching video details: {e}")
            continue
    return videos_data

def get_comments_for_video(video_id):
    """Fetch all comments for a specific video, with artist and video information."""
    comments = []
    url = f"{BASE_URL}commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": 100,
        "key": YOUTUBE_API_KEY
    }
    while True:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [])
            for item in items:
                comment = item["snippet"]["topLevelComment"]["snippet"]
                comments.append({
                    "author": comment.get("authorDisplayName"),
                    "text": comment.get("textDisplay"),
                    "published_at": comment.get("publishedAt"),
                    "like_count": comment.get("likeCount", 0),
                    "artist_id": ARTIST_ID,
                    "artist_name_kor": ARTIST_NAME_KOR,
                    "artist_name_eng": ARTIST_NAME_ENG,
                    "melon_artist_id": MELON_ID,
                    "youtube_channel_id": YOUTUBE_CHANNEL_ID,
                    "video_id": video_id
                })
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params["pageToken"] = next_page_token
            time.sleep(0.1)  # Small delay to respect rate limits
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching comments for video {video_id}: {e}")
            break
    return comments

def get_youtube_videos():
    """Collect YouTube videos and comments."""
    playlist_id = get_channel_uploads_playlist(YOUTUBE_CHANNEL_ID)
    if not playlist_id:
        logger.error("업로드 플레이리스트를 가져오지 못했습니다.")
        return [], []
    
    video_ids = get_videos_from_playlist(playlist_id)
    if not video_ids:
        logger.error("비디오 ID를 가져오지 못했습니다.")
        return [], []
    
    videos = get_video_details(video_ids)
    logger.info(f"총 {len(videos)}개의 비디오 데이터를 수집했습니다.")

    all_comments = []
    for video in videos:
        title = video["snippet"]["title"]
        video_id = video["id"]
        publish_date = video["snippet"]["publishedAt"]
        view_count = video["statistics"].get("viewCount", "N/A")
        like_count = video["statistics"].get("likeCount", "N/A")
        
        logger.info(f"제목: {title}")
        logger.info(f"업로드 날짜: {publish_date}")
        logger.info(f"조회수: {view_count}, 좋아요: {like_count}")
        
        # Fetch and display comments for each video
        video_comments = get_comments_for_video(video_id)
        logger.info(f"{video_id}에서 총 {len(video_comments)}개의 댓글을 가져왔습니다.")
        all_comments.extend(video_comments)

    return videos, all_comments