import requests

import os
import sys

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='FB_youtube'
def fb_youtube():
    load_data = check_record(DATA_TARGET)
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    result = 0
    if YOUTUBE_CHANNEL_ID and YOUTUBE_API_KEY:
        
        print('DATA UNLOADED')
        url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            "part": "statistics",
            "id": YOUTUBE_CHANNEL_ID,
            "key": YOUTUBE_API_KEY
        }
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json().get("items", [])
            if data:
                result = int(data[0]["statistics"].get("subscriberCount", 0))
        else:
            print("Error:", response.status_code, response.json())
            result = 0

    save_record(DATA_TARGET, result)
    return result
    
