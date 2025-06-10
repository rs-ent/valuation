import requests

import os
import sys

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

TWITTER_ACCOUNT = os.getenv("TWITTER_ACCOUNT")

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_KEY_SECRET = os.getenv("TWITTER_API_KEY_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
TWITTER_CLIENT_ID = os.getenv("TWITTER_CLIENT_ID")
TWITTER_CLIENT_SECRET = os.getenv("TWITTER_CLIENT_SECRET")

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET = 'FB_twitter'
def fb_twitter():
    load_data = check_record(DATA_TARGET)
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    if TWITTER_ACCOUNT:
        url = f"https://api.twitter.com/2/users/by/username/{TWITTER_ACCOUNT}"
        headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
        params = {"user.fields": "public_metrics"}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            user_data = response.json().get('data', {})
            result = user_data.get("public_metrics", {}).get("followers_count", 0)
        else:
            print("Error:", response.status_code, response.json())
            result = 0
    else:
        result = 0

    save_record(DATA_TARGET, result)
    return result