import instaloader
import os
import sys

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

INSTAGRAM_ACCOUNT_USERNAME=os.getenv("INSTAGRAM_ACCOUNT_USERNAME")
INSTAGRAM_ACCOUNT_PASSWORD=os.getenv("INSTAGRAM_ACCOUNT_PASSWORD")

TARGET_INSTAGRAM_ACCOUNT=os.getenv("INSTAGRAM_ACCOUNT")

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET = 'FB_instagram'
def fb_instagram():
    load_data = check_record(DATA_TARGET)
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    # 인스턴스 생성
    L = instaloader.Instaloader()
    L.login(user=INSTAGRAM_ACCOUNT_USERNAME, passwd=INSTAGRAM_ACCOUNT_PASSWORD)

    target_account = TARGET_INSTAGRAM_ACCOUNT

    profile = instaloader.Profile.from_username(L.context, target_account)
    print(f'Profile : {profile}')

    try:
        result = profile.followers
    except AttributeError:
        result = 0

    save_record(DATA_TARGET, result)
    return result