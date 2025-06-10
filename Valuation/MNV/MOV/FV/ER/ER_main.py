##### Valuation/MNV/MOV/FV/ER/ER_main.py #####
'''
ER_main.py는 아티스트의 소셜 미디어 참여 지표(Engagement Ratio)를 산출하는 모듈임함
openpyxl을 이용하여 Statista 엑셀 파일에서 플랫폼별 사용자 수를 추출함
dotenv를 통해 환경변수에서 ARTIST_ID, ARTIST_NAME_KOR, ARTIST_NAME_ENG, MELON_ID를 로드함
er_youtube, er_twitter, er_instagram 함수를 호출하여 YouTube, Twitter, Instagram의 콘텐츠 및 통계 데이터를 수집함
fb_youtube, fb_twitter, fb_instagram 함수를 통해 각 플랫폼의 팔로워 수를 별도 획득함
플랫폼별 사용자 수를 기반으로 영향력 계수를 산출하여 각 플랫폼 참여율에 적용함
동영상, 트윗, 게시물의 좋아요 수를 팔로워 수로 나누어 개별 참여율을 계산함
각 플랫폼의 참여율에 산출된 영향력을 곱해 최종 참여 지표(ER)를 도출함
여러 플랫폼의 참여 지표를 평균화하여 전체 소셜 미디어 참여율을 산출함
최종 결과는 아티스트 정보와 상세 플랫폼 통계로 구성되어 Firebase에 저장됨
'''

import openpyxl
import os
import sys

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

from Valuation.MNV.MOV.FV.ER.ER_twitter import er_twitter
from Valuation.MNV.MOV.FV.ER.ER_youtube import er_youtube
from Valuation.MNV.MOV.FV.ER.ER_instagram import er_instagram
from Valuation.MNV.MOV.FV.FB.FB_twitter import fb_twitter
from Valuation.MNV.MOV.FV.FB.FB_youtube import fb_youtube
from Valuation.MNV.MOV.FV.FB.FB_instagram import fb_instagram

def load_platform_users(target_spreadsheet, target_worksheet):
    spreadsheet = openpyxl.load_workbook(target_spreadsheet)
    worksheet = spreadsheet[target_worksheet]

    platform_names = [worksheet[f"B{row}"].value for row in range(6, 21)]
    user_counts = [worksheet[f"C{row}"].value for row in range(6, 21)]

    platform_data = dict(zip(platform_names, user_counts))
    print(platform_data)
    return platform_data

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='ER'
def er():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'sub_data')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    print('DATA UNLOADED')
    platform_users = load_platform_users('Valuation/MNV/MOV/FV/FB/statista.xlsx', 'Data')
    
    youtube_user_count = platform_users.get('YouTube', 0.000001)
    twitter_user_count = platform_users.get('X/Twitter', 0.000001)
    instagram_user_count = platform_users.get('Instagram', 0.000001)
    total_user_count = sum(filter(None, platform_users.values()))

    youtube_influence = 1 + (youtube_user_count / total_user_count)
    twitter_influence = 1 + (twitter_user_count / total_user_count)
    instagram_influence = 1 + (instagram_user_count / total_user_count)
    
    youtube_data = er_youtube()
    youtube_followers = fb_youtube()
    youtube_videos = youtube_data['videos']
    youtube_videos_count = len(youtube_videos)

    youtube_engagement = 0
    for video in youtube_videos:
        video_engagement = video['likes'] / youtube_followers
        youtube_engagement += video_engagement
    youtube_fandom_interaction_ratio = youtube_engagement / youtube_videos_count if youtube_videos_count > 0 else 0
    youtube_er = youtube_fandom_interaction_ratio * youtube_influence
    print(f'Youtube Engagement Ratio : {youtube_fandom_interaction_ratio}')

    twitter_data = er_twitter()
    twitter_followers = fb_twitter()
    twitter_tweets = twitter_data['tweets']
    twitter_tweets_count = len(twitter_tweets)

    twitter_engagement = 0
    tweet_count = 0
    for tweet in twitter_tweets:
        if tweet['likes'] and tweet['likes'] > 0:
            tweet_engagement = tweet['likes'] / twitter_followers
            twitter_engagement += tweet_engagement
            tweet_count += 1
    twitter_fandom_interaction_ratio = 0
    if twitter_engagement > 0 and tweet_count > 0 : 
        twitter_fandom_interaction_ratio = twitter_engagement / tweet_count
        
    twitter_er = twitter_fandom_interaction_ratio * twitter_influence
    print(f'Twitter Engagement Ratio : {twitter_fandom_interaction_ratio}')

    instagram_data = er_instagram()
    instagram_followers = fb_instagram()
    instagram_posts = instagram_data['posts']
    instagram_posts_count = len(instagram_posts)

    instagram_engagement = 0
    for post in instagram_posts:
        post_engagement = post['likes'] / instagram_followers
        instagram_engagement += post_engagement
    instagram_fandom_interaction_ratio = instagram_engagement / instagram_posts_count if instagram_posts_count > 0 else 0
    instagram_er = instagram_fandom_interaction_ratio * instagram_influence
    print(f'Instagram Engagement Ratio : {instagram_fandom_interaction_ratio}')

    er = 0
    er_count = 0
    if youtube_er > 0 :
        er += youtube_er
        er_count += 1
    if twitter_er > 0 :
        er += twitter_er
        er_count += 1
    if instagram_er > 0 :
        er += instagram_er
        er_count += 1

    er = er / er_count
    print(f'Engagement Ratio : {er}')

    result = {
        'artist_id': ARTIST_ID,
        'melon_artist_id': MELON_ID,
        'artist_name': ARTIST_NAME_KOR,
        'artist_name_eng': ARTIST_NAME_ENG,

        'sub_data' : [],

        'youtube_videos_count' : youtube_videos_count,
        'youtube_engagement' : youtube_engagement,
        'youtube_fandom_interaction_ratio' : youtube_fandom_interaction_ratio,
        'youtube_influence' : youtube_influence,
        'youtube_er' : youtube_er,

        'twitter_tweets_count' : twitter_tweets_count,
        'twitter_engagement' : twitter_engagement,
        'twitter_fandom_interaction_ratio' : twitter_fandom_interaction_ratio,
        'twitter_influence' : twitter_influence,
        'twitter_er' : twitter_er,

        'instagram_posts_count' : instagram_posts_count,
        'instagram_engagement' : instagram_engagement,
        'instagram_fandom_interaction_ration' : instagram_fandom_interaction_ratio,
        'instagram_influence' : instagram_influence,
        'instagram_er' : instagram_er,

        'er' : er
    }

    result['sub_data'].append({
        'youtube_videos_count' : youtube_videos_count,
        'youtube_engagement' : youtube_engagement,
        'youtube_fandom_interaction_ratio' : youtube_fandom_interaction_ratio,
        'youtube_influence' : youtube_influence,
        'youtube_er' : youtube_er,

        'twitter_tweets_count' : twitter_tweets_count,
        'twitter_engagement' : twitter_engagement,
        'twitter_fandom_interaction_ratio' : twitter_fandom_interaction_ratio,
        'twitter_influence' : twitter_influence,
        'twitter_er' : twitter_er,

        'instagram_posts_count' : instagram_posts_count,
        'instagram_engagement' : instagram_engagement,
        'instagram_fandom_interaction_ration' : instagram_fandom_interaction_ratio,
        'instagram_influence' : instagram_influence,
        'instagram_er' : instagram_er,})

    save_record(DATA_TARGET, result, DATA_TARGET, 'sub_data')
    return result