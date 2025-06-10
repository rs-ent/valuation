##### Valuation/MNV/MOV/PCV/MCV/MCV_main.py #####
'''
MCV_main.py는 YouTube, Twitter, Instagram에서 수집한 미디어 전환 가치를 통합하여 최종 MCV를 산출함
mcv_youtube 모듈은 YouTube API를 통해 아티스트 채널의 비디오 데이터를 수집하고, 조회수, 좋아요, 댓글, 재생시간 등 지표를 기반으로 영상별 MCV를 계산함
mcv_twitter 모듈은 Tweepy API를 사용하여 아티스트의 트윗 데이터를 수집하고, 트윗의 좋아요, 리트윗, 답글, 인용 수를 반영하여 Twitter MCV를 산출함
mcv_instagram 모듈은 인스타그램 게시물의 좋아요와 댓글 데이터를 수집하고, 게시물 게시 시점을 고려한 할인율을 적용하여 Instagram MCV를 계산함
각 모듈에서 산출된 원시 MCV 값은 설정된 가중치(YOUTUBE_WEIGHT, TWITTER_WEIGHT, INSTAGRAM_WEIGHT)와 곱해져 최종 MCV에 반영됨
수집된 데이터는 참여도, 효율성, 시계열 할인 요인 등 경제적 가치 산출에 필요한 정량적 지표를 포함함
이 데이터들은 표준화 및 모델링 과정을 거쳐 플랫폼별 미디어 가치를 효과적으로 평가함
최종 MCV는 Firebase에 저장되어 후속 분석 및 보고에 활용됨
모듈화된 설계로 유지보수와 확장성이 보장됨
'''
import os
import sys
from datetime import datetime
import pandas as pd
import statsmodels.api as sm

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

from Valuation.MNV.MOV.PCV.MCV.MCV_youtube import mcv_youtube
from Valuation.MNV.MOV.PCV.MCV.MCV_twitter import mcv_twitter
from Valuation.MNV.MOV.PCV.MCV.MCV_instagram import mcv_instagram
from Valuation.utils.weights import Weights
YOUTUBE_WEIGHT = Weights.PCV.MCV_YOUTUBE
TWITTER_WEIGHT = Weights.PCV.MCV_TWITTER
INSTAGRAM_WEIGHT = Weights.PCV.MCV_INSTAGRAM

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='MCV'

def mcv():
    load_data = check_record(DATA_TARGET)
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    youtube_data = mcv_youtube()
    twitter_data = mcv_twitter()
    instagram_data = mcv_instagram()
    mcv_value = (youtube_data.get('mcv_youtube') * YOUTUBE_WEIGHT) + (twitter_data.get('mcv_twitter') * TWITTER_WEIGHT) + (instagram_data.get('mcv_instagram') * INSTAGRAM_WEIGHT)

    result = {
        'mcv_youtube_raw': youtube_data.get('mcv_youtube'),
        'youtube_weight': YOUTUBE_WEIGHT,
        'mcv_youtube': youtube_data.get('mcv_youtube') * YOUTUBE_WEIGHT,
        'mcv_twitter_raw': twitter_data.get('mcv_twitter'),
        'twitter_weight': TWITTER_WEIGHT,
        'mcv_twitter': twitter_data.get('mcv_twitter') * TWITTER_WEIGHT,
        'mcv_instagram_raw': instagram_data.get('mcv_instagram'),
        'instagram_weight': INSTAGRAM_WEIGHT,
        'mcv_instagram': instagram_data.get('mcv_instagram') * INSTAGRAM_WEIGHT,
        'mcv': mcv_value
    }

    save_record(DATA_TARGET, result)
    return result