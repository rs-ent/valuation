##### Valuation/MNV/MOV/PCV/MCV/MCV_instagram.py #####

from Valuation.utils.weights import Weights, Variables
import pandas as pd
from datetime import datetime, timezone
import math
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

# 인스타그램 MCV 관련 가중치 (예시)
W_LIKES = Weights.PCV.MCV_INSTAGRAM_LIKES
W_COMMENTS = Weights.PCV.MCV_INSTAGRAM_COMMENTS
EV_WEIGHT = Weights.PCV.MCV_EV_WEIGHT
DISCOUNT_RATE = Variables.DISCOUNT_RATE

CURRENT_DATE = datetime.now(timezone.utc)

from Valuation.MNV.MOV.FV.ER.ER_instagram import er_instagram
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='MCV_instagram'
def mcv_instagram():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'details')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    if TARGET_INSTAGRAM_ACCOUNT:
        er_instagram_data = er_instagram()
        posts = er_instagram_data.get('posts', [])
        if not posts:
            print("No Instagram posts data available.")
            result = {
                'mcv_instagram': 0,
                'details': []
            }
            save_record(DATA_TARGET, result)
            return result
        
        total_mcv = 0
        posts_data = []
        for post in posts:
            L = post.get('likes', 0)
            C = post.get('comments', 0)
            
            # engagement value 계산
            # ev = (L * (1+W_LIKES)) + (C * (1+W_COMMENTS))
            ev = (L * (1 + W_LIKES)) + (C * (1 + W_COMMENTS))
            engagement_value = ev ** EV_WEIGHT

            # 게시물 날짜 처리
            post_date_str = post.get('date')
            if not post_date_str:
                # 날짜 없으면 discount 없이 처리
                DR = 1.0
            else:
                # post_date_str이 ISO 8601 형식으로 가정
                post_date = datetime.fromisoformat(post_date_str)
                
                # Ensure both datetimes are timezone-aware or naive for comparison
                if post_date.tzinfo is None:
                    post_date = post_date.replace(tzinfo=timezone.utc)

                t = (CURRENT_DATE - post_date).days / 30
                DR = (1 - DISCOUNT_RATE) ** t if t > 0 else 1.0

            mcv = engagement_value * DR
            total_mcv += mcv

            post_data = {
                'url': post.get('url'),
                'likes': L,
                'comments': C,
                'date': post_date_str,
                'ev': ev,
                'engagement_value': engagement_value,
                'mcv': mcv
            }
            posts_data.append(post_data)

        result = {
            'mcv_instagram': total_mcv,
            'weights': {
                'likes': W_LIKES,
                'comments': W_COMMENTS,
                'ev_weight': EV_WEIGHT
            },
            'details': posts_data
        }
    else:
        result = {
            'mcv_instagram': 0,
            'weights': {
                'likes': W_LIKES,
                'comments': W_COMMENTS,
                'ev_weight': EV_WEIGHT
            },
            'details': []
        }

    save_record(DATA_TARGET, result, DATA_TARGET, 'details')
    return result