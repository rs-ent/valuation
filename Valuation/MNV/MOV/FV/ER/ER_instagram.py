##### Valuation/MNV/MOV/FV/ER/ER_instagram.py #####
'''
Instagram 대상 계정의 게시물 및 상호작용 데이터를 안정적으로 수집함
dotenv 라이브러리로 환경변수에서 로그인 정보와 타깃 계정 정보를 로드함
instaloader 모듈을 활용하여 인스타그램에 로그인 및 프로필 객체를 생성함
fetch_posts_with_backoff 함수는 profile.get_posts() 호출 시 예외 발생에 대비해 백오프 전략을 구현함
요청 실패 시 재시도 로직과 지수적 대기 시간을 적용하여 데이터 요청의 안정성을 확보함
각 게시물의 URL, 좋아요 수, 댓글 수, 작성일을 추출해 리스트에 집계함
프로필의 팔로워 수와 총 게시물 수, 누적 좋아요 및 댓글 수를 계산함
Firebase 캐시 검증 및 저장 로직으로 중복 데이터 수집을 방지함
수집된 결과는 소셜 미디어 영향력 평가 및 데이터 분석에 활용 가능함
'''

import instaloader
import os
import sys
import time

from dotenv import load_dotenv
load_dotenv()

ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")

INSTAGRAM_ACCOUNT_USERNAME=os.getenv("INSTAGRAM_ACCOUNT_USERNAME")
INSTAGRAM_ACCOUNT_PASSWORD=os.getenv("INSTAGRAM_ACCOUNT_PASSWORD")

TARGET_INSTAGRAM_ACCOUNT=os.getenv("INSTAGRAM_ACCOUNT")

def fetch_posts_with_backoff(profile, max_posts=50, max_retries=3):
    """ 
    profile.get_posts()를 사용하여 게시물 데이터를 가져오는 함수.
    요청 실패 시 백오프 전략으로 재시도한다.
    """
    backoff_delay = 1  # 첫 실패 시 1초 대기
    retries = 0

    while True:
        try:
            posts = profile.get_posts()
            post_data_list = []
            total_likes = 0
            total_comments = 0

            for i, post in enumerate(posts, start=1):
                likes = getattr(post, 'likes', 0)
                comments = getattr(post, 'comments', 0)

                total_likes += likes
                total_comments += comments

                post_data_list.append({
                    "url": post.url,
                    "likes": likes,
                    "comments": comments,
                    "date": post.date.isoformat()
                })

                if i == max_posts:
                    break

            return post_data_list, total_likes, total_comments

        except (instaloader.exceptions.ConnectionException,
                instaloader.exceptions.QueryReturnedBadRequestException,
                instaloader.exceptions.RateLimitExceededException) as e:
            # 요청 실패 시 재시도
            retries += 1
            if retries > max_retries:
                # 재시도 횟수 초과 시 예외 또는 기본값 반환
                print("데이터 요청 실패: 최대 재시도 횟수 초과")
                raise e  # 여기서 raise 대신 기본값을 반환할 수도 있음
            else:
                print(f"요청 실패, {retries}회 재시도. {backoff_delay}초 대기 후 재시도...")
                time.sleep(backoff_delay)
                backoff_delay *= 2  # 실패할 때마다 대기 시간 2배

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET = 'ER_instagram'

def er_instagram():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'posts')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    result = {
        "profile": '',
        "followers": 0,
        "total_posts": 0,
        "total_likes": 0,
        "total_comments": 0,
        "posts": []
    }
    
    if TARGET_INSTAGRAM_ACCOUNT:
        
        L = instaloader.Instaloader()
        L.login(user=INSTAGRAM_ACCOUNT_USERNAME, passwd=INSTAGRAM_ACCOUNT_PASSWORD)

        profile = instaloader.Profile.from_username(L.context, TARGET_INSTAGRAM_ACCOUNT)
        followers = getattr(profile, 'followers', 0)

        # 백오프 전략 포함한 게시물 데이터 가져오기
        post_data_list, total_likes, total_comments = fetch_posts_with_backoff(profile, max_posts=3000, max_retries=3)

        result['profile'] = TARGET_INSTAGRAM_ACCOUNT
        result["followers"] = followers
        result["posts"] = post_data_list
        result['total_posts'] = len(post_data_list)
        result['total_likes'] = total_likes
        result["total_comments"] = total_comments

    save_record(DATA_TARGET, result, DATA_TARGET, 'posts')
    return result