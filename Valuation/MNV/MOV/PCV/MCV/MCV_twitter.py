##### Valuation/MNV/MOV/PCV/MCV/MCV_twitter.py #####

import tweepy
import json
from datetime import datetime, timezone
import os

TWITTER_ACCOUNT = os.getenv("TWITTER_ACCOUNT")

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_KEY_SECRET = os.getenv("TWITTER_API_KEY_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
TWITTER_CLIENT_ID = os.getenv("TWITTER_CLIENT_ID")
TWITTER_CLIENT_SECRET = os.getenv("TWITTER_CLIENT_SECRET")

client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN,
                       consumer_key=TWITTER_API_KEY,
                       consumer_secret=TWITTER_API_KEY_SECRET,
                       access_token=TWITTER_ACCESS_TOKEN,
                       access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
                       wait_on_rate_limit=True)

from Valuation.utils.weights import Weights, Variables
W_LIKES=Weights.PCV.MCV_TWITTER_LIKES
EV_WEIGHT=Weights.PCV.MCV_EV_WEIGHT
W_COMMNETS=Weights.PCV.MCV_TWITTER_COMMENTS
W_RETWEETS=Weights.PCV.MCV_TWITTER_RETWEET
W_QUOTES=Weights.PCV.MCV_TWITTER_QUOTE
WEIGHTS = {
    'likes': W_LIKES,
    'comments': W_COMMNETS,
    'retweets': W_RETWEETS,
    'quotes': W_QUOTES
}
DISCOUNT_RATE=Variables.DISCOUNT_RATE

def get_user_id(username):
    user = client.get_user(username=username)
    if user and user.data:
        return user.data.id
    else:
        raise Exception(f"사용자 @{username}를 찾을 수 없습니다.")
    
def get_all_tweets():
    max_tweets=999
    user_id = get_user_id(TWITTER_ACCOUNT)

    tweets = []
    paginator = tweepy.Paginator(
        client.get_users_tweets,
        id=user_id,
        max_results=100,
        tweet_fields=['created_at', 'public_metrics', 'text'],
        pagination_token=None
    )

    for response in paginator:
        if response.data:
            tweets.extend(response.data)
            print(f"Fetched {len(tweets)} tweets so far...")
            if len(tweets) >= max_tweets:
                break
        else:
            break

    return tweets[:max_tweets]

def tweet_to_dict(tweet):
    user_id = tweet.author_id
    
    entities = tweet.entities or {}

    media_urls = []
    if 'media_keys' in entities:
        media_keys = entities['media_keys']
        media = entities.get('media', [])
        media_urls = [m['url'] for m in media if 'url' in m]
    
    is_retweet = False
    if tweet.referenced_tweets:
        for ref in tweet.referenced_tweets:
            if ref.type == 'retweeted':
                is_retweet = True
                break

    return {
        "id": str(tweet.id),
        "text": tweet.text,
        "created_at": tweet.created_at.isoformat() if tweet.created_at else None,
        "user_id": user_id,
        "retweet_count": tweet.public_metrics.get('retweet_count', 0),
        "like_count": tweet.public_metrics.get('like_count', 0),
        "reply_count": tweet.public_metrics.get('reply_count', 0),
        "quote_count": tweet.public_metrics.get('quote_count', 0),
        "source": tweet.source,
        "lang": tweet.lang,
        "entities": {
            "hashtags": [hashtag['tag'] for hashtag in entities.get('hashtags', [])],
            "urls": [url['expanded_url'] for url in entities.get('urls', [])],
            "user_mentions": [
                {"username": mention['username'], "id": mention['id']}
                for mention in entities.get('mentions', [])
            ],
            "media": media_urls,
        },
        "is_retweet": is_retweet,
    }

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='MCV_twitter'

def mcv_twitter():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'tweets')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    total_mcv = 0
    CURRENT_DATE = datetime.now(timezone.utc)

    if TWITTER_ACCOUNT:
        tweets = get_all_tweets()
        tweets_data = []
        for tweet in tweets:
            L = tweet.public_metrics.get('like_count', 0)
            R = tweet.public_metrics.get('retweet_count', 0)
            C = tweet.public_metrics.get('reply_count', 0)
            Q = tweet.public_metrics.get('quote_count', 0)

            ev = (L * (1 + WEIGHTS['likes']) +
                                C * (1 + WEIGHTS['comments']) +
                                R * (1 + WEIGHTS['retweets']) +
                                Q * (1 + WEIGHTS['quotes']))
            engagement_value = ev ** EV_WEIGHT

            created_at = tweet.created_at
            t = (CURRENT_DATE - created_at).days / 30
            DR = (1 - DISCOUNT_RATE) ** t

            total_mcv += engagement_value * DR
            print(tweet)
            tweet_data = tweet_to_dict(tweet)
            tweet_data['ev'] = ev
            tweet_data['engagement_value'] = engagement_value
            tweet_data['mcv'] = engagement_value * DR
            tweets_data.append(tweet_data)

        result = {
            'mcv_twitter': total_mcv,
            'weights': WEIGHTS,
            'tweets': tweets_data
        }
    else:
        result = {
            'mcv_twitter': 0,
            'weights': WEIGHTS,
            'tweets': []
        }

    save_record(DATA_TARGET, result, DATA_TARGET, 'tweets')
    return result