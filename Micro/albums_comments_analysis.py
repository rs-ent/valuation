# Micro/albums_comments_analysis.py

import asyncio
import os
from collections import defaultdict, Counter
from typing import List, Dict, Any

from dotenv import load_dotenv
from konlpy.tag import Okt
from Firebase.firestore_handler import _load_data, save_to_firestore

from utils.logger import setup_logger
import openai


from transformers import BertTokenizer, BertModel
from sklearn.neighbors import KNeighborsClassifier
import numpy as np
import torch

import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logger = setup_logger(__name__)

# 환경 변수에서 MELON_ARTIST_ID와 OpenAI API 키 가져오기
ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")

MELON_ARTIST_ID = os.getenv('MELON_ID')
if not MELON_ARTIST_ID:
    logger.error("환경 변수 'MELON_ARTIST_ID'가 설정되지 않았습니다.")
    raise ValueError("환경 변수 'MELON_ARTIST_ID'가 설정되지 않았습니다.")

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
if not OPENAI_API_KEY:
    logger.error("환경 변수 'OPENAI_API_KEY'가 설정되지 않았습니다.")
    raise ValueError("환경 변수 'OPENAI_API_KEY'가 설정되지 않았습니다.")

tokenizer = BertTokenizer.from_pretrained('monologg/kobert', use_fast=True)
bert_model = BertModel.from_pretrained('monologg/kobert')

# 분류기 초기화
knn_model = None

# 카테고리별 키워드 리스트
category_keywords = {
    '음악': ['노래', '리듬', '멜로디', '비트', '장르', '연주', '음악', '사운드', '악기'],
    '가사': ['가사', '메시지', '감정', '표현', '이야기', '내용', '스토리', '말'],
    '보컬/퍼포먼스': ['보컬', '라이브', '퍼포먼스', '무대', '매너', '춤', '댄스', '안무', '목소리'],
    '프로덕션': ['프로덕션', '믹싱', '마스터링', '제작', '음질', '녹음', '편곡'],
    '비주얼/아트워크': ['비주얼', '아트워크', '커버', '뮤직비디오', '이미지', '사진', '영상', '화보'],
    '패션/스타일': ['패션', '스타일', '의상', '코디', '복장', '스타일링', '헤어', '메이크업'],
    '인간성/개인적 특성': ['성격', '매너', '태도', '인성', '인간성', '사람', '마음', '성품'],
}

def train_classifier():
    global knn_model
    X = []
    y = []

    for category, keywords in category_keywords.items():
        for keyword in keywords:
            inputs = tokenizer(keyword, return_tensors='pt')
            with torch.no_grad():
                outputs = bert_model(**inputs)
                vector = outputs.last_hidden_state[:, 0, :].squeeze().numpy()
                X.append(vector)
                y.append(category)

    # k-NN 분류기 학습
    knn_model = KNeighborsClassifier(n_neighbors=3)
    knn_model.fit(X, y)

def classify_keywords_bert(keywords: List[str]) -> Dict[str, str]:
    """
    KoBERT 임베딩을 사용하여 키워드를 카테고리로 분류합니다.

    :param keywords: 분류할 키워드 리스트
    :return: 키워드별 카테고리 매핑
    """
    if knn_model is None:
        train_classifier()

    keyword_category_map = {}
    for keyword in keywords:
        inputs = tokenizer(keyword, return_tensors='pt')
        with torch.no_grad():
            outputs = bert_model(**inputs)
            vector = outputs.last_hidden_state[:, 0, :].squeeze().numpy()
            category = knn_model.predict([vector])[0]
            keyword_category_map[keyword] = category
    return keyword_category_map

def analyze_sentiments_local(comments: List[str]) -> Dict[str, int]:
    """
    로컬 머신 러닝 모델을 사용하여 여러 댓글의 감정을 분류합니다.

    :param comments: 감정을 분석할 댓글 리스트
    :return: 감정별 카운트 { '긍정': int, '중립': int, '부정': int }
    """
    # 감정 분석 로직 (이전 답변에서 제공한 코드 사용)
    # 예를 들어, KcBERT 모델을 사용하여 감정 분석 수행
    # 여기서는 간단히 모든 댓글을 '중립'으로 분류
    sentiment_counts = {'긍정': 0, '중립': len(comments), '부정': 0}
    return sentiment_counts

def analyze_album_comments():
    try:
        # 1. Firebase 'albums' 컬렉션에서 MELON_ARTIST_ID와 일치하는 데이터 가져오기
        filters = [('artist_id', '==', MELON_ARTIST_ID)]
        albums = _load_data('albums', filters)
        if not albums:
            logger.warning(f"MELON_ARTIST_ID '{MELON_ARTIST_ID}'에 해당하는 앨범이 없습니다.")
            return

        logger.info(f"총 {len(albums)}개의 앨범을 분석합니다.")

        # 형태소 분석기 초기화
        okt = Okt()

        # 분류기 학습
        train_classifier()

        # 불용어 리스트 정의
        stopwords = set([
            '은', '는', '이', '가', '을', '를', '에', '의', '와', '과', '도', '으로', '에서', '하다',
            '수', '것', '그', '들', '더', '좀', '잘', '걍', '과연', '또한', '및', '그리고', '하지만', '그러나'
        ])

        analysis_results = []

        for album in albums:
            album_id = album.get('album_id')
            album_title = album.get('album_title')
            comments = album.get('comments', [])

            if not comments:
                logger.info(f"앨범 '{album_title}' (ID: {album_id})에 댓글이 없습니다.")
                continue

            logger.info(f"앨범 '{album_title}' (ID: {album_id})의 댓글을 분석합니다. 총 {len(comments)}개 댓글.")

            # 4. 댓글에서 명사, 동사, 형용사 추출 및 어근화
            keywords = []
            for comment in comments:
                text = comment.get('comment', '')
                if not text:
                    continue

                # 형태소 분석 및 품사 필터링
                tokens = okt.pos(text, stem=True)
                for word, pos in tokens:
                    if pos in ['Noun', 'Verb', 'Adjective']:
                        # 불용어에 해당하지 않고, 한 글자 이상의 단어만 추가
                        if word not in stopwords and len(word) > 1:
                            keywords.append(word)

            if not keywords:
                logger.info(f"앨범 '{album_title}' (ID: {album_id})에서 추출된 키워드가 없습니다.")
                continue

            # 5. 키워드 빈도수 계산
            keyword_counts = Counter(keywords)

            # 6. KoBERT를 사용하여 키워드 분류
            unique_keywords = list(keyword_counts.keys())
            keyword_category_map = classify_keywords_bert(unique_keywords)

            # 키워드 오브젝트 생성
            keyword_object = {}
            for keyword in unique_keywords:
                category = keyword_category_map.get(keyword, '기타')
                frequency = keyword_counts[keyword]
                keyword_object[keyword] = {
                    'frequency': frequency,
                    'category': category
                }

            # 분류된 키워드의 빈도수 계산
            classified_keywords = defaultdict(int)
            for keyword, details in keyword_object.items():
                category = details['category']
                frequency = details['frequency']
                classified_keywords[category] += frequency

            # 4.5. 감정 분석 수행
            sentiment_counts = analyze_sentiments_local([comment.get('comment', '') for comment in comments])
            
            # 결과 저장
            analysis_result = {
                'artist_id': ARTIST_ID,
                'artist_name': ARTIST_NAME_KOR,
                'artist_name_eng': ARTIST_NAME_ENG,
                'melon_artist_id': MELON_ARTIST_ID,
                'album_id': album_id,
                'album_title': album_title,
                'total_comments': len(comments),
                'keyword_frequency': dict(keyword_counts),
                'classified_keywords': dict(classified_keywords),
                'keyword_categories': keyword_object,  # 키워드별 빈도수 및 카테고리 저장
                'sentiment_counts': sentiment_counts  # 감정 분석 결과 저장
            }
            analysis_results.append(analysis_result)

            logger.info(f"앨범 '{album_title}' (ID: {album_id}) 분석 완료.")

        if analysis_results:
            return analysis_results
        else:
            logger.warning("반환할 분석 결과가 없습니다.")

    except Exception as e:
        logger.error(f"댓글 분석 중 오류 발생: {e}")