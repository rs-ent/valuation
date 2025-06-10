import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')
import re
import logging
import joblib
import numpy as np
import pandas as pd
from typing import Tuple, List
from Firebase.firestore_handler import _load_data
from google.cloud.firestore_v1._helpers import DatetimeWithNanoseconds
import pytz

from konlpy.tag import Okt
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.feature_extraction.text import TfidfVectorizer

# XGBoost와 LightGBM 임포트
from xgboost import XGBRegressor

# TensorFlow 임포트
import tensorflow as tf
from keras import layers, models

# PyTorch 임포트
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# NLTK 데이터 다운로드 설정
import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('punkt')
nltk.download('stopwords')

english_stopwords = set(stopwords.words('english'))

def load_data():
    """
    데이터 로드 함수: 공연 데이터, 앨범 데이터, 노래 데이터, 아티스트 데이터, YouTube 데이터 로드
    Firestore에서 데이터를 로드합니다.
    """
    # Firestore 컬렉션에서 데이터 로드
    performance_data = _load_data('performance')
    albums_data = _load_data('albums')
    songs_data = _load_data('songs')
    artists_data = _load_data('artists')
    youtube_data = _load_data('YoutubeVideos')

    # 로드한 데이터를 DataFrame으로 변환
    performance_df = pd.DataFrame(performance_data)
    albums_df = pd.DataFrame(albums_data)
    songs_df = pd.DataFrame(songs_data)
    artists_df = pd.DataFrame(artists_data)
    youtube_df = pd.DataFrame(youtube_data)
    snippet_df = pd.json_normalize(youtube_df['snippet'])
    youtube_df = pd.concat([youtube_df, snippet_df], axis=1)

    logger.info("Firestore에서 데이터 로딩 완료")
    return performance_df, albums_df, songs_df, artists_df, youtube_df

def preprocess_data(performance_df, albums_df, songs_df, artists_df, youtube_df):
    """
    데이터 전처리 및 피처 엔지니어링 함수
    """
    # 'None' 문자열과 실제 None 값을 모두 NaN으로 변환
    performance_df['revenue'] = performance_df['revenue'].replace(['None', None], np.nan)

    # 쉼표 제거 후 숫자로 변환 시도
    performance_df['revenue'] = performance_df['revenue'].astype(str).str.replace(',', '').replace('nan', np.nan).astype(float)

    # revenue가 0인 경우를 NaN으로 변환
    performance_df['revenue'] = performance_df['revenue'].replace(0, np.nan)

    performance_df['start_period'] = performance_df['start_period'].astype(str).str.rstrip('.')
    performance_df['end_period'] = performance_df['end_period'].astype(str).str.rstrip('.')
    
    # 날짜 형식 변환
    performance_df['start_period'] = pd.to_datetime(performance_df['start_period'], format='%Y.%m.%d', errors='coerce')
    performance_df['end_period'] = pd.to_datetime(performance_df['end_period'], format='%Y.%m.%d', errors='coerce')
    
    albums_df['released_date'] = pd.to_datetime(albums_df['released_date'], errors='coerce')
    songs_df['released_date'] = pd.to_datetime(songs_df['released_date'], errors='coerce')
    youtube_df['publishedAt'] = pd.to_datetime(youtube_df['publishedAt'], errors='coerce')
    youtube_df['publishedAt'] = youtube_df['publishedAt'].dt.tz_localize(None)

    # 공연 기간 계산 (일수)
    performance_df['performance_duration'] = (performance_df['end_period'] - performance_df['start_period']).dt.days + 1

    # 아티스트 정보 병합
    performance_df = performance_df.merge(
        artists_df[['id', 'followers', 'genre']].rename(columns={'id': 'melon_artist_id'}),
        on='melon_artist_id',
        how='left'
    )
    
    # 앨범 메트릭 추가
    performance_df = add_album_metrics(performance_df, albums_df)

    # 노래 메트릭 추가
    performance_df = add_song_metrics(performance_df, songs_df)

    # 장르 피처 추가
    performance_df = add_genre_features(performance_df, songs_df)

    # YouTube 메트릭 추가
    performance_df = add_youtube_metrics(performance_df, youtube_df)

    # 결측치 처리
    performance_df.fillna(0, inplace=True)

    # 텍스트 피처 생성 (title과 location에서 키워드 추출)
    performance_df = extract_text_features(performance_df)

    # 날짜 관련 피처 추가
    performance_df['performance_month'] = performance_df['start_period'].dt.month.fillna(0).astype(int)
    performance_df['performance_day'] = performance_df['start_period'].dt.day.fillna(0).astype(int)
    performance_df['performance_weekday'] = performance_df['start_period'].dt.weekday.fillna(0).astype(int)

    logger.info("데이터 전처리 및 피처 엔지니어링 완료")
    return performance_df

def add_album_metrics(performance_df, albums_df):
    """
    앨범 메트릭을 공연 데이터에 추가하는 함수
    """
    def get_related_album_features(row):
        try:
            artist_albums = albums_df[albums_df['artist_id'] == row['melon_artist_id']]
            related_albums = artist_albums[artist_albums['released_date'] <= row['start_period']]
            if not related_albums.empty:
                recent_album = related_albums.sort_values('released_date', ascending=False).iloc[0]
                return pd.Series({
                    'comments_count': recent_album.get('comments_count', 0),
                    'album_likes': recent_album.get('likes', 0),
                    'album_rating': recent_album.get('rating', 0),
                    'album_rating_count': recent_album.get('rating_count', 0),
                    'track_count': recent_album.get('track_count', 0)
                })
            else:
                return pd.Series({
                    'comments_count': 0,
                    'album_likes': 0,
                    'album_rating': 0,
                    'album_rating_count': 0,
                    'track_count': 0
                })
        except Exception as e:
            logger.error(f"add_album_metrics에서 오류 발생 (행 {row.name}): {e}")
            return pd.Series({
                'comments_count': 0,
                'album_likes': 0,
                'album_rating': 0,
                'album_rating_count': 0,
                'track_count': 0
            })

    album_metrics = performance_df.apply(get_related_album_features, axis=1)
    performance_df = pd.concat([performance_df, album_metrics], axis=1)
    return performance_df

def add_song_metrics(performance_df, songs_df):
    """
    노래 메트릭을 공연 데이터에 추가하는 함수
    """
    def get_song_metrics(row):
        try:
            artist_songs = songs_df[songs_df['artist_id'] == row['melon_artist_id']]
            related_songs = artist_songs[artist_songs['released_date'] <= row['start_period']]
            if not related_songs.empty:
                avg_streams = pd.to_numeric(related_songs['streams'], errors='coerce').mean()
                avg_popularity = pd.to_numeric(related_songs['spotify_popularity'], errors='coerce').mean()
                return pd.Series({
                    'avg_streams': avg_streams if not np.isnan(avg_streams) else 0,
                    'avg_popularity': avg_popularity if not np.isnan(avg_popularity) else 0
                })
            else:
                return pd.Series({'avg_streams': 0, 'avg_popularity': 0})
        except Exception as e:
            logger.error(f"add_song_metrics에서 오류 발생 (행 {row.name}): {e}")
            return pd.Series({'avg_streams': 0, 'avg_popularity': 0})

    song_metrics = performance_df.apply(get_song_metrics, axis=1)
    performance_df = pd.concat([performance_df, song_metrics], axis=1)
    return performance_df

def add_genre_features(performance_df, songs_df):
    """
    노래 데이터에서 장르를 추출하여 공연 데이터에 추가하는 함수
    """
    try:
        songs_df['genre_list'] = songs_df['genre'].apply(
            lambda x: x if isinstance(x, list) else [x] if pd.notnull(x) else []
        )

        def get_artist_genres(row):
            artist_songs = songs_df[songs_df['artist_id'] == row['melon_artist_id']]
            related_songs = artist_songs[artist_songs['released_date'] <= row['start_period']]
            genres = set()
            for genres_list in related_songs['genre_list']:
                genres.update(genres_list)
            return list(genres)

        performance_df['artist_genres'] = performance_df.apply(get_artist_genres, axis=1)

        all_genres = set()
        for genres in performance_df['artist_genres']:
            all_genres.update(genres)

        genre_counts = pd.Series([genre for sublist in performance_df['artist_genres'] for genre in sublist]).value_counts()
        top_genres = genre_counts.head(20).index.tolist()

        for genre in top_genres:
            genre_col_name = 'genre_' + re.sub(r'\W+', '_', genre)
            performance_df[genre_col_name] = performance_df['artist_genres'].apply(lambda x: 1 if genre in x else 0)

        performance_df.drop(['artist_genres'], axis=1, inplace=True)

        return performance_df
    except Exception as e:
        logger.error(f"add_genre_features에서 오류 발생: {e}")
        raise

def add_youtube_metrics(performance_df, youtube_df):
    """
    YouTube 메트릭을 공연 데이터에 추가하는 함수
    """
    def get_youtube_metrics(row):
        try:
            artist_videos = youtube_df[
                (youtube_df['publishedAt'] <= row['start_period'])
            ]
            print(artist_videos)
            if not artist_videos.empty:
                total_views = pd.to_numeric(artist_videos['viewCount'], errors='coerce').sum()
                total_likes = pd.to_numeric(artist_videos['likeCount'], errors='coerce').sum()
                total_comments = pd.to_numeric(artist_videos['commentCount'], errors='coerce').sum()
                video_count = len(artist_videos)
                avg_views = total_views / video_count if video_count > 0 else 0
                avg_likes = total_likes / video_count if video_count > 0 else 0
                avg_comments = total_comments / video_count if video_count > 0 else 0
                latest_video_date = artist_videos['publishedAt'].max()
                days_since_last_video = (row['start_period'] - latest_video_date).days
                return pd.Series({
                    'total_views': total_views,
                    'total_likes': total_likes,
                    'total_comments': total_comments,
                    'avg_views': avg_views,
                    'avg_likes': avg_likes,
                    'avg_comments': avg_comments,
                    'video_count': video_count,
                    'days_since_last_video': days_since_last_video
                })
            else:
                return pd.Series({
                    'total_views': 0,
                    'total_likes': 0,
                    'total_comments': 0,
                    'avg_views': 0,
                    'avg_likes': 0,
                    'avg_comments': 0,
                    'video_count': 0,
                    'days_since_last_video': np.nan
                })
        except Exception as e:
            logger.error(f"add_youtube_metrics에서 오류 발생 (행 {row.name}): {e}")
            return pd.Series({
                'total_views': 0,
                'total_likes': 0,
                'total_comments': 0,
                'avg_views': 0,
                'avg_likes': 0,
                'avg_comments': 0,
                'video_count': 0,
                'days_since_last_video': np.nan
            })

    youtube_metrics = performance_df.apply(get_youtube_metrics, axis=1)
    performance_df = pd.concat([performance_df, youtube_metrics], axis=1)
    return performance_df

def extract_text_features(performance_df):
    """
    title과 location에서 키워드를 추출하여 텍스트 피처를 생성하는 함수
    """
    # 형태소 분석기 초기화
    okt = Okt()

    def tokenize_text(text):
        try:
            text = str(text)
            text = re.sub(r'[^가-힣a-zA-Z\s]', ' ', text)
            text = text.lower()

            korean_tokens = okt.nouns(text)
            english_tokens = [
                word for word in word_tokenize(text)
                if word.isalpha() and word not in english_stopwords
            ]

            all_tokens = korean_tokens + english_tokens

            if not all_tokens:
                all_tokens = ["default_token"]

            return ' '.join(all_tokens)
        except Exception as e:
            logger.error(f"텍스트 토큰화 중 오류 발생: {e}")
            return ''

    performance_df['title_tokens'] = performance_df['title'].apply(tokenize_text)
    performance_df['location_tokens'] = performance_df['location'].apply(tokenize_text)
    performance_df['text_features'] = performance_df['title_tokens'] + ' ' + performance_df['location_tokens']
    performance_df.drop(['title_tokens', 'location_tokens'], axis=1, inplace=True)
    return performance_df

class PerformanceDataset(Dataset):
    def __init__(self, X_numeric, X_text, y=None):
        self.X_numeric = torch.tensor(X_numeric, dtype=torch.float32)
        self.X_text = torch.tensor(X_text, dtype=torch.float32)
        if y is not None:
            if isinstance(y, pd.Series):
                self.y = torch.tensor(y.values, dtype=torch.float32)
            elif isinstance(y, np.ndarray):
                self.y = torch.tensor(y, dtype=torch.float32)
            else:
                self.y = torch.tensor(y, dtype=torch.float32)
        else:
            self.y = None

    def __len__(self):
        return len(self.X_numeric)

    def __getitem__(self, idx):
        if self.y is not None:
            return self.X_numeric[idx], self.X_text[idx], self.y[idx]
        else:
            return self.X_numeric[idx], self.X_text[idx]
        
def train_tensorflow_model(X_train, y_train, X_val, y_val, numeric_features, text_feature):
    # 수치형 데이터 스케일링
    scaler = StandardScaler()
    X_train_numeric = scaler.fit_transform(X_train[numeric_features])
    X_val_numeric = scaler.transform(X_val[numeric_features])

    # 텍스트 데이터 벡터화
    vectorizer = TfidfVectorizer(max_features=1000)
    X_train_text = vectorizer.fit_transform(X_train[text_feature]).toarray()
    X_val_text = vectorizer.transform(X_val[text_feature]).toarray()

    # 모델 정의
    numeric_input = layers.Input(shape=(X_train_numeric.shape[1],), name='numeric_input')
    text_input = layers.Input(shape=(X_train_text.shape[1],), name='text_input')

    numeric_dense = layers.Dense(128, activation='relu')(numeric_input)
    text_dense = layers.Dense(128, activation='relu')(text_input)

    combined = layers.concatenate([numeric_dense, text_dense])
    output = layers.Dense(1)(combined)

    model = models.Model(inputs=[numeric_input, text_input], outputs=output)

    # 컴파일
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])

    # 훈련
    history = model.fit(
        [X_train_numeric, X_train_text],
        y_train,
        epochs=50,
        batch_size=64,
        validation_data=([X_val_numeric, X_val_text], y_val),
        verbose=0  # 훈련 진행 상황을 보고 싶다면 1로 설정
    )

    return model, scaler, vectorizer, history

def train_pytorch_model(X_train, y_train, X_val, y_val, numeric_features, text_feature):
    # 수치형 데이터 스케일링
    scaler = StandardScaler()
    X_train_numeric = scaler.fit_transform(X_train[numeric_features])
    X_val_numeric = scaler.transform(X_val[numeric_features])

    # 텍스트 데이터 벡터화
    vectorizer = TfidfVectorizer(max_features=1000)
    X_train_text = vectorizer.fit_transform(X_train[text_feature]).toarray()
    X_val_text = vectorizer.transform(X_val[text_feature]).toarray()

    # 데이터셋 생성
    train_dataset = PerformanceDataset(X_train_numeric, X_train_text, y_train)
    val_dataset = PerformanceDataset(X_val_numeric, X_val_text, y_val)

    train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=64)

    # 모델 정의
    class RevenuePredictor(nn.Module):
        def __init__(self, num_numeric_features, num_text_features):
            super(RevenuePredictor, self).__init__()
            self.numeric_fc = nn.Linear(num_numeric_features, 128)
            self.text_fc = nn.Linear(num_text_features, 128)
            self.fc = nn.Linear(256, 1)
            self.relu = nn.ReLU()

        def forward(self, numeric, text):
            numeric_out = self.relu(self.numeric_fc(numeric))
            text_out = self.relu(self.text_fc(text))
            combined = torch.cat((numeric_out, text_out), dim=1)
            output = self.fc(combined)
            return output

    model = RevenuePredictor(X_train_numeric.shape[1], X_train_text.shape[1])

    # 손실 함수와 옵티마이저
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    # 훈련 루프
    num_epochs = 50
    for epoch in range(num_epochs):
        model.train()
        for numeric, text, target in train_loader:
            optimizer.zero_grad()
            outputs = model(numeric, text)
            loss = criterion(outputs, target.view(-1, 1))
            loss.backward()
            optimizer.step()

    # 평가
    model.eval()
    with torch.no_grad():
        predictions = []
        targets = []
        for numeric, text, target in val_loader:
            outputs = model(numeric, text)
            predictions.extend(outputs.squeeze().numpy())
            targets.extend(target.numpy())
    mae = mean_absolute_error(targets, predictions)
    r2 = r2_score(targets, predictions)

    return model, scaler, vectorizer, mae, r2

def train_and_evaluate_models(performance_df):
    """
    다양한 모델을 훈련하고 평가하여 가장 성능이 좋은 모델을 선택합니다.
    """
    # 예측에 사용할 피처 선택
    features = [
        'performance_duration', 'followers', 'comments_count', 'album_likes',
        'album_rating', 'album_rating_count', 'track_count', 'avg_streams',
        'avg_popularity', 'total_views', 'total_likes', 'total_comments',
        'avg_views', 'avg_likes', 'avg_comments', 'video_count',
        'days_since_last_video', 'performance_month', 'performance_day', 'performance_weekday'
    ]

    # 장르 피처 자동 추가
    genre_cols = [col for col in performance_df.columns if col.startswith('genre_')]
    features.extend(genre_cols)

    # 텍스트 피처 추가
    text_feature = 'text_features'

    # 데이터 준비
    X = performance_df[features + [text_feature]]
    y = performance_df['revenue'].astype(float)

    # 수익이 있는 데이터만 사용하여 모델 훈련
    X_known = X[performance_df['revenue'].notnull()]
    y_known = y[performance_df['revenue'].notnull()]

    # 데이터 분할
    X_train, X_val, y_train, y_val = train_test_split(X_known, y_known, test_size=0.2, random_state=42)

    # 수치형 및 텍스트 데이터 전처리 설정 (scikit-learn 모델용)
    numeric_features = features
    numeric_transformer = StandardScaler()
    text_transformer = TfidfVectorizer(max_features=1000)

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('text', text_transformer, text_feature)
        ]
    )

    # 다양한 모델 리스트
    models = {
        'RandomForest': RandomForestRegressor(random_state=42),
        'XGBoost': XGBRegressor(random_state=42, objective='reg:squarederror')
    }

    # 모델별 성능 저장을 위한 딕셔너리
    model_performance = {}

    # 각 모델에 대해 훈련 및 평가
    for model_name, model in models.items():
        logger.info(f"{model_name} 모델 훈련 시작")
        clf = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('regressor', model)
        ])

        # 모델 훈련
        clf.fit(X_train, y_train)

        # 예측 및 평가
        y_pred = clf.predict(X_val)
        mae = mean_absolute_error(y_val, y_pred)
        r2 = r2_score(y_val, y_pred)
        logger.info(f"{model_name} MAE: {mae:.2f}")
        logger.info(f"{model_name} R² Score: {r2:.2f}")

        # 교차 검증
        cv_mae = -cross_val_score(clf, X_known, y_known, cv=5, scoring='neg_mean_absolute_error').mean()
        cv_r2 = cross_val_score(clf, X_known, y_known, cv=5, scoring='r2').mean()
        logger.info(f"{model_name} 교차 검증 MAE: {cv_mae:.2f}")
        logger.info(f"{model_name} 교차 검증 R² Score: {cv_r2:.2f}")

        # 성능 저장
        model_performance[model_name] = {
            'model': clf,
            'mae': mae,
            'r2': r2,
            'cv_mae': cv_mae,
            'cv_r2': cv_r2
        }

    # TensorFlow 모델 훈련
    logger.info("TensorFlow 모델 훈련 시작")
    tf_model, tf_scaler, tf_vectorizer, tf_history = train_tensorflow_model(
        X_train, y_train, X_val, y_val, numeric_features, text_feature
    )
    tf_predictions = tf_model.predict([
        tf_scaler.transform(X_val[numeric_features]),
        tf_vectorizer.transform(X_val[text_feature]).toarray()
    ]).flatten()
    tf_mae = mean_absolute_error(y_val, tf_predictions)
    tf_r2 = r2_score(y_val, tf_predictions)
    logger.info(f"TensorFlow MAE: {tf_mae:.2f}")
    logger.info(f"TensorFlow R² Score: {tf_r2:.2f}")

    model_performance['TensorFlow'] = {
        'model': tf_model,
        'scaler': tf_scaler,
        'vectorizer': tf_vectorizer,
        'mae': tf_mae,
        'r2': tf_r2,
        'cv_mae': None,
        'cv_r2': None
    }

    '''
    # PyTorch 모델 훈련
    logger.info("PyTorch 모델 훈련 시작")
    pt_model, pt_scaler, pt_vectorizer, pt_mae, pt_r2 = train_pytorch_model(
        X_train, y_train, X_val, y_val, numeric_features, text_feature
    )
    logger.info(f"PyTorch MAE: {pt_mae:.2f}")
    logger.info(f"PyTorch R² Score: {pt_r2:.2f}")

    model_performance['PyTorch'] = {
        'model': pt_model,
        'scaler': pt_scaler,
        'vectorizer': pt_vectorizer,
        'mae': pt_mae,
        'r2': pt_r2,
        'cv_mae': None,
        'cv_r2': None
    }
    '''

    # 가장 성능이 좋은 모델 선택 (MAE 기준)
    best_model_name = min(model_performance, key=lambda x: model_performance[x]['mae'] if model_performance[x]['mae'] is not None else np.inf)
    best_model_info = model_performance[best_model_name]
    logger.info(f"가장 성능이 좋은 모델은 {best_model_name}입니다.")

    # 모델 저장
    if best_model_name in ['RandomForest', 'GradientBoosting', 'XGBoost', 'LightGBM']:
        joblib.dump(best_model_info['model'], 'best_model.pkl')
        logger.info(f"최고의 모델을 'best_model.pkl'로 저장했습니다.")
    elif best_model_name == 'TensorFlow':
        best_model_info['model'].save('best_model_tf.h5')
        joblib.dump((best_model_info['scaler'], best_model_info['vectorizer']), 'tf_preprocessing.pkl')
        logger.info(f"최고의 TensorFlow 모델을 'best_model_tf.h5'로 저장했습니다.")
        logger.info(f"TensorFlow 전처리 도구를 'tf_preprocessing.pkl'로 저장했습니다.")
    elif best_model_name == 'PyTorch':
        torch.save(best_model_info['model'].state_dict(), 'best_model_pt.pth')
        joblib.dump((best_model_info['scaler'], best_model_info['vectorizer']), 'pt_preprocessing.pkl')
        logger.info(f"최고의 PyTorch 모델을 'best_model_pt.pth'로 저장했습니다.")
        logger.info(f"PyTorch 전처리 도구를 'pt_preprocessing.pkl'로 저장했습니다.")

    return best_model_info['model'], features + [text_feature], best_model_name, model_performance

def train_tensorflow_model(X_train, y_train, X_val, y_val, numeric_features, text_feature):
    # 수치형 데이터 스케일링
    scaler = StandardScaler()
    X_train_numeric = scaler.fit_transform(X_train[numeric_features])
    X_val_numeric = scaler.transform(X_val[numeric_features])

    # 텍스트 데이터 벡터화
    vectorizer = TfidfVectorizer(max_features=1000)
    X_train_text = vectorizer.fit_transform(X_train[text_feature]).toarray()
    X_val_text = vectorizer.transform(X_val[text_feature]).toarray()

    # 모델 정의
    numeric_input = layers.Input(shape=(X_train_numeric.shape[1],), name='numeric_input')
    text_input = layers.Input(shape=(X_train_text.shape[1],), name='text_input')

    numeric_dense = layers.Dense(128, activation='relu')(numeric_input)
    text_dense = layers.Dense(128, activation='relu')(text_input)

    combined = layers.concatenate([numeric_dense, text_dense])
    output = layers.Dense(1)(combined)

    model = models.Model(inputs=[numeric_input, text_input], outputs=output)

    # 컴파일
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])

    # 훈련
    history = model.fit(
        [X_train_numeric, X_train_text],
        y_train,
        epochs=50,
        batch_size=64,
        validation_data=([X_val_numeric, X_val_text], y_val),
        verbose=0  # 훈련 진행 상황을 보고 싶다면 1로 설정
    )

    return model, scaler, vectorizer, history

def train_pytorch_model(X_train, y_train, X_val, y_val, numeric_features, text_feature):
    # 수치형 데이터 스케일링
    scaler = StandardScaler()
    X_train_numeric = scaler.fit_transform(X_train[numeric_features])
    X_val_numeric = scaler.transform(X_val[numeric_features])

    # 텍스트 데이터 벡터화
    vectorizer = TfidfVectorizer(max_features=1000)
    X_train_text = vectorizer.fit_transform(X_train[text_feature]).toarray()
    X_val_text = vectorizer.transform(X_val[text_feature]).toarray()

    # 데이터셋 생성
    train_dataset = PerformanceDataset(X_train_numeric, X_train_text, y_train)
    val_dataset = PerformanceDataset(X_val_numeric, X_val_text, y_val)

    train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=64)

    # 모델 정의
    class RevenuePredictor(nn.Module):
        def __init__(self, num_numeric_features, num_text_features):
            super(RevenuePredictor, self).__init__()
            self.numeric_fc = nn.Linear(num_numeric_features, 128)
            self.text_fc = nn.Linear(num_text_features, 128)
            self.fc = nn.Linear(256, 1)
            self.relu = nn.ReLU()

        def forward(self, numeric, text):
            numeric_out = self.relu(self.numeric_fc(numeric))
            text_out = self.relu(self.text_fc(text))
            combined = torch.cat((numeric_out, text_out), dim=1)
            output = self.fc(combined)
            return output

    model = RevenuePredictor(X_train_numeric.shape[1], X_train_text.shape[1])

    # 손실 함수와 옵티마이저
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    # 훈련 루프
    num_epochs = 50
    for epoch in range(num_epochs):
        model.train()
        for numeric, text, target in train_loader:
            optimizer.zero_grad()
            outputs = model(numeric, text)
            loss = criterion(outputs, target.view(-1, 1))
            loss.backward()
            optimizer.step()

    # 평가
    model.eval()
    with torch.no_grad():
        predictions = []
        targets = []
        for numeric, text, target in val_loader:
            outputs = model(numeric, text)
            predictions.extend(outputs.squeeze().numpy())
            targets.extend(target.numpy())
    mae = mean_absolute_error(targets, predictions)
    r2 = r2_score(targets, predictions)

    return model, scaler, vectorizer, mae, r2

def predict_revenue(
    best_model,
    performance_df: pd.DataFrame,
    features: List[str],
    best_model_name: str,
    model_performance: dict
) -> pd.DataFrame:
    try:
        # 'text_features'를 텍스트 피처로 정의
        text_feature = 'text_features'
        # 'text_features'를 제외한 나머지 피처를 수치형 피처로 정의
        numeric_features = [f for f in features if f != text_feature]
        
        X = performance_df[features]
        y = performance_df['revenue'].astype(float)
        X_unknown = X[(y.isnull()) | (y == 0)]
        indices_unknown = X_unknown.index
        logger.info(f"예측할 레코드 수: {len(X_unknown)}")

        if not X_unknown.empty:
            if best_model_name in ['RandomForest', 'GradientBoosting', 'XGBoost', 'LightGBM']:
                # scikit-learn 파이프라인 사용
                predicted_revenues = best_model.predict(X_unknown)
            elif best_model_name == 'TensorFlow':
                # TensorFlow 전처리 도구 로드
                tf_scaler, tf_vectorizer = model_performance['TensorFlow']['scaler'], model_performance['TensorFlow']['vectorizer']
                X_unknown_numeric = tf_scaler.transform(X_unknown[numeric_features])
                X_unknown_text = tf_vectorizer.transform(X_unknown[text_feature]).toarray()
                predicted_revenues = best_model.predict([X_unknown_numeric, X_unknown_text]).flatten()
            elif best_model_name == 'PyTorch':
                # PyTorch 전처리 도구 로드
                pt_scaler, pt_vectorizer = model_performance['PyTorch']['scaler'], model_performance['PyTorch']['vectorizer']
                X_unknown_numeric = pt_scaler.transform(X_unknown[numeric_features])
                X_unknown_text = pt_vectorizer.transform(X_unknown[text_feature]).toarray()

                # 텐서 변환
                X_numeric_tensor = torch.tensor(X_unknown_numeric, dtype=torch.float32)
                X_text_tensor = torch.tensor(X_unknown_text, dtype=torch.float32)

                # 모델 예측
                best_model.eval()
                with torch.no_grad():
                    outputs = best_model(X_numeric_tensor, X_text_tensor).squeeze().numpy()
                predicted_revenues = outputs
            else:
                logger.error(f"지원되지 않는 모델 이름: {best_model_name}")
                raise ValueError(f"Unsupported model name: {best_model_name}")

            performance_df.loc[indices_unknown, 'predicted_revenue'] = predicted_revenues
            logger.info(f"{len(predicted_revenues)}개의 레코드에 대한 수익을 예측했습니다.")
        else:
            logger.info("예측할 레코드가 없습니다.")
        return performance_df
    except Exception as e:
        logger.error(f"수익 예측 중 오류 발생: {e}")
        raise

def save_results(performance_df: pd.DataFrame):
    try:
        performance_df.to_csv('performance_with_predictions.csv', index=False)
        logger.info("예측 결과를 'performance_with_predictions.csv'로 저장했습니다.")
    except Exception as e:
        logger.error(f"결과 저장 중 오류 발생: {e}")
        raise

def predict_performance_revenue():
    try:
        performance_df, albums_df, songs_df, artists_df, youtube_df = load_data()
        performance_df = preprocess_data(performance_df, albums_df, songs_df, artists_df, youtube_df)
        best_model, features, best_model_name, model_performance = train_and_evaluate_models(performance_df)
        performance_df = predict_revenue(best_model, performance_df, features, best_model_name, model_performance)
        save_results(performance_df)
    except Exception as e:
        logger.error(f"예측 과정 중 오류 발생: {e}")
        raise

predict_performance_revenue()