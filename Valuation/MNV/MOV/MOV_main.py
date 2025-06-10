##### Valuation/MNV/MOV/MOV_main.py #####

'''
MOV_main.py는 다양한 가치 모듈(FV, PFV, PCV, MRV)을 통합하여 팬덤 및 음악 산업 전반의 총 가치를 시계열 데이터로 산출함
timeline_df의 각 컬럼은 팬덤 가치(FV_t), 스트리밍 가치(SV_t), 인기도 가치(APV_t), 음반판매 가치(RV_t), 공연/이벤트 가치(CEV_t), 미디어/콘텐츠/소셜미디어 가치(MCV_t), 굿즈/MD 가치(MDS_t), 매니지먼트 가치(MRV_t)를 나타냄
mcv_youtube는 유튜브 비디오 데이터(조회수, 좋아요, 댓글 등)를 기반으로 콘텐츠 전환 가치를 산출함
mcv_twitter는 트윗의 참여 지표(좋아요, 리트윗, 답글, 인용 등)를 분석하여 소셜미디어 가치를 계산함
mcv_instagram은 인스타그램 게시물의 좋아요와 댓글 데이터를 활용하여 미디어 가치를 평가함
각 모듈의 결과에 가중치를 적용하여 PFV, PCV, MRV 값이 보정됨
PFVFunc와 PCVFunc 클래스를 통해 음반 및 공연 관련 데이터를 시간에 따라 분산 보정함
MRV, FV_t 데이터와 결합하여 MOV_t(총 가치)를 산출함
산출된 타임라인 데이터는 visualizer 모듈로 시각화되어 분석에 활용됨
전체 파이프라인은 다양한 플랫폼 데이터를 통합, 정규화 및 시계열 분석으로 산업 가치를 평가함

[timeline_df의 Columns 설명]
FV_t : 팬덤 가치
SV_t : 스트리밍 가치
APV_t : 인기도 가치 
RV_t : 음반판매 가치
CEV_t : 콘서트/이벤트 가치
MCV_t : 미디어/콘텐츠/소셜미디어 가치
MDS_t : 굿즈/MD 가치
MRV_t : 방송/드라마/영화/상표권 가치
'''

from Valuation.MNV.MOV.FV.FV_t import fv_t
from Valuation.MNV.MOV.PFV.PFV_main import pfv
from Valuation.MNV.MOV.PCV.PCV_main import pcv
from Valuation.MNV.MOV.PCV.CEV.CEV_main import cev
from Valuation.MNV.MOV.PCV.MDS.MDS_main import mds
from Valuation.MNV.MOV.PCV.MCV.MCV_twitter import mcv_twitter
from Valuation.MNV.MOV.PCV.MCV.MCV_youtube import mcv_youtube
from Valuation.MNV.MOV.PCV.MCV.MCV_instagram import mcv_instagram
from Valuation.MNV.MOV.MRV.MRV_main import mrv
from Valuation.MNV.MOV.MOV_visualization import visualizer

import calendar
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

WEIGHT = {
    "저작권": 8.9,
    "음반": 1,
    "스트리밍": 10,
    "인기도": 3,
    "공연": 2,
    "트위터": 40000,
    "인스타그램": 10,
    "유튜브": 40,
    "MD판매": 0.7,
    "매니지먼트": 0.2,
}

def convert_to_utc(timestamp):
    timestamp = pd.to_datetime(timestamp)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    else:
        return timestamp.tz_convert("UTC")

class PFVFunc:
    @staticmethod
    def distribute_rv_over_time(rv, start_date, end_date):
        start_date = convert_to_utc(start_date)
        end_date = convert_to_utc(end_date)
        date_range = pd.date_range(start=start_date, end=end_date, freq='ME')
        num_months = len(date_range)

        if num_months > 12:
            initial_decay_rates = np.linspace(0.6, 0.4, 3)
            mid_decay_rates = np.linspace(0.4, 0.3, 6)  
            long_decay_rates = 0.3 * (0.9 ** (np.arange(max(0, num_months - 6)) / 3))
            decay_rates = np.concatenate([initial_decay_rates, mid_decay_rates, long_decay_rates])
        else:
            initial_decay_rates = np.linspace(0.6, 0.4, min(3, num_months))
            mid_decay_rates = np.linspace(0.4, 0.3, num_months - 2)
            decay_rates = np.concatenate([initial_decay_rates, mid_decay_rates])

        rv_values = rv * decay_rates[:num_months] * WEIGHT['음반']
        df = pd.DataFrame({'date': date_range, 'rv_t': rv_values})
        return df
    
    @staticmethod
    def distribute_value_over_time(value, start_date, end_date, decay_rate=None, residual_rate=0.001, key_str='sv_t'):
        start_date = convert_to_utc(start_date)
        end_date = convert_to_utc(end_date)
        date_range = pd.date_range(start=start_date, end=end_date, freq='ME')
        t_values = np.arange(len(date_range))
        total_periods = 70 * 12

        if decay_rate is None:
            decay_rate = -np.log(residual_rate) / total_periods

        value_t_values = value * np.exp(-decay_rate * t_values)
        total_value_t = value_t_values.sum()
        value_t_values_normalized = value_t_values * (value / total_value_t)

        df = pd.DataFrame({'date': date_range, key_str: value_t_values_normalized})
        return df
    
    @staticmethod
    def integrate_pfv_data(timeline_df, pfv_data, end_date, decay_rate=None, residual_rate=0.001):
        albums_data = pfv_data['av_a']
        sv_value_t_dfs = []
        apv_value_t_dfs = []
        rv_value_t_dfs = []

        for item in albums_data:
            sv = item.get('sv', 0)
            apv = item.get('apv', 0)
            rv = item.get('rv', 0)
            release_date = pd.to_datetime(item['release_date'])

            if sv > 0:
                sv_df = PFVFunc.distribute_value_over_time(
                    sv, release_date, end_date, decay_rate=decay_rate, residual_rate=residual_rate, key_str='sv_t'
                )
                sv_df.set_index('date', inplace=True)
                sv_value_t_dfs.append(sv_df['sv_t'])

            if apv > 0:
                apv_df = PFVFunc.distribute_value_over_time(
                    apv, release_date, end_date, decay_rate=decay_rate, residual_rate=residual_rate, key_str='apv_t'
                )
                apv_df.set_index('date', inplace=True)
                apv_value_t_dfs.append(apv_df['apv_t'])

            if rv > 0:
                rv_t_df = PFVFunc.distribute_rv_over_time(rv, release_date, end_date)
                rv_t_df.set_index('date', inplace=True)
                rv_value_t_dfs.append(rv_t_df['rv_t'])

        if sv_value_t_dfs:
            total_sv_t = pd.concat(sv_value_t_dfs, axis=1).sum(axis=1)
            timeline_df = timeline_df.join(total_sv_t.rename('sv_t'), how='left')
            timeline_df['sv_t'] = timeline_df['sv_t'] * WEIGHT['스트리밍']
            timeline_df['sv_t'] = timeline_df['sv_t'].fillna(0)
        else:
            timeline_df['sv_t'] = 0.0

        if apv_value_t_dfs:
            total_apv_t = pd.concat(apv_value_t_dfs, axis=1).sum(axis=1)
            timeline_df = timeline_df.join(total_apv_t.rename('apv_t'), how='left')
            timeline_df['apv_t'] = timeline_df['apv_t'] * WEIGHT['인기도']
            timeline_df['apv_t'] = timeline_df['apv_t'].fillna(0)
        else:
            timeline_df['apv_t'] = 0.0

        if rv_value_t_dfs:
            total_rv_t = pd.concat(rv_value_t_dfs, axis=1).sum(axis=1)
            timeline_df = timeline_df.join(total_rv_t.rename('rv_t'), how='left')
            timeline_df['rv_t'] = timeline_df['rv_t'].fillna(0)
        else:
            timeline_df['rv_t'] = 0.0

        return timeline_df
    
class PCVFunc:
    @staticmethod
    def integrate_cev_events(timeline_df, cev_events, decay_rate=0.1, base_influence_months=2, max_influence_months=12, min_influence_months=1):
        timeline_df['cev_t'] = 0.0

        if not cev_events:
            return timeline_df
        
        max_cer = max(event.get('cer', 0) for event in cev_events)

        for event in cev_events:
            cer = event.get('cer', 0)
            start_period = event.get('start_period', None)
            if start_period is None:
                continue
            try:
                event_date = pd.to_datetime(start_period)
                last_day = calendar.monthrange(event_date.year, event_date.month)[1]
                event_month = event_date.replace(day=last_day)
                event_month = convert_to_utc(event_month)

                influence_months = int(base_influence_months + (cer / max_cer * (max_influence_months - base_influence_months)))
                influence_months = min(influence_months, max_influence_months)
                influence_months = max(influence_months, min_influence_months)

                relevant_dates = timeline_df.loc[event_month:].index[:influence_months]
                months_since_event = np.arange(len(relevant_dates))

                decay_values = cer * np.exp(-decay_rate * months_since_event)

                timeline_df.loc[relevant_dates, 'cev_t'] += decay_values * WEIGHT['공연']

            except Exception:
                continue
        return timeline_df
    
    @staticmethod
    def integrate_mcv_events(timeline_df, mcv_twitter, mcv_youtube, mcv_instagram):
        def compute_fraction_decay_factor(t, peak_month=6, initial_decay_rate=0.001, decay_increment=0.0005, max_decay_rate=0.1, max_factor=0.01):
            if t <= peak_month:
                return (t / peak_month) * max_factor
            else:
                current_decay_rate = initial_decay_rate + (t - peak_month) * decay_increment
                current_decay_rate = min(current_decay_rate, max_decay_rate)
                decay = max_factor - (t - peak_month) * current_decay_rate
                return max(decay, 0.0)

        timeline_df['mcv_twitter'] = 0.0
        timeline_df['mcv_youtube'] = 0.0 
        timeline_df['mcv_instagram'] = 0.0   
        
        twitter_events = mcv_twitter.get('tweets', [])
        tweets_df = pd.DataFrame(twitter_events)
        if not tweets_df.empty:
            tweets_df['created_at'] = pd.to_datetime(tweets_df['created_at'], errors='coerce')
            tweets_df = tweets_df.dropna(subset=['created_at'])
            
            tweets_df['release_date'] = tweets_df['created_at'] + pd.offsets.MonthEnd(0)
            tweets_df['release_date'] = tweets_df['release_date'].apply(convert_to_utc)
            tweets_df['mcv'] = pd.to_numeric(tweets_df['mcv'], errors='coerce').fillna(0.0)

            for _, row in tweets_df.iterrows():
                release_date = row['release_date']
                mcv_value = row['mcv'] * WEIGHT['트위터']

                if release_date not in timeline_df.index:
                    release_date = timeline_df.index[timeline_df.index >= release_date]
                    if not release_date.empty:
                        release_date = release_date[0]
                    else:
                        continue

                relevant_dates = timeline_df.loc[release_date:].index
                months_since_release = (relevant_dates.year - release_date.year) * 12 + (relevant_dates.month - release_date.month)

                decay_factors = months_since_release.map(
                    lambda t: compute_fraction_decay_factor(t)
                )
                
                contributions = mcv_value * decay_factors
                timeline_df.loc[relevant_dates, 'mcv_twitter'] += contributions

        instagram_events = mcv_instagram.get('posts', [])
        instagram_df = pd.DataFrame(instagram_events)
        if not instagram_df.empty:
            instagram_df['date'] = pd.to_datetime(instagram_df['date'], errors='coerce')
            instagram_df = instagram_df.dropna(subset=['date'])
            
            instagram_df['release_date'] = instagram_df['date'] + pd.offsets.MonthEnd(0)
            instagram_df['release_date'] = instagram_df['release_date'].apply(convert_to_utc)
            instagram_df['mcv'] = pd.to_numeric(instagram_df['mcv'], errors='coerce').fillna(0.0)

            for _, row in instagram_df.iterrows():
                release_date = row['release_date']
                mcv_value = row['mcv'] * WEIGHT['인스타그램']

                if release_date not in timeline_df.index:
                    release_date = timeline_df.index[timeline_df.index >= release_date]
                    if not release_date.empty:
                        release_date = release_date[0]
                    else:
                        continue

                relevant_dates = timeline_df.loc[release_date:].index
                months_since_release = (relevant_dates.year - release_date.year) * 12 + (relevant_dates.month - release_date.month)

                decay_factors = months_since_release.map(
                    lambda t: compute_fraction_decay_factor(t)
                )
                
                contributions = mcv_value * decay_factors
                timeline_df.loc[relevant_dates, 'mcv_instagram'] += contributions

        youtube_events = mcv_youtube.get('details', [])
        youtube_df = pd.DataFrame(youtube_events)
        if not youtube_df.empty:
            youtube_df['publishedAt'] = pd.to_datetime(youtube_df['publishedAt'], errors='coerce')
            youtube_df = youtube_df.dropna(subset=['publishedAt'])
            
            youtube_df['release_date'] = youtube_df['publishedAt'] + pd.offsets.MonthEnd(0)
            youtube_df['release_date'] = youtube_df['release_date'].apply(convert_to_utc)
            youtube_df['MCV'] = pd.to_numeric(youtube_df['MCV'], errors='coerce').fillna(0.0)
            
            for _, row in youtube_df.iterrows():
                release_date = row['release_date']
                mcv_value = row['MCV'] * WEIGHT['유튜브']
                
                if release_date not in timeline_df.index:
                    release_date = timeline_df.index[timeline_df.index >= release_date]
                    if not release_date.empty:
                        release_date = release_date[0]
                    else:
                        continue
                
                relevant_dates = timeline_df.loc[release_date:].index
                months_since_release = (relevant_dates.year - release_date.year) * 12 + (relevant_dates.month - release_date.month)
                
                decay_factors = months_since_release.map(
                    lambda t: compute_fraction_decay_factor(t)
                )
                contributions = mcv_value * decay_factors
                timeline_df.loc[relevant_dates, 'mcv_youtube'] += contributions

        timeline_df['mcv_t'] = timeline_df['mcv_twitter'] + timeline_df['mcv_youtube']
        return timeline_df
    
    @staticmethod
    def integrate_mds_events(timeline_df, mds_events):
        timeline_df['mds_t'] = 0.0  # 굿즈 가치 추가
        for event in mds_events:
            mds = event.get('MDS_t', 0)
            date = event.get('date', None)
            if date is None:
                continue
            try:
                event_date = pd.to_datetime(date)
                last_day = calendar.monthrange(event_date.year, event_date.month)[1]
                event_month = event_date.replace(day=last_day)
                event_month = convert_to_utc(event_month)

                if event_month in timeline_df.index:
                    timeline_df.at[event_month, 'mds_t'] += (mds * WEIGHT['MD판매'])
            except Exception:
                continue
        return timeline_df
    
class MRVFunc:
    @staticmethod
    def integrate_mrv_events(timeline_df, mrv_events, decay_rate=0.1, base_influence_months=2, max_influence_months=12, min_influence_months=1):
        timeline_df['mrv_t'] = 0.0
        # mrv_events가 비어있는지 확인
        if not mrv_events:
            return timeline_df
        
        max_cer = max(event.get('BF_event', 0) for event in mrv_events)

        for event in mrv_events:
            mrv = event.get('BF_event', 0)
            start_period = event.get('start_period', None)
            if start_period is None:
                continue
            try:
                event_date = pd.to_datetime(start_period)
                last_day = calendar.monthrange(event_date.year, event_date.month)[1]
                event_month = event_date.replace(day=last_day)
                event_month = convert_to_utc(event_month)

                influence_months = int(base_influence_months + (mrv / max_cer * (max_influence_months - base_influence_months)))
                influence_months = min(influence_months, max_influence_months)
                influence_months = max(influence_months, min_influence_months)

                relevant_dates = timeline_df.loc[event_month:].index[:influence_months]
                months_since_event = np.arange(len(relevant_dates))

                decay_values = mrv * np.exp(-decay_rate * months_since_event)

                timeline_df.loc[relevant_dates, 'mrv_t'] += decay_values * WEIGHT['매니지먼트']
            except Exception:
                continue
        return timeline_df

def set_timeline(fv_t_data, pfv_data, pcv_data, cev_events, mds_records, mcv_youtube_data, mcv_twitter_data, mcv_instagram_data, mrv_data, decay_rate=None, residual_rate=0.001):
    timeline_dates = [item['date'] for item in fv_t_data['sub_data']]
    start_date = min(timeline_dates)
    end_date = max(timeline_dates)
    timeline_df = pd.DataFrame({'date': pd.date_range(start=start_date, end=end_date, freq='ME')})
    timeline_df.set_index('date', inplace=True)

    # PFV 데이터 통합
    timeline_df = PFVFunc.integrate_pfv_data(timeline_df, pfv_data, end_date, decay_rate=decay_rate, residual_rate=residual_rate)
    timeline_df['pfv_t'] = timeline_df['sv_t'] + timeline_df['apv_t'] + timeline_df['rv_t']

    # PCV 데이터 통합
    timeline_df = PCVFunc.integrate_cev_events(timeline_df, cev_events)
    timeline_df = PCVFunc.integrate_mcv_events(timeline_df, mcv_twitter_data, mcv_youtube_data, mcv_instagram_data)
    timeline_df = PCVFunc.integrate_mds_events(timeline_df, mds_records)
    timeline_df['pcv_t'] = timeline_df['cev_t'] + timeline_df['mcv_t'] + timeline_df['mds_t']

    # MRV 데이터 통합
    mrv_events = mrv_data.get('record', [])
    timeline_df = MRVFunc.integrate_mrv_events(timeline_df, mrv_events)

    # FV_t 데이터 통합
    fv_t_df = pd.DataFrame(fv_t_data['sub_data'])
    fv_t_df['date'] = pd.to_datetime(fv_t_df['date'])
    fv_t_df.set_index('date', inplace=True)
    timeline_df = timeline_df.join(fv_t_df[['FV_t']], how='left')
    timeline_df['FV_t'] = timeline_df['FV_t'].fillna(0)

    # MOV_t 계산
    timeline_df['MOV_t'] = (
        timeline_df['FV_t'] +
        timeline_df['pfv_t'] +
        timeline_df['pcv_t'] +
        timeline_df['mrv_t']
    )

    timeline_df.reset_index(inplace=True)
    return timeline_df.to_dict('records')

def plot_timeline(distribution_result):
    df = pd.DataFrame(distribution_result)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    df['cumulative_MOV_t'] = df['MOV_t'].cumsum()

    plt.rc('font', family='AppleGothic')
    plt.rcParams['axes.unicode_minus'] = False
    plt.style.use('seaborn-v0_8-pastel')
    plt.figure(figsize=(14, 8))

    linewidth_base = 1
    linewidth_multiplier = {
        'FV_t': 2,
        'pfv_t': 2,
        'sv_t': 1,
        'apv_t': 1,
        'rv_t': 1,
        'pcv_t': 2,
        'cev_t': 1,
        'mcv_t': 1,
        'mds_t': 1,
        'mrv_t': 2,
        'MOV_t': 3
    }

    plt.plot(df.index, df['FV_t'], label='FV_t (팬덤 가치)', linewidth=linewidth_base * linewidth_multiplier['FV_t'], color='purple')
    plt.plot(df.index, df['pfv_t'], label='PFV_t (음반/음원 가치)', linewidth=linewidth_base * linewidth_multiplier['pfv_t'], color='yellow')
    plt.plot(df.index, df['sv_t'], label='SV_t (스트리밍 가치)', linewidth=linewidth_base * linewidth_multiplier['sv_t'], color='green')
    plt.plot(df.index, df['apv_t'], label='APV_t (인기도 가치)', linewidth=linewidth_base * linewidth_multiplier['apv_t'], color='blue')
    plt.plot(df.index, df['rv_t'], label='RV_t (피지컬 앨범 판매량)', linewidth=linewidth_base * linewidth_multiplier['rv_t'], color='orange')
    plt.plot(df.index, df['pcv_t'], label='PCV_t (공연/콘텐츠/MD 가치)', linewidth=linewidth_base * linewidth_multiplier['pcv_t'], color='coral')
    plt.plot(df.index, df['cev_t'], label='CEV_t (공연 가치)', linewidth=linewidth_base * linewidth_multiplier['cev_t'], color='red')
    plt.plot(df.index, df['mcv_t'], label='MCV_t (콘텐츠 가치)', linewidth=linewidth_base * linewidth_multiplier['mcv_t'], color='brown')
    plt.plot(df.index, df['mds_t'], label='MCV_t (MD 가치)', linewidth=linewidth_base * linewidth_multiplier['mds_t'], color='pink')
    plt.plot(df.index, df['mrv_t'], label='MRV_t (매니지먼트 가치)', linewidth=linewidth_base * linewidth_multiplier['mrv_t'], color='gold')
    plt.plot(df.index, df['MOV_t'], label='MOV_t (총 가치)', linewidth=linewidth_base * linewidth_multiplier['MOV_t'], color='black')

    plt.title('Valuation Timeline', fontsize=20)
    plt.xlabel('Date', fontsize=16)
    plt.ylabel('Value', fontsize=16)
    plt.legend(loc='upper right', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()

from Valuation.firebase.firebase_handler import save_record
DATA_TARGET='WEIGHT'
def mov():
    total_periods = 70 * 12  # 70년을 월 단위로 환산
    residual_rate = 0.001    # 70년 후 가치가 초기의 0.1%로 감소
    decay_rate = -np.log(residual_rate) / total_periods * WEIGHT['저작권']

    fv_t_data = fv_t()
    pfv_data = pfv()
    pcv_data = pcv()
    cev_data = cev()
    print(f"CEV Data : {cev_data}")
    cev_events = cev_data.get('events')
    mds_records = mds().get('records')
    mcv_youtube_data = mcv_youtube()
    mcv_twitter_data = mcv_twitter()
    mcv_instagram_data = mcv_instagram()
    mrv_data = mrv()

    timeline_result = set_timeline(
                        fv_t_data, 
                        pfv_data, 
                        pcv_data, 
                        cev_events,
                        mds_records,
                        mcv_youtube_data,
                        mcv_twitter_data,
                        mcv_instagram_data,
                        mrv_data,
                        decay_rate=decay_rate, 
                        residual_rate=residual_rate)
    
    weight_result = {
        'fv_t': 1,
        'sv_t': WEIGHT['스트리밍'],
        'apv_t': WEIGHT['인기도'],
        'rv_t': WEIGHT['음반'],
        'cev_t': WEIGHT['공연'],
        'mcv_twitter': WEIGHT['트위터'],
        'mcv_youtube': WEIGHT['유튜브'],
        'mcv_instagram': WEIGHT['인스타그램'],
        'mds_t': WEIGHT['MD판매'],
        'mrv_t': WEIGHT['매니지먼트'],
        'copyright': WEIGHT['저작권']
    }
    save_record(DATA_TARGET, weight_result)

    visualizer(timeline_result)
    return timeline_result

'''
export const WEIGHT = {
    fv_t: 1,          // 팬덤 지표에 따른 추정 가치
    sv_t: 1.5,          // 음원 스트리밍 횟수에 따른 추정 가치
    apv_t: 3.5,         // 음원 인기도 값(0~100)에 따른 추정 가치
    rv_t: 1,          // 음반 판매량에 따른 추정 가치
    cev_t: 0.5,         // 콘서트&행사 수익에 따른 추정 가치
    mcv_twitter: 1,   // 트위터 계정 추정 가치
    mcv_youtube: 3,   // 유튜브 계정 추정 가치
    mds_t: 0.3,         // 굿즈&MD판매 수익에 따른 추정 가치
    mrv_t: 0.15,         // 매니지먼트(출연료/초상권/상표권) 가치
    MOV: 9            // 총합
};
'''