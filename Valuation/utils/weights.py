class Weights:
    class MOV:
        FV_WEIGHT=0.5
        PFV_WEIGHT=1.0
        PCV_WEIGHT=1.0
        MRV_WEIGHT=1.0
        K_WEIGHT=8.9
    class FV:
        FB_WEIGHT=1.0
        ER_WEIGHT=1.0
        G_WEIGHT=1.0
        FV_WEIGHT=0.001
        FV_TREND_WEIGHT=1.003
        FV_T_WEIGHT=0.0001
    class PFV:
        SV_WEIGHT=1.0
        RV_WEIGHT=1.0
        APV_WEIGHT=2300
        UDI_ALPHA=1.0
        AV_WEIGHT=1.0
        PFV_WEIGHT=1.0
    class PCV:
        CEV_WEIGHT=1.0
        CEV_ALPHA_AV_DEPENDENCY=0.77
        CEV_ALPHA_AV_PROPORTION=0.5
        CEV_BETA_PER_EVENTS=0.05
        CEV_FV_T_WEIGHT=1.00018
        MCV_WEIGHT=1.0
        MCV_YOUTUBE=1.0
        MCV_VIEW=0.43
        MCV_COMMENTS=0.35
        MCV_LIKES=0.2
        MCV_DURATION=0.02
        MCV_TWITTER=1590
        MCV_EV_WEIGHT=1.00003
        MCV_TWITTER_LIKES=0.1
        MCV_TWITTER_COMMENTS=0.2
        MCV_TWITTER_RETWEET=0.3
        MCV_TWITTER_QUOTE=0.4
        MCV_INSTAGRAM=30
        MCV_INSTAGRAM_LIKES=0.2
        MCV_INSTAGRAM_COMMENTS=0.4
        MDS_WEIGHT=1.0
        MDS_AIF_WEIGHT=1.0004
        MDS_AV_WEIGHT=1.000004
    class MRV:
        BV_AV_WEIGHT=1.0023
        BV_AV_RATIO=0.000025
        BV_FV_WEIGHT=1.0002
        BV_FV_RATIO=0.0000065
        BV_CATEGORY_WEIGHT={
            '드라마':100,
            '예능':30,
            '음악방송':5,
            '기타':10
        }

class Variables:
    LAP=23000
    DISCOUNT_RATE=0.06
    REVENUE_PER_STREAM=6
    REVENUE_PER_ENGAGEMENT=0.1