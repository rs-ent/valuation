##### Valuation/MNV/MOV/PCV/PCV_main.py #####
'''
PCV_main.py는 공연 경제 가치(CEV), 미디어 전환 가치(MCV) 및 음악 데이터 점수(MDS)를 통합하여 최종 팬 가치(PCV)를 산출함
각 모듈은 Firebase 캐시를 확인 후 데이터를 반환함
CEV 모듈은 공연 이벤트의 실제 수익 데이터를 기반으로 경제 가치를 계산함
MCV 모듈은 YouTube, Twitter, Instagram에서 수집한 데이터를 통해 미디어 전환 가치를 산출함
구체적으로 mcv_youtube는 채널 비디오의 조회수, 좋아요, 댓글 등 참여 지표를 반영함, mcv_twitter는 트윗의 좋아요, 리트윗, 답글, 인용 수를 기반으로 계산함, mcv_instagram은 게시물의 좋아요 및 댓글 데이터를 활용함
MDS 모듈은 팬 밸류 트렌드와 앨범 평가 데이터를 활용하여 음악 데이터 점수를 산출함
각 모듈의 결과에 설정된 가중치(CEV_WEIGHT, MCV_WEIGHT, MDS_WEIGHT)를 적용하여 보정함
보정된 CEV, MCV, MDS 값을 합산하여 최종 PCV 값을 도출함
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

from Valuation.MNV.MOV.PCV.CEV.CEV_main import cev
from Valuation.MNV.MOV.PCV.MCV.MCV_main import mcv
from Valuation.MNV.MOV.PCV.MDS.MDS_main import mds
from Valuation.utils.weights import Weights
CEV_WEIGHT = Weights.PCV.CEV_WEIGHT
MCV_WEIGHT = Weights.PCV.MCV_WEIGHT
MDS_WEIGHT = Weights.PCV.MDS_WEIGHT

from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='PCV'
def pcv():
    load_data = check_record(DATA_TARGET)
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    cev_data = cev()
    mcv_data = mcv()
    mds_data = mds()

    cev_result = cev_data.get('cev') * CEV_WEIGHT
    mcv_result = mcv_data.get('mcv') * MCV_WEIGHT
    mds_result = mds_data.get('mds') * MDS_WEIGHT
    pcv_result = cev_result + mcv_result + mds_result
    result = {
        'cev_a': cev_data.get('cev'),
        'cev_weight': CEV_WEIGHT,
        'cev': cev_result,
        'mcv_a': mcv_data.get('mcv'),
        'mcv_weight': MCV_WEIGHT,
        'mcv': mcv_result,
        'mds_a': mds_data.get('mds'),
        'mds_weight': MDS_WEIGHT,
        'mds': mds_result,
        'pcv': pcv_result
    }

    save_record(DATA_TARGET, result)
    return result