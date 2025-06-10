from typing import List, Dict, Optional

import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.firestore import WriteBatch
from google.api_core.exceptions import DeadlineExceeded
from google.cloud.firestore_v1 import Client as FirestoreClient
from GoogleSheets.sheets import get_or_create_spreadsheet, write_data

from dotenv import load_dotenv
import os
import json
import time
import logging
from collections import OrderedDict
import asyncio
from utils.logger import setup_logger
logger = setup_logger(__name__)

from datetime import datetime, timezone, timedelta

# 환경 변수 로드
load_dotenv()

# 환경 변수에서 설정 가져오기
FIRESTORE_SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', 'Firebase/firebase.json')
ARTIST_ID = os.getenv("ARTIST_ID")
ARTIST_NAME_KOR = os.getenv("ARTIST_NAME_KOR")
ARTIST_NAME_ENG = os.getenv("ARTIST_NAME_ENG")
MELON_ID = os.getenv("MELON_ID")
SPREADSHEET_FOLDER_ID = os.getenv("PIPELINE_FOLDER_ID")

# Firestore 클라이언트 초기화
if not firebase_admin._apps:
    try:
        cred = credentials.Certificate(FIRESTORE_SERVICE_ACCOUNT_FILE)
        firebase_admin.initialize_app(cred)
    except Exception as e:
        raise

db: FirestoreClient = firestore.client()

def save_data(collection_name, data, doc_id):
    doc_ref = db.collection(collection_name).document(doc_id)
    doc_ref.set(data)

def save_record(field_name, data, sub_collection: Optional[str] = None, subcollection_field: Optional[str] = None):
    collection_name = 'valuation'
    doc_id = ARTIST_ID

    prev_data = load_record()
    if not prev_data:
        prev_data = {'id': doc_id}

    # sub_collection 모드일 경우 subcollection_field의 데이터를 분리
    subcollection_items = []
    if sub_collection and subcollection_field and subcollection_field in data:
        subcollection_items = data.pop(subcollection_field, [])

    prev_data[field_name] = data
    prev_data['artist_name'] = ARTIST_NAME_KOR
    prev_data['artist_name_eng'] = ARTIST_NAME_ENG
    prev_data['artist_melon_id'] = MELON_ID

    try:
        prev_data['timestamp'] = firestore.SERVER_TIMESTAMP
        doc_ref = db.collection(collection_name).document(doc_id)
        doc_ref.set(prev_data)
    except Exception as e:
        logger.error(f"Error saving single record to Firestore: {e}")
        raise
    
    # 서브컬렉션에 대한 처리는 subcollection_items가 존재할 때만 수행
    logger.info(f'Sub Collection Items: {subcollection_items}')
    if sub_collection and subcollection_items:
        subcollection_ref = doc_ref.collection(sub_collection)
        batch = db.batch()
        for item in subcollection_items:
            sub_doc_id = item.get('id')
            if not sub_doc_id:
                sub_doc_ref = subcollection_ref.document()
            else:
                sub_doc_ref = subcollection_ref.document(str(sub_doc_id))
            batch.set(sub_doc_ref, item)

        try:
            batch.commit()
            logger.info(f"Subcollection '{sub_collection}' with {len(subcollection_items)} items saved to Firestore.")

            def extract_all_keys(items):
                keys = set()
                for item in items:
                    for k, v in item.items():
                        if isinstance(v, dict):
                            keys.update([f"{k}.{subk}" for subk in v.keys()])
                        else:
                            keys.add(k)
                return sorted(keys)

            def expand_dict_fields(item, all_keys):
                row = OrderedDict((key, "") for key in all_keys)
                for k, v in item.items():
                    if isinstance(v, dict):
                        for subk, subv in v.items():
                            combined_key = f"{k}.{subk}"
                            if combined_key in row:
                                row[combined_key] = str(subv)
                    else:
                        if k in row:
                            row[k] = str(v)
                return list(row.values())
            
            all_keys = extract_all_keys(subcollection_items)
            spreadsheet = get_or_create_spreadsheet(SPREADSHEET_FOLDER_ID, ARTIST_NAME_KOR)
            
            try:
                worksheet = spreadsheet.worksheet(sub_collection)
                worksheet.clear()
                # 기존 워크시트에서도 헤더 삽입
                if len(subcollection_items) > 0:
                    worksheet.insert_row(all_keys, 1)
            except:
                worksheet = spreadsheet.add_worksheet(title=sub_collection, rows=1000, cols=50)
                # 새 워크시트 생성 시 헤더 삽입
                if len(subcollection_items) > 0:
                    worksheet.insert_row(all_keys, 1)

            rows = [expand_dict_fields(item, all_keys) for item in subcollection_items]
            worksheet.insert_rows(rows, 2)
            logger.info(f"Subcollection '{sub_collection}' data saved to Google Sheets.")

        except Exception as e:
            logger.error(f"Error saving subcollection '{sub_collection}' to Firestore or Google Sheets: {e}")
            raise
    
def load_record(target: Optional[str] = None, sub_collection: Optional[str] = None, field_name: Optional[str] = None) -> Optional[Dict]:
    """
    메인 컬렉션에서 문서를 로드하고, sub_collection과 field_name이 지정된 경우 해당 서브컬렉션도 로드하여 field_name 키로 결과 dict에 넣는다.
    """
    collection_name = 'valuation'
    doc_id = ARTIST_ID

    try:
        doc_ref = db.collection(collection_name).document(doc_id)
        doc = doc_ref.get()
        if doc.exists:
            record = doc.to_dict()
            record['id'] = doc.id

            # sub_collection과 field_name이 지정된 경우 서브컬렉션도 로드한다.
            if target and sub_collection and field_name:
                sub_col_ref = doc_ref.collection(sub_collection)
                sub_docs = sub_col_ref.stream()
                sub_data = []
                for d in sub_docs:
                    item = d.to_dict()
                    # 필요 시 item에 추가 가공 로직 가능
                    sub_data.append(item)
                record[target][field_name] = sub_data

            return record
        else:
            return None
    except Exception as e:
        print(f"Error loading document '{doc_id}' from Firestore collection '{collection_name}': {e}")
        return None
    
def load_with_filter(collection_name: str, filters: Optional[List[tuple]] = None) -> Optional[Dict]:
    try:
        collection_ref = db.collection(collection_name)
        
        if filters:
            query = collection_ref
            for field, op, value in filters:
                query = query.where(field, op, value)
            docs = query.stream()
        else:
            docs = collection_ref.stream()
        
        data = []
        for doc in docs:
            record = doc.to_dict()
            record['id'] = doc.id
            data.append(record)
        return data
    except Exception as e:
        return []
    
def check_record(target: str, sub_collection: Optional[str] = None, field_name: Optional[str] = None) -> Optional[Dict]:
    """
    target 필드가 존재하고 timestamp가 어제보다 최신일 경우 데이터를 반환.
    sub_collection과 field_name이 지정된 경우 해당 서브컬렉션도 읽어 field_name 키로 데이터에 추가.
    """
    collection_name = 'valuation'
    doc_id = ARTIST_ID

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    # sub_collection과 field_name이 있을 경우 load_record에 전달
    prev_data = load_record(target=target, sub_collection=sub_collection, field_name=field_name)
    if prev_data and prev_data.get(target) and prev_data.get('timestamp') > yesterday:
        return prev_data
    else:
        return None