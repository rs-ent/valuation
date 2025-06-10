# Firestore/firestore_handler.py

import asyncio
from typing import List, Dict, Optional

import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.firestore import WriteBatch
from google.api_core.exceptions import DeadlineExceeded
from google.cloud.firestore_v1 import Client as FirestoreClient

from dotenv import load_dotenv
import os
import time

from utils.logger import setup_logger

# 환경 변수 로드
load_dotenv()

# 환경 변수에서 설정 가져오기
FIRESTORE_SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', 'Firebase/firebase.json')

# 로깅 설정
logger = setup_logger(__name__)

# Firestore 클라이언트 초기화
if not firebase_admin._apps:
    try:
        cred = credentials.Certificate(FIRESTORE_SERVICE_ACCOUNT_FILE)
        firebase_admin.initialize_app(cred)
        logger.info("Firebase Admin SDK initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Firebase Admin SDK: {e}")
        raise

db: FirestoreClient = firestore.client()

def load_songs() -> List[Dict]:
    return _load_data('songs')

def load_single_song(doc_id: str) -> Optional[Dict]:
    return _load_single_record('songs', doc_id)

async def save_to_firestore(collection_name: str, data: List[Dict]) -> None:
    loop = asyncio.get_event_loop()
    try:
        # Execute Firestore operations asynchronously
        if isinstance(data, list):
            await loop.run_in_executor(None, _save_data, collection_name, data)
            logger.info(f"Successfully saved {len(data)} records to Firestore collection '{collection_name}'.")
        elif isinstance(data, dict):
            await loop.run_in_executor(None, _save_single_record, collection_name, data)
            logger.info(f"Successfully saved single record to Firestore collection '{collection_name}'.")
        else:
            logger.error(f"Data format not supported for Firestore save: {type(data)}")
    except Exception as e:
        logger.error(f"Failed to save data to Firestore: {e}")

def _save_data(collection_name: str, data: List[Dict]) -> None:
    try:
        batch_size = 100  # Reduced batch size to 100
        total_records = len(data)
        batches = [data[i:i + batch_size] for i in range(0, total_records, batch_size)]
        for batch_num, batch_data in enumerate(batches, start=1):
            success = False
            retries = 3
            delay = 1
            for attempt in range(retries):
                try:
                    batch = db.batch()
                    for record in batch_data:
                        record['timestamp'] = firestore.SERVER_TIMESTAMP
                        # Use the 'id' field as the document ID if available
                        doc_id = record.pop('id', None)
                        doc_ref = db.collection(collection_name).document(doc_id) if doc_id else db.collection(collection_name).document()
                        sanitized_record = {k: (v if v != '' else None) for k, v in record.items()}
                        batch.set(doc_ref, sanitized_record)
                    # Commit the batch with a timeout
                    batch.commit(timeout=30)  # Set a timeout of 30 seconds for the commit
                    logger.info(f"Committed batch {batch_num}/{len(batches)} with {len(batch_data)} records to Firestore")
                    success = True
                    break  # Exit the retry loop if successful
                except DeadlineExceeded as e:
                    logger.warning(f"Batch commit {batch_num} attempt {attempt+1} failed due to Deadline Exceeded. Retrying after {delay} seconds...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                except Exception as e:
                    logger.error(f"Error during Firestore batch commit: {e}")
                    raise
            if not success:
                logger.error(f"Failed to commit batch {batch_num} after {retries} attempts")
                raise Exception(f"Failed to commit batch {batch_num} after {retries} attempts")
    except Exception as e:
        logger.error(f"Error during Firestore batch commits: {e}")
        raise

def _save_single_record(collection_name: str, data: Dict) -> None:
    try:
        data['timestamp'] = firestore.SERVER_TIMESTAMP
        # Use the 'id' field as the document ID if available
        doc_id = data.pop('id', None)
        doc_ref = db.collection(collection_name).document(doc_id) if doc_id else db.collection(collection_name).document()
        sanitized_record = {k: (v if v != '' else None) for k, v in data.items()}
        doc_ref.set(sanitized_record)
        logger.info("Successfully saved single record to Firestore.")
    except Exception as e:
        logger.error(f"Error saving single record to Firestore: {e}")
        raise

def _load_data(collection_name: str, filters: Optional[List[tuple]] = None) -> List[Dict]:
    """
    지정된 컬렉션의 모든 문서를 로드하거나, 필터가 제공되면 해당 필터를 적용하여 문서를 로드합니다.
    
    :param collection_name: Firestore 컬렉션 이름
    :param filters: 필터 리스트 (예: [('field', '==', 'value'), ...])
    :return: 문서의 리스트
    """
    try:
        collection_ref = db.collection(collection_name)
        
        if filters:
            query = collection_ref
            for field, op, value in filters:
                query = query.where(field, op, value)
            docs = query.stream()
            logger.info(f"Loaded filtered records from Firestore collection '{collection_name}' with filters: {filters}")
        else:
            docs = collection_ref.stream()
            logger.info(f"Loaded all records from Firestore collection '{collection_name}'.")
        
        data = []
        for doc in docs:
            record = doc.to_dict()
            record['id'] = doc.id  # 문서 ID를 포함
            data.append(record)
        logger.info(f"Total records loaded: {len(data)}")
        return data
    except Exception as e:
        logger.error(f"Error loading data from Firestore collection '{collection_name}': {e}")
        return []
    
def _load_single_record(collection_name: str, doc_id: str) -> Optional[Dict]:
    """
    지정된 컬렉션에서 특정 문서를 로드합니다.
    """
    try:
        doc_ref = db.collection(collection_name).document(doc_id)
        doc = doc_ref.get()
        if doc.exists:
            record = doc.to_dict()
            record['id'] = doc.id
            logger.info(f"Loaded record with ID '{doc_id}' from Firestore collection '{collection_name}'.")
            return record
        else:
            logger.warning(f"Document with ID '{doc_id}' does not exist in collection '{collection_name}'.")
            return None
    except Exception as e:
        logger.error(f"Error loading document '{doc_id}' from Firestore collection '{collection_name}': {e}")
        return None

from GoogleSheets.sheets import get_or_create_spreadsheet
async def load_data_from_sheets_and_save_to_firestore(spreadsheet_title: str, collection_name: str):
    try:
        # Google Sheets에서 데이터 가져오기
        folder_id = os.getenv('PIPELINE_FOLDER_ID')
        spreadsheet = get_or_create_spreadsheet(folder_id, spreadsheet_title)
        worksheet = spreadsheet.sheet1
        
        all_data = worksheet.get_all_values()
        if not all_data or all_data == [[]]:
            logger.warning("스프레드시트가 비어 있습니다.")
            return

        # 첫 번째 행을 헤더로 사용하고 나머지 행을 데이터로 처리
        headers = all_data[0]
        data_rows = all_data[1:]

        # 헤더와 데이터를 매핑하여 딕셔너리 리스트 생성
        data = []
        for row in data_rows:
            try:
                # 각 행의 데이터를 딕셔너리로 매핑
                row_data = {headers[i]: value for i, value in enumerate(row) if i < len(headers)}
                data.append(row_data)
            except Exception as e:
                logger.error(f"데이터 매핑 중 오류 발생: {e}")
                continue

        # Firestore에 저장
        if data:
            await save_to_firestore(collection_name, data)
            logger.info(f"{len(data)}개의 데이터를 Firestore 컬렉션 '{collection_name}'에 저장했습니다.")
        else:
            logger.warning("저장할 데이터가 없습니다.")

    except Exception as e:
        logger.error(f"데이터 로드 또는 저장 중 오류 발생: {e}")