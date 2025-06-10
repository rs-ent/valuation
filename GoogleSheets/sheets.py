import os
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

from utils.logger import setup_logger
logger = setup_logger(__name__)

def get_or_create_spreadsheet(folder_id, title):
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    # 서비스 계정 인증
    credentials = Credentials.from_service_account_file(
        os.getenv('SERVICE_ACCOUNT_FILE'),
        scopes=SCOPES
    )

    gc = gspread.authorize(credentials)
    service = build('drive', 'v3', credentials=credentials)

    query = f"name = '{title}' and mimeType = 'application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    items = results.get('files', [])

    if items:
        logger.info(f"스프레드시트 '{title}'을(를) 찾았습니다.")
        spreadsheet_id = items[0]['id']
    else:
        logger.info(f"스프레드시트 '{title}'을(를) 생성합니다.")
        spreadsheet = gc.create(title)
        spreadsheet_id = spreadsheet.id
        # 생성된 스프레드시트를 지정된 폴더로 이동
        service.files().update(
            fileId=spreadsheet_id,
            addParents=folder_id,
            removeParents='root',
            fields='id, parents'
        ).execute()
    return gc.open_by_key(spreadsheet_id)

def write_data(worksheet, data, start_row=2):
    try:
        if isinstance(data, list) and all(isinstance(row, list) for row in data):
            worksheet.insert_rows(data, row=start_row)
            logger.info(f"총 {len(data)}개의 데이터를 스프레드시트에 추가했습니다.")
        elif isinstance(data, list) and all(isinstance(row, dict) for row in data):
            headers = worksheet.row_values(1)
            rows = []
            for row_dict in data:
                row = [row_dict.get(header, "") for header in headers]
                rows.append(row)
            worksheet.insert_rows(rows, row=start_row)
            logger.info(f"총 {len(rows)}개의 데이터를 스프레드시트에 추가했습니다.")
        else:
            logger.error("데이터 형식이 올바르지 않습니다.")
    except Exception as e:
        logger.error(f"데이터를 스프레드시트에 쓰는 중 오류 발생: {e}")

def read_data(worksheet, start_row=2, end_row=None, start_col=1, end_col=None):
    try:
        if end_row and end_col:
            cell_range = f"{gspread.utils.rowcol_to_a1(start_row, start_col)}:{gspread.utils.rowcol_to_a1(end_row, end_col)}"
            cells = worksheet.get(cell_range)
        else:
            cells = worksheet.get_all_values()

        headers = cells[0]
        data = []
        for row in cells[start_row-1:end_row]:
            row_dict = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
            data.append(row_dict)
        logger.info(f"스프레드시트에서 총 {len(data)}개의 데이터를 읽어왔습니다.")
        return data
    except Exception as e:
        logger.error(f"스프레드시트에서 데이터를 읽는 중 오류 발생: {e}")
        return []

def update_data(worksheet, row, col, value):
    try:
        worksheet.update_cell(row, col, value)
        logger.info(f"셀 ({row}, {col})을(를) '{value}'로 업데이트했습니다.")
    except Exception as e:
        logger.error(f"셀 ({row}, {col})을 업데이트하는 중 오류 발생: {e}")

def delete_row(worksheet, row_number):
    try:
        worksheet.delete_rows(row_number)
        logger.info(f"행 {row_number}을(를) 삭제했습니다.")
    except Exception as e:
        logger.error(f"행 {row_number}을 삭제하는 중 오류 발생: {e}")
