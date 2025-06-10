# Macro/market_growth.py

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
import gspread
from google.oauth2.service_account import Credentials
from aiocache import cached, Cache, SimpleMemoryCache
from dotenv import load_dotenv
import os
from openai import OpenAI

from utils.logger import setup_logger

load_dotenv()

SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', 'rs_ent_contact.json')
SHEET_ID = os.getenv('SHEET_ID_MARKET_GROWTH')
SHEET_INDEX = int(os.getenv('SHEET_INDEX_MARKET_GROWTH', '0'))
OPENAIKEY = os.getenv('OPENAI_API_KEY')
client = OpenAI(
    api_key=OPENAIKEY,
)

logger = setup_logger(__name__)

cache = SimpleMemoryCache(ttl=600)

executor = ThreadPoolExecutor(max_workers=5)

def get_gspread_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    return client

async def fetch_sheet_data(sheet_id: str, sheet_index: int) -> Optional[List[Dict]]:
    loop = asyncio.get_event_loop()
    try:
        client = await loop.run_in_executor(executor, get_gspread_client)
        sheet = await loop.run_in_executor(executor, lambda: client.open_by_key(sheet_id))
        worksheet = await loop.run_in_executor(executor, lambda: sheet.get_worksheet(sheet_index))
        data = await loop.run_in_executor(executor, worksheet.get_all_records)
        logger.info(f"Fetched {len(data)} records from Google Sheet ID: {sheet_id}, Sheet Index: {sheet_index}")
                
        return data
    except Exception as e:
        logger.error(f"Failed to fetch data from Google Sheets: {e}")
        return None
    
async def generate_comment_openai(data: List[Dict]) -> Optional[str]:
    # 총 매출과 성장률 요약 프롬프트 (120자 내외)
    prompt = "다음 날짜별 평균 매출과 성장률 데이터에 기반해, 투자 분석 리포트를 위한 미래 성장 가능성과 시장 리스크를 120자 내외로 요약해 주세요:\n\n"
    for entry in data:
        prompt += f"- 날짜: {entry['date']}, 평균 매출: {entry['average_sales']}원, 성장률: {entry['sales_growth']}%\n"

    try:
        response = client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="gpt-3.5-turbo",
            max_tokens=200,
            temperature=0.5,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Failed to generate comment_openai: {e}")
        return None


async def generate_detail_comment_openai(data: List[Dict]) -> Optional[str]:
    '''
    "date": year,
    "average_sales": average_sales,
    "average_sales_music": average_sales_music,
    "average_sales_management": average_sales_management,
    "average_sales_contents": average_sales_contents,
    "sales_growth": sales_growth,
    "music_growth": music_growth,
    "management_growth": management_growth,
    "contents_growth": contents_growth
    '''
    prompt = (
        "음반/음원, 공연/콘텐츠/MD판매, 출연료/초상권 항목별 날짜별 매출과 성장률 데이터를 기반으로, 각 항목의 미래 성장 가능성과 리스크를 투자 관점에서 분석해 주세요. 분석은 230자 내외로 작성해 주세요:\n\n"
    )
    for entry in data:
        prompt += (
            f"- 날짜: {entry['date']}\n"
            f"  * 음반/음원 매출: {entry['average_sales_music']}원, 성장률: {entry['music_growth']}%\n"
            f"  * 공연/콘텐츠/MD판매 매출: {entry['average_sales_contents']}원, 성장률: {entry['contents_growth']}%\n"
            f"  * 출연료/초상권 매출: {entry['average_sales_management']}원, 성장률: {entry['management_growth']}%\n"
        )
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.5,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Failed to generate detail_comment_openai: {e}")
        return None


async def interpret_market_data(data: List[Dict]) -> Dict[str, Optional[str]]:
    # 1. 연도별 매출 데이터를 수집
    metrics_by_year = {}
    for entry in data:
        year = int(entry["date"])
        sales = int(entry["sales"])
        sales_music = int(entry["sales_music"]) if entry.get("sales_music") else 0
        sales_management = int(entry["sales_management"]) if entry.get("sales_management") else 0
        sales_contents = int(entry["sales_contents"]) if entry.get("sales_contents") else 0
        
        if year not in metrics_by_year:
            metrics_by_year[year] = {"sales": [], "sales_music": [], "sales_management": [], "sales_contents": []}
        
        metrics_by_year[year]["sales"].append(sales)
        metrics_by_year[year]["sales_music"].append(sales_music)
        metrics_by_year[year]["sales_management"].append(sales_management)
        metrics_by_year[year]["sales_contents"].append(sales_contents)

    # 2. 연도별 평균 매출 계산 및 데이터 업데이트
    processed_data = []
    sorted_years = sorted(metrics_by_year.keys())
    for i, year in enumerate(sorted_years):
        metrics = metrics_by_year[year]
        average_sales = sum(metrics["sales"]) / len(metrics["sales"])
        average_sales_music = sum(metrics["sales_music"]) / len(metrics["sales_music"])
        average_sales_management = sum(metrics["sales_management"]) / len(metrics["sales_management"])
        average_sales_contents = sum(metrics["sales_contents"]) / len(metrics["sales_contents"])

        # 성장률 계산 (이전 연도 데이터가 있는 경우)
        if i > 0:
            prev_year = sorted_years[i - 1]
            prev_metrics = metrics_by_year[prev_year]
            prev_avg_sales = sum(prev_metrics["sales"]) / len(prev_metrics["sales"])
            prev_avg_music = sum(prev_metrics["sales_music"]) / len(prev_metrics["sales_music"])
            prev_avg_management = sum(prev_metrics["sales_management"]) / len(prev_metrics["sales_management"])
            prev_avg_contents = sum(prev_metrics["sales_contents"]) / len(prev_metrics["sales_contents"])

            sales_growth = ((average_sales - prev_avg_sales) / prev_avg_sales) * 100 if prev_avg_sales else 0
            music_growth = ((average_sales_music - prev_avg_music) / prev_avg_music) * 100 if prev_avg_music else 0
            management_growth = ((average_sales_management - prev_avg_management) / prev_avg_management) * 100 if prev_avg_management else 0
            contents_growth = ((average_sales_contents - prev_avg_contents) / prev_avg_contents) * 100 if prev_avg_contents else 0
        else:
            sales_growth = music_growth = management_growth = contents_growth = 0  # 첫 연도는 성장률 0

        # 새 데이터 구조에 추가
        processed_data.append({
            "date": year,
            "average_sales": average_sales,
            "average_sales_music": average_sales_music,
            "average_sales_management": average_sales_management,
            "average_sales_contents": average_sales_contents,
            "sales_growth": sales_growth,
            "music_growth": music_growth,
            "management_growth": management_growth,
            "contents_growth": contents_growth
        })
        
    # 3. OpenAI API를 통한 요약 생성
    comment_openai = await generate_comment_openai(processed_data)
    detail_comment_openai = await generate_detail_comment_openai(processed_data)

    return {
        "comment_openai": comment_openai,
        "detail_comment_openai": detail_comment_openai
    }

@cached(ttl=600, cache=SimpleMemoryCache)
async def get_market_data_from_sheets(sheet_id: str = SHEET_ID, sheet_index: int = SHEET_INDEX) -> Optional[List[Dict]]:
    return await fetch_sheet_data(sheet_id, sheet_index)
