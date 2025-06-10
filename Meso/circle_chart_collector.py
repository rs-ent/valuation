import httpx
import asyncio
from typing import Dict, Any, List
from dotenv import load_dotenv
import os
import sys
import gzip
import zlib
import json
import brotli
import re

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.logger import setup_logger

load_dotenv()

logger = setup_logger(__name__)

COOKIES = {
    "_gid": os.getenv("COOKIE__gid", "GA1.2.1175521585.1729883435"),
    "perf_dv6Tr4n": os.getenv("COOKIE_perf_dv6Tr4n", "1"),
    "_ga": os.getenv("COOKIE__ga", "GA1.2.224839636.1729661617"),
    # 추가 쿠키 필요 시 여기에 추가
}

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    # "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,ko;q=0.8",
    "cache-control": "no-cache",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "dnt": "1",
    "origin": "https://circlechart.kr",
    "pragma": "no-cache",
    "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
    "x-requested-with": "XMLHttpRequest",
}

async def fetch_api_data(client: httpx.AsyncClient, url: str, data: Dict[str, Any]) -> Any:
    try:
        response = await client.post(url, data=data)
        response.raise_for_status()

        # 수동 압축 해제 코드 제거
        return response.json()
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for {url}: {e}")
    except httpx.HTTPError as e:
        logger.error(f"HTTP error while fetching {url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while fetching {url}: {e}")
    return None

async def parse_streaming_data(client: httpx.AsyncClient, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    parsed_list = []
    list_data = raw_data.get("List", {})
    for key, item in list_data.items():
        try:
            parsed_item = {
                "title": item.get("SONG_NAME", ""),
                "album": item.get("ALBUM_NAME", ""),
                "artist": item.get("ARTIST_NAME", ""),
                "production": [comp.strip() for comp in item.get("MAKE_COMPANY_NAME", "").split(",")],
                "distribution": [comp.strip() for comp in item.get("DE_COMPANY_NAME", "").split(",")],
                "rank": int(item.get("SERVICE_RANKING", 0)),
                "rank_status": item.get("RankChange", ""),
                "album_img": f"https://circlechart.kr{item.get('ALBUMIMG', '')}",
                "seq_mom": item.get("SEQ_MOM", 0),
                "hit_count": int(item.get("HIT_CNT", 0)),
                "hit_ratio": int(item.get("HIT_RATIO", 0)),
            }
            try:
                melon_album_id = await get_melon_album_id(client, parsed_item["seq_mom"])
                if melon_album_id:
                    parsed_item["melon_album_id"] = melon_album_id
                    parsed_item["melon_album_url"] = f"https://www.melon.com/album/detail.htm?albumId={melon_album_id}"
                else:
                    parsed_item["melon_album_id"] = ''
                    parsed_item["melon_album_url"] = ''
            except Exception as e:
                logger.error(f"Error parsing melon album id for key {key}: {e}")
                parsed_item["melon_album_id"] = ''
                parsed_item["melon_album_url"] = ''

            parsed_list.append(parsed_item)
        except Exception as e:
            logger.error(f"Error parsing streaming data for key {key}: {e}")
    return parsed_list

async def parse_retail_data(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    parsed_list = []
    list_data = raw_data.get("List", {})
    for key, item in list_data.items():
        try:
            parsed_item = {
                "album": item.get("Album", "").replace("[수입] ", ""),
                "barcode": item.get("Barcode", ""),
                "sales": int(item.get("ESum", 0)) + int(item.get("KSum", 0)),
                "domestic_sales": int(item.get("KSum", 0)),
                "overseas_sales": int(item.get("ESum", 0)),
                "artist": item.get("Artist", ""),
                "distribution": [comp.strip() for comp in item.get("De_company_name", "").split(",")],
                "rank": int(item.get("RankOrder", 0)),
                "rank_status": item.get("RankStatus", ""),
                "album_img": f"https://circlechart.kr/uploadDir/{item.get('save_name', '').replace('\\', '/')}",
                "chart_data_domestic": [int(item.get(f"K_{i}", 0)) for i in range(1, 32)],
                "chart_data_overseas": [int(item.get(f"E_{i}", 0)) for i in range(1, 32)]
            }

            parsed_list.append(parsed_item)
        except Exception as e:
            logger.error(f"Error parsing retail data for key {key}: {e}")
    return parsed_list

async def parse_global_data(client: httpx.AsyncClient, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    parsed_list = []
    list_data = raw_data.get("List", {})
    for key, item in list_data.items():
        try:
            parsed_item = {
                "title": item.get("Title", ""),
                "album": item.get("Album", ""),
                "artist": [artist.strip() for artist in item.get("Artist", "").split(",")],
                "production": [prod.strip() for prod in item.get("CompanyMake", "").split(",")],
                "distribution": [dist.strip() for dist in item.get("CompanyDist", "").split(",")],
                "rank": int(item.get("Rank", 0)),
                "rank_status": item.get("RankStatus", ""),
                "album_img": f"https://circlechart.kr/{item.get('ALBUMIMG', '')}",
                "seq_mom": item.get("Seq_Mom", 0),
            }
            melon_album_id = await get_melon_album_id(client, parsed_item["seq_mom"])
            parsed_item["melon_album_id"] = melon_album_id
            parsed_item["melon_album_url"] = "https://www.melon.com/album/detail.htm?albumId=" + melon_album_id
            parsed_list.append(parsed_item)
        except Exception as e:
            logger.error(f"Error parsing global data for key {key}: {e}")
    return parsed_list


async def get_melon_album_id(client: httpx.AsyncClient, seq_mom: str) -> str:
    url = "https://circlechart.kr/data/api/chart/onoff/play"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://circlechart.kr",
        "Referer": "https://circlechart.kr/page_chart/onoff.circle?serviceGbn=S1040&termGbn=month",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    }
    data = {
        "seq_mom": seq_mom,
        "seq_company": "3715",  # 동일하게 유지되는 다른 필드 값
    }

    try:
        response = await client.post(url, headers=headers, data=data)
        response.raise_for_status()
        response_json = response.json()

        list_data = response_json.get("List", {})
        if not list_data:
            logger.warning(f"No List data found for seq_mom: {seq_mom}")
            return ""

        # 첫 번째 return_url에서 albumId 추출 (필요에 따라 모든 return_url을 순회할 수 있음)
        first_key = next(iter(list_data))
        return_url = list_data[first_key].get("return_url", "")
        if not return_url:
            logger.warning(f"No return_url found for seq_mom: {seq_mom}")
            return ""

        # 정규표현식을 사용하여 albumId 추출
        match = re.search(r'albumId=(\d+)', return_url)
        if match:
            album_id = match.group(1)
            return album_id
        else:
            logger.warning(f"albumId not found in return_url: {return_url}")
            return ""

    except httpx.HTTPError as e:
        logger.error(f"HTTP error while fetching Melon album ID for seq_mom {seq_mom}: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error while fetching Melon album ID for seq_mom {seq_mom}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while fetching Melon album ID for seq_mom {seq_mom}: {e}")

    return ""


async def get_circle_chart(yyyymm: str) -> Dict[str, Any]:

    year = yyyymm[:4]
    month = yyyymm[-2:]

    base_url = "https://circlechart.kr/data/api/chart"
    endpoints = {
        "streaming": "onoff",
        "retail": "retail_list",
        "global": "global",
    }

    # 각 엔드포인트에 필요한 POST 데이터 구성
    post_data = {
        "streaming": {
            "PageSize": "200",
            "curUrl": f"circlechart.kr/page_chart/onoff.circle?nationGbn=T&serviceGbn=S1040&targetTime={month}&hitYear={year}&termGbn=month&yearTime=3",
            "hitYear": year,
            "nationGbn": "T",
            "serviceGbn": "S1040",
            "targetTime": month,
            "termGbn": "month",
            "yearTime": "3",
        },
        "retail": {
            "termGbn": "month",
            "yyyymmdd": yyyymm,
        },
        "global": {
            "termGbn": "month",
            "yyyymmdd": yyyymm,
        },
    }

    results = {
        "global": [],
        "streaming": [],
        "retail": [],
        "target": yyyymm
    }

    async with httpx.AsyncClient(headers=HEADERS, cookies=COOKIES, timeout=30.0) as client:
        tasks = []
        for key, endpoint in endpoints.items():
            url = f"{base_url}/{endpoint}"
            data = post_data.get(key, {})
            tasks.append(fetch_api_data(client, url, data))

        responses = await asyncio.gather(*tasks)

        for key, response in zip(endpoints.keys(), responses):
            if response:
                if key == "global":
                    parsed_global = await parse_global_data(client, response)
                    results[key] = parsed_global
                    logger.info(f"Fetched and parsed {len(parsed_global)} records for {key}")
                elif key == "streaming":
                    parsed_streaming = await parse_streaming_data(client, response)
                    results[key] = parsed_streaming
                    logger.info(f"Fetched and parsed {len(parsed_streaming)} records for {key}")
                elif key == "retail":
                    parsed_retail = await parse_retail_data(response)
                    results[key] = parsed_retail
                    logger.info(f"Fetched and parsed {len(parsed_retail)} records for {key}")
            else:
                logger.warning(f"No data fetched for {key}")

    return results