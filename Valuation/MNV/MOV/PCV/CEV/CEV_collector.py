##### Valuation/MNV/MOV/PCV/CEV/CEV_collector.py #####
'''
CEV_collector.py는 NAVER 콘서트 탭에서 아티스트 공연 데이터를 Selenium을 통해 자동 수집함
dotenv와 환경변수로 기본 URL 및 아티스트 정보를 동적 로드함
ChromeDriverManager와 headless 옵션을 사용하여 Selenium WebDriver를 구성함
WebDriverWait, Expected Conditions, ActionChains를 활용해 공연 탭 클릭 및 페이지 네비게이션을 수행함
각 공연 아이템에서 제목, 링크, 이미지, 장소, 공연 기간 등의 정보를 추출함
추출한 데이터를 딕셔너리 리스트로 구성하여 CSV 파일 및 Google Sheets에 기록함
유틸리티 함수로 중복 데이터 검증 및 신규 공연 데이터 필터링을 적용함
비동기 처리로 Google Sheets 데이터를 Firestore에 동기화하여 클라우드 저장을 구현함
전체 파이프라인은 환경 설정, 데이터 수집, 전처리, 저장 및 동기화를 포함하여 공연 이벤트 데이터를 효율적으로 관리함
'''

import os
import time
from datetime import datetime
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from dotenv import load_dotenv
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 로깅 설정
from utils.logger import setup_logger
logger = setup_logger(__name__)


def get_naver_concert_data():
    # 환경 변수 로드
    load_dotenv()
    base_url = os.getenv('NAVER_CONCERT_TAB')
    ARTIST_ID = os.getenv('ARTIST_ID')
    ARTIST_NAME_KOR = os.getenv('ARTIST_NAME_KOR')
    ARTIST_NAME_ENG = os.getenv('ARTIST_NAME_ENG')
    MELON_ARTIST_ID = os.getenv('MELON_ID')

    if not base_url:
        logger.error("환경 변수 'NAVER_CONCERT_TAB'가 설정되지 않았습니다.")
        exit(1)

    logger.info(f"Base URL: {base_url}")

    # Selenium WebDriver 설정
    options = Options()
    options.add_argument("--headless")  # 디버깅 시 주석 처리
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(base_url)

    wait = WebDriverWait(driver, 60)
    concerts = []
    current_page = 1

    try:
        # 공연 탭 클릭
        try:
            performance_tab = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//li[contains(@class, '_tab')]/a[contains(.//span, '공연')]")
                )
            )
            performance_tab.click()
            logger.info("공연 탭 클릭 완료.")
        except Exception as e:
            logger.error(f"공연 탭 클릭 중 오류 발생: {e}")
            return []

        # 공연 탭 내용 로딩 대기
        time.sleep(5)

        while True:
            # 페이지 로딩 대기
            time.sleep(5)
            try:
                play_area = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div._tab_content._tab_area_play'))
                )
                logger.info("Play area 로드 완료.")
            except Exception as e:
                logger.error(f"Play area를 찾는 중 오류 발생: {e}")
                break

            # 총 페이지 수와 현재 페이지 수 추출
            try:
                total_pages_text = play_area.find_element(By.CLASS_NAME, '_total').text.strip()
                total_pages = int(total_pages_text) if total_pages_text else 1
                logger.info(f"총 페이지 수: {total_pages}")
            except Exception as e:
                logger.error(f"총 페이지 수를 찾는 중 오류 발생: {e}")
                total_pages = 1

            try:
                current_page_element = play_area.find_element(By.CSS_SELECTOR, '.npgs_now._current')
                current_page_text = current_page_element.text.strip()
                current_page = int(current_page_text) if current_page_text else 1
                logger.info(f"현재 페이지: {current_page}")
            except Exception as e:
                logger.error(f"현재 페이지 번호를 찾는 중 오류 발생: {e}")
                current_page = 1

            # 현재 표시되는 card_area 찾기
            try:
                card_areas = play_area.find_elements(By.CLASS_NAME, 'card_area')
                visible_card_area = next((c for c in card_areas if c.is_displayed()), None)
                if not visible_card_area:
                    logger.error("가시적인 card_area를 찾을 수 없습니다.")
                    break
                concert_items = visible_card_area.find_elements(By.CLASS_NAME, 'card_item')
                logger.info(f"공연 아이템 수: {len(concert_items)}")
            except Exception as e:
                logger.error(f"공연 아이템을 찾는 중 오류 발생: {e}")
                break

            # 공연 아이템 추출
            for index, item in enumerate(concert_items, start=1):
                try:
                    title_element = item.find_element(By.CSS_SELECTOR, 'strong.this_text a._text')
                    title = title_element.text.strip()
                    concert_url = title_element.get_attribute('href').strip()

                    img_element = item.find_element(By.CSS_SELECTOR, 'a.img_box img.bg_nimg')
                    image_url = img_element.get_attribute('src').strip()
                    image_alt = img_element.get_attribute('alt').strip()

                    info_groups = item.find_elements(By.CLASS_NAME, 'info_group')
                    if len(info_groups) < 2:
                        logger.warning(f"공연 아이템 {index}에서 정보 그룹이 부족합니다.")
                        location = ""
                        start_period = ""
                        end_period = ""
                    else:
                        # 장소
                        try:
                            location = info_groups[0].find_element(By.TAG_NAME, 'dd').text.strip()
                        except:
                            location = ""

                        # 기간
                        try:
                            period_element = info_groups[1].find_element(By.TAG_NAME, 'dd')
                            period_text = period_element.text.replace('\n', ' ').strip()
                            if '~' in period_text:
                                parts = period_text.split('~')
                                start_period = parts[0].strip()
                                end_period = parts[1].strip()
                            else:
                                start_period = end_period = period_text
                        except:
                            start_period = end_period = ""

                    concerts.append({
                        "title": title,
                        "concert_url": concert_url,
                        "image_url": image_url,
                        "image_alt": image_alt,
                        "location": location,
                        "start_period": start_period,
                        "end_period": end_period,
                        "artist_id": ARTIST_ID,
                        "artist_name_kor": ARTIST_NAME_KOR,
                        "artist_name_eng": ARTIST_NAME_ENG,
                        "melon_artist_id": MELON_ARTIST_ID,
                        "revenue": 0
                    })

                except Exception as e:
                    logger.error(f"공연 아이템 {index}에서 정보를 추출하는 중 오류 발생: {e}")
                    continue

            # 다음 페이지로 이동할지 결정
            if current_page >= total_pages:
                logger.info("마지막 페이지에 도달했습니다.")
                break

            # 다음 페이지 버튼 확인
            try:
                next_button = play_area.find_element(By.CSS_SELECTOR, '[data-kgs-page-action-next]')
                next_button_classes = next_button.get_attribute('class')
                if 'on' not in next_button_classes:
                    logger.info("다음 페이지 버튼이 비활성화되었습니다.")
                    break
            except Exception as e:
                logger.error(f"다음 페이지 버튼을 찾는 중 오류 발생: {e}")
                break

            # 다음 페이지 아이템 변화 대기용: 현재 첫 아이템 정보 확보
            first_item_url_before = ""
            if concert_items:
                try:
                    first_item_title = concert_items[0].find_element(By.CSS_SELECTOR, 'strong.this_text a._text')
                    first_item_url_before = first_item_title.get_attribute('href')
                except:
                    first_item_url_before = ""

            # 다음 페이지 버튼 클릭
            try:
                ActionChains(driver).move_to_element(next_button).click().perform()
                logger.info("다음 페이지 버튼을 클릭했습니다.")
            except Exception as e:
                logger.error(f"다음 페이지 버튼 클릭 중 오류 발생: {e}")
                break

            # 다음 페이지 로딩 대기: 첫 아이템 변화까지
            def items_changed(driver):
                try:
                    new_play_area = driver.find_element(By.CSS_SELECTOR, 'div._tab_content._tab_area_play')
                    new_card_areas = new_play_area.find_elements(By.CLASS_NAME, 'card_area')
                    new_visible_card_area = next((c for c in new_card_areas if c.is_displayed()), None)
                    if not new_visible_card_area:
                        return False
                    new_concert_items = new_visible_card_area.find_elements(By.CLASS_NAME, 'card_item')
                    if not new_concert_items:
                        return False

                    first_item_title_new = new_concert_items[0].find_element(By.CSS_SELECTOR, 'strong.this_text a._text')
                    first_item_url_new = first_item_title_new.get_attribute('href')

                    return first_item_url_new != first_item_url_before
                except:
                    return False

            try:
                WebDriverWait(driver, 120).until(items_changed)
                logger.info("다음 페이지 컨텐츠가 로드되었습니다.")
            except TimeoutException:
                logger.error("다음 페이지 로딩 대기 중 타임아웃 발생")
                break

    except Exception as e:
        logger.error(f"스크립트 실행 중 오류 발생: {e}")

    finally:
        # 결과 출력
        for concert in concerts:
            print(f"아티스트명: {concert['artist_name_kor']} ({concert['artist_name_eng']})")
            print(f"공연 제목: {concert['title']}")
            print(f"공연 링크: {concert['concert_url']}")
            print(f"이미지 URL: {concert['image_url']}")
            print(f"이미지 설명: {concert['image_alt']}")
            print(f"장소: {concert['location']}")
            print(f"시작일: {concert['start_period']}")
            print(f"종료일: {concert['end_period']}")
            print("-" * 50)

        driver.quit()
        return concerts

from Valuation.utils.processor import get_or_create_spreadsheet, read_data, write_data, load_data_from_sheets_and_save_to_firestore
def get_naver_concert_data_and_save_to_googlesheet():
    
    load_dotenv()
    folder_id = os.getenv('PIPELINE_FOLDER_ID')
    if not folder_id:
        logger.error("환경 변수 'PIPELINE_FOLDER_ID'가 설정되지 않았습니다.")
        exit(1)

    spreadsheet_title = '공연 데이터'
    spreadsheet = get_or_create_spreadsheet(folder_id, spreadsheet_title)

    worksheet = spreadsheet.sheet1
    
    worksheet_data = worksheet.get_all_values()
    if not worksheet_data or worksheet_data == [[]]:
        headers = ["title", "concert_url", "image_url", "image_alt", "location", "start_period", "end_period", "artist_id", "artist_name_kor", "artist_name_eng", "melon_artist_id", "revenue"]
        worksheet.append_row(headers)
        logger.info("헤더를 추가했습니다.")
    else:
        logger.info("헤더가 이미 존재합니다.")

    data = get_naver_concert_data()
    existing_data = read_data(worksheet, start_row=2)
    existing_urls = set(row['concert_url'] for row in existing_data)

    new_data = [concert for concert in data if concert['concert_url'] not in existing_urls]
    start_row = len(worksheet_data) + 1

    if new_data:
        write_data(worksheet, new_data, start_row=start_row)
        logger.info(f"총 {len(new_data)}개의 새로운 공연 데이터를 스프레드시트에 추가했습니다.")
    else:
        logger.info("추가할 새로운 공연 데이터가 없습니다.")
    
    all_data = existing_data + new_data

    return all_data

def load_performance_data_from_sheet_and_save_to_firestore():
    data = get_naver_concert_data_and_save_to_googlesheet()
    return data


import asyncio
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='CEV_collector'
def cev_collector():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'events')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    data = load_performance_data_from_sheet_and_save_to_firestore()
    asyncio.run(load_data_from_sheets_and_save_to_firestore(spreadsheet_title = "공연 데이터", collection_name = "performance"))
    result = {
        "events": data,
        "collected_time": datetime.now().strftime("%Y-%m-%d")
    }
    print(f'CEV collector result : {result}')

    save_record(DATA_TARGET, result, DATA_TARGET, 'events')
    return result