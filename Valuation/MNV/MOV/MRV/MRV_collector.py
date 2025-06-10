##### Valuation/MNV/MOV/MRV/MRV_collector.py #####

import os
import time
import csv
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 환경 변수 로드
load_dotenv()
env_names = os.getenv("NAVER_BROADCAST_TAB_NAMES", "").split("|")
env_urls = os.getenv("NAVER_BROADCAST_TAB_URLS", "").split("|")
envdata = [{"name": name, "url": url} for name, url in zip(env_names, env_urls)]

# 로깅 설정
from utils.logger import setup_logger
logger = setup_logger(__name__)

def scrape_event_details(driver, wait, event_url):
    details = {}
    try:
        driver.get(event_url)
        logger.info(f"상세 페이지로 이동: {event_url}")

        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.cm_content_wrap'))
        )
        logger.info("상세 페이지 로드 완료.")

        try:
            info_groups = driver.find_elements(By.CLASS_NAME, 'info_group')
            for group in info_groups:
                dt_elements = group.find_elements(By.TAG_NAME, 'dt')
                dd_elements = group.find_elements(By.TAG_NAME, 'dd')
                for dt, dd in zip(dt_elements, dd_elements):
                    key = dt.text.strip()
                    if '채널' in key or '편성' in key:
                        # 'channels' 키가 없다면 빈 리스트로 초기화
                        if "channels" not in details:
                            details["channels"] = []
                        # '채널'이나 '편성' 항목의 링크 텍스트를 리스트에 추가
                        channel_links = dd.find_elements(By.TAG_NAME, 'a')
                        channels = [link.text.strip() for link in channel_links if link.text.strip()]
                        details["channels"].extend(channels)
                    else:
                        if dd.find_elements(By.TAG_NAME, 'a'):
                            value = [a.text.strip() for a in dd.find_elements(By.TAG_NAME, 'a')]
                        else:
                            value = dd.text.strip() 

                        details[key] = value
                    
                    logger.info(f"추출된 {key}: {value}")

        except Exception as e:
            logger.warning(f"'채널' 또는 '편성' 정보를 추출하는 중 오류 발생: {e}")

    except Exception as e:
        logger.error(f"상세 페이지 {event_url}에서 오류 발생: {e}")

    finally:
        return details

def get_naver_broadcast_data():

    ARTIST_ID = os.getenv('ARTIST_ID')
    ARTIST_NAME_KOR = os.getenv('ARTIST_NAME_KOR')
    ARTIST_NAME_ENG = os.getenv('ARTIST_NAME_ENG')
    MELON_ARTIST_ID = os.getenv('MELON_ID')
    
    # Selenium WebDriver 설정
    options = Options()
    options.add_argument("--headless")  # 디버깅 시 주석 처리하여 브라우저 창을 표시
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")  # 헤드리스 시에도 창 크기 설정

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    broadcast = []
    for env_data in envdata:
        env_name = env_data.get('name')
        base_url = env_data.get('url')
        if not base_url:
            logger.error("환경 변수 'NAVER_BROADCAST_TAB'가 설정되지 않았습니다.")
            exit(1)

        logger.info(f"Base URL: {base_url}")
        driver.get(base_url)

        try:
            try:
                tab_container = driver.find_element(By.CLASS_NAME, 'area_scroll_date')
                tabs = tab_container.find_elements(By.CLASS_NAME, '_tab')
                logger.info(f"탭 수: {len(tabs)}")

                broadcast_tab = None
                for tab in tabs:
                    tab_text = tab.text.strip()
                    logger.info(f"탭 텍스트: {tab_text}")
                    if '방송' in tab_text:
                        broadcast_tab = tab
                        break

                if broadcast_tab:
                    broadcast_tab.find_element(By.TAG_NAME, 'a').click()
                    logger.info("'방송' 탭을 클릭했습니다.")

                    wait.until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, 'div._tab_content._tab_area_onair'))
                    )
                    logger.info("'방송' 탭의 콘텐츠가 로드되었습니다.")
                else:
                    logger.error("'방송' 탭을 찾을 수 없습니다.")

            except Exception as e:
                logger.warning(f"'방송' 탭을 클릭하는 중 오류 발생, '방송' 탭 버튼을 클릭하지 않아도 되는 경우일 수 있습니다: {e}")

            current_page = 1

            while True:
                # 페이지 로딩 대기
                wait = WebDriverWait(driver, 5)  # 대기 시간을 늘림
                try:
                    onair_content = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div._tab_content._tab_area_onair'))
                    )
                    logger.info("On air 로드 완료.")
                except Exception as e:
                    logger.error(f"On air를 찾는 중 오류 발생: {e}")
                    break

                # 총 페이지 수와 현재 페이지 수 추출
                try:
                    total_pages_text = onair_content.find_element(By.CLASS_NAME, '_total').text.strip()
                    # 페이지 수가 비어 있으면 1로 설정
                    total_pages = int(total_pages_text) if total_pages_text else 1
                    logger.info(f"총 페이지 수: {total_pages}")
                except Exception as e:
                    logger.error(f"총 페이지 수를 찾는 중 오류 발생, 총 페이지가 1페이지일 수 있습니다: {e}")
                    total_pages = 1  # 오류 발생 시 기본값 설정

                try:
                    current_page_element = onair_content.find_element(By.CSS_SELECTOR, '.npgs_now._current')
                    current_page_text = current_page_element.text.strip()
                    current_page = int(current_page_text) if current_page_text else 1  # 비어 있으면 1로 설정
                    logger.info(f"현재 페이지: {current_page}")
                except Exception as e:
                    logger.error(f"현재 페이지를 찾는 중 오류 발생: {e}")
                    current_page = 1  # 오류 발생 시 기본값 설정
                    break
                
                time.sleep(5)
                # 가시적인 card_area 찾기
                try:
                    card_areas = onair_content.find_elements(By.CLASS_NAME, 'card_area')
                    logger.info(f"'방송' 탭에서 찾은 card_area 요소 수: {len(card_areas)}")

                    for idx, card_area in enumerate(card_areas, start=1):
                        logger.info(f"card_area {idx}: is_displayed={card_area.is_displayed()} - {card_area}")

                    visible_card_area = next((card_area for card_area in card_areas if card_area.is_displayed()), None)
                    if not visible_card_area:
                        logger.error("'방송' 탭에서 가시적인 card_area를 찾을 수 없습니다.")
                        break
                    else:
                        broadcast_items = visible_card_area.find_elements(By.CLASS_NAME, 'card_item')
                        if not broadcast_items:
                            logger.warning("가시적인 card_area에서 방송 아이템을 찾을 수 없습니다.")
                            break
                        else:
                            logger.info(f"방송 아이템 수: {len(broadcast_items)}")
                except Exception as e:
                    logger.error(f"방송 아이템을 찾는 중 오류 발생: {e}")
                    break

                # 방송 아이템 추출
                for index, item in enumerate(broadcast_items, start=1):
                    try:
                        # 제목 추출
                        title_element = item.find_element(By.CSS_SELECTOR, 'strong.this_text a._text')
                        title = title_element.text.strip()

                        # 상세 페이지 링크 추출
                        event_url = title_element.get_attribute('href').strip()

                        # 이미지 URL과 대체 텍스트 추출
                        img_element = item.find_element(By.CSS_SELECTOR, 'a.img_box img.bg_nimg')
                        image_url = img_element.get_attribute('src').strip()
                        image_alt = img_element.get_attribute('alt').strip()

                        # 정보 그룹 추출
                        info_groups = item.find_elements(By.CLASS_NAME, 'info_group')
                        if len(info_groups) < 1:
                            logger.warning(f"방송 아이템 {index}에서 정보 그룹이 부족합니다.")
                            start_period = ""
                            end_period = ""
                        else:
                            # 기간 처리: '~'가 있으면 시작과 끝을 따로, 없으면 동일하게 저장
                            try:
                                period_element = info_groups[-1].find_element(By.TAG_NAME, 'dd')
                                period_html = period_element.get_attribute('innerHTML').replace('<br>', ' ').replace('\n', ' ').strip()
                                period_text = period_html
                                if '~' in period_text:
                                    parts = period_text.split('~')
                                    start_period = parts[0].strip()
                                    end_period = parts[1].strip()
                                else:
                                    start_period = end_period = period_text
                            except Exception as e:
                                logger.warning(f"공연 아이템 {index}에서 기간을 추출하는 중 오류 발생: {e}")
                                start_period = end_period = ""
                        
                        broadcast.append({
                            "artists": env_name,
                            "category": "방송",
                            "title": title,
                            "event_url": event_url,
                            "image_url": image_url,
                            "image_alt": image_alt,
                            "start_period": start_period,
                            "end_period": end_period,
                            "artist_id": ARTIST_ID,
                            "artist_name_kor": ARTIST_NAME_KOR,
                            "artist_name_eng": ARTIST_NAME_ENG,
                            "melon_artist_id": MELON_ARTIST_ID,
                            "revenue": 0,
                        })

                    except Exception as e:
                        logger.error(f"공연 아이템 {index}에서 정보를 추출하는 중 오류 발생: {e}")
                        continue

                # 다음 페이지로 이동
                if current_page >= total_pages:
                    logger.info("마지막 페이지에 도달했습니다.")
                    break

                try:
                    next_button = onair_content.find_element(By.CSS_SELECTOR, '[data-kgs-page-action-next]')
                    next_button_classes = next_button.get_attribute('class')
                    if 'on' not in next_button_classes:
                        logger.info("다음 페이지 버튼이 비활성화되었습니다.")
                        break

                    # 이전 페이지 번호 저장
                    previous_page = current_page

                    # 다음 페이지 버튼 클릭
                    ActionChains(driver).move_to_element(next_button).click().perform()
                    logger.info("다음 페이지 버튼을 클릭했습니다.")

                    # 페이지 전환 후 현재 페이지 번호가 변경될 때까지 대기
                    wait.until(
                        lambda d: int(d.find_element(By.CSS_SELECTOR, '.npgs_now._current').text) > previous_page
                    )
                    logger.info("페이지 번호가 업데이트되었습니다.")

                    # 페이지가 완전히 로드될 때까지 추가 대기
                    time.sleep(5)  # 필요에 따라 조정

                except Exception as e:
                    logger.error(f"다음 페이지 버튼을 클릭하는 중 오류 발생: {e}")
                    break

        except Exception as e:
            logger.error(f"스크립트 실행 중 오류 발생: {e}")

        finally:
            for event in broadcast:
                try:
                    additional_details = scrape_event_details(driver, wait, event["event_url"])
                    event.update(additional_details)
                except Exception as e:
                    logger.error(f"상세 정보 수집 중 오류 발생: {e}")
                    continue
                
                print(f"아티스트명: {event['artists']}")
                print(f"카테고리: {event['category']}")
                print(f"아티스트명: {event['artist_name_kor']} ({event['artist_name_eng']})")
                print(f"이벤트 제목: {event['title']}")
                print(f"이벤트 링크: {event['event_url']}")
                print(f"이미지 URL: {event['image_url']}")
                print(f"이미지 설명: {event['image_alt']}")
                print(f"시작일: {event['start_period']}")
                print(f"종료일: {event['end_period']}")
                print(f"채널: {','.join(event['channels']) if event['channels'] else '정보 없음'}")
                print("-" * 50)

    # 드라이버 종료
    driver.quit()
    return broadcast


from Valuation.utils.processor import get_or_create_spreadsheet, read_data, write_data, load_data_from_sheets_and_save_to_firestore
def get_naver_broadcast_data_and_save_to_googlesheet():
    
    load_dotenv()
    folder_id = os.getenv('PIPELINE_FOLDER_ID')
    if not folder_id:
        logger.error("환경 변수 'PIPELINE_FOLDER_ID'가 설정되지 않았습니다.")
        exit(1)

    spreadsheet_title = '방송 데이터'
    spreadsheet = get_or_create_spreadsheet(folder_id, spreadsheet_title)

    worksheet = spreadsheet.sheet1
    
    worksheet_data = worksheet.get_all_values()
    if not worksheet_data or worksheet_data == [[]]:
        headers = ["title","event_url","image_url","image_alt","start_period","end_period","artist_id","artist_name_kor","artist_name_eng","melon_artist_id","revenue"]
        worksheet.append_row(headers)
        logger.info("헤더를 추가했습니다.")
    else:
        logger.info("헤더가 이미 존재합니다.")

    if env_urls and len(env_urls) > 0:
        data = get_naver_broadcast_data()
    else:
        data = []

    existing_data = read_data(worksheet, start_row=2)
    existing_urls = set(row['event_url'] for row in existing_data)

    new_data = [event for event in data if event['event_url'] not in existing_urls]
    start_row = len(worksheet_data) + 1

    if new_data:
        write_data(worksheet, new_data, start_row=start_row)
        logger.info(f"총 {len(new_data)}개의 새로운 공연 데이터를 스프레드시트에 추가했습니다.")
    else:
        logger.info("추가할 새로운 방송 데이터가 없습니다.")
    
    all_data = existing_data + new_data

    return all_data

def load_broadcast_data_from_sheet_and_save_to_firestore():
    data = get_naver_broadcast_data_and_save_to_googlesheet()
    return data


import asyncio
from Valuation.firebase.firebase_handler import save_record, check_record
DATA_TARGET='MRV_collector'
def mrv_collector():
    load_data = check_record(DATA_TARGET, DATA_TARGET, 'events')
    if load_data:
        print(f'{DATA_TARGET} Loaded')
        return load_data.get(DATA_TARGET)
    
    data = load_broadcast_data_from_sheet_and_save_to_firestore()
    asyncio.run(load_data_from_sheets_and_save_to_firestore(spreadsheet_title = "방송 데이터", collection_name = "broadcast"))
    result = {
        "events": data
    }
    print(result)

    save_record(DATA_TARGET, result, DATA_TARGET, 'events')
    return result