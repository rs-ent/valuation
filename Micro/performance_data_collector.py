import os
import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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
    options.add_argument("--headless")  # 디버깅 시 주석 처리하여 브라우저 창을 표시
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")  # 헤드리스 시에도 창 크기 설정

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(base_url)

    concerts = []
    current_page = 1

    try:
        while True:
            # 페이지 로딩 대기
            wait = WebDriverWait(driver, 20)  # 대기 시간을 늘림
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
                # 페이지 수가 비어 있으면 1로 설정
                total_pages = int(total_pages_text) if total_pages_text else 1
                logger.info(f"총 페이지 수: {total_pages}")
            except Exception as e:
                logger.error(f"총 페이지 수를 찾는 중 오류 발생, 총 페이지가 1페이지일 수 있습니다: {e}")
                total_pages = 1  # 오류 발생 시 기본값 설정

            try:
                current_page_element = play_area.find_element(By.CSS_SELECTOR, '.npgs_now._current')
                current_page_text = current_page_element.text.strip()
                current_page = int(current_page_text) if current_page_text else 1  # 비어 있으면 1로 설정
                logger.info(f"현재 페이지: {current_page}")
            except Exception as e:
                logger.error(f"현재 페이지를 찾는 중 오류 발생: {e}")
                current_page = 1  # 오류 발생 시 기본값 설정
                break

            # 가시적인 card_area 찾기
            try:
                card_areas = play_area.find_elements(By.CLASS_NAME, 'card_area')
                visible_card_area = None
                for card_area in card_areas:
                    if card_area.is_displayed():
                        visible_card_area = card_area
                        break
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
                    # 제목 추출
                    title_element = item.find_element(By.CSS_SELECTOR, 'strong.this_text a._text')
                    title = title_element.text.strip()

                    # 상세 페이지 링크 추출
                    concert_url = title_element.get_attribute('href').strip()

                    # 이미지 URL과 대체 텍스트 추출
                    img_element = item.find_element(By.CSS_SELECTOR, 'a.img_box img.bg_nimg')
                    image_url = img_element.get_attribute('src').strip()
                    image_alt = img_element.get_attribute('alt').strip()

                    # 정보 그룹 추출
                    info_groups = item.find_elements(By.CLASS_NAME, 'info_group')
                    if len(info_groups) < 2:
                        logger.warning(f"공연 아이템 {index}에서 정보 그룹이 부족합니다.")
                        location = ""
                        period = ""
                    else:
                        # 장소
                        try:
                            location = info_groups[0].find_element(By.TAG_NAME, 'dd').text.strip()
                        except Exception as e:
                            logger.warning(f"공연 아이템 {index}에서 장소를 추출하는 중 오류 발생: {e}")
                            location = ""

                        # 기간 처리: '~'가 있으면 시작과 끝을 따로, 없으면 동일하게 저장
                        try:
                            period_element = info_groups[1].find_element(By.TAG_NAME, 'dd')
                            period_text = period_element.text.replace('\n', ' ').strip()
                            if '~' in period_text:
                                parts = period_text.split('~')
                                start_period = parts[0].strip()
                                end_period = parts[1].strip()
                            else:
                                start_period = end_period = period_text
                        except Exception as e:
                            logger.warning(f"공연 아이템 {index}에서 기간을 추출하는 중 오류 발생: {e}")
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

            # 다음 페이지로 이동
            if current_page >= total_pages:
                logger.info("마지막 페이지에 도달했습니다.")
                break

            try:
                next_button = play_area.find_element(By.CSS_SELECTOR, '[data-kgs-page-action-next]')
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
                time.sleep(0.05)  # 필요에 따라 조정

            except Exception as e:
                logger.error(f"다음 페이지 버튼을 클릭하는 중 오류 발생: {e}")
                break

    except Exception as e:
        logger.error(f"스크립트 실행 중 오류 발생: {e}")

    finally:
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

        # 드라이버 종료
        driver.quit()
        return concerts

