#0단계. artist data : 멜론 아티스트 데이터
'''
Melon 웹사이트에서 아티스트 상세 정보를 수집하여 Firebase에 저장하는 것을 목표로 함
캐시 체크를 통해 기존 데이터 중복 수집을 방지하는 로직을 구현함
HTTP GET 요청을 requests 모듈로 전송하여 아티스트 페이지의 HTML을 획득함
BeautifulSoup을 활용하여 HTML 문서를 파싱하고 주요 요소를 추출함
정규표현식으로 데뷔곡, 그룹 멤버 등 세부 데이터의 식별자를 추출함
환경변수를 통해 아티스트 관련 식별자 및 정보를 동적으로 불러옴
추가 API 호출로 JSON 응답을 받아 팬 수 데이터를 보완함
'''
#from Valuation.utils.data_collector import get_artist_data
#result_artist = get_artist_data()
#print(result_artist)

#1단계. APV : 스포티파이 인기도
'''
APV_main.py는 Spotify API를 활용하여 아티스트 앨범 데이터와 APV(Album Popularity Value)를 산출함
환경 변수에서 SPOTIFY_ID 및 아티스트 정보를 동적으로 불러옴
Firebase의 check_record 함수를 통해 기존 데이터 존재 여부를 확인함
SPOTIFY_ID가 존재할 경우 spotify_album_data 함수를 호출하여 아티스트 팔로워 수와 앨범 리스트를 수집함
각 앨범 내 트랙들의 인기도를 합산하여 album[‘track_popularity’]를 산출함
트랙 인기도와 아티스트 팔로워 수의 곱으로 개별 앨범의 APV 값을 계산함
모든 앨범의 APV 값을 총합하여 전체 APV를 도출함
'''
#from Valuation.MNV.MOV.PFV.AV.APV.APV_main import apv
#result_apv = apv()
#print(result_apv)

#2단계. SV : 멜론 스트리밍 횟수
'''
SV_main.py는 Melon 웹사이트에서 아티스트 앨범 및 곡 데이터를 수집, 집계하여 수익 지표를 산출함
환경변수를 통해 ARTIST_ID, MELON_ID, MELON_IDS를 동적으로 불러옴
get_albums_data와 get_songs_data 함수를 호출하여 앨범 및 곡 세부 정보를 획득함
앨범별로 각 곡의 멜론 수익, 스트림, 좋아요, 청취자 수를 합산함
앨범에 대응하는 곡 데이터를 트랙 리스트에 할당함
중복 앨범 검증 로직을 통해 고유 앨범만 결과에 포함시킴
앨범 수익을 누적하여 전체 멜론 총 수익을 산출함
최종 결과 딕셔너리에 앨범 목록, 멜론 총 수익, SV 값이 포함됨
'''
#from Valuation.MNV.MOV.PFV.AV.SV.SV_main import sv
#result_sv = sv()
#print(result_sv)

#3단계. RV : 써클차트 앨범 판매 횟수
'''
RV_main.py는 CircleChart 웹사이트에서 앨범 판매 데이터를 수집함
환경변수로 ARTIST_ID, ARTIST_NAME_KOR, ARTIST_NAME_ENG, MELON_ID 및 검색 기준을 불러옴
get_sales_record_data 함수는 연도별 페이지를 요청하여 스크립트 내 res_list 데이터를 파싱함
정규표현식을 이용하여 앨범 정보와 아티스트명을 추출 및 구조화함
get_sales_from_list 함수는 추가 API 호출을 통해 상세 판매 정보를 획득함
fetch_sales 함수는 중복 제거 로직을 적용하여 고유 앨범 데이터를 구성함
Variables 모듈의 LAP와 DISCOUNT_RATE를 활용하여 할인된 판매 가격을 계산함
각 앨범의 판매량과 할인된 가격으로 개별 및 누적 수익을 산출함
최종 결과는 판매 데이터, 총 판매량, 최신 앨범 가격, 할인율, 총 수익을 포함함
'''
#from Valuation.MNV.MOV.PFV.AV.RV.RV_main import rv
#result_rv = rv()
#print(result_rv)

#4-1단계. UDI : 앨범 균등 지표
'''
UDI_main.py의 목표는 Spotify와 Melon에서 수집한 앨범 메트릭스를 통합하여 UDI(Unique Diversity Index)를 산출함
rv, sv, apv 모듈을 통해 판매, 스트리밍, 인기도 데이터를 개별적으로 수집함
normalize_release_date_spotify와 normalize_release_date_melon 함수로 각 플랫폼의 발매일을 표준화함
match_albums_by_release_date 함수로 발매일을 기준으로 Spotify와 Melon 앨범을 매핑함
is_valid_track 함수로 불필요한 인스트루멘탈, 인터루드 등 트랙을 필터링함
combine_metrics 함수에서 각 앨범의 스트림, 청취, 좋아요, 인기도 데이터를 정규화 및 통합함
normalize_values 함수를 활용해 메트릭 데이터의 상대적 크기를 산출함
entropy_ratio와 calculate_normalized_entropy 함수로 각 메트릭의 엔트로피를 계산함
UDI는 네 엔트로피의 평균으로 산출되며 0.5에서 1.0 사이로 제한됨
'''
#from Valuation.MNV.MOV.PFV.AV.UDI.UDI_main import udi
#result_udi = udi()
#print(result_udi)

#4-2단계. AV : 앨범 가치 평가
'''
AV_main.py는 Spotify와 Melon 플랫폼의 앨범 메트릭 데이터를 통합하여 앨범 평가(AV)를 산출하는 기능을 수행함
Weights와 Variables 모듈을 통해 RV, SV, APV 가중치 및 REVENUE_PER_STREAM 값을 설정함
parse_date_any_format 함수는 다양한 날짜 포맷을 지원하여 문자열을 datetime 객체로 파싱함
av() 함수는 Firebase 캐시를 확인 후, UDI_main 모듈을 호출하여 UDI와 결합된 메트릭 데이터를 획득함
각 앨범에 대해 할인된 수익(RV_a), 스트림 기반 수익(SV_a), 인기도 기반 값(APV_a)을 산출함
UDI 값을 기본값 또는 계산값으로 적용하여 최종 AV 값(AV_a)을 가중치와 결합하여 계산함
각 앨범의 메트릭 데이터는 spotify_album_id, melon_album_id, 제목, 발매일 등으로 구성됨
'''
#from Valuation.MNV.MOV.PFV.AV.AV_main import av
#result_av = av()
#print(result_av)

#5단계. PFV : 포트폴리오 가치 평가
'''
PFV_main.py는 앨범 평가(AV)를 기반으로 최종 PFV 지표를 산출함
Weights 모듈에서 AV_WEIGHT와 PFV_WEIGHT 값을 불러와 가중치 적용에 활용함
av() 함수를 호출하여 AV 데이터를 수집함
Firebase의 check_record 함수로 캐시 여부를 확인함
캐시 존재 시 저장된 데이터를 반환하여 연산을 최적화함
캐시 미존재 시 AV 값을 기반으로 pfv_value를 계산함
계산된 pfv_value에 AV_WEIGHT와 PFV_WEIGHT를 차례로 적용함
결과는 앨범 메트릭, 가중치, AV 및 PFV 값으로 구성됨
'''
#from Valuation.MNV.MOV.PFV.PFV_main import pfv
#result_pfv = pfv()
#print(result_pfv)



#6단계. FB : 유튜브/인스타그램/트위터 팔로워
'''
아티스트 소셜 미디어 데이터 수집 및 팬 베이스 산출 기능을 수행함
환경변수를 통해 ARTIST_ID, ARTIST_NAME_KOR, ARTIST_NAME_ENG, MELON_ID를 로드함
openpyxl로 Statista 엑셀 파일에서 플랫폼 이름과 사용자 수를 추출함
추출된 데이터를 딕셔너리 형태로 매핑하여 플랫폼별 사용자 수를 구성함
fb_youtube, fb_instagram, fb_twitter 모듈을 통해 각 플랫폼의 팔로워 수를 수집함
전체 사용자 수를 기반으로 각 플랫폼의 영향력을 산출함
플랫폼별 팬 베이스는 팔로워 수와 영향력의 곱으로 계산됨
모든 플랫폼의 팬 베이스를 합산하여 최종 팬 베이스를 도출함
'''
#from Valuation.MNV.MOV.FV.FB.FB_main import fb
#result_fb = fb()
#print(result_fb)

#7-1단계. ER Youtube : 유튜브
'''
ER_youtube.py는 YouTube API를 통해 지정된 채널의 최신 동영상 데이터를 수집함
dotenv를 사용하여 환경변수에서 API 키 및 채널 ID를 로드함
firebase_handler의 check_record로 캐시된 데이터를 확인하여 중복 호출을 방지함
캐시가 없으면 YouTube 검색 API를 호출하여 최신 동영상 ID 목록을 확보함
수집된 동영상 ID를 기반으로 영상 API를 호출하여 조회수와 좋아요 통계를 집계함
각 동영상의 통계 데이터를 누적하여 총 동영상 수, 조회수, 좋아요 수를 계산함
결과 데이터를 딕셔너리로 구성하고 save_record로 Firebase에 저장함
'''
#from Valuation.MNV.MOV.FV.ER.ER_youtube import er_youtube
#result_er_youtube = er_youtube()
#print(result_er_youtube)

#7-2단계. ER Twitter : 트위터
'''
트위터 API를 활용하여 지정된 계정의 트윗 데이터를 수집함
환경변수에서 API 키, 토큰, 계정 정보 등을 불러와 설정함
get_user_id() 함수는 Twitter 계정의 사용자 ID를 요청 API를 통해 획득함
er_twitter() 함수는 캐시된 데이터를 check_record로 확인 후, 최신 트윗을 최대 100개 조회함
각 트윗의 공공 통계(public_metrics)에서 좋아요 수를 추출하여 누적함
수집된 트윗 데이터는 결과 딕셔너리로 구성되어 총 트윗 수와 누적 좋아요 수를 포함함
'''
#from Valuation.MNV.MOV.FV.ER.ER_twitter import er_twitter
#result_er_twitter = er_twitter()
#print(result_er_twitter)

#7-3단계. ER Instagram : 인스타그램
'''
Instagram 대상 계정의 게시물 및 상호작용 데이터를 안정적으로 수집함
dotenv 라이브러리로 환경변수에서 로그인 정보와 타깃 계정 정보를 로드함
instaloader 모듈을 활용하여 인스타그램에 로그인 및 프로필 객체를 생성함
fetch_posts_with_backoff 함수는 profile.get_posts() 호출 시 예외 발생에 대비해 백오프 전략을 구현함
요청 실패 시 재시도 로직과 지수적 대기 시간을 적용하여 데이터 요청의 안정성을 확보함
각 게시물의 URL, 좋아요 수, 댓글 수, 작성일을 추출해 리스트에 집계함
프로필의 팔로워 수와 총 게시물 수, 누적 좋아요 및 댓글 수를 계산함
'''
#from Valuation.MNV.MOV.FV.ER.ER_instagram import er_instagram
#result_er_instagram = er_instagram()
#print(result_er_instagram)

#7-4단계. ER : 유튜브/인스타그램/트위터 팬 참여도
'''
ER_main.py는 아티스트의 소셜 미디어 참여 지표(Engagement Ratio)를 산출하는 모듈임함
openpyxl을 이용하여 Statista 엑셀 파일에서 플랫폼별 사용자 수를 추출함
dotenv를 통해 환경변수에서 ARTIST_ID, ARTIST_NAME_KOR, ARTIST_NAME_ENG, MELON_ID를 로드함
er_youtube, er_twitter, er_instagram 함수를 호출하여 YouTube, Twitter, Instagram의 콘텐츠 및 통계 데이터를 수집함
fb_youtube, fb_twitter, fb_instagram 함수를 통해 각 플랫폼의 팔로워 수를 별도 획득함
플랫폼별 사용자 수를 기반으로 영향력 계수를 산출하여 각 플랫폼 참여율에 적용함
동영상, 트윗, 게시물의 좋아요 수를 팔로워 수로 나누어 개별 참여율을 계산함
각 플랫폼의 참여율에 산출된 영향력을 곱해 최종 참여 지표(ER)를 도출함
여러 플랫폼의 참여 지표를 평균화하여 전체 소셜 미디어 참여율을 산출함
'''
#from Valuation.MNV.MOV.FV.ER.ER_main import er
#result_er = er()
#print(result_er)

#8단계. G : 리스너 인구 통계 지표
#접속 : https://songstats.com/artist/ji2rm1hs/knk/audience
#데이터 : https://data.songstats.com/api/v1/audience/map_stats?idUnique=rlm7ou49&source=spotify&
'''
G_main.py는 Songstats 기반 청취자 데이터를 활용하여 국가별 팬덤 경제력을 산출함
make_csv 함수는 JSON 파일을 파싱하여 국가, 도시, 월간 청취자 및 최고 기록 데이터를 CSV로 저장함
CSV 데이터는 청취자 통계를 국가별로 집계하여 팬덤 비율을 계산하는 기초 자료로 활용됨
get_country_gdp 함수는 pycountry와 World Bank API를 통해 국가별 1인당 명목 GDP 값을 조회함
calculate_fandom_economic_power 함수는 pandas로 청취자 비율과 GDP를 곱해 팬덤경제력을 산출함
get_usd_to_krw 함수는 환율 API를 호출하여 USD 대비 KRW 환율을 반환함
g 함수는 Firebase 캐시를 확인하고, 팬덤경제력 총합에 환율을 곱해 최종 경제력 지표를 도출함
모듈 간 데이터 흐름과 예외 처리로 안정적 데이터 처리 및 재사용성을 보장함
산출된 지표는 아티스트의 국제적 영향력 및 경제적 가치를 평가하는 데 응용 가능함
'''
#from Valuation.MNV.MOV.FV.G.G_main import g
#result_g = g()
#print(result_g)

#9단계. FV : 팬덤 가치
'''
아티스트 식별자와 정보를 환경변수로부터 로드하여 기본 설정을 수행함
Weights 모듈에서 FB, ER, G, FV의 가중치 값을 불러와 연산에 적용함
fb, er, g 모듈을 호출하여 각각 팬 베이스, 소셜 참여율, 팬덤 경제력을 산출함
Firebase 캐시 확인 후, 기존 데이터가 존재하면 이를 반환하여 중복 연산을 방지함
각 모듈로부터 획득한 지표를 콘솔 출력으로 확인하여 디버깅에 활용함
팬 베이스, 참여율, 경제력 값을 지수 연산 방식으로 통합하여 Fan Valuation을 계산함
계산 결과에 FV_WEIGHT를 곱해 최종 Fan Valuation을 보정함
산출된 결과는 각 가중치와 개별 지표, 최종 Fan Valuation을 포함하는 딕셔너리로 구성됨
'''
#from Valuation.MNV.MOV.FV.FV_main import fv
#result_fv = fv()
#print(result_fv)

#10-1단계. FV_trends : 구글 트렌드 값 SERP API로 가져오기
'''
!!! 중요 !!!
1. FV 선행 - FV 값 필요
2. PFV 선행 - PFV, AV 값과 Album Metrics 필요.

>> FV의 시계열 데이터를 구하고 PFV와 MRV에 적용

==============

FV_trends.py는 Google Trends API를 통해 아티스트 검색 관심도 시계열 데이터를 수집함
본 코드는 FV 및 PFV 산출에 필요한 선행 데이터를 확보하는 역할을 수행함
sv() 함수를 호출하여 앨범 메트릭 데이터를 가져오고 가장 이른 발매일을 기준으로 시작 기간을 설정함
get_interest_over_time() 함수를 통해 웹과 유튜브의 관심도 데이터를 각각 조회함
GoogleSearch 객체를 사용하여 SERPAPI로부터 타임시리즈 데이터를 요청함
조회된 데이터는 ‘timeline_data’ 키를 통해 Firebase 캐시로 저장 및 불러옴
환경변수를 통해 API 키 및 아티스트 관련 정보를 동적으로 로드함
수집된 웹과 유튜브 트렌드 데이터는 이후 PFV와 MRV 계산에 적용됨
시계열 데이터는 FV 트렌드 분석에 활용 가능한 형태로 가공됨
'''
#from Valuation.MNV.MOV.FV.FV_trends import fv_trends
#result_fv_trends = fv_trends()
#print(result_fv_trends)

#10-2단계. FV_t : 시계열 팬덤 가치
'''
FV_t.py는 아티스트 팬 밸류 트렌드(FV_t)를 시계열 데이터로 산출하기 위한 모듈임함
parse_trend_data 함수는 SERPAPI로부터 수집한 타임시리즈 데이터에서 날짜, 타임스탬프, 쿼리, 추출값을 추출하여 DataFrame으로 정제함
clean_trend_df 함수는 아티스트 한글 및 영문 이름으로 필터링하고, 이상치 문자를 0으로 대체하여 정수형으로 변환함
normalize_date_string 함수는 특수 공백과 대시를 표준 하이픈으로 치환하여 날짜 문자열을 정규화함
convert_date 함수는 다양한 날짜 포맷을 지원하여 정규화된 문자열을 datetime 객체로 변환하고 월말 보정을 적용함
calculate_fv_t 함수는 팬 베이스(FB), 참여율(ER), 팬덤 경제력(G) 지표에 가중치 지수 연산을 적용해 FV_t 값을 산출함
fb, er, g 함수 호출로 각각 소셜 미디어, 참여율, 경제력 데이터를 획득하고 fv_trends 함수를 통해 웹 및 유튜브 관심도 데이터를 수집함
웹과 유튜브 트렌드 DataFrame은 날짜 기준 외부 조인을 통해 통합되어 각 날짜별 트렌드 비율을 산출함
'''
#from Valuation.MNV.MOV.FV.FV_t import fv_t
#result_fvt = fv_t()
#print(result_fvt)

#11-1단계. CEV Collector : 네이버 공연 데이터
'''
CEV_collector.py는 NAVER 콘서트 탭에서 아티스트 공연 데이터를 Selenium을 통해 자동 수집함
dotenv와 환경변수로 기본 URL 및 아티스트 정보를 동적 로드함
ChromeDriverManager와 headless 옵션을 사용하여 Selenium WebDriver를 구성함
WebDriverWait, Expected Conditions, ActionChains를 활용해 공연 탭 클릭 및 페이지 네비게이션을 수행함
각 공연 아이템에서 제목, 링크, 이미지, 장소, 공연 기간 등의 정보를 추출함
추출한 데이터를 딕셔너리 리스트로 구성하여 CSV 파일 및 Google Sheets에 기록함
유틸리티 함수로 중복 데이터 검증 및 신규 공연 데이터 필터링을 적용함
'''
#from Valuation.MNV.MOV.PCV.CEV.CEV_collector import cev_collector
#result_cev_collector = cev_collector()
#print(result_cev_collector)

#11단계. CEV : 콘서트 가치
'''
!!! 중요 !!!
1. FV 선행 - FV 값 필요
2. PFV 선행 - PFV, AV 값과 Album Metrics 필요.
3. Firebase Firestore의 <performance> 컬렉션에 실제 수익 데이터가 있어야 정상 작동함

===========

아티스트 공연 수익 데이터와 앨범 평가, 팬 밸류 트렌드 데이터를 활용하여 CEV(Concert Economic Value)를 산출함
환경변수를 통해 아티스트 정보와 NAVER_CONCERT_TAB URL 등을 동적으로 로드함
av(), er(), fv_t() 모듈을 호출하여 앨범 메트릭, 소셜 참여율, 팬 밸류 트렌드 데이터를 각각 수집함
parse_revenue, clean_start_period, calculate_discount_factor 함수로 데이터 전처리 및 할인율을 계산함
find_latest_album과 find_latest_fv 함수로 이벤트 발생 시점 이전의 최신 앨범 및 팬 밸류 데이터를 추출함
Firebase Firestore의 performance 컬렉션에서 실제 수익 데이터를 필터링하여 이벤트 정보를 획득함
수익 데이터가 있는 이벤트와 누락된 이벤트를 분리하여 각각 할인 적용 후 누적 수익을 산출함
이벤트별 AV dependency와 CEV 알파 계수를 계산하여 최종 CEV 값을 보정함
'''
#from Valuation.MNV.MOV.PCV.CEV.CEV_main import cev
#result_cev = cev()

#12단계. MCV : 미디어/콘텐츠 가치
'''
MCV_main.py는 YouTube, Twitter, Instagram에서 수집한 미디어 전환 가치를 통합하여 최종 MCV를 산출함
mcv_youtube 모듈은 YouTube API를 통해 아티스트 채널의 비디오 데이터를 수집하고, 조회수, 좋아요, 댓글, 재생시간 등 지표를 기반으로 영상별 MCV를 계산함
mcv_twitter 모듈은 Tweepy API를 사용하여 아티스트의 트윗 데이터를 수집하고, 트윗의 좋아요, 리트윗, 답글, 인용 수를 반영하여 Twitter MCV를 산출함
mcv_instagram 모듈은 인스타그램 게시물의 좋아요와 댓글 데이터를 수집하고, 게시물 게시 시점을 고려한 할인율을 적용하여 Instagram MCV를 계산함
각 모듈에서 산출된 원시 MCV 값은 설정된 가중치(YOUTUBE_WEIGHT, TWITTER_WEIGHT, INSTAGRAM_WEIGHT)와 곱해져 최종 MCV에 반영됨
수집된 데이터는 참여도, 효율성, 시계열 할인 요인 등 경제적 가치 산출에 필요한 정량적 지표를 포함함
이 데이터들은 표준화 및 모델링 과정을 거쳐 플랫폼별 미디어 가치를 효과적으로 평가함
'''
#from Valuation.MNV.MOV.PCV.MCV.MCV_main import mcv
#result_mcv = mcv()
#print(result_mcv)

#13단계. MDS : 굿즈/MD 추정 가치
'''
MDS_main.py는 FV 트렌드 데이터, 소셜 참여율 및 앨범 평가 데이터를 활용하여 공연 경제 가치를 산출함
decay_factor 함수는 앨범 출시 후 경과 일수에 따른 지수 감쇠율을 계산하여 AIF 요소를 산출함
calculate_discount_factor 함수는 이벤트 발생일과 현재일 사이의 할인율을 연 단위로 산출함
fv_t, er, av 모듈을 호출하여 각각 팬 밸류 트렌드, 소셜 참여율, 앨범 평가 데이터를 불러옴
fv_t 데이터를 DataFrame으로 변환하고 날짜별 이동 평균(FV_t_rolling)을 계산함
앨범 데이터에서 최대 AV 값을 기준으로 각 앨범의 상대적 가치와 감쇠 효과를 적용하여 AIF_t를 산출함
각 날짜별로 최신 앨범의 AV 값과 해당 시점까지 출시된 앨범의 AIF_t, 그리고 er 값을 결합하여 MDS_t를 계산함
계산된 MDS_t 값은 할인 요인을 적용하여 억 단위로 변환 후 출력 및 누적 합산됨
최종 결과는 날짜별 MDS_t 기록과 총 MDS 값으로 구성되어 Firebase에 저장되어 후속 분석에 활용됨
'''
#from Valuation.MNV.MOV.PCV.MDS.MDS_main import mds
#result_mds = mds()
#print(result_mds)

#14단계. PCV : 프로덕션 가치
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
#from Valuation.MNV.MOV.PCV.PCV_main import pcv
#result_pcv = pcv()
#print(result_pcv)



#15단계. MRV : 매니지먼트 가치
'''
MRV_main.py는 FV, PFV, AV 및 실제 방송 수익 데이터를 결합하여 방송 가치(MRV)를 산출함
Firebase Firestore의  컬렉션에 저장된 실제 수익 데이터를 기반으로 이벤트 정보를 불러옴
parse_revenue와 clean_start_period 함수를 통해 문자열 형태의 수익과 시작일을 정제함
find_latest_album 함수는 이벤트 발생 시점 이전의 최신 앨범 데이터를 추출함
fv_t, er, av 모듈을 호출하여 팬 밸류 트렌드, 소셜 참여율, 앨범 평가 데이터를 획득함
fv_t 데이터로부터 날짜별 이동 평균(FV_t_rolling)을 계산하여 시계열 추세를 반영함
각 방송 이벤트의 카테고리에 따라 가중치(CATEGORY_WEIGHT)를 적용함
이벤트별로 해당 시점의 FV_t_rolling과 최신 앨범의 AV 값을 결합하여 개별 방송 가치를 산출함
할인 요인을 적용하여 이벤트 발생 시점까지의 시간 가치를 보정함
최종적으로 모든 방송 이벤트의 가치를 누적하여 총 MRV를 계산
'''
#from Valuation.MNV.MOV.MRV.MRV_main import mrv
#result_mrv = mrv()
#print(result_mrv)


#16단계. MOV : 총 가치

'''
MOV_main.py는 다양한 가치 모듈(FV, PFV, PCV, MRV)을 통합하여 팬덤 및 음악 산업 전반의 총 가치를 시계열 데이터로 산출함
timeline_df의 각 컬럼은 팬덤 가치(FV_t), 스트리밍 가치(SV_t), 인기도 가치(APV_t), 음반판매 가치(RV_t), 공연/이벤트 가치(CEV_t), 미디어/콘텐츠/소셜미디어 가치(MCV_t), 굿즈/MD 가치(MDS_t), 매니지먼트 가치(MRV_t)를 나타냄
각 모듈의 결과에 가중치를 적용하여 PFV, PCV, MRV 값이 보정됨
PFVFunc와 PCVFunc 클래스를 통해 음반 및 공연 관련 데이터를 시간에 따라 분산 보정함
MRV, FV_t 데이터와 결합하여 MOV_t(총 가치)를 산출함
전체 파이프라인은 다양한 플랫폼 데이터를 통합, 정규화 및 시계열 분석으로 산업 가치를 평가함

[timeline_df의 Columns 설명]
FV_t : 팬덤 가치
SV_t : 스트리밍 가치
APV_t : 인기도 가치 
RV_t : 음반판매 가치
CEV_t : 콘서트/이벤트 가치
MCV_t : 미디어/콘텐츠/소셜미디어 가치
MDS_t : 굿즈/MD 가치
MRV_t : 방송/드라마/영화/상표권 가치
'''
from Valuation.MNV.MOV.MOV_main import mov
result_mov = mov()
print(result_mov)