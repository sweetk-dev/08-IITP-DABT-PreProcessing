import json
import re
import requests
import logging
from datetime import datetime, timedelta, timezone

from file_utils import mask_auth_in_text

# (connect, read) 타임아웃 — 서버 무응답 시 무한 대기 방지
HTTP_TIMEOUT = (5, 60)

def mask_auth_in_url(url):
    """로그 출력용 URL 인증키 마스킹.

    마스킹 규칙은 file_utils.mask_auth_in_text 한 곳에서 관리한다(이동편의 수집기의
    collectors.base._mask_url 과 동일 규칙 — apiKey / serviceKey / KEY).
    """
    return mask_auth_in_text(url)


# KOSIS 오류 코드 → 운영자가 바로 읽을 수 있는 원인. 실행 요약·알림 메일·운영 콘솔에 그대로 쓰인다.
# KOSIS 오류 메시지(errMsg)만으로는 "왜 이게 났는지"를 알기 어렵다. 예를 들어 err=20 은 문구가
# "필수요청변수값이 누락되었습니다"지만, 운영 중인 통계에서 이 오류가 새로 났다면 대부분 KOSIS 가
# 통계표 구조(분류 단계)를 바꿔 기존 조회 주소의 objL 이 모자라게 된 경우다(2026-10 N007 등 4종).
KOSIS_ERR_HINTS = {
    '10': '인증키 누락 — sys_ext_api_info.auth 확인',
    '11': '인증키 기간 만료 — KOSIS 인증키 재발급 필요',
    '20': '필수 요청변수 누락 — 통계표 구조(분류 단계) 변경 의심, 조회 주소 objL 점검 필요',
    '21': '요청변수 값 오류 — 조회 주소의 분류·항목 코드가 원천에서 바뀌었는지 점검 필요',
    '30': '조회 결과 없음 — 요청한 기간에 자료가 없음',
    '31': '조회 결과 4만 셀 초과 — 기간 분할로도 해결되지 않음',
    '40': '호출 가능 횟수 초과 — 잠시 뒤 재시도',
    '41': '호출 가능 건수 초과 — 잠시 뒤 재시도',
    '42': '사용자별 이용 제한 — KOSIS 이용 조건 확인',
    '50': 'KOSIS 서버 오류 — 원천 장애',
}


def kosis_err_hint(err_code):
    """KOSIS 오류 코드의 원인 안내 문구. 모르는 코드면 빈 문자열."""
    return KOSIS_ERR_HINTS.get(str(err_code or '').strip(), '')


class KosisApiError(RuntimeError):
    """KOSIS 가 HTTP 200 본문으로 돌려준 오류 응답(``{"err": "...", "errMsg": "..."}``).

    속성:
        err_code: KOSIS 오류 코드 문자열(예: '20', '30'). 응답에 없으면 ''.
        err_msg : KOSIS 오류 메시지. 응답에 없으면 ''.
        hint    : 오류 코드의 원인 안내(KOSIS_ERR_HINTS). 모르는 코드면 ''.
    메시지에는 오류 코드·원인 안내·메시지·호출 종류만 담는다. 요청 URL 과 인증키는 담지 않는다.
    메시지 형식 예: ``KOSIS data API 오류 응답: err=20(필수 요청변수 누락 — ...), errMsg=...``
    """

    def __init__(self, kind, err_code, err_msg):
        self.kind = kind
        self.err_code = '' if err_code is None else str(err_code)
        self.err_msg = '' if err_msg is None else str(err_msg)
        self.hint = kosis_err_hint(self.err_code)
        code_part = f"{self.err_code}({self.hint})" if self.hint else self.err_code
        super().__init__(
            f"KOSIS {kind} API 오류 응답: err={code_part}, errMsg={self.err_msg}"
        )


class KosisHttpStatusError(RuntimeError):
    """KOSIS 가 200 이 아닌 HTTP 상태 코드를 돌려줬을 때 내부에서 올리는 예외.

    요청 함수의 바깥 except 가 이 예외를 받아 상태 코드를 사유에 옮긴다(_abort_error 참고).
    RuntimeError 의 하위 클래스이므로 RuntimeError 를 잡는 기존 호출자는 그대로 동작한다.

    속성:
        status_code: HTTP 상태 코드(int).
    메시지에는 상태 코드만 담는다. 요청 URL 과 인증키는 담지 않는다.
    """

    def __init__(self, status_code):
        self.status_code = status_code
        super().__init__(f"KOSIS API 요청 실패: HTTP {status_code}")


def _abort_error(kind, exc):
    """요청 실패를 상위로 올릴 RuntimeError 를 만든다(사유에 HTTP 상태 코드·예외 종류 포함).

    문구가 "KOSIS API 처리 중단" 뿐이면 실행 요약(run_summary.log)의 실패 사유만으로는
    인증키 만료(4xx)·원천 장애(5xx)·네트워크 단절(ConnectionError)·시간 초과(Timeout)를
    구분할 수 없다. 사유에 "HTTP <상태 코드>" 또는 예외 클래스 이름을 덧붙인다.

    예외 문자열(str(exc))은 싣지 않는다 — requests 예외 문자열에는 요청 URL(apiKey 포함)이
    들어 있다. 상태 코드와 클래스 이름에는 URL·인증키가 포함되지 않는다.

    인자:
        kind: 호출 종류('data' / 'meta' / 'latest').
        exc: 요청 중 잡힌 예외. KosisHttpStatusError 면 상태 코드를, 그 외에는 클래스 이름을 쓴다.
    반환:
        RuntimeError — 메시지 예: ``KOSIS API 처리 중단(latest): HTTP 503`` /
        ``KOSIS API 처리 중단(data): ConnectionError``. 호출자가 ``raise ... from None`` 으로 올린다.
    """
    if isinstance(exc, KosisHttpStatusError):
        detail = f"HTTP {exc.status_code}"
    else:
        detail = type(exc).__name__
    return RuntimeError(f"KOSIS API 처리 중단({kind}): {detail}")


# 기간이 너무 넓어 건수 제한을 넘었을 때의 코드. 이 코드만 "기간을 나눠 다시 호출"로 처리한다.
KOSIS_ERR_PERIOD_TOO_WIDE = '31'

# 요청한 기간에 자료가 없을 때의 코드. 기간 분할 수집 중 "아직 공표되지 않은 연도"만 담긴 구간에서
# 정상적으로 나온다(fetch_kosis_data_split 참고). 그 밖의 경우에는 실패로 본다.
KOSIS_ERR_NO_DATA = '30'

# 한국 표준시. 수집 종료연도("올해")를 서버 시간대와 무관하게 정하기 위해 쓴다.
_KST = timezone(timedelta(hours=9))

# 조회 주소의 수록주기(prdSe). 종료연도를 "올해"로 넓히는 것은 수록시점이 연도(YYYY)로 표기되는
# 주기에서만 한다. Y=년, F=다년(부정기) — 둘 다 startPrdDe/endPrdDe 에 연도를 넣는다.
# 월(M)·분기(Q)·반기(H) 주기는 시점 표기가 YYYYMM 등이라 연도 치환 방식 자체가 맞지 않으므로
# 종전처럼 등록된 수집기간을 그대로 쓴다.
_YEARLY_PRD_SE = ('Y', 'F')
_PRD_SE_RE = re.compile(r'[?&]prdSe=([A-Za-z]+)')


def _year_of(value):
    """'2024', '2024-12-31', 2024 → 2024. 해석할 수 없으면 None."""
    text = str(value or '').strip()
    return int(text[:4]) if len(text) >= 4 and text[:4].isdigit() else None


def resolve_collect_years(stats_src, stats_src_data_info, today=None):
    """이번 실행에서 요청할 수집 기간 (from_year, to_year, known_end_year) 을 정한다.

    - from_year      : stats_src_data_info.collect_start_dt 의 연도(종전과 같다).
    - known_end_year : 등록된 수집 종료연도(collect_end_dt). 지금까지 자료가 있음이 확인된 마지막 연도다.
    - to_year        : 연도 주기(Y·F) 통계는 max(known_end_year, 올해(KST)). 그 밖의 주기는 known_end_year.

    왜 "올해"까지 넓히는가 — 수집 종료연도가 등록값(예: 2024)으로 고정돼 있으면 KOSIS 가 새 연도를
    공표해도 영영 수집되지 않는다(2026-10 실측: 등록장애인 통계 2025년, 사회참여 2024년 미수집).
    KOSIS 는 범위에 아직 없는 연도가 섞여 있으면 있는 연도만 돌려준다(err 없음, 2026-10-13 실측).
    적재에 성공하면 collect_end_dt 는 실제로 받은 마지막 연도로 갱신된다(db_processing 참고).

    인자:
        today: 테스트용 기준 날짜(date/datetime). None 이면 현재 KST 날짜.
    반환:
        (from_year, to_year, known_end_year) — 셋 다 int. 등록값을 해석할 수 없으면 0.
    """
    from_year = _year_of(stats_src_data_info.get('collect_start_dt')) or 0
    known_end = _year_of(stats_src_data_info.get('collect_end_dt')) or 0
    prd_se = None
    try:
        url = json.loads(stats_src.get('api_data_url') or '{}').get('url', '')
        m = _PRD_SE_RE.search(url)
        prd_se = m.group(1).upper() if m else None
    except (ValueError, AttributeError):
        prd_se = None
    to_year = known_end
    if prd_se in _YEARLY_PRD_SE:
        this_year = (today or datetime.now(_KST)).year
        to_year = max(known_end, this_year)
    return from_year, to_year, known_end


def is_error_response(response):
    """응답이 KOSIS 오류 본문(``err`` 키를 가진 dict)인지 판별한다."""
    return isinstance(response, dict) and 'err' in response


def raise_if_error_response(response, kind, allow_period_split=False, allow_no_data=False):
    """KOSIS 오류 본문이면 KosisApiError 를 올린다.

    KOSIS 는 인증키 오류·필수 파라미터 누락·데이터 없음 등을 HTTP 200 +
    ``{"err": "20", "errMsg": "..."}`` 로 돌려준다. 이것을 정상 응답으로 넘기면
    파일로 저장된 뒤 적재 단계에서 1행짜리 데이터로 취급되어 기존 통계가 지워진다.
    수집 단계에서 실패로 확정해 그 통계의 적재를 시작하지 않게 한다.

    인자:
        response: ``response.json()`` 결과(또는 text 형식 응답 문자열).
        kind: 로그·예외 메시지에 쓸 호출 종류('data' / 'meta' / 'latest').
        allow_period_split: True 면 err=31(건수 초과)은 올리지 않고 통과시킨다.
            호출자가 기간을 반으로 나눠 다시 시도하기 때문이다.
        allow_no_data: True 면 err=30(자료 없음)은 올리지 않고 통과시킨다.
            기간 분할 수집에서 "아직 공표되지 않은 연도만 담긴 구간"을 빈 결과로 다루기 위한 것이다.
    반환:
        없음(오류 본문이 아니면 아무 일도 하지 않는다).
    실패 시 동작:
        KosisApiError — 오류 코드·메시지 포함, 인증키·URL 미포함.
    """
    if not is_error_response(response):
        return
    err_code = str(response.get('err'))
    if allow_period_split and err_code == KOSIS_ERR_PERIOD_TOO_WIDE:
        return
    if allow_no_data and err_code == KOSIS_ERR_NO_DATA:
        return
    error = KosisApiError(kind, err_code, response.get('errMsg'))
    logging.error(str(error))
    raise error

def build_kosis_url(api_info, stats_src, stats_src_data_info, url_key, from_year=None, to_year=None):
    """
    url_key: 'api_meta_url', 'api_latest_chn_dt_url', 'api_data_url'
    """
    use_base = stats_src.get('use_base_url_yn', 'N') == 'Y'
    base_url = api_info.get('ext_url', '')
    url_info = stats_src.get(url_key)
    if not url_info:
        return None, None
    url_info = json.loads(url_info)
    url = url_info.get('url', '')
    file_format = url_info.get('format', 'json')
    # 치환
    url = url.replace('{API_AUTH_KEY}', api_info.get('auth', ''))
    if url_key == 'api_data_url':
        # 기간 계산 — 호출자가 기간을 주지 않으면 resolve_collect_years 규칙(연도 주기는 올해까지)을 쓴다.
        if from_year is None or to_year is None:
            r_from, r_to, _known = resolve_collect_years(stats_src, stats_src_data_info)
            if from_year is None:
                from_year = r_from
            if to_year is None:
                to_year = r_to
        url = url.replace('{from}', str(from_year))
        url = url.replace('{to}', str(to_year))
    if use_base:
        url = base_url + url
    return url, file_format

def is_error_31(response):
    """
    응답이 Error 31인지 확인
    """
    if isinstance(response, dict):
        return response.get('err') == '31'
    return False

def fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, from_year, to_year,
                            known_end_year=None):
    """
    특정 기간의 데이터만 수집

    known_end_year: 기간 분할 수집에서만 넘긴다. 이 구간의 시작연도가 known_end_year(자료가 있음이
        확인된 마지막 연도)보다 뒤이면 "아직 공표되지 않은 연도만 담긴 구간"이므로 err=30(자료 없음)을
        빈 결과([])로 돌려준다. 그 밖의 구간에서 나온 err=30 은 자료가 있어야 할 기간이 비었다는 뜻이라
        종전처럼 실패로 올린다(빠진 연도를 성공으로 삼키지 않는다).
    """
    url, file_format = build_kosis_url(api_info, stats_src, stats_src_data_info, 'api_data_url', from_year, to_year)
    if not url:
        logging.error('KOSIS data url 생성 실패')
        return None
    
    try:
        response = requests.get(url, timeout=HTTP_TIMEOUT)
        if response.status_code != 200:
            logging.error(f'KOSIS data API 요청 실패: status={response.status_code}, url={mask_auth_in_url(url)}, response={response.text[:200]}')
            print(f"[ERROR] KOSIS data API 요청 실패: status={response.status_code}, url={mask_auth_in_url(url)}")
            raise KosisHttpStatusError(response.status_code)
    except Exception as e:
        # requests 예외 문자열에는 요청 URL(apiKey 포함)이 들어 있다. 그대로 로그에 쓰거나
        # exc_info=True 로 트레이스백을 남기면 인증키가 로그 파일에 기록되므로
        # 예외 종류 + 마스킹한 문자열만 남기고, 원인 예외 연결도 끊는다(from None —
        # 연결돼 있으면 상위의 exc_info=True 로그에 원본 예외 문자열이 다시 출력된다).
        safe_msg = f"{type(e).__name__}: {mask_auth_in_url(str(e))}"
        logging.error(f'KOSIS data API 요청 중 예외 발생: {safe_msg}')
        print(f"[ERROR] KOSIS data API 요청 중 예외 발생: {safe_msg}")
        # 상위로 올리는 사유에는 HTTP 상태 코드 또는 예외 종류만 싣는다(URL·인증키 제외).
        raise _abort_error('data', e) from None
    
    if file_format == 'json':
        data = response.json()
        # err=31(건수 초과)은 호출자(fetch_kosis_data_with_retry / fetch_kosis_data_split)가
        # 기간을 나눠 다시 호출하므로 그대로 돌려준다. 그 밖의 오류 본문은 여기서 실패로 확정한다.
        # 분할 수집 경로도 모두 이 함수를 거치므로, 분할 구간 하나가 오류를 돌려줘도
        # 오류 dict 가 정상 행 사이에 섞여 반환되지 않는다(부분 응답으로 기존 데이터를 덮지 않는다).
        future_only = known_end_year is not None and from_year > known_end_year
        raise_if_error_response(data, 'data', allow_period_split=True, allow_no_data=future_only)
        if future_only and is_error_response(data) and str(data.get('err')) == KOSIS_ERR_NO_DATA:
            logging.info(f"분할 구간 {from_year}~{to_year}: 아직 공표되지 않은 기간(자료 없음) — 빈 결과로 처리")
            return []
        return data
    else:
        return response.text

def fetch_kosis_data_split(api_info, stats_src, stats_src_data_info, from_year, to_year,
                           known_end_year=None):
    """
    기간을 1/2씩 분할하여 데이터 수집
    갭이 1년이 될 때까지 반복

    known_end_year: fetch_kosis_data_single 참고. 미래 연도만 담긴 구간의 err=30 을 빈 결과로 다룬다.
    """
    all_data = []
    year_gap = to_year - from_year + 1
    
    if year_gap > 1:  # 1년 초과 시에만 분할
        mid_year = from_year + (year_gap // 2)
        
        # 전반부 수집
        logging.warning(f"분할 수집 시도: {from_year}~{mid_year-1} ({year_gap//2}년)")
        response1 = fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, from_year, mid_year-1,
                                            known_end_year=known_end_year)
        
        if is_error_31(response1):
            # 전반부도 분할 필요
            all_data.extend(fetch_kosis_data_split(api_info, stats_src, stats_src_data_info, from_year, mid_year-1,
                                                   known_end_year=known_end_year))
        else:
            all_data.extend(response1 if isinstance(response1, list) else [response1])
        
        # 후반부 수집
        logging.warning(f"분할 수집 시도: {mid_year}~{to_year} ({year_gap//2}년)")
        response2 = fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, mid_year, to_year,
                                            known_end_year=known_end_year)
        
        if is_error_31(response2):
            # 후반부도 분할 필요
            all_data.extend(fetch_kosis_data_split(api_info, stats_src, stats_src_data_info, mid_year, to_year,
                                                   known_end_year=known_end_year))
        else:
            all_data.extend(response2 if isinstance(response2, list) else [response2])
        
        return all_data
    
    # 1년 단위 도달
    logging.warning(f"1년 단위 수집 시도: {from_year}~{to_year}")
    response = fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, from_year, to_year,
                                       known_end_year=known_end_year)
    
    if is_error_31(response):
        logging.error(f"Error 31: 1년 단위({from_year}~{to_year})에서도 데이터 수집 실패")
        print(f"[ERROR] KOSIS API Error 31: 1년 단위({from_year}~{to_year})에서도 데이터 수집 실패")
        # 요약 로그의 실패 사유로 그대로 쓰이므로 원인(err=31, 더 나눌 수 없는 1년 구간)을 문구에 담는다.
        raise RuntimeError(f"KOSIS API 처리 중단(data): err=31, 1년 단위({from_year}~{to_year})에서도 건수 초과")
    
    return response if isinstance(response, list) else [response]

def fetch_kosis_data_with_retry(api_info, stats_src, stats_src_data_info):
    """
    Error 31 발생 시 1년 단위까지 자동 분할 수집
    """
    # 수집 기간: 연도 주기 통계는 종료연도를 올해까지 넓힌다(resolve_collect_years 참고).
    from_year, to_year, known_end_year = resolve_collect_years(stats_src, stats_src_data_info)
    logging.info(f"[{stats_src.get('stat_tbl_id')}] 수집 기간 {from_year}~{to_year} (등록 종료연도 {known_end_year})")
    
    # 1차 시도: 전체 기간
    response = fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, from_year, to_year)
    
    if not is_error_31(response):
        return response
    
    # Error 31 발생 시 분할 수집 시작
    logging.warning(f"Error 31 발생: {from_year}~{to_year} 전체 기간 데이터 수집 실패, 분할 수집 시작")
    return fetch_kosis_data_split(api_info, stats_src, stats_src_data_info, from_year, to_year,
                                  known_end_year=known_end_year)

def fetch_kosis_meta(api_info, stats_src, stats_src_data_info):
    url, file_format = build_kosis_url(api_info, stats_src, stats_src_data_info, 'api_meta_url')
    if not url:
        logging.error('KOSIS meta url 생성 실패')
        return None
    try:
        response = requests.get(url, timeout=HTTP_TIMEOUT)
        if response.status_code != 200:
            logging.error(f'KOSIS meta API 요청 실패: status={response.status_code}, url={mask_auth_in_url(url)}, response={response.text[:200]}')
            print(f"[ERROR] KOSIS meta API 요청 실패: status={response.status_code}, url={mask_auth_in_url(url)}")
            raise KosisHttpStatusError(response.status_code)
    except Exception as e:
        # requests 예외 문자열에는 요청 URL(apiKey 포함)이 들어 있다. 그대로 로그에 쓰거나
        # exc_info=True 로 트레이스백을 남기면 인증키가 로그 파일에 기록되므로
        # 예외 종류 + 마스킹한 문자열만 남기고, 원인 예외 연결도 끊는다(from None —
        # 연결돼 있으면 상위의 exc_info=True 로그에 원본 예외 문자열이 다시 출력된다).
        safe_msg = f"{type(e).__name__}: {mask_auth_in_url(str(e))}"
        logging.error(f'KOSIS meta API 요청 중 예외 발생: {safe_msg}')
        print(f"[ERROR] KOSIS meta API 요청 중 예외 발생: {safe_msg}")
        # 상위로 올리는 사유에는 HTTP 상태 코드 또는 예외 종류만 싣는다(URL·인증키 제외).
        raise _abort_error('meta', e) from None
    if file_format == 'json':
        result = response.json()
        # meta 호출도 오류를 HTTP 200 + {"err": ...} 로 돌려준다. 정상 응답으로 저장하면
        # 적재 단계가 메타 없이 진행하므로 수집 실패로 처리한다.
        raise_if_error_response(result, 'meta')
        return result
    else:
        return response.text

def fetch_kosis_latest(api_info, stats_src, stats_src_data_info):
    url, file_format = build_kosis_url(api_info, stats_src, stats_src_data_info, 'api_latest_chn_dt_url')
    if not url:
        logging.error('KOSIS latest url 생성 실패')
        return None
    try:
        response = requests.get(url, timeout=HTTP_TIMEOUT)
        if response.status_code != 200:
            logging.error(f'KOSIS latest API 요청 실패: status={response.status_code}, url={mask_auth_in_url(url)}, response={response.text[:200]}')
            print(f"[ERROR] KOSIS latest API 요청 실패: status={response.status_code}, url={mask_auth_in_url(url)}")
            raise KosisHttpStatusError(response.status_code)
    except Exception as e:
        # requests 예외 문자열에는 요청 URL(apiKey 포함)이 들어 있다. 그대로 로그에 쓰거나
        # exc_info=True 로 트레이스백을 남기면 인증키가 로그 파일에 기록되므로
        # 예외 종류 + 마스킹한 문자열만 남기고, 원인 예외 연결도 끊는다(from None —
        # 연결돼 있으면 상위의 exc_info=True 로그에 원본 예외 문자열이 다시 출력된다).
        safe_msg = f"{type(e).__name__}: {mask_auth_in_url(str(e))}"
        logging.error(f'KOSIS latest API 요청 중 예외 발생: {safe_msg}')
        print(f"[ERROR] KOSIS latest API 요청 중 예외 발생: {safe_msg}")
        # 상위로 올리는 사유에는 HTTP 상태 코드 또는 예외 종류만 싣는다(URL·인증키 제외).
        raise _abort_error('latest', e) from None
    if file_format == 'json':
        result = response.json()
        # latest 호출도 오류를 HTTP 200 + {"err": ...} 로 돌려준다. 정상 응답으로 저장하면
        # 적재 단계가 갱신일을 읽지 못한 채(None) 진행하므로 수집 실패로 처리한다.
        raise_if_error_response(result, 'latest')
        return result
    else:
        return response.text

def fetch_kosis_data(api_info, stats_src, stats_src_data_info):
    """
    KOSIS 데이터 API 호출 (Error 31 자동 분할 처리 포함)
    """
    return fetch_kosis_data_with_retry(api_info, stats_src, stats_src_data_info)