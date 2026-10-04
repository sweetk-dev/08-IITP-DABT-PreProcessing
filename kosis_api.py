import json
import re
import requests
import logging

from file_utils import mask_auth_in_text

# (connect, read) 타임아웃 — 서버 무응답 시 무한 대기 방지
HTTP_TIMEOUT = (5, 60)

def mask_auth_in_url(url):
    """로그 출력용 URL 인증키 마스킹.

    마스킹 규칙은 file_utils.mask_auth_in_text 한 곳에서 관리한다(이동편의 수집기의
    collectors.base._mask_url 과 동일 규칙 — apiKey / serviceKey / KEY).
    """
    return mask_auth_in_text(url)


class KosisApiError(RuntimeError):
    """KOSIS 가 HTTP 200 본문으로 돌려준 오류 응답(``{"err": "...", "errMsg": "..."}``).

    속성:
        err_code: KOSIS 오류 코드 문자열(예: '20', '30'). 응답에 없으면 ''.
        err_msg : KOSIS 오류 메시지. 응답에 없으면 ''.
    메시지에는 오류 코드·메시지·호출 종류만 담는다. 요청 URL 과 인증키는 담지 않는다.
    """

    def __init__(self, kind, err_code, err_msg):
        self.kind = kind
        self.err_code = '' if err_code is None else str(err_code)
        self.err_msg = '' if err_msg is None else str(err_msg)
        super().__init__(
            f"KOSIS {kind} API 오류 응답: err={self.err_code}, errMsg={self.err_msg}"
        )


# 기간이 너무 넓어 건수 제한을 넘었을 때의 코드. 이 코드만 "기간을 나눠 다시 호출"로 처리한다.
KOSIS_ERR_PERIOD_TOO_WIDE = '31'


def is_error_response(response):
    """응답이 KOSIS 오류 본문(``err`` 키를 가진 dict)인지 판별한다."""
    return isinstance(response, dict) and 'err' in response


def raise_if_error_response(response, kind, allow_period_split=False):
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
        # 기간 계산
        if from_year is None:
            from_year = int(str(stats_src_data_info.get('collect_start_dt', '0'))[:4])
        if to_year is None:
            to_year = int(str(stats_src_data_info.get('collect_end_dt', '0'))[:4])
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

def fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, from_year, to_year):
    """
    특정 기간의 데이터만 수집
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
            raise RuntimeError("KOSIS API 요청 실패")
    except Exception as e:
        # requests 예외 문자열에는 요청 URL(apiKey 포함)이 들어 있다. 그대로 로그에 쓰거나
        # exc_info=True 로 트레이스백을 남기면 인증키가 로그 파일에 기록되므로
        # 예외 종류 + 마스킹한 문자열만 남기고, 원인 예외 연결도 끊는다(from None —
        # 연결돼 있으면 상위의 exc_info=True 로그에 원본 예외 문자열이 다시 출력된다).
        safe_msg = f"{type(e).__name__}: {mask_auth_in_url(str(e))}"
        logging.error(f'KOSIS data API 요청 중 예외 발생: {safe_msg}')
        print(f"[ERROR] KOSIS data API 요청 중 예외 발생: {safe_msg}")
        raise RuntimeError("KOSIS API 처리 중단") from None
    
    if file_format == 'json':
        data = response.json()
        # err=31(건수 초과)은 호출자(fetch_kosis_data_with_retry / fetch_kosis_data_split)가
        # 기간을 나눠 다시 호출하므로 그대로 돌려준다. 그 밖의 오류 본문은 여기서 실패로 확정한다.
        # 분할 수집 경로도 모두 이 함수를 거치므로, 분할 구간 하나가 오류를 돌려줘도
        # 오류 dict 가 정상 행 사이에 섞여 반환되지 않는다(부분 응답으로 기존 데이터를 덮지 않는다).
        raise_if_error_response(data, 'data', allow_period_split=True)
        return data
    else:
        return response.text

def fetch_kosis_data_split(api_info, stats_src, stats_src_data_info, from_year, to_year):
    """
    기간을 1/2씩 분할하여 데이터 수집
    갭이 1년이 될 때까지 반복
    """
    all_data = []
    year_gap = to_year - from_year + 1
    
    if year_gap > 1:  # 1년 초과 시에만 분할
        mid_year = from_year + (year_gap // 2)
        
        # 전반부 수집
        logging.warning(f"분할 수집 시도: {from_year}~{mid_year-1} ({year_gap//2}년)")
        response1 = fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, from_year, mid_year-1)
        
        if is_error_31(response1):
            # 전반부도 분할 필요
            all_data.extend(fetch_kosis_data_split(api_info, stats_src, stats_src_data_info, from_year, mid_year-1))
        else:
            all_data.extend(response1 if isinstance(response1, list) else [response1])
        
        # 후반부 수집
        logging.warning(f"분할 수집 시도: {mid_year}~{to_year} ({year_gap//2}년)")
        response2 = fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, mid_year, to_year)
        
        if is_error_31(response2):
            # 후반부도 분할 필요
            all_data.extend(fetch_kosis_data_split(api_info, stats_src, stats_src_data_info, mid_year, to_year))
        else:
            all_data.extend(response2 if isinstance(response2, list) else [response2])
        
        return all_data
    
    # 1년 단위 도달
    logging.warning(f"1년 단위 수집 시도: {from_year}~{to_year}")
    response = fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, from_year, to_year)
    
    if is_error_31(response):
        logging.error(f"Error 31: 1년 단위({from_year}~{to_year})에서도 데이터 수집 실패")
        print(f"[ERROR] KOSIS API Error 31: 1년 단위({from_year}~{to_year})에서도 데이터 수집 실패")
        raise RuntimeError("KOSIS API 처리 중단")
    
    return response if isinstance(response, list) else [response]

def fetch_kosis_data_with_retry(api_info, stats_src, stats_src_data_info):
    """
    Error 31 발생 시 1년 단위까지 자동 분할 수집
    """
    from_year = int(str(stats_src_data_info.get('collect_start_dt', '0'))[:4])
    to_year = int(str(stats_src_data_info.get('collect_end_dt', '0'))[:4])
    
    # 1차 시도: 전체 기간
    response = fetch_kosis_data_single(api_info, stats_src, stats_src_data_info, from_year, to_year)
    
    if not is_error_31(response):
        return response
    
    # Error 31 발생 시 분할 수집 시작
    logging.warning(f"Error 31 발생: {from_year}~{to_year} 전체 기간 데이터 수집 실패, 분할 수집 시작")
    return fetch_kosis_data_split(api_info, stats_src, stats_src_data_info, from_year, to_year)

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
            raise RuntimeError("KOSIS API 요청 실패")
    except Exception as e:
        # requests 예외 문자열에는 요청 URL(apiKey 포함)이 들어 있다. 그대로 로그에 쓰거나
        # exc_info=True 로 트레이스백을 남기면 인증키가 로그 파일에 기록되므로
        # 예외 종류 + 마스킹한 문자열만 남기고, 원인 예외 연결도 끊는다(from None —
        # 연결돼 있으면 상위의 exc_info=True 로그에 원본 예외 문자열이 다시 출력된다).
        safe_msg = f"{type(e).__name__}: {mask_auth_in_url(str(e))}"
        logging.error(f'KOSIS meta API 요청 중 예외 발생: {safe_msg}')
        print(f"[ERROR] KOSIS meta API 요청 중 예외 발생: {safe_msg}")
        raise RuntimeError("KOSIS API 처리 중단") from None
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
            raise RuntimeError("KOSIS API 요청 실패")
    except Exception as e:
        # requests 예외 문자열에는 요청 URL(apiKey 포함)이 들어 있다. 그대로 로그에 쓰거나
        # exc_info=True 로 트레이스백을 남기면 인증키가 로그 파일에 기록되므로
        # 예외 종류 + 마스킹한 문자열만 남기고, 원인 예외 연결도 끊는다(from None —
        # 연결돼 있으면 상위의 exc_info=True 로그에 원본 예외 문자열이 다시 출력된다).
        safe_msg = f"{type(e).__name__}: {mask_auth_in_url(str(e))}"
        logging.error(f'KOSIS latest API 요청 중 예외 발생: {safe_msg}')
        print(f"[ERROR] KOSIS latest API 요청 중 예외 발생: {safe_msg}")
        raise RuntimeError("KOSIS API 처리 중단") from None
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