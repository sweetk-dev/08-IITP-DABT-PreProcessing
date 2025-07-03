import json
import requests
import logging

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
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f'KOSIS data API 요청 실패: status={response.status_code}, url={url}, response={response.text[:200]}')
            print(f"[ERROR] KOSIS data API 요청 실패: status={response.status_code}, url={url}")
            import sys; sys.exit(1)
    except Exception as e:
        logging.error(f'KOSIS data API 요청 중 예외 발생: {e}', exc_info=True)
        print(f"[ERROR] KOSIS data API 요청 중 예외 발생: {e}")
        import sys; sys.exit(1)
    
    if file_format == 'json':
        return response.json()
    else:
        return response.text

def fetch_kosis_data_split(api_info, stats_src, stats_src_data_info, from_year, to_year):
    """
    기간을 1/2씩 분할하여 데이터 수집
    갭이 1년이 될 때까지 반복
    """
    all_data = []
    year_gap = to_year - from_year + 1
    
    while year_gap > 1:  # 1년 초과 시에만 분할
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
        import sys; sys.exit(1)
    
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
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f'KOSIS meta API 요청 실패: status={response.status_code}, url={url}, response={response.text[:200]}')
            print(f"[ERROR] KOSIS meta API 요청 실패: status={response.status_code}, url={url}")
            import sys; sys.exit(1)
    except Exception as e:
        logging.error(f'KOSIS meta API 요청 중 예외 발생: {e}', exc_info=True)
        print(f"[ERROR] KOSIS meta API 요청 중 예외 발생: {e}")
        import sys; sys.exit(1)
    if file_format == 'json':
        return response.json()
    else:
        return response.text

def fetch_kosis_latest(api_info, stats_src, stats_src_data_info):
    url, file_format = build_kosis_url(api_info, stats_src, stats_src_data_info, 'api_latest_chn_dt_url')
    if not url:
        logging.error('KOSIS latest url 생성 실패')
        return None
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f'KOSIS latest API 요청 실패: status={response.status_code}, url={url}, response={response.text[:200]}')
            print(f"[ERROR] KOSIS latest API 요청 실패: status={response.status_code}, url={url}")
            import sys; sys.exit(1)
    except Exception as e:
        logging.error(f'KOSIS latest API 요청 중 예외 발생: {e}', exc_info=True)
        print(f"[ERROR] KOSIS latest API 요청 중 예외 발생: {e}")
        import sys; sys.exit(1)
    if file_format == 'json':
        return response.json()
    else:
        return response.text

def fetch_kosis_data(api_info, stats_src, stats_src_data_info):
    """
    KOSIS 데이터 API 호출 (Error 31 자동 분할 처리 포함)
    """
    return fetch_kosis_data_with_retry(api_info, stats_src, stats_src_data_info)