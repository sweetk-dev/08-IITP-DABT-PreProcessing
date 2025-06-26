import json
import requests
import logging

def build_kosis_url(api_info, stats_src, stats_src_data_info, url_key):
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
        from_year = int(str(stats_src_data_info.get('collect_start_dt', '0'))[:4])
        to_year = int(str(stats_src_data_info.get('collect_end_dt', '0'))[:4])
        prd_cnt = to_year - from_year + 1
        url = url.replace('{PRD_CNT}', str(prd_cnt))
    if use_base:
        url = base_url + url
    return url, file_format

def fetch_kosis_meta(api_info, stats_src, stats_src_data_info):
    url, file_format = build_kosis_url(api_info, stats_src, stats_src_data_info, 'api_meta_url')
    if not url:
        logging.error('KOSIS meta url 생성 실패')
        return None
    response = requests.get(url)
    response.raise_for_status()
    if file_format == 'json':
        return response.json()
    else:
        return response.text

def fetch_kosis_latest(api_info, stats_src, stats_src_data_info):
    url, file_format = build_kosis_url(api_info, stats_src, stats_src_data_info, 'api_latest_chn_dt_url')
    if not url:
        logging.error('KOSIS latest url 생성 실패')
        return None
    response = requests.get(url)
    response.raise_for_status()
    if file_format == 'json':
        return response.json()
    else:
        return response.text

def fetch_kosis_data(api_info, stats_src, stats_src_data_info):
    url, file_format = build_kosis_url(api_info, stats_src, stats_src_data_info, 'api_data_url')
    if not url:
        logging.error('KOSIS data url 생성 실패')
        return None
    response = requests.get(url)
    response.raise_for_status()
    if file_format == 'json':
        return response.json()
    else:
        return response.text