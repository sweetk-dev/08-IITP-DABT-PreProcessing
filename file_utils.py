import os
import json
from datetime import datetime
import logging
import re

def safe_filename(filename, max_length=100):
    # 파일명에 사용할 수 없는 문자 제거/치환
    filename = re.sub(r'[\\/:*?"<>|]', '_', filename)
    # 너무 긴 파일명은 잘라냄
    if len(filename) > max_length:
        filename = filename[:max_length]
    return filename

def save_meta_file(meta, stats_src, meta_dir, src_data_id, stat_title, from_year, to_year, file_format):
    now = datetime.now()
    time_str = now.strftime('%Y%m%d%H%M%S')
    stat_title_safe = safe_filename(stat_title)
    src_data_id_safe = safe_filename(str(src_data_id))
    filename = f"meta_{src_data_id_safe}-{stat_title_safe}-{from_year}-{to_year}_{time_str}.{file_format}"
    meta_path = os.path.join(meta_dir, filename)
    with open(meta_path, 'w', encoding='utf-8') as f:
        if file_format == 'json':
            json.dump(meta, f, ensure_ascii=False, indent=2)
        else:
            f.write(str(meta))
    return meta_path

def save_latest_file(latest, stats_src, latest_dir, src_data_id, stat_title, from_year, to_year, file_format):
    now = datetime.now()
    time_str = now.strftime('%Y%m%d%H%M%S')
    stat_title_safe = safe_filename(stat_title)
    src_data_id_safe = safe_filename(str(src_data_id))
    filename = f"latest_{src_data_id_safe}-{stat_title_safe}-{from_year}-{to_year}_{time_str}.{file_format}"
    latest_path = os.path.join(latest_dir, filename)
    with open(latest_path, 'w', encoding='utf-8') as f:
        if file_format == 'json':
            json.dump(latest, f, ensure_ascii=False, indent=2)
        else:
            f.write(str(latest))
    return latest_path 

def save_data_file(data, stats_src, data_dir, src_data_id, stat_title, from_str, to_str, file_format):
    now = datetime.now()
    time_str = now.strftime('%Y%m%d%H%M%S')
    stat_title_safe = safe_filename(stat_title)
    src_data_id_safe = safe_filename(str(src_data_id))
    filename = f"data_{src_data_id_safe}-{stat_title_safe}-{from_str}-{to_str}_{time_str}.{file_format}"
    data_path = os.path.join(data_dir, filename)
    with open(data_path, 'w', encoding='utf-8') as f:
        if file_format == 'json':
            json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            f.write(str(data))
    logging.debug(f"save_data_file: 파일 저장 완료 {data_path}")
    return data_path 


# ---------------------------------------------------------------------------
# 로그·예외 메시지용 인증키 마스킹 (통계·이동편의 수집기 공통)
# ---------------------------------------------------------------------------
# 가리는 쿼리 파라미터 이름. 대소문자를 구분하지 않는다.
#   apiKey     — KOSIS OpenAPI
#   serviceKey — 공공데이터포털(data.go.kr) 계열: GBIS, 한국철도공사, 장애인편의시설, 무장애여행
#   KEY        — 경기데이터드림(openapi.gg.go.kr)
# 이름 앞에 영숫자·밑줄이 붙은 경우(예: "monkey=")는 다른 파라미터이므로 건드리지 않는다
# (apiKey / serviceKey 는 접두어까지 포함해 별도 항목으로 등록돼 있어 그대로 가려진다).
_AUTH_PARAM_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_])((?:apiKey|serviceKey|key)=)[^&\s'\"<>)]+",
    flags=re.IGNORECASE,
)


def mask_auth_in_text(value):
    """문자열 안의 인증키 쿼리 파라미터 값을 ``***`` 로 바꿔 돌려준다.

    URL 한 줄뿐 아니라 URL 이 섞인 임의의 문장(예: requests 예외 문자열
    ``... Max retries exceeded with url: /path?serviceKey=abc&pageNo=1 (Caused by ...)``)에도
    쓸 수 있도록, 값의 끝을 ``&``·공백·따옴표·괄호 중 먼저 나오는 곳으로 본다.

    인자:
        value: URL 또는 URL 이 포함된 문자열. 문자열이 아니면 ``str()`` 로 바꾼 뒤 처리한다.
    반환:
        마스킹된 문자열. ``value`` 가 None 이거나 빈 값이면 받은 값을 그대로 돌려준다.
    실패 시 동작:
        예외를 올리지 않는다(정규식 치환만 수행).
    """
    if not value:
        return value
    return _AUTH_PARAM_PATTERN.sub(r"\1***", str(value))
