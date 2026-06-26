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