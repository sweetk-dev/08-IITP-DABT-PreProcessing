import os
from dotenv import load_dotenv
import configparser
import logging

load_dotenv()

#ENV Info
_DB_URL = os.getenv('DB_URL')
_LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
_DB_BATCH_SIZE = int(os.getenv('DB_BATCH_SIZE', '100'))
_EXT_API_INFO_KOSIS_SYS = os.getenv('EXT_API_INFO_KOSIS_SYS', 'KOSIS')
_PARALLEL_WORKERS_FILE = int(os.getenv('PARALLEL_WORKERS_FILE', '4'))
_PARALLEL_WORKERS_DB = int(os.getenv('PARALLEL_WORKERS_DB', '2'))
_MAX_KOSIS_API_GET_DATA_CNT = int(os.getenv('MAX_KOSIS_API_GET_DATA_CNT', '40000'))

# Data option
_KOSIS_SYS = os.getenv('EXT_API_INFO_KOSIS_SYS', 'KOSIS').upper()
_DATA_COLLECTION_SCOPE = os.getenv('DATA_COLLECTION_SCOPE', 'ALL').upper()
_CHCEK_DATA_LATEST_DATE_MODE = os.getenv('CHCEK_DATA_LATEST_DATE', '20250624').upper()




def get_db_url():
    return _DB_URL

def get_log_level():
    return _LOG_LEVEL

def get_db_batch_size():
    return _DB_BATCH_SIZE

def get_kosis_sys():
    return _EXT_API_INFO_KOSIS_SYS

def get_data_collection_scope():
    return _DATA_COLLECTION_SCOPE

def get_check_data_latest_date_mode():
    return _CHCEK_DATA_LATEST_DATE_MODE


def get_parallel_workers_file():
    return min(_PARALLEL_WORKERS_FILE, 10)

def get_parallel_workers_db():
    return min(_PARALLEL_WORKERS_DB, 5)

def  get_max_kosis_api_get_data_cnt():
    return _MAX_KOSIS_API_GET_DATA_CNT

# 여러 줄 섹션 기반 테이블 ID 리스트 로드 함수
def load_target_src_tbl_id_list(env_path='.env'):
    parser = configparser.ConfigParser(allow_no_value=True)
    parser.optionxform = str  # 대소문자 구분
    try:
        parser.read(env_path, encoding='utf-8')
        if 'TARGET_SRC_TBL_ID_LIST' in parser._sections:
            raw_keys = list(parser._sections['TARGET_SRC_TBL_ID_LIST'].keys())
            result = []
            for line in raw_keys:
                parts = [p.strip() for p in line.split(',')]
                stat_tbl_id = parts[0]
                from_year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
                result.append({'stat_tbl_id': stat_tbl_id, 'from_year': from_year})
            return result
        return []
    except Exception as e:
        logging.error(f".env 파일 파싱 실패: {e}")
        # fallback: 직접 파싱
        return [{'stat_tbl_id': tid, 'from_year': None} for tid in _parse_env_file_directly(env_path)]

def _parse_env_file_directly(env_path):
    """섹션 헤더가 없는 .env 파일을 직접 파싱하여 TARGET_SRC_TBL_ID_LIST 섹션의 값들을 추출"""
    target_ids = []
    in_target_section = False
    
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                if line == '[TARGET_SRC_TBL_ID_LIST]':
                    in_target_section = True
                    continue
                
                if in_target_section:
                    if line.startswith('[') and line.endswith(']'):
                        # 새로운 섹션 시작
                        break
                    if line:
                        target_ids.append(line)
        
        return target_ids
    except Exception as e:
        logging.error(f".env 파일 파싱 실패: {e}")
        return [] 