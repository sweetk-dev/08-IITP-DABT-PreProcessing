import os
from dotenv import load_dotenv
import configparser
import logging

load_dotenv()

# --- DB 연결 설정 ---
_DB_URL = os.getenv('DB_URL')
_DB_BATCH_SIZE = int(os.getenv('DB_BATCH_SIZE', '100'))

# --- 로깅 설정 ---
_LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# --- KOSIS API 설정 ---
_EXT_API_INFO_KOSIS_SYS = os.getenv('EXT_API_INFO_KOSIS_SYS', 'KOSIS')

# --- 병렬처리 성능 설정 ---
_PARALLEL_WORKERS_FILE = int(os.getenv('PARALLEL_WORKERS_FILE', '4'))
_PARALLEL_WORKERS_DB = int(os.getenv('PARALLEL_WORKERS_DB', '2'))

# --- 데이터 수집 옵션 ---
_DATA_COLLECTION_SCOPE = os.getenv('DATA_COLLECTION_SCOPE', 'ALL').upper()
_CHECK_DATA_LATEST_DATE_MODE = os.getenv('CHECK_DATA_LATEST_DATE_MODE', 'OFF').upper()


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
    """
    KOSIS 최신 변경일 기준 업데이트 여부 체크 모드를 반환합니다.

    반환값:
        - 'ON' : KOSIS 최종 변경일이 내부 기록보다 최신인 경우에만 DB 업데이트
        - 'OFF': 항상 DB 업데이트 (기본값)

    .env 설정 키: CHECK_DATA_LATEST_DATE_MODE=ON 또는 OFF
    사용 위치: db_processing.py process_single_statistic() (구현 예정)
    """
    return _CHECK_DATA_LATEST_DATE_MODE


def get_parallel_workers_file():
    return min(_PARALLEL_WORKERS_FILE, 10)

def get_parallel_workers_db():
    return min(_PARALLEL_WORKERS_DB, 5)


# 여러 줄 섹션 기반 테이블 ID 리스트 로드 함수
def _parse_target_line(line):
    """[TARGET_SRC_TBL_ID_LIST] 섹션의 한 줄을 {'stat_tbl_id', 'from_year'} 로 바꾼다.

    서식(.env.test.example 기준): 한 줄에 통계표 ID 하나. 선택적으로 쉼표 뒤에 수집 시작 연도를
    붙일 수 있다 — ``DT_117102_A047`` 또는 ``DT_117102_A047, 2015``.
    쉼표를 나누지 않으면 ``"DT_117102_A047, 2015"`` 전체가 통계표 ID 로 취급되어
    DB 에 없는 ID 로 판정되고 실행이 중단된다.

    from_year 는 숫자일 때만 정수로 넣고, 그 외에는 None 이다.
    """
    parts = [p.strip() for p in line.split(',')]
    stat_tbl_id = parts[0]
    from_year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    return {'stat_tbl_id': stat_tbl_id, 'from_year': from_year}


def load_target_src_tbl_id_list(env_path='.env'):
    """PARTIAL 모드의 수집 대상 통계표 목록을 .env 의 [TARGET_SRC_TBL_ID_LIST] 섹션에서 읽는다.

    1차로 configparser 를 쓰고, 실패하면 줄 단위 직접 파싱(_parse_env_file_directly)으로 넘어간다.
    일반적인 .env 는 첫머리가 ``DB_URL=...`` 같은 "섹션 없는 키=값" 이라 configparser 가
    MissingSectionHeaderError 를 내므로, 실제로는 직접 파싱 경로가 주로 쓰인다.
    두 경로 모두 한 줄을 _parse_target_line 으로 해석해 같은 결과를 낸다.

    반환: [{'stat_tbl_id': str, 'from_year': int|None}, ...] (섹션이 없으면 빈 리스트)
    """
    parser = configparser.ConfigParser(allow_no_value=True)
    parser.optionxform = str  # 대소문자 구분
    try:
        parser.read(env_path, encoding='utf-8')
        if 'TARGET_SRC_TBL_ID_LIST' in parser._sections:
            raw_keys = list(parser._sections['TARGET_SRC_TBL_ID_LIST'].keys())
            return [_parse_target_line(line) for line in raw_keys]
        return []
    except Exception as e:
        # 예외 원문을 로그에 쓰지 않는다. configparser 의 파싱 예외 메시지에는 문제가 된 줄의
        # 내용이 그대로 들어가는데, .env 의 첫 유효 줄은 보통 DB 접속 문자열(DB_URL=...://user:password@...)
        # 이다. 예외 종류만 남기면 원인(섹션 헤더 없음 등)은 알 수 있고 비밀값은 남지 않는다.
        logging.error(f".env 파일 파싱 실패 ({type(e).__name__}) — 줄 단위 파싱으로 진행")
        # fallback: 직접 파싱
        return [_parse_target_line(line) for line in _parse_env_file_directly(env_path)]

def _parse_env_file_directly(env_path):
    """섹션 헤더가 없는 .env 파일을 직접 파싱하여 TARGET_SRC_TBL_ID_LIST 섹션의 줄들을 추출

    반환값은 섹션 안의 "주석·빈 줄을 뺀 원문 줄" 리스트다. 쉼표 분리(ID, 시작연도)는
    호출자(load_target_src_tbl_id_list)가 _parse_target_line 으로 한다.
    """
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
        # 파일 열기·디코딩 예외에는 경로와 OS 오류만 들어가지만, 위와 같은 기준으로 종류만 남긴다.
        logging.error(f".env 파일 파싱 실패 ({type(e).__name__})")
        return []
