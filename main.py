import argparse
import os
import logging
import sys
import json
from datetime import datetime, date
from file_utils import save_data_and_meta_files, save_meta_file, save_latest_file, save_data_file
from db import get_db_url, get_api_info, get_stats_src_api_info, get_stats_src_data_info
from kosis_api import fetch_kosis_data, fetch_kosis_meta, fetch_kosis_latest
from config import load_target_src_tbl_id_list, get_log_level, get_data_collection_scope, get_parallel_workers_file
from db_processing import process_db_insertion
from collectors import BaseCollector, KosisCollector
from concurrent.futures import ThreadPoolExecutor, as_completed


# --- 이슈 #29: ext_sys 기반 멀티소스 라우팅 ---------------------------------
# 진입점 (main.py) 가 외부 시스템(ext_sys) 인자/환경변수를 받아 알맞은
# BaseCollector 어댑터로 분기합니다. 기존 KOSIS 단일 경로를 한 번에 일반화하지
# 않고, KOSIS 를 default 로 묶어 두는 방식으로 후방호환을 보장합니다.
#
# - resolve_ext_sys() : 우선순위 CLI --ext-sys > env EXT_SYS > 'KOSIS'
# - get_collector_class() : ext_sys -> BaseCollector subclass 매핑
# - _COLLECTOR_REGISTRY : 신규 소스는 이 매핑 한 줄로 등록 가능
#
# 설계 근거: docs/design/26-multi-source-architecture.md §5 마이그레이션 플랜
# 후방호환 전략: §2.4 — KOSIS 호출 경로는 어댑터를 거쳐도 동일한 응답 형태 유지.
DEFAULT_EXT_SYS = 'KOSIS'

# ext_sys 식별자 -> BaseCollector 서브클래스. 신규 소스 추가 시 한 줄만 더하면 됨.
_COLLECTOR_REGISTRY = {
    'KOSIS': KosisCollector,
}


def resolve_ext_sys(cli_value):
    """ext_sys 우선순위 해석: CLI > env(EXT_SYS) > default 'KOSIS'.

    값은 항상 대문자로 정규화하여 sys_ext_api_info.ext_sys 와 일치하도록 한다.
    """
    if cli_value:
        return cli_value.upper()
    env_value = os.getenv('EXT_SYS')
    if env_value:
        return env_value.upper()
    return DEFAULT_EXT_SYS


def get_collector_class(ext_sys):
    """ext_sys 키에 등록된 BaseCollector subclass 를 반환.

    미등록 ext_sys 는 ValueError. 새 소스 추가는 _COLLECTOR_REGISTRY 한 줄 + 새 어댑터 모듈.
    """
    cls = _COLLECTOR_REGISTRY.get(ext_sys.upper())
    if cls is None:
        raise ValueError(f"Unsupported ext_sys: {ext_sys!r}. registered: {list(_COLLECTOR_REGISTRY)}")
    return cls


def setup_logging():
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    today = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f'{today}.log')
    db_log_file = os.path.join(log_dir, f'db_{today}.log')
    log_level = getattr(logging, get_log_level().upper(), logging.INFO)
    log_format = '%(asctime)s [%(levelname)s] [PID:%(process)d][%(threadName)s] %(filename)s:%(lineno)d %(funcName)s() - %(message)s'

    # 전체 로그
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    # DB 전용 로그 핸들러
    db_logger = logging.getLogger('db')
    db_handler = logging.FileHandler(db_log_file, encoding='utf-8')
    db_handler.setFormatter(logging.Formatter(log_format))
    db_logger.addHandler(db_handler)
    db_logger.setLevel(log_level)


# 이슈 #29: 멀티소스 저장 경로 일반화 + KOSIS 후방호환
#
# KOSIS 경로 (ext_sys='KOSIS', default):
#     kosis_data/<YYYYMMDD>/{data,meta,latest}             # 기존 패턴 그대로 유지
#
# 그 외 ext_sys (ext_sys != 'KOSIS'):
#     ext_data/<EXT_SYS>/<YYYYMMDD>/{data,meta,latest}     # 신규 일반화 패턴
#
# 후방호환 결정 근거: KOSIS 가 기존 운영 중인 유일한 소스이므로 KOSIS 의
# 디렉터리 트리 / 로그 파일명 패턴 등을 변경하지 않는다 (회귀 0 보장).
LEGACY_KOSIS_DATA_ROOT = "kosis_data"
GENERIC_EXT_DATA_ROOT = "ext_data"


def _resolve_data_roots(ext_sys_norm):
    """ext_sys 별로 (data_root, root_dir) 디렉터리 경로를 결정한다.

    KOSIS 는 레거시 경로(``kosis_data/<YYYYMMDD>``) 를 그대로 사용하고,
    그 외 ext_sys 는 새 패턴(``ext_data/<EXT_SYS>/<YYYYMMDD>``) 을 사용한다.
    """
    today_str = datetime.now().strftime('%Y%m%d')
    if ext_sys_norm == DEFAULT_EXT_SYS:
        data_root = LEGACY_KOSIS_DATA_ROOT
        root_dir = os.path.join(data_root, today_str)
        ensure_dirs = [data_root, root_dir]
    else:
        data_root = GENERIC_EXT_DATA_ROOT
        root_dir = os.path.join(data_root, ext_sys_norm, today_str)
        ensure_dirs = [data_root, os.path.join(data_root, ext_sys_norm), root_dir]
    return data_root, root_dir, ensure_dirs


def create_data_save_directory(ext_sys=DEFAULT_EXT_SYS):
    """수집 결과 저장 경로 트리를 생성한다.

    - KOSIS (default): ``kosis_data/<YYYYMMDD>/{data,meta,latest}`` (후방호환)
    - 그 외:          ``ext_data/<EXT_SYS>/<YYYYMMDD>/{data,meta,latest}``

    Parameters
    ----------
    ext_sys:
        외부 시스템 식별자. 대문자로 정규화되어 디렉터리 이름에 사용.
    """
    ext_sys_norm = (ext_sys or DEFAULT_EXT_SYS).upper()
    data_root, root_dir, ensure_dirs = _resolve_data_roots(ext_sys_norm)

    data_dir = os.path.join(root_dir, "data")
    meta_dir = os.path.join(root_dir, "meta")
    latest_dir = os.path.join(root_dir, "latest")

    for d in ensure_dirs + [data_dir, meta_dir, latest_dir]:
        if not os.path.exists(d):
            os.makedirs(d)

    return {
        "root": root_dir,
        "data": data_dir,
        "meta": meta_dir,
        "latest": latest_dir,
        "ext_sys": ext_sys_norm,
    }

def parse_args():
    parser = argparse.ArgumentParser(description='외부 통계 API 연동 툴 (KOSIS 등 멀티소스 지원)')
    parser.add_argument('--mode', choices=['file', 'db'], required=True, help='file: 파일 저장만, db: 파일 저장 후 DB 삽입')
    parser.add_argument(
        '--ext-sys',
        dest='ext_sys',
        default=None,
        help='외부 시스템 식별자 (예: KOSIS). 미지정 시 환경변수 EXT_SYS, 그래도 없으면 KOSIS 사용.'
    )
    return parser.parse_args()

def check_required_env_and_args(args):
    if args.mode not in ['file', 'db']:
        logging.error(f"잘못된 실행 옵션: {args.mode}")
        print("[ERROR] 실행 옵션은 --mode file 또는 --mode db 만 가능합니다.")
        sys.exit(1)
    if not get_db_url():
        logging.error("DB_URL 환경변수가 설정되어 있지 않습니다.")
        print("[ERROR] DB_URL 환경변수가 설정되어 있지 않습니다. .env 파일을 확인하세요.")
        sys.exit(1)

def get_filtered_stats_src_list(data_collection_scope, ext_sys=DEFAULT_EXT_SYS):
    # 이슈 #27: get_api_info / get_stats_src_api_info 가 ext_sys 인자를 받도록 일반화됨.
    # default ext_sys='KOSIS' 사용 시 기존 KOSIS 경로와 동일 동작 (후방호환).
    api_info = get_api_info(ext_sys)
    stats_src_list = get_stats_src_api_info(api_info.get('ext_api_id'))
    env_target_list = None
    if data_collection_scope == 'PART':
        env_target_list = load_target_src_tbl_id_list()
        target_id_set = set(item['stat_tbl_id'] for item in env_target_list)
        existing_stat_tbl_ids = {s.get('stat_tbl_id') for s in stats_src_list}
        missing_stat_tbl_ids = target_id_set - existing_stat_tbl_ids
        logging.info(f"[DEBUG] env target_id_set!: {target_id_set}")
        logging.info(f"[DEBUG] DB existing_stat_tbl_ids: {existing_stat_tbl_ids}")
        logging.info(f"[DEBUG] missing_stat_tbl_ids: {missing_stat_tbl_ids}")
        logging.info(f"[DEBUG] stats_src_list(before filter): {stats_src_list}")
        if missing_stat_tbl_ids:
            error_msg = f"설정된 stat_tbl_id 중 DB에 존재하지 않는 것들: {list(missing_stat_tbl_ids)}"
            logging.error(error_msg)
            print(f"[ERROR] {error_msg}")
            sys.exit(1)
        stats_src_list = [s for s in stats_src_list if s.get('stat_tbl_id') in target_id_set]
        logging.info(f"[DEBUG] stats_src_list(after filter): {stats_src_list}")
        if not stats_src_list:
            logging.warning("[DEBUG] PART 모드에서 stats_src_list가 비어 있습니다!")
        logging.info(f"PART 모드: {len(target_id_set)} -> {len(stats_src_list)}개 통계 소스 필터링 완료")
    return api_info, stats_src_list, env_target_list

def prepare_data_directories(ext_sys=DEFAULT_EXT_SYS):
    """ext_sys 를 전달하여 저장 경로 트리를 준비한다."""
    return create_data_save_directory(ext_sys=ext_sys)

def save_single_file(args):
    api_info, stats_src, dirs, data_info = args
    stat_tbl_id = stats_src['stat_tbl_id']
    src_data_id = data_info.get('src_data_id', 'unknown')
    stat_title = data_info.get('stat_title', 'unknown')
    _start = data_info.get('collect_start_dt', 'unknown')
    _end = data_info.get('collect_end_dt', 'unknown')
    from_str = str(_start)[:4] if str(_start) not in ('unknown', '', 'None') else 'unknown'
    to_str = str(_end)[:4] if str(_end) not in ('unknown', '', 'None') else 'unknown'
    meta_format = 'xml'
    func_name = 'save_single_file'

    # 이슈 #29: BaseCollector 어댑터를 통해 ext_sys 별로 수집 호출 분기.
    # KOSIS 의 경우 KosisCollector 가 기존 fetch_kosis_* 함수들을 동일 시그니처로 위임 호출하므로
    # 응답 형식/오류 동작이 그대로 보존된다 (#28 어댑터에서 검증).
    ext_sys = dirs.get('ext_sys', DEFAULT_EXT_SYS) if isinstance(dirs, dict) else DEFAULT_EXT_SYS
    collector_cls = get_collector_class(ext_sys)
    collector = collector_cls(api_info=api_info, stats_src=stats_src)

    try:
        logging.info(f"[{stat_tbl_id}] {func_name} - 메타 파일 저장 시작 (ext_sys={ext_sys})")
        if stats_src.get('api_meta_url'):
            try:
                meta_url_info = json.loads(stats_src['api_meta_url'])
                meta_format = meta_url_info.get('format', 'xml')
            except Exception as e:
                logging.debug(f"[{stat_tbl_id}] {func_name} - api_meta_url 파싱 실패: {e}")
        meta = collector.fetch_meta(data_info)
        logging.debug(f"[{stat_tbl_id}] {func_name} - fetch_meta 결과: {str(meta)[:200]}")
        meta_path = save_meta_file(meta, stats_src, dirs['meta'], src_data_id, stat_title, from_str, to_str, meta_format)
        logging.info(f"[{stat_tbl_id}] {func_name} - 메타 파일 저장 완료: {meta_path}")

        logging.info(f"[{stat_tbl_id}] {func_name} - latest 파일 저장 시작")
        latest_format = 'json'
        if stats_src.get('api_latest_chn_dt_url'):
            try:
                latest_url_info = json.loads(stats_src['api_latest_chn_dt_url'])
                latest_format = latest_url_info.get('format', 'json')
            except Exception as e:
                logging.debug(f"[{stat_tbl_id}] {func_name} - api_latest_chn_dt_url 파싱 실패: {e}")
        latest = collector.fetch_latest(data_info)
        logging.debug(f"[{stat_tbl_id}] {func_name} - fetch_latest 결과: {str(latest)[:200]}")
        latest_path = save_latest_file(latest, stats_src, dirs['latest'], src_data_id, stat_title, from_str, to_str, latest_format)
        logging.info(f"[{stat_tbl_id}] {func_name} - latest 파일 저장 완료: {latest_path}")

        logging.info(f"[{stat_tbl_id}] {func_name} - 데이터 파일 저장 시작")
        data_format = 'json'
        if stats_src.get('api_data_url'):
            try:
                data_url_info = json.loads(stats_src['api_data_url'])
                data_format = data_url_info.get('format', 'json')
            except Exception as e:
                logging.debug(f"[{stat_tbl_id}] {func_name} - api_data_url 파싱 실패: {e}")
        data = collector.fetch_data(data_info)
        logging.debug(f"[{stat_tbl_id}] {func_name} - fetch_data 결과: {str(data)[:200]}")
        data_path = save_data_file(data, stats_src, dirs['data'], src_data_id, stat_title, from_str, to_str, data_format)
        logging.info(f"[{stat_tbl_id}] {func_name} - 데이터 파일 저장 완료: {data_path}")
        return {
            'stat_tbl_id': stat_tbl_id,
            'meta_path': meta_path,
            'latest_path': latest_path,
            'data_path': data_path,
            'ext_api_id': api_info.get('ext_api_id'),
            'stat_api_id': stats_src.get('stat_api_id'),
            'src_data_id': src_data_id,
            'ext_sys': ext_sys,
        }
    except Exception as e:
        logging.error(f"[{stat_tbl_id}] {func_name} - 파일 저장 중 에러: {e}", exc_info=True)
        raise RuntimeError(f"[{stat_tbl_id}] {func_name} - 파일 저장 실패") from e

def save_all_files(api_info, stats_src_list, dirs, stats_src_data_info_dict):
    saved_files_info = []
    parallel_workers = get_parallel_workers_file()
    args_list = []
    for stats_src in stats_src_list:
        stat_tbl_id = str(stats_src['stat_tbl_id'])
        data_info = stats_src_data_info_dict.get(stat_tbl_id, {})
        if not data_info:
            logging.warning(f"[{stat_tbl_id}] DB 매핑 정보 없음. 파일명에 unknown이 들어갈 수 있습니다.")
        args_list.append((api_info, stats_src, dirs, data_info))
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = [executor.submit(save_single_file, args) for args in args_list]
        for future in as_completed(futures):
            result = future.result()
            saved_files_info.append(result)
    return saved_files_info

def main():
    setup_logging()
    try:
        args = parse_args()
        check_required_env_and_args(args)
        ext_sys = resolve_ext_sys(getattr(args, 'ext_sys', None))
        logging.info(f"수집 외부 시스템: ext_sys={ext_sys}")
        data_collection_scope = get_data_collection_scope()
        api_info, stats_src_list, env_target_list = get_filtered_stats_src_list(data_collection_scope, ext_sys=ext_sys)
        dirs = prepare_data_directories(ext_sys=ext_sys)
        # dirs['ext_sys'] 는 create_data_save_directory 에서 이미 정규화되어 채워짐.
        stat_tbl_id_list = [s['stat_tbl_id'] for s in stats_src_list]
        ext_api_id = stats_src_list[0]['ext_api_id'] if stats_src_list else None
        stats_src_data_info_dict = get_stats_src_data_info(ext_api_id, stat_tbl_id_list)

        saved_files_info = save_all_files(api_info, stats_src_list, dirs, stats_src_data_info_dict)

        if args.mode == 'db':
            logging.info("DB 삽입 모드를 시작합니다.")
            process_db_insertion(saved_files_info, api_info, stats_src_list, stats_src_data_info_dict)
            logging.info("DB 삽입/수정 작업이 성공적으로 완료되었습니다.")

        logging.info("모든 작업이 성공적으로 완료되었습니다.")

    except Exception as e:
        logging.error(f"예상치 못한 에러 발생: {e}", exc_info=True)
        print(f"[ERROR] 실행 중 예기치 않은 에러가 발생했습니다: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
