import argparse
import os
import logging
import sys
import json
from datetime import datetime, date
from file_utils import save_data_and_meta_files, save_meta_file, save_latest_file, save_data_file
from db import get_db_url, get_api_info, get_stats_src_api_info, insert_data_to_db, get_stats_src_data_info
from kosis_api import fetch_kosis_data, fetch_kosis_meta, fetch_kosis_latest
from config import load_target_src_tbl_id_list, get_log_level, get_data_collection_scope, get_parallel_workers_file
from db_processing import process_db_insertion
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def create_data_save_directory():
    data_root = "kosis_data"
    today_str = datetime.now().strftime('%Y%m%d')
    root_dir = os.path.join(data_root, today_str)
    data_dir = os.path.join(root_dir, "data")
    meta_dir = os.path.join(root_dir, "meta")
    latest_dir = os.path.join(root_dir, "latest")

    for d in [data_root, root_dir, data_dir, meta_dir, latest_dir]:
        if not os.path.exists(d):
            os.makedirs(d)

    return {
        "root": root_dir,
        "data": data_dir,
        "meta": meta_dir,
        "latest": latest_dir
    }

def parse_args():
    parser = argparse.ArgumentParser(description='KOSIS 데이터 API 연동 툴')
    parser.add_argument('--mode', choices=['file', 'db'], required=True, help='file: 파일 저장만, db: 파일 저장 후 DB 삽입')
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

def get_filtered_stats_src_list(data_collection_scope):
    api_info = get_api_info()
    stats_src_list = get_stats_src_api_info(api_info.get('ext_api_id'))
    env_target_list = None
    if data_collection_scope == 'PART':
        env_target_list = load_target_src_tbl_id_list()
        target_id_set = set(item['stat_tbl_id'] for item in env_target_list)
        existing_stat_tbl_ids = {s.get('stat_tbl_id') for s in stats_src_list}
        missing_stat_tbl_ids = target_id_set - existing_stat_tbl_ids
        if missing_stat_tbl_ids:
            error_msg = f"설정된 stat_tbl_id 중 DB에 존재하지 않는 것들: {list(missing_stat_tbl_ids)}"
            logging.error(error_msg)
            print(f"[ERROR] {error_msg}")
            sys.exit(1)
        stats_src_list = [s for s in stats_src_list if s.get('stat_tbl_id') in target_id_set]
        logging.info(f"PART 모드: {len(target_id_set)} -> {len(stats_src_list)}개 통계 소스 필터링 완료")
    return api_info, stats_src_list, env_target_list

def prepare_data_directories():
    return create_data_save_directory()

def save_single_file(args):
    api_info, stats_src, dirs, data_info = args
    stat_tbl_id = stats_src['stat_tbl_id']
    src_data_id = data_info.get('src_data_id', 'unknown')
    stat_title = data_info.get('stat_title', 'unknown')
    from_str = data_info.get('collect_start_dt', 'unknown')
    to_str = data_info.get('collect_end_dt', 'unknown')
    meta_format = 'xml'
    func_name = 'save_single_file'
    try:
        logging.info(f"[{stat_tbl_id}] {func_name} - 메타 파일 저장 시작")
        if stats_src.get('api_meta_url'):
            try:
                meta_url_info = json.loads(stats_src['api_meta_url'])
                meta_format = meta_url_info.get('format', 'xml')
            except Exception as e:
                logging.debug(f"[{stat_tbl_id}] {func_name} - api_meta_url 파싱 실패: {e}")
        meta = fetch_kosis_meta(api_info, stats_src, data_info)
        logging.debug(f"[{stat_tbl_id}] {func_name} - fetch_kosis_meta 결과: {str(meta)[:200]}")
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
        latest = fetch_kosis_latest(api_info, stats_src, data_info)
        logging.debug(f"[{stat_tbl_id}] {func_name} - fetch_kosis_latest 결과: {str(latest)[:200]}")
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
        data = fetch_kosis_data(api_info, stats_src, data_info)
        logging.debug(f"[{stat_tbl_id}] {func_name} - fetch_kosis_data 결과: {str(data)[:200]}")
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
        }
    except Exception as e:
        logging.error(f"[{stat_tbl_id}] {func_name} - 파일 저장 중 에러: {e}", exc_info=True)
        raise

def save_all_files(api_info, stats_src_list, dirs, stats_src_data_info_dict):
    saved_files_info = []
    parallel_workers = get_parallel_workers_file()
    args_list = []
    for stats_src in stats_src_list:
        stat_tbl_id = stats_src['stat_tbl_id']
        data_info = stats_src_data_info_dict.get(stat_tbl_id, {})
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
        data_collection_scope = get_data_collection_scope()
        api_info, stats_src_list, env_target_list = get_filtered_stats_src_list(data_collection_scope)
        dirs = prepare_data_directories()
        stat_tbl_id_list = [s['stat_tbl_id'] for s in stats_src_list]
        stat_api_id = stats_src_list[0]['stat_api_id'] if stats_src_list else None
        stats_src_data_info_dict = get_stats_src_data_info(stat_api_id, stat_tbl_id_list)
        
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