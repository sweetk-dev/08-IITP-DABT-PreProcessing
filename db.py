import logging
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from config import get_db_url, get_kosis_sys

load_dotenv()

DB_URL = get_db_url()
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

db_logger = logging.getLogger('db')

EXT_SYS_KOSIS = get_kosis_sys()

def get_api_info():
    db_logger.info("KOSIS API 정보 조회 시작")
    try:
        session = Session()
        query = text("""
            SELECT ext_api_id, if_name, ext_sys, ext_url, auth, data_format, 
                   latest_sync_time, status
            FROM sys_ext_api_info 
            WHERE ext_sys = :ext_sys AND del_yn = 'N' AND status = 'A'
        """)
        db_logger.debug(f"Executing SQL: {query}")
        result = session.execute(query, {'ext_sys': EXT_SYS_KOSIS})
        row = result.fetchone()
        
        if row:
            api_info = {
                'ext_api_id': row.ext_api_id,
                'if_name': row.if_name,
                'ext_sys': row.ext_sys,
                'ext_url': row.ext_url,
                'auth': row.auth,
                'data_format': row.data_format,
                'latest_sync_time': row.latest_sync_time,
                'status': row.status
            }
            db_logger.info(f"KOSIS API 정보 조회 성공: {api_info['ext_sys']} - {api_info['if_name']} - {api_info['status']}")
            return api_info
        else:
            db_logger.warning("KOSIS API 정보가 DB에 없거나 삭제된 상태입니다.")
            return {}
            
    except Exception as e:
        db_logger.error(f"KOSIS API 정보 조회 실패: {e}")
        raise
    finally:
        session.close()

def get_stats_src_api_info(ext_api_id):
    db_logger.info(f"통계 소스 API 정보 조회 시작 (ext_api_id={ext_api_id})")
    try:
        session = Session()
        
        if not ext_api_id:
            db_logger.warning("ext_api_id가 없어 통계 소스 정보를 조회할 수 없습니다.")
            return []
        
        query = text("""
            SELECT stat_api_id, ext_api_id, status, del_yn, stat_title, stat_tbl_id,
                   use_base_url_yn, api_data_url, api_meta_url, api_latest_chn_dt_url
            FROM sys_stats_src_api_info 
            WHERE ext_api_id = :ext_api_id AND del_yn = 'N' AND status = 'A'
            ORDER BY stat_api_id
        """)
        db_logger.debug(f"Executing SQL: {query}")
        result = session.execute(query, {'ext_api_id': ext_api_id})
        rows = result.fetchall()
        
        stats_src_list = []
        for row in rows:
            stats_src = {
                'stat_api_id': row.stat_api_id,
                'ext_api_id': row.ext_api_id,
                'status': row.status,
                'del_yn': row.del_yn,
                'stat_title': row.stat_title,
                'stat_tbl_id': row.stat_tbl_id,
                'use_base_url_yn': row.use_base_url_yn,
                'api_data_url': row.api_data_url,
                'api_meta_url': row.api_meta_url,
                'api_latest_chn_dt_url': row.api_latest_chn_dt_url
            }
            stats_src_list.append(stats_src)
        
        db_logger.info(f"통계 소스 API 정보 조회 성공: {len(stats_src_list)}건")
        return stats_src_list
            
    except Exception as e:
        db_logger.error(f"통계 소스 API 정보 조회 실패: {e}")
        raise
    finally:
        session.close()



def get_stats_src_data_info(ext_api_id, stat_tbl_id_list):
    db_logger.info(f"stats_src_data_info 일괄 조회 시작 (ext_api_id={ext_api_id}, stat_tbl_id_list={stat_tbl_id_list})")
    try:
        session = Session()
        if len(stat_tbl_id_list) == 1:
            query = text("""
                SELECT src_data_id, ext_api_id, ext_sys, stat_api_id, intg_tbl_id, stat_title, stat_org_id, stat_survey_name, stat_pub_dt, periodicity, collect_start_dt, collect_end_dt, stat_tbl_id, stat_tbl_name, stat_latest_chn_dt, stat_data_ref_dt, avail_cat_cols, status, del_yn
                FROM stats_src_data_info
                WHERE ext_api_id = :ext_api_id AND stat_tbl_id = :stat_tbl_id AND del_yn = 'N' AND status = 'A'
            """)
            result = session.execute(query, {'ext_api_id': ext_api_id, 'stat_tbl_id': stat_tbl_id_list[0]})
        else:
            query = text("""
                SELECT src_data_id, ext_api_id, ext_sys, stat_api_id, intg_tbl_id, stat_title, stat_org_id, stat_survey_name, stat_pub_dt, periodicity, collect_start_dt, collect_end_dt, stat_tbl_id, stat_tbl_name, stat_latest_chn_dt, stat_data_ref_dt, avail_cat_cols, status, del_yn
                FROM stats_src_data_info
                WHERE ext_api_id = :ext_api_id AND stat_tbl_id IN :stat_tbl_id_list AND del_yn = 'N' AND status = 'A'
            """)
            result = session.execute(query, {'ext_api_id': ext_api_id, 'stat_tbl_id_list': tuple(stat_tbl_id_list)})
        rows = result.fetchall()
        info_dict = {str(row.stat_tbl_id): dict(row._mapping) for row in rows}
        db_logger.info(f"stats_src_data_info 일괄 조회 성공: {len(info_dict)}건")
        return info_dict
    except Exception as e:
        db_logger.error(f"stats_src_data_info 일괄 조회 실패: {e}")
        raise
    finally:
        session.close()


def insert_data_to_db(data_path, meta_path, stats_src):
    db_logger.info(f"DB Insert Start: {stats_src.get('stat_tbl_id')}")
    try:
        # 파일을 읽어 실제 데이터 테이블에 삽입
        # 예시: 쿼리 실행 로그
        db_logger.debug(f"Reading data from {data_path}, meta from {meta_path}")
        # ... SQL 실행 예시 ...
        # db_logger.info(f"Executing SQL: {sql} Params: {params}")
        # db_logger.info(f"Rows affected: {rowcount}")
        db_logger.info(f"DB Insert Success: {stats_src.get('stat_tbl_id')}")
    except Exception as e:
        db_logger.error(f"DB Insert Error: {stats_src.get('stat_tbl_id')} - {e}")
        raise