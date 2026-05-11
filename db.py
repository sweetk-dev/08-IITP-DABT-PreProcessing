import logging
import os
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from config import get_db_url, get_kosis_sys

load_dotenv()

DB_URL = get_db_url()
engine = create_engine(DB_URL) if DB_URL else None
Session = sessionmaker(bind=engine) if engine else None

db_logger = logging.getLogger('db')

EXT_SYS_KOSIS = get_kosis_sys()

def set_timezone(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("SET TIME ZONE 'Asia/Seoul';")
    cursor.close()

# 엔진에 이벤트 리스너 등록
if engine:
    try:
        event.listen(engine, 'connect', set_timezone)
        db_logger.info("DB 세션 타임존을 Asia/Seoul로 설정했습니다.")
    except Exception as e:
        db_logger.error(f"DB 타임존 설정 실패: {e}")

def get_api_info(ext_sys: str = 'KOSIS'):
    """sys_ext_api_info 테이블에서 ext_sys 키로 외부 API 메타데이터를 단건 조회한다.

    이슈 #27 — 멀티소스 동적 조회 지원.
    `sys_ext_api_info.ext_sys` 컬럼을 라우팅 키로 사용하여 KOSIS 이외의 신규
    외부 API 소스도 동일한 인터페이스로 조회할 수 있도록 일반화했다.

    Args:
        ext_sys: 조회할 외부 시스템 식별자.
            - 'KOSIS' (default): 국가통계포털 (후방호환 — 인자 미지정 시)
            - 'DATA_GO_KR': 공공데이터포털 (예시)
            - 'MICRODATA': 마이크로데이터 (예시)
            기타 sys_ext_api_info 에 등록된 임의 ext_sys 값.

    Returns:
        단건 dict — 컬럼: ext_api_id / if_name / ext_sys / ext_url / auth /
        data_format / latest_sync_time / status.
        해당 ext_sys 의 active 행이 없으면 빈 dict ({}) 반환.

    예시:
        >>> info = get_api_info()                  # 기존 KOSIS 호출 (후방호환)
        >>> info = get_api_info('KOSIS')           # 명시 호출
        >>> info = get_api_info('DATA_GO_KR')      # 신규 소스
    """
    db_logger.info(f"외부 API 정보 조회 시작 (ext_sys={ext_sys})")
    try:
        session = Session()
        query = text("""
            SELECT ext_api_id, if_name, ext_sys, ext_url, auth, data_format, 
                   latest_sync_time, status
            FROM sys_ext_api_info 
            WHERE ext_sys = :ext_sys AND del_yn = 'N' AND status = 'A'
        """)
        db_logger.debug(f"Executing SQL: {query}")
        result = session.execute(query, {'ext_sys': ext_sys})
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
            db_logger.info(f"외부 API 정보 조회 성공: {api_info['ext_sys']} - {api_info['if_name']} - {api_info['status']}")
            return api_info
        else:
            db_logger.warning(f"외부 API 정보가 DB에 없거나 삭제된 상태입니다 (ext_sys={ext_sys}).")
            return {}
            
    except Exception as e:
        db_logger.error(f"외부 API 정보 조회 실패 (ext_sys={ext_sys}): {e}")
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


