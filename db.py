import logging
from sqlalchemy import bindparam, create_engine, text, event
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from config import get_db_url

load_dotenv()

DB_URL = get_db_url()
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,      # 끊긴 커넥션 자동 감지(원격 DB 일시 단절 대비)
    pool_recycle=1800,       # 30분마다 커넥션 재생성(stale 방지)
    # 세션 시간대를 접속 옵션으로 KST 에 고정한다. 아래 set_timezone(접속 직후 SET TIME ZONE)만으로는
    # 그 SET 이 트랜잭션 안에서 실행된 뒤 커넥션 풀 반환 시 ROLLBACK 으로 취소되어, 두 번째 사용부터
    # 세션이 UTC 로 돌아간다(2026-10-13 실측: engine.connect 는 Asia/Seoul, Session 은 Etc/UTC).
    # 그 결과 'YYYY-MM-DD HH:MM:SS'(KST) 문자열로 쓰는 latest_sync_time·updated_at 이 UTC 로 해석돼
    # 9시간 늦게 저장됐다. options 는 접속 시작 파라미터라 롤백과 무관하게 유지된다.
    connect_args={'connect_timeout': 10, 'options': '-c timezone=Asia/Seoul'},
) if DB_URL else None
Session = sessionmaker(bind=engine) if engine else None

db_logger = logging.getLogger('db')

def set_timezone(dbapi_connection, connection_record):
    # 접속 옵션(connect_args.options)이 시간대를 정한다. 이 SET 은 커밋해 두어 옵션을 쓰지 않는
    # 드라이버·URL 에서도 풀 반환 ROLLBACK 에 취소되지 않게 한다.
    cursor = dbapi_connection.cursor()
    cursor.execute("SET TIME ZONE 'Asia/Seoul';")
    cursor.close()
    try:
        dbapi_connection.commit()
    except Exception:  # noqa: BLE001 — autocommit 커넥션 등
        pass

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

    설계 근거:
        docs/design/26-multi-source-architecture.md §3.3.1
        (이슈 #26 — 멀티 외부 API 소스 확장 아키텍처)
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
        
        # intg_col_map(통합 테이블 칸 배치 규칙, 01 v1.9.0)은 칼럼이 있을 때만 읽는다.
        # 01 마이그레이션 전 DB 에서도 배치가 그대로 돌게 하기 위해서다(없으면 전 통계 NULL = 종전 규칙).
        has_col_map = bool(session.execute(text("""
            SELECT 1 FROM information_schema.columns
             WHERE table_name = 'sys_stats_src_api_info' AND column_name = 'intg_col_map'
        """)).first())
        if not has_col_map:
            db_logger.warning("sys_stats_src_api_info.intg_col_map 칼럼 없음 — 01 v1.9.0 미적용, 종전 칸 배치 규칙으로 진행")
        col_map_sql = "intg_col_map::text AS intg_col_map" if has_col_map else "NULL AS intg_col_map"
        query = text(f"""
            SELECT stat_api_id, ext_api_id, status, del_yn, stat_title, stat_tbl_id,
                   use_base_url_yn, api_data_url, api_meta_url, api_latest_chn_dt_url,
                   {col_map_sql}
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
                'api_latest_chn_dt_url': row.api_latest_chn_dt_url,
                # JSON 텍스트(또는 None). 해석·검증은 적재 단계(db_processing.parse_intg_col_map)가 한다 —
                # 잘못된 규칙이면 그 통계만 실패시키고 나머지는 계속 진행하기 위해서다.
                'intg_col_map': row.intg_col_map,
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
            """).bindparams(bindparam('stat_tbl_id_list', expanding=True))
            # expanding=True: SQLAlchemy 가 목록을 IN (:p_1, :p_2, ...) 처럼 값 개수만큼 펼친 뒤 드라이버에 넘긴다.
            # 튜플을 그대로 넘기면 psycopg2 는 클라이언트에서 ('a','b') 로 펼쳐 주지만, psycopg(3) 는 서버 측
            # 파라미터로 보내 'IN $2' 가 되어 SQL 문법 오류가 난다(SQLAlchemy 2.1 은 postgresql:// 의 기본 드라이버가
            # psycopg). 같은 형태의 db_mobility·db_odcloud_emp 조회와 같은 방식으로 맞춘다.
            result = session.execute(query, {'ext_api_id': ext_api_id, 'stat_tbl_id_list': list(stat_tbl_id_list)})
        rows = result.fetchall()
        info_dict = {str(row.stat_tbl_id): dict(row._mapping) for row in rows}
        db_logger.info(f"stats_src_data_info 일괄 조회 성공: {len(info_dict)}건")
        return info_dict
    except Exception as e:
        db_logger.error(f"stats_src_data_info 일괄 조회 실패: {e}")
        raise
    finally:
        session.close()


