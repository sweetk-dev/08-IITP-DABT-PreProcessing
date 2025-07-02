import logging
import json
import xml.etree.ElementTree as ET
from sqlalchemy.orm import sessionmaker
from db import engine
from datetime import datetime
from sqlalchemy import text
from config import get_db_batch_size, get_parallel_workers_db
import json as pyjson
from concurrent.futures import ThreadPoolExecutor, as_completed

# SQLAlchemy Session을 생성합니다.
Session = sessionmaker(bind=engine)

def process_db_insertion(saved_files_info, api_info, stats_src_list, stats_src_data_info_dict):
    """
    저장된 파일들을 기반으로 DB에 데이터를 삽입/수정하는 전체 프로세스를 관리합니다.
    :param saved_files_info: save_all_files에서 반환된 파일 경로 정보 리스트
    :param api_info: API 정보
    :param stats_src_list: 통계 소스 정보 리스트
    :param stats_src_data_info_dict: 통계 소스 데이터 정보 딕셔너리
    """
    logging.info("DB 삽입/수정 프로세스를 시작합니다.")

    parallel_workers = get_parallel_workers_db()
    def worker(file_info):
        session = Session()
        try:
            stat_tbl_id = file_info['stat_tbl_id']
            stats_src = next((s for s in stats_src_list if s['stat_tbl_id'] == stat_tbl_id), None)
            stats_data_info = stats_src_data_info_dict.get(stat_tbl_id, {})
            process_single_statistic(session, file_info, api_info, stats_src, stats_data_info)
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"DB 처리 중 에러(통계: {file_info['stat_tbl_id']}): {e}", exc_info=True)
            raise
        finally:
            session.close()

    try:
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = [executor.submit(worker, file_info) for file_info in saved_files_info]
            for future in as_completed(futures):
                future.result()  # 예외 발생 시 raise
        # 모든 통계 성공 후에만 시스템 전체 동기화 시각 갱신
        _update_sys_ext_api_info(Session(), api_info.get('ext_api_id'))
        logging.info("DB 처리가 성공적으로 완료되었습니다.")
    except Exception as e:
        logging.error(f"DB 처리 중 에러가 발생하여 롤백합니다: {e}", exc_info=True)
        raise

def process_single_statistic(session, file_info, api_info, stats_src, stats_data_info):
    """
    하나의 통계 데이터에 대한 DB 처리 로직을 담당합니다.
    (1단계 ~ 5단계 로직이 여기에 구현됩니다)
    """
    logging.info(f"[{stats_src.get('stat_tbl_id')}] 단일 통계 처리 시작.")
    # 1. latest 파일 파싱 (최신 날짜 추출)
    latest_path = file_info['latest_path']
    latest_date = _parse_latest_file_for_latest_date(latest_path)
    logging.info(f"최신 SendDe 날짜 추출: {latest_date}")

    # 2. data 파일 파싱
    data_path = file_info['data_path']
    with open(data_path, 'r', encoding='utf-8') as f:
        data_json = json.load(f)
    logging.info(f"data 파일 로드: {data_path}, 레코드 수: {len(data_json) if isinstance(data_json, list) else '1'}")

    # 3. stats_kosis_origin_data 테이블에 bulk insert
    _insert_origin_data(session, data_json, file_info, stats_src, stats_data_info, latest_date)
    logging.info(f"stats_kosis_origin_data 테이블에 데이터 삽입 완료.")

    # 4. 통계 통합 테이블(intg_tbl_id)로 데이터 이관
    _transfer_to_integration_table(session, file_info, stats_src, stats_data_info, latest_date)
    logging.info(f"통계 통합 테이블({stats_data_info.get('intg_tbl_id')})로 데이터 이관 완료.")

    # 5. 메타데이터 테이블(stats_kosis_metadata_code) 적재
    meta_path = file_info['meta_path']
    _insert_metadata(session, meta_path, file_info, stats_src, stats_data_info, latest_date)
    logging.info(f"stats_kosis_metadata_code 테이블에 메타데이터 적재 완료.")

    # 6. stats_src_data_info 테이블 업데이트
    _update_stats_src_data_info(session, file_info, data_json, latest_date)

    # 7. 관리 테이블(sys_stats_src_api_info, sys_ext_api_info) 최신화
    _update_management_tables(session, file_info, api_info, stats_src, stats_data_info)

def _parse_latest_file_for_latest_date(latest_path):
    """
    latest xml 파일에서 SendDe 중 가장 최신 날짜(YYYY-MM-DD)를 추출
    """
    tree = ET.parse(latest_path)
    root = tree.getroot()
    send_de_list = [row.findtext('SendDe') for row in root.findall('.//MetaRow') if row.findtext('SendDe')]
    # YYYY-MM-DD 형식으로 변환 (예: 2024-12-30)
    send_de_list = [d for d in send_de_list if d]
    if not send_de_list:
        return None
    # 가장 최신 날짜 반환
    return max(send_de_list)

def _insert_origin_data(session, data_json, file_info, stats_src, stats_data_info, latest_date):
    """
    stats_kosis_origin_data 테이블에 데이터 bulk insert
    """
    from datetime import date
    rows = []
    src_data_id = file_info['src_data_id']
    stat_latest_chn_dt = latest_date
    data_ref_dt = date.today()
    created_by = "SYS-BACH"

    # data_json이 리스트가 아닐 경우 리스트로 변환
    if not isinstance(data_json, list):
        data_json = [data_json]

    for row in data_json:
        db_row = {
            'src_data_id': src_data_id,
            'org_id': row.get('ORG_ID') or row.get('ORG_NM') or 0,  # 실제 데이터에 맞게 조정 필요
            'tbl_id': row.get('TBL_ID') or row.get('TBL_NM') or stats_src.get('stat_tbl_id'),
            'tbl_nm': row.get('TBL_NM') or '',
            'c1': row.get('C1') or '',
            'c2': row.get('C2') or '',
            'c3': row.get('C3') or '',
            'c4': row.get('C4') or '',
            'c1_obj_nm': row.get('C1_OBJ_NM') or '',
            'c2_obj_nm': row.get('C2_OBJ_NM') or '',
            'c3_obj_nm': row.get('C3_OBJ_NM') or '',
            'c4_obj_nm': row.get('C4_OBJ_NM') or '',
            'c1_nm': row.get('C1_NM') or '',
            'c2_nm': row.get('C2_NM') or '',
            'c3_nm': row.get('C3_NM') or '',
            'c4_nm': row.get('C4_NM') or '',
            'itm_id': row.get('ITM_ID') or row.get('ITM_NM') or '',
            'itm_nm': row.get('ITM_NM') or '',
            'unit_nm': row.get('UNIT_NM') or '',
            'prd_se': row.get('PRD_SE') or '',
            'prd_de': row.get('PRD_DE') or '',
            'dt': row.get('DT') or '',
            'lst_chn_de': row.get('LST_CHN_DE') or '',
            'stat_latest_chn_dt': stat_latest_chn_dt,
            'data_ref_dt': data_ref_dt,
            'created_by': created_by
        }
        rows.append(db_row)

    if not rows:
        logging.warning("삽입할 데이터가 없습니다.")
        return

    insert_sql = '''
    INSERT INTO stats_kosis_origin_data (
        src_data_id, org_id, tbl_id, tbl_nm,
        c1, c2, c3, c4,
        c1_obj_nm, c2_obj_nm, c3_obj_nm, c4_obj_nm,
        c1_nm, c2_nm, c3_nm, c4_nm,
        itm_id, itm_nm, unit_nm,
        prd_se, prd_de, dt, lst_chn_de,
        stat_latest_chn_dt, data_ref_dt, created_by
    ) VALUES (
        :src_data_id, :org_id, :tbl_id, :tbl_nm,
        :c1, :c2, :c3, :c4,
        :c1_obj_nm, :c2_obj_nm, :c3_obj_nm, :c4_obj_nm,
        :c1_nm, :c2_nm, :c3_nm, :c4_nm,
        :itm_id, :itm_nm, :unit_nm,
        :prd_se, :prd_de, :dt, :lst_chn_de,
        :stat_latest_chn_dt, :data_ref_dt, :created_by
    )
    '''
    batch_size = get_db_batch_size()
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        session.execute(
            text(insert_sql),
            batch
        )
        logging.info(f"stats_kosis_origin_data에 {len(batch)}건 bulk insert 완료.")

def _transfer_to_integration_table(session, file_info, stats_src, stats_data_info, latest_date):
    """
    stats_kosis_origin_data에서 intg_tbl_id(통합 테이블)로 데이터 이관
    1. 기존 데이터 삭제
    2. 신규 데이터 insert
    """
    intg_tbl_id = stats_data_info.get('intg_tbl_id')
    src_data_id = file_info['src_data_id']
    stat_tbl_id = file_info['stat_tbl_id']
    stat_latest_chn_dt = latest_date
    if not intg_tbl_id:
        logging.warning(f"intg_tbl_id가 없어 통합 테이블 이관을 건너뜁니다. stat_tbl_id={stat_tbl_id}")
        return

    # 1. 기존 데이터 삭제
    delete_sql = f"""
    DELETE FROM {intg_tbl_id}
    WHERE src_data_id = :src_data_id
      AND src_latest_chn_dt = :stat_latest_chn_dt
    """
    session.execute(
        text(delete_sql),
        {
            'src_data_id': src_data_id,
            'stat_latest_chn_dt': stat_latest_chn_dt
        }
    )
    logging.info(f"{intg_tbl_id}에서 기존 데이터 삭제 완료.")

    # 2. 신규 데이터 insert (stats_kosis_origin_data에서 select하여 insert)
    if intg_tbl_id == "stats_dis_hlth_disease_cost_sub":
        # dt는 문자열로 insert
        insert_sql = f"""
        INSERT INTO {intg_tbl_id} (
            src_data_id, prd_de, c1, c2, c3, itm_id, unit_nm, dt, lst_chn_de, src_latest_chn_dt, created_by
        )
        SELECT 
            src_data_id,
            CAST(prd_de AS INTEGER),
            c1, c2, c3,
            itm_id,
            unit_nm,
            dt,  -- 문자열 그대로
            NULLIF(lst_chn_de, '')::date,
            :stat_latest_chn_dt,
            'SYS-BACH'
        FROM stats_kosis_origin_data
        WHERE src_data_id = :src_data_id
          AND tbl_id = :stat_tbl_id
          AND stat_latest_chn_dt = :stat_latest_chn_dt
        """
    else:
        # dt는 숫자로 변환, '-'일 경우 0으로 변환
        insert_sql = f"""
        INSERT INTO {intg_tbl_id} (
            src_data_id, prd_de, c1, c2, c3, itm_id, unit_nm, dt, lst_chn_de, src_latest_chn_dt, created_by
        )
        SELECT 
            src_data_id,
            CAST(prd_de AS INTEGER),
            c1, c2, c3,
            itm_id,
            unit_nm,
            CASE WHEN dt = '-' THEN 0 ELSE CAST(dt AS NUMERIC(15,3)) END,
            NULLIF(lst_chn_de, '')::date,
            :stat_latest_chn_dt,
            'SYS-BACH'
        FROM stats_kosis_origin_data
        WHERE src_data_id = :src_data_id
          AND tbl_id = :stat_tbl_id
          AND stat_latest_chn_dt = :stat_latest_chn_dt
        """
    session.execute(
        text(insert_sql),
        {
            'src_data_id': src_data_id,
            'stat_tbl_id': stat_tbl_id,
            'stat_latest_chn_dt': stat_latest_chn_dt
        }
    )
    logging.info(f"{intg_tbl_id}로 신규 데이터 insert 완료.")

def parse_xml_skip_leading_nonxml(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if line.lstrip().startswith('<'):
            xml_start = i
            break
    else:
        raise ValueError("XML 시작 태그를 찾을 수 없습니다.")
    xml_str = ''.join(lines[xml_start:])
    return ET.fromstring(xml_str)

def _insert_metadata(session, meta_path, file_info, stats_src, stats_data_info, latest_date):
    """
    meta XML 파일을 파싱하여 stats_kosis_metadata_code 테이블에 데이터 삭제 및 bulk insert
    """
    from config import get_db_batch_size
    src_data_id = file_info['src_data_id']
    stat_tbl_id = file_info['stat_tbl_id']
    stat_latest_chn_dt = latest_date
    created_by = "SYS-BACH"

    # 1. 기존 데이터 삭제
    delete_sql = """
    DELETE FROM stats_kosis_metadata_code
    WHERE src_data_id = :src_data_id
      AND tbl_id = :stat_tbl_id
      AND stat_latest_chn_dt = :stat_latest_chn_dt
    """
    session.execute(
        text(delete_sql),
        {
            'src_data_id': src_data_id,
            'stat_tbl_id': stat_tbl_id,
            'stat_latest_chn_dt': stat_latest_chn_dt
        }
    )
    logging.info("stats_kosis_metadata_code에서 기존 메타데이터 삭제 완료.")

    # 2. meta XML 파싱 및 row 매핑 (설명문 등 무시)
    root = parse_xml_skip_leading_nonxml(meta_path)
    rows = []
    for row in root.findall('.//MetaRow'):
        db_row = {
            'src_data_id': src_data_id,
            'tbl_id': stat_tbl_id,
            'obj_id': row.findtext('objId') or '',
            'obj_nm': row.findtext('objNm') or '',
            'itm_id': row.findtext('itmId') or '',
            'itm_nm': row.findtext('itmNm') or '',
            'up_itm_id': row.findtext('upItmId') or '',
            'obj_id_sn': row.findtext('objIdSn') or None,
            'unit_id': row.findtext('unitId') or '',
            'unit_nm': row.findtext('unitNm') or '',
            'stat_latest_chn_dt': stat_latest_chn_dt,
            'created_by': created_by
        }
        rows.append(db_row)
    if not rows:
        logging.warning("삽입할 메타데이터가 없습니다.")
        return

    # 3. bulk insert
    insert_sql = """
    INSERT INTO stats_kosis_metadata_code (
        src_data_id, tbl_id, obj_id, obj_nm, itm_id, itm_nm, up_itm_id, obj_id_sn, unit_id, unit_nm, stat_latest_chn_dt, created_by
    ) VALUES (
        :src_data_id, :tbl_id, :obj_id, :obj_nm, :itm_id, :itm_nm, :up_itm_id, :obj_id_sn, :unit_id, :unit_nm, :stat_latest_chn_dt, :created_by
    )
    """
    batch_size = get_db_batch_size()
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        session.execute(
            text(insert_sql),
            batch
        )
        logging.info(f"stats_kosis_metadata_code에 {len(batch)}건 bulk insert 완료.")

def _update_stats_src_data_info(session, file_info, data_json, latest_date):
    """
    stats_src_data_info 테이블의 stat_latest_chn_dt, stat_data_ref_dt, avail_cat_cols 컬럼 업데이트
    """
    from datetime import date
    src_data_id = file_info['src_data_id']
    stat_tbl_id = file_info['stat_tbl_id']
    stat_latest_chn_dt = latest_date
    stat_data_ref_dt = date.today().strftime('%Y-%m-%d')

    # avail_cat_cols: data_json에서 실제 값이 존재하는 c1~c4만 추출
    cat_cols = []
    for c in ['c1', 'c2', 'c3', 'c4']:
        if any((row.get(c.upper()) or row.get(c)) for row in data_json):
            cat_cols.append(c)
    avail_cat_cols = pyjson.dumps(cat_cols, ensure_ascii=False)

    update_sql = """
    UPDATE stats_src_data_info
    SET stat_latest_chn_dt = :stat_latest_chn_dt,
        stat_data_ref_dt = :stat_data_ref_dt,
        avail_cat_cols = :avail_cat_cols
    WHERE src_data_id = :src_data_id
      AND stat_tbl_id = :stat_tbl_id
    """
    session.execute(
        text(update_sql),
        {
            'stat_latest_chn_dt': stat_latest_chn_dt,
            'stat_data_ref_dt': stat_data_ref_dt,
            'avail_cat_cols': avail_cat_cols,
            'src_data_id': src_data_id,
            'stat_tbl_id': stat_tbl_id
        }
    )
    logging.info(f"stats_src_data_info({src_data_id}, {stat_tbl_id}) 업데이트 완료: stat_latest_chn_dt={stat_latest_chn_dt}, stat_data_ref_dt={stat_data_ref_dt}, avail_cat_cols={avail_cat_cols}")

def _update_management_tables(session, file_info, api_info, stats_src, stats_data_info):
    """
    sys_stats_src_api_info 테이블의 최신화(업데이트)
    """
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    # sys_stats_src_api_info 업데이트
    update_sql1 = """
    UPDATE sys_stats_src_api_info
    SET latest_sync_time = :latest_sync_time
    WHERE ext_api_id = :ext_api_id
      AND stat_api_id = :stat_api_id
      AND stat_tbl_id = :stat_tbl_id
    """
    session.execute(
        text(update_sql1),
        {
            'latest_sync_time': now_str,
            'ext_api_id': file_info['ext_api_id'],
            'stat_api_id': file_info['stat_api_id'],
            'stat_tbl_id': file_info['stat_tbl_id']
        }
    )
    logging.info(f"sys_stats_src_api_info({file_info['ext_api_id']}, {file_info['stat_api_id']}, {file_info['stat_tbl_id']}) 최신화 완료: latest_sync_time={now_str}")

def _update_sys_ext_api_info(session, ext_api_id):
    """
    sys_ext_api_info 테이블의 최신화(업데이트) - 전체 성공 후 한 번만 호출
    """
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    update_sql2 = """
    UPDATE sys_ext_api_info
    SET latest_sync_time = :latest_sync_time
    WHERE ext_api_id = :ext_api_id
    """
    session.execute(
        text(update_sql2),
        {
            'latest_sync_time': now_str,
            'ext_api_id': ext_api_id
        }
    )
    logging.info(f"sys_ext_api_info({ext_api_id}) 최신화 완료: latest_sync_time={now_str}")

# 세부 단계별 함수들 (추후 구현)
# def _parse_latest_file(...)
# def _insert_origin_data(...)
# def _transfer_to_integration_table(...)
# def _insert_metadata(...)
# def _update_management_tables(...) 