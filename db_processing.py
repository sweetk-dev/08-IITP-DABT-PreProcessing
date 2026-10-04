import logging
import json
import xml.etree.ElementTree as ET
from sqlalchemy.orm import sessionmaker
from db import engine
from datetime import datetime
from sqlalchemy import text
from config import get_db_batch_size, get_parallel_workers_db
import json as pyjson
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# SQLAlchemy Session을 생성합니다.
Session = sessionmaker(bind=engine)


class KosisDataValidationError(ValueError):
    """수집된 data 응답이 적재할 수 없는 형태일 때 올린다(오류 본문·빈 응답·시점 없는 행).

    이 예외가 오르면 해당 통계는 DB 에 아무것도 쓰지 않은 상태로 실패 집계된다.
    """


# 통합 테이블의 prd_de 는 정수 컬럼이라 숫자로만 된 시점만 이관된다(이관 SQL 의
# "prd_de ~ '^[0-9]+$'" 와 같은 기준). 검증과 이관이 서로 다른 기준을 쓰면
# "검증은 통과했는데 이관은 0행"이 될 수 있으므로 같은 식을 쓴다.
_NUMERIC_PRD_DE = re.compile(r'^[0-9]+$')


def validate_kosis_data_response(data_json):
    """data 응답(파일에서 읽은 JSON)이 적재 가능한지 검사한다. DB 를 쓰지 않는 순수 함수.

    적재 가능 조건(모두 만족해야 한다):
      1. 최상위가 리스트다.
         - KOSIS 는 오류를 HTTP 200 + ``{"err": "20", "errMsg": "..."}`` dict 로 돌려준다.
           dict 를 1행짜리 데이터로 취급하면 prd_de='' 인 행이 원본에 들어가고, 통합 테이블은
           기존 행이 지워진 뒤 0행이 이관된다.
      2. 리스트가 비어 있지 않다.
         - 빈 리스트도 "기존 행 삭제 + 0행 이관"이 되어 통합 테이블이 빈다.
      3. 모든 원소가 dict 이고 ``err`` 키가 없다.
         - 기간 분할 수집 결과를 이어 붙인 리스트에 오류 dict 가 섞여 있으면 일부 기간이
           빠진 응답이다. 그대로 적재하면 빠진 기간의 기존 행이 사라진다.
      4. 숫자로만 된 ``PRD_DE`` 를 가진 행이 1개 이상이다.
         - 통합 테이블로 이관되는 것은 이런 행뿐이다. 0개면 이관 결과가 0행이다.

    인자:
        data_json: ``json.load`` 결과. 어떤 타입이든 받을 수 있다.
    반환:
        (ok, reason) 튜플. ok=True 면 reason 은 '' 이고, ok=False 면 reason 은 로그·요약에
        남길 한 줄 사유다(오류 코드·메시지 포함, 인증키는 응답 본문에 없으므로 포함되지 않는다).
    실패 시 동작:
        예외를 올리지 않는다. 판정 결과만 돌려준다.
    """
    if isinstance(data_json, dict):
        if 'err' in data_json:
            return False, (
                f"KOSIS 오류 응답(err={data_json.get('err')}, "
                f"errMsg={data_json.get('errMsg')})"
            )
        return False, "응답이 리스트가 아님(dict)"
    if not isinstance(data_json, list):
        return False, f"응답이 리스트가 아님({type(data_json).__name__})"
    if not data_json:
        return False, "응답이 빈 리스트"
    numeric_rows = 0
    for row in data_json:
        if not isinstance(row, dict):
            return False, f"응답 행이 dict 가 아님({type(row).__name__})"
        if 'err' in row:
            return False, (
                f"응답에 KOSIS 오류 행 포함(err={row.get('err')}, "
                f"errMsg={row.get('errMsg')})"
            )
        if _NUMERIC_PRD_DE.match(str(row.get('PRD_DE') or '')):
            numeric_rows += 1
    if numeric_rows == 0:
        return False, f"숫자 PRD_DE 를 가진 행이 없음(전체 {len(data_json)}행)"
    return True, ''

def process_db_insertion(saved_files_info, api_info, stats_src_list, stats_src_data_info_dict,
                         collect_failed=None):
    """
    저장된 파일들을 기반으로 DB에 데이터를 삽입/수정하는 전체 프로세스를 관리합니다.
    통계표 단위로 격리 커밋하며, 스레드에서는 종료시키지 않고 예외를 상위로 전달하여
    성공/실패를 집계합니다. 전체 성공일 때만 시스템 전체 동기화 시각을 갱신합니다.

    과거 데이터 정리는 통계 1건의 트랜잭션 안에서 "해당 통계의 기존 행 삭제 → 이번 응답 적재"로
    끝나므로(_insert_origin_data / _transfer_to_integration_table / _insert_metadata 참고),
    배치 끝에서 따로 도는 정리 단계는 없다.

    :param collect_failed: 수집(파일 저장) 단계에서 실패해 saved_files_info 에 없는 통계 목록
        [(stat_tbl_id, 사유), ...]. 하나라도 있으면 이 배치는 "전체 성공"이 아니므로
        sys_ext_api_info.latest_sync_time 을 갱신하지 않는다. None/빈 목록이면 영향 없음.
    :return: {"succeeded": [stat_tbl_id, ...], "failed": [(stat_tbl_id, error), ...]}
        failed 에는 DB 처리 단계의 실패만 담는다(collect_failed 는 호출자가 이미 갖고 있다).
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
            return stat_tbl_id
        except Exception as e:
            session.rollback()
            logging.error(f"DB 처리 중 에러(통계: {file_info['stat_tbl_id']}): {e}", exc_info=True)
            raise
        finally:
            session.close()

    succeeded = []
    failed = []
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        future_map = {executor.submit(worker, fi): fi for fi in saved_files_info}
        for future in as_completed(future_map):
            fi = future_map[future]
            try:
                future.result()
                succeeded.append(fi['stat_tbl_id'])
            except Exception as e:
                failed.append((fi['stat_tbl_id'], str(e)))

    if failed:
        logging.error(
            f"DB 처리 실패 {len(failed)}건 / 성공 {len(succeeded)}건. "
            f"실패 통계: {[f[0] for f in failed]} — 동기화 시각 갱신 보류."
        )
        return {"succeeded": succeeded, "failed": failed}

    if collect_failed:
        # 수집 단계에서 빠진 통계가 있으면 DB 단계가 전부 성공했어도 배치 전체는 성공이 아니다.
        # sys_ext_api_info.latest_sync_time 은 "이 외부 시스템의 전 통계가 마지막으로 정상
        # 동기화된 시각"이므로 갱신하지 않는다(통계별 시각은 성공한 통계만 이미 갱신됐다).
        logging.error(
            f"수집 단계 실패 {len(collect_failed)}건 — 시스템 동기화 시각 갱신 보류. "
            f"실패 통계: {[f[0] for f in collect_failed]}"
        )
        return {"succeeded": succeeded, "failed": []}

    # 전체 성공 시에만 시스템 전체 동기화 시각 갱신(세션 누수 방지 위해 try/finally close)
    sync_session = Session()
    try:
        _update_sys_ext_api_info(sync_session, api_info.get('ext_api_id'))
    finally:
        sync_session.close()
    logging.info("DB 처리가 성공적으로 완료되었습니다.")
    return {"succeeded": succeeded, "failed": []}

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

    # 2-1. 응답 검증 — DB 에 쓰기 전에 끝낸다.
    # 아래 3·4단계는 해당 통계의 기존 행을 지우고 이번 응답으로 바꿔 넣는다. 오류 본문·빈 응답·
    # 시점 없는 응답으로 이 단계를 밟으면 기존 데이터가 지워지고 0행이 남는다.
    # 여기서 예외를 올리면 이 트랜잭션에서는 아직 아무 SQL 도 실행하지 않았으므로 원본·통합·메타
    # 테이블과 동기화 시각(sys_stats_src_api_info.latest_sync_time 등)이 모두 그대로 남고,
    # 호출자(process_db_insertion 의 worker)가 이 통계를 실패로 집계한다.
    ok, reason = validate_kosis_data_response(data_json)
    if not ok:
        raise KosisDataValidationError(
            f"[{file_info.get('stat_tbl_id')}] 적재 중단(기존 데이터 유지): {reason}"
        )

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

    # 7. sys_data_summary_info 테이블 업데이트
    _update_sys_data_summary_info(session, file_info, stats_data_info, latest_date)

    # 8. 관리 테이블(sys_stats_src_api_info, sys_ext_api_info) 최신화
    _update_management_tables(session, file_info, api_info, stats_src, stats_data_info)

def _parse_latest_file_for_latest_date(latest_path):
    """
    latest 파일에서 SendDe 중 가장 최신 날짜(YYYY-MM-DD)를 추출.
    파일 확장자(.json / .xml)에 따라 적절한 파서를 사용한다.
    """
    send_de_list = []
    if latest_path.endswith('.json'):
        with open(latest_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    val = item.get('SendDe')
                    if val:
                        send_de_list.append(val)
        elif isinstance(data, dict):
            val = data.get('SendDe')
            if val:
                send_de_list.append(val)
    else:
        tree = ET.parse(latest_path)
        root = tree.getroot()
        send_de_list = [row.findtext('SendDe') for row in root.findall('.//MetaRow') if row.findtext('SendDe')]
    # YYYY-MM-DD 형식 (예: 2024-12-30)
    send_de_list = [d for d in send_de_list if d]
    if not send_de_list:
        return None
    # 가장 최신 날짜 반환
    return max(send_de_list)

def _insert_origin_data(session, data_json, file_info, stats_src, stats_data_info, latest_date):
    """
    stats_kosis_origin_data 테이블에서 해당 통계(src_data_id)의 기존 행을 지우고
    이번 응답을 bulk insert 한다.

    기존 행을 먼저 지우는 이유: 원본 테이블에 전회차 적재분이 남아 있는 상태에서 이번 응답을
    더 넣으면, 갱신일(stat_latest_chn_dt)이 같은 통계는 같은 행이 2벌이 되고 이관 SELECT 가
    두 벌을 모두 통합 테이블로 옮긴다. 삭제와 적재가 같은 트랜잭션(호출자의 session)이므로
    이후 단계가 실패하면 삭제도 함께 롤백되어 전회차 적재분이 그대로 남는다.

    호출 전제: data_json 은 validate_kosis_data_response() 를 통과한 리스트다.
    """
    from datetime import date
    rows = []
    src_data_id = file_info['src_data_id']
    stat_latest_chn_dt = latest_date
    data_ref_dt = date.today()
    created_by = "SYS-BATCH"

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

    # 이 통계의 원본 행을 전부 지운다(갱신일·생성일 조건 없음).
    # src_data_id 는 stats_src_data_info 의 PK 로 통계표 1개에 1:1 대응하므로 다른 통계의
    # 행은 지워지지 않는다. 지운 뒤 원본 테이블에 남는 이 통계의 행은 "이번 실행분"뿐이라
    # 이관 SELECT 를 src_data_id 만으로 한정할 수 있다.
    deleted = session.execute(
        text("DELETE FROM stats_kosis_origin_data WHERE src_data_id = :src_data_id"),
        {'src_data_id': src_data_id}
    )
    logging.info(f"stats_kosis_origin_data 기존 행 삭제: src_data_id={src_data_id}, {deleted.rowcount}건")

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
    1. 통합 테이블에서 해당 통계(src_data_id)의 행을 전부 삭제
    2. 방금 적재한 원본 행에서 이관(INSERT ... SELECT)

    실행 후 통합 테이블에는 해당 통계의 행이 "이번 응답 1벌"만 남는다.

    삭제 조건에 갱신일(src_latest_chn_dt)·생성일(created_at)을 두지 않는 이유
      - 갱신일이 같은 통계를 다른 날 다시 수집하면, 갱신일 조건으로는 원본의 전회차·이번
        적재분이 함께 선택되어 2벌이 이관된다.
      - "오늘 이전 생성분만 삭제" 조건은 같은 날 재실행 때 직전 실행분을 남겨 실행할 때마다
        한 벌씩 늘어난다. 또 호스트 시각과 DB 접속의 시간대가 다르면 날짜 비교가 어긋난다.
      - 통합 테이블은 통계별 최신 1벌만 보관한다(갱신일이 다른 과거 판을 함께 보관하지 않는다).
        조건 없이 지우면 이미 여러 벌로 들어 있던 행도 다음 정상 실행에서 1벌로 수렴한다.

    안전 장치
      - 이 함수는 응답 검증(validate_kosis_data_response)을 통과한 뒤에만 호출된다.
      - 이관 결과가 0행이면 예외를 올린다. 삭제와 이관은 호출자의 한 트랜잭션 안에 있으므로
        예외 시 삭제까지 롤백되어 기존 행이 그대로 남는다(통합 테이블이 비는 일이 없다).

    실패 시 동작: 예외 전파 → 호출자(worker)가 rollback 후 실패 집계.
    """
    intg_tbl_id = stats_data_info.get('intg_tbl_id')
    src_data_id = file_info['src_data_id']
    stat_tbl_id = file_info['stat_tbl_id']
    stat_latest_chn_dt = latest_date
    if not intg_tbl_id:
        logging.warning(f"intg_tbl_id가 없어 통합 테이블 이관을 건너뜁니다. stat_tbl_id={stat_tbl_id}")
        return

    # 1. 기존 데이터 삭제 — 해당 통계의 행 전부(갱신일·생성일 조건 없음, 이유는 docstring 참고)
    delete_sql = f"""
    DELETE FROM {intg_tbl_id}
    WHERE src_data_id = :src_data_id
    """
    deleted = session.execute(text(delete_sql), {'src_data_id': src_data_id})
    logging.info(f"{intg_tbl_id}에서 기존 데이터 삭제 완료: src_data_id={src_data_id}, {deleted.rowcount}건")

    # 2. 신규 데이터 insert (stats_kosis_origin_data에서 select하여 insert)
    # 원본에서 고르는 조건은 src_data_id + tbl_id 다. 갱신일(stat_latest_chn_dt) 조건은 두지 않는다.
    #   - _insert_origin_data 가 같은 트랜잭션에서 이 통계의 기존 원본 행을 지우고 넣었으므로
    #     src_data_id 로 고른 행은 전부 이번 실행분이다.
    #   - 갱신일을 읽지 못해 latest_date 가 None 인 경우 "stat_latest_chn_dt = NULL" 은 어떤
    #     행과도 일치하지 않아 0행이 이관된다.
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
            'SYS-BATCH'
        FROM stats_kosis_origin_data
        WHERE src_data_id = :src_data_id
          AND tbl_id = :stat_tbl_id
          AND prd_de ~ '^[0-9]+$'   -- 빈/비숫자 period 행 제외(CAST 실패 방지)
        """
    else:
        # dt는 숫자로 변환, '-' 또는 ''일 경우 0으로 변환
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
            CASE WHEN dt = '-' OR dt = '' THEN 0 ELSE CAST(dt AS NUMERIC(15,3)) END,
            NULLIF(lst_chn_de, '')::date,
            :stat_latest_chn_dt,
            'SYS-BATCH'
        FROM stats_kosis_origin_data
        WHERE src_data_id = :src_data_id
          AND tbl_id = :stat_tbl_id
          AND prd_de ~ '^[0-9]+$'   -- 빈/비숫자 period 행 제외(CAST 실패 방지)
        """
    inserted = session.execute(
        text(insert_sql),
        {
            'src_data_id': src_data_id,
            'stat_tbl_id': stat_tbl_id,
            'stat_latest_chn_dt': stat_latest_chn_dt
        }
    )
    # 이관 0행이면 위 DELETE 만 반영되어 통합 테이블이 빈다. 응답 검증을 통과했더라도
    # 응답의 TBL_ID 가 등록된 통계표 ID 와 다르면 tbl_id 조건에서 전부 걸러져 0행이 될 수 있다.
    # 예외를 올려 트랜잭션 전체(원본 삭제·적재, 통합 삭제)를 되돌린다.
    if inserted.rowcount == 0:
        raise KosisDataValidationError(
            f"[{stat_tbl_id}] {intg_tbl_id} 이관 결과 0행 — 기존 데이터 유지를 위해 중단"
        )
    logging.info(f"{intg_tbl_id}로 신규 데이터 insert 완료: {inserted.rowcount}건")

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

    삭제 범위는 해당 통계(src_data_id + tbl_id)의 메타 전부다(갱신일 조건 없음).
      - 갱신일이 바뀌면 이전 갱신일의 메타 행은 더 쓰이지 않는다. 통계 단위 트랜잭션에서
        함께 지워 두면 배치 끝의 별도 정리 단계가 필요 없다.
      - 갱신일을 읽지 못한 경우(None) "stat_latest_chn_dt = NULL" 조건은 아무 행도 지우지
        못해 실행할 때마다 메타가 한 벌씩 늘어난다.
    파싱을 삭제보다 먼저 한다: 메타 파일을 읽지 못하거나 MetaRow 가 하나도 없을 때
    기존 메타를 지운 채 끝나지 않게 하기 위해서다(행이 없으면 기존 메타를 그대로 둔다).
    """
    from config import get_db_batch_size
    src_data_id = file_info['src_data_id']
    stat_tbl_id = file_info['stat_tbl_id']
    stat_latest_chn_dt = latest_date
    created_by = "SYS-BATCH"

    # 1. meta XML 파싱 및 row 매핑 (설명문 등 무시) — 삭제보다 먼저 수행
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
        logging.warning("삽입할 메타데이터가 없습니다. 기존 메타데이터를 유지합니다.")
        return

    # 2. 기존 데이터 삭제 — 해당 통계의 메타 전부(이전 갱신일 분 포함)
    delete_sql = """
    DELETE FROM stats_kosis_metadata_code
    WHERE src_data_id = :src_data_id
      AND tbl_id = :stat_tbl_id
    """
    session.execute(
        text(delete_sql),
        {
            'src_data_id': src_data_id,
            'stat_tbl_id': stat_tbl_id
        }
    )
    logging.info(f"stats_kosis_metadata_code에서 기존 메타데이터 삭제 완료., {src_data_id}-{stat_tbl_id}")

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
    updated_at, updated_by도 같이 업데이트
    """
    from datetime import date
    src_data_id = file_info['src_data_id']
    stat_tbl_id = file_info['stat_tbl_id']
    stat_latest_chn_dt = latest_date
    stat_data_ref_dt = date.today().strftime('%Y-%m-%d')
    updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    updated_by = 'SYS-BATCH'
    # avail_cat_cols: data_json에서 실제 값이 존재하는 c1~c4만 추출
    # 단일 dict 응답(KOSIS 단건)도 리스트로 정규화하고, dict 아닌 요소는 건너뜀
    rows_iter = data_json if isinstance(data_json, list) else [data_json]
    cat_cols = []
    for c in ['c1', 'c2', 'c3', 'c4']:
        if any(isinstance(row, dict) and (row.get(c.upper()) or row.get(c)) for row in rows_iter):
            cat_cols.append(c)
    avail_cat_cols = pyjson.dumps(cat_cols, ensure_ascii=False)
    update_sql = """
    UPDATE stats_src_data_info
    SET stat_latest_chn_dt = :stat_latest_chn_dt,
        stat_data_ref_dt = :stat_data_ref_dt,
        avail_cat_cols = :avail_cat_cols,
        updated_at = :updated_at,
        updated_by = :updated_by
    WHERE src_data_id = :src_data_id
      AND stat_tbl_id = :stat_tbl_id
    """
    session.execute(
        text(update_sql),
        {
            'stat_latest_chn_dt': stat_latest_chn_dt,
            'stat_data_ref_dt': stat_data_ref_dt,
            'avail_cat_cols': avail_cat_cols,
            'updated_at': updated_at,
            'updated_by': updated_by,
            'src_data_id': src_data_id,
            'stat_tbl_id': stat_tbl_id
        }
    )
    logging.info(f"stats_src_data_info({src_data_id}, {stat_tbl_id}) 업데이트 완료: stat_latest_chn_dt={stat_latest_chn_dt}, stat_data_ref_dt={stat_data_ref_dt}, avail_cat_cols={avail_cat_cols}, updated_at={updated_at}, updated_by={updated_by}")

def _update_sys_data_summary_info(session, file_info, stats_data_info, latest_date):
    """
    sys_data_summary_info 테이블의 src_latest_chn_dt, sys_data_ref_dt 컬럼 업데이트
    updated_at도 같이 업데이트
    """
    from datetime import date
    intg_tbl_id = stats_data_info.get('intg_tbl_id')
    stat_latest_chn_dt = latest_date
    sys_data_ref_dt = date.today().strftime('%Y-%m-%d')
    
    if not intg_tbl_id:
        logging.warning(f"intg_tbl_id가 없어 sys_data_summary_info 업데이트를 건너뜁니다.")
        return
    
    # sys_data_summary_info에서 해당 레코드가 존재하는지 먼저 확인
    check_sql = """
    SELECT COUNT(*) as cnt
    FROM sys_data_summary_info
    WHERE sys_tbl_id = :sys_tbl_id AND del_yn = 'N' AND status = 'A'
    """
    result = session.execute(text(check_sql), {'sys_tbl_id': intg_tbl_id}).fetchone()
    
    if result.cnt == 0:
        error_msg = f"sys_data_summary_info에서 sys_tbl_id='{intg_tbl_id}'에 해당하는 레코드를 찾을 수 없습니다."
        logging.error(error_msg)
        raise ValueError(error_msg)
    
    # 업데이트 실행 (updated_at은 CURRENT_TIMESTAMP 사용)
    update_sql = """
    UPDATE sys_data_summary_info
    SET src_latest_chn_dt = :src_latest_chn_dt,
        sys_data_ref_dt = :sys_data_ref_dt,
        updated_at = CURRENT_TIMESTAMP
    WHERE sys_tbl_id = :sys_tbl_id
      AND del_yn = 'N' 
      AND status = 'A'
    """
    update_result = session.execute(
        text(update_sql),
        {
            'src_latest_chn_dt': stat_latest_chn_dt,
            'sys_data_ref_dt': sys_data_ref_dt,
            'sys_tbl_id': intg_tbl_id
        }
    )
    
    # 업데이트된 행 수 확인
    rows_affected = update_result.rowcount
    if rows_affected == 0:
        error_msg = f"sys_data_summary_info 업데이트 실패: sys_tbl_id='{intg_tbl_id}'에 해당하는 레코드를 업데이트할 수 없습니다."
        logging.error(error_msg)
        raise ValueError(error_msg)
    elif rows_affected > 1:
        error_msg = f"sys_data_summary_info 업데이트 경고: sys_tbl_id='{intg_tbl_id}'에 해당하는 레코드가 {rows_affected}개 업데이트되었습니다. (예상: 1개)"
        logging.warning(error_msg)
    
    logging.info(f"sys_data_summary_info({intg_tbl_id}) 업데이트 완료: src_latest_chn_dt={stat_latest_chn_dt}, sys_data_ref_dt={sys_data_ref_dt}, updated_rows={rows_affected}")

def _update_management_tables(session, file_info, api_info, stats_src, stats_data_info):
    """
    sys_stats_src_api_info 테이블의 최신화(업데이트)
    updated_at, updated_by도 같이 업데이트
    """
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    # sys_stats_src_api_info 업데이트
    update_sql1 = """
    UPDATE sys_stats_src_api_info
    SET latest_sync_time = :latest_sync_time,
        updated_at = :updated_at,
        updated_by = :updated_by
    WHERE ext_api_id = :ext_api_id
      AND stat_api_id = :stat_api_id
      AND stat_tbl_id = :stat_tbl_id
    """
    session.execute(
        text(update_sql1),
        {
            'latest_sync_time': now_str,
            'updated_at': now_str,
            'updated_by': 'SYS-BATCH',
            'ext_api_id': file_info['ext_api_id'],
            'stat_api_id': file_info['stat_api_id'],
            'stat_tbl_id': file_info['stat_tbl_id']
        }
    )
    logging.info(f"sys_stats_src_api_info({file_info['ext_api_id']}, {file_info['stat_api_id']}, {file_info['stat_tbl_id']}) 최신화 완료: latest_sync_time={now_str}, updated_at={now_str}, updated_by=SYS-BATCH")

def _update_sys_ext_api_info(session, ext_api_id):
    """
    sys_ext_api_info 테이블의 최신화(업데이트) - 전체 성공 후 한 번만 호출
    updated_at, updated_by도 같이 업데이트
    """
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    update_sql2 = """
    UPDATE sys_ext_api_info
    SET latest_sync_time = :latest_sync_time,
        updated_at = :updated_at,
        updated_by = :updated_by
    WHERE ext_api_id = :ext_api_id
    """
    session.execute(
        text(update_sql2),
        {
            'latest_sync_time': now_str,
            'updated_at': now_str,
            'updated_by': 'SYS-BATCH',
            'ext_api_id': ext_api_id
        }
    )
    session.commit()
    logging.info(f"sys_ext_api_info({ext_api_id}) 최신화 완료: latest_sync_time={now_str}, updated_at={now_str}, updated_by=SYS-BATCH")
