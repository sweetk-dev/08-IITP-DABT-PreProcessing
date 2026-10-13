import logging
import json
import xml.etree.ElementTree as ET
from sqlalchemy.orm import sessionmaker
from db import engine
from datetime import datetime
from sqlalchemy import text
from config import get_db_batch_size, get_parallel_workers_db
import json as pyjson
import os
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


# ===========================================================================
# 통합 테이블 칸 배치 규칙 (sys_stats_src_api_info.intg_col_map, 01 v1.9.0)
# ---------------------------------------------------------------------------
# 통합 테이블(stats_dis_*)의 칸은 c1·c2·c3·itm_id 4개다. 종전에는 원본의 C1→c1, C2→c2, C3→c3,
# ITM_ID→itm_id 로 고정해 옮겼다. 2026-10 KOSIS 가 등록장애인 통계 등 4종에서 예전 "항목"(성별·
# 사회참여 활동)을 "분류"로 옮기면서 분류가 한 단계씩 늘었다. 통합 테이블과 그 소비처(공개 API·분석
# 대시보드)는 예전 형태를 전제로 하므로, 늘어난 분류를 itm_id 칸으로 되돌려 넣는 규칙을 통계별로 둔다.
#
# 규칙 형식(JSON 객체) — 적지 않은 키는 종전 규칙
#   "c1" | "c2" | "c3" | "itm_id" : 원본 칼럼 "C1"~"C4" | "ITM" | null(빈 값)
#   "item_obj"                     : 메타데이터에서 "항목"으로 다룰 분류 객체 ID(OBJ_ID)
# 예) 전국 연령별·장애정도별·성별 등록장애인수(N007, 분류 3단계: 연령·장애정도·성별)
#     {"c3": null, "itm_id": "C3", "item_obj": "D"}
#     → c1=연령, c2=장애정도, c3=빈 값, itm_id=성별(T001 계·T002 남자·T003 여자). 원래 항목(T001 인원)은 버린다.
# ===========================================================================

# 메타데이터에서 "항목" 객체를 나타내는 값. 공개 API(09)가 obj_nm = '항목' 으로 항목 이름을 찾는다.
ITEM_OBJ_ID = 'ITEM'
ITEM_OBJ_NM = '항목'

_COL_MAP_TARGETS = ('c1', 'c2', 'c3', 'itm_id')
# 규칙에 쓸 수 있는 원본 칼럼 → stats_kosis_origin_data 칼럼명. SQL 은 이 표의 값으로만 조립한다
# (설정값 문자열을 SQL 에 그대로 넣지 않는다).
_COL_MAP_SOURCES = {'C1': 'c1', 'C2': 'c2', 'C3': 'c3', 'C4': 'c4', 'ITM': 'itm_id'}
# 원본 칼럼 → KOSIS 응답 JSON 키(적재 전 구조 검사용).
_SRC_RESPONSE_KEY = {'C1': 'C1', 'C2': 'C2', 'C3': 'C3', 'C4': 'C4', 'ITM': 'ITM_ID'}
_DEFAULT_COL_MAP = {'c1': 'C1', 'c2': 'C2', 'c3': 'C3', 'itm_id': 'ITM'}
_OBJ_ID_RE = re.compile(r'^[A-Za-z0-9_]{1,40}$')
# 통합 테이블 이름(stats_src_data_info.intg_tbl_id)은 SQL 에 식별자로 들어간다. 바인드 변수로 넣을 수 없으므로
# 형식을 검사해 영문 소문자·숫자·밑줄로 된 이름만 허용한다(설정 오류·변조된 값이 SQL 로 섞이는 것을 막는다).
_SAFE_IDENT = re.compile(r'^[a-z][a-z0-9_]{0,62}$')


def _require_safe_table(name, stat_tbl_id=''):
    if not name or not _SAFE_IDENT.match(str(name)):
        raise KosisDataValidationError(f"[{stat_tbl_id}] 통합 테이블 이름 형식 오류: {name!r}")
    return name


def parse_intg_col_map(raw, stat_tbl_id=''):
    """intg_col_map 설정값을 해석·검증해 완성된 규칙을 돌려준다. DB 를 쓰지 않는 순수 함수.

    인자:
        raw: None / '' (종전 규칙) / JSON 문자열 / dict.
        stat_tbl_id: 오류 사유 머리에 넣을 통계표 ID.
    반환:
        {'c1': 'C1', 'c2': 'C2', 'c3': 'C3', 'itm_id': 'ITM', 'item_obj': None, 'custom': False} 형태.
        c2·c3 는 None(빈 값)일 수 있다. custom 은 설정값이 있었는지 여부.
    실패 시 동작:
        KosisDataValidationError — 잘못된 키·값, c1/itm_id 를 비우는 규칙, item_obj 없이 itm_id 를
        분류에서 가져오는 규칙. DB 에 쓰기 전에 그 통계만 실패시킨다.
    """
    head = f"[{stat_tbl_id}] 칸 배치 규칙(intg_col_map) 오류"
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return dict(_DEFAULT_COL_MAP, item_obj=None, custom=False)
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError as e:
            raise KosisDataValidationError(f"{head}: JSON 해석 실패({e})")
    if not isinstance(raw, dict):
        raise KosisDataValidationError(f"{head}: JSON 객체가 아님({type(raw).__name__})")
    unknown = set(raw) - set(_COL_MAP_TARGETS) - {'item_obj'}
    if unknown:
        raise KosisDataValidationError(f"{head}: 알 수 없는 키 {sorted(unknown)}")
    rule = dict(_DEFAULT_COL_MAP)
    for target in _COL_MAP_TARGETS:
        if target not in raw:
            continue
        src = raw[target]
        if src is not None:
            src = str(src).strip().upper()
            if src not in _COL_MAP_SOURCES:
                raise KosisDataValidationError(f"{head}: {target} 의 원본 칼럼 '{raw[target]}' 은 허용되지 않음(C1~C4·ITM·null)")
        rule[target] = src
    for required in ('c1', 'itm_id'):
        if rule[required] is None:
            raise KosisDataValidationError(f"{head}: {required} 는 비울 수 없음(통합 테이블 NOT NULL)")
    item_obj = raw.get('item_obj')
    if item_obj is not None:
        item_obj = str(item_obj).strip()
        if not _OBJ_ID_RE.match(item_obj):
            raise KosisDataValidationError(f"{head}: item_obj '{raw.get('item_obj')}' 형식 오류")
    if rule['itm_id'] != 'ITM' and not item_obj:
        # itm_id 에 분류 코드를 넣으면, 공개 API 가 그 코드의 이름을 메타의 '항목' 객체에서 찾는다.
        # item_obj 로 메타도 함께 옮기지 않으면 이름을 못 찾아 응답이 실패한다.
        raise KosisDataValidationError(f"{head}: itm_id 를 분류({rule['itm_id']})에서 가져오면 item_obj 를 지정해야 함")
    used = [rule[t] for t in _COL_MAP_TARGETS if rule[t]]
    if len(used) != len(set(used)):
        raise KosisDataValidationError(f"{head}: 같은 원본 칼럼을 두 칸에 배치함 {used}")
    rule['item_obj'] = item_obj
    rule['custom'] = True
    return rule


def check_response_structure(data_json, rule, stat_tbl_id=''):
    """칸 배치 규칙이 이번 응답의 구조와 맞는지 적재 전에 확인한다. DB 를 쓰지 않는 순수 함수.

    validate_kosis_data_response 를 통과한 응답(dict 행의 리스트)을 받는다. 확인하는 것:
      1. 값이 들어 있는 분류 단계(C1~C4)가 모두 통합 테이블 어딘가에 배치되는가.
         - 배치되지 않은 단계가 있으면 그 분류가 통째로 버려진다. 이것이 KOSIS 분류 단계가 늘었을 때의
           전형적인 증상이다(예: 성별이 4번째 분류로 추가되면 통합 테이블에 c4 칸이 없어 성별 구분이
           사라지고, 키가 겹쳐 적재가 중복 키 오류로 끝난다). 원인이 드러나는 사유로 먼저 실패시킨다.
      2. c1·itm_id 로 옮길 값이 빈 행이 없는가(통합 테이블 NOT NULL 칸).
      3. itm_id 를 분류에서 가져오는 규칙이면, 원본의 항목(ITM_ID)이 1개뿐인가.
         - 항목이 2개 이상으로 늘면(예: 인원·구성비) 분류를 itm_id 로 옮길 때 항목 구분이 사라진다.
    반환: 없음.
    실패 시 동작: KosisDataValidationError — 사유에 단계 이름(C#_OBJ_NM)·건수를 담는다.
    """
    head = f"[{stat_tbl_id}] 적재 중단(기존 데이터 유지): 구조 변경 감지"
    rows = [r for r in data_json if _NUMERIC_PRD_DE.match(str(r.get('PRD_DE') or ''))]
    used = {rule[t] for t in _COL_MAP_TARGETS if rule.get(t)}
    for src in ('C1', 'C2', 'C3', 'C4'):
        key = _SRC_RESPONSE_KEY[src]
        filled = [r for r in rows if str(r.get(key) or '').strip()]
        if filled and src not in used:
            obj_nm = str(filled[0].get(f'{key}_OBJ_NM') or '').strip() or '이름 없음'
            raise KosisDataValidationError(
                f"{head} — 응답의 {src}({obj_nm}) 분류가 통합 테이블 칸에 배치되지 않음. "
                f"KOSIS 분류 단계가 늘었을 수 있음(조회 주소 objL·칸 배치 규칙 intg_col_map 점검)"
            )
    for target in ('c1', 'itm_id'):
        key = _SRC_RESPONSE_KEY[rule[target]]
        empty = sum(1 for r in rows if not str(r.get(key) or '').strip())
        if empty:
            raise KosisDataValidationError(
                f"{head} — 통합 테이블 {target} 에 넣을 {rule[target]} 값이 빈 행 {empty}건"
            )
    if rule['itm_id'] != 'ITM':
        items = sorted({str(r.get('ITM_ID') or '').strip() for r in rows})
        if len(items) != 1:
            raise KosisDataValidationError(
                f"{head} — 항목이 {len(items)}개({','.join(items[:5])}). 분류를 itm_id 로 옮기는 규칙은 "
                f"항목이 1개일 때만 쓸 수 있음(조회 주소 itmId·칸 배치 규칙 점검)"
            )


def remap_meta_rows(rows, item_obj, stat_tbl_id=''):
    """메타 행 목록에 칸 배치 규칙의 item_obj 를 적용한다. DB 를 쓰지 않는 순수 함수.

    - item_obj 객체의 행 → obj_id='ITEM', obj_nm='항목' 으로 바꾼다(공개 API 가 itm_id 이름을 여기서 찾는다).
    - 원래 항목 객체의 행(obj_id='ITEM' 또는 obj_nm='항목')은 버린다. 남겨 두면 같은 코드(예: T001 인원 /
      T001 계)가 항목에 두 번 들어가 이름이 엇갈린다.
    item_obj 가 없으면 rows 를 그대로 돌려준다.
    실패 시 동작: 메타에 item_obj 객체가 없으면 KosisDataValidationError(규칙과 원천이 어긋남).
    """
    if not item_obj:
        return rows
    target = [r for r in rows if r.get('obj_id') == item_obj]
    if not target:
        objs = sorted({str(r.get('obj_id')) for r in rows})
        raise KosisDataValidationError(
            f"[{stat_tbl_id}] 적재 중단(기존 데이터 유지): 메타에 item_obj '{item_obj}' 객체가 없음(메타 객체 {objs})"
        )
    out = []
    for r in rows:
        if r.get('obj_id') == item_obj:
            r = dict(r, obj_id=ITEM_OBJ_ID, obj_nm=ITEM_OBJ_NM)
        elif r.get('obj_id') == ITEM_OBJ_ID or r.get('obj_nm') == ITEM_OBJ_NM:
            continue
        out.append(r)
    return out


def max_numeric_prd_de(data_json):
    """응답에서 숫자로만 된 PRD_DE 의 최댓값(문자열). 없으면 None."""
    vals = [str(r.get('PRD_DE')) for r in data_json
            if isinstance(r, dict) and _NUMERIC_PRD_DE.match(str(r.get('PRD_DE') or ''))]
    return max(vals, key=lambda v: (len(v), v)) if vals else None


# 재구성 허용 — 운영자가 원천의 분류 체계 변경을 확인하고 "이번 한 번은 바뀐 대로 적재"할 때 쓴다.
# 예) KOSIS_ACCEPT_RESHAPE=DT_11761_N007 python main.py --mode db   (ALL 이면 전 통계)
# 수동 실행 명령줄에만 붙인다. .env 나 cron 에 넣어 두면 가드가 늘 꺼진 상태가 된다.
ACCEPT_RESHAPE_ENV = 'KOSIS_ACCEPT_RESHAPE'
# 같은 연도의 기존 행 가운데 새 응답과 키가 일치해야 하는 최소 비율. 원천이 값을 소급 수정해도 키
# (시점·분류·항목 코드)는 그대로이므로 정상 갱신에서는 거의 100% 일치한다. 절반 아래로 떨어지면
# 분류 체계·코드 변경이거나 칸 배치가 어긋난 것이다.
KEY_MATCH_MIN_RATIO = 0.5


def _reshape_accepted(stat_tbl_id):
    raw = os.getenv(ACCEPT_RESHAPE_ENV, '')
    ids = {x.strip() for x in raw.split(',') if x.strip()}
    return 'ALL' in ids or stat_tbl_id in ids

def process_db_insertion(saved_files_info, api_info, stats_src_list, stats_src_data_info_dict,
                         collect_failed=None, update_system_sync=True):
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
    :param update_system_sync: False 면 전부 성공해도 sys_ext_api_info.latest_sync_time 을 갱신하지 않는다.
        일부 통계만 실행(DATA_COLLECTION_SCOPE=PARTIAL)할 때 쓴다 — 그 시각은 "이 외부 시스템의 전 통계가
        마지막으로 정상 동기화된 시각"인데, 일부만 돌린 실행으로 갱신하면 나머지 통계가 실패 중이어도
        전체가 최신인 것처럼 보인다. 통계별 시각(sys_stats_src_api_info)은 성공한 통계만 그대로 갱신된다.
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

    if not update_system_sync:
        logging.info("일부 통계 실행(PARTIAL) — 시스템 전체 동기화 시각은 갱신하지 않습니다.")
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
    # 0. 칸 배치 규칙 해석 — 설정이 잘못됐으면 DB 에 쓰기 전에 이 통계만 실패시킨다.
    col_rule = parse_intg_col_map(stats_src.get('intg_col_map'), file_info.get('stat_tbl_id'))
    if col_rule['custom']:
        logging.info(f"[{file_info.get('stat_tbl_id')}] 칸 배치 규칙 적용: "
                     f"{ {k: col_rule[k] for k in _COL_MAP_TARGETS} }, item_obj={col_rule['item_obj']}")
    # 1. latest 파일 파싱 (최신 날짜 추출)
    # 갱신일(SendDe)을 얻지 못하면 여기서 실패로 확정한다(_require_latest_date 참고).
    # 이 시점에는 아직 아무 SQL 도 실행하지 않았으므로 기존 데이터가 그대로 남는다.
    latest_path = file_info['latest_path']
    latest_date = _require_latest_date(latest_path, file_info.get('stat_tbl_id'))
    logging.info(f"최신 SendDe 날짜 추출: {latest_date}")

    # 1-1. meta 파일이 오류 본문인지 확인 — DB 에 쓰기 전에 끝낸다.
    # meta 가 text(XML) 형식으로 설정된 통계는 수집 단계가 응답 본문을 그대로 파일에 쓴다
    # (JSON 형식과 달리 오류 본문 판별을 거치지 않는다). 오류 본문이 저장된 채로 진행하면
    # 5단계(메타 적재)에서 XML 해석 예외가 나고, 요약 로그에는 "XML 시작 태그를 찾을 수
    # 없습니다" 같은 해석 오류만 남아 원천이 오류를 돌려줬다는 사실이 드러나지 않는다.
    # 판별할 수 있는 형태({"err": ...} JSON 텍스트)만 여기서 걸러 사유에 오류 코드를 남긴다.
    # 그 밖의 해석 불가 본문은 종전대로 5단계에서 실패하고 트랜잭션이 되돌려진다.
    meta_error = _describe_error_body(_read_text_or_none(file_info.get('meta_path')))
    if meta_error:
        raise KosisDataValidationError(
            f"[{file_info.get('stat_tbl_id')}] 적재 중단(기존 데이터 유지): "
            f"meta 응답이 오류 본문 — {meta_error}"
        )

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
    # 2-2. 구조 검사 — 응답의 분류 단계·항목 수가 칸 배치 규칙과 맞는지(KOSIS 구조 변경 감지).
    check_response_structure(data_json, col_rule, file_info.get('stat_tbl_id'))

    # 3. stats_kosis_origin_data 테이블에 bulk insert
    _insert_origin_data(session, data_json, file_info, stats_src, stats_data_info, latest_date)
    logging.info(f"stats_kosis_origin_data 테이블에 데이터 삽입 완료.")

    # 4. 통계 통합 테이블(intg_tbl_id)로 데이터 이관
    _transfer_to_integration_table(session, file_info, stats_src, stats_data_info, latest_date, col_rule)
    logging.info(f"통계 통합 테이블({stats_data_info.get('intg_tbl_id')})로 데이터 이관 완료.")

    # 5. 메타데이터 테이블(stats_kosis_metadata_code) 적재
    meta_path = file_info['meta_path']
    _insert_metadata(session, meta_path, file_info, stats_src, stats_data_info, latest_date,
                     item_obj=col_rule['item_obj'])
    logging.info(f"stats_kosis_metadata_code 테이블에 메타데이터 적재 완료.")

    # 5-1. 메타 무결성 — 통합 테이블의 코드가 같은 갱신일 메타에 모두 있는지. 공개 API 는 이름을 못 찾는
    # 코드가 하나라도 있으면 그 통계 요청 전체를 오류로 끝낸다. 어긋나면 이 트랜잭션 전체를 되돌린다.
    _check_meta_integrity(session, file_info, stats_data_info, latest_date)

    # 6. stats_src_data_info 테이블 업데이트
    _update_stats_src_data_info(session, file_info, data_json, latest_date, col_rule)

    # 7. sys_data_summary_info 테이블 업데이트
    _update_sys_data_summary_info(session, file_info, stats_data_info, latest_date)

    # 8. 관리 테이블(sys_stats_src_api_info, sys_ext_api_info) 최신화
    _update_management_tables(session, file_info, api_info, stats_src, stats_data_info)

# 오류 본문을 사유 문자열에 옮길 때의 최대 길이. 사유는 요약 로그 한 줄에 들어가므로
# (main._short_reason 이 전체 사유를 다시 120자로 자른다) 본문 일부만 싣는다.
_ERROR_BODY_SNIPPET_LEN = 80


def _read_text_or_none(path):
    """파일 내용을 문자열로 읽어 돌려준다. 경로가 없거나 읽지 못하면 None.

    오류 본문 판별(_describe_error_body)의 보조 함수다. 판별은 사유를 구체적으로 남기기
    위한 것이므로, 읽지 못했다고 여기서 예외를 올리지 않는다(읽기 실패 자체는 이후의
    본 파싱 단계가 예외로 드러낸다).
    """
    if not path:
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except (OSError, UnicodeDecodeError):
        return None


def _describe_error_body(body):
    """text 형식으로 저장된 meta/latest 응답 본문이 오류 본문이면 사유 문자열을 돌려준다.

    판별 범위(확인된 형식만 다룬다):
      1. JSON 텍스트 ``{"err": "20", "errMsg": "..."}`` — KOSIS 가 HTTP 200 으로 돌려주는
         오류 본문. 응답 형식을 XML 로 요청해도 오류는 이 형태로 올 수 있고, 수집 단계의
         오류 판별은 JSON 형식 응답에만 적용되므로 text 형식에서는 파일에 그대로 저장된다.
      2. XML 본문 안의 ``err`` / ``errMsg`` 요소 — 같은 필드 이름을 XML 로 옮긴 형태.
         KOSIS 의 XML 오류 본문 규격은 확인된 바가 없어 이 이름이 있을 때만 값을 싣는다.
         이 요소가 없으면 None 을 돌려주며, 그 경우의 실패 판정은 호출자의 기준
         (latest: SendDe 요소 유무)에 맡긴다.

    인자:
        body: 파일 본문 문자열. None/빈 문자열이면 None 을 돌려준다.
    반환:
        오류 본문이면 ``KOSIS 오류 응답(err=..., errMsg=...)`` 형태의 사유, 아니면 None.
        본문에는 요청 URL·인증키가 없으므로 사유에도 포함되지 않는다.
    실패 시 동작:
        예외를 올리지 않는다. 해석할 수 없는 본문은 "오류 본문으로 판별되지 않음"(None)이다.
    """
    if not body or not body.strip():
        return None
    stripped = body.strip()
    if stripped.startswith('{'):
        try:
            parsed = json.loads(stripped)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict) and 'err' in parsed:
            return f"KOSIS 오류 응답(err={parsed.get('err')}, errMsg={parsed.get('errMsg')})"
        return None
    if stripped.startswith('<'):
        try:
            root = ET.fromstring(stripped)
        except ET.ParseError:
            return None
        # 루트 자신이 err 인 경우와 하위 요소인 경우를 모두 본다.
        err = root.text if root.tag == 'err' else root.findtext('.//err')
        err_msg = root.findtext('.//errMsg')
        if err or err_msg:
            return f"KOSIS 오류 응답(err={(err or '').strip()}, errMsg={(err_msg or '').strip()})"
    return None


def _require_latest_date(latest_path, stat_tbl_id):
    """latest 파일에서 갱신일(SendDe)을 읽어 돌려준다. 얻지 못하면 적재 전 실패로 확정한다.

    갱신일이 None 인 채로 적재를 진행하면 원본·통합 테이블 교체까지 실행된 뒤
    stats_src_data_info.stat_latest_chn_dt(NOT NULL) 갱신에서 제약 위반으로 롤백된다.
    데이터는 보존되지만 요약 로그의 사유가 DB 제약 위반 문구로 남아 "원천에서 갱신일을
    받지 못했다"는 실제 원인을 알 수 없고, 되돌릴 삭제·적재를 한 번 실행하게 된다.
    DB 에 쓰기 전에 같은 결과(기존 데이터 유지 + 실패 집계)를 명시적 사유로 낸다.

    실패로 보는 경우:
      - SendDe 값이 하나도 없다(빈 목록, SendDe 없는 행, XML 에 SendDe 요소 없음).
      - latest 파일을 JSON/XML 로 해석할 수 없다(오류 안내문 등 다른 본문이 저장된 경우).
    KOSIS 의 XML 오류 본문 규격은 확인된 바가 없으므로 "SendDe 요소를 찾지 못함"을 기준으로
    삼고, 본문이 판별 가능한 오류 형태면(_describe_error_body) 그 코드·메시지를 사유에 덧붙인다.

    인자:
        latest_path: 수집 단계가 저장한 latest 파일 경로(.json 또는 그 외=XML).
        stat_tbl_id: 사유 문자열 머리에 넣을 통계표 ID.
    반환:
        갱신일 문자열(YYYY-MM-DD). None 을 돌려주지 않는다.
    실패 시 동작:
        KosisDataValidationError — 호출자(worker)가 롤백 후 이 통계를 실패로 집계한다.
        파일 자체가 없는 경우(OSError)는 판정 대상이 아니므로 그대로 전파한다.
    """
    detail = None
    try:
        latest_date = _parse_latest_file_for_latest_date(latest_path)
    except (ValueError, ET.ParseError) as e:
        # ValueError: json.JSONDecodeError 포함. ET.ParseError: XML 이 아닌 본문.
        latest_date = None
        detail = f"latest 응답을 해석할 수 없음({type(e).__name__})"
    if latest_date:
        return latest_date

    body = _read_text_or_none(latest_path)
    error_body = _describe_error_body(body)
    if error_body:
        detail = error_body
    elif detail is None:
        detail = "latest 응답에 SendDe 없음"
        snippet = ' '.join((body or '').split())[:_ERROR_BODY_SNIPPET_LEN]
        if snippet:
            detail += f", 본문: {snippet}"
    raise KosisDataValidationError(
        f"[{stat_tbl_id}] 적재 중단(기존 데이터 유지): 갱신일을 확인하지 못함 — {detail}"
    )


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

def _intg_select_exprs(col_rule):
    """칸 배치 규칙 → 이관 SELECT 의 (c1, c2, c3, itm_id) 식. 원본 칼럼명은 화이트리스트에서만 고른다."""
    out = {}
    for target in _COL_MAP_TARGETS:
        src = col_rule.get(target)
        out[target] = _COL_MAP_SOURCES[src] if src else "''"
    return out


def _check_reshape(session, intg_tbl_id, file_info, exprs):
    """이번 응답이 기존 통합 행과 같은 모양인지 확인한다(이관 직전, 기존 행 삭제 전).

    - 기존에 있던 연도가 이번 응답에 없으면 실패: 원천 제공 기간이 줄었거나 응답 일부가 빠진 경우다.
      그대로 적재하면 그 연도가 통합 테이블에서 사라진다(통계별 전량 교체 방식이므로).
    - 같은 연도의 기존 행 가운데 키(시점·분류·항목)가 이번 응답과 일치하는 비율이 KEY_MATCH_MIN_RATIO
      미만이면 실패: 분류 체계·코드가 바뀌었거나 칸 배치 규칙이 어긋난 경우다. 오류 없이 다른 모양으로
      덮어쓰는 것(예: 성별이 itm_id 대신 c3 로 들어가는 것)을 막는다.
    기존 행이 없으면(최초 적재) 확인하지 않는다. KOSIS_ACCEPT_RESHAPE 로 통계별 1회 해제할 수 있다.
    """
    stat_tbl_id = file_info['stat_tbl_id']
    sql = f"""
    WITH old AS (
        SELECT prd_de, c1, COALESCE(c2, '') AS c2, COALESCE(c3, '') AS c3, itm_id
          FROM {intg_tbl_id} WHERE src_data_id = :src_data_id
    ), new AS (
        SELECT DISTINCT CAST(prd_de AS INTEGER) AS prd_de,
               {exprs['c1']} AS c1, COALESCE({exprs['c2']}, '') AS c2,
               COALESCE({exprs['c3']}, '') AS c3, {exprs['itm_id']} AS itm_id
          FROM stats_kosis_origin_data
         WHERE src_data_id = :src_data_id AND tbl_id = :stat_tbl_id AND prd_de ~ '^[0-9]+$'
    )
    SELECT (SELECT count(*) FROM old) AS old_n,
           (SELECT count(*) FROM old WHERE prd_de IN (SELECT prd_de FROM new)) AS old_common,
           (SELECT count(*) FROM old o WHERE EXISTS (
                SELECT 1 FROM new n WHERE n.prd_de = o.prd_de AND n.c1 = o.c1
                   AND n.c2 = o.c2 AND n.c3 = o.c3 AND n.itm_id = o.itm_id)) AS matched,
           (SELECT string_agg(DISTINCT prd_de::text, ',') FROM old
             WHERE prd_de NOT IN (SELECT prd_de FROM new)) AS missing_years
    """
    r = session.execute(text(sql), {'src_data_id': file_info['src_data_id'],
                                    'stat_tbl_id': stat_tbl_id}).mappings().one()
    if not r['old_n']:
        return
    problems = []
    if r['missing_years']:
        years = sorted(r['missing_years'].split(','))
        problems.append(f"기존 연도 {','.join(years)} 가 이번 응답에 없음(원천 제공 기간 축소·응답 누락 의심)")
    if r['old_common']:
        ratio = r['matched'] / r['old_common']
        logging.info(f"[{stat_tbl_id}] 기존 키 일치율 {ratio:.1%} ({r['matched']}/{r['old_common']})")
        if ratio < KEY_MATCH_MIN_RATIO:
            problems.append(
                f"같은 연도 기존 행의 {ratio:.0%}만 이번 응답과 키가 일치(분류 체계·코드 변경 또는 칸 배치 규칙 불일치 의심)")
    if not problems:
        return
    if _reshape_accepted(stat_tbl_id):
        logging.warning(f"[{stat_tbl_id}] {ACCEPT_RESHAPE_ENV} 로 허용 — {'; '.join(problems)}")
        return
    raise KosisDataValidationError(
        f"[{stat_tbl_id}] 적재 중단(기존 데이터 유지): {'; '.join(problems)}. "
        f"원천 변경을 확인했다면 {ACCEPT_RESHAPE_ENV}={stat_tbl_id} 로 수동 실행"
    )


def _check_meta_integrity(session, file_info, stats_data_info, latest_date):
    """통합 테이블의 이 통계 코드가 같은 갱신일 메타에 모두 있는지 확인한다(공개 API 조회 규칙과 같은 기준).

    공개 API(09)는 통합 행의 c1~c3 이름을 메타의 분류 객체(obj_nm <> '항목')에서, itm_id 이름을 항목 객체
    (obj_nm = '항목')에서 찾고, 하나라도 못 찾으면 그 요청 전체를 오류로 끝낸다. 갱신일은
    stats_src_data_info.stat_latest_chn_dt(= 이번 latest_date)와 같은 메타만 본다.
    실패 시 동작: KosisDataValidationError(못 찾은 코드 최대 10개를 사유에 담는다) → 트랜잭션 롤백.
    """
    intg_tbl_id = stats_data_info.get('intg_tbl_id')
    if not intg_tbl_id:
        return
    _require_safe_table(intg_tbl_id, file_info['stat_tbl_id'])
    params = {'src_data_id': file_info['src_data_id'], 'latest_date': latest_date,
              'item_nm': ITEM_OBJ_NM}
    sql = f"""
    WITH meta AS (
        SELECT itm_id, obj_nm FROM stats_kosis_metadata_code
         WHERE src_data_id = :src_data_id AND stat_latest_chn_dt = :latest_date
    ), cat AS (
        SELECT DISTINCT v FROM (
            SELECT c1 AS v FROM {intg_tbl_id} WHERE src_data_id = :src_data_id
            UNION SELECT c2 FROM {intg_tbl_id} WHERE src_data_id = :src_data_id
            UNION SELECT c3 FROM {intg_tbl_id} WHERE src_data_id = :src_data_id) a
         WHERE COALESCE(v, '') <> ''
    ), itm AS (
        SELECT DISTINCT itm_id AS v FROM {intg_tbl_id} WHERE src_data_id = :src_data_id
    ), miss_cat AS (   -- NOT IN 은 메타에 NULL 이 하나라도 있으면 아무것도 걸러내지 못한다 → NOT EXISTS
        SELECT v FROM cat c WHERE NOT EXISTS
            (SELECT 1 FROM meta m WHERE m.itm_id = c.v AND m.obj_nm <> :item_nm)
    ), miss_itm AS (
        SELECT v FROM itm i WHERE NOT EXISTS
            (SELECT 1 FROM meta m WHERE m.itm_id = i.v AND m.obj_nm = :item_nm)
    )
    SELECT (SELECT string_agg(v, ',') FROM (SELECT v FROM miss_cat ORDER BY v LIMIT 10) x) AS miss_cat,
           (SELECT count(*) FROM miss_cat) AS miss_cat_n,
           (SELECT string_agg(v, ',') FROM (SELECT v FROM miss_itm ORDER BY v LIMIT 10) y) AS miss_itm,
           (SELECT count(*) FROM miss_itm) AS miss_itm_n
    """
    r = session.execute(text(sql), params).mappings().one()
    if r['miss_cat_n'] or r['miss_itm_n']:
        parts = []
        if r['miss_cat_n']:
            parts.append(f"분류 코드 {r['miss_cat_n']}건({r['miss_cat']})")
        if r['miss_itm_n']:
            parts.append(f"항목 코드 {r['miss_itm_n']}건({r['miss_itm']})")
        raise KosisDataValidationError(
            f"[{file_info['stat_tbl_id']}] 적재 중단(기존 데이터 유지): 메타에 이름이 없는 "
            f"{' · '.join(parts)} — 그대로 두면 공개 API 조회가 실패함(메타 응답·칸 배치 규칙 점검)"
        )


def _transfer_to_integration_table(session, file_info, stats_src, stats_data_info, latest_date, col_rule=None):
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

    칸 배치 규칙(col_rule)
      - parse_intg_col_map 이 돌려준 규칙. None 이면 종전 규칙(C1→c1, C2→c2, C3→c3, ITM→itm_id).
      - 이관 SELECT 의 c1·c2·c3·itm_id 식은 _intg_select_exprs 가 화이트리스트로만 조립한다.
      - 기존 행을 지우기 전에 _check_reshape 로 "같은 모양으로 갱신되는지"를 확인한다.

    실패 시 동작: 예외 전파 → 호출자(worker)가 rollback 후 실패 집계.
    """
    if col_rule is None:
        col_rule = parse_intg_col_map(None)
    exprs = _intg_select_exprs(col_rule)
    intg_tbl_id = stats_data_info.get('intg_tbl_id')
    src_data_id = file_info['src_data_id']
    stat_tbl_id = file_info['stat_tbl_id']
    stat_latest_chn_dt = latest_date
    if not intg_tbl_id:
        logging.warning(f"intg_tbl_id가 없어 통합 테이블 이관을 건너뜁니다. stat_tbl_id={stat_tbl_id}")
        return
    _require_safe_table(intg_tbl_id, stat_tbl_id)

    # 0. 모양 확인 — 기존 연도 누락·키 불일치면 지우기 전에 중단한다.
    _check_reshape(session, intg_tbl_id, file_info, exprs)

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
            {exprs['c1']}, {exprs['c2']}, {exprs['c3']},
            {exprs['itm_id']},
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
            {exprs['c1']}, {exprs['c2']}, {exprs['c3']},
            {exprs['itm_id']},
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

def _insert_metadata(session, meta_path, file_info, stats_src, stats_data_info, latest_date, item_obj=None):
    """
    meta XML 파일을 파싱하여 stats_kosis_metadata_code 테이블에 데이터 삭제 및 bulk insert

    삭제 범위는 해당 통계(src_data_id + tbl_id)의 메타 전부다(갱신일 조건 없음).
      - 갱신일이 바뀌면 이전 갱신일의 메타 행은 더 쓰이지 않는다. 통계 단위 트랜잭션에서
        함께 지워 두면 배치 끝의 별도 정리 단계가 필요 없다.
      - 갱신일을 읽지 못한 경우(None) "stat_latest_chn_dt = NULL" 조건은 아무 행도 지우지
        못해 실행할 때마다 메타가 한 벌씩 늘어난다.
    파싱을 삭제보다 먼저 한다: 메타 파일을 읽지 못하거나 MetaRow 가 하나도 없을 때
    기존 메타를 지운 채 끝나지 않게 하기 위해서다(행이 없으면 기존 메타를 그대로 둔다).

    item_obj: 칸 배치 규칙의 item_obj. 지정되면 remap_meta_rows 로 그 분류 객체를 '항목'으로 바꾸고
    원래 항목 행은 버린다(통합 테이블 itm_id 에 그 분류 코드가 들어가므로).
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
    rows = remap_meta_rows(rows, item_obj, stat_tbl_id)

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

def _update_stats_src_data_info(session, file_info, data_json, latest_date, col_rule=None):
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
    if col_rule and col_rule.get('custom'):
        # 칸 배치 규칙이 있으면 통합 테이블에 실제로 값이 들어간 분류 칸을 적는다(원본 응답의 C# 이 아니라).
        # 예) N007: 응답은 C1~C3 이지만 C3(성별)은 itm_id 로 옮겨지므로 ["c1","c2"].
        for c in ['c1', 'c2', 'c3']:
            src = col_rule.get(c)
            key = _SRC_RESPONSE_KEY[src] if src else None
            if key and any(isinstance(row, dict) and str(row.get(key) or '').strip() for row in rows_iter):
                cat_cols.append(c)
    else:
        for c in ['c1', 'c2', 'c3', 'c4']:
            if any(isinstance(row, dict) and (row.get(c.upper()) or row.get(c)) for row in rows_iter):
                cat_cols.append(c)
    avail_cat_cols = pyjson.dumps(cat_cols, ensure_ascii=False)
    # 수집 종료연도(collect_end_dt) — 이번에 실제로 받은 마지막 시점으로 앞당긴다(줄이지는 않는다).
    # 수집 요청은 연도 주기 통계에서 "올해"까지 넓혀 보내므로(kosis_api.resolve_collect_years), 새 연도가
    # 공표되어 들어왔으면 여기서 등록값도 따라간다. 공개 API 의 수집기간 표기가 이 값을 쓴다.
    # 연도 표기(4자리)일 때만 갱신한다 — 월·분기 시점(YYYYMM 등)은 등록값 형식과 달라 건드리지 않는다.
    max_prd = max_numeric_prd_de(rows_iter)
    new_end = max_prd if (max_prd and len(max_prd) == 4) else None
    update_sql = """
    UPDATE stats_src_data_info
    SET stat_latest_chn_dt = :stat_latest_chn_dt,
        stat_data_ref_dt = :stat_data_ref_dt,
        avail_cat_cols = :avail_cat_cols,
        collect_end_dt = CASE
            WHEN CAST(:new_end AS varchar) IS NOT NULL
             AND collect_end_dt ~ '^[0-9]{4}$'
             AND CAST(:new_end AS varchar) > collect_end_dt THEN CAST(:new_end AS varchar)
            ELSE collect_end_dt END,
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
            'new_end': new_end,
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
