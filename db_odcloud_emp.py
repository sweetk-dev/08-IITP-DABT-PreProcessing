"""장애인 고용 파일데이터(ODCLOUD_EMP) 적재 모듈 — v1.19.0 · 판 보존 v1.20.0.

대상 표: emp_dis_* 14종(01-IITP-DABT-Database emp init). 행 변환은 collectors/odcloud_emp.

판(edition) 추적 — 새 표를 만들지 않는다
    sys_ext_api_info       ODCLOUD_EMP 1행 (ext_url = https://api.odcloud.kr/api, 인증키 = 공공데이터포털 키)
    sys_stats_src_api_info 표마다 1행 (stat_tbl_id = 표 이름)
        api_meta_url     명세 주소(https://infuser.odcloud.kr/oas/docs?namespace={ID}/v1)
        api_data_url     이 수집기가 마지막으로 적재한 판의 경로('/{ID}/v1/uddi:...'). '' = 아직 없음
        latest_sync_time 마지막 적재 시각
    sys_data_summary_info  sys_tbl_id = 표 이름인 행
        src_latest_chn_dt 적재한 판의 기준일 'YYYY-MM-DD' (2025-07 첫 적재 때부터 쓰던 표기 그대로)
        sys_data_ref_dt   적재한 날 'YYYY-MM-DD' (KOSIS 적재기와 같은 규칙)
        updated_at        적재 시각
    KOSIS 수집기는 sys_stats_src_api_info 를 KOSIS 의 ext_api_id 로만 고르므로(db.get_stats_src_api_info)
    여기 등록하는 행을 읽지 않는다.

행마다 판 기준일 — src_base_dt (01 v1.12.0)
    모든 행에 그 행을 넣은 판의 기준일(판 이름 끝 _YYYYMMDD)을 적는다. 지난 판을 지우지 않고 남겨 두기
    위해서다(v1.19.0 은 연도 칸이 없는 표를 새 판으로 통째로 바꿔 지난 판이 사라졌다).

데이터셋 1종 = 트랜잭션 1개
    잠금(pg_advisory_xact_lock — 같은 표를 두 실행이 동시에 바꾸지 않게) → 표 칸 정보 조회 → 행 검사
    (src_base_dt 칸 존재·NOT NULL·글자 수·숫자 범위) → 급감 방어 → 교체 범위 삭제 → 새 행 입력 → 추적 행 갱신.
    중간에 하나라도 실패하면 그 데이터셋 전체가 롤백되어 표·추적 행이 모두 그대로 남는다.

적재 범위
    MODE_EDITION  연도 칸이 없는 9개 표 — 같은 판 기준일(src_base_dt)의 행만 지우고 판 전체를 넣는다.
                  다른 판의 행은 그대로 남는다(판 이력 보존). 같은 판을 다시 넣으면(FORCE) 그 판만 바뀐다.
                  기준일이 NULL 인 행(판을 모르는 옛 행)은 지우지 않는다.
    MODE_YEAR     연도 칸이 있는 5개 표 — 판에 들어 있는 연도의 행만 지우고 넣는다. 판에 없는 연도는 그대로
                  남는다. 넣는 행에는 그 판의 기준일을 적는다(연도마다 어느 판에서 왔는지 남는다).

급감 방어 — 새 행 수가 비교 대상의 50% 미만이면 원천 장애(부분 파일)로 보고 그 데이터셋을 멈춘다.
    MODE_EDITION  비교 대상 = 같은 판을 다시 넣는 경우 그 판의 기존 행 수, 아니면 직전 판(이 판보다 이른
                  가장 늦은 기준일)의 행 수, 판이 하나도 없으면 기준일 NULL 행 수.
    MODE_YEAR     비교 대상 = 판에 있는 연도의 기존 행 수(합계와 연도마다).
    실제로 줄어든 것이 맞으면 ODCLOUD_EMP_ACCEPT_DROP=<표이름>[,<표이름>] 또는 ALL 을 붙여 수동 실행한다.
    (비교 대상이 0 이면 검사하지 않는다.)

옛 판 보충(ODCLOUD_EMP_EDITION 으로 기록된 기준일보다 이른 판을 지정) — 추적 행은 바꾸지 않는다
    MODE_EDITION  그 판의 행만 지우고 넣는다(다른 판, 특히 최신 판은 그대로).
    MODE_YEAR     표에 행이 없는 연도만 넣는다(이미 있는 연도를 옛 값으로 덮지 않는다).

created_by / updated_by 는 공통코드 sys_work_type 의 정본 'SYS-BACH' 를 쓴다(db_mobility 와 같음).
"""
from __future__ import annotations

import datetime
import logging
import os
from decimal import Decimal
from typing import Dict, List

from sqlalchemy import bindparam, column, insert, table, text

from db import engine

logger = logging.getLogger('db')

CREATED_BY = 'SYS-BACH'

# 급감 방어 — 새 행 수 / 바꿔치울 기존 행 수 가 이 값 미만이면 멈춘다.
DROP_MIN_RATIO = 0.5

# 정수형 칸의 범위(PostgreSQL)
_INT_RANGE = {
    'smallint': (-32768, 32767),
    'integer': (-2147483648, 2147483647),
    'bigint': (-9223372036854775808, 9223372036854775807),
}


def env_table_set(name: str) -> set:
    """'a,b' / 'ALL' 형식 환경변수 → 집합. 공백은 무시한다."""
    return {t.strip() for t in os.getenv(name, '').split(',') if t.strip()}


def _engine():
    if engine is None:
        raise RuntimeError('DB engine not configured (DB_URL)')
    return engine


# ---------------------------------------------------------------------------
# 추적 행 읽기
# ---------------------------------------------------------------------------
def load_tracking(ext_api_id: int) -> Dict[str, dict]:
    """ODCLOUD_EMP 의 표별 추적 정보 {표 이름: {...}}. 삭제·비활성 행은 제외한다.

    sys_data_summary_info 의 현재 기준일(src_latest_chn_dt)도 함께 돌려준다 — 수동으로 옛 판을
    보충 적재할 때 추적 값을 되돌리지 않기 위해서다.
    """
    with _engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT s.stat_api_id, s.stat_tbl_id, s.stat_title, s.api_meta_url, s.api_data_url,
                   s.latest_sync_time,
                   (SELECT d.src_latest_chn_dt FROM sys_data_summary_info d
                     WHERE d.sys_tbl_id = s.stat_tbl_id AND d.del_yn = 'N' AND d.status = 'A'
                     ORDER BY d.data_id LIMIT 1) AS src_latest_chn_dt
              FROM sys_stats_src_api_info s
             WHERE s.ext_api_id = :e AND s.del_yn = 'N' AND s.status = 'A'
             ORDER BY s.stat_api_id"""), {'e': ext_api_id}).mappings().all()
    return {r['stat_tbl_id']: dict(r) for r in rows}


# ---------------------------------------------------------------------------
# 표 칸 정보 + 행 검사 (검사 함수는 DB 없이 단위 테스트한다)
# ---------------------------------------------------------------------------
def table_columns(conn, table_name: str) -> Dict[str, dict]:
    """information_schema 에서 표 칸 정보 {칸: {type, max_len, nullable, precision, scale}}."""
    rows = conn.execute(text("""
        SELECT column_name, data_type, character_maximum_length, is_nullable,
               numeric_precision, numeric_scale
          FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = :t"""), {'t': table_name}).all()
    if not rows:
        raise RuntimeError('%s 표가 없음' % table_name)
    return {r[0]: {'type': r[1], 'max_len': r[2], 'nullable': r[3] == 'YES',
                   'precision': r[4], 'scale': r[5]} for r in rows}


def validate_rows(table_name: str, rows: List[dict], colinfo: Dict[str, dict], max_samples: int = 3) -> None:
    """적재 전에 행을 표 정의에 비춰 검사한다. 문제가 있으면 ValueError(무엇이 몇 건인지 + 예시).

    DB 오류로 실패시키지 않고 미리 보는 이유: DB 오류는 '첫 1건' 만 알려 주고, 메시지에 값이 그대로
    실려 원인 파악이 어렵다. 여기서는 칸마다 위반 건수와 예시를 한 번에 보여 준다.
      · 표에 없는 칸
      · NOT NULL 칸의 빈 값
      · 글자 수 초과(varchar(n) — PostgreSQL 은 글자 수로 센다)
      · 정수 범위 초과, numeric(p,s) 의 정수부 자리 초과
    """
    if not rows:
        return
    problems = []
    missing = [c for c in rows[0].keys() if c not in colinfo]
    if 'src_base_dt' in missing:
        # 판 기준일 칸이 없으면 지난 판을 구분해 남길 수 없다 — 적재하지 않는다.
        raise ValueError('%s 에 src_base_dt(원천 판 기준일) 칸이 없음 — 01 v1.12.0(emp_src_base_dt_v1120.sql) 적용 필요'
                         % table_name)
    if missing:
        raise ValueError('%s 에 없는 칸: %s — 01 마이그레이션 적용 여부 확인' % (table_name, ', '.join(missing)))
    for col in rows[0].keys():
        info = colinfo[col]
        nulls = over = rng = 0
        samples: List[str] = []
        for r in rows:
            v = r.get(col)
            if v is None:
                if not info['nullable']:
                    nulls += 1
                continue
            if info['max_len'] and isinstance(v, str) and len(v) > info['max_len']:
                over += 1
                if len(samples) < max_samples:
                    samples.append('%d자 %r' % (len(v), v[:40]))
            if info['type'] in _INT_RANGE and isinstance(v, int):
                lo, hi = _INT_RANGE[info['type']]
                if not lo <= v <= hi:
                    rng += 1
                    if len(samples) < max_samples:
                        samples.append(repr(v))
            if info['type'] == 'numeric' and info['precision'] and isinstance(v, (int, Decimal)):
                int_digits = info['precision'] - (info['scale'] or 0)
                if abs(Decimal(v)) >= Decimal(10) ** int_digits:
                    rng += 1
                    if len(samples) < max_samples:
                        samples.append(str(v))
        if nulls:
            problems.append('%s NOT NULL 칸 빈 값 %d건' % (col, nulls))
        if over:
            problems.append('%s 글자 수 %d 초과 %d건(예: %s)' % (col, info['max_len'], over, '; '.join(samples)))
        if rng:
            problems.append('%s 숫자 범위(%s) 초과 %d건(예: %s)' % (col, info['type'], rng, '; '.join(samples)))
    if problems:
        raise ValueError('%s 표 정의와 맞지 않음: %s' % (table_name, ' / '.join(problems)))


def check_drop(table_name: str, new_count: int, replaced_count: int, accept: set, scope: str = '') -> None:
    """급감 방어. 새 행이 바꿔치울 행의 DROP_MIN_RATIO 미만이면 ValueError.

    scope: 메시지에 붙일 범위 설명(연도별 검사의 '2024년' 등). 비우면 표 전체.
    """
    if replaced_count <= 0:
        return
    if new_count >= replaced_count * DROP_MIN_RATIO:
        return
    where = '%s(%s)' % (table_name, scope) if scope else table_name
    if 'ALL' in accept or table_name in accept:
        logger.warning('%s: 행 수 급감(%d → %d)이지만 ODCLOUD_EMP_ACCEPT_DROP 로 허용', where,
                       replaced_count, new_count)
        return
    raise ValueError('%s: 새 판 %d행이 비교 대상 기존 %d행의 %d%% 미만 — 원천 장애로 보고 적재 중단. '
                     '실제로 줄어든 것이 맞으면 ODCLOUD_EMP_ACCEPT_DROP=%s 로 수동 실행'
                     % (where, new_count, replaced_count, int(DROP_MIN_RATIO * 100), table_name))


# ---------------------------------------------------------------------------
# 적재
# ---------------------------------------------------------------------------
def _existing_by_year(conn, phys: str, years: List[int]) -> Dict[int, int]:
    """판에 있는 연도별 기존 행 수 {연도: 행 수}(행이 없는 연도는 빠진다)."""
    rows = conn.execute(text('SELECT year, count(*) FROM ' + phys + ' WHERE year IN :ys GROUP BY year')
                        .bindparams(bindparam('ys', expanding=True)), {'ys': years}).all()
    return {int(y): int(n) for y, n in rows}


def check_drop_plan(table_name: str, mode: str, rows: List[dict], old_total: int,
                    old_by_year: Dict[int, int], accept: set) -> None:
    """급감 방어 판정(DB 없이 계산 — 드라이런·단위 테스트에서도 쓴다). 걸리면 ValueError.

    MODE_YEAR 는 합계뿐 아니라 연도마다 따로 본다 — 누적 판에서 한 해가 통째로 빠지거나 크게 줄어도
    다른 해의 행 수 때문에 합계 비율이 50% 를 넘으면 합계 검사만으로는 잡히지 않는다.
    old_total  : 비교 대상 행 수(MODE_YEAR 는 판에 있는 연도의 합, MODE_EDITION 은 edition_baseline).
    old_by_year: MODE_YEAR 의 연도별 기존 행 수.
    """
    if mode == 'year':
        new: Dict[int, int] = {}
        for r in rows:
            new[r['year']] = new.get(r['year'], 0) + 1
        for y in sorted(new):
            check_drop(table_name, new[y], old_by_year.get(y, 0), accept, scope='%d년' % y)
    check_drop(table_name, len(rows), old_total, accept)


def edition_baseline(conn, phys: str, base_date: datetime.date) -> tuple:
    """MODE_EDITION 의 급감 방어 비교 대상. 반환 (비교 행 수, 비교 판 기준일 또는 None, 같은 판 기존 행 수).

    1) 같은 기준일의 행이 이미 있으면(같은 판 다시 적재) 그 행 수
    2) 아니면 이 판보다 이른 판 중 가장 늦은 판(직전 판)의 행 수
    3) 판이 하나도 없으면 기준일 NULL 행(판을 모르는 옛 행) 수 — 비교 판 기준일은 None
    """
    same = conn.execute(text('SELECT count(*) FROM ' + phys + ' WHERE src_base_dt = :d'),
                        {'d': base_date}).scalar_one()
    if same:
        return same, base_date, same
    prev = conn.execute(text('SELECT src_base_dt, count(*) FROM ' + phys +
                             ' WHERE src_base_dt < :d GROUP BY 1 ORDER BY 1 DESC LIMIT 1'),
                        {'d': base_date}).first()
    if prev:
        return int(prev[1]), prev[0], 0
    nulls = conn.execute(text('SELECT count(*) FROM ' + phys + ' WHERE src_base_dt IS NULL')).scalar_one()
    return nulls, None, 0


def _guard(conn, table_name: str, mode: str, rows: List[dict], years: List[int], accept: set,
           base_date: datetime.date) -> dict:
    """지울 행 수와 급감 방어 비교 대상을 세고 check_drop_plan 으로 판정한다(걸리면 ValueError).

    반환: {'replace': 이번에 지울 기존 행 수, 'baseline': 비교 행 수, 'baseline_dt': 비교 판 기준일}
    """
    phys = table_name
    if mode == 'year':
        old = _existing_by_year(conn, phys, years)
        info = {'replace': sum(old.values()), 'baseline': sum(old.values()), 'baseline_dt': None}
        check_drop_plan(table_name, mode, rows, info['baseline'], old, accept)
        return info
    baseline, baseline_dt, same = edition_baseline(conn, phys, base_date)
    info = {'replace': same, 'baseline': baseline, 'baseline_dt': baseline_dt}
    check_drop_plan(table_name, mode, rows, baseline, {}, accept)
    return info


def plan_dataset(table_name: str, mode: str, rows: List[dict], accept_drop: set,
                 base_date: datetime.date = None) -> dict:
    """적재하지 않고 무엇이 바뀔지만 본다(file 모드·사전 점검용). DB 에 쓰지 않는다.

    반환: {'replace': 지울 기존 행, 'insert': 새 행, 'years': [...], 'baseline': 급감 비교 행 수,
           'baseline_dt': 비교 판 기준일, 'drop_ok': bool, 'problem': str|None}
    """
    phys = table_name
    years = sorted({r['year'] for r in rows}) if mode == 'year' else []
    if base_date is None and rows:
        base_date = rows[0].get('src_base_dt')
    out = {'replace': None, 'insert': len(rows), 'years': years, 'baseline': None, 'baseline_dt': None,
           'drop_ok': True, 'problem': None}
    with _engine().connect() as conn:
        try:
            validate_rows(phys, rows, table_columns(conn, phys))
            # 급감 방어에 걸려도 지울 행·비교 행 수는 보여 준다(점검 결과를 판단하는 데 필요하다).
            if mode == 'year':
                old = _existing_by_year(conn, phys, years)
                out['replace'] = out['baseline'] = sum(old.values())
            else:
                out['baseline'], out['baseline_dt'], out['replace'] = edition_baseline(conn, phys, base_date)
            _guard(conn, table_name, mode, rows, years, accept_drop, base_date)
        except ValueError as exc:
            out['drop_ok'] = False
            out['problem'] = str(exc)
    if isinstance(out['baseline_dt'], datetime.date):
        out['baseline_dt'] = out['baseline_dt'].strftime('%Y-%m-%d')   # 실행 요약 JSON 에 그대로 쓴다
    return out


def load_dataset(table_name: str, mode: str, edition_path: str, base_date: datetime.date,
                 rows: List[dict], tracking: dict, accept_drop: set, pinned: bool = False) -> dict:
    """데이터셋 1종을 한 트랜잭션으로 적재하고 추적 행을 갱신한다. 실패하면 예외(전부 롤백).

    tracking: load_tracking() 의 해당 표 값(stat_api_id, src_latest_chn_dt 등).
    pinned  : 운영자가 ODCLOUD_EMP_EDITION 으로 판을 지정한 수동 실행인가.
    rows    : 모든 행에 src_base_dt(= base_date)가 들어 있어야 한다(build_rows 가 채운다).

    옛 판 보충(pinned 이고 판 기준일이 기록된 기준일보다 이름)
      · MODE_EDITION: 그 판 기준일의 행만 지우고 넣는다 — 최신 판 등 다른 판은 그대로 남는다.
      · MODE_YEAR   : 표에 행이 **하나도 없는 연도만** 넣는다. 누적 판(2018~2024)을 지정해도 이미 있는
                      연도(최신 판으로 넣은 값)는 옛 값으로 덮지 않는다 — 빠진 해를 채우는 용도로만 쓴다.
      · 두 경우 모두 추적 행(판·기준일)은 그대로 둔다. 바꾸면 다음 정기 실행이 최신 판을 다시 적재해야 한다.
    정기 실행(pinned=False)은 기록된 기준일과 관계없이 항상 추적 행을 갱신한다(기록 값이 잘못돼 있어도
    매달 같은 판을 다시 적재하는 일이 생기지 않게).
    반환: {'deleted', 'inserted', 'years', 'skipped_years', 'baseline', 'baseline_dt', 'tracking_updated'}
    """
    if not rows:
        raise ValueError('%s: 적재할 행이 없음' % table_name)
    if any(r.get('src_base_dt') != base_date for r in rows):
        # 판 기준일이 빠지거나 다른 판이 섞인 행은 판 단위 교체를 깨뜨린다(build_rows 가 채우지 않은 경우 등).
        raise ValueError('%s: src_base_dt 가 판 기준일 %s 과 다른 행이 있음' % (table_name, base_date))
    phys = table_name
    base_str = base_date.strftime('%Y-%m-%d')
    recorded = str((tracking or {}).get('src_latest_chn_dt') or '')[:10]
    backfill = bool(pinned and recorded and base_str < recorded)
    with _engine().begin() as conn:
        # 같은 표를 두 실행이 동시에 바꾸지 않도록 표 이름 기준 트랜잭션 잠금(커밋·롤백 때 풀린다).
        conn.execute(text('SELECT pg_advisory_xact_lock(hashtext(:k))'), {'k': 'ODCLOUD_EMP:' + phys})
        validate_rows(phys, rows, table_columns(conn, phys))
        skipped: List[int] = []
        years: List[int] = []
        if mode == 'year':
            years = sorted({r['year'] for r in rows})
            if backfill:
                present = _existing_by_year(conn, phys, years)
                skipped = [y for y in years if present.get(y)]
                years = [y for y in years if not present.get(y)]
                rows = [r for r in rows if r['year'] in years]
                logger.warning('%s: 옛 판 보충 — 이미 있는 연도 %s 는 건너뛰고 빈 연도 %s 만 넣음',
                               table_name, skipped, years)
                if not rows:
                    return {'deleted': 0, 'inserted': 0, 'years': [], 'skipped_years': skipped,
                            'baseline': 0, 'baseline_dt': None, 'tracking_updated': False}
        guard = _guard(conn, table_name, mode, rows, years, accept_drop, base_date)
        if mode == 'year':
            deleted = conn.execute(text('DELETE FROM ' + phys + ' WHERE year IN :ys')
                                   .bindparams(bindparam('ys', expanding=True)), {'ys': years}).rowcount
        else:
            # 같은 판의 행만 지운다. 다른 판·기준일 NULL 행은 남는다.
            deleted = conn.execute(text('DELETE FROM ' + phys + ' WHERE src_base_dt = :d'),
                                   {'d': base_date}).rowcount
        cols = list(rows[0].keys()) + ['created_by']
        target = table(phys, *[column(c) for c in cols])
        # insert() 구성체로 넣어야 SQLAlchemy 가 여러 행을 묶어 보낸다(insertmanyvalues) — 4만 행도 수 초.
        conn.execute(insert(target), [dict(r, created_by=CREATED_BY) for r in rows])
        if not backfill:
            _update_tracking(conn, table_name, tracking, edition_path, base_str)
        else:
            logger.warning('%s: 지정 판 기준일 %s 이 기록된 기준일 %s 보다 이름 — 추적 행은 그대로 둠(보충 적재)',
                           table_name, base_str, recorded)
    logger.info('%s: 판 %s(%s) 적재 — 삭제 %d · 입력 %d%s · 급감 비교 %s행(%s)', phys,
                edition_path.rsplit('/', 1)[-1][5:13], base_str, deleted, len(rows),
                (' · 연도 %s' % years) if years else '', guard['baseline'], guard['baseline_dt'] or '-')
    return {'deleted': deleted, 'inserted': len(rows), 'years': years, 'skipped_years': skipped,
            'baseline': guard['baseline'],
            'baseline_dt': guard['baseline_dt'].strftime('%Y-%m-%d') if guard['baseline_dt'] else None,
            'tracking_updated': not backfill}


def _update_tracking(conn, table_name: str, tracking: dict, edition_path: str, base_str: str) -> None:
    """sys_stats_src_api_info(판·시각) + sys_data_summary_info(기준일·적재일) 갱신. 같은 트랜잭션."""
    # 시각은 DB 의 CURRENT_TIMESTAMP 로 쓴다(서버 OS 시간대와 무관하게 timestamptz 로 정확히 남는다).
    n1 = conn.execute(text("""
        UPDATE sys_stats_src_api_info
           SET api_data_url = :path, latest_sync_time = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP,
               updated_by = :by
         WHERE stat_api_id = :id AND stat_tbl_id = :t"""),
        {'path': edition_path, 'by': CREATED_BY,
         'id': tracking['stat_api_id'], 't': table_name}).rowcount
    if n1 != 1:
        raise RuntimeError('sys_stats_src_api_info(%s) 갱신 %d행 — 1행이어야 함' % (table_name, n1))
    n2 = conn.execute(text("""
        UPDATE sys_data_summary_info
           SET src_latest_chn_dt = :base, sys_data_ref_dt = :today, updated_at = CURRENT_TIMESTAMP
         WHERE sys_tbl_id = :t AND del_yn = 'N' AND status = 'A'"""),
        {'base': base_str, 'today': datetime.date.today().strftime('%Y-%m-%d'), 't': table_name}).rowcount
    if n2 == 0:
        # KOSIS 적재기와 같은 규칙 — 요약 행이 없으면 공개 API·포털 목록과 어긋나므로 실패로 둔다.
        raise RuntimeError('sys_data_summary_info 에 sys_tbl_id=%s 행이 없음' % table_name)
    if n2 > 1:
        logger.warning('sys_data_summary_info(%s) %d행 갱신(예상 1행)', table_name, n2)

