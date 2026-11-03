"""공공데이터포털 장애인 파일데이터(ODCLOUD_DIS) 적재 모듈 — v1.22.0.

대상 표: 01-IITP-DABT-Database v1.17.0(odcloud_dis_collect_v1170.sql)의 16개 표. 행 변환은 collectors/odcloud_dis.
검사·급감 방어·판/연도 단위 교체는 고용 적재기(db_odcloud_emp)의 함수를 그대로 쓰고, 여기서는
  · 목록형 원천의 '최신 판으로 교체'(MODE_REPLACE, 범위 = 표 전체 또는 종류 칸 값)
  · 판 추적 키(stat_tbl_id)가 표 이름과 다를 수 있는 점(지원기관 목록 'poi_dis_support_org:<종류>')
  · 공개 데이터 요약표 행이 아직 없는 표(06 플랫폼 노출 전) — 행이 있을 때만 갱신
을 더한다.

데이터셋 1종 = 트랜잭션 1개 (고용 적재기와 같음)
    잠금(키 단위) → 표 칸 정보 조회 → 행 검사 → 급감 방어 → 교체 범위 삭제 → 새 행 입력 → 추적 행 갱신.
    중간에 실패하면 그 데이터셋 전체가 롤백되어 표·추적 행이 모두 그대로 남는다.

교체 범위
    MODE_EDITION  같은 판 기준일(src_base_dt)의 행만 — 다른 판은 남는다(고용 14종과 같음).
    MODE_YEAR     판에 들어 있는 연도의 행만.
    MODE_REPLACE  범위 전체(scope 칸이 있으면 그 값의 행만, 없으면 표 전체). 목록형 원천은 판을 쌓지 않고
                  최신 판만 둔다. 급감 방어 비교 대상 = 범위의 기존 행 수.
급감 방어 허용: ODCLOUD_DIS_ACCEPT_DROP=<키>[,<키>] 또는 ALL (키 = stat_tbl_id).
"""
from __future__ import annotations

import datetime
import logging
from typing import Dict, List, Tuple

from sqlalchemy import bindparam, column, insert, table, text

import db_odcloud_emp as base
from collectors.odcloud_dis import MODE_REPLACE, DisDataset

logger = logging.getLogger('db')

CREATED_BY = base.CREATED_BY
ACCEPT_ENV = 'ODCLOUD_DIS_ACCEPT_DROP'


def _engine():
    return base._engine()


def load_tracking(ext_api_id: int) -> Dict[str, dict]:
    """ODCLOUD_DIS 의 키별 추적 정보 {stat_tbl_id: {...}} — 고용 적재기와 같은 조회."""
    return base.load_tracking(ext_api_id)


def _scope_sql(ds: DisDataset) -> Tuple[str, dict]:
    """MODE_REPLACE 의 범위 조건 ' WHERE …' 과 바인드 값. scope 가 없으면 표 전체(빈 조건)."""
    vals = ds.scope_values
    if not vals:
        return '', {}
    parts, params = [], {}
    for i, (col, val) in enumerate(sorted(vals.items())):
        parts.append('%s = :s%d' % (col, i))
        params['s%d' % i] = val
    return ' WHERE ' + ' AND '.join(parts), params


def _replace_count(conn, ds: DisDataset) -> int:
    where, params = _scope_sql(ds)
    return int(conn.execute(text('SELECT count(*) FROM ' + ds.table + where), params).scalar_one())


def plan_dataset(ds: DisDataset, rows: List[dict], accept_drop: set, base_date: datetime.date) -> dict:
    """적재하지 않고 무엇이 바뀔지만 본다(file 모드). 반환 모양은 고용 적재기의 plan_dataset 과 같다."""
    if ds.mode != MODE_REPLACE:
        return base.plan_dataset(ds.table, ds.mode, rows, accept_drop, base_date=base_date)
    out = {'replace': None, 'insert': len(rows), 'years': [], 'baseline': None, 'baseline_dt': None,
           'drop_ok': True, 'problem': None}
    with _engine().connect() as conn:
        try:
            base.validate_rows(ds.table, rows, base.table_columns(conn, ds.table))
            out['replace'] = out['baseline'] = _replace_count(conn, ds)
            base.check_drop(ds.key, len(rows), out['baseline'], accept_drop, env_name=ACCEPT_ENV)
        except ValueError as exc:
            out['drop_ok'] = False
            out['problem'] = str(exc)
    return out


def load_dataset(ds: DisDataset, edition_path: str, base_date: datetime.date, rows: List[dict],
                 tracking: dict, accept_drop: set) -> dict:
    """데이터셋 1종을 한 트랜잭션으로 적재하고 추적 행을 갱신한다. 실패하면 예외(전부 롤백).

    반환: {'deleted', 'inserted', 'years', 'baseline', 'baseline_dt', 'tracking_updated'}
    """
    if not rows:
        raise ValueError('%s: 적재할 행이 없음' % ds.key)
    if any(r.get('src_base_dt') != base_date for r in rows):
        raise ValueError('%s: src_base_dt 가 판 기준일 %s 과 다른 행이 있음' % (ds.key, base_date))
    phys = ds.table
    years: List[int] = []
    with _engine().begin() as conn:
        # 같은 데이터셋을 두 실행이 동시에 바꾸지 않도록 키 기준 트랜잭션 잠금.
        conn.execute(text('SELECT pg_advisory_xact_lock(hashtext(:k))'), {'k': 'ODCLOUD_DIS:' + ds.key})
        base.validate_rows(phys, rows, base.table_columns(conn, phys))
        if ds.mode == MODE_REPLACE:
            old = _replace_count(conn, ds)
            base.check_drop(ds.key, len(rows), old, accept_drop, env_name=ACCEPT_ENV)
            where, params = _scope_sql(ds)
            deleted = conn.execute(text('DELETE FROM ' + phys + where), params).rowcount
            guard = {'baseline': old, 'baseline_dt': None}
        else:
            if ds.mode == 'year':
                years = sorted({r['year'] for r in rows})
            guard = base._guard(conn, phys, ds.mode, rows, years, accept_drop, base_date, env_name=ACCEPT_ENV)
            if ds.mode == 'year':
                deleted = conn.execute(text('DELETE FROM ' + phys + ' WHERE year IN :ys')
                                       .bindparams(bindparam('ys', expanding=True)), {'ys': years}).rowcount
            else:
                deleted = conn.execute(text('DELETE FROM ' + phys + ' WHERE src_base_dt = :d'),
                                       {'d': base_date}).rowcount
        cols = list(rows[0].keys()) + ['created_by']
        target = table(phys, *[column(c) for c in cols])
        conn.execute(insert(target), [dict(r, created_by=CREATED_BY) for r in rows])
        _update_tracking(conn, ds, tracking, edition_path, base_date.strftime('%Y-%m-%d'))
    baseline_dt = guard.get('baseline_dt')
    logger.info('%s: 판 %s(%s) 적재 — 삭제 %d · 입력 %d%s · 급감 비교 %s행', ds.key,
                edition_path.rsplit('/', 1)[-1][5:13], base_date, deleted, len(rows),
                (' · 연도 %s' % years) if years else '', guard.get('baseline'))
    return {'deleted': deleted, 'inserted': len(rows), 'years': years, 'baseline': guard.get('baseline'),
            'baseline_dt': baseline_dt.strftime('%Y-%m-%d') if isinstance(baseline_dt, datetime.date) else None,
            'tracking_updated': True}


def _update_tracking(conn, ds: DisDataset, tracking: dict, edition_path: str, base_str: str) -> None:
    """sys_stats_src_api_info(판·시각) 갱신 + 요약표 행이 있으면 기준일·적재일 갱신. 같은 트랜잭션."""
    n1 = conn.execute(text("""
        UPDATE sys_stats_src_api_info
           SET api_data_url = :path, latest_sync_time = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP,
               updated_by = :by
         WHERE stat_api_id = :id AND stat_tbl_id = :t"""),
        {'path': edition_path, 'by': CREATED_BY, 'id': tracking['stat_api_id'], 't': ds.key}).rowcount
    if n1 != 1:
        raise RuntimeError('sys_stats_src_api_info(%s) 갱신 %d행 — 1행이어야 함' % (ds.key, n1))
    if ds.key != ds.table:
        # 표 하나에 원천 여러 개(지원기관 목록) — 표 단위 요약표 기준일은 원천마다 달라 갱신하지 않는다.
        return
    conn.execute(text("""
        UPDATE sys_data_summary_info
           SET src_latest_chn_dt = :base, sys_data_ref_dt = :today, updated_at = CURRENT_TIMESTAMP
         WHERE sys_tbl_id = :t AND del_yn = 'N' AND status = 'A'"""),
        {'base': base_str, 'today': datetime.date.today().strftime('%Y-%m-%d'), 't': ds.table})
