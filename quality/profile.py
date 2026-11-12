"""명세서 초안 만들기 — DB 카탈로그와 실제 값 분포로 테이블/컬럼 정의서 초안을 만든다.

초안에 자동으로 채우는 것
    - type      : DB 타입을 숫자/문자/날짜로 바꿔 적는다
    - key       : PK / FK
    - not_null  : DB 에 NOT NULL 제약이 있으면 O
    - unique    : PK 이거나 단일 컬럼 UNIQUE 제약이면 O
    - desc      : 컬럼 COMMENT
비워 두는 것(작업자가 관측값을 보고 정한다): range_min·range_max·rule_format·rule_date
판단을 돕는 관측값(obs_* 열): 행 수, 빈 값 수, 고유값 수, 최솟값·최댓값, 가장 흔한 값 모양과 그 비율
    값 모양은 숫자→9, 영문→A, 한글→가 로 바꾼 것이다(예: '41173' → '99999', '2025-01' → '9999-99').

기존 명세서(--base)를 주면 그 명세서에 이미 적힌 규칙을 그대로 옮겨 오고 관측값만 새로 붙인다.
"""
from __future__ import annotations

import fnmatch
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import text

from quality.checks import fetch_columns, pg_type_class, _q
from quality.spec import ColumnSpec

TYPE_LABEL = {"number": "숫자", "text": "문자", "date": "날짜"}

# 기본 제외: 시스템·계정·백업 테이블(수집 데이터가 아님)
DEFAULT_EXCLUDE = ["sys_*", "open_*", "*_bak_*", "*_bak", "spatial_ref_sys"]

# 값 모양(shape): 숫자→9, 영문→A, 한글→가. 그 밖의 글자는 그대로 둔다.
SHAPE_SQL = ("regexp_replace(regexp_replace(regexp_replace(left({c}::text, 40), "
             "'[0-9]', '9', 'g'), '[A-Za-z]', 'A', 'g'), '[가-힣]', '가', 'g')")


def list_tables(conn, schema="public", include: Optional[List[str]] = None,
                exclude: Optional[List[str]] = None) -> List[str]:
    """검사 대상 테이블 목록. include 를 주면 그 패턴만, exclude 패턴은 뺀다."""
    rows = conn.execute(text(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = :s AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"), {"s": schema}).fetchall()
    names = [r[0] for r in rows]
    ex = DEFAULT_EXCLUDE if exclude is None else exclude
    out = []
    for n in names:
        if include and not any(fnmatch.fnmatch(n, p) for p in include):
            continue
        if any(fnmatch.fnmatch(n, p) for p in ex):
            continue
        out.append(n)
    return out


def _constraints(conn, schema="public") -> Tuple[Dict, Dict, set]:
    """(테이블, 컬럼) 별 PK·FK 표시와 단일 컬럼 UNIQUE 집합, NOT NULL 집합을 읽는다."""
    keys: Dict[Tuple[str, str], str] = {}
    uniq: set = set()
    rows = conn.execute(text("""
        SELECT tc.table_name, kcu.column_name, tc.constraint_type, tc.constraint_name,
               count(*) OVER (PARTITION BY tc.constraint_name) AS ncols
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
         WHERE tc.table_schema = :s AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')"""),
        {"s": schema}).fetchall()
    for t, c, typ, _name, ncols in rows:
        if typ == "PRIMARY KEY":
            keys[(t, c)] = "PK"
            if ncols == 1:
                uniq.add((t, c))
        elif typ == "FOREIGN KEY":
            keys.setdefault((t, c), "FK")
        elif typ == "UNIQUE" and ncols == 1:
            uniq.add((t, c))
    nn = {(r[0], r[1]) for r in conn.execute(text(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = :s AND is_nullable = 'NO'"), {"s": schema}).fetchall()}
    return keys, uniq, nn


def _comments(conn, schema="public") -> Dict[Tuple[str, str], str]:
    rows = conn.execute(text("""
        SELECT c.relname, a.attname, col_description(c.oid, a.attnum)
          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
         WHERE n.nspname = :s AND c.relkind = 'r'"""), {"s": schema}).fetchall()
    return {(r[0], r[1]): (r[2] or "") for r in rows}


def observe(conn, table: str, columns: List[Tuple[str, str]]) -> Dict[str, Dict[str, str]]:
    """컬럼별 관측값. 테이블을 한 번 훑어 빈 값·고유값·최솟값·최댓값을 내고, 값 모양은 컬럼마다 따로 센다."""
    exprs = ["count(*) AS n"]
    for i, (c, dt) in enumerate(columns):
        qc = _q(c)
        exprs.append(f"count({qc}) AS nn{i}")
        exprs.append(f"count(DISTINCT {qc}) AS d{i}")
        if pg_type_class(dt) in ("number", "date"):
            exprs.append(f"min({qc})::text AS mn{i}")
            exprs.append(f"max({qc})::text AS mx{i}")
        else:
            exprs.append(f"min(length({qc}::text))::text AS mn{i}")   # 문자는 길이 범위를 본다
            exprs.append(f"max(length({qc}::text))::text AS mx{i}")
    row = conn.execute(text(f"SELECT {', '.join(exprs)} FROM {_q(table)}")).mappings().one()
    n = int(row["n"])
    out: Dict[str, Dict[str, str]] = {}
    for i, (c, dt) in enumerate(columns):
        nn = int(row[f"nn{i}"])
        shape, ratio = "", ""
        if pg_type_class(dt) in ("text", "number") and nn:
            r = conn.execute(text(
                f"SELECT {SHAPE_SQL.format(c=_q(c))} AS s, count(*) AS k FROM {_q(table)} "
                f"WHERE {_q(c)} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 1")).first()
            if r:
                shape, ratio = r[0], f"{100.0 * r[1] / nn:.1f}"
        out[c] = {
            "obs_rows": str(n), "obs_null": str(n - nn), "obs_distinct": str(row[f"d{i}"]),
            ("obs_min" if pg_type_class(dt) in ("number", "date") else "obs_len_min"): row[f"mn{i}"] or "",
            ("obs_max" if pg_type_class(dt) in ("number", "date") else "obs_len_max"): row[f"mx{i}"] or "",
            "obs_top_shape": shape, "obs_top_shape_pct": ratio, "db_type": dt,
        }
    return out


def draft(conn, tables: Iterable[str], base: Optional[List[ColumnSpec]] = None,
          schema: str = "public", with_observation: bool = True) -> List[ColumnSpec]:
    """명세서 초안을 만든다. base 명세서에 같은 (테이블, 컬럼)이 있으면 그 규칙을 그대로 쓴다."""
    db_cols = fetch_columns(conn, schema)
    keys, uniq, nn = _constraints(conn, schema)
    cmts = _comments(conn, schema)
    base_map = {(s.table, s.column): s for s in (base or [])}
    ordinal = {r[0:2]: r[2] for r in conn.execute(text(
        "SELECT table_name, column_name, ordinal_position FROM information_schema.columns WHERE table_schema = :s"),
        {"s": schema}).fetchall()}
    out: List[ColumnSpec] = []
    for t in tables:
        cols = sorted([(c, dt) for (tt, c), dt in db_cols.items() if tt == t], key=lambda x: ordinal[(t, x[0])])
        obs = observe(conn, t, cols) if with_observation else {}
        for c, dt in cols:
            b = base_map.get((t, c))
            if b:
                s = ColumnSpec(**{**b.__dict__, "extra": dict(b.extra)})
                s.extra["source"] = "base"
            else:
                s = ColumnSpec(table=t, column=c, type=TYPE_LABEL.get(pg_type_class(dt) or "", ""),
                               key=keys.get((t, c), ""), not_null=(t, c) in nn, unique=(t, c) in uniq,
                               extra={"desc": cmts.get((t, c), ""), "source": "db"})
            s.extra.update(obs.get(c, {}))
            out.append(s)
    return out
