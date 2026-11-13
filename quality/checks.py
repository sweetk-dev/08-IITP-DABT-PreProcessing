"""정형 데이터 검사 — 명세서 규칙을 DB 에 적용해 컬럼별 통과율을 낸다.

검사 5종(2025년 품질 시험에서 쓴 심사 종류와 같다)
    - 컬럼 적합성: 명세서 타입(숫자/문자/날짜)과 DB 컬럼 타입의 분류가 같은가(컬럼 단위)
    - 구문 유효성(패턴): '규칙(형식)'·'규칙(날짜)' 정규식에 값 전체가 맞는가(값 단위, 빈 값 제외)
    - 범위 유효성: 값이 Min~Max 안에 있는가(값 단위, 빈 값 제외, 숫자로 못 읽는 값은 이탈)
    - 값 유일성: 중복 불허 컬럼에 같은 값이 두 번 이상 나오는가(값 단위, 빈 값 제외)
    - 값 완전성: Not Null 컬럼에 빈 값이 있는가(행 단위)

통과율 = 1 - (어긋난 수 / 검사한 수). 여러 컬럼을 합칠 때는 컬럼별 비율의 평균이 아니라
어긋난 수와 검사한 수를 각각 더해서 나눈다(값이 많은 컬럼이 그만큼 무겁게 반영된다).

DB 는 읽기만 한다(SELECT). 테이블마다 한 번만 훑도록 그 테이블의 검사를 쿼리 하나로 묶는다.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import text

from quality.spec import ColumnSpec, by_table

# PostgreSQL data_type → 비교용 분류
_PG_NUMBER = {"smallint", "integer", "bigint", "numeric", "real", "double precision", "decimal"}
_PG_TEXT = {"character varying", "character", "text", "varchar", "char", "bpchar", "uuid", "json", "jsonb"}
_PG_DATE = {"date", "timestamp without time zone", "timestamp with time zone", "time without time zone",
            "time with time zone"}


def pg_type_class(data_type: str) -> Optional[str]:
    """information_schema.columns.data_type 값을 number/text/date 로 나눈다. 그 밖(배열·geometry 등)은 None."""
    d = (data_type or "").lower()
    if d in _PG_NUMBER:
        return "number"
    if d in _PG_TEXT:
        return "text"
    if d in _PG_DATE:
        return "date"
    return None


@dataclass
class CheckResult:
    """검사 하나(컬럼 × 검사 종류)의 결과."""
    table: str
    column: str
    check: str           # column_type / pattern / date / range / unique / not_null
    checked: int         # 검사한 수(컬럼 적합성은 1)
    failed: int          # 어긋난 수
    note: str = ""       # 사람이 판정할 때 참고할 말(예: DB 타입, 없는 컬럼)

    @property
    def pass_rate(self) -> Optional[float]:
        return None if self.checked == 0 else 100.0 * (1 - self.failed / self.checked)

    def as_dict(self) -> dict:
        d = asdict(self)
        d["pass_rate"] = None if self.pass_rate is None else round(self.pass_rate, 4)
        return d


def _q(name: str) -> str:
    """식별자 인용. 이름은 DB 카탈로그에서 확인한 것만 쓰지만, 큰따옴표는 한 번 더 막는다."""
    return '"' + name.replace('"', '""') + '"'


def fetch_columns(conn, schema: str = "public") -> Dict[Tuple[str, str], str]:
    """DB 의 (테이블, 컬럼) → data_type. 명세서에 적힌 이름이 실제로 있는지도 이것으로 확인한다."""
    rows = conn.execute(text(
        "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = :s"),
        {"s": schema}).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def check_column_types(specs: Iterable[ColumnSpec], db_cols: Dict[Tuple[str, str], str]) -> List[CheckResult]:
    """컬럼 적합성. 명세서 타입이 비어 있으면 검사 대상에서 뺀다. DB 에 없는 컬럼은 불일치로 센다."""
    out = []
    for s in specs:
        want = s.type_class
        if want is None:
            continue
        dt = db_cols.get((s.table, s.column))
        if dt is None:
            out.append(CheckResult(s.table, s.column, "column_type", 1, 1, "DB 에 컬럼 없음"))
            continue
        got = pg_type_class(dt)
        out.append(CheckResult(s.table, s.column, "column_type", 1, 0 if got == want else 1,
                               f"명세서 {s.type} / DB {dt}"))
    return out


def _range_expr(col: str, dt: str, spec: ColumnSpec) -> Optional[str]:
    """범위 이탈 여부를 내는 SQL 식(참=이탈). 빈 값은 검사하지 않는다."""
    cls = pg_type_class(dt)
    lo, hi = spec.range_min, spec.range_max
    if cls == "number":
        v = col
    elif cls == "date":
        # 범위가 연도(4자리 이하)로 적혀 있으면 연도로, 아니면 YYYYMMDD 숫자로 비교한다
        big = max(abs(x) for x in (lo, hi) if x is not None)
        v = f"EXTRACT(YEAR FROM {col})" if big <= 9999 else f"to_char({col}, 'YYYYMMDD')::numeric"
    else:
        # 문자 컬럼: 숫자로 읽히면 숫자로 비교, 못 읽으면 이탈
        v = (f"(CASE WHEN btrim({col}::text) ~ '^-?[0-9]+(\\.[0-9]+)?$' "
             f"THEN btrim({col}::text)::numeric END)")
    conds = []
    if lo is not None:
        conds.append(f"{v} < {lo!r}")
    if hi is not None:
        conds.append(f"{v} > {hi!r}")
    if cls not in ("number", "date"):
        conds.append(f"{v} IS NULL")   # 숫자로 못 읽은 값
    return "(" + " OR ".join(conds) + ")" if conds else None


def check_table(conn, table: str, specs: List[ColumnSpec], db_cols: Dict[Tuple[str, str], str],
                blank_as_null: bool = False) -> List[CheckResult]:
    """한 테이블의 패턴·날짜·범위·유일성·완전성 검사를 쿼리 하나로 실행한다.

    blank_as_null: 참이면 공백뿐인 문자열도 빈 값으로 본다(2025 시험은 NULL 만 빈 값으로 봤다 — 기본 거짓).
    """
    exprs: List[str] = ["count(*) AS n"]
    plan: List[Tuple[str, str, str]] = []  # (column, check, alias prefix)
    for i, s in enumerate(specs):
        dt = db_cols.get((table, s.column))
        if dt is None:
            continue  # 없는 컬럼은 컬럼 적합성에서 이미 불일치로 셌다
        c = _q(s.column)
        empty = f"({c} IS NULL" + (f" OR btrim({c}::text) = ''" if blank_as_null and pg_type_class(dt) == "text" else "") + ")"
        nonempty = f"(NOT {empty})"
        if s.pattern_regex:
            exprs.append(f"count(*) FILTER (WHERE {nonempty}) AS p{i}_n")
            exprs.append(f"count(*) FILTER (WHERE {nonempty} AND NOT ({c}::text ~ :re{i})) AS p{i}_f")
            plan.append((s.column, "pattern", f"p{i}"))
        if s.date_regex:
            # 날짜형 컬럼은 명세서 형식으로 바꿔 놓고 비교한다(YYYY-MM-DD 등)
            fmt = s.rule_date.upper() if s.rule_date.upper() in ("YYYY", "YYYYMM", "YYYYMMDD", "YYYY-MM", "YYYY-MM-DD") else None
            val = f"to_char({c}, '{fmt}')" if (fmt and pg_type_class(dt) == "date") else f"{c}::text"
            exprs.append(f"count(*) FILTER (WHERE {nonempty}) AS d{i}_n")
            exprs.append(f"count(*) FILTER (WHERE {nonempty} AND NOT ({val} ~ :dre{i})) AS d{i}_f")
            plan.append((s.column, "date", f"d{i}"))
        if s.has_range:
            rx = _range_expr(c, dt, s)
            if rx:
                exprs.append(f"count(*) FILTER (WHERE {nonempty}) AS r{i}_n")
                exprs.append(f"count(*) FILTER (WHERE {nonempty} AND {rx}) AS r{i}_f")
                plan.append((s.column, "range", f"r{i}"))
        if s.unique:
            exprs.append(f"count({c}) AS u{i}_n")
            exprs.append(f"count({c}) - count(DISTINCT {c}) AS u{i}_f")
            plan.append((s.column, "unique", f"u{i}"))
        if s.not_null:
            exprs.append(f"count(*) AS c{i}_n")
            exprs.append(f"count(*) FILTER (WHERE {empty}) AS c{i}_f")
            plan.append((s.column, "not_null", f"c{i}"))
    if not plan:
        return []
    params = {}
    for i, s in enumerate(specs):
        if s.pattern_regex:
            params[f"re{i}"] = s.pattern_regex
        if s.date_regex:
            params[f"dre{i}"] = s.date_regex
    sql = f"SELECT {', '.join(exprs)} FROM {_q(table)}"
    row = conn.execute(text(sql), params).mappings().one()
    return [CheckResult(table, col, chk, int(row[f"{a}_n"]), int(row[f"{a}_f"])) for col, chk, a in plan]


def run_all(conn, specs: List[ColumnSpec], schema: str = "public", blank_as_null: bool = False) -> List[CheckResult]:
    """명세서 전체를 검사한다. DB 에 없는 테이블은 컬럼 적합성 불일치로만 남는다."""
    db_cols = fetch_columns(conn, schema)
    results = check_column_types(specs, db_cols)
    tables = {t for (t, _c) in db_cols}
    for table, cols in by_table(specs).items():
        if table in tables:
            results.extend(check_table(conn, table, cols, db_cols, blank_as_null))
    return results


def aggregate(results: Iterable[CheckResult], checks: Iterable[str]) -> Tuple[int, int, Optional[float]]:
    """검사 종류들을 합쳐 (검사한 수, 어긋난 수, 통과율%) 를 낸다."""
    keep = set(checks)
    n = f = 0
    for r in results:
        if r.check in keep:
            n += r.checked
            f += r.failed
    return n, f, (None if n == 0 else 100.0 * (1 - f / n))


def summarize(results: List[CheckResult]) -> Dict[str, dict]:
    """지표 1·2·3·5 를 계산한다.

    - 컬럼 적합성 = 타입이 맞은 컬럼 수 / 검사한 컬럼 수
    - 패턴       = '규칙(형식)'·'규칙(날짜)' 구문 유효성 통과율
    - 범위       = 범위 유효성 통과율
    - 전처리     = (유일성 통과율 + 완전성 통과율) / 2, 소수점 첫째 자리에서 내림(정수 %)
    """
    out = {}
    for key, checks in (("profiling_column", ["column_type"]), ("profiling_pattern", ["pattern", "date"]),
                        ("profiling_range", ["range"]), ("uniqueness", ["unique"]), ("completeness", ["not_null"])):
        n, f, rate = aggregate(results, checks)
        out[key] = {"checked": n, "failed": f, "pass_rate": None if rate is None else round(rate, 4)}
    u, c = out["uniqueness"]["pass_rate"], out["completeness"]["pass_rate"]
    if u is not None and c is not None:
        pre = (u + c) / 2
    else:
        pre = u if c is None else c
    out["preprocessing"] = {"pass_rate_raw": None if pre is None else round(pre, 4),
                            "pass_rate": None if pre is None else math.floor(pre)}
    return out
