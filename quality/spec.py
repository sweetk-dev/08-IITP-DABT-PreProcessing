"""데이터 명세서(테이블/컬럼 정의서) 읽기·쓰기.

명세서는 CSV 한 장이다. 열 구성은 2025년 품질 시험에 낸 명세서와 같다.

    table,column,type,key,not_null,range_min,range_max,rule_format,rule_date,unique,desc

- type      : 숫자 / 문자 / 날짜 (명세서 표기 그대로)
- key       : PK, FK 또는 빈칸
- not_null  : O 이면 값 완전성 검사 대상
- range_min / range_max : 둘 중 하나라도 있으면 범위 검사 대상(한쪽만 있으면 그쪽만 본다)
- rule_format : 정규식. 값 전체가 맞아야 통과한다(앞뒤 ^ $ 를 자동으로 붙인다)
- rule_date   : 날짜 형식(YYYY, YYYYMM, YYYY-MM-DD, YYYYMMDD 중 하나 또는 정규식)
- unique    : O 이면 값 유일성(중복 불허) 검사 대상
- 그 밖의 열(seq·desc 등)은 그대로 보존만 한다.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional

COLUMNS = ["seq", "table", "column", "type", "key", "not_null", "range_min", "range_max",
           "rule_format", "rule_date", "unique", "desc"]

# 명세서 '타입' 값 → 비교용 분류. 영어 표기도 받아 준다.
TYPE_ALIASES = {
    "숫자": "number", "수치": "number", "number": "number", "numeric": "number",
    "문자": "text", "문자열": "text", "text": "text", "string": "text",
    "날짜": "date", "일시": "date", "date": "date", "datetime": "date", "timestamp": "date",
}

# 명세서 '규칙(날짜)' 의 대표 표기 → 정규식
DATE_RULES = {
    "YYYY": r"[0-9]{4}",
    "YYYYMM": r"[0-9]{4}(0[1-9]|1[0-2])",
    "YYYYMMDD": r"[0-9]{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])",
    "YYYY-MM": r"[0-9]{4}-(0[1-9]|1[0-2])",
    "YYYY-MM-DD": r"[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])",
}


def _yes(v: Optional[str]) -> bool:
    """명세서의 동그라미 표시를 참/거짓으로 바꾼다. O·○·Y·1·TRUE 를 참으로 본다."""
    return str(v or "").strip().upper() in {"O", "○", "◯", "Y", "YES", "1", "TRUE"}


def _num(v: Optional[str]) -> Optional[float]:
    """범위 칸의 숫자를 읽는다. 빈칸·숫자가 아닌 값은 None(검사하지 않음)."""
    s = str(v or "").replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


@dataclass
class ColumnSpec:
    """명세서 한 줄(컬럼 하나)의 규칙."""
    table: str
    column: str
    type: str = ""
    key: str = ""
    not_null: bool = False
    range_min: Optional[float] = None
    range_max: Optional[float] = None
    rule_format: str = ""
    rule_date: str = ""
    unique: bool = False
    extra: Dict[str, str] = field(default_factory=dict)

    @property
    def type_class(self) -> Optional[str]:
        """비교용 타입 분류(number/text/date). 명세서에 타입이 없거나 모르는 표기면 None."""
        return TYPE_ALIASES.get(self.type.strip().lower()) or TYPE_ALIASES.get(self.type.strip())

    @property
    def has_range(self) -> bool:
        return self.range_min is not None or self.range_max is not None

    @property
    def pattern_regex(self) -> Optional[str]:
        """'규칙(형식)' 정규식(값 전체 일치). 없으면 None."""
        r = self.rule_format.strip()
        return _anchor(r) if r else None

    @property
    def date_regex(self) -> Optional[str]:
        """'규칙(날짜)' 를 정규식으로. 대표 표기가 아니면 적힌 값을 정규식으로 본다."""
        r = self.rule_date.strip()
        if not r:
            return None
        return _anchor(DATE_RULES.get(r.upper(), r))

    def to_row(self) -> Dict[str, str]:
        d = {k: "" for k in COLUMNS}
        d.update(self.extra)
        d.update({
            "table": self.table, "column": self.column, "type": self.type, "key": self.key,
            "not_null": "O" if self.not_null else "", "unique": "O" if self.unique else "",
            "range_min": _fmt(self.range_min), "range_max": _fmt(self.range_max),
            "rule_format": self.rule_format, "rule_date": self.rule_date,
        })
        return d


def _anchor(regex: str) -> str:
    """정규식이 값 전체와 맞도록 ^ $ 를 붙인다(이미 붙어 있으면 그대로)."""
    r = regex
    if not r.startswith("^"):
        r = "^(" + r + ")" if not r.endswith("$") else "^(" + r[:-1] + ")$"
    if not r.endswith("$"):
        r = r + "$"
    return r


def _fmt(v: Optional[float]) -> str:
    if v is None:
        return ""
    return str(int(v)) if float(v).is_integer() else str(v)


def load(path: str | Path) -> List[ColumnSpec]:
    """명세서 CSV 를 읽는다(UTF-8, BOM 허용). 테이블명·컬럼명이 없는 줄은 건너뛴다."""
    out: List[ColumnSpec] = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            t, c = (row.get("table") or "").strip(), (row.get("column") or "").strip()
            if not t or not c:
                continue
            known = {"table", "column", "type", "key", "not_null", "range_min", "range_max",
                     "rule_format", "rule_date", "unique"}
            out.append(ColumnSpec(
                table=t, column=c, type=(row.get("type") or "").strip(), key=(row.get("key") or "").strip(),
                not_null=_yes(row.get("not_null")), unique=_yes(row.get("unique")),
                range_min=_num(row.get("range_min")), range_max=_num(row.get("range_max")),
                rule_format=(row.get("rule_format") or "").strip(), rule_date=(row.get("rule_date") or "").strip(),
                extra={k: v for k, v in row.items() if k not in known and k is not None},
            ))
    return out


def save(specs: Iterable[ColumnSpec], path: str | Path) -> None:
    """명세서 CSV 로 쓴다. 엑셀에서 바로 열리도록 UTF-8 BOM 을 붙인다.

    명세서 기본 열 뒤에 extra 의 열(예: 초안의 관측값 obs_*)을 붙여 함께 쓴다.
    """
    specs = list(specs)
    extra_cols: List[str] = []
    for s in specs:
        for k in s.extra:
            if k not in COLUMNS and k not in extra_cols:
                extra_cols.append(k)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS + extra_cols, extrasaction="ignore")
        w.writeheader()
        for i, s in enumerate(specs, start=1):
            row = s.to_row()
            row["seq"] = row.get("seq") or str(i)
            w.writerow(row)


def by_table(specs: Iterable[ColumnSpec]) -> Dict[str, List[ColumnSpec]]:
    """테이블별로 묶는다(명세서 순서 유지)."""
    out: Dict[str, List[ColumnSpec]] = {}
    for s in specs:
        out.setdefault(s.table, []).append(s)
    return out
