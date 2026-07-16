"""국가철도공단 휠체어리프트 CSV 적재 스크립트 — Issue #76 (KRNA_LIFT).

사용:
    python scripts/load_lift_csv.py --csv <파일.csv> [--csv <파일2.csv>] [--base-dt YYYY-MM-DD]

CSV 헤더(cp949): 철도운영기관명,선명,역명,관리번호,출입구번호,상세위치,길이,폭,시작층,종료층
적재: poi_station_wheelchair_lift (UPSERT), 이후 sys_ext_api_info.latest_sync_time(KRNA_LIFT) 갱신.
파일 데이터라 배치 주기는 분기 1회 수동 확인(데이터 갱신 시 재실행).
"""
from __future__ import annotations

import argparse
import csv
import datetime
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import db_mobility  # noqa: E402

HEADER_MAP = {
    '철도운영기관명': 'oper_org',
    '선명': 'line_name',
    '역명': 'stn_name',
    '관리번호': 'mng_no',
    '출입구번호': 'exit_no',
    '상세위치': 'detail_loc',
    '길이': 'length_mm',
    '폭': 'width_mm',
    '시작층': 'start_floor',
    '종료층': 'end_floor',
}

INT_COLS = ('length_mm', 'width_mm')


def read_rows(path: str, base_dt: str) -> list:
    rows = []
    with open(path, encoding='cp949', newline='') as f:
        for record in csv.DictReader(f):
            row = {}
            for src, col in HEADER_MAP.items():
                value = (record.get(src) or '').strip()
                if col in INT_COLS:
                    row[col] = int(value) if value.isdigit() else None
                else:
                    row[col] = value or None
            row['base_dt'] = base_dt
            if row['line_name'] and row['stn_name'] and row['mng_no']:
                rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser(description='국가철도공단 휠체어리프트 CSV 적재')
    parser.add_argument('--csv', action='append', required=True, help='CSV 파일 경로(복수 지정 가능)')
    parser.add_argument('--base-dt', default=datetime.date.today().isoformat(), help='데이터 기준일')
    args = parser.parse_args()

    total = 0
    for path in args.csv:
        rows = read_rows(path, args.base_dt)
        count = db_mobility.upsert_wheelchair_lifts(rows)
        print(f'{path}: {count}행 적재')
        total += count
    db_mobility.touch_latest_sync('KRNA_LIFT')
    print(f'완료 — 총 {total}행')


if __name__ == '__main__':
    main()
