"""국가철도공단 역사 설비 CSV 적재 스크립트 — KRNA_STN (01 v1.3.0 테이블 3종).

사용:
    python scripts/load_krna_station_csv.py --csv <파일.csv> [--csv <파일2.csv> ...] [--base-dt YYYY-MM-DD]
    python scripts/load_krna_station_csv.py --dir <폴더>            # 폴더의 *.csv 전부
    python scripts/load_krna_station_csv.py --csv a.csv --dry-run  # 파싱 결과만 출력

파일 종류는 **헤더로 자동 판별**한다(파일명에 의존하지 않는다).

| 종류 | 헤더(cp949) | 적재 |
|---|---|---|
| ev       | 철도운영기관명,선명,역명,출입구번호,상세위치,정원_인원,정원_중량 | poi_station_elevator_unit (선명 단위 교체) |
| toilet   | 철도운영기관명,선명,역명,지상구분,역층,게이트내외,출구번호,상세위치,화장실구분 | poi_station_toilet_unit (선명·disabled_yn 단위 교체) |
| platform | 철도운영기관명,선명,역명,승강장번호,상하행,지상구분,역층,승강장연결 여부,스크린도어 유무,안전발판 유무 | poi_station_platform (UPSERT) |
| gap      | 철도운영기관명,선명,역명,승강장번호,차량순서,차량출입문번호,안전거리 | poi_station_platform 의 gap_min/max/avg_cm·door_cnt (승강장 단위 요약 UPDATE) |

화장실과 장애인화장실 파일은 헤더가 같다 — `--disabled` 로 지정하거나, 파일명에 '장애인'
이 들어 있으면 disabled_yn='Y' 로 적재한다.

연 1회 파일 갱신이므로 배치 등록 없이 수동 재실행한다. 완료 후 sys_ext_api_info(KRNA_STN)
latest_sync_time 을 갱신한다.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import glob
import io
import os
import sys
from collections import OrderedDict, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

HEADERS = {
    'ev': ['철도운영기관명', '선명', '역명', '출입구번호', '상세위치', '정원_인원', '정원_중량'],
    'toilet': ['철도운영기관명', '선명', '역명', '지상구분', '역층', '게이트내외', '출구번호', '상세위치', '화장실구분'],
    'platform': ['철도운영기관명', '선명', '역명', '승강장번호', '상하행', '지상구분', '역층',
                 '승강장연결 여부', '스크린도어 유무', '안전발판 유무'],
    'gap': ['철도운영기관명', '선명', '역명', '승강장번호', '차량순서', '차량출입문번호', '안전거리'],
}


def _clean(v) -> str:
    return (v or '').replace('\ufeff', '').strip()


def _int(v):
    v = _clean(v)
    if not v:
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def _float(v):
    v = _clean(v)
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _yn(v):
    v = _clean(v).upper()
    if v in ('Y', 'YES', '유', 'O', '있음', 'TRUE', '1'):
        return 'Y'
    if v in ('N', 'NO', '무', 'X', '없음', 'FALSE', '0'):
        return 'N'
    return None


def read_text(path: str) -> str:
    """cp949(포털 기본) → utf-8-sig 순으로 시도."""
    raw = open(path, 'rb').read()
    for enc in ('utf-8-sig', 'cp949', 'euc-kr'):
        try:
            txt = raw.decode(enc)
        except UnicodeDecodeError:
            continue
        # utf-8-sig 로 디코드된 cp949 파일은 한글이 깨져 있으므로 헤더로 재확인
        if '철도운영기관명' in txt.splitlines()[0]:
            return txt
    raise ValueError('헤더를 인식할 수 없는 파일: %s' % path)


def detect_kind(header: list) -> str:
    h = [_clean(x) for x in header]
    for kind, expect in HEADERS.items():
        if h[:len(expect)] == expect:
            return kind
    raise ValueError('알 수 없는 헤더: %s' % h)


def parse_rows(text: str, base_dt: str, disabled: bool = False):
    """(kind, rows) — kind 별 적재 행. unit_seq 는 (선명, 역명[, 구분]) 안에서 1부터."""
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    kind = detect_kind(header)
    idx = {name: i for i, name in enumerate([_clean(x) for x in header])}

    def col(rec, name):
        i = idx.get(name)
        return _clean(rec[i]) if i is not None and i < len(rec) else ''

    rows = []
    seq = defaultdict(int)
    for rec in reader:
        if not rec or not col(rec, '역명') or not col(rec, '선명'):
            continue
        base = {'oper_org': col(rec, '철도운영기관명') or None,
                'line_name': col(rec, '선명'), 'stn_name': col(rec, '역명'),
                'base_dt': base_dt}
        if kind == 'ev':
            k = (base['line_name'], base['stn_name'])
            seq[k] += 1
            base.update({'unit_seq': seq[k], 'exit_no': col(rec, '출입구번호') or None,
                         'detail_loc': col(rec, '상세위치') or None,
                         'capacity_person': _int(col(rec, '정원_인원')),
                         'capacity_kg': _int(col(rec, '정원_중량'))})
        elif kind == 'toilet':
            dis = 'Y' if disabled else 'N'
            k = (base['line_name'], base['stn_name'], dis)
            seq[k] += 1
            base.update({'disabled_yn': dis, 'unit_seq': seq[k],
                         'ground_dv': col(rec, '지상구분') or None,
                         'floor_no': col(rec, '역층') or None,
                         'gate_inout': col(rec, '게이트내외') or None,
                         'exit_no': col(rec, '출구번호') or None,
                         'detail_loc': col(rec, '상세위치') or None,
                         'toilet_kind': col(rec, '화장실구분') or None})
        elif kind == 'platform':
            base.update({'platform_no': col(rec, '승강장번호'),
                         'updown': col(rec, '상하행') or None,
                         'ground_dv': col(rec, '지상구분') or None,
                         'floor_no': col(rec, '역층') or None,
                         'platform_connect_yn': _yn(col(rec, '승강장연결 여부')),
                         'screen_door_yn': _yn(col(rec, '스크린도어 유무')),
                         'safety_plate_yn': _yn(col(rec, '안전발판 유무'))})
            if not base['platform_no']:
                continue
        else:   # gap — 출입문 단위 원자료. 요약은 aggregate_gaps 에서
            gap = _float(col(rec, '안전거리'))
            if gap is None or not col(rec, '승강장번호'):
                continue
            base.update({'platform_no': col(rec, '승강장번호'), 'gap_cm': gap})
        rows.append(base)
    return kind, rows


def aggregate_gaps(rows: list) -> list:
    """출입문별 이격거리 → 승강장별 min/max/avg/door_cnt."""
    acc = OrderedDict()
    for r in rows:
        k = (r['line_name'], r['stn_name'], r['platform_no'])
        a = acc.setdefault(k, {'oper_org': r.get('oper_org'), 'line_name': k[0], 'stn_name': k[1],
                               'platform_no': k[2], 'base_dt': r.get('base_dt'), 'vals': []})
        a['vals'].append(r['gap_cm'])
    out = []
    for a in acc.values():
        vals = a.pop('vals')
        a.update({'gap_min_cm': round(min(vals), 1), 'gap_max_cm': round(max(vals), 1),
                  'gap_avg_cm': round(sum(vals) / len(vals), 1), 'door_cnt': len(vals)})
        out.append(a)
    return out


def load_file(path: str, base_dt: str, disabled=None, dry_run: bool = False) -> tuple:
    """파일 1개 적재. 반환 (kind, 적재 행 수)."""
    text = read_text(path)
    if disabled is None:
        disabled = '장애인' in os.path.basename(path)
    kind, rows = parse_rows(text, base_dt, disabled=disabled)
    if kind == 'gap':
        rows = aggregate_gaps(rows)
    if dry_run:
        return kind, len(rows)
    import db_mobility  # noqa: E402  (DB 미설정 환경에서도 --dry-run 은 돈다)
    if kind == 'ev':
        n = db_mobility.replace_station_elevators(rows)
    elif kind == 'toilet':
        n = db_mobility.replace_station_toilets(rows)
    elif kind == 'platform':
        n = db_mobility.upsert_station_platforms(rows)
    else:
        n = db_mobility.update_platform_gaps(rows)
    return kind, n


def main():
    parser = argparse.ArgumentParser(description='국가철도공단 역사 설비 CSV 적재 (EV·화장실·승강장·이격거리)')
    parser.add_argument('--csv', action='append', default=[], help='CSV 파일 경로(복수 지정 가능)')
    parser.add_argument('--dir', help='폴더 안의 *.csv 전부')
    parser.add_argument('--base-dt', default=datetime.date.today().isoformat(), help='데이터 기준일')
    parser.add_argument('--disabled', action='store_true',
                        help='화장실 파일을 장애인화장실(disabled_yn=Y)로 적재 (파일명에 "장애인"이 있으면 자동)')
    parser.add_argument('--dry-run', action='store_true', help='DB 없이 파싱 결과만 출력')
    args = parser.parse_args()

    paths = list(args.csv)
    if args.dir:
        paths += sorted(glob.glob(os.path.join(args.dir, '*.csv')))
    if not paths:
        parser.error('--csv 또는 --dir 이 필요합니다')

    # 이격거리 요약은 승강장 행이 먼저 있어야 UPDATE 가 붙는다 — gap 파일을 뒤로 보낸다
    ordered = []
    for p in paths:
        kind = detect_kind(next(csv.reader(io.StringIO(read_text(p)))))
        ordered.append((1 if kind == 'gap' else 0, p))
    ordered.sort()

    total = 0
    for _, path in ordered:
        kind, n = load_file(path, args.base_dt,
                            disabled=(True if args.disabled else None), dry_run=args.dry_run)
        print(f'{path}: [{kind}] {n}행' + (' (dry-run)' if args.dry_run else ' 적재'))
        total += n
    if not args.dry_run:
        import db_mobility
        db_mobility.touch_latest_sync('KRNA_STN')
    print(f'완료 — 총 {total}행')


if __name__ == '__main__':
    main()
