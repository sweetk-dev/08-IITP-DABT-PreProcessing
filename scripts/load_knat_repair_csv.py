# -*- coding: utf-8 -*-
"""중앙보조기기센터 전국 보조기기 수리센터 명부 적재 — KNAT_REPAIR.

원천: 보건복지부 국립재활원 중앙보조기기센터 「전국 보조기기 관련 센터 > 수리 센터」
      https://www.knat.go.kr/knw/home/knat/knat_repair.php  (시도별 POST loc_idx=1~17)
      2026-09-08 수집 기준 17개 시도 합계 244건
적재: poi_emergency_support (support_type='repair', confidence='L')

사용:
    python scripts/load_knat_repair_csv.py --csv data/knat_repair_20260908.csv
    python scripts/load_knat_repair_csv.py --csv <원본> --geocoded data/knat_repair_geocoded_20260908.csv
    python scripts/load_knat_repair_csv.py --csv <원본> --dry-run --out-csv /tmp/norm.csv

이 원천을 왜 confidence='L' 로 넣는가
  이것은 위치 데이터가 아니라 **지원사업 지정업체 명부**다. 원천에 주소·좌표·전화·
  운영시간이 없고, 컬럼은 시도명·시군구명·운영방식·업체명·연간 지원기준뿐이다.
  게다가 체인 상호(액티피아, 휠로피아, 케어존 등)가 여러 자치구에 반복 등재되는데,
  이는 그 구에 지점이 있다는 뜻이 아니라 그 구가 그 업체에서 수리비를 지원해 준다는
  뜻이다. 따라서 시군구명만 보고 좌표를 붙이면 틀린 좌표가 들어간다.

좌표를 채우는 규칙 (--geocoded)
  별도 보강 파일(카카오 로컬 검색 결과)에서 geo_grade='A' 인 행만 좌표를 받는다.
  'A' 는 ①카카오 주소에 명부의 시군구명이 들어 있고 ②상호가 서로 포함 관계일 때만
  부여된다. 통과 못 한 행은 주소·좌표를 비운 채 적재한다 — 추정 좌표를 넣지 않는다.

운영시간
  원천에 없다. 비워 둔다. 야간 대응 가능 여부는 이 값이 채워져야 판단할 수 있으므로
  기관별 홈페이지 확인이라는 별도 트랙이 필요하다(설계 문서 참조).
"""
from __future__ import annotations

import argparse
import csv
import datetime
import io
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

SOURCE = 'KNAT_REPAIR'
SUPPORT_TYPE = 'repair'

# 명부는 시도를 약칭으로 쓴다 → 01 sys_common_code(grp_id='sido_code')
SIDO_CODE = {
    '서울': '9110000', '부산': '9260000', '대구': '9270000', '인천': '9280000',
    '광주': '9290000', '대전': '9300000', '울산': '9310000', '세종': '9360000',
    '경기': '9410000', '강원': '9510000', '충북': '9430000', '충남': '9440000',
    '전북': '9520000', '전남': '9460000', '경북': '9470000', '경남': '9480000',
    '제주': '9500000',
}

DB_FIELDS = ('support_type', 'sido_code', 'sgg_name', 'name', 'addr_road', 'addr_jibun',
             'zip_code', 'latitude', 'longitude', 'tel', 'homepage', 'open_hours',
             'note', 'source', 'confidence', 'base_dt')


def _clean(value):
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _to_float(value):
    token = _clean(value)
    if token is None:
        return None
    try:
        return float(token)
    except ValueError:
        return None


def read_csv(path, required):
    for encoding in ('utf-8-sig', 'cp949'):
        try:
            with io.open(path, encoding=encoding, newline='') as handle:
                rows = list(csv.DictReader(handle))
            break
        except UnicodeDecodeError:
            rows = None
    if rows is None:
        raise RuntimeError('CSV 인코딩을 판별하지 못했습니다: %s' % path)
    if not rows:
        raise RuntimeError('CSV 에 데이터 행이 없습니다: %s' % path)
    missing = [h for h in required if h not in rows[0]]
    if missing:
        raise RuntimeError('CSV 헤더가 원천과 다릅니다 — 누락: %s' % ', '.join(missing))
    return rows


def load_geocode(path):
    """보강 파일 → (시도, 시군구, 업체명) 키의 검증 통과분만."""
    if not path:
        return {}
    rows = read_csv(path, ('sido', 'sigungu', 'name', 'geo_grade'))
    table = {}
    for row in rows:
        if _clean(row.get('geo_grade')) != 'A':
            continue
        key = (_clean(row.get('sido')), _clean(row.get('sigungu')), _clean(row.get('name')))
        table[key] = {
            'addr_road': _clean(row.get('addr_road')),
            'addr_jibun': _clean(row.get('addr_jibun')),
            'latitude': _to_float(row.get('latitude')),
            'longitude': _to_float(row.get('longitude')),
            'tel': _clean(row.get('tel')),
            'kakao_name': _clean(row.get('kakao_name')),
        }
    return table


def map_row(src, geo, base_dt):
    sido = _clean(src.get('시도명'))
    sgg = _clean(src.get('시군구명'))
    name = _clean(src.get('업체명'))
    hit = geo.get((sido, sgg, name)) or {}
    return {
        'support_type': SUPPORT_TYPE,
        'sido_code': SIDO_CODE.get(sido or ''),
        'sgg_name': sgg,
        'name': name,
        'addr_road': hit.get('addr_road'),
        'addr_jibun': hit.get('addr_jibun'),
        'zip_code': None,
        'latitude': hit.get('latitude'),
        'longitude': hit.get('longitude'),
        'tel': hit.get('tel'),
        'homepage': None,
        # 원천에 운영시간 항목이 없다 — 추정치를 넣지 않는다
        'open_hours': None,
        'note': None,
        'source': SOURCE,
        'confidence': 'L',
        'base_dt': base_dt,
        '_sido': sido,
        '_oper': _clean(src.get('운영방식')),
        '_support': _clean(src.get('지원기준')),
        '_geocoded': bool(hit),
    }


def merge_rows(rows):
    """자연키(유형, 상호, 도로명주소)가 같은 행을 합치고 지원 시군구를 note 에 모은다.

    좌표를 못 붙인 체인 업체는 도로명주소가 비어 있어 자연키가 상호 하나로 모인다.
    그대로 두면 어느 시군구에서 지원되는지가 사라지므로 note 에 전부 남긴다.
    """
    merged, order = {}, []
    for row in rows:
        key = (row['support_type'], row['name'], row['addr_road'] or '')
        if key not in merged:
            row['_sggs'] = [row['sgg_name']] if row['sgg_name'] else []
            row['_opers'] = [row['_oper']] if row['_oper'] else []
            row['_supports'] = [row['_support']] if row['_support'] else []
            merged[key] = row
            order.append(key)
            continue
        head = merged[key]
        if row['sgg_name'] and row['sgg_name'] not in head['_sggs']:
            head['_sggs'].append(row['sgg_name'])
        if row['_oper'] and row['_oper'] not in head['_opers']:
            head['_opers'].append(row['_oper'])
        if row['_support'] and row['_support'] not in head['_supports']:
            head['_supports'].append(row['_support'])
        for field in ('addr_jibun', 'latitude', 'longitude', 'tel'):
            if head.get(field) in (None, '') and row.get(field) not in (None, ''):
                head[field] = row[field]

    out = []
    for key in order:
        row = merged[key]
        bits = []
        sggs = row.pop('_sggs', [])
        opers = row.pop('_opers', [])
        supports = row.pop('_supports', [])
        if len(sggs) > 1:
            row['sgg_name'] = None      # 여러 시군구에 걸친 지정업체 — 한 곳으로 못 적는다
            bits.append('지원 지정 시군구: ' + ', '.join(sggs))
        if opers:
            bits.append('운영방식: ' + ', '.join(opers))
        if supports:
            bits.append('연간 지원기준: ' + ' ; '.join(supports))
        if row.pop('_geocoded', False):
            bits.append('주소·좌표는 카카오 로컬 검증매칭(시군구+상호 일치)으로 보강 — 현장 확인 필요')
        else:
            bits.append('원천에 주소·좌표 없음 — 위치 미확인')
        row['note'] = (' | '.join(bits))[:1000]
        row.pop('_sido', None)
        row.pop('_oper', None)
        row.pop('_support', None)
        out.append(row)
    return out, len(rows) - len(out)


def build(csv_path, geo_path, base_dt, sidos, exclude):
    raw = read_csv(csv_path, ('시도명', '시군구명', '운영방식', '업체명'))
    geo = load_geocode(geo_path)
    rows, skipped = [], 0
    for src in raw:
        row = map_row(src, geo, base_dt)
        if not row['name']:
            skipped += 1
            continue
        if sidos and row['_sido'] not in sidos:
            skipped += 1
            continue
        if exclude and row['_sido'] in exclude:
            # 같은 시도를 더 나은 원천이 이미 덮고 있을 때 쓴다.
            # 경기는 GG_ASSIST_REPAIR(누림센터, 주소·좌표 보유)가 우선이다.
            skipped += 1
            continue
        if row['sido_code'] is None:
            print('  [시도코드 미매핑] %s — %s' % (row['_sido'], row['name']))
        rows.append(row)

    located = sum(1 for r in rows if r['latitude'] is not None)
    rows, merged = merge_rows(rows)
    print('원본 %d행 → 적재 대상 %d건 (범위 밖 제외 %d, 자연키 병합 %d)'
          % (len(raw), len(rows), skipped, merged))
    print('좌표 보강 %d행 / 미확인 %d행 (보강 파일 검증통과 %d건)'
          % (located, len(raw) - skipped - located, len(geo)))
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True, help='knat 명부 원본 CSV')
    parser.add_argument('--geocoded', default=None, help='카카오 보강 CSV (geo_grade 포함)')
    parser.add_argument('--base-dt', default=None, help='수집 기준일자 (기본: 오늘)')
    parser.add_argument('--sido', default=None, help='쉼표 구분 시도 약칭. 생략하면 전국')
    parser.add_argument('--exclude-sido', default='경기',
                        help='제외할 시도 약칭(쉼표 구분). 기본 경기 — 누림센터 원천이 우선')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--out-csv', default=None)
    args = parser.parse_args()

    base_dt = args.base_dt or datetime.date.today().isoformat()
    sidos = None
    if args.sido and args.sido.strip():
        sidos = {t.strip() for t in args.sido.split(',') if t.strip()}
    exclude = {t.strip() for t in (args.exclude_sido or '').split(',') if t.strip()}

    rows = build(args.csv, args.geocoded, base_dt, sidos, exclude)

    if args.out_csv:
        with io.open(args.out_csv, 'w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(DB_FIELDS))
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k) for k in DB_FIELDS})
        print('정규화 CSV 저장: %s' % args.out_csv)

    if args.dry_run:
        for row in rows[:20]:
            print('  %-8s %-30s %-30s %s' % (row['sgg_name'] or '-', row['name'],
                                             row['addr_road'] or '(위치 미확인)',
                                             row['latitude'] or ''))
        return 0

    import db_mobility
    count = db_mobility.upsert_emergency_support(rows)
    db_mobility.touch_latest_sync(SOURCE)
    print('poi_emergency_support 적재 %d건' % count)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
