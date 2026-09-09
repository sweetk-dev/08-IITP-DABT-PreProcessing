# -*- coding: utf-8 -*-
"""중앙보조기기센터 전국 보조기기센터 명부 적재 — KNAT_CENTER.

원천: 보건복지부 국립재활원 중앙보조기기센터 「전국 보조기기 관련 센터 > 보조기기센터」
      https://www.knat.go.kr/knw/home/knat/knat_map.php
      중앙 1곳 + 시도·권역 32곳 = 33곳. tools/fetch_knat_center.ps1 로 수집한다.
적재: poi_emergency_support (support_type='repair', confidence='H')

사용:
    python scripts/load_knat_center_csv.py --csv data/knat_center_20260908.csv
    python scripts/load_knat_center_csv.py --csv <원본> --geocoded data/knat_center_geocoded_20260908.csv
    python scripts/load_knat_center_csv.py --csv <원본> --dry-run --out-csv /tmp/norm.csv

같은 사이트의 KNAT_REPAIR 과 무엇이 다른가
  KNAT_REPAIR(knat_repair.php, 244건)는 지자체 수리 지원사업 **지정업체 명부**이고
  주소·좌표·전화가 전혀 없다. 이쪽(knat_map.php)은 **공공 위탁 보조기기센터**라
  주소·전화·홈페이지가 원천에 있고 수리·개조·세척을 직접 수행한다. 그래서 'H' 다.
  두 원천은 자연키가 겹치지 않으므로 함께 적재해도 충돌하지 않는다.

운영시간
  이 원천에도 운영시간 항목이 **없다.** 공공기관이라 평일 09:00~18:00 일 것이라는
  추정으로 채우지 않는다 — 야간 대응 가능 여부를 사실처럼 안내하게 되기 때문이다.
  기관별 홈페이지 확인은 별도 트랙이다.

전담지역
  '전국', '부산광역시', '경기북부' 처럼 관할 범위다. 소재지 시군구와 다를 수 있어
  sgg_name 으로 쓰지 않고 note 에 남긴다.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

SOURCE = 'KNAT_CENTER'
SUPPORT_TYPE = 'repair'

# 주소 앞머리(시도) → 01 sys_common_code(grp_id='sido_code')
SIDO_PREFIX = (
    ('서울', '9110000'), ('부산', '9260000'), ('대구', '9270000'), ('인천', '9280000'),
    ('광주', '9290000'), ('대전', '9300000'), ('울산', '9310000'), ('세종', '9360000'),
    ('경기', '9410000'), ('충청북도', '9430000'), ('충북', '9430000'),
    ('충청남도', '9440000'), ('충남', '9440000'),
    ('전북', '9520000'), ('전라북도', '9520000'),
    ('전남', '9460000'), ('전라남도', '9460000'),
    ('경상북도', '9470000'), ('경북', '9470000'),
    ('경상남도', '9480000'), ('경남', '9480000'),
    ('제주', '9500000'), ('강원', '9510000'),
)

DB_FIELDS = ('support_type', 'sido_code', 'sgg_name', 'name', 'addr_road', 'addr_jibun',
             'zip_code', 'latitude', 'longitude', 'tel', 'homepage', 'open_hours',
             'note', 'source', 'confidence', 'base_dt')

PLACEHOLDER_URLS = {'www.', 'www', '-', 'N/A'}
SEJONG = '9360000'


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


def resolve_sido(addr):
    head = (addr or '').strip()
    for prefix, code in SIDO_PREFIX:
        if head.startswith(prefix):
            return code
    return None


def extract_sgg(addr, sido_code=None, geo_sgg=None):
    """시군구명. 지오코딩이 준 값이 있으면 그것을 쓴다(카카오 region_2depth_name).

    문자열 파싱은 두 군데서 틀린다.
      - 세종특별자치시는 자치구가 없어 도로명이 잡히거나 None 이 된다
      - '수원시 권선구' 처럼 일반구가 있는 시는 '수원시'에서 끊겨 하위 구가 사라진다
    """
    if geo_sgg:
        return geo_sgg
    head = (addr or '').strip()
    if sido_code == SEJONG or head.startswith('세종'):
        return '세종특별자치시'
    tokens = head.split()
    if len(tokens) >= 3 and tokens[1].endswith('시') and tokens[2].endswith(('구', '군')):
        return '%s %s' % (tokens[1], tokens[2])
    for token in tokens[1:3]:
        if token.endswith(('시', '군', '구')) and not token.endswith(('특별시', '광역시', '특별자치시')):
            return token
    return None


def _cut(value, limit):
    """DB 컬럼 길이 가드 — 원천에 긴 값이 섞이면 트랜잭션이 통째로 롤백된다."""
    if value is None:
        return None
    token = str(value).strip()
    if not token:
        return None
    return token[:limit]


def normalize_homepage(value):
    """도메인처럼 생긴 값만 URL 로 본다.

    수집기가 href 를 못 잡으면 앵커 텍스트('바로가기')가 넘어올 수 있고, 그대로
    'http://' 를 붙이면 http://바로가기 같은 값이 DB 에 들어간다.
    """
    token = _clean(value)
    if not token or token in PLACEHOLDER_URLS:
        return None
    if not re.search(r'[A-Za-z0-9-]+\.[A-Za-z]{2,}', token):
        return None
    if not token.startswith(('http://', 'https://')):
        token = 'http://' + token
    return token[:400]


def load_geocode(path):
    if not path:
        return {}
    rows = read_csv(path, ('센터명', '주소', 'geo_grade'))
    table = {}
    for row in rows:
        if _clean(row.get('geo_grade')) != 'A':
            continue
        table[(_clean(row.get('센터명')), _clean(row.get('주소')))] = {
            'road_norm': _clean(row.get('addr_road')),      # 카카오 정규화 도로명(참고용)
            'addr_jibun': _clean(row.get('addr_jibun')),
            'zip_code': _clean(row.get('zip_code')),
            'sgg_name': _clean(row.get('sgg_name')),
            'latitude': _to_float(row.get('latitude')),
            'longitude': _to_float(row.get('longitude')),
        }
    return table


def map_row(src, geo, base_dt):
    name = _clean(src.get('센터명'))
    addr = _clean(src.get('주소'))
    area = _clean(src.get('전담지역'))
    hit = geo.get((name, addr)) or {}

    bits = []
    if area:
        bits.append('전담지역: ' + area)
    bits.append('공공 위탁 보조기기센터 — 수리·개조·세척 수행')
    if hit:
        bits.append('좌표는 원천 주소를 카카오 주소검색으로 변환(건물 단위 일치분만)')
        if hit.get('road_norm'):
            bits.append('도로명(정규화): ' + hit['road_norm'])
    else:
        bits.append('좌표 미확보 — 주소만 보유')

    sido_code = resolve_sido(addr)
    return {
        'support_type': SUPPORT_TYPE,
        'sido_code': sido_code,
        'sgg_name': _cut(extract_sgg(addr, sido_code, hit.get('sgg_name')), 60),
        'name': _cut(name, 200),
        # ⚠️ 자연키가 (유형, 이름, 도로명주소) 라서 addr_road 는 **항상 원천 주소**여야
        #    한다. 지오코딩 유무로 이 값이 바뀌면 33개 센터가 통째로 중복 적재된다.
        'addr_road': _cut(addr, 600),
        'addr_jibun': _cut(hit.get('addr_jibun'), 600),
        # 원천이 "(우)01022" 형태로 우편번호를 주므로 지오코딩 결과보다 원천을 우선한다
        'zip_code': _cut(_clean(src.get('우편번호')) or hit.get('zip_code'), 10),
        'latitude': hit.get('latitude'),
        'longitude': hit.get('longitude'),
        'tel': _cut(src.get('전화번호'), 40),
        'homepage': normalize_homepage(src.get('홈페이지')),
        'open_hours': None,          # 원천에 없음 — 복무규정 추정 금지
        'note': (' | '.join(bits))[:1000],
        'source': SOURCE,
        'confidence': 'H',
        'base_dt': base_dt,
    }


def build(csv_path, geo_path, base_dt):
    raw = read_csv(csv_path, ('센터명', '주소'))
    geo = load_geocode(geo_path)
    rows, skipped = [], 0
    seen = set()
    for src in raw:
        row = map_row(src, geo, base_dt)
        if not row['name']:
            skipped += 1
            continue
        if row['sido_code'] is None:
            print('  [시도코드 미매핑] %s — %s' % (row['name'], row['addr_road']))
        key = (row['support_type'], row['name'], row['addr_road'] or '')
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        rows.append(row)

    located = sum(1 for r in rows if r['latitude'] is not None)
    print('원본 %d행 → 적재 대상 %d건 (제외·중복 %d)' % (len(raw), len(rows), skipped))
    print('좌표 보강 %d건 / 주소만 %d건' % (located, len(rows) - located))
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True)
    parser.add_argument('--geocoded', default=None)
    parser.add_argument('--base-dt', default=None)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--out-csv', default=None)
    args = parser.parse_args()

    base_dt = args.base_dt or datetime.date.today().isoformat()
    rows = build(args.csv, args.geocoded, base_dt)

    if args.out_csv:
        with io.open(args.out_csv, 'w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(DB_FIELDS))
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k) for k in DB_FIELDS})
        print('정규화 CSV 저장: %s' % args.out_csv)

    if args.dry_run:
        for row in rows:
            print('  %-10s %-30s %-38s %s' % (row['sgg_name'] or '-', row['name'],
                                              (row['addr_road'] or '')[:38], row['tel'] or ''))
        return 0

    import db_mobility
    count = db_mobility.upsert_emergency_support(rows)
    db_mobility.touch_latest_sync(SOURCE)
    print('poi_emergency_support 적재 %d건' % count)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
