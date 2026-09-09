# -*- coding: utf-8 -*-
"""국민건강보험공단 장애인보조기기 급여 등록업소 적재 — NHIS_ASSIST_STORE.

원천: 국민건강보험공단 「장애인 보조기기 등록업소 확인」
      https://www.nhis.or.kr/nhis/policy/retrieveAssistingDevicesRegStoreList.do
      오픈API 없음. 조회 화면(POST 폼)을 tools/fetch_nhis_assist_store.ps1 로 수집한다.
      2026-09-08 실측: 전동휠체어(가군,나군) 취급 전국 2,022건 / 의료용 스쿠터 2,015건
적재: poi_emergency_support (support_type='repair', confidence='L')

사용:
    python scripts/load_nhis_assist_store_csv.py --csv data/nhis_assist_store_20260908.csv
    python scripts/load_nhis_assist_store_csv.py --csv <원본> --geocoded data/nhis_assist_store_geocoded_20260908.csv
    python scripts/load_nhis_assist_store_csv.py --csv <원본> --dry-run --out-csv /tmp/norm.csv

왜 confidence='L' 인가
  이 목록은 **급여 구입처로 공단에 등록된 판매·수리 업소**다. 지자체 수리 지원사업
  지정업체(KNAT_REPAIR)와도, 공공 위탁 거점(KNAT_CENTER)과도 성격이 다르다.
  등록은 급여 제품 판매 자격이지 수리 역량을 보증하지 않고, 공단 안내문도 폐업 등으로
  수시 탈퇴된다고 명시한다. 그래서 확인 대상으로 표시해 둔다.

  다만 knat 지정업체 명부와 달리 **주소가 원천에 있다.** 그래서 좌표는 상호명 키워드
  검색이 아니라 주소 검색으로 붙인다(tools/enrich_address_kakao.ps1).

운영시간
  원천에 없다. 비워 둔다. 민간 업체 영업시간을 주는 공개 API 는 국내에 없다
  (네이버 검색API·카카오 로컬 모두 해당 필드 없음) — 추정치를 넣지 않는다.
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

SOURCE = 'NHIS_ASSIST_STORE'
SUPPORT_TYPE = 'repair'

# 공단 조회 화면의 시도코드 → 01 sys_common_code(grp_id='sido_code')
# 공단은 2026-07 출범한 '전남광주통합특별시'(12)를 한 코드로 쓴다. 내부 코드는 아직
# 전라남도(9460000)/광주광역시(9290000)로 나뉘어 있어 주소 앞머리로 다시 가른다.
SIDO_CODE = {
    '11': '9110000', '26': '9260000', '27': '9270000', '28': '9280000',
    '30': '9300000', '31': '9310000', '36': '9360000', '41': '9410000',
    '43': '9430000', '44': '9440000', '47': '9470000', '48': '9480000',
    '50': '9500000', '51': '9510000', '52': '9520000',
}
MERGED_SIDO = '12'                       # 전남광주통합특별시
GWANGJU = '9290000'
JEONNAM = '9460000'
SEJONG = '9360000'
# 통합특별시 주소에서 광주 몫을 가려내는 자치구 목록
GWANGJU_GU = ('동구', '서구', '남구', '북구', '광산구')

DB_FIELDS = ('support_type', 'sido_code', 'sgg_name', 'name', 'addr_road', 'addr_jibun',
             'zip_code', 'latitude', 'longitude', 'tel', 'homepage', 'open_hours',
             'note', 'source', 'confidence', 'base_dt')

# 전동보장구와 무관한 품목만 취급하는 업소는 적재 대상이 아니다
MOBILITY_ITEMS = ('전동휠체어', '의료용 스쿠터', '수동휠체어')


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


def resolve_sido(sido_cd, addr):
    """시도코드 → 내부 코드. 통합특별시는 주소 두 번째 토큰으로 광주/전남을 가른다.

    ⚠️ 앞머리 문자열로 가르면 안 된다. 원천 주소가 '전남광주통합특별시 광산구 …'
       형태라 startswith('전남') 이 참이 되어 광주 자치구가 전부 전남으로 넘어간다.
    """
    code = _clean(sido_cd) or ''
    if code != MERGED_SIDO:
        return SIDO_CODE.get(code)
    head = (addr or '').strip()
    tokens = head.split()
    if head.startswith('전남광주통합특별시'):
        if len(tokens) > 1 and tokens[1] in GWANGJU_GU:
            return GWANGJU
        return JEONNAM
    if head.startswith(('광주광역시', '광주시', '광주')):
        return GWANGJU
    if head.startswith(('전라남도', '전남')):
        return JEONNAM
    return None


def extract_sgg(addr, sido_code=None, geo_sgg=None):
    """시군구명. 지오코딩이 준 값이 있으면 그것을 쓴다(카카오 region_2depth_name).

    문자열 파싱은 두 군데서 틀린다.
      - 세종특별자치시는 자치구가 없어 '한누리대로' 같은 도로명이 잡히거나 None 이 된다
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


def load_geocode(path):
    """주소 보강 결과 → (업소명, 원본주소) 키의 검증 통과분만."""
    if not path:
        return {}
    rows = read_csv(path, ('업소명', '주소', 'geo_grade'))
    table = {}
    for row in rows:
        if _clean(row.get('geo_grade')) != 'A':
            continue
        key = (_clean(row.get('업소명')), _clean(row.get('주소')))
        table[key] = {
            'road_norm': _clean(row.get('addr_road')),      # 카카오 정규화 도로명(참고용)
            'addr_jibun': _clean(row.get('addr_jibun')),
            'zip_code': _clean(row.get('zip_code')),
            'sgg_name': _clean(row.get('sgg_name')),
            'latitude': _to_float(row.get('latitude')),
            'longitude': _to_float(row.get('longitude')),
        }
    return table


def map_row(src, geo, base_dt):
    name = _clean(src.get('업소명'))
    addr = _clean(src.get('주소'))
    items = _clean(src.get('취급품목'))
    hit = geo.get((name, addr)) or {}

    note_bits = []
    if items:
        note_bits.append('취급품목: ' + items)
    note_bits.append('국민건강보험공단 급여 등록업소 — 수리 가능 여부는 개별 확인 필요')
    if hit:
        note_bits.append('좌표는 원천 주소를 카카오 주소검색으로 변환(건물 단위 일치분만)')
        if hit.get('road_norm'):
            note_bits.append('도로명(정규화): ' + hit['road_norm'])
    else:
        note_bits.append('좌표 미확보 — 주소만 보유')

    sido_code = resolve_sido(src.get('시도코드'), addr)
    return {
        'support_type': SUPPORT_TYPE,
        'sido_code': sido_code,
        'sgg_name': _cut(extract_sgg(addr, sido_code, hit.get('sgg_name')), 60),
        'name': _cut(name, 200),
        # ⚠️ 자연키가 (유형, 이름, 도로명주소) 라서 addr_road 는 **항상 원천 주소**여야 한다.
        #    지오코딩 유무에 따라 이 값이 바뀌면 같은 업소가 UPDATE 되지 않고 새 행으로
        #    쌓인다. 카카오 정규화 도로명은 note 에만 남긴다.
        'addr_road': _cut(addr, 600),
        'addr_jibun': _cut(hit.get('addr_jibun'), 600),
        'zip_code': _cut(hit.get('zip_code'), 10),
        'latitude': hit.get('latitude'),
        'longitude': hit.get('longitude'),
        'tel': _cut(src.get('전화번호'), 40),
        'homepage': None,
        'open_hours': None,          # 원천에 없음 — 추정치 금지
        'note': (' | '.join(note_bits))[:1000],
        'source': SOURCE,
        'confidence': 'L',
        'base_dt': base_dt,
        '_items': items or '',
        '_sido_cd': _clean(src.get('시도코드')),
    }


def build(csv_path, geo_path, base_dt, sidos, require_item):
    raw = read_csv(csv_path, ('시도코드', '업소명', '주소'))
    geo = load_geocode(geo_path)
    rows, skipped, unmapped, merged = [], 0, 0, 0
    seen = {}
    for src in raw:
        row = map_row(src, geo, base_dt)
        if not row['name']:
            skipped += 1
            continue
        if sidos and row['_sido_cd'] not in sidos:
            skipped += 1
            continue
        # 원천을 pumMokCd(전동휠체어)로 조회했으면 전 행이 이미 대상이다. 그래서 기본은
        # 거르지 않는다. --require-mobility-item 을 준 경우에만 취급품목을 확인하고,
        # 이때 품목이 비어 있는 행은 "확인 불가"이므로 통과시키지 않는다.
        if require_item:
            items_text = row.get('_items') or ''
            if not any(k in items_text for k in MOBILITY_ITEMS):
                skipped += 1
                continue
        if row['sido_code'] is None:
            unmapped += 1
            print('  [시도코드 미매핑] %s — %s' % (row['_sido_cd'], row['name']))
        # 같은 업소가 품목별 조회에서 중복 수집될 수 있다. 자연키로 접되, 뒤에 온 행의
        # 취급품목은 버리지 말고 앞 행 note 에 합친다.
        key = (row['support_type'], row['name'], row['addr_road'] or '')
        if key in seen:
            merged += 1
            head = seen[key]
            extra = [t.strip() for t in (row.get('_items') or '').split('|') if t.strip()]
            if extra:
                have = head['note']
                add = [t for t in extra if t not in have]
                if add:
                    head['note'] = (have + ' | 추가 취급품목: ' + ', '.join(add))[:1000]
            if head.get('latitude') is None and row.get('latitude') is not None:
                for f in ('latitude', 'longitude', 'addr_jibun', 'zip_code', 'sgg_name'):
                    head[f] = row[f]
            continue
        seen[key] = row
        rows.append(row)

    for row in rows:
        row.pop('_items', None)
        row.pop('_sido_cd', None)

    located = sum(1 for r in rows if r['latitude'] is not None)
    print('원본 %d행 → 적재 대상 %d건 (제외 %d, 자연키 병합 %d, 시도코드 미매핑 %d)'
          % (len(raw), len(rows), skipped, merged, unmapped))
    print('좌표 보강 %d건 / 주소만 %d건 (보강 파일 검증통과 %d건)'
          % (located, len(rows) - located, len(geo)))
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True, help='fetch_nhis_assist_store.ps1 산출 CSV')
    parser.add_argument('--geocoded', default=None, help='enrich_address_kakao.ps1 산출 CSV')
    parser.add_argument('--base-dt', default=None, help='수집 기준일자 (기본: 오늘)')
    parser.add_argument('--sido', default=None, help='쉼표 구분 공단 시도코드. 생략하면 전국')
    parser.add_argument('--require-mobility-item', action='store_true',
                        help='취급품목에 전동보장구 계열이 명시된 행만 적재(품목 공란은 제외)')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--out-csv', default=None)
    args = parser.parse_args()

    base_dt = args.base_dt or datetime.date.today().isoformat()
    sidos = None
    if args.sido and args.sido.strip():
        sidos = {t.strip() for t in args.sido.split(',') if t.strip()}

    rows = build(args.csv, args.geocoded, base_dt, sidos, args.require_mobility_item)

    if args.out_csv:
        with io.open(args.out_csv, 'w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(DB_FIELDS))
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k) for k in DB_FIELDS})
        print('정규화 CSV 저장: %s' % args.out_csv)

    if args.dry_run:
        for row in rows[:20]:
            print('  %-8s %-28s %-40s %s' % (row['sgg_name'] or '-', row['name'],
                                             (row['addr_road'] or '')[:40], row['tel'] or ''))
        return 0

    import db_mobility
    count = db_mobility.upsert_emergency_support(rows)
    db_mobility.touch_latest_sync(SOURCE)
    print('poi_emergency_support 적재 %d건' % count)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
