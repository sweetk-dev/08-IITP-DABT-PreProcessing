# -*- coding: utf-8 -*-
"""전국전동휠체어급속충전기표준데이터 적재 — STD_WCHAIR_CHARGER.

원천: 행정안전부 개방표준 「전국전동휠체어급속충전기표준데이터」
      공공데이터포털 데이터ID 15034533 (소관 보건복지부 / 제공 지방자치단체 218곳)
      갱신 반기 · 개별 기관 등록분을 매월 초 병합
적재: poi_emergency_support (support_type='charge')

사용:
    python scripts/load_national_charger_csv.py --csv data/wheelchair_charger_20260908.csv
    python scripts/load_national_charger_csv.py --csv <파일> --dry-run --out-csv /tmp/norm.csv
    python scripts/load_national_charger_csv.py --csv <파일> --sido 경기도,서울특별시

CSV 는 `_tools/fetch_std_dataset.ps1 -Pk 15034533` 이 만든 파일이다(표준데이터 그리드
다운로드 채널, 인증키·로그인 불필요). 오픈API 를 쓰려면 활용신청 후
https://api.data.go.kr/openapi/tn_pubr_public_electr_whlchairhgh_spdchrgr_api 를 호출한다.

주의 — 좌표를 그대로 믿으면 안 된다
  위경도 결측은 0건이지만, **서로 다른 시설·다른 주소인데 좌표가 똑같은 행**이 있다.
  2026-09-08 실측: 36개 그룹 / 87행 (대전 19그룹·인천 6·대구 3). 부천시청·부천종합운동장·
  삼정종합사회복지관·장애인회관·중동역 5곳이 한 좌표를 공유하는 식이다. 청사나 읍면동
  대표 좌표를 일괄 입력한 것으로 보인다. 이런 행은 note 에 표시해 근접 조회에서
  좌표를 신뢰하지 않도록 한다(값 자체는 원천 그대로 두고 판단 근거만 남긴다).

자연키 — 설치 지점까지 포함한다 (01 v1.7.0)
  자연키는 (support_type, name, coalesce(addr_road,''), coalesce(install_desc,'')) 다.
  이 원천에는 같은 건물에 설치 지점이 여러 곳인 행이 있다. 4,191행 중 380행이 주소까지
  같고, 그중에는 좌표까지 다른 것도 있다.
    김포시청 / 사우중로 1        — "제3별관 1층 로비" · "민원동 장애인화장실 옆"
    운정중앙역 환승센터           — 북측 대합실(37.7163) · 남측 대합실(37.7159)
  install_desc 를 키에 넣어 **각 설치 지점을 개별 행으로 보존**한다. 설치장소설명까지
  같은 완전 중복만 접는다(원천이 같은 행을 두 번 올린 경우).
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

SOURCE = 'STD_WCHAIR_CHARGER'
SUPPORT_TYPE = 'charge'

# 내부 시도 코드 — 01 sys_common_code(grp_id='sido_code')
SIDO_CODE = {
    '서울특별시': '9110000', '부산광역시': '9260000', '대구광역시': '9270000',
    '인천광역시': '9280000', '광주광역시': '9290000', '대전광역시': '9300000',
    '울산광역시': '9310000', '세종특별자치시': '9360000', '경기도': '9410000',
    '강원특별자치도': '9510000', '충청북도': '9430000', '충청남도': '9440000',
    '전북특별자치도': '9520000', '전라남도': '9460000', '경상북도': '9470000',
    '경상남도': '9480000', '제주특별자치도': '9500000',
}
# 원천에 개편 전 명칭이 섞여 들어온다
SIDO_ALIAS = {
    '전라북도': '전북특별자치도',
    '강원도': '강원특별자치도',
    '제주도': '제주특별자치도',
    '세종시': '세종특별자치시',
}

REQUIRED_HEADERS = ('시설명', '시도명', '시군구명', '소재지도로명주소', '위도', '경도')


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


def normalize_sido(name):
    token = _clean(name) or ''
    return SIDO_ALIAS.get(token, token)


def build_open_hours(src):
    """평일/토/공휴일 운영시각을 한 줄로.

    원천이 00:00~23:59 로 채운 곳은 상시 개방이라는 뜻이다. 시작·종료가 비어 있으면
    그 요일은 아예 적지 않는다 — 추정해서 채우지 않는다.
    """
    def span(open_key, close_key):
        o, c = _clean(src.get(open_key)), _clean(src.get(close_key))
        if not o or not c:
            return None
        if (o, c) in (('00:00', '23:59'), ('00:00', '24:00'), ('0:00', '23:59')):
            return '24시간'
        if o == c == '00:00':
            return None          # 미운영(휴무)을 0시로 적은 행
        return '%s-%s' % (o, c)

    parts = []
    weekday = span('평일운영시작시각', '평일운영종료시각')
    sat = span('토요일운영시작시각', '토요일운영종료시각')
    holiday = span('공휴일운영시작시각', '공휴일운영종료시각')
    if weekday:
        parts.append('평일 ' + weekday)
    if sat:
        parts.append('토 ' + sat)
    if holiday:
        parts.append('공휴일 ' + holiday)
    return ' / '.join(parts) or None


def build_note(src):
    """비고 — 부가 기능을 원문 뜻 그대로 남긴다.

    설치장소설명은 install_desc 컬럼으로 올렸으므로 note 에 중복해서 넣지 않는다.
    """
    bits = []
    count = _clean(src.get('동시사용가능대수'))
    if count and count not in ('0',):
        bits.append('동시사용 %s대' % count)
    if _clean(src.get('공기주입가능여부')) == 'Y':
        bits.append('공기주입 가능')
    if _clean(src.get('휴대전화충전가능여부')) == 'Y':
        bits.append('휴대전화 충전 가능')
    return ' | '.join(bits) or None


def map_row(src):
    sido = normalize_sido(src.get('시도명'))
    return {
        'support_type': SUPPORT_TYPE,
        'sido_code': SIDO_CODE.get(sido),
        'sgg_name': _clean(src.get('시군구명')),
        'name': _clean(src.get('시설명')),
        'addr_road': _clean(src.get('소재지도로명주소')),
        'addr_jibun': _clean(src.get('소재지지번주소')),
        # 표준데이터에 우편번호·홈페이지 항목이 없다. 지어내지 않는다.
        'zip_code': None,
        'latitude': _to_float(src.get('위도')),
        'longitude': _to_float(src.get('경도')),
        'tel': _clean(src.get('관리기관전화번호')),
        'homepage': None,
        'open_hours': build_open_hours(src),
        # 자연키 구성요소 — 같은 건물의 다른 설치 지점을 구분한다
        'install_desc': (_clean(src.get('설치장소설명')) or None) and _clean(src.get('설치장소설명'))[:300],
        'note': build_note(src),
        'source': SOURCE,
        'confidence': 'H',
        'base_dt': _clean(src.get('데이터기준일자')),
        '_sido': sido,
        '_desc': _clean(src.get('설치장소설명')),
        '_count': _to_float(src.get('동시사용가능대수')) or 0,
    }


def read_csv(path):
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
    missing = [h for h in REQUIRED_HEADERS if h not in rows[0]]
    if missing:
        raise RuntimeError('CSV 헤더가 원천과 다릅니다 — 누락: %s' % ', '.join(missing))
    return rows


def dedupe_exact(rows):
    """설치 지점까지 같은 완전 중복만 접는다.

    설치 지점이 다르면 별개 충전기이므로 남긴다(01 v1.7.0 자연키). 원천이 같은
    행을 두 번 올린 경우만 제거하고, 그때 기준일자는 최신 것을 남긴다.
    """
    merged, order = {}, []
    for row in rows:
        key = (row['support_type'], row['name'], row['addr_road'] or '', row['install_desc'] or '')
        if key not in merged:
            merged[key] = row
            order.append(key)
            continue
        head = merged[key]
        if (row.get('base_dt') or '') > (head.get('base_dt') or ''):
            head['base_dt'] = row['base_dt']
        for field in ('addr_jibun', 'latitude', 'longitude', 'tel', 'open_hours'):
            if head.get(field) in (None, '') and row.get(field) not in (None, ''):
                head[field] = row[field]
    return [merged[k] for k in order], len(rows) - len(order)


DB_FIELDS = ('support_type', 'sido_code', 'sgg_name', 'name', 'addr_road', 'addr_jibun',
             'zip_code', 'latitude', 'longitude', 'tel', 'homepage', 'open_hours',
             'install_desc', 'note', 'source', 'confidence', 'base_dt')


def mark_suspect_coords(rows):
    """다른 시설·다른 주소가 같은 좌표를 쓰는 행에 표시를 남긴다.

    같은 건물에 여러 대를 둔 정상 케이스(정립회관1~4)와 구분해야 하므로,
    시설명 기본형과 도로명주소가 **둘 다** 다를 때만 의심으로 본다.
    """
    import collections
    import re as _re

    def base(name):
        token = _re.sub(r'\(\d+\)$', '', (name or '').strip())
        return _re.sub(r'[\s\d]+$', '', token).strip()

    groups = collections.defaultdict(list)
    for row in rows:
        if row['latitude'] is None or row['longitude'] is None:
            continue
        groups[(round(row['latitude'], 6), round(row['longitude'], 6))].append(row)

    flagged = 0
    for members in groups.values():
        if len(members) < 2:
            continue
        names = {base(m['name']) for m in members}
        addrs = {(m['addr_road'] or '').strip() for m in members}
        if len(names) > 1 and len(addrs) > 1:
            for m in members:
                note = m.get('note') or ''
                m['note'] = (note + (' | ' if note else '')
                             + '좌표 확인 필요 — 다른 시설과 동일 좌표(원천 대표좌표 의심)')[:1000]
                flagged += 1
    return flagged


def build(path, sidos):
    raw = read_csv(path)
    rows, skipped = [], 0
    for src in raw:
        row = map_row(src)
        if not row['name']:
            skipped += 1
            continue
        if sidos and row['_sido'] not in sidos:
            skipped += 1
            continue
        if row['latitude'] is None or row['longitude'] is None:
            print('  [좌표 없음] %s (%s %s)' % (row['name'], row['_sido'], row['sgg_name']))
        if row['sido_code'] is None:
            print('  [시도코드 미매핑] %s — %s' % (row['_sido'], row['name']))
        rows.append(row)

    rows, duped = dedupe_exact(rows)
    for row in rows:
        row.pop('_sido', None)
        row.pop('_desc', None)
        row.pop('_count', None)
    flagged = mark_suspect_coords(rows)
    print('원본 %d행 → 적재 대상 %d건 (범위 밖 제외 %d, 완전중복 제거 %d)'
          % (len(raw), len(rows), skipped, duped))
    print('좌표 의심 표시 %d건 (다른 시설·다른 주소가 동일 좌표)' % flagged)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True)
    parser.add_argument('--sido', default=None,
                        help='쉼표 구분 시도명. 생략하면 전국 전체')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--out-csv', default=None, help='정규화 결과를 CSV 로 저장(검수용)')
    args = parser.parse_args()

    sidos = None
    if args.sido and args.sido.strip():
        sidos = {normalize_sido(t) for t in args.sido.split(',') if t.strip()}

    rows = build(args.csv, sidos)

    if args.out_csv:
        with io.open(args.out_csv, 'w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(DB_FIELDS))
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k) for k in DB_FIELDS})
        print('정규화 CSV 저장: %s' % args.out_csv)

    if args.dry_run:
        for row in rows[:20]:
            print('  %-12s %-28s %-34s %s' % (row['sgg_name'], row['name'],
                                              row['addr_road'], row['open_hours']))
        return 0

    import db_mobility
    count = db_mobility.upsert_emergency_support(rows)
    db_mobility.touch_latest_sync(SOURCE)
    print('poi_emergency_support 적재 %d건' % count)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
