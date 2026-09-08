# -*- coding: utf-8 -*-
"""경기데이터드림 보조기기 수리 서비스센터 CSV 적재 — GG_ASSIST_REPAIR.

원천: 경기데이터드림 「경기도_시군별 보조기기 수리 서비스센터 현황」
      (제공: 경기도장애인복지종합지원센터(누림센터), 갱신 주기 연 1회, 원본 시스템 파일)
적재: poi_emergency_support (support_type='repair')

사용:
    python scripts/load_gg_assist_repair_csv.py --csv data/gg_assist_repair_20260619.csv
    python scripts/load_gg_assist_repair_csv.py --csv <파일> --dry-run
    python scripts/load_gg_assist_repair_csv.py --csv <파일> --region ""      # 전 시군 적재

원천이 OpenAPI 가 아니라 파일이고 갱신이 연 1회라, 배치 등록 없이 갱신 시 수동
재실행한다(KRNA_STN 과 같은 방식). 완료 후 sys_ext_api_info 의 latest_sync_time 을
갱신한다.

기본 적재 범위는 안양 생활권이다(`--region` 기본값). 홈페이지 칸에 'www.' 만 적힌
행이 다수라 그런 값은 빈 값으로 본다.
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

GYEONGGI_INTERNAL_SIDO = '9410000'
SOURCE = 'GG_ASSIST_REPAIR'
SUPPORT_TYPE = 'repair'

# 안양 생활권 — 안양시 + 도 단위 광역 센터 + 인접 시군
DEFAULT_REGIONS = ('안양시', '경기도', '군포시', '의왕시', '과천시', '광명시', '안산시')

HEADER = ['번호', '시군명', '기관명', '소재지지번주소', '소재지도로명주소', '소재지우편번호',
          'WGS84위도', 'WGS84경도', '전화번호정보', '홈페이지URL', '비고']

# 홈페이지 칸에 자리표시자로 들어 있는 값
PLACEHOLDER_URLS = {'www.', 'www', '-', 'N/A'}
# 전화번호 칸의 자리표시자
PLACEHOLDER_TELS = {'000-000-0000', '-'}


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


def map_row(src: dict, base_dt: str) -> dict:
    """CSV 한 줄 → poi_emergency_support 컬럼."""
    tel = _clean(src.get('전화번호정보'))
    if tel in PLACEHOLDER_TELS:
        tel = None
    homepage = _clean(src.get('홈페이지URL'))
    if homepage in PLACEHOLDER_URLS:
        homepage = None
    return {
        'support_type': SUPPORT_TYPE,
        'sido_code': GYEONGGI_INTERNAL_SIDO,
        'sgg_name': _clean(src.get('시군명')),
        'name': _clean(src.get('기관명')),
        'addr_road': _clean(src.get('소재지도로명주소')),
        'addr_jibun': _clean(src.get('소재지지번주소')),
        'zip_code': _clean(src.get('소재지우편번호')),
        'latitude': _to_float(src.get('WGS84위도')),
        'longitude': _to_float(src.get('WGS84경도')),
        'tel': tel,
        'homepage': homepage,
        # 원천에 운영시간 항목이 없다. 야간 대응 가능 여부는 이 값이 채워져야 판단할
        # 수 있으므로, 확인 전까지 비워 두고 추정치를 넣지 않는다.
        'open_hours': None,
        'note': _clean(src.get('비고')),
        'source': SOURCE,
        'confidence': 'H',
        'base_dt': base_dt,
    }


def read_csv(path: str) -> list:
    """CSV 읽기 — utf-8(BOM 포함) 우선, 실패하면 cp949."""
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
    missing = [h for h in ('시군명', '기관명', 'WGS84위도', 'WGS84경도') if h not in rows[0]]
    if missing:
        raise RuntimeError('CSV 헤더가 원천과 다릅니다 — 누락: %s' % ', '.join(missing))
    return rows


def build(path: str, regions, base_dt: str) -> list:
    out, skipped = [], 0
    for src in read_csv(path):
        row = map_row(src, base_dt)
        if not row['name']:
            skipped += 1
            continue
        if regions and (row['sgg_name'] or '') not in regions:
            skipped += 1
            continue
        if row['latitude'] is None or row['longitude'] is None:
            # 좌표가 없으면 근접 조회에 쓸 수 없다 — 조용히 넣지 않고 남긴다.
            print('  [좌표 없음] %s (%s)' % (row['name'], row['sgg_name']))
        out.append(row)
    print('적재 대상 %d건 (제외 %d건)' % (len(out), skipped))
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True)
    parser.add_argument('--base-dt', default=None, help='데이터 기준일자 (기본: 오늘)')
    parser.add_argument('--region', default=None,
                        help='쉼표 구분 시군명. 빈 문자열이면 전 시군 (기본: 안양 생활권)')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    base_dt = args.base_dt or datetime.date.today().isoformat()
    if args.region is None:
        regions = set(DEFAULT_REGIONS)
    elif args.region.strip() == '':
        regions = None
    else:
        regions = {token.strip() for token in args.region.split(',') if token.strip()}

    rows = build(args.csv, regions, base_dt)
    if args.dry_run:
        for row in rows:
            print('  %-10s %-30s %s' % (row['sgg_name'], row['name'], row['addr_road']))
        return 0

    import db_mobility
    count = db_mobility.upsert_emergency_support(rows)
    db_mobility.touch_latest_sync(SOURCE)
    print('poi_emergency_support 적재 %d건' % count)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
