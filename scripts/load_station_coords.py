"""역위치 CSV -> poi_station_access_status 좌표 갱신 스크립트 — Issue #80.

사용:
    python scripts/load_station_coords.py --csv <역위치1.csv> [--csv <역위치2.csv>] [--dry-run]

전체 재수집(KORAIL_CONV) 없이 latitude/longitude 만 갱신한다.
정기 수집 경로는 collectors/korail_conv.py 가 KORAIL_LOC_CSV 환경변수로 같은
CSV 를 읽어 수집 시점에 보강하므로, 이 스크립트는 최초 1회 소급 갱신과
CSV 갱신 시 재적재용이다.

소스 CSV (cp949):
  - 국가철도공단_수도권1호선_역위치 (data.go.kr 15041300) — 석수·관악·안양·명학
  - 국가철도공단_수도권4호선_역위치 (data.go.kr 15041303) — 인덕원·평촌·범계
  헤더: 철도운영기관,선명,역명,경도,위도
  ※ 설계문서의 코레일 역위치 CSV(15127532)는 주요 여객취급역 204곳만 담고 있어
    안양 7역이 전부 빠져 있다(2026-07-30 실측) — 위 소스로 교체.
"""
from __future__ import annotations

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import db_mobility  # noqa: E402
from collectors.korail_conv import load_station_coords, norm_station_name  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description='역위치 CSV 좌표 갱신')
    parser.add_argument('--csv', action='append', required=True,
                        help='역위치 CSV 경로(복수 지정 가능)')
    parser.add_argument('--dry-run', action='store_true',
                        help='DB 갱신 없이 매칭 결과만 출력')
    args = parser.parse_args()

    coords = load_station_coords(args.csv)
    if not coords:
        print('CSV 에서 좌표를 읽지 못했습니다 — 경로·헤더(역명/위도/경도) 확인')
        sys.exit(1)
    print(f'CSV 좌표 {len(coords)}역 로드')

    stations = db_mobility.fetch_station_names()
    updates, unmatched = [], []
    for st in stations:
        pair = coords.get(norm_station_name(st['stn_name']))
        if pair:
            updates.append({'stn_cd': st['stn_cd'],
                            'latitude': pair[0], 'longitude': pair[1]})
        else:
            unmatched.append(st['stn_name'])

    print(f'DB {len(stations)}역 중 매칭 {len(updates)} / 미매칭 {len(unmatched)}')
    if args.dry_run:
        for u in updates[:20]:
            print('  갱신 예정:', u)
        sys.exit(0)

    count = db_mobility.update_station_coords(updates)
    print(f'완료 — {count}행 좌표 갱신')
    remain = [s for s in stations if s['latitude'] is None
              and norm_station_name(s['stn_name']) not in coords]
    if remain:
        print(f'좌표 미확보(NULL 유지) {len(remain)}역 — CSV 소스 추가 필요')


if __name__ == '__main__':
    main()
