"""한국철도공사 편의시설정보 수집 어댑터 — 적재: poi_station_access_status.

Issue #76. B551457/convenience 의 stationFacilities(역사내) 와
weekPersonFacilities(교통약자) 를 전 페이지 수집해 stn_cd 기준으로 병합한다.
역명 필터 파라미터는 API 가 지원하지 않아(2026-07-13 실측) 전 역을 적재하고
안양 실증 대상 7역은 anyang_yn='Y' 로 표시한다.

좌표 보강 (Issue #80): convenience API 는 좌표를 제공하지 않는다.
KORAIL_LOC_CSV(콤마 구분 다중 경로)에 지정한 역위치 CSV 로 latitude/longitude
를 병합 시점에 보강한다. 소스는 국가철도공단 노선별 역위치
(data.go.kr 15041300 수도권1호선 / 15041303 수도권4호선 — 안양 7역 전부 포함).
※ 설계문서에 있던 코레일 역위치 CSV(15127532)는 주요 여객취급역 204곳만 담고
있어 안양 7역(광역전철)이 전부 빠져 있다(2026-07-30 실측) — 소스를 교체함.
"""
from __future__ import annotations

import csv
import datetime
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

from collectors.mobility_base import MobilityCollector, to_int

logger = logging.getLogger(__name__)

ANYANG_STATIONS = ('석수', '관악', '안양', '명학', '인덕원', '평촌', '범계')

# 역위치 CSV 헤더 별칭 — 국가철도공단 노선별(철도운영기관,선명,역명,경도,위도) 과
# 코레일 주요역(지역본부,역명,위도,경도,출입구 개수) 두 형식을 모두 흡수한다.
_COORD_ALIASES = {
    'name': ('역명', 'stn_name', 'station_name'),
    'lat': ('위도', 'latitude', 'lat'),
    'lng': ('경도', 'longitude', 'lng', 'lon'),
}


def norm_station_name(name: str) -> str:
    """역명 매칭 키 정규화 — 공백 제거, 괄호 병기 제거, 말미 '역' 제거.

    '총신대입구(이수)' -> '총신대입구', '안양역' -> '안양', '안 양' -> '안양'
    """
    s = re.sub(r'\([^)]*\)', '', str(name or ''))
    s = re.sub(r'\s+', '', s)
    if len(s) > 1 and s.endswith('역'):
        s = s[:-1]
    return s


def _read_csv_rows(path: str) -> List[dict]:
    for enc in ('cp949', 'utf-8-sig'):
        try:
            with open(path, encoding=enc, newline='') as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError('korail_loc_csv', b'', 0, 1,
                             '%s: cp949/utf-8 로 해석할 수 없습니다' % path)


def load_station_coords(paths: List[str]) -> Dict[str, Tuple[float, float]]:
    """역위치 CSV(들) -> {정규화 역명: (위도, 경도)}.

    같은 역명이 여러 파일/노선에 다른 좌표로 나오면 먼저 읽힌 값을 유지한다
    (환승역은 동일 역사라 좌표 차가 무시 가능 수준 — 로그로만 남긴다).
    """
    coords: Dict[str, Tuple[float, float]] = {}
    for path in paths:
        if not path or not os.path.exists(path):
            if path:
                logger.warning('역위치 CSV 없음 — 건너뜀: %s', path)
            continue
        for record in _read_csv_rows(path):
            keys = {str(k).strip().lstrip('﻿'): k for k in record.keys() if k}
            picked = {}
            for field, aliases in _COORD_ALIASES.items():
                src = next((keys[a] for a in aliases if a in keys), None)
                picked[field] = (record.get(src) or '').strip() if src else ''
            name = norm_station_name(picked['name'])
            if not name:
                continue
            try:
                lat, lng = float(picked['lat']), float(picked['lng'])
            except ValueError:
                continue
            if name in coords:
                if abs(coords[name][0] - lat) > 0.001 or abs(coords[name][1] - lng) > 0.001:
                    logger.info('역명 중복(좌표 상이) — 최초값 유지: %s', name)
                continue
            coords[name] = (lat, lng)
    return coords


def coord_csv_paths() -> List[str]:
    """KORAIL_LOC_CSV 환경변수(콤마 구분) -> 경로 목록."""
    raw = os.getenv('KORAIL_LOC_CSV', '')
    return [p.strip() for p in raw.split(',') if p.strip()]


class KorailConvCollector(MobilityCollector):
    EXT_SYS = 'KORAIL_CONV'
    DEFAULT_BASE_URL = 'https://apis.data.go.kr/B551457/convenience'

    @property
    def page_size(self) -> int:
        return int(os.getenv('KORAIL_PAGE_SIZE', '500'))

    def _url(self, op: str, page_no: int) -> str:
        return (self.base_url + '/' + op
                + '?serviceKey=' + self.api_key
                + '&pageNo=' + str(page_no)
                + '&numOfRows=' + str(self.page_size)
                + '&dataType=JSON')

    def fetch_all(self, op: str) -> List[dict]:
        """오퍼레이션 전 페이지 수집."""
        items: List[dict] = []
        page = 1
        while True:
            data = self.get_json(self._url(op, page))
            body = ((data or {}).get('response') or {}).get('body') or {}
            chunk = (body.get('items') or {}).get('item') or []
            if isinstance(chunk, dict):
                chunk = [chunk]
            items.extend(chunk)
            total = to_int(body.get('totalCount')) or 0
            if len(items) >= total or not chunk:
                break
            page += 1
            self.pause()
        return items

    @staticmethod
    def enrich_coords(rows: List[dict],
                      coords: Dict[str, Tuple[float, float]]) -> Tuple[int, int]:
        """병합 행에 latitude/longitude 보강. 반환: (매칭 수, 미매칭 수).

        CSV 에 없는 역은 None 으로 둔다 — 적재 시 COALESCE 로 기존값(임시 보강분
        포함)을 보존하므로, 부분 커버리지 CSV 가 기존 좌표를 지우지 않는다.
        """
        hit = miss = 0
        for row in rows:
            key = norm_station_name(row.get('stn_name'))
            pair = coords.get(key)
            if pair:
                row['latitude'], row['longitude'] = pair
                hit += 1
            else:
                row.setdefault('latitude', None)
                row.setdefault('longitude', None)
                miss += 1
        return hit, miss

    @staticmethod
    def merge(station_items: List[dict], weak_items: List[dict]) -> List[dict]:
        """stationFacilities + weekPersonFacilities → stn_cd 기준 병합 행."""
        merged: Dict[str, dict] = {}
        today = datetime.date.today().isoformat()
        for it in station_items:
            cd = str(it.get('stn_cd') or '')
            if not cd:
                continue
            merged[cd] = {
                'stn_cd': cd,
                'stn_name': str(it.get('stn_nm') or ''),
                'elevator_cnt': to_int(it.get('elevt_cnt')),
                'escalator_cnt': to_int(it.get('esclt_cnt')),
                'gen_toilet_yn': it.get('gen_tolt_estnc'),
                'nursing_room_yn': it.get('nrsrm_estnc'),
                'info_center_yn': it.get('altm_lead_cntr_estnc'),
                'wheelchair_lift_cnt': None,
                'dis_slope_yn': None,
                'dis_toilet_yn': None,
                'anyang_yn': 'Y' if str(it.get('stn_nm') or '') in ANYANG_STATIONS else 'N',
                'base_dt': today,
            }
        for it in weak_items:
            cd = str(it.get('stn_cd') or '')
            if not cd:
                continue
            row = merged.setdefault(cd, {
                'stn_cd': cd,
                'stn_name': str(it.get('stn_nm') or ''),
                'elevator_cnt': None, 'escalator_cnt': None,
                'gen_toilet_yn': None, 'nursing_room_yn': None, 'info_center_yn': None,
                'wheelchair_lift_cnt': None, 'dis_slope_yn': None, 'dis_toilet_yn': None,
                'anyang_yn': 'Y' if str(it.get('stn_nm') or '') in ANYANG_STATIONS else 'N',
                'base_dt': today,
            })
            row['wheelchair_lift_cnt'] = to_int(it.get('whlch_liftt_cnt'))
            row['dis_slope_yn'] = it.get('pwdbs_slwy_estnc')
            row['dis_toilet_yn'] = it.get('pwdbs_tolt_estnc')
        return list(merged.values())

    def collect(self) -> List[dict]:
        station_items = self.fetch_all('stationFacilities')
        self.pause()
        weak_items = self.fetch_all('weekPersonFacilities')
        rows = self.merge(station_items, weak_items)
        paths = coord_csv_paths()
        coords = load_station_coords(paths) if paths else {}
        hit, miss = self.enrich_coords(rows, coords)
        if coords:
            logger.info('역위치 좌표 보강: 매칭 %d / 미매칭 %d (CSV %d곳)',
                        hit, miss, len(coords))
        elif paths:
            logger.warning('KORAIL_LOC_CSV 지정됐으나 좌표 0건 — 파일·헤더 확인 필요')
        return rows
