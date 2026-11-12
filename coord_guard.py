"""적재 전 좌표 보정 — 국내 범위 밖 좌표를 고치거나 비운다 (v1.25.0).

이동편의 소스(mobility_pipeline)가 원천 응답을 ext_data 에 보존한 뒤, DB 에 넣기 전에 행마다 본다.

    1. 위도·경도가 국내 범위(위도 33~39, 경도 124~132) 안이면 그대로 둔다.
    2. 0,0 이나 한쪽만 있는 값은 쓸 수 없는 좌표로 보고 아래 3~5 를 거친다(둘 다 비어 있으면 그대로 둔다).
    3. 위도·경도를 맞바꾸면 범위 안에 들어오면 맞바꾼다.
    4. 그래도 범위 밖이면 행의 주소로 좌표를 다시 찾는다(카카오 로컬 주소 검색, KAKAO_REST_API_KEY 가 있을 때).
    5. 찾지 못하면 위도·경도를 비운다(NULL) — 틀린 위치를 지도·경로 안내에 내보내지 않기 위해서다.

원천이 준 값은 ext_data/<EXT_SYS>/<날짜>/rows.json 에 그대로 남는다. 보정 건수는 실행 요약(coord_guard)에 남긴다.
COORD_GUARD=false 로 끌 수 있다.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

LAT_RANGE = (33.0, 39.0)
LON_RANGE = (124.0, 132.0)
ADDR_KEYS = ('addr_road', 'addr', 'address', 'addr_jibun')
NAME_KEYS = ('facl_name', 'fclt_name', 'toilet_name', 'station_name', 'name', 'title')

Geocoder = Callable[[str], Optional[Tuple[float, float]]]


def _num(value) -> Optional[float]:
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def in_korea(lat: Optional[float], lon: Optional[float]) -> bool:
    return (lat is not None and lon is not None
            and LAT_RANGE[0] <= lat <= LAT_RANGE[1] and LON_RANGE[0] <= lon <= LON_RANGE[1])


def _addr_of(row: dict) -> str:
    for k in ADDR_KEYS:
        v = row.get(k)
        if v and str(v).strip():
            return str(v)
    return ''


def clean_address(addr: str) -> str:
    """주소 검색에 넣을 형태 — 괄호 병기·쉼표 뒤 상세(층·호·건물명)를 뗀다."""
    text = re.sub(r'\([^)]*\)', ' ', addr or '')
    text = text.split(',')[0]
    return re.sub(r'\s+', ' ', text).strip()


def fix_row(row: dict, geocoder: Optional[Geocoder] = None) -> str:
    """행 하나를 보고 고친다. 반환: ok / none / swapped / geocoded / nulled."""
    if 'latitude' not in row and 'longitude' not in row:
        return 'none'
    lat, lon = _num(row.get('latitude')), _num(row.get('longitude'))
    if lat is None and lon is None:
        return 'none'
    if in_korea(lat, lon):
        return 'ok'
    if in_korea(lon, lat):
        row['latitude'], row['longitude'] = lon, lat
        return 'swapped'
    addr = clean_address(_addr_of(row))
    if geocoder and addr:
        hit = geocoder(addr)
        if hit and in_korea(*hit):
            row['latitude'], row['longitude'] = hit
            return 'geocoded'
    row['latitude'], row['longitude'] = None, None
    return 'nulled'


def guard_rows(rows: Iterable[dict], geocoder: Optional[Geocoder] = None, label: str = '') -> dict:
    """행 목록을 제자리에서 고치고 건수를 돌려준다. 고친 행은 이름·원래 좌표를 examples 에 몇 건 남긴다."""
    stats = {'ok': 0, 'none': 0, 'swapped': 0, 'geocoded': 0, 'nulled': 0, 'examples': []}
    for row in rows:
        before = (row.get('latitude'), row.get('longitude'))
        result = fix_row(row, geocoder)
        stats[result] += 1
        if result in ('swapped', 'geocoded', 'nulled') and len(stats['examples']) < 10:
            name = next((row.get(k) for k in NAME_KEYS if row.get(k)), '')
            stats['examples'].append({'name': name, 'before': before, 'after': (row.get('latitude'), row.get('longitude')),
                                      'result': result})
    fixed = stats['swapped'] + stats['geocoded'] + stats['nulled']
    if fixed:
        logger.warning('%s 좌표 보정: 맞바꿈 %d · 주소로 다시 찾음 %d · 비움 %d (정상 %d)',
                       label, stats['swapped'], stats['geocoded'], stats['nulled'], stats['ok'])
    return stats


class KakaoGeocoder:
    """카카오 로컬 주소 검색. 같은 주소는 한 번만 묻는다. 키·쿼터 오류가 나면 이번 실행에서는 더 묻지 않는다."""

    URL = 'https://dapi.kakao.com/v2/local/search/address.json'

    def __init__(self, key: str, timeout: float = 10.0, delay: float = 0.12):
        self._key, self._timeout, self._delay = key, timeout, delay
        self._cache: dict = {}
        self.disabled = False
        self.calls = 0

    def _ask(self, query: str) -> Optional[Tuple[float, float]]:
        url = self.URL + '?' + urllib.parse.urlencode({'query': query, 'size': 1})
        req = urllib.request.Request(url, headers={'Authorization': 'KakaoAK ' + self._key})
        self.calls += 1
        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            docs = json.load(resp).get('documents') or []
        time.sleep(self._delay)
        if not docs:
            return None
        return float(docs[0]['y']), float(docs[0]['x'])

    def __call__(self, addr: str) -> Optional[Tuple[float, float]]:
        if self.disabled or not addr:
            return None
        if addr in self._cache:
            return self._cache[addr]
        hit = None
        try:
            hit = self._ask(addr)
            if hit is None:
                # 끝의 상세(동·층·호)를 한 단어씩 떼며 두 번 더 찾아본다
                parts = addr.split()
                for n in (1, 2):
                    if len(parts) - n < 3:
                        break
                    hit = self._ask(' '.join(parts[:-n]))
                    if hit:
                        break
        except urllib.error.HTTPError as ex:
            if ex.code in (401, 403, 429):
                logger.warning('카카오 주소 검색 HTTP %d — 이번 실행에서는 주소로 다시 찾지 않습니다', ex.code)
                self.disabled = True
            else:
                logger.warning('카카오 주소 검색 HTTP %d (%s)', ex.code, addr)
        except Exception as ex:  # 네트워크 오류는 그 행만 비우고 넘어간다
            logger.warning('카카오 주소 검색 실패 (%s): %s', addr, ex)
        self._cache[addr] = hit
        return hit


def enabled() -> bool:
    raw = os.getenv('COORD_GUARD')
    return raw is None or str(raw).strip().lower() not in ('0', 'false', 'no', 'off')


def geocoder_from_env() -> Optional[KakaoGeocoder]:
    key = (os.getenv('KAKAO_REST_API_KEY') or '').strip()
    return KakaoGeocoder(key) if key else None


def guard(rows: List[dict], label: str = '', geocoder: Optional[Geocoder] = None) -> Optional[dict]:
    """파이프라인용 진입점 — 꺼져 있으면 None, 아니면 건수 요약."""
    if not enabled():
        return None
    return guard_rows(rows, geocoder if geocoder is not None else geocoder_from_env(), label)
