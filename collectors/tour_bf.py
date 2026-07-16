"""한국관광공사 무장애여행(KorWithService2) 수집 어댑터 — 적재: poi_tour_bf_facility.

Issue #76. areaBasedList2(지역 목록) + detailWithTour2(무장애 편의정보) 를 결합해
기존 poi_tour_bf_facility 스키마의 _yn 플래그로 파생 매핑한다.
기본 대상: 경기(31) 안양시(17) — 2026-07-13 실측 13건.
편의정보 원문은 ext_data 파일(raw json)로 보존한다.
"""
from __future__ import annotations

import datetime
import os
from typing import List, Optional

from collectors.mobility_base import MobilityCollector

GYEONGGI_INTERNAL_SIDO = '9410000'  # sys_common_code sido_code (내부용, prefix 9)

# detailWithTour2 응답 필드 → poi_tour_bf_facility 컬럼
DETAIL_FIELD_MAP = {
    'restroom': 'toilet_yn',
    'elevator': 'elevator_yn',
    'parking': 'parking_yn',
    'exit': 'slope_yn',
    'wheelchair': 'wheelchair_rent_yn',
    'braileblock': 'tactile_map_yn',
    'audioguide': 'audio_guide_yn',
    'lactationroom': 'nursing_room_yn',
    'room': 'accessible_room_yn',
    'stroller': 'stroller_rent_yn',
}


def flag_from_text(text: Optional[str]) -> Optional[str]:
    """편의정보 서술 텍스트 → Y/N/None 휴리스틱.

    - 빈 값: None (정보 없음)
    - '없' 포함 & '있' 미포함: 'N' (예: '엘리베이터 없음')
    - 그 외: 'Y'
    """
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    if ('없' in s) and ('있' not in s):
        return 'N'
    return 'Y'


class TourBfCollector(MobilityCollector):
    EXT_SYS = 'TOUR_BF_API'
    DEFAULT_BASE_URL = 'https://apis.data.go.kr/B551011/KorWithService2'

    @property
    def area_code(self) -> str:
        return os.getenv('TOUR_AREA_CODE', '31')

    @property
    def sigungu_code(self) -> str:
        return os.getenv('TOUR_SIGUNGU_CODE', '17')

    def _common_qs(self) -> str:
        return ('serviceKey=' + self.api_key
                + '&MobileOS=ETC&MobileApp=iitp-dabt&_type=json')

    def _list_url(self, page_no: int) -> str:
        return (self.base_url + '/areaBasedList2?' + self._common_qs()
                + '&areaCode=' + self.area_code
                + '&sigunguCode=' + self.sigungu_code
                + '&numOfRows=100&pageNo=' + str(page_no))

    def _detail_url(self, content_id) -> str:
        return (self.base_url + '/detailWithTour2?' + self._common_qs()
                + '&contentId=' + str(content_id))

    @staticmethod
    def _body(data: dict) -> dict:
        return ((data or {}).get('response') or {}).get('body') or {}

    def fetch_area_list(self) -> List[dict]:
        items: List[dict] = []
        page = 1
        while True:
            body = self._body(self.get_json(self._list_url(page)))
            chunk = (body.get('items') or {}).get('item') or []
            if isinstance(chunk, dict):
                chunk = [chunk]
            items.extend(chunk)
            total = int(body.get('totalCount') or 0)
            if len(items) >= total or not chunk:
                break
            page += 1
            self.pause()
        return items

    def fetch_detail(self, content_id) -> dict:
        body = self._body(self.get_json(self._detail_url(content_id)))
        item = (body.get('items') or {}).get('item') or {}
        if isinstance(item, list):
            item = item[0] if item else {}
        return item

    @staticmethod
    def map_row(area_item: dict, detail_item: dict) -> dict:
        """areaBasedList2 + detailWithTour2 → poi_tour_bf_facility 컬럼 매핑."""
        row = {
            'sido_code': GYEONGGI_INTERNAL_SIDO,
            'fclt_name': str(area_item.get('title') or ''),
            'addr_road': area_item.get('addr1'),
            'addr_jibun': area_item.get('addr2'),
            'latitude': float(area_item['mapy']) if area_item.get('mapy') else None,
            'longitude': float(area_item['mapx']) if area_item.get('mapx') else None,
            'base_dt': datetime.date.today().isoformat(),
        }
        for src_field, col in DETAIL_FIELD_MAP.items():
            row[col] = flag_from_text(detail_item.get(src_field))
        public_transport = str(detail_item.get('publictransport') or '')
        row['subway_yn'] = 'Y' if '지하철' in public_transport else (None if not public_transport else 'N')
        row['bus_stop_yn'] = 'Y' if '버스' in public_transport else (None if not public_transport else 'N')
        return row

    def collect(self) -> List[dict]:
        rows = []
        self._raw_details = []
        for area_item in self.fetch_area_list():
            detail = self.fetch_detail(area_item.get('contentid'))
            self._raw_details.append({'area': area_item, 'detail': detail})
            rows.append(self.map_row(area_item, detail))
            self.pause()
        return rows
