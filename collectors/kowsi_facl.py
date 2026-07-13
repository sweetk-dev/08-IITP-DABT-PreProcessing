"""한국사회보장정보원 장애인편의시설 수집 어댑터 — 적재: poi_facility_accessibility.

Issue #76. B554287/DisabledPersonConvenientFacility (XML 전용).
- 목록: getDisConvFaclList 페이징 → 주소(lcMnad) 필터(기본 '안양시')
- 기구표: getFacInfoOpenApiJpEvalInfoList?wfcltId= — 트래픽 한도(개발계정) 고려로
  기본 OFF (KOWSI_FETCH_EVAL=ON 일 때만 필터 통과 시설에 한해 호출)

운영 유의: 전국 총건수가 크므로(약 18만) KOWSI_PAGE_SIZE 를 크게(기본 5000),
KOWSI_MAX_PAGES 가드로 트래픽 한도를 방어한다. 페이지 크기 상한이 서버에서
제한될 경우 ServerA 운영 시 조정한다.
"""
from __future__ import annotations

import datetime
import os
from typing import List, Optional
import xml.etree.ElementTree as ET

from collectors.mobility_base import MobilityCollector

EVAL_FLAG_MAP = {
    '승강기': 'elevator_yn',
    '장애인사용가능화장실': 'dis_toilet_yn',
    '장애인전용주차구역': 'dis_parking_yn',
    '주출입구 높이차이 제거': 'entrance_ramp_yn',
    '주출입구(문)': 'entrance_door_yn',
    '주출입구 접근로': 'approach_road_yn',
}


def parse_eval_flags(eval_info: Optional[str]) -> dict:
    """기구표 원문(콤마 구분) → _yn 플래그 dict. 원문 없으면 전부 None."""
    flags = {col: None for col in EVAL_FLAG_MAP.values()}
    if not eval_info:
        return flags
    for col in EVAL_FLAG_MAP.values():
        flags[col] = 'N'
    for token in str(eval_info).split(','):
        name = token.strip()
        col = EVAL_FLAG_MAP.get(name)
        if col:
            flags[col] = 'Y'
    return flags


class KowsiFaclCollector(MobilityCollector):
    EXT_SYS = 'KOWSI_FACL'
    DEFAULT_BASE_URL = 'https://apis.data.go.kr/B554287/DisabledPersonConvenientFacility'

    @property
    def addr_filter(self) -> str:
        return os.getenv('KOWSI_ADDR_FILTER', '안양시')

    @property
    def page_size(self) -> int:
        return int(os.getenv('KOWSI_PAGE_SIZE', '5000'))

    @property
    def max_pages(self) -> int:
        return int(os.getenv('KOWSI_MAX_PAGES', '80'))

    @property
    def fetch_eval(self) -> bool:
        return os.getenv('KOWSI_FETCH_EVAL', 'OFF').upper() == 'ON'

    def _list_url(self, page_no: int) -> str:
        return (self.base_url + '/getDisConvFaclList'
                + '?serviceKey=' + self.api_key
                + '&pageNo=' + str(page_no)
                + '&numOfRows=' + str(self.page_size))

    def _eval_url(self, wfclt_id: str) -> str:
        return (self.base_url + '/getFacInfoOpenApiJpEvalInfoList'
                + '?serviceKey=' + self.api_key
                + '&wfcltId=' + str(wfclt_id))

    @staticmethod
    def _text(elem: ET.Element, tag: str) -> Optional[str]:
        node = elem.find(tag)
        if node is None or node.text is None:
            return None
        return node.text.strip()

    def _parse_list_page(self, root: ET.Element) -> (int, List[dict]):
        total = int((root.findtext('totalCount') or '0').strip() or 0)
        rows = []
        today = datetime.date.today().isoformat()
        for serv in root.iter('servList'):
            addr = self._text(serv, 'lcMnad') or ''
            rows.append({
                'facl_inf_id': self._text(serv, 'faclInfId'),
                'facl_name': self._text(serv, 'faclNm') or '',
                'facl_type': self._text(serv, 'faclTyCd'),
                'addr': addr,
                'latitude': self._to_float(self._text(serv, 'faclLat')),
                'longitude': self._to_float(self._text(serv, 'faclLng')),
                'estb_date': self._text(serv, 'estbDate'),
                'eval_info_raw': None,
                'wfclt_id': self._text(serv, 'wfcltId'),
                'base_dt': today,
            })
        return total, rows

    @staticmethod
    def _to_float(value):
        try:
            if value is None or value == '':
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def collect(self) -> List[dict]:
        matched: List[dict] = []
        page = 1
        seen = 0
        while page <= self.max_pages:
            root = self.get_xml(self._list_url(page))
            total, rows = self._parse_list_page(root)
            seen += len(rows)
            for row in rows:
                if row['facl_inf_id'] and self.addr_filter in (row['addr'] or ''):
                    matched.append(row)
            if seen >= total or not rows:
                break
            page += 1
            self.pause()
        if self.fetch_eval:
            for row in matched:
                key = row.get('wfclt_id') or row.get('facl_inf_id')
                try:
                    root = self.get_xml(self._eval_url(key))
                    eval_info = root.findtext('.//evalInfo')
                except RuntimeError:
                    eval_info = None
                row['eval_info_raw'] = eval_info
                row.update(parse_eval_flags(eval_info))
                self.pause()
        else:
            for row in matched:
                row.update(parse_eval_flags(None))
        return matched
