"""한국철도공사 편의시설정보 수집 어댑터 — 적재: poi_station_access_status.

Issue #76. B551457/convenience 의 stationFacilities(역사내) 와
weekPersonFacilities(교통약자) 를 전 페이지 수집해 stn_cd 기준으로 병합한다.
역명 필터 파라미터는 API 가 지원하지 않아(2026-07-13 실측) 전 역을 적재하고
안양 실증 대상 7역은 anyang_yn='Y' 로 표시한다.
"""
from __future__ import annotations

import datetime
import os
from typing import Dict, List

from collectors.mobility_base import MobilityCollector, to_int

ANYANG_STATIONS = ('석수', '관악', '안양', '명학', '인덕원', '평촌', '범계')


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
        return self.merge(station_items, weak_items)
