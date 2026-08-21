"""GBIS(경기버스정보) 노선·정류장 수집 어댑터.

적재 대상 테이블 3종:
- tran_bus_route_info    노선·배차 메타 (Issue #76)
- tran_bus_station_info  경유정류소 마스터 (Issue #85)
- tran_bus_route_station 노선-정류장 경유 관계 (Issue #85)

안양 연관 노선(regionName 필터, 기본 '안양')의 노선번호/유형/기점종점/운수사/
배차간격/첫차막차를 수집하고, 같은 노선 목록으로 경유정류소를 이어서 수집한다.
실시간 위치(lowPlate 포함)는 수집·저장하지 않는다 — 서비스가 직접 호출.

노선 열거: getBusRouteListv2 는 키워드 검색만 지원하므로 숫자 0~9 스캔으로
경기도 전 노선을 열거한 뒤 regionName 으로 필터한다(2026-07-13 실측 검증).

경유정류소: getBusRouteStationListv2 는 노선ID 단위 조회만 지원하므로 대상 노선을
순회한다. 여러 노선이 같은 정류장을 경유하므로 정류장은 stationId 로 중복 제거한다.
좌표는 WGS84 로 제공되며 x=경도, y=위도 이다(2026-08-21 실측 검증).
"""
from __future__ import annotations

import datetime
import os
from typing import List, Tuple

from collectors.mobility_base import MobilityCollector, to_float, to_int, to_yn


class GbisCollector(MobilityCollector):
    EXT_SYS = 'GBIS'
    DEFAULT_BASE_URL = 'https://apis.data.go.kr/6410000'

    KEYWORDS = '0123456789'

    @property
    def region_filter(self) -> str:
        return os.getenv('GBIS_REGION_FILTER', '안양')

    def _route_list_url(self, keyword: str) -> str:
        return (self.base_url + '/busrouteservice/v2/getBusRouteListv2'
                + '?serviceKey=' + self.api_key
                + '&keyword=' + str(keyword) + '&format=json')

    def _route_info_url(self, route_id) -> str:
        return (self.base_url + '/busrouteservice/v2/getBusRouteInfoItemv2'
                + '?serviceKey=' + self.api_key
                + '&routeId=' + str(route_id) + '&format=json')

    def _route_station_url(self, route_id) -> str:
        return (self.base_url + '/busrouteservice/v2/getBusRouteStationListv2'
                + '?serviceKey=' + self.api_key
                + '&routeId=' + str(route_id) + '&format=json')

    @staticmethod
    def _msg_body(data: dict) -> dict:
        return ((data or {}).get('response') or {}).get('msgBody') or {}

    def enumerate_routes(self) -> dict:
        """숫자 키워드 스캔으로 노선 전수 열거 → {routeId: 요약행}."""
        routes = {}
        for kw in self.KEYWORDS:
            body = self._msg_body(self.get_json(self._route_list_url(kw)))
            lst = body.get('busRouteList') or []
            if isinstance(lst, dict):
                lst = [lst]
            for item in lst:
                rid = item.get('routeId')
                if rid is not None:
                    routes[rid] = item
            self.pause()
        return routes

    def collect(self) -> List[dict]:
        routes = self.enumerate_routes()
        region = self.region_filter
        targets = [r for r in routes.values() if region in str(r.get('regionName') or '')]
        rows = []
        for r in targets:
            body = self._msg_body(self.get_json(self._route_info_url(r['routeId'])))
            item = body.get('busRouteInfoItem') or {}
            if item:
                rows.append(self.map_route(item))
            self.pause()
        return rows

    @staticmethod
    def map_route(item: dict) -> dict:
        """GBIS busRouteInfoItem → tran_bus_route_info 컬럼 매핑."""
        return {
            'route_id': to_int(item.get('routeId')),
            'route_name': str(item.get('routeName') or ''),
            'route_type_cd': to_int(item.get('routeTypeCd')),
            'route_type_name': item.get('routeTypeName'),
            'region_name': item.get('regionName'),
            'admin_name': item.get('adminName'),
            'start_station_id': to_int(item.get('startStationId')),
            'start_station_name': item.get('startStationName'),
            'end_station_id': to_int(item.get('endStationId')),
            'end_station_name': item.get('endStationName'),
            'company_name': item.get('companyName'),
            'company_tel': item.get('companyTel'),
            'peek_alloc': to_int(item.get('peekAlloc')),
            'npeek_alloc': to_int(item.get('nPeekAlloc')),
            'sat_peek_alloc': to_int(item.get('satPeekAlloc')),
            'sat_npeek_alloc': to_int(item.get('satNPeekAlloc')),
            'sun_peek_alloc': to_int(item.get('sunPeekAlloc')),
            'sun_npeek_alloc': to_int(item.get('sunNPeekAlloc')),
            'we_peek_alloc': to_int(item.get('wePeekAlloc')),
            'we_npeek_alloc': to_int(item.get('weNPeekAlloc')),
            'up_first_time': item.get('upFirstTime'),
            'up_last_time': item.get('upLastTime'),
            'down_first_time': item.get('downFirstTime'),
            'down_last_time': item.get('downLastTime'),
            'base_dt': datetime.date.today().isoformat(),
        }

    def collect_stations(self, route_ids: List) -> Tuple[List[dict], List[dict]]:
        """대상 노선의 경유정류소 수집.

        반환: ``(정류장 목록, 노선-정류장 목록)``.
        정류장은 stationId 로 중복 제거하며, 순번(stationSeq)이 없는 행은
        노선-정류장 자연키를 구성할 수 없으므로 관계에서 제외한다.
        """
        stations = {}
        links = []
        for route_id in route_ids:
            body = self._msg_body(self.get_json(self._route_station_url(route_id)))
            items = body.get('busRouteStationList') or []
            if isinstance(items, dict):
                items = [items]
            for item in items:
                station = self.map_station(item)
                if station['station_id'] is None:
                    continue
                stations[station['station_id']] = station
                link = self.map_route_station(route_id, item)
                if link['station_seq'] is not None:
                    links.append(link)
            self.pause()
        return list(stations.values()), links

    @staticmethod
    def map_station(item: dict) -> dict:
        """GBIS busRouteStationList 항목 → tran_bus_station_info 컬럼 매핑."""
        return {
            'station_id': to_int(item.get('stationId')),
            # mobileNo 는 응답에 선행 공백이 붙어 오므로 trim 후 저장한다.
            'mobile_no': (str(item.get('mobileNo')).strip() or None) if item.get('mobileNo') is not None else None,
            'station_name': str(item.get('stationName') or ''),
            'region_name': item.get('regionName'),
            'admin_name': item.get('adminName'),
            'latitude': to_float(item.get('y')),
            'longitude': to_float(item.get('x')),
            'center_yn': to_yn(item.get('centerYn')),
            'district_cd': to_int(item.get('districtCd')),
            'base_dt': datetime.date.today().isoformat(),
        }

    @staticmethod
    def map_route_station(route_id, item: dict) -> dict:
        """GBIS busRouteStationList 항목 → tran_bus_route_station 컬럼 매핑."""
        return {
            'route_id': to_int(route_id),
            'station_id': to_int(item.get('stationId')),
            'station_seq': to_int(item.get('stationSeq')),
            'turn_seq': to_int(item.get('turnSeq')),
            'turn_yn': to_yn(item.get('turnYn')),
            'base_dt': datetime.date.today().isoformat(),
        }
