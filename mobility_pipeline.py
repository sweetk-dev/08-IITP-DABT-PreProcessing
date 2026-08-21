"""이동편의 소스 오케스트레이션 — Issue #76.

main.py 가 ext_sys 가 MOBILITY_EXT_SYS 에 속하면 이 모듈로 위임한다.
흐름: collector.collect() → ext_data/<EXT_SYS>/<YYYYMMDD>/ 원본 보존
     → (--mode db) db_mobility upsert → sys_ext_api_info.latest_sync_time 갱신.

GBIS 는 노선 메타에 이어 경유정류소(정류장 마스터 + 노선-정류장 관계)까지
같은 실행에서 수집한다 (Issue #85). GBIS_COLLECT_STATIONS=false 로 끌 수 있다.
"""
from __future__ import annotations

import datetime
import logging
import os

import db_mobility
from db import get_api_info
from collectors.gbis import GbisCollector
from collectors.korail_conv import KorailConvCollector
from collectors.kowsi_facl import KowsiFaclCollector
from collectors.tour_bf import TourBfCollector

logger = logging.getLogger(__name__)

GENERIC_EXT_DATA_ROOT = 'ext_data'

MOBILITY_COLLECTORS = {
    'GBIS': GbisCollector,
    'KORAIL_CONV': KorailConvCollector,
    'KOWSI_FACL': KowsiFaclCollector,
    'TOUR_BF_API': TourBfCollector,
}

MOBILITY_EXT_SYS = tuple(MOBILITY_COLLECTORS)

_UPSERT_DISPATCH = {
    'GBIS': db_mobility.upsert_bus_routes,
    'KORAIL_CONV': db_mobility.upsert_station_access,
    'KOWSI_FACL': db_mobility.upsert_facilities,
    'TOUR_BF_API': db_mobility.upsert_tour_bf,
}


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in ('0', 'false', 'no', 'off')


def _collect_gbis_stations(collector, route_rows, save_dir, mode: str, summary: dict) -> None:
    """GBIS 경유정류소 수집·보존·적재 (Issue #85).

    노선 메타 수집 결과의 route_id 를 그대로 재사용하므로 노선 열거를 다시 하지 않는다.
    정류장 수집이 실패해도 노선 메타 적재 결과는 유지한다.
    """
    if not _env_flag('GBIS_COLLECT_STATIONS'):
        logger.info('GBIS_COLLECT_STATIONS=false — 경유정류소 수집 생략')
        return
    route_ids = [r['route_id'] for r in route_rows if r.get('route_id') is not None]
    if not route_ids:
        logger.warning('GBIS 대상 노선이 없어 경유정류소 수집을 생략합니다')
        return

    stations, links = collector.collect_stations(route_ids)
    logger.info('GBIS 경유정류소 수집 완료: 정류장 %d개 / 노선-정류장 %d행',
                len(stations), len(links))
    collector.save_response(stations, save_dir, 'stations.json')
    collector.save_response(links, save_dir, 'route_stations.json')
    summary['stations'] = len(stations)
    summary['route_stations'] = len(links)

    if mode == 'db':
        summary['stations_db_ok'] = db_mobility.upsert_bus_stations(stations)
        summary['route_stations_db_ok'] = db_mobility.upsert_bus_route_stations(links)
        logger.info('GBIS 경유정류소 DB 적재 완료: 정류장 %d행 / 노선-정류장 %d행',
                    summary['stations_db_ok'], summary['route_stations_db_ok'])


def run_mobility(ext_sys: str, mode: str) -> dict:
    """이동편의 소스 1건 수집·적재. 반환: 요약 dict (targets/files_ok/db_ok/db_fail)."""
    ext_sys = ext_sys.upper()
    collector_cls = MOBILITY_COLLECTORS[ext_sys]
    api_info = get_api_info(ext_sys) or {}
    if not api_info:
        logger.warning('sys_ext_api_info 에 %s 행이 없습니다 — .env 설정으로 진행', ext_sys)
    collector = collector_cls(api_info=api_info, stats_src={})

    rows = collector.collect()
    logger.info('%s 수집 완료: %d행', ext_sys, len(rows))

    today = datetime.datetime.now().strftime('%Y%m%d')
    save_dir = os.path.join(GENERIC_EXT_DATA_ROOT, ext_sys, today)
    collector.save_response(rows, save_dir, 'rows.json')
    raw_details = getattr(collector, '_raw_details', None)
    if raw_details:
        collector.save_response(raw_details, save_dir, 'raw_details.json')

    summary = {'targets': len(rows), 'files_ok': len(rows), 'db_ok': 0, 'db_fail': 0}
    if mode == 'db':
        upsert = _UPSERT_DISPATCH[ext_sys]
        summary['db_ok'] = upsert(rows)
        logger.info('%s DB 적재 완료: %d행', ext_sys, summary['db_ok'])

    if ext_sys == 'GBIS':
        _collect_gbis_stations(collector, rows, save_dir, mode, summary)

    if mode == 'db':
        db_mobility.touch_latest_sync(ext_sys)
    return summary
