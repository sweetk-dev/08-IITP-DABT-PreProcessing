"""이동편의 소스 오케스트레이션 — Issue #76.

main.py 가 ext_sys 가 MOBILITY_EXT_SYS 에 속하면 이 모듈로 위임한다.
흐름: collector.collect() → ext_data/<EXT_SYS>/<YYYYMMDD>/ 원본 보존
     → (--mode db) db_mobility upsert → sys_ext_api_info.latest_sync_time 갱신.
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
        db_mobility.touch_latest_sync(ext_sys)
        logger.info('%s DB 적재 완료: %d행', ext_sys, summary['db_ok'])
    return summary
