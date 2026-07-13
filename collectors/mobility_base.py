"""MobilityCollector — 이동편의(교통약자) 데이터 소스 어댑터 공통 베이스.

Issue #76 — GBIS / KORAIL_CONV / KOWSI_FACL / TOUR_BF_API.
이들 소스는 KOSIS 형 통계가 아니라 행(row) 단위 기준 데이터를 반환하므로
통계용 추상 메서드는 no-op 로 구현하고, 각 어댑터는 ``collect() -> list[dict]``
를 노출한다. 적재는 db_mobility.py, 오케스트레이션은 mobility_pipeline.py 담당.
"""
from __future__ import annotations

import os
import time
import xml.etree.ElementTree as ET
from typing import Any, List, Optional

from collectors.base import BaseCollector

DEFAULT_PAUSE_SEC = 0.15


def to_int(value) -> Optional[int]:
    """정수 변환 실패 시 None (외부 API 필드 방어)."""
    try:
        if value is None or value == '':
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


class MobilityCollector(BaseCollector):
    """이동편의 소스 공통 베이스. 서브클래스는 collect() 만 구현하면 된다."""

    EXT_SYS: str = ""
    DEFAULT_BASE_URL: str = ""

    def __init__(self, api_info: dict = None, stats_src: dict = None):
        super().__init__(api_info or {}, stats_src or {})

    # --- 설정 해석 ---------------------------------------------------------
    @property
    def base_url(self) -> str:
        url = (
            (self.api_info.get('ext_url') if self.api_info else None)
            or os.getenv(self.EXT_SYS + '_BASE_URL')
            or self.DEFAULT_BASE_URL
        )
        return str(url).rstrip('/')

    @property
    def api_key(self) -> str:
        key = (
            (self.api_info.get('auth') if self.api_info else None)
            or os.getenv(self.EXT_SYS + '_API_KEY')
            or os.getenv('DATA_GO_KR_API_KEY')
        )
        if not key:
            raise RuntimeError(
                self.EXT_SYS
                + ' API key not configured (sys_ext_api_info.auth / '
                + self.EXT_SYS + '_API_KEY / DATA_GO_KR_API_KEY)'
            )
        return key

    # --- HTTP 헬퍼 ---------------------------------------------------------
    def get_json(self, url: str) -> dict:
        resp = self.http_get(url, timeout=30, retries=2, backoff_sec=1.0)
        return resp.json()

    def get_xml(self, url: str) -> ET.Element:
        resp = self.http_get(url, timeout=30, retries=2, backoff_sec=1.0)
        return ET.fromstring(resp.text)

    @staticmethod
    def pause():
        time.sleep(DEFAULT_PAUSE_SEC)

    # --- BaseCollector 추상 메서드 (통계용 — 이동편의 소스에는 해당 없음) ---
    def fetch_meta(self, data_info: dict):
        return ''

    def fetch_latest(self, data_info: dict):
        return ''

    def fetch_data(self, data_info: dict):
        return self.collect()

    def is_retryable_error(self, response: Any) -> bool:
        status = getattr(response, 'status_code', None)
        return status in (429, 500, 502, 503, 504)

    # --- 어댑터 계약 --------------------------------------------------------
    def collect(self) -> List[dict]:
        """소스에서 전체 대상 행을 수집해 DB 컬럼명 기준 dict 목록으로 반환."""
        raise NotImplementedError
