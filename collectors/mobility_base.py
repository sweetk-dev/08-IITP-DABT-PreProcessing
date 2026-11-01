"""MobilityCollector — 이동편의(교통약자) 데이터 소스 어댑터 공통 베이스.

Issue #76 — GBIS / KORAIL_CONV / KOWSI_FACL / TOUR_BF_API.
이들 소스는 KOSIS 형 통계가 아니라 행(row) 단위 기준 데이터를 반환하므로
통계용 추상 메서드는 no-op 로 구현하고, 각 어댑터는 ``collect() -> list[dict]``
를 노출한다. 적재는 db_mobility.py, 오케스트레이션은 mobility_pipeline.py 담당.
"""
from __future__ import annotations

import logging
import os
import time
import xml.etree.ElementTree as ET
from typing import Any, List, Optional

from collectors.base import BaseCollector, _mask_url
from file_utils import mask_auth_in_text

logger = logging.getLogger(__name__)

DEFAULT_PAUSE_SEC = 0.15

# 원천이 200 응답에 중간이 끊기거나 깨진 본문을 실어 보낼 때가 있다(2026-11 경기데이터드림 실측 —
# 같은 주소를 곧바로 다시 받으면 정상). 본문이 JSON 으로 읽히지 않으면 이 횟수만큼 다시 받는다.
JSON_REFETCH = 2
JSON_REFETCH_WAIT_SEC = 2.0


def to_int(value) -> Optional[int]:
    """정수 변환 실패 시 None (외부 API 필드 방어)."""
    try:
        if value is None or value == '':
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value) -> Optional[float]:
    """실수 변환 실패 시 None (좌표 등 외부 API 필드 방어)."""
    try:
        if value is None or value == '':
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def to_yn(value) -> Optional[str]:
    """외부 API 의 여부 필드를 'Y'/'N' 한 글자로 정규화. 판별 불가 시 None."""
    if value is None:
        return None
    token = str(value).strip().upper()
    if token in ('Y', 'YES', 'TRUE', '1'):
        return 'Y'
    if token in ('N', 'NO', 'FALSE', '0'):
        return 'N'
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
        """GET → JSON. 본문이 JSON 으로 읽히지 않으면(끊김·깨짐) JSON_REFETCH 번 다시 받는다.

        끝내 읽히지 않으면 '원천 응답 JSON 깨짐' 문구로 RuntimeError — 운영 알림이 이 문구로
        조치 안내(다시 실행)를 고른다.
        """
        attempts = JSON_REFETCH + 1
        last_exc: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            resp = self.http_get(url, timeout=30, retries=2, backoff_sec=1.0)
            try:
                return resp.json()
            except ValueError as exc:   # requests·json 의 JSONDecodeError 는 ValueError 하위
                head = (getattr(resp, 'text', '') or '').lstrip()[:120]
                if not head.startswith(('{', '[')):
                    # JSON 이 아닌 본문(XML 오류 안내·HTML 등)은 끊긴 응답이 아니다 — 다시 받지 않는다
                    raise RuntimeError('%s: 원천 응답이 JSON 이 아님 — 앞부분: %s'
                                       % (self.EXT_SYS or 'MOBILITY', mask_auth_in_text(head))) from exc
                last_exc = exc
                logger.warning('%s GET %s 응답 본문이 JSON 으로 읽히지 않음(%d/%d, %d바이트): %s',
                               self.EXT_SYS or 'MOBILITY', _mask_url(url), attempt, attempts,
                               len(getattr(resp, 'content', b'') or b''), exc)
                if attempt < attempts:
                    time.sleep(JSON_REFETCH_WAIT_SEC)
        raise RuntimeError('%s: 원천 응답 JSON 깨짐 — %d번 받아도 읽을 수 없음(원천 일시 오류 추정): %s'
                           % (self.EXT_SYS or 'MOBILITY', attempts, last_exc))

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
