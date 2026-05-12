"""KosisCollector — adapter wrapping the existing KOSIS function-style API.

Issue #28 — kosis_api.py refactor. The adapter inherits from BaseCollector
(``docs/design/26-multi-source-architecture.md`` §2.4 "후방호환 전략") and
isolates every KOSIS-specific concern in one place:

- URL template parsing & ``{API_AUTH_KEY}`` / ``{from}`` / ``{to}`` substitution
- Error 31 (period-too-wide) recursive year-split retry
- KOSIS JSON-vs-text content negotiation based on ``url_info['format']``

The base class stays source-agnostic — it knows nothing about KOSIS error
codes, URL templates, or authentication headers. Future adapters (e.g.
``DataGoKrCollector`` in #29) plug in next to this module without touching
``base.py``.
"""
from __future__ import annotations

import logging
from typing import Any, Union

import kosis_api  # legacy function-style module — kept for backward compat

from .base import BaseCollector

logger = logging.getLogger(__name__)


# KOSIS-specific response keys / error codes that must not leak into base.
KOSIS_ERROR_CODE_PERIOD_WIDE = "31"
KOSIS_RESPONSE_ERR_FIELD = "err"


class KosisCollector(BaseCollector):
    """Adapter exposing KOSIS as a BaseCollector implementation.

    Implementation detail: the adapter delegates to ``kosis_api`` so that the
    legacy function-style API keeps working for callers that have not yet
    migrated (``main.py``, batch scripts, etc.). When the legacy module is
    eventually retired, the helpers can be inlined into this class without
    changing the public BaseCollector contract.
    """

    EXT_SYS = "KOSIS"

    # --- Required abstract overrides ---------------------------------------

    def fetch_meta(self, data_info: dict) -> Union[dict, str]:
        """KOSIS 통계표 메타 조회.

        KOSIS-specific: URL template uses ``api_meta_url`` key with auth
        substitution. Response key/format negotiation handled inside helper.
        """
        return kosis_api.fetch_kosis_meta(self.api_info, self.stats_src, data_info)

    def fetch_latest(self, data_info: dict) -> Union[dict, str]:
        """KOSIS 최신 변경일 조회 (api_latest_chn_dt_url 템플릿)."""
        return kosis_api.fetch_kosis_latest(self.api_info, self.stats_src, data_info)

    def fetch_data(self, data_info: dict) -> Union[dict, list, str]:
        """KOSIS 통계 데이터 조회.

        KOSIS-specific retry policy: Error 31 (period-too-wide) triggers a
        recursive year-range split until a 1-year window succeeds. Handled
        internally by ``kosis_api.fetch_kosis_data_with_retry``.
        """
        return kosis_api.fetch_kosis_data(self.api_info, self.stats_src, data_info)

    def is_retryable_error(self, response: Any) -> bool:
        """Return True for KOSIS Error 31 ({err: '31'}); False otherwise.

        KOSIS-specific shape: dict with field 'err' carrying the code as a
        string. Non-dict responses (text bodies) are never retryable.
        """
        if isinstance(response, dict):
            return response.get(KOSIS_RESPONSE_ERR_FIELD) == KOSIS_ERROR_CODE_PERIOD_WIDE
        return False
