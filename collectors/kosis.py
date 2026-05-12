"""KosisCollector — adapter wrapping the existing KOSIS function-style API.

Issue #28 — kosis_api.py refactor. The adapter inherits from BaseCollector
(``docs/design/26-multi-source-architecture.md`` §2.4 "후방호환 전략") and
delegates to the legacy ``kosis_api`` functions so that behavior remains
identical during the migration window.

KOSIS-specific concerns (URL templating, Error 31 split retry) stay isolated
inside this module — the base class stays source-agnostic.
"""
from __future__ import annotations

import logging
from typing import Any, Union

import kosis_api  # legacy function-style module — kept for backward compat

from .base import BaseCollector

logger = logging.getLogger(__name__)


class KosisCollector(BaseCollector):
    """Adapter exposing KOSIS as a BaseCollector implementation."""

    EXT_SYS = "KOSIS"

    # --- Required abstract overrides ---------------------------------------

    def fetch_meta(self, data_info: dict) -> Union[dict, str]:
        return kosis_api.fetch_kosis_meta(self.api_info, self.stats_src, data_info)

    def fetch_latest(self, data_info: dict) -> Union[dict, str]:
        return kosis_api.fetch_kosis_latest(self.api_info, self.stats_src, data_info)

    def fetch_data(self, data_info: dict) -> Union[dict, list, str]:
        # Delegates to existing wrapper which already encapsulates the
        # Error 31 recursive year-split retry.
        return kosis_api.fetch_kosis_data(self.api_info, self.stats_src, data_info)

    def is_retryable_error(self, response: Any) -> bool:
        return kosis_api.is_error_31(response)
