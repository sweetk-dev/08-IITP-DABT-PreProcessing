"""BaseCollector — abstract interface for external API source collectors.

Issue #28 — kosis_api.py 범용 외부 API 수집 모듈 추상화.
Spec source of truth: docs/design/26-multi-source-architecture.md §2.

Each subclass represents one ``ext_sys`` identifier and encapsulates:
- URL building / authentication header injection
- Source-specific retry/split logic
- Source-specific error detection

The base class provides common file-saving behavior and a shared HTTP GET
helper with retry/timeout so that adapters do not duplicate that code.
"""
from __future__ import annotations

import json
import logging
import re
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Optional, Union

import requests


def _mask_url(url):
    """로그 출력용 URL 인증키 마스킹"""
    if not url:
        return url
    return re.sub(r"(apiKey=)[^&]+", r"\1***", str(url), flags=re.IGNORECASE)

logger = logging.getLogger(__name__)

# Default HTTP behavior (overridable per-call). Conservative values that
# preserve current KOSIS behavior (single try, no timeout) when callers do
# not opt in — see KosisCollector usage in collectors/kosis.py.
DEFAULT_TIMEOUT_SEC = 30
DEFAULT_RETRY_COUNT = 0
DEFAULT_RETRY_BACKOFF_SEC = 1.0


class BaseCollector(ABC):
    """Common interface for external API source collectors.

    Concrete adapters set the ``EXT_SYS`` class attribute to the source
    identifier stored in ``sys_ext_api_info.ext_sys`` (e.g. ``'KOSIS'``,
    ``'DATA_GO_KR'``).
    """

    EXT_SYS: str = ""

    def __init__(self, api_info: dict, stats_src: dict):
        """Instantiate a collector bound to a specific source row.

        Parameters
        ----------
        api_info:
            Single ``dict`` returned by ``db.get_api_info(ext_sys)``.
        stats_src:
            One row of ``db.get_stats_src_api_info(ext_api_id)``.
        """
        self.api_info = api_info
        self.stats_src = stats_src

    # --- Abstract methods (subclasses must implement) ----------------------

    @abstractmethod
    def fetch_meta(self, data_info: dict) -> Union[dict, str]:
        """Fetch the statistical-table metadata for ``data_info``."""

    @abstractmethod
    def fetch_latest(self, data_info: dict) -> Union[dict, str]:
        """Fetch the most-recent-change timestamp for ``data_info``."""

    @abstractmethod
    def fetch_data(self, data_info: dict) -> Union[dict, list, str]:
        """Fetch the statistical data, applying source-specific retry rules."""

    @abstractmethod
    def is_retryable_error(self, response: Any) -> bool:
        """Return ``True`` when ``response`` indicates a retryable error."""

    # --- Common HTTP utility ----------------------------------------------

    def http_get(
        self,
        url: str,
        timeout: Optional[int] = None,
        retries: Optional[int] = None,
        backoff_sec: Optional[float] = None,
    ) -> requests.Response:
        """Issue a GET request with optional retry/timeout.

        ``retries=0`` disables the retry loop and produces a single request
        identical to plain ``requests.get(url)`` — used to preserve historic
        KOSIS behavior for callers that opt out.

        On HTTP failure the method raises ``RuntimeError`` after exhausting
        retries; on success it returns the ``Response`` untouched so that
        adapter code can inspect ``status_code`` / ``json()`` / ``text``.
        """
        timeout = timeout if timeout is not None else DEFAULT_TIMEOUT_SEC
        retries = retries if retries is not None else DEFAULT_RETRY_COUNT
        backoff_sec = backoff_sec if backoff_sec is not None else DEFAULT_RETRY_BACKOFF_SEC

        last_exc: Optional[Exception] = None
        attempts = retries + 1
        for attempt in range(1, attempts + 1):
            try:
                resp = requests.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                logger.warning(
                    "%s GET %s failed status=%s body=%s",
                    self.EXT_SYS or "BASE",
                    _mask_url(url),
                    resp.status_code,
                    resp.text[:200],
                )
                if attempt >= attempts:
                    raise RuntimeError(
                        f"{self.EXT_SYS or 'BASE'} GET failed status={resp.status_code}"
                    )
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "%s GET %s exception attempt=%d/%d: %s",
                    self.EXT_SYS or "BASE",
                    _mask_url(url),
                    attempt,
                    attempts,
                    exc,
                )
                if attempt >= attempts:
                    raise RuntimeError(
                        f"{self.EXT_SYS or 'BASE'} GET exception: {exc}"
                    ) from last_exc
            time.sleep(backoff_sec)
        # Unreachable, but keep mypy happy.
        raise RuntimeError(f"{self.EXT_SYS or 'BASE'} GET unreachable state")

    # --- Common utilities (shared across sources) --------------------------

    def save_response(self, response: Any, save_dir: str, filename: str) -> str:
        """Persist ``response`` under ``save_dir/filename`` and return the path.

        Behaves identically across sources; adapters should not override.
        ``response`` may be a ``dict``/``list`` (encoded as JSON) or a ``str``.
        """
        os.makedirs(save_dir, exist_ok=True)
        path = os.path.join(save_dir, filename)
        if isinstance(response, (dict, list)):
            with open(path, "w", encoding="utf-8") as f:
                json.dump(response, f, ensure_ascii=False, indent=2)
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(str(response))
        logger.info("%s response saved: %s", self.EXT_SYS or "BASE", path)
        return path

    # --- Repr helper -------------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<{self.__class__.__name__} ext_sys={self.EXT_SYS!r}>"
