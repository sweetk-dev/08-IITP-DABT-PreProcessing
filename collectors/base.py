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

from file_utils import mask_auth_in_text


def _mask_url(url):
    """로그 출력용 URL 인증키 마스킹.

    apiKey(KOSIS) 뿐 아니라 serviceKey(공공데이터포털 계열)·KEY(경기데이터드림)도 가린다.
    이동편의 수집기는 전부 serviceKey / KEY 를 쓰므로 apiKey 만 가리면 요청 실패 로그에
    인증키가 그대로 남는다. 규칙은 file_utils.mask_auth_in_text 한 곳에서 관리한다
    (kosis_api.mask_auth_in_url 도 같은 함수를 쓴다).
    """
    return mask_auth_in_text(url)

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
                    # requests 예외 문자열에는 요청 URL(쿼리의 인증키 포함)이 그대로 들어 있다.
                    # 예: "HTTPSConnectionPool(...): Max retries exceeded with url: /x?serviceKey=..."
                    _mask_url(exc),
                )
                if attempt >= attempts:
                    # (1) 메시지에 담는 예외 문자열도 마스킹한다 — 이 RuntimeError 는 상위에서
                    #     로그·실행 요약(run_summary.log)에 그대로 기록된다.
                    # (2) "from None" 으로 원인 예외 연결을 끊는다. 연결해 두면 상위에서
                    #     exc_info=True 로 남기는 트레이스백에 원본 예외 문자열(마스킹 전 URL)이
                    #     함께 출력된다. 진단에 필요한 예외 종류는 메시지에 클래스명으로 남긴다.
                    raise RuntimeError(
                        f"{self.EXT_SYS or 'BASE'} GET exception: "
                        f"{type(exc).__name__}: {_mask_url(exc)}"
                    ) from None
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
