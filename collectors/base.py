"""BaseCollector — abstract interface for external API source collectors.

Issue #28 — kosis_api.py 범용 외부 API 수집 모듈 추상화.
Spec source of truth: docs/design/26-multi-source-architecture.md §2.

Each subclass represents one ``ext_sys`` identifier and encapsulates:
- URL building / authentication header injection
- Source-specific retry/split logic
- Source-specific error detection

The base class provides common file-saving behavior so that adapters do not
duplicate filesystem code.
"""
from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Union

logger = logging.getLogger(__name__)


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
