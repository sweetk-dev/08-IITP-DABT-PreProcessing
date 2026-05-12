"""External API source collectors package.

Plugin-style architecture introduced in issue #28. Each external API source
is represented by a ``BaseCollector`` subclass. KOSIS is the first adapter;
adding a new source (e.g. data.go.kr, MDIS) is one new module here plus a
single ``sys_ext_api_info`` row — no changes to base.py.

Quick usage::

    from collectors import KosisCollector
    from db import get_api_info, get_stats_src_api_info

    api_info = get_api_info('KOSIS')
    stats_src = get_stats_src_api_info(api_info['ext_api_id'])[0]
    collector = KosisCollector(api_info=api_info, stats_src=stats_src)
    meta = collector.fetch_meta(data_info)
    data = collector.fetch_data(data_info)

Adding a new source (5-line skeleton)::

    from collectors.base import BaseCollector
    class MySourceCollector(BaseCollector):
        EXT_SYS = 'MY_SOURCE'
        def fetch_meta(self, data_info): ...
        def fetch_latest(self, data_info): ...
        def fetch_data(self, data_info): ...
        def is_retryable_error(self, response): ...

See ``docs/design/26-multi-source-architecture.md`` §2 for the full contract.
"""

from .base import BaseCollector  # noqa: F401
from .kosis import KosisCollector  # noqa: F401

__all__ = ["BaseCollector", "KosisCollector"]
