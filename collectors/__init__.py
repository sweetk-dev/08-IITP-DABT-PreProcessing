"""External API source collectors package.

Introduces a plugin-style architecture (issue #28) where each external API
source is represented by a ``BaseCollector`` subclass. KOSIS is the first
adapter; future sources (e.g. data.go.kr, MDIS) add a new module here.

See ``docs/design/26-multi-source-architecture.md`` §2 for the contract.
"""

from .base import BaseCollector  # noqa: F401
