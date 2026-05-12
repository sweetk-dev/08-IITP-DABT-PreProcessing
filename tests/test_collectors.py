"""Unit tests for collectors.base.BaseCollector

Issue #28 — BaseCollector 추상 인터페이스 검증.
Verifies ABC contract: direct instantiation is blocked, required abstract
methods are enforced, common save_response works for both dict and string
payloads. KosisCollector adapter behavior is exercised separately.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors.base import BaseCollector  # noqa: E402


class BaseCollectorAbcTests(unittest.TestCase):
    """Verify the ABC contract from design #26 §2."""

    def test_cannot_instantiate_base_directly(self):
        with self.assertRaises(TypeError):
            BaseCollector(api_info={}, stats_src={})  # type: ignore[abstract]

    def test_subclass_missing_abstract_blocks_instantiation(self):
        class Incomplete(BaseCollector):
            EXT_SYS = "DUMMY"
            # fetch_meta / fetch_latest / fetch_data / is_retryable_error missing

        with self.assertRaises(TypeError):
            Incomplete(api_info={}, stats_src={})  # type: ignore[abstract]

    def test_subclass_full_can_instantiate(self):
        class Complete(BaseCollector):
            EXT_SYS = "DUMMY"
            def fetch_meta(self, data_info): return {}
            def fetch_latest(self, data_info): return {}
            def fetch_data(self, data_info): return []
            def is_retryable_error(self, response): return False

        inst = Complete(api_info={"a": 1}, stats_src={"b": 2})
        self.assertEqual(inst.EXT_SYS, "DUMMY")
        self.assertEqual(inst.api_info, {"a": 1})
        self.assertEqual(inst.stats_src, {"b": 2})


class SaveResponseTests(unittest.TestCase):
    """Common save_response() works identically across sources."""

    class _Dummy(BaseCollector):
        EXT_SYS = "DUMMY"
        def fetch_meta(self, data_info): return {}
        def fetch_latest(self, data_info): return {}
        def fetch_data(self, data_info): return []
        def is_retryable_error(self, response): return False

    def test_save_dict_payload(self):
        c = self._Dummy(api_info={}, stats_src={})
        with tempfile.TemporaryDirectory() as td:
            path = c.save_response({"x": 1, "k": "한글"}, td, "out.json")
            self.assertTrue(os.path.exists(path))
            with open(path, encoding="utf-8") as f:
                loaded = json.load(f)
            self.assertEqual(loaded, {"x": 1, "k": "한글"})

    def test_save_string_payload(self):
        c = self._Dummy(api_info={}, stats_src={})
        with tempfile.TemporaryDirectory() as td:
            path = c.save_response("hello world", td, "out.txt")
            with open(path, encoding="utf-8") as f:
                self.assertEqual(f.read(), "hello world")

    def test_save_creates_missing_directories(self):
        c = self._Dummy(api_info={}, stats_src={})
        with tempfile.TemporaryDirectory() as td:
            nested = os.path.join(td, "a", "b", "c")
            path = c.save_response([1, 2, 3], nested, "list.json")
            self.assertTrue(os.path.exists(path))


if __name__ == "__main__":
    unittest.main()
