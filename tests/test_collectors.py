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


# -----------------------------------------------------------------------------
# KosisCollector parity tests
# -----------------------------------------------------------------------------

from unittest.mock import patch  # noqa: E402

from collectors.kosis import KosisCollector  # noqa: E402


class KosisCollectorAdapterTests(unittest.TestCase):
    """KosisCollector must delegate to legacy kosis_api functions identically.

    These tests verify the adapter does not alter behavior: each adapter
    method calls the matching legacy function and returns its result.
    """

    API_INFO = {"ext_api_id": 1, "ext_url": "https://example.test", "auth": "X"}
    STATS_SRC = {"stat_tbl_id": "T1"}
    DATA_INFO = {"collect_start_dt": "2020", "collect_end_dt": "2024"}

    def test_ext_sys_constant(self):
        c = KosisCollector(self.API_INFO, self.STATS_SRC)
        self.assertEqual(c.EXT_SYS, "KOSIS")

    def test_fetch_meta_delegates(self):
        with patch("collectors.kosis.kosis_api.fetch_kosis_meta", return_value={"ok": 1}) as m:
            c = KosisCollector(self.API_INFO, self.STATS_SRC)
            result = c.fetch_meta(self.DATA_INFO)
        m.assert_called_once_with(self.API_INFO, self.STATS_SRC, self.DATA_INFO)
        self.assertEqual(result, {"ok": 1})

    def test_fetch_latest_delegates(self):
        with patch("collectors.kosis.kosis_api.fetch_kosis_latest", return_value="2026-05-01") as m:
            c = KosisCollector(self.API_INFO, self.STATS_SRC)
            result = c.fetch_latest(self.DATA_INFO)
        m.assert_called_once_with(self.API_INFO, self.STATS_SRC, self.DATA_INFO)
        self.assertEqual(result, "2026-05-01")

    def test_fetch_data_delegates(self):
        rows = [{"y": 2020}, {"y": 2021}]
        with patch("collectors.kosis.kosis_api.fetch_kosis_data", return_value=rows) as m:
            c = KosisCollector(self.API_INFO, self.STATS_SRC)
            result = c.fetch_data(self.DATA_INFO)
        m.assert_called_once_with(self.API_INFO, self.STATS_SRC, self.DATA_INFO)
        self.assertEqual(result, rows)

    def test_is_retryable_error_matches_is_error_31(self):
        c = KosisCollector(self.API_INFO, self.STATS_SRC)
        self.assertTrue(c.is_retryable_error({"err": "31"}))
        self.assertFalse(c.is_retryable_error({"err": "0"}))
        self.assertFalse(c.is_retryable_error("plain text"))


class KosisCollectorParityTests(unittest.TestCase):
    """Adapter result must equal legacy module result for the same inputs."""

    def test_parity_fetch_meta(self):
        sentinel = {"sentinel": "meta"}
        import kosis_api as legacy
        with patch.object(legacy, "fetch_kosis_meta", return_value=sentinel):
            legacy_out = legacy.fetch_kosis_meta({}, {}, {})
            adapter_out = KosisCollector({}, {}).fetch_meta({})
        self.assertEqual(legacy_out, adapter_out)

    def test_parity_fetch_data(self):
        sentinel = [{"row": 1}]
        import kosis_api as legacy
        with patch.object(legacy, "fetch_kosis_data", return_value=sentinel):
            legacy_out = legacy.fetch_kosis_data({}, {}, {})
            adapter_out = KosisCollector({}, {}).fetch_data({})
        self.assertEqual(legacy_out, adapter_out)


# ---------------------------------------------------------------------------
# 인증키 마스킹 — apiKey / serviceKey / KEY, 요청 예외 문자열 포함
# ---------------------------------------------------------------------------
class AuthMaskingTests(unittest.TestCase):
    """http_get 의 실패 로그·예외 메시지에 인증키가 남지 않는다."""

    SECRET = 'SvcKey%2BSECRET=='

    def _collector(self):
        class _C(BaseCollector):
            EXT_SYS = 'MASKTEST'

            def fetch_meta(self, data_info):
                return ''

            def fetch_latest(self, data_info):
                return ''

            def fetch_data(self, data_info):
                return []

            def is_retryable_error(self, response):
                return False

        return _C(api_info={}, stats_src={})

    def test_mask_url_covers_service_key_and_key(self):
        from collectors.base import _mask_url
        self.assertEqual(_mask_url('https://h/p?serviceKey=abc&pageNo=1'), 'https://h/p?serviceKey=***&pageNo=1')
        self.assertEqual(_mask_url('https://h/p?KEY=abc&Type=json'), 'https://h/p?KEY=***&Type=json')
        self.assertEqual(_mask_url('https://h/p?apikey=abc'), 'https://h/p?apikey=***')
        # 이름이 key 로 끝나는 다른 파라미터는 건드리지 않는다
        self.assertEqual(_mask_url('https://h/p?monkey=1&KEY=abc'), 'https://h/p?monkey=1&KEY=***')

    def test_request_exception_message_is_masked(self):
        import logging
        import traceback
        import requests

        for param in ('serviceKey', 'KEY'):
            url = 'https://api.example/list?%s=%s&pageNo=1' % (param, self.SECRET)

            def boom(_url, timeout=None):
                raise requests.exceptions.ConnectionError(
                    "HTTPSConnectionPool(host='api.example', port=443): Max retries exceeded with url: "
                    "/list?%s=%s&pageNo=1 (Caused by NewConnectionError('refused'))" % (param, self.SECRET))

            with self.subTest(param=param):
                with patch('collectors.base.requests.get', side_effect=boom), \
                        self.assertLogs('collectors.base', level=logging.WARNING) as logs:
                    try:
                        self._collector().http_get(url, retries=0, backoff_sec=0)
                        self.fail('RuntimeError 가 올라와야 한다')
                    except RuntimeError as exc:
                        message = str(exc)
                        rendered = traceback.format_exc()
                log_text = '\n'.join(r.getMessage() for r in logs.records)
                for where, text in (('log', log_text), ('message', message), ('traceback', rendered)):
                    self.assertNotIn(self.SECRET, text, where)
                self.assertIn(param + '=***', message)
                self.assertIn('ConnectionError', message)

    def test_non_200_log_is_masked(self):
        import logging

        class _Resp:
            status_code = 500
            text = 'server error'

        url = 'https://api.example/list?serviceKey=%s&pageNo=1' % self.SECRET
        with patch('collectors.base.requests.get', return_value=_Resp()), \
                self.assertLogs('collectors.base', level=logging.WARNING) as logs:
            with self.assertRaises(RuntimeError):
                self._collector().http_get(url, retries=0, backoff_sec=0)
        self.assertNotIn(self.SECRET, '\n'.join(r.getMessage() for r in logs.records))
