"""Integration tests for main.py entry point — issue #29.

Two suites:
  (1) KOSIS path parity — when ext_sys defaults to 'KOSIS' (or env unset / CLI omitted),
      the save directory tree and ext_sys propagation match the historical KOSIS layout.
  (2) Multi-source routing — when ext_sys is set to a non-KOSIS value, the generic
      pattern ``ext_data/<EXT_SYS>/<YYYYMMDD>/...`` is used and a registered collector
      class is selected via ``get_collector_class``.

These tests run without DB / network access by monkey-patching ``cwd`` and only
exercising the path-resolution + registry logic. The actual KosisCollector
adapter behavior is covered by tests/test_collectors.py (issue #28).
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import main as main_module  # noqa: E402


# ---------------------------------------------------------------------------
# Suite 1 — KOSIS path parity (commit#3 contract — backward compatibility)
# ---------------------------------------------------------------------------
class KosisPathParityTests(unittest.TestCase):
    """KOSIS 기본 동작이 v1.4.0 이전과 100% 동일한지 검증."""

    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp = tempfile.mkdtemp(prefix="kosis_parity_")
        os.chdir(self._tmp)

    def tearDown(self):
        os.chdir(self._cwd)

    def test_default_ext_sys_is_KOSIS(self):
        # CLI 미지정 + env 미지정 → 'KOSIS'
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('EXT_SYS', None)
            self.assertEqual(main_module.resolve_ext_sys(None), 'KOSIS')

    def test_kosis_save_path_matches_legacy(self):
        """KOSIS 기본 경로 = 'kosis_data/<YYYYMMDD>/{data,meta,latest}'."""
        dirs = main_module.create_data_save_directory(ext_sys='KOSIS')
        today = datetime.now().strftime('%Y%m%d')
        expected_root = os.path.join('kosis_data', today)
        self.assertEqual(dirs['root'], expected_root)
        self.assertEqual(dirs['data'], os.path.join(expected_root, 'data'))
        self.assertEqual(dirs['meta'], os.path.join(expected_root, 'meta'))
        self.assertEqual(dirs['latest'], os.path.join(expected_root, 'latest'))
        self.assertEqual(dirs['ext_sys'], 'KOSIS')
        # directories actually created
        self.assertTrue(os.path.isdir(dirs['data']))
        self.assertTrue(os.path.isdir(dirs['meta']))
        self.assertTrue(os.path.isdir(dirs['latest']))

    def test_kosis_save_path_does_not_use_generic_root(self):
        """KOSIS 가 'ext_data/' 트리를 만들지 않아야 함 (회귀 방지)."""
        main_module.create_data_save_directory(ext_sys='KOSIS')
        self.assertFalse(
            os.path.exists('ext_data'),
            "KOSIS path must NOT create ext_data/ (backward compat regression)",
        )
        self.assertTrue(os.path.exists('kosis_data'))

    def test_kosis_lowercase_normalized(self):
        """소문자 'kosis' 입력 시에도 정규화되어 레거시 경로 사용."""
        dirs = main_module.create_data_save_directory(ext_sys='kosis')
        today = datetime.now().strftime('%Y%m%d')
        self.assertEqual(dirs['root'], os.path.join('kosis_data', today))
        self.assertEqual(dirs['ext_sys'], 'KOSIS')

    def test_kosis_collector_selected_by_default(self):
        from collectors import KosisCollector
        self.assertIs(main_module.get_collector_class('KOSIS'), KosisCollector)


# ---------------------------------------------------------------------------
# Suite 2 — resolve_ext_sys priority (CLI > env > default)
# ---------------------------------------------------------------------------
class ResolveExtSysPriorityTests(unittest.TestCase):
    """우선순위: --ext-sys CLI > env EXT_SYS > 'KOSIS' default."""

    def test_cli_value_wins_over_env(self):
        with patch.dict(os.environ, {'EXT_SYS': 'DATA_GO_KR'}):
            self.assertEqual(main_module.resolve_ext_sys('MICRODATA'), 'MICRODATA')

    def test_env_used_when_cli_absent(self):
        with patch.dict(os.environ, {'EXT_SYS': 'DATA_GO_KR'}):
            self.assertEqual(main_module.resolve_ext_sys(None), 'DATA_GO_KR')

    def test_default_when_both_absent(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('EXT_SYS', None)
            self.assertEqual(main_module.resolve_ext_sys(None), 'KOSIS')

    def test_uppercase_normalization(self):
        self.assertEqual(main_module.resolve_ext_sys('kosis'), 'KOSIS')
        with patch.dict(os.environ, {'EXT_SYS': 'data_go_kr'}):
            self.assertEqual(main_module.resolve_ext_sys(None), 'DATA_GO_KR')


# ---------------------------------------------------------------------------
# Suite 3 — Multi-source routing via stub adapter (commit#5)
# ---------------------------------------------------------------------------
class MultiSourceRoutingTests(unittest.TestCase):
    """가짜 ext_sys 어댑터를 _COLLECTOR_REGISTRY 에 등록해 라우팅을 검증.

    실제 KOSIS API 호출 없이 다음을 확인한다.
      - 신규 ext_sys 등록은 모듈 한 줄 추가만으로 가능
      - 저장 경로가 'ext_data/<EXT_SYS>/<YYYYMMDD>/...' 패턴으로 분기됨
      - 어댑터 인스턴스가 BaseCollector 인터페이스를 만족
    """

    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp = tempfile.mkdtemp(prefix="multi_route_")
        os.chdir(self._tmp)
        # Stash registry to restore in tearDown
        self._original_registry = dict(main_module._COLLECTOR_REGISTRY)

    def tearDown(self):
        os.chdir(self._cwd)
        # Restore registry to avoid leaking stub into other tests
        main_module._COLLECTOR_REGISTRY.clear()
        main_module._COLLECTOR_REGISTRY.update(self._original_registry)

    def _make_stub_collector(self, ext_sys_id):
        """BaseCollector 를 실제로 상속한 더미 어댑터 생성."""
        from collectors.base import BaseCollector

        class _StubCollector(BaseCollector):
            EXT_SYS = ext_sys_id
            calls = []

            def fetch_meta(self, data_info):
                self.calls.append(('meta', data_info))
                return {'stub': 'meta', 'ext_sys': ext_sys_id}

            def fetch_latest(self, data_info):
                self.calls.append(('latest', data_info))
                return {'stub': 'latest', 'ext_sys': ext_sys_id}

            def fetch_data(self, data_info):
                self.calls.append(('data', data_info))
                return {'stub': 'data', 'ext_sys': ext_sys_id}

            def is_retryable_error(self, response):
                return False

        return _StubCollector

    def test_unregistered_ext_sys_raises(self):
        with self.assertRaises(ValueError) as cm:
            main_module.get_collector_class('NEVER_REGISTERED')
        self.assertIn('NEVER_REGISTERED', str(cm.exception))

    def test_stub_ext_sys_routes_correctly(self):
        StubCollector = self._make_stub_collector('DATA_GO_KR')
        main_module._COLLECTOR_REGISTRY['DATA_GO_KR'] = StubCollector
        cls = main_module.get_collector_class('DATA_GO_KR')
        self.assertIs(cls, StubCollector)
        # Resolved EXT_SYS preserved
        self.assertEqual(cls.EXT_SYS, 'DATA_GO_KR')

    def test_non_kosis_save_path_uses_generic_pattern(self):
        dirs = main_module.create_data_save_directory(ext_sys='DATA_GO_KR')
        today = datetime.now().strftime('%Y%m%d')
        expected_root = os.path.join('ext_data', 'DATA_GO_KR', today)
        self.assertEqual(dirs['root'], expected_root)
        self.assertEqual(dirs['data'], os.path.join(expected_root, 'data'))
        self.assertEqual(dirs['ext_sys'], 'DATA_GO_KR')
        self.assertTrue(os.path.isdir(dirs['data']))
        # legacy kosis_data must not appear for non-KOSIS sources
        self.assertFalse(os.path.exists('kosis_data'))

    def test_multiple_sources_isolated(self):
        """동시에 서로 다른 ext_sys 디렉터리가 독립적으로 만들어진다."""
        dirs_a = main_module.create_data_save_directory(ext_sys='SOURCE_A')
        dirs_b = main_module.create_data_save_directory(ext_sys='SOURCE_B')
        self.assertNotEqual(dirs_a['root'], dirs_b['root'])
        self.assertIn('SOURCE_A', dirs_a['root'])
        self.assertIn('SOURCE_B', dirs_b['root'])
        self.assertTrue(os.path.isdir(dirs_a['data']))
        self.assertTrue(os.path.isdir(dirs_b['data']))

    def test_stub_collector_implements_base_interface(self):
        """등록된 어댑터가 BaseCollector ABC 계약을 만족한다."""
        from collectors.base import BaseCollector
        StubCollector = self._make_stub_collector('MY_SOURCE')
        main_module._COLLECTOR_REGISTRY['MY_SOURCE'] = StubCollector
        cls = main_module.get_collector_class('MY_SOURCE')
        instance = cls(api_info={'auth': 'x', 'ext_url': 'https://example.com'}, stats_src={})
        self.assertIsInstance(instance, BaseCollector)
        # All four abstract methods callable
        self.assertEqual(instance.fetch_meta({'a': 1})['stub'], 'meta')
        self.assertEqual(instance.fetch_latest({})['stub'], 'latest')
        self.assertEqual(instance.fetch_data({})['stub'], 'data')
        self.assertFalse(instance.is_retryable_error({'err': '31'}))


if __name__ == '__main__':
    unittest.main()
