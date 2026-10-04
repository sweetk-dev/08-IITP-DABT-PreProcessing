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


# ---------------------------------------------------------------------------
# Suite 4 — 통계 단위 수집 격리와 종료 코드
# ---------------------------------------------------------------------------
class CollectIsolationTests(unittest.TestCase):
    """통계 1건의 수집 예외가 배치 전체를 중단시키지 않는다. 실패가 있으면 종료 코드는 0 이 아니다."""

    STATS = [{'stat_tbl_id': 'DT_OK1', 'ext_api_id': 1},
             {'stat_tbl_id': 'DT_BAD', 'ext_api_id': 1},
             {'stat_tbl_id': 'DT_OK2', 'ext_api_id': 1}]

    @staticmethod
    def _fake_save_single_file(args):
        _api_info, stats_src, _dirs, _data_info = args
        stat_tbl_id = stats_src['stat_tbl_id']
        if stat_tbl_id == 'DT_BAD':
            try:
                raise RuntimeError('KOSIS data API 오류 응답: err=20, errMsg=필수요청변수값 누락')
            except RuntimeError as cause:
                # 실제 save_single_file 과 같은 모양: 일반 문구의 예외로 감싸 올린다
                raise RuntimeError('[DT_BAD] save_single_file - 파일 저장 실패') from cause
        return {'stat_tbl_id': stat_tbl_id}

    def test_save_all_files_continues_after_one_failure(self):
        failed = []
        with patch.object(main_module, 'save_single_file', side_effect=self._fake_save_single_file):
            saved = main_module.save_all_files({}, self.STATS, {}, {}, failed_out=failed)
        self.assertEqual(sorted(s['stat_tbl_id'] for s in saved), ['DT_OK1', 'DT_OK2'])
        self.assertEqual([f[0] for f in failed], ['DT_BAD'])
        # 요약에 남길 사유는 감싼 문구가 아니라 원인 예외의 내용이다
        self.assertIn('err=20', failed[0][1])

    def test_save_all_files_without_failed_out_still_continues(self):
        with patch.object(main_module, 'save_single_file', side_effect=self._fake_save_single_file):
            saved = main_module.save_all_files({}, self.STATS, {}, {})
        self.assertEqual(len(saved), 2)

    def _run_main(self, mode, db_result=None):
        """main() 을 외부 의존(DB·네트워크·로그 파일) 없이 실행하고 (종료코드, 요약, DB 호출 인자)를 돌려준다."""
        captured = {}
        db_calls = []

        def fake_db(saved, api_info, stats_src_list, info, collect_failed=None):
            db_calls.append({'saved': [s['stat_tbl_id'] for s in saved],
                             'collect_failed': list(collect_failed or [])})
            return db_result if db_result is not None else {
                'succeeded': [s['stat_tbl_id'] for s in saved], 'failed': []}

        class _Args:
            ext_sys = 'KOSIS'

        _Args.mode = mode
        with patch.object(main_module, 'setup_logging'), \
                patch.object(main_module, 'parse_args', return_value=_Args), \
                patch.object(main_module, 'check_required_env_and_args'), \
                patch.object(main_module, 'get_data_collection_scope', return_value='ALL'), \
                patch.object(main_module, 'get_filtered_stats_src_list',
                             return_value=({'ext_api_id': 1}, self.STATS, None)), \
                patch.object(main_module, 'prepare_data_directories', return_value={'ext_sys': 'KOSIS'}), \
                patch.object(main_module, 'get_stats_src_data_info', return_value={}), \
                patch.object(main_module, 'save_single_file', side_effect=self._fake_save_single_file), \
                patch.object(main_module, 'process_db_insertion', side_effect=fake_db), \
                patch.object(main_module, 'write_run_summary', side_effect=captured.update):
            with self.assertRaises(SystemExit) as cm:
                main_module.main()
        return cm.exception.code, captured, db_calls

    def test_file_mode_exit_code_is_2_when_one_statistic_fails(self):
        code, summary, db_calls = self._run_main('file')
        self.assertEqual(code, 2)
        self.assertEqual(summary['status'], 'PARTIAL')
        self.assertEqual((summary['targets'], summary['files_ok']), (3, 2))
        self.assertIn('collect_fail:DT_BAD(', summary['error'])
        self.assertIn('err=20', summary['error'])
        self.assertEqual(db_calls, [])

    def test_db_mode_loads_remaining_statistics_and_exits_2(self):
        code, summary, db_calls = self._run_main('db')
        self.assertEqual(code, 2)
        self.assertEqual(summary['status'], 'PARTIAL')
        # 수집에 성공한 통계는 DB 단계로 넘어가고, 실패 목록도 함께 전달된다
        self.assertEqual(sorted(db_calls[0]['saved']), ['DT_OK1', 'DT_OK2'])
        self.assertEqual([f[0] for f in db_calls[0]['collect_failed']], ['DT_BAD'])
        self.assertEqual((summary['db_ok'], summary['db_fail']), (2, 0))

    def test_exit_code_is_0_when_every_statistic_succeeds(self):
        with patch.object(CollectIsolationTests, 'STATS', [s for s in self.STATS if s['stat_tbl_id'] != 'DT_BAD']):
            code, summary, db_calls = self._run_main('db')
        self.assertEqual(code, 0)
        self.assertEqual(summary['status'], 'SUCCESS')
        self.assertIsNone(summary['error'])
        self.assertEqual(db_calls[0]['collect_failed'], [])

    def test_db_failure_reason_is_written_to_summary(self):
        db_result = {'succeeded': ['DT_OK1'],
                     'failed': [('DT_OK2', '[DT_OK2] 적재 중단(기존 데이터 유지): 응답이 빈 리스트')]}
        code, summary, _ = self._run_main('db', db_result=db_result)
        self.assertEqual(code, 2)
        self.assertEqual((summary['db_ok'], summary['db_fail']), (1, 1))
        self.assertIn('db_fail:DT_OK2(', summary['error'])
        self.assertIn('빈 리스트', summary['error'])
        self.assertIn('collect_fail:DT_BAD(', summary['error'])

    # --- 수집 전부 실패 → 치명적 오류(종료 코드 1) ---------------------------
    @staticmethod
    def _fake_save_all_fail(args):
        """모든 통계의 수집이 실패하는 상황(인증키 만료·원천 전체 장애) — 실제 예외 모양을 따른다."""
        _api_info, stats_src, _dirs, _data_info = args
        stat_tbl_id = stats_src['stat_tbl_id']
        try:
            raise RuntimeError('KOSIS API 처리 중단(meta): HTTP 503')
        except RuntimeError as cause:
            raise RuntimeError('[%s] save_single_file - 파일 저장 실패' % stat_tbl_id) from cause

    def _run_main_all_fail(self, mode):
        # 클래스 속성을 바꿔 끼우므로 staticmethod 로 감싼다(감싸지 않으면 self 가 첫 인자로 넘어간다).
        with patch.object(CollectIsolationTests, '_fake_save_single_file',
                          staticmethod(CollectIsolationTests._fake_save_all_fail)):
            return self._run_main(mode)

    def test_file_mode_exit_code_is_1_when_every_statistic_fails(self):
        code, summary, db_calls = self._run_main_all_fail('file')
        self.assertEqual(code, 1)
        self.assertEqual(summary['status'], 'ERROR')
        self.assertEqual((summary['targets'], summary['files_ok']), (3, 0))
        # 사유는 부분 완료 때와 같은 형식으로 남고, HTTP 상태 코드가 들어 있다
        self.assertIn('collect_fail:', summary['error'])
        self.assertIn('DT_OK1(', summary['error'])
        self.assertIn('HTTP 503', summary['error'])
        self.assertEqual(db_calls, [])

    def test_db_mode_exit_code_is_1_when_every_statistic_fails(self):
        code, summary, db_calls = self._run_main_all_fail('db')
        self.assertEqual(code, 1)
        self.assertEqual(summary['status'], 'ERROR')
        self.assertEqual((summary['files_ok'], summary['db_ok'], summary['db_fail']), (0, 0, 0))
        # 적재 단계에는 처리할 통계가 넘어가지 않는다(기존 데이터 유지)
        self.assertEqual(db_calls[0]['saved'], [])
        self.assertEqual(len(db_calls[0]['collect_failed']), 3)

    def test_no_targets_is_not_treated_as_total_failure(self):
        """대상이 0건이면 "전부 실패"가 아니다 — 종전처럼 정상 종료한다."""
        with patch.object(CollectIsolationTests, 'STATS', []):
            code, summary, _ = self._run_main('file')
        self.assertEqual(code, 0)
        self.assertEqual(summary['status'], 'SUCCESS')


if __name__ == '__main__':
    unittest.main()
