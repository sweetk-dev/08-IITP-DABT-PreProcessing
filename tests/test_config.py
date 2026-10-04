"""config.load_target_src_tbl_id_list 단위 테스트 — PARTIAL 모드 대상 목록 파싱.

일반적인 .env 는 첫머리가 ``DB_URL=...`` 처럼 섹션 없는 키=값이라 configparser 가 실패하고
줄 단위 파싱으로 넘어간다. 이 경로에서 (1) 비밀값이 로그에 남지 않고 (2) 대상 줄이
configparser 경로와 같은 규칙(쉼표 뒤 시작연도)으로 해석되는지 확인한다.
"""
from __future__ import annotations

import logging
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config  # noqa: E402

DB_LINE = 'DB_URL=postgresql://appuser:S3cretPw@db.example:5432/appdb'


class LoadTargetListTests(unittest.TestCase):

    def _write(self, body):
        handle = tempfile.NamedTemporaryFile('w', suffix='.env', delete=False, encoding='utf-8')
        handle.write(body)
        handle.close()
        self.addCleanup(os.unlink, handle.name)
        return handle.name

    def test_env_without_leading_section_does_not_log_first_line(self):
        path = self._write(
            '# 설정\n' + DB_LINE + '\nLOG_LEVEL=INFO\n\n[TARGET_SRC_TBL_ID_LIST]\nDT_117102_A047\n')
        with self.assertLogs(level=logging.ERROR) as logs:
            result = config.load_target_src_tbl_id_list(path)
        text = '\n'.join(r.getMessage() for r in logs.records)
        self.assertNotIn('S3cretPw', text)
        self.assertNotIn('appuser', text)
        self.assertNotIn('DB_URL', text)
        self.assertIn('MissingSectionHeaderError', text)     # 예외 종류는 남긴다
        self.assertEqual(result, [{'stat_tbl_id': 'DT_117102_A047', 'from_year': None}])

    def test_fallback_splits_comma_like_configparser_path(self):
        """줄 단위 파싱도 "ID, 시작연도" 를 나눈다(전체를 ID 로 취급하지 않는다)."""
        body = '[TARGET_SRC_TBL_ID_LIST]\nDT_A\nDT_B, 2015\n# DT_C\nDT_D,abc\n'
        expected = [
            {'stat_tbl_id': 'DT_A', 'from_year': None},
            {'stat_tbl_id': 'DT_B', 'from_year': 2015},
            {'stat_tbl_id': 'DT_D', 'from_year': None},
        ]
        # configparser 경로(섹션으로 시작하는 파일)
        self.assertEqual(config.load_target_src_tbl_id_list(self._write(body)), expected)
        # 줄 단위 파싱 경로(앞에 섹션 없는 키=값이 있는 파일)
        with self.assertLogs(level=logging.ERROR):
            self.assertEqual(
                config.load_target_src_tbl_id_list(self._write(DB_LINE + '\n' + body)), expected)

    def test_missing_section_returns_empty(self):
        with self.assertLogs(level=logging.ERROR):
            self.assertEqual(config.load_target_src_tbl_id_list(self._write(DB_LINE + '\n')), [])


if __name__ == '__main__':
    unittest.main()
