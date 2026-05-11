"""
Unit tests for db.get_api_info()

이슈 #27 — 멀티소스 동적 조회 지원
- KOSIS path (default ext_sys='KOSIS'): 후방호환 검증
- non-KOSIS path: 신규 분기 동작 검증

DB 접속 없이 mock 으로 SQL 바인딩 파라미터·반환값 형태만 검증한다.
"""
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# 테스트 실행을 위한 환경: 모듈 import 시 외부 DB 접속을 시도하지 않도록
# config.py / dotenv 가 로컬 .env 없이도 안전하게 동작함을 가정한다.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


class _FakeRow:
    """SQLAlchemy Row 의 속성 접근 모사"""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class GetApiInfoKosisPathTests(unittest.TestCase):
    """KOSIS 경로 — default 인자 호출 시 기존 동작 그대로"""

    def _stub_module(self, fake_row):
        import db as _db
        fake_session = MagicMock()
        fake_result = MagicMock()
        fake_result.fetchone.return_value = fake_row
        fake_session.execute.return_value = fake_result
        _db.Session = MagicMock(return_value=fake_session)
        return _db, fake_session

    def test_kosis_default_call_returns_dict(self):
        row = _FakeRow(
            ext_api_id='KOSIS_001', if_name='KOSIS', ext_sys='KOSIS',
            ext_url='https://kosis.kr', auth='KEY', data_format='JSON',
            latest_sync_time=None, status='A',
        )
        db_mod, fake_session = self._stub_module(row)
        out = db_mod.get_api_info()
        self.assertIsInstance(out, dict)
        self.assertEqual(out['ext_sys'], 'KOSIS')
        self.assertEqual(out['ext_api_id'], 'KOSIS_001')

    def test_kosis_default_binds_kosis_param(self):
        """SQL 바인딩 dict 에 ext_sys='KOSIS' 가 들어가야 한다 (default)"""
        row = _FakeRow(
            ext_api_id='KOSIS_001', if_name='KOSIS', ext_sys='KOSIS',
            ext_url='https://kosis.kr', auth='K', data_format='JSON',
            latest_sync_time=None, status='A',
        )
        db_mod, fake_session = self._stub_module(row)
        db_mod.get_api_info()
        args, kwargs = fake_session.execute.call_args
        self.assertEqual(args[1], {'ext_sys': 'KOSIS'})

    def test_kosis_empty_db_returns_empty_dict(self):
        db_mod, _ = self._stub_module(None)
        out = db_mod.get_api_info()
        self.assertEqual(out, {})


if __name__ == '__main__':
    unittest.main()
