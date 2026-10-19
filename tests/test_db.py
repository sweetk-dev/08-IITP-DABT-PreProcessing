"""
Unit tests for db.get_api_info()

이슈 #27 — 멀티소스 동적 조회 지원
- KOSIS path (default ext_sys='KOSIS'): 후방호환 검증
- non-KOSIS path: 신규 분기 동작 검증

DB 접속 없이 mock 으로 SQL 바인딩 파라미터·반환값 형태만 검증한다.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock

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




class GetApiInfoNonKosisPathTests(unittest.TestCase):
    """non-KOSIS 경로 — ext_sys 명시 호출 시 신규 분기"""

    def _stub_module(self, fake_row):
        import db as _db
        fake_session = MagicMock()
        fake_result = MagicMock()
        fake_result.fetchone.return_value = fake_row
        fake_session.execute.return_value = fake_result
        _db.Session = MagicMock(return_value=fake_session)
        return _db, fake_session

    def test_non_kosis_param_binds_correctly(self):
        """ext_sys='DATA_GO_KR' 호출 시 SQL 바인딩에도 동일 값"""
        row = _FakeRow(
            ext_api_id='DGK_001', if_name='DataGoKr', ext_sys='DATA_GO_KR',
            ext_url='https://api.data.go.kr', auth='KEY', data_format='JSON',
            latest_sync_time=None, status='A',
        )
        db_mod, fake_session = self._stub_module(row)
        out = db_mod.get_api_info(ext_sys='DATA_GO_KR')
        args, kwargs = fake_session.execute.call_args
        self.assertEqual(args[1], {'ext_sys': 'DATA_GO_KR'})
        self.assertEqual(out['ext_sys'], 'DATA_GO_KR')
        self.assertEqual(out['ext_api_id'], 'DGK_001')

    def test_non_kosis_positional_arg(self):
        """positional 인자도 동일하게 동작"""
        row = _FakeRow(
            ext_api_id='MD_001', if_name='Microdata', ext_sys='MICRODATA',
            ext_url='https://md.kr', auth='K', data_format='CSV',
            latest_sync_time=None, status='A',
        )
        db_mod, fake_session = self._stub_module(row)
        out = db_mod.get_api_info('MICRODATA')
        args, kwargs = fake_session.execute.call_args
        self.assertEqual(args[1], {'ext_sys': 'MICRODATA'})
        self.assertEqual(out['ext_sys'], 'MICRODATA')

    def test_non_kosis_empty_returns_empty_dict(self):
        db_mod, _ = self._stub_module(None)
        out = db_mod.get_api_info(ext_sys='DATA_GO_KR')
        self.assertEqual(out, {})



class GetStatsSrcDataInfoTests(unittest.TestCase):
    """get_stats_src_data_info — 통계표 ID 목록 조회가 DB 드라이버와 무관하게 동작하는지 (이슈 #133)

    다건 조회의 ``IN :stat_tbl_id_list`` 는 expanding 바인딩이어야 한다. 튜플을 그대로 넘기면
    psycopg(3) 드라이버에서 ``IN $2`` 로 전송되어 SQL 문법 오류가 난다.
    """

    def _stub_module(self, rows):
        import db as _db
        fake_session = MagicMock()
        fake_result = MagicMock()
        fake_result.fetchall.return_value = rows
        fake_session.execute.return_value = fake_result
        _db.Session = MagicMock(return_value=fake_session)
        return _db, fake_session

    @staticmethod
    def _row(stat_tbl_id):
        row = _FakeRow(stat_tbl_id=stat_tbl_id)
        row._mapping = {'stat_tbl_id': stat_tbl_id}
        return row

    def test_multi_ids_use_expanding_bind(self):
        db_mod, fake_session = self._stub_module([self._row('DT_A'), self._row('DT_B')])
        out = db_mod.get_stats_src_data_info(13, ['DT_A', 'DT_B'])
        self.assertEqual(sorted(out), ['DT_A', 'DT_B'])
        query, params = fake_session.execute.call_args[0]
        self.assertTrue(query._bindparams['stat_tbl_id_list'].expanding)
        self.assertEqual(params['stat_tbl_id_list'], ['DT_A', 'DT_B'])

    def test_multi_ids_compile_to_expanded_in(self):
        """PostgreSQL 방언으로 컴파일하면 IN 목록이 값 개수만큼 펼쳐져야 한다"""
        from sqlalchemy.dialects import postgresql
        db_mod, fake_session = self._stub_module([])
        db_mod.get_stats_src_data_info(13, ['DT_A', 'DT_B', 'DT_C'])
        query, params = fake_session.execute.call_args[0]
        compiled = query.bindparams(**params).compile(
            dialect=postgresql.dialect(), compile_kwargs={'render_postcompile': True})
        sql = ' '.join(str(compiled).split())
        self.assertIn('stat_tbl_id IN (', sql)
        self.assertEqual(sql.count('stat_tbl_id_list_'), 3)

    def test_single_id_uses_equality(self):
        db_mod, fake_session = self._stub_module([self._row('DT_A')])
        out = db_mod.get_stats_src_data_info(13, ['DT_A'])
        self.assertEqual(list(out), ['DT_A'])
        query, params = fake_session.execute.call_args[0]
        self.assertEqual(params, {'ext_api_id': 13, 'stat_tbl_id': 'DT_A'})


if __name__ == '__main__':
    unittest.main()
