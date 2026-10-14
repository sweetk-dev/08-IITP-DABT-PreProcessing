"""GG_CULTURE 적재기 통합 테스트 — 실제 PostgreSQL 이 있을 때만 돈다.

ITEST_DB_URL 이 설정된 환경에서만(DB_URL 만 있으면 건너뜀) poi_culture_sports_facility 를
본뜬 임시 테이블(인덱스·제약 포함)을 만들어 동기화 규칙을 검증한다. 원본 테이블은 건드리지 않는다.
대상 DB 에는 01-IITP-DABT-Database v1.10.0 (culture_sports_facility_v1100.sql) 이 적용돼 있어야 한다.
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# DB_URL 로 대신하지 않는다 — 배포 폴더의 .env 를 읽어 운영 DB 에서 테스트가 도는 것을 막는다.
ITEST_URL = os.getenv('ITEST_DB_URL') or None
TABLE = 'poi_culture_sports_facility_itest'


def _row(ftype, name, road=None, jibun=None, **kw):
    base = {
        'facility_type': ftype, 'sido_code': '9410000', 'sigun_cd': '41170', 'sigun_nm': '안양시',
        'name': name, 'addr_road': road, 'addr_jibun': jibun, 'zip_code': None,
        'latitude': 37.39, 'longitude': 126.95, 'tel': '031-000-0000', 'homepage': None,
        'install_report_dt': None, 'corp_name': None, 'source': 'GG_CULTURE',
    }
    base.update(kw)
    return base


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL 미설정 — DB 통합 테스트 생략')
class SyncCultureDbTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import db_mobility
        from sqlalchemy import create_engine, text
        cls.dm = db_mobility
        cls.text = staticmethod(text)
        cls.engine = create_engine(ITEST_URL)
        cls._orig_engine = db_mobility.engine
        db_mobility.engine = cls.engine
        with cls.engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS ' + TABLE))
            # 부분 유니크 인덱스·CHECK 제약까지 복제해 운영 표와 같은 조건에서 검사한다.
            conn.execute(text('CREATE TABLE ' + TABLE + ' (LIKE poi_culture_sports_facility '
                              'INCLUDING DEFAULTS INCLUDING IDENTITY INCLUDING CONSTRAINTS INCLUDING INDEXES)'))
        cls._orig_table = db_mobility.CULTURE_TABLE
        db_mobility.CULTURE_TABLE = TABLE

    @classmethod
    def tearDownClass(cls):
        cls.dm.CULTURE_TABLE = cls._orig_table
        with cls.engine.begin() as conn:
            conn.execute(cls.text('DROP TABLE IF EXISTS ' + TABLE))
        cls.dm.engine = cls._orig_engine
        cls.engine.dispose()

    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute(self.text('TRUNCATE TABLE ' + TABLE))

    def _state(self):
        with self.engine.begin() as conn:
            rows = conn.execute(self.text(
                'SELECT facility_id, facility_type, name, del_yn, addr_road FROM ' + TABLE +
                ' ORDER BY facility_id')).fetchall()
        return rows

    def _id(self, name, active=True):
        for fid, _, n, d, _ in self._state():
            if n == name and (d == 'N') == active:
                return fid
        return None

    def test_first_load_then_idempotent(self):
        rows = [_row('braille_lib', 'A도서관', '경기도 안양시 x로 1'),
                _row('sports_org', '안양시장애인체육회', '경기도 안양시 y로 2', install_report_dt='2024-08-08')]
        first = self.dm.sync_culture_facilities([dict(r) for r in rows])
        self.assertEqual(first['db_ok'], 2)
        self.assertEqual(first['failures'], [])
        ids = {n: self._id(n) for n in ('A도서관', '안양시장애인체육회')}
        second = self.dm.sync_culture_facilities([dict(r) for r in rows])
        self.assertEqual(second['by_type']['braille_lib'], {'matched': 1, 'revived': 0, 'inserted': 0, 'deleted': 0})
        self.assertEqual({n: self._id(n) for n in ids}, ids)          # id 보존
        self.assertEqual(len(self._state()), 2)

    def test_disappear_then_revive_keeps_id(self):
        a = _row('booknarae_lib', '가도서관', '경기도 안양시 a로 1')
        b = _row('booknarae_lib', '나도서관', '경기도 안양시 b로 2')
        self.dm.sync_culture_facilities([dict(a), dict(b)])
        fid = self._id('나도서관')
        res = self.dm.sync_culture_facilities([dict(a)])                    # 나도서관 사라짐
        self.assertEqual(res['by_type']['booknarae_lib']['deleted'], 1)
        self.assertIsNone(self._id('나도서관'))
        res = self.dm.sync_culture_facilities([dict(a), dict(b)])          # 다시 나타남
        self.assertEqual(res['by_type']['booknarae_lib']['revived'], 1)
        self.assertEqual(self._id('나도서관'), fid)                         # 같은 행이 되살아남
        self.assertEqual(len(self._state()), 2)                              # 중복 행 없음

    def test_address_change_is_same_facility(self):
        self.dm.sync_culture_facilities([_row('sports_fac', '반다비체육센터', '경기도 안양시 만안구 냉천로 39')])
        fid = self._id('반다비체육센터')
        res = self.dm.sync_culture_facilities([_row('sports_fac', '반다비체육센터', '경기도 안양시 만안구 냉천로 41')])
        self.assertEqual(res['by_type']['sports_fac'], {'matched': 1, 'revived': 0, 'inserted': 0, 'deleted': 0})
        self.assertEqual(self._id('반다비체육센터'), fid)

    def test_same_name_two_places_not_merged(self):
        rows = [_row('booknarae_lib', '작은도서관', '경기도 a시 1'), _row('booknarae_lib', '작은도서관', '경기도 b시 2')]
        self.dm.sync_culture_facilities([dict(r) for r in rows])
        res = self.dm.sync_culture_facilities([_row('booknarae_lib', '작은도서관', '경기도 c시 3'),
                                               _row('booknarae_lib', '작은도서관', '경기도 d시 4')])
        # 동명이 둘 이상이면 주소 변경으로 잇지 않는다 → 신규 2·논리삭제 2
        self.assertEqual(res['by_type']['booknarae_lib']['inserted'], 2)
        self.assertEqual(res['by_type']['booknarae_lib']['deleted'], 2)

    def test_missing_type_untouched(self):
        self.dm.sync_culture_facilities([_row('braille_lib', 'A', 'x 1'), _row('sports_org', 'B', 'y 2')])
        self.dm.sync_culture_facilities([_row('sports_org', 'B', 'y 2')])     # braille 수집 실패 상황
        self.assertIsNotNone(self._id('A'))                                    # 지워지지 않음

    def test_large_type_drop_is_isolated(self):
        many = [_row('booknarae_lib', '도서관%03d' % i, '경기도 x로 %d' % i) for i in range(60)]
        self.dm.sync_culture_facilities(many + [_row('sports_org', 'B', 'y 2')])
        res = self.dm.sync_culture_facilities(many[:10] + [_row('sports_org', 'B', 'y 2', tel='031-111-1111')])
        self.assertEqual([f['id'] for f in res['failures']], ['GG_CULTURE:booknarae_lib'])
        self.assertEqual(sum(1 for r in self._state() if r[1] == 'booknarae_lib' and r[3] == 'N'), 60)  # 롤백
        self.assertIn('sports_org', res['by_type'])                                                       # 다른 종류는 반영
        with self.engine.begin() as conn:
            tel = conn.execute(self.text("SELECT tel FROM " + TABLE + " WHERE name='B'")).scalar()
        self.assertEqual(tel, '031-111-1111')

    def test_small_type_collapse_is_blocked(self):
        # 32건 → 1건: 31건 삭제는 허용 9건(30%)을 넘어 중단, 기존 행 유지
        orgs = [_row('sports_org', '%02d시장애인체육회' % i, '경기도 %02d시 1' % i) for i in range(32)]
        self.dm.sync_culture_facilities([dict(r) for r in orgs])
        res = self.dm.sync_culture_facilities([dict(orgs[0])])
        self.assertEqual([f['id'] for f in res['failures']], ['GG_CULTURE:sports_org'])
        self.assertEqual(sum(1 for r in self._state() if r[3] == 'N'), 32)

    def test_small_type_normal_closure_allowed(self):
        # 5건 → 3건(2곳 폐관)은 정상 변동으로 반영
        libs = [_row('braille_lib', '점자%d' % i, '경기도 x로 %d' % i) for i in range(5)]
        self.dm.sync_culture_facilities([dict(r) for r in libs])
        res = self.dm.sync_culture_facilities([dict(r) for r in libs[:3]])
        self.assertEqual(res['failures'], [])
        self.assertEqual(res['by_type']['braille_lib']['deleted'], 2)

    def test_accept_drop_override(self):
        orgs = [_row('sports_org', '%02d체육회' % i, '경기도 %02d 1' % i) for i in range(32)]
        self.dm.sync_culture_facilities([dict(r) for r in orgs])
        os.environ['GG_CULTURE_ACCEPT_DROP'] = 'sports_org'
        try:
            res = self.dm.sync_culture_facilities([dict(orgs[0])])
        finally:
            del os.environ['GG_CULTURE_ACCEPT_DROP']
        self.assertEqual(res['failures'], [])
        self.assertEqual(res['by_type']['sports_org']['deleted'], 31)

    def test_addressless_row_idempotent_and_revived(self):
        r = _row('sports_org', '주소없는체육회')
        self.dm.sync_culture_facilities([dict(r), _row('sports_org', '다른곳', 'z 1')])
        fid = self._id('주소없는체육회')
        res = self.dm.sync_culture_facilities([dict(r), _row('sports_org', '다른곳', 'z 1')])
        self.assertEqual(res['failures'], [])
        self.assertEqual(res['by_type']['sports_org']['matched'], 2)
        self.dm.sync_culture_facilities([_row('sports_org', '다른곳', 'z 1')])         # 사라짐
        res = self.dm.sync_culture_facilities([dict(r), _row('sports_org', '다른곳', 'z 1')])
        self.assertEqual(res['by_type']['sports_org']['revived'], 1)
        self.assertEqual(self._id('주소없는체육회'), fid)

    def test_parenthesised_branches_not_merged(self):
        self.dm.sync_culture_facilities([_row('booknarae_lib', '희망도서관(본관)', '경기도 a로 1')])
        old = self._id('희망도서관(본관)')
        self.dm.sync_culture_facilities([_row('booknarae_lib', '희망도서관(분관)', '경기도 b로 2')])
        self.assertIsNone(self._id('희망도서관(본관)'))
        self.assertNotEqual(self._id('희망도서관(분관)'), old)    # 본관 번호를 분관이 물려받지 않는다

    def test_empty_is_noop(self):
        self.assertEqual(self.dm.sync_culture_facilities([])['db_ok'], 0)


if __name__ == '__main__':
    unittest.main()
