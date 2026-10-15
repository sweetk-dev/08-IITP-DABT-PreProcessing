"""ODCLOUD_EMP 적재기(db_odcloud_emp) 통합 테스트 — 실제 PostgreSQL 이 있을 때만 돈다.

ITEST_DB_URL 이 설정된 환경에서만(DB_URL 만 있으면 건너뜀) 운영 표를 본뜬 임시 표 2개와 그 표 이름으로 된
추적 행(sys_ext_api_info·sys_stats_src_api_info·sys_data_summary_info)을 만들어 적재 규칙을 검증한다.
운영 표·운영 추적 행은 건드리지 않고, 끝나면 만든 것을 모두 지운다.
대상 DB 에는 01-IITP-DABT-Database emp init 과 v1.11.0(emp_odcloud_collect_v1110.sql)이 적용돼 있어야 한다.
"""
from __future__ import annotations

import datetime
import os
import sys
import unittest
from decimal import Decimal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# DB_URL 로 대신하지 않는다 — 배포 폴더의 .env 를 읽어 운영 DB 에서 테스트가 도는 것을 막는다.
ITEST_URL = os.getenv('ITEST_DB_URL') or None
YEAR_T = 'emp_dis_regional_status_itest'
FULL_T = 'emp_dis_obligation_status_itest'
EXT_SYS = 'ITEST_ODCLOUD'
D1 = datetime.date(2025, 12, 31)


def regional(year, region, cnt=1):
    return {'region': region, 'company_count': cnt, 'worker_count': 10, 'obligation_count': 1,
            'severe_2x_count': 1, 'severe_2x_rate': Decimal('2.50'), 'year': year}


def obligation(name, n=1):
    return {'org_name': name, 'workplace_count': n, 'worker_count': 10, 'disabled_count': 1,
            'emp_rate': Decimal('3.10')}


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL 미설정 — DB 통합 테스트 생략')
class LoadDatasetDbTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import db_odcloud_emp
        from sqlalchemy import create_engine, text
        cls.D = db_odcloud_emp
        cls.text = staticmethod(text)
        cls.engine = create_engine(ITEST_URL)
        cls._orig_engine = db_odcloud_emp.engine
        db_odcloud_emp.engine = cls.engine
        with cls.engine.begin() as conn:
            for t, src in ((YEAR_T, 'emp_dis_regional_status'), (FULL_T, 'emp_dis_obligation_status')):
                conn.execute(text('DROP TABLE IF EXISTS ' + t))
                # 기본값·자동 번호·NOT NULL·유니크 인덱스까지 복제해 운영 표와 같은 조건에서 검사한다.
                conn.execute(text('CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING IDENTITY '
                                  'INCLUDING CONSTRAINTS INCLUDING INDEXES)' % (t, src)))
            cls._cleanup_tracking(conn)
            cls.ext_api_id = conn.execute(text(
                "INSERT INTO sys_ext_api_info (if_name, ext_sys, ext_url, auth, data_format, created_by)"
                " VALUES ('ITEST 고용 파일데이터', :s, 'http://invalid.example', 'k', 'JSON', 'ITEST')"
                " RETURNING ext_api_id"), {'s': EXT_SYS}).scalar()
            for t in (YEAR_T, FULL_T):
                conn.execute(text(
                    "INSERT INTO sys_stats_src_api_info (ext_api_id, stat_title, stat_tbl_id, use_base_url_yn,"
                    " api_data_url, api_meta_url, created_by) VALUES (:e, :t, :t, 'Y', '', 'http://spec', 'ITEST')"),
                    {'e': cls.ext_api_id, 't': t})
                conn.execute(text(
                    "INSERT INTO sys_data_summary_info (data_type, title, sys_tbl_id, src_org_name,"
                    " src_latest_chn_dt, sys_data_reg_dt, open_api_url)"
                    " VALUES ('emp', :t, :t, 'ITEST', '2024-12-31', '2025-07-04', '/itest')"), {'t': t})

    @classmethod
    def _cleanup_tracking(cls, conn):
        text = cls.text
        conn.execute(text("DELETE FROM sys_stats_src_api_info WHERE ext_api_id IN "
                          "(SELECT ext_api_id FROM sys_ext_api_info WHERE ext_sys = :s)"), {'s': EXT_SYS})
        conn.execute(text("DELETE FROM sys_ext_api_info WHERE ext_sys = :s"), {'s': EXT_SYS})
        conn.execute(text("DELETE FROM sys_data_summary_info WHERE sys_tbl_id IN (:a, :b)"),
                     {'a': YEAR_T, 'b': FULL_T})

    @classmethod
    def tearDownClass(cls):
        with cls.engine.begin() as conn:
            for t in (YEAR_T, FULL_T):
                conn.execute(cls.text('DROP TABLE IF EXISTS ' + t))
            cls._cleanup_tracking(conn)
        cls.D.engine = cls._orig_engine
        cls.engine.dispose()

    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute(self.text('TRUNCATE TABLE %s, %s' % (YEAR_T, FULL_T)))
            conn.execute(self.text(
                "UPDATE sys_stats_src_api_info SET api_data_url = '', latest_sync_time = NULL"
                " WHERE ext_api_id = :e"), {'e': self.ext_api_id})
            conn.execute(self.text(
                "UPDATE sys_data_summary_info SET src_latest_chn_dt = '2024-12-31', sys_data_ref_dt = NULL"
                " WHERE sys_tbl_id IN (:a, :b)"), {'a': YEAR_T, 'b': FULL_T})

    # --- 도우미 ------------------------------------------------------------------
    def _q(self, sql, **kw):
        with self.engine.begin() as conn:
            return conn.execute(self.text(sql), kw).all()

    def _seed(self, table, rows):
        from sqlalchemy import column, insert, table as tbl
        with self.engine.begin() as conn:
            conn.execute(insert(tbl(table, *[column(c) for c in list(rows[0]) + ['created_by']])),
                         [dict(r, created_by='SYS-MANUAL') for r in rows])

    def _tracking(self, table):
        return self.D.load_tracking(self.ext_api_id)[table]

    def _load(self, table, mode, rows, path='/1/v1/uddi:new', base=D1, accept=frozenset(), pinned=False):
        return self.D.load_dataset(table, mode, path, base, rows, self._tracking(table), set(accept),
                                   pinned=pinned)

    # --- 시험 --------------------------------------------------------------------
    def test_load_tracking_returns_rows_with_summary_date(self):
        tr = self.D.load_tracking(self.ext_api_id)
        self.assertEqual(set(tr), {YEAR_T, FULL_T})
        self.assertEqual(tr[YEAR_T]['src_latest_chn_dt'], '2024-12-31')
        self.assertEqual(tr[YEAR_T]['api_data_url'], '')

    def test_year_mode_replaces_only_years_in_edition(self):
        self._seed(YEAR_T, [regional(2020, '서울 종로구'), regional(2020, '서울 중구'), regional(2025, '서울 종로구', 9)])
        res = self._load(YEAR_T, 'year', [regional(2025, '서울 종로구', 5), regional(2025, '서울 중구', 6)])
        self.assertEqual((res['deleted'], res['inserted'], res['years']), (1, 2, [2025]))
        rows = self._q('SELECT year, region, company_count, created_by FROM %s ORDER BY year, region' % YEAR_T)
        self.assertEqual([tuple(r[:3]) for r in rows],
                         [(2020, '서울 종로구', 1), (2020, '서울 중구', 1), (2025, '서울 종로구', 5), (2025, '서울 중구', 6)])
        self.assertEqual(rows[-1][3], 'SYS-BACH')
        url, sync = self._q("SELECT api_data_url, latest_sync_time FROM sys_stats_src_api_info"
                            " WHERE ext_api_id = :e AND stat_tbl_id = :t", e=self.ext_api_id, t=YEAR_T)[0]
        self.assertEqual(url, '/1/v1/uddi:new')
        self.assertIsNotNone(sync)
        base, ref = self._q("SELECT src_latest_chn_dt, sys_data_ref_dt FROM sys_data_summary_info"
                            " WHERE sys_tbl_id = :t", t=YEAR_T)[0]
        self.assertEqual((base, ref), ('2025-12-31', datetime.date.today().strftime('%Y-%m-%d')))

    def test_replace_mode_replaces_whole_table(self):
        self._seed(FULL_T, [obligation('옛기관%d' % i) for i in range(4)])
        res = self._load(FULL_T, 'replace', [obligation('중앙행정기관'), obligation('헌법기관')])
        self.assertEqual((res['deleted'], res['inserted']), (4, 2))
        self.assertEqual([r[0] for r in self._q('SELECT org_name FROM %s ORDER BY 1' % FULL_T)],
                         ['중앙행정기관', '헌법기관'])

    def test_drop_guard_rolls_back_everything(self):
        self._seed(FULL_T, [obligation('기관%d' % i) for i in range(10)])
        with self.assertRaisesRegex(ValueError, 'ODCLOUD_EMP_ACCEPT_DROP'):
            self._load(FULL_T, 'replace', [obligation('새기관%d' % i) for i in range(4)])
        self.assertEqual(self._q('SELECT count(*) FROM %s' % FULL_T)[0][0], 10)
        self.assertEqual(self._tracking(FULL_T)['api_data_url'], '')
        self._load(FULL_T, 'replace', [obligation('새기관%d' % i) for i in range(4)], accept={FULL_T})
        self.assertEqual(self._q('SELECT count(*) FROM %s' % FULL_T)[0][0], 4)

    def test_invalid_rows_are_rejected_before_writing(self):
        self._seed(FULL_T, [obligation('기관')])
        with self.assertRaisesRegex(ValueError, 'org_name 글자 수 100 초과'):
            self._load(FULL_T, 'replace', [obligation('가' * 101)])
        with self.assertRaisesRegex(ValueError, 'emp_rate 숫자 범위'):
            self._load(FULL_T, 'replace', [dict(obligation('기관2'), emp_rate=Decimal('1000.00'))])
        self.assertEqual([r[0] for r in self._q('SELECT org_name FROM %s' % FULL_T)], ['기관'])

    def test_unique_index_violation_rolls_back(self):
        self._seed(YEAR_T, [regional(2025, '서울 종로구')])
        with self.assertRaises(Exception):
            self._load(YEAR_T, 'year', [regional(2025, '서울 중구'), regional(2025, '서울 중구')])
        self.assertEqual(self._q('SELECT region FROM %s' % YEAR_T)[0][0], '서울 종로구')

    def test_older_pinned_edition_fills_only_empty_years_and_keeps_tracking(self):
        # 2022 는 최신 판으로 이미 들어 있고 2021 은 비어 있다. 2021~2022 누적 옛 판을 지정해 보충한다.
        self._seed(YEAR_T, [regional(2022, '서울 종로구', 7)])
        res = self._load(YEAR_T, 'year', [regional(2021, '서울 종로구', 1), regional(2022, '서울 종로구', 2)],
                         path='/1/v1/uddi:old', base=datetime.date(2023, 12, 31), pinned=True)
        self.assertFalse(res['tracking_updated'])
        self.assertEqual((res['years'], res['skipped_years'], res['inserted']), ([2021], [2022], 1))
        self.assertEqual(self._q('SELECT year, company_count FROM %s ORDER BY year' % YEAR_T),
                         [(2021, 1), (2022, 7)])                     # 2022 는 옛 값으로 덮이지 않음
        self.assertEqual(self._tracking(YEAR_T)['api_data_url'], '')
        self.assertEqual(self._tracking(YEAR_T)['src_latest_chn_dt'], '2024-12-31')
        # 넣을 연도가 없으면 아무것도 바꾸지 않는다
        res = self._load(YEAR_T, 'year', [regional(2022, '서울 종로구', 2)], path='/1/v1/uddi:old',
                         base=datetime.date(2023, 12, 31), pinned=True)
        self.assertEqual((res['inserted'], res['skipped_years']), (0, [2022]))
        with self.assertRaisesRegex(ValueError, '전체 교체 표'):
            self._load(FULL_T, 'replace', [obligation('a')], base=datetime.date(2023, 12, 31), pinned=True)

    def test_regular_run_updates_tracking_even_if_recorded_date_is_later(self):
        # 기록된 기준일이 잘못돼 최신 판보다 늦게 적혀 있어도 정기 실행은 추적 행을 갱신한다(매달 재적재 방지)
        with self.engine.begin() as conn:
            conn.execute(self.text("UPDATE sys_data_summary_info SET src_latest_chn_dt = '2099-12-31'"
                                   " WHERE sys_tbl_id = :t"), {'t': YEAR_T})
        res = self._load(YEAR_T, 'year', [regional(2025, '서울 종로구')])
        self.assertTrue(res['tracking_updated'])
        self.assertEqual(self._tracking(YEAR_T)['api_data_url'], '/1/v1/uddi:new')

    def test_year_drop_guard_checks_each_year(self):
        # 합계로는 50% 이상(20 → 12)이지만 2024년이 10 → 2 로 줄었다 → 중단
        self._seed(YEAR_T, [regional(2023, 'r%d' % i) for i in range(10)] +
                   [regional(2024, 'r%d' % i) for i in range(10)])
        rows = [regional(2023, 'r%d' % i) for i in range(10)] + [regional(2024, 'r%d' % i) for i in range(2)]
        with self.assertRaisesRegex(ValueError, r'\(2024년\)'):
            self._load(YEAR_T, 'year', rows)
        self.assertEqual(self._q('SELECT count(*) FROM %s' % YEAR_T)[0][0], 20)
        plan = self.D.plan_dataset(YEAR_T, 'year', rows, set())
        self.assertFalse(plan['drop_ok'])

    def test_missing_summary_row_fails_and_rolls_back(self):
        with self.engine.begin() as conn:
            conn.execute(self.text("UPDATE sys_data_summary_info SET del_yn = 'Y' WHERE sys_tbl_id = :t"),
                         {'t': FULL_T})
        try:
            tracking = dict(self._tracking(FULL_T), src_latest_chn_dt=None)
            with self.assertRaisesRegex(RuntimeError, 'sys_data_summary_info'):
                self.D.load_dataset(FULL_T, 'replace', '/1/v1/uddi:new', D1, [obligation('a')], tracking, set())
            self.assertEqual(self._q('SELECT count(*) FROM %s' % FULL_T)[0][0], 0)
            self.assertEqual(self._tracking(FULL_T)['api_data_url'], '')
        finally:
            with self.engine.begin() as conn:
                conn.execute(self.text("UPDATE sys_data_summary_info SET del_yn = 'N' WHERE sys_tbl_id = :t"),
                             {'t': FULL_T})

    def test_plan_is_read_only(self):
        self._seed(FULL_T, [obligation('기관%d' % i) for i in range(10)])
        plan = self.D.plan_dataset(FULL_T, 'replace', [obligation('a')], set())
        self.assertEqual((plan['replace'], plan['insert'], plan['drop_ok']), (10, 1, False))
        self.assertIn('ODCLOUD_EMP_ACCEPT_DROP', plan['problem'])
        plan = self.D.plan_dataset(YEAR_T, 'year', [regional(2025, 'x')], set())
        self.assertEqual((plan['replace'], plan['years'], plan['drop_ok']), (0, [2025], True))
        self.assertEqual(self._q('SELECT count(*) FROM %s' % FULL_T)[0][0], 10)


if __name__ == '__main__':
    unittest.main()
