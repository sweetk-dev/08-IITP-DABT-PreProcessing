"""ODCLOUD_DIS 적재기(db_odcloud_dis) 통합 테스트 — 실제 PostgreSQL 이 있을 때만 돈다.

ITEST_DB_URL 이 설정된 환경에서만(DB_URL 로 대신하지 않는다) 운영 표를 본뜬 임시 표와 그 이름의 판 추적 행을
만들어 종류별 교체(MODE_REPLACE)·연도별 교체·요약표 행 없는 표를 검증한다. 운영 표·운영 추적 행은 건드리지 않고,
끝나면 만든 것을 모두 지운다. 대상 DB 에는 01 v1.17.0(odcloud_dis_collect_v1170.sql)이 적용돼 있어야 한다.
"""
from __future__ import annotations

import dataclasses
import datetime
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

ITEST_URL = os.getenv('ITEST_DB_URL') or None
ORG_T = 'poi_dis_support_org_itest'
STAT_T = 'stats_dis_pension_sido_itest'
EXT_SYS = 'ITEST_ODCLOUD_DIS'
D0 = datetime.date(2025, 6, 30)
D1 = datetime.date(2026, 6, 30)


def org(name, org_type, dt=D1):
    return {'name': name, 'category': '구분', 'sido': '경기', 'tel': '031', 'detail': json.dumps({'연번': 1}),
            'org_type': org_type, 'src_data_id': '1', 'src_base_dt': dt}


def pension(year, sido, n=1, dt=D1):
    return {'year': year, 'sido': sido, 'basic_living_general': n, 'basic_living_facility': 1,
            'near_poor_edu_housing': 1, 'above_near_poor': 1, 'src_base_dt': dt}


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL 미설정 — DB 통합 테스트 생략')
class LoadDisDbTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import db_odcloud_dis
        import db_odcloud_emp
        from collectors import odcloud_dis as D
        from sqlalchemy import create_engine, text
        cls.L, cls.D, cls.text = db_odcloud_dis, D, staticmethod(text)
        cls.engine = create_engine(ITEST_URL)
        cls._orig_engine = db_odcloud_emp.engine
        db_odcloud_emp.engine = cls.engine          # db_odcloud_dis 는 고용 적재기의 엔진을 쓴다
        cls.ds_a = dataclasses.replace(D.DATASET_BY_KEY['poi_dis_support_org:job_center'], table=ORG_T,
                                       key=ORG_T + ':job_center')
        cls.ds_b = dataclasses.replace(D.DATASET_BY_KEY['poi_dis_support_org:aware_edu'], table=ORG_T,
                                       key=ORG_T + ':aware_edu')
        cls.ds_s = dataclasses.replace(D.DATASET_BY_KEY['stats_dis_pension_sido'], table=STAT_T, key=STAT_T)
        with cls.engine.begin() as conn:
            for t, src in ((ORG_T, 'poi_dis_support_org'), (STAT_T, 'stats_dis_pension_sido')):
                conn.execute(text('DROP TABLE IF EXISTS ' + t))
                conn.execute(text('CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING IDENTITY '
                                  'INCLUDING CONSTRAINTS INCLUDING INDEXES)' % (t, src)))
            cls._cleanup_tracking(conn)
            cls.ext_api_id = conn.execute(text(
                "INSERT INTO sys_ext_api_info (if_name, ext_sys, ext_url, auth, data_format, created_by)"
                " VALUES ('ITEST 장애인 파일데이터', :s, 'http://invalid.example', 'k', 'JSON', 'ITEST')"
                " RETURNING ext_api_id"), {'s': EXT_SYS}).scalar()
            for k in (cls.ds_a.key, cls.ds_b.key, cls.ds_s.key):
                conn.execute(text(
                    "INSERT INTO sys_stats_src_api_info (ext_api_id, stat_title, stat_tbl_id, use_base_url_yn,"
                    " api_data_url, api_meta_url, created_by) VALUES (:e, :t, :t, 'Y', '', 'http://spec', 'ITEST')"),
                    {'e': cls.ext_api_id, 't': k})

    @classmethod
    def _cleanup_tracking(cls, conn):
        conn.execute(cls.text("DELETE FROM sys_stats_src_api_info WHERE ext_api_id IN "
                              "(SELECT ext_api_id FROM sys_ext_api_info WHERE ext_sys = :s)"), {'s': EXT_SYS})
        conn.execute(cls.text("DELETE FROM sys_ext_api_info WHERE ext_sys = :s"), {'s': EXT_SYS})

    @classmethod
    def tearDownClass(cls):
        import db_odcloud_emp
        with cls.engine.begin() as conn:
            for t in (ORG_T, STAT_T):
                conn.execute(cls.text('DROP TABLE IF EXISTS ' + t))
            cls._cleanup_tracking(conn)
        db_odcloud_emp.engine = cls._orig_engine
        cls.engine.dispose()

    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute(self.text('TRUNCATE TABLE %s, %s' % (ORG_T, STAT_T)))
            conn.execute(self.text("UPDATE sys_stats_src_api_info SET api_data_url = '', latest_sync_time = NULL"
                                   " WHERE ext_api_id = :e"), {'e': self.ext_api_id})

    def _q(self, sql, **kw):
        with self.engine.begin() as conn:
            return conn.execute(self.text(sql), kw).all()

    def _seed(self, t, rows):
        from sqlalchemy import column, insert, table as tbl
        with self.engine.begin() as conn:
            conn.execute(insert(tbl(t, *[column(c) for c in list(rows[0]) + ['created_by']])),
                         [dict(r, created_by='SYS-MANUAL') for r in rows])

    def _load(self, ds, rows, accept=frozenset(), base=D1):
        track = self.L.load_tracking(self.ext_api_id)[ds.key]
        return self.L.load_dataset(ds, '/1/v1/uddi:new', base, rows, track, set(accept))

    def test_replace_touches_only_its_org_type(self):
        self._seed(ORG_T, [org('옛 센터1', 'job_center', D0), org('옛 센터2', 'job_center', D0),
                           org('교육기관', 'aware_edu', D0)])
        res = self._load(self.ds_a, [org('새 센터1', 'job_center'), org('새 센터2', 'job_center')])
        self.assertEqual((res['deleted'], res['inserted']), (2, 2))
        got = self._q('SELECT org_type, name, src_base_dt, detail->>\'연번\' FROM %s ORDER BY org_type, name' % ORG_T)
        self.assertEqual([(r[0], r[1]) for r in got],
                         [('aware_edu', '교육기관'), ('job_center', '새 센터1'), ('job_center', '새 센터2')])
        self.assertEqual(got[1][2], D1)
        self.assertEqual(got[1][3], '1')        # detail 이 jsonb 로 들어갔다
        tr = self._q('SELECT api_data_url, latest_sync_time IS NOT NULL FROM sys_stats_src_api_info'
                     ' WHERE stat_tbl_id = :k', k=self.ds_a.key)
        self.assertEqual(tr, [('/1/v1/uddi:new', True)])

    def test_replace_drop_guard(self):
        self._seed(ORG_T, [org('센터%d' % i, 'job_center', D0) for i in range(10)])
        with self.assertRaises(ValueError) as cm:
            self._load(self.ds_a, [org('센터0', 'job_center')])
        self.assertIn('ODCLOUD_DIS_ACCEPT_DROP', str(cm.exception))
        self.assertEqual(self._q('SELECT count(*) FROM %s' % ORG_T)[0][0], 10)      # 롤백 — 그대로
        self._load(self.ds_a, [org('센터0', 'job_center')], accept={self.ds_a.key})
        self.assertEqual(self._q('SELECT count(*) FROM %s' % ORG_T)[0][0], 1)

    def test_year_mode_without_summary_row(self):
        self._seed(STAT_T, [pension(2023, '경기', 5, D0), pension(2024, '경기', 6, D0)])
        res = self._load(self.ds_s, [pension(2024, '경기', 7), pension(2024, '서울', 8)])
        self.assertEqual((res['deleted'], res['inserted'], res['years']), (1, 2, [2024]))
        got = self._q('SELECT year, sido, basic_living_general FROM %s ORDER BY 1, 2 COLLATE "C"' % STAT_T)
        self.assertEqual(got, [(2023, '경기', 5), (2024, '경기', 7), (2024, '서울', 8)])


if __name__ == '__main__':
    unittest.main()
