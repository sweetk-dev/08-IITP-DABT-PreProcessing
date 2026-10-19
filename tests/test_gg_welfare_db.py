"""GG_WELFARE·GG_DIS_REG 적재와 요약표 갱신 DB 통합 테스트 (v1.21.0).

ITEST_DB_URL 이 설정된 환경에서만(DB_URL 만 있으면 건너뜀) 실제 표 정의를 복제한 임시 표에서 돈다.
대상 DB 에 01 v1.14.0(local_stats_welfare_v1140.sql)이 적용돼 있어야 한다.
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

ITEST_URL = os.getenv('ITEST_DB_URL') or None
W_TABLE = 'itest_poi_welfare_facility'
E_TABLE = 'itest_stats_gg_dis_reg_emd'
S_TBL_ID = 'itest_summary_tbl'


def _w(ftype, name, road=None, **kw):
    base = {'facility_type': ftype, 'sub_type': None, 'sido_code': '9410000', 'sigun_cd': '41170',
            'sigun_nm': '안양시', 'name': name, 'addr_road': road, 'addr_jibun': None, 'zip_code': None,
            'latitude': 37.39, 'longitude': 126.95, 'tel': None, 'homepage': None, 'capacity_cnt': 10,
            'current_cnt': 8, 'staff_capacity_cnt': 5, 'staff_current_cnt': 5, 'install_report_dt': '2001-11-23',
            'corp_name': None, 'source': 'GG_WELFARE'}
    base.update(kw)
    return base


def _e(sigun, emd, t, dt, total=10, gu=None):
    return {'base_dt': dt, 'sigun_nm': sigun, 'gu_nm': gu, 'emd_nm': emd, 'dis_type': t, 'tot_cnt': total,
            'male_cnt': None, 'female_cnt': None, 'sev_cnt': None, 'sev_male_cnt': None, 'sev_female_cnt': None,
            'nsev_cnt': None, 'nsev_male_cnt': None, 'nsev_female_cnt': None}


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL 미설정 — DB 통합 테스트 생략')
class WelfareDisRegDbTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import db_mobility
        from sqlalchemy import create_engine, text
        cls.dm = db_mobility
        cls.text = staticmethod(text)
        cls.engine = create_engine(ITEST_URL)
        cls._orig = (db_mobility.engine, db_mobility.WELFARE_TABLE, db_mobility.GG_DIS_REG_TABLE)
        db_mobility.engine = cls.engine
        with cls.engine.begin() as conn:
            for tmp, src in ((W_TABLE, 'poi_welfare_facility'), (E_TABLE, 'stats_gg_dis_reg_emd')):
                conn.execute(text('DROP TABLE IF EXISTS ' + tmp))
                conn.execute(text('CREATE TABLE ' + tmp + ' (LIKE ' + src + ' INCLUDING DEFAULTS INCLUDING IDENTITY '
                                  'INCLUDING CONSTRAINTS INCLUDING INDEXES)'))
        db_mobility.WELFARE_TABLE = W_TABLE
        db_mobility.GG_DIS_REG_TABLE = E_TABLE

    @classmethod
    def tearDownClass(cls):
        cls.dm.engine, cls.dm.WELFARE_TABLE, cls.dm.GG_DIS_REG_TABLE = cls._orig
        with cls.engine.begin() as conn:
            conn.execute(cls.text('DROP TABLE IF EXISTS ' + W_TABLE))
            conn.execute(cls.text('DROP TABLE IF EXISTS ' + E_TABLE))
            conn.execute(cls.text("DELETE FROM sys_data_summary_info WHERE sys_tbl_id = :t"), {'t': S_TBL_ID})
        cls.engine.dispose()

    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute(self.text('TRUNCATE TABLE ' + W_TABLE))
            conn.execute(self.text('TRUNCATE TABLE ' + E_TABLE))

    def _scalar(self, sql, **p):
        with self.engine.begin() as conn:
            return conn.execute(self.text(sql), p).scalar()

    # --- 장애인복지시설 ---------------------------------------------------------
    def test_welfare_load_idempotent_and_counts(self):
        rows = [_w('residence', '예가원', '경기도 a로 1', sub_type='지적장애인시설'),
                _w('protect_workshop', '한마음일터', '경기도 b로 2', capacity_cnt=22)]
        res = self.dm.sync_welfare_facilities([dict(r) for r in rows])
        self.assertEqual(res['failures'], [])
        self.assertEqual(sum(v['inserted'] for v in res['by_type'].values()), 2)
        res = self.dm.sync_welfare_facilities([dict(r) for r in rows[:1]] + [dict(rows[1], current_cnt=3)])
        self.assertEqual(sum(v['matched'] for v in res['by_type'].values()), 2)
        self.assertEqual(self._scalar("SELECT current_cnt FROM " + W_TABLE + " WHERE name='한마음일터'"), 3)
        self.assertEqual(self._scalar("SELECT install_report_dt::text FROM " + W_TABLE + " WHERE name='예가원'"),
                         '2001-11-23')

    def test_welfare_drop_guard_uses_own_env(self):
        many = [_w('il_center', '센터%02d' % i, '경기도 c로 %d' % i) for i in range(20)]
        self.dm.sync_welfare_facilities([dict(r) for r in many])
        res = self.dm.sync_welfare_facilities([dict(r) for r in many[:5]])
        self.assertEqual([f['id'] for f in res['failures']], ['GG_WELFARE:il_center'])
        self.assertIn('GG_WELFARE_ACCEPT_DROP', res['failures'][0]['reason'])
        os.environ['GG_WELFARE_ACCEPT_DROP'] = 'il_center'
        try:
            res = self.dm.sync_welfare_facilities([dict(r) for r in many[:5]])
        finally:
            del os.environ['GG_WELFARE_ACCEPT_DROP']
        self.assertEqual(res['by_type']['il_center']['deleted'], 15)

    # --- 읍면동 등록장애인 ------------------------------------------------------
    def test_dis_reg_editions_accumulate(self):
        old = [_e('안양시', '안양%d동' % i, '지체', '2024-08-01') for i in range(4)]
        new = [_e('안양시', '안양%d동' % i, '지체', '2025-08-07', total=20) for i in range(4)]
        self.dm.sync_gg_dis_reg(old)
        res = self.dm.sync_gg_dis_reg(new + [_e('의왕시', '부곡동', '간', '2025-07-14')])
        self.assertEqual(res['failures'], [])
        self.assertEqual(self._scalar("SELECT count(*) FROM " + E_TABLE), 9)                 # 지난 판 유지
        self.assertEqual(res['by_type']['안양시 2025-08-07']['kind'], 'new')
        # 같은 판 다시 → 그 판만 교체
        res = self.dm.sync_gg_dis_reg([dict(r, tot_cnt=30) for r in new])
        self.assertEqual(res['by_type']['안양시 2025-08-07']['kind'], 'same')
        self.assertEqual(self._scalar("SELECT sum(tot_cnt) FROM " + E_TABLE +
                                      " WHERE base_dt=DATE '2025-08-07'"), 120)
        self.assertEqual(self._scalar("SELECT count(*) FROM " + E_TABLE), 9)                 # 의왕 판 그대로

    def test_dis_reg_drop_guard_per_sigun(self):
        self.dm.sync_gg_dis_reg([_e('안양시', '동%d' % i, '지체', '2025-08-07') for i in range(10)] +
                                [_e('과천시', '동%d' % i, '지체', '2026-05-06') for i in range(4)])
        res = self.dm.sync_gg_dis_reg([_e('안양시', '동%d' % i, '지체', '2026-01-01') for i in range(3)] +
                                      [_e('과천시', '동%d' % i, '지체', '2026-05-06', total=1) for i in range(4)])
        self.assertEqual([f['id'] for f in res['failures']], ['GG_DIS_REG:안양시'])
        self.assertEqual(self._scalar("SELECT count(*) FROM " + E_TABLE + " WHERE base_dt=DATE '2026-01-01'"), 0)
        self.assertEqual(self._scalar("SELECT sum(tot_cnt) FROM " + E_TABLE + " WHERE sigun_nm='과천시'"), 4)

    def test_dis_reg_unique_key_null_gu(self):
        with self.assertRaises(Exception):
            with self.engine.begin() as conn:
                for _ in range(2):
                    conn.execute(self.text("INSERT INTO " + E_TABLE + " (base_dt, sigun_nm, gu_nm, emd_nm, dis_type,"
                                           " created_by) VALUES (DATE '2025-01-01', '의왕시', NULL, '부곡동', '간',"
                                           " 'ITEST')"))

    # --- 요약표 갱신 ------------------------------------------------------------
    def test_refresh_data_summary(self):
        with self.engine.begin() as conn:
            conn.execute(self.text("DELETE FROM sys_data_summary_info WHERE sys_tbl_id=:t"), {'t': S_TBL_ID})
            conn.execute(self.text(
                "INSERT INTO sys_data_summary_info (data_type, title, sys_tbl_id, src_org_name, src_latest_chn_dt,"
                " sys_data_reg_dt, open_api_url) VALUES ('poi', 'ITEST', :t, 'ITEST', '2025-01-01', '2025-01-01',"
                " '/itest')"), {'t': S_TBL_ID})
        saved = dict(self.dm.SUMMARY_REFRESH)
        try:
            self.dm.SUMMARY_REFRESH['ITEST_NEW'] = (S_TBL_ID, "SELECT DATE '2026-02-03'")
            self.dm.SUMMARY_REFRESH['ITEST_OLD'] = (S_TBL_ID, "SELECT DATE '2025-06-30'")
            self.dm.SUMMARY_REFRESH['ITEST_NONE'] = ('itest_missing_tbl', None)
            self.assertEqual(self.dm.refresh_data_summary('ITEST_OLD'), '2025-06-30')
            self.assertEqual(self.dm.refresh_data_summary('ITEST_NEW'), '2026-02-03')      # 적재 행 기준으로 덮어씀
            self.assertIsNone(self.dm.refresh_data_summary('ITEST_NONE'))                   # 행 없음 → None
        finally:
            self.dm.SUMMARY_REFRESH.clear(); self.dm.SUMMARY_REFRESH.update(saved)
        self.assertEqual(self._scalar("SELECT src_latest_chn_dt FROM sys_data_summary_info WHERE sys_tbl_id=:t",
                                      t=S_TBL_ID), '2026-02-03')


if __name__ == '__main__':
    unittest.main()
