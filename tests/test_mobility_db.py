"""이동편의 적재기 통합 테스트 — 실제 PostgreSQL 이 있을 때만 돈다.

ITEST_DB_URL(또는 DB_URL) 이 설정된 환경에서, 01-IITP-DABT-Database 의 mobility init
스크립트가 적용된 DB 를 대상으로 한다. 원본 테이블의 기존 행은 건드리지 않는다:
'ITEST-' 로 시작하는 전용 키의 행만 넣고, 끝나면 그 행만 지운다.
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

ITEST_URL = os.getenv('ITEST_DB_URL') or os.getenv('DB_URL')

FACL_ID = 'ITEST-FACL-0001'
SUPPORT_NAME = 'ITEST-충전기-시청'


class _DbCase(unittest.TestCase):
    """db_mobility.engine 을 테스트 대상 DB 엔진으로 바꿔 끼우는 공통 준비."""

    @classmethod
    def setUpClass(cls):
        import db_mobility
        from sqlalchemy import create_engine, text
        cls.db_mobility = db_mobility
        cls.text = staticmethod(text)
        cls.engine = create_engine(ITEST_URL)
        cls._orig_engine = db_mobility.engine
        db_mobility.engine = cls.engine
        cls._cleanup()

    @classmethod
    def tearDownClass(cls):
        cls._cleanup()
        cls.db_mobility.engine = cls._orig_engine
        cls.engine.dispose()

    @classmethod
    def _cleanup(cls):
        with cls.engine.begin() as conn:
            conn.execute(cls.text("DELETE FROM poi_facility_accessibility WHERE facl_inf_id = :i"),
                         {'i': FACL_ID})
            conn.execute(cls.text("DELETE FROM poi_emergency_support WHERE name = :n"), {'n': SUPPORT_NAME})


def _facility(**kw):
    base = {
        'facl_inf_id': FACL_ID, 'wfclt_id': 'ITEST-B-1', 'facl_name': '시험 시설', 'facl_type': 'UC0A01',
        'addr': '경기도 안양시 시험로 1', 'latitude': 37.39, 'longitude': 126.95, 'estb_date': '20200101',
        'elevator_yn': None, 'dis_toilet_yn': None, 'dis_parking_yn': None, 'entrance_ramp_yn': None,
        'entrance_door_yn': None, 'approach_road_yn': None, 'guide_facility_yn': None,
        'accessible_room_yn': None, 'eval_info_raw': None, 'base_dt': '2026-09-01',
    }
    base.update(kw)
    return base


FLAG_COLS = ('elevator_yn', 'dis_toilet_yn', 'dis_parking_yn', 'entrance_ramp_yn', 'entrance_door_yn',
             'approach_road_yn', 'guide_facility_yn', 'accessible_room_yn', 'eval_info_raw')


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL/DB_URL 미설정 — DB 통합 테스트 생략')
class UpsertFacilitiesDbTests(_DbCase):
    """기구표 플래그·원문은 새 값이 NULL 이면 기존 값을 유지하고, 값이 있으면 갱신한다."""

    def _row(self):
        with self.engine.begin() as conn:
            return dict(conn.execute(self.text(
                "SELECT facl_name, base_dt::text AS base_dt, " + ", ".join(FLAG_COLS) +
                " FROM poi_facility_accessibility WHERE facl_inf_id = :i"), {'i': FACL_ID}).mappings().one())

    def test_null_flags_keep_existing_values_and_new_values_overwrite(self):
        with_eval = _facility(
            elevator_yn='Y', dis_toilet_yn='N', dis_parking_yn='Y', entrance_ramp_yn='N',
            entrance_door_yn='Y', approach_road_yn='Y', guide_facility_yn='N', accessible_room_yn='Y',
            eval_info_raw='승강기, 장애인전용주차구역')
        self.db_mobility.upsert_facilities([with_eval])

        # 기구표 조회를 끈 실행 / 조회 실패: 플래그·원문이 전부 None 으로 온다
        self.db_mobility.upsert_facilities([_facility(facl_name='시험 시설(개명)', base_dt='2026-10-01')])
        row = self._row()
        for col in FLAG_COLS:
            self.assertEqual(row[col], with_eval[col], col)
        # 플래그가 아닌 컬럼은 종전처럼 새 값으로 갱신된다
        self.assertEqual(row['facl_name'], '시험 시설(개명)')
        self.assertEqual(row['base_dt'], '2026-10-01')

        # 새 값이 있으면 갱신된다
        self.db_mobility.upsert_facilities([_facility(elevator_yn='N', eval_info_raw='주출입구 접근로')])
        row = self._row()
        self.assertEqual(row['elevator_yn'], 'N')
        self.assertEqual(row['eval_info_raw'], '주출입구 접근로')
        self.assertEqual(row['dis_parking_yn'], 'Y')      # 이번에 None 으로 온 항목은 유지


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL/DB_URL 미설정 — DB 통합 테스트 생략')
class UpsertEmergencySupportDbTests(_DbCase):
    """poi_emergency_support 자연키(유형, 이름, 도로명주소, 설치지점)로 적재·재적재가 된다."""

    @staticmethod
    def _support(install_desc, **kw):
        base = {
            'support_type': 'charge', 'sido_code': '9410000', 'sgg_name': '시험시', 'name': SUPPORT_NAME,
            'addr_road': '경기도 시험시 시청로 1', 'addr_jibun': None, 'zip_code': None,
            'latitude': 37.6, 'longitude': 126.7, 'tel': None, 'homepage': None, 'open_hours': None,
            'install_desc': install_desc, 'note': None, 'source': 'ITEST', 'confidence': 'H',
            'base_dt': '2026-09-01',
        }
        base.update(kw)
        return base

    def test_same_building_two_install_points_and_rerun(self):
        rows = [self._support('제3별관 1층 로비'), self._support('민원동 장애인화장실 옆')]
        self.db_mobility.upsert_emergency_support([dict(r) for r in rows])
        self.db_mobility.upsert_emergency_support([dict(r, tel='031-000-0000') for r in rows])
        with self.engine.begin() as conn:
            got = conn.execute(self.text(
                "SELECT install_desc, tel FROM poi_emergency_support WHERE name = :n ORDER BY install_desc"),
                {'n': SUPPORT_NAME}).fetchall()
        self.assertEqual([g[0] for g in got], ['민원동 장애인화장실 옆', '제3별관 1층 로비'])
        self.assertEqual({g[1] for g in got}, {'031-000-0000'})


if __name__ == '__main__':
    unittest.main()
