"""GG_TOILET 적재기 통합 테스트 — 실제 PostgreSQL 이 있을 때만 돈다.

ITEST_DB_URL(또는 DB_URL) 이 설정된 환경에서 임시 테이블을 만들어
[기존 행 → 1차 동기화 → 2차 재실행(멱등)] 을 검증한다. 원본 테이블은 건드리지 않는다.
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from db_mobility import public_toilet_match_keys  # noqa: E402

ITEST_URL = os.getenv('ITEST_DB_URL') or os.getenv('DB_URL')
TABLE = 'poi_public_toilet_info_itest'


def _row(name, road=None, jibun=None, **kw):
    base = {
        'sido_code': '9410000', 'toilet_name': name, 'toilet_type': '공중화장실', 'basis': None,
        'addr_road': road, 'addr_jibun': jibun,
        'm_toilet_count': 1, 'm_urinal_count': 1, 'm_dis_toilet_count': 1, 'm_dis_urinal_count': 0,
        'm_child_toilet_count': 0, 'm_child_urinal_count': 0, 'f_toilet_count': 1,
        'f_dis_toilet_count': 1, 'f_child_toilet_count': 0, 'managing_org': '안양시',
        'phone_number': None, 'open_time': '상시', 'open_time_detail': None, 'install_dt': None,
        'latitude': 37.39, 'longitude': 126.95, 'owner_type': None, 'waste_process_type': '수세식',
        'safety_target_yn': None, 'emg_bell_yn': 'Y', 'emg_bell_location': None, 'cctv_yn': 'N',
        'diaper_table_yn': None, 'diaper_table_location': None, 'remodeled_dt': None, 'unisex_yn': 'N',
        'base_dt': '2025-07-17',
    }
    base.update(kw)
    return base


def state_id_of(case, name):
    with case.engine.begin() as conn:
        return conn.execute(case.text(
            "SELECT toilet_id FROM " + TABLE + " WHERE toilet_name=:n AND del_yn='N'"), {'n': name}).scalar()


class MatchKeyTests(unittest.TestCase):
    def test_keys_require_name_and_address(self):
        self.assertEqual(public_toilet_match_keys('A', None, None), [])
        self.assertEqual(public_toilet_match_keys('A', '경기도 안양시 동안구 동안로 66 (호계동)', None),
                         [('A', '경기도안양시동안구동안로66')])

    def test_region_prefix_is_stripped(self):
        a = public_toilet_match_keys('A', '만안구 안양로 317', None, '경기도 안양시')
        b = public_toilet_match_keys('A', '경기도 안양시 만안구 안양로 317', None, '경기도 안양시')
        self.assertEqual(a, b)

    def test_both_addresses_give_two_keys(self):
        keys = public_toilet_match_keys('A', '경기도 안양시 x 1', '경기도 안양시 y 2')
        self.assertEqual(len(keys), 2)


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL/DB_URL 미설정 — DB 통합 테스트 생략')
class SyncPublicToiletsDbTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import db_mobility
        from sqlalchemy import text
        cls.db_mobility = db_mobility
        cls.text = staticmethod(text)
        cls.engine = db_mobility.engine
        with cls.engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS ' + TABLE))
            conn.execute(text('CREATE TABLE ' + TABLE +
                              ' (LIKE poi_public_toilet_info INCLUDING DEFAULTS INCLUDING IDENTITY)'))
        cls._orig_table = db_mobility.PUBLIC_TOILET_TABLE
        db_mobility.PUBLIC_TOILET_TABLE = TABLE

    @classmethod
    def tearDownClass(cls):
        cls.db_mobility.PUBLIC_TOILET_TABLE = cls._orig_table
        with cls.engine.begin() as conn:
            conn.execute(cls.text('DROP TABLE IF EXISTS ' + TABLE))

    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute(self.text('TRUNCATE TABLE ' + TABLE))

    def _seed(self):
        sql = self.text(
            "INSERT INTO " + TABLE + " (sido_code, toilet_name, toilet_type, addr_road, addr_jibun,"
            " open_time_detail, latitude, base_dt, created_by)"
            " VALUES (:sido, :name, '공중화장실', :road, :jibun, :detail, :lat, '2022-12-31', 'SYS-MANUAL')"
            " RETURNING toilet_id")
        seeds = [
            # 이름+도로명 일치(괄호 병기 차이) + 상세시간 보유 → UPDATE·상세 유지
            dict(sido='9410000', name='어린이도서관 화장실', road='경기도 안양시 동안구 동안로 66 (호계동)', jibun=None, detail='10:00~19:00', lat=None),
            # 시 이름 없는 과거 주소 — 지번으로 매칭
            dict(sido='9410000', name='삼덕공원지하', road='만안구 안양로 317', jibun='만안구 안양동 782-24', detail=None, lat=None),
            # 원천에서 사라진 시설 → 논리삭제
            dict(sido='9410000', name='폐쇄된 화장실', road='경기도 안양시 만안구 없는로 1', jibun=None, detail='09:00~18:00', lat=None),
            # 동명이지만 주소가 다른 시설 → 매칭되면 안 됨(상세시간 복제 금지)
            dict(sido='9410000', name='어린이공원 화장실', road='경기도 안양시 동안구 갈산로 1', jibun=None, detail='06:00~22:00', lat=None),
            # 다른 지역(서울 안양천로) → 건드리면 안 됨
            dict(sido='9110000', name='안양천 화장실', road='서울특별시 양천구 안양천로 1', jibun=None, detail=None, lat=None),
        ]
        ids = {}
        with self.engine.begin() as conn:
            for s in seeds:
                ids[s['name']] = conn.execute(sql, s).scalar()
        return ids

    def _active(self):
        with self.engine.begin() as conn:
            rows = conn.execute(self.text(
                "SELECT toilet_id, toilet_name, del_yn, open_time_detail, base_dt::text FROM " + TABLE +
                " ORDER BY toilet_id")).fetchall()
        return {r[1]: r for r in rows}

    def test_sync_twice_is_idempotent_and_keeps_ids(self):
        ids = self._seed()
        new_rows = [
            _row('어린이도서관 화장실', road='경기도 안양시 동안구 동안로 66', jibun='경기도 안양시 동안구 호계동 1'),
            _row('삼덕공원지하', road='경기도 안양시 만안구 안양로 317', jibun='경기도 안양시 만안구 안양동 782-24'),
            _row('어린이공원 화장실', road='경기도 안양시 만안구 다른로 9'),   # 동명·다른 주소 → 신규
            _row('신규 화장실', road='경기도 안양시 동안구 새로 1'),
        ]
        # 삼덕공원지하 과거 행은 '만안구 안양동 782-24' 처럼 시 이름이 없다 → 지역 접두어를 떼고 비교해 매칭돼야 한다.
        first = self.db_mobility.sync_public_toilets(list(map(dict, new_rows)), addr_filter='경기도 안양시')
        self.assertEqual(first['matched'], 2)
        self.assertEqual(first['inserted'], 2)
        self.assertEqual(first['deleted'], 2)   # 폐쇄·동명 다른주소
        self.assertEqual(state_id_of(self, '삼덕공원지하'), ids['삼덕공원지하'])
        self.assertEqual(first['kept_detail'], 1)

        state = self._active()
        lib = state['어린이도서관 화장실']
        self.assertEqual(lib[0], ids['어린이도서관 화장실'])     # id 보존
        self.assertEqual(lib[2], 'N')
        self.assertEqual(lib[3], '10:00~19:00')                # 상세 유지
        self.assertEqual(lib[4], '2025-07-17')
        self.assertEqual(state['폐쇄된 화장실'][2], 'Y')
        self.assertEqual(state['안양천 화장실'][2], 'N')       # 다른 지역 불변
        with self.engine.begin() as conn:
            dup = conn.execute(self.text(
                "SELECT count(*) FROM " + TABLE + " WHERE toilet_name='어린이공원 화장실' AND del_yn='N'")).scalar()
            old_detail = conn.execute(self.text(
                "SELECT open_time_detail FROM " + TABLE + " WHERE toilet_name='어린이공원 화장실' AND del_yn='N'")).scalar()
        self.assertEqual(dup, 1)
        self.assertIsNone(old_detail)   # 동명 행의 상세시간이 복제되지 않았다

        second = self.db_mobility.sync_public_toilets(list(map(dict, new_rows)), addr_filter='경기도 안양시')
        self.assertEqual((second['matched'], second['inserted'], second['deleted']), (4, 0, 0))
        state2 = self._active()
        self.assertEqual({k: v[0] for k, v in state.items()}, {k: v[0] for k, v in state2.items()})
        self.assertEqual(state2['어린이도서관 화장실'][3], '10:00~19:00')

    def test_short_collection_aborts(self):
        self._seed()
        with self.assertRaises(RuntimeError):
            self.db_mobility.sync_public_toilets([_row('하나만', road='경기도 안양시 x 1')], addr_filter='경기도 안양시')

    def test_empty_collection_is_noop(self):
        self.assertEqual(self.db_mobility.sync_public_toilets([], addr_filter='경기도 안양시')['matched'], 0)


if __name__ == '__main__':
    unittest.main()
