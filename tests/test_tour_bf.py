"""TOUR_BF_API 목록 조회(법정동 코드)·적재 범위 논리삭제 테스트 — #120 (v1.17.1).

- 수집기: 시군구 코드마다 조회해 합치고 contentid 로 중복 제거, 결과 코드 오류는 예외
- 적재기: ITEST_DB_URL 이 있을 때만 임시 테이블로 [기존 행 → 동기화] 를 검증
"""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors.tour_bf import TourBfCollector  # noqa: E402

# 통합 테스트 대상 DB 는 ITEST_DB_URL 로만 정한다(DB_URL 로 대신하지 않는다) — test_gg_toilet_db 와 같은 이유.
ITEST_URL = os.getenv('ITEST_DB_URL') or None
TABLE = 'poi_tour_bf_facility_itest'


def _page(items, total, code='0000'):
    if code is None:   # 결과 코드가 없는 응답(게이트웨이 오류 본문 등)
        return {'response': {'body': {'items': '', 'totalCount': 0}}}
    return {'response': {'header': {'resultCode': code, 'resultMsg': 'OK'},
                         'body': {'items': {'item': items} if items else '', 'totalCount': total}}}


def _collector():
    c = TourBfCollector(api_info={'auth': 'k', 'ext_url': 'https://example.test/KorWithService2'})
    c.pause = lambda: None
    return c


class ListUrlTests(unittest.TestCase):
    def test_uses_legal_dong_codes(self):
        with mock.patch.dict(os.environ, {'TOUR_LDONG_REGN_CD': '41', 'TOUR_LDONG_SIGNGU_CDS': '171, 173'}):
            c = _collector()
            self.assertEqual(c.ldong_signgu_cds, ['171', '173'])
            url = c._list_url('173', 2)
        self.assertIn('/areaBasedList2?', url)
        self.assertIn('&lDongRegnCd=41&lDongSignguCd=173', url)
        self.assertIn('&pageNo=2', url)
        self.assertNotIn('areaCode=', url)
        self.assertNotIn('sigunguCode=', url)

    def test_defaults_are_anyang(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop('TOUR_LDONG_REGN_CD', None)
            os.environ.pop('TOUR_LDONG_SIGNGU_CDS', None)
            c = _collector()
            self.assertEqual((c.ldong_regn_cd, c.ldong_signgu_cds), ('41', ['171', '173']))

    def test_rejects_non_numeric_codes(self):
        for bad in ('171&x=1', '', ' , '):
            with mock.patch.dict(os.environ, {'TOUR_LDONG_SIGNGU_CDS': bad}):
                with self.assertRaises(ValueError):
                    _collector().ldong_signgu_cds
        with mock.patch.dict(os.environ, {'TOUR_LDONG_REGN_CD': '41;'}):
            with self.assertRaises(ValueError):
                _collector().ldong_regn_cd


class FetchAreaListTests(unittest.TestCase):
    def _run(self, pages):
        calls = []

        def fake_get_json(url):
            calls.append(url)
            key = (url.split('lDongSignguCd=')[1].split('&')[0], int(url.split('pageNo=')[1]))
            return pages[key]

        with mock.patch.dict(os.environ, {'TOUR_LDONG_SIGNGU_CDS': '171,173'}):
            c = _collector()
            c.get_json = fake_get_json
            return c.fetch_area_list(), calls

    def test_merges_districts_paginates_and_dedupes(self):
        page171 = [{'contentid': str(i), 'title': 'M%d' % i} for i in range(1, 101)]
        pages = {
            ('171', 1): _page(page171, 101),
            ('171', 2): _page([{'contentid': '101', 'title': 'M101'}], 101),
            # 경계 시설이 두 구 응답에 모두 나오는 경우 — contentid 로 하나만 남긴다
            ('173', 1): _page([{'contentid': '5', 'title': 'M5'}, {'contentid': '900', 'title': 'D900'}], 2),
        }
        items, calls = self._run(pages)
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(items), 102)
        self.assertEqual([i['title'] for i in items][-1], 'D900')
        self.assertEqual(sum(1 for i in items if i['contentid'] == '5'), 1)

    def test_single_item_dict_and_empty_district(self):
        pages = {
            ('171', 1): _page({'contentid': '1', 'title': 'only'}, 1),
            ('173', 1): _page(None, 0),
        }
        items, _ = self._run(pages)
        self.assertEqual([i['title'] for i in items], ['only'])

    def test_missing_total_count_still_paginates(self):
        full = [{'contentid': 'a%d' % i, 'title': 'A%d' % i} for i in range(100)]
        pages = {
            ('171', 1): _page(full, None),
            ('171', 2): _page([{'contentid': 'b1', 'title': 'B1'}], None),
            ('173', 1): _page(None, 0),
        }
        items, calls = self._run(pages)
        self.assertEqual(len(items), 101)
        self.assertEqual(len(calls), 3)

    def test_missing_result_code_raises(self):
        pages = {('171', 1): _page(None, 0, code=None), ('173', 1): _page([{'contentid': '1'}], 1)}
        with self.assertRaises(RuntimeError) as cm:
            self._run(pages)
        self.assertIn('(없음)', str(cm.exception))

    def test_error_result_code_raises(self):
        pages = {('171', 1): _page(None, 0, code='30'), ('173', 1): _page(None, 0)}
        with self.assertRaises(RuntimeError) as cm:
            self._run(pages)
        self.assertIn('resultCode=30', str(cm.exception))


def _row(name, road='경기도 안양시 동안구 평촌대로 1', **kw):
    base = {
        'sido_code': '9410000', 'fclt_name': name, 'addr_road': road, 'addr_jibun': None,
        'latitude': 37.39, 'longitude': 126.95, 'base_dt': '2026-10-13', 'detail_raw': None,
    }
    for col in ('toilet_yn', 'elevator_yn', 'parking_yn', 'slope_yn', 'subway_yn', 'bus_stop_yn',
                'wheelchair_rent_yn', 'tactile_map_yn', 'audio_guide_yn', 'nursing_room_yn',
                'accessible_room_yn', 'stroller_rent_yn'):
        base[col] = 'Y'
    base.update(kw)
    return base


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL 미설정 — DB 통합 테스트 생략')
class UpsertTourBfDbTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import db_mobility
        from sqlalchemy import create_engine, text
        cls.db_mobility = db_mobility
        cls.text = staticmethod(text)
        cls.engine = create_engine(ITEST_URL)
        cls._orig_engine = db_mobility.engine
        db_mobility.engine = cls.engine
        with cls.engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS ' + TABLE))
            conn.execute(text('CREATE TABLE ' + TABLE +
                              ' (LIKE poi_tour_bf_facility INCLUDING DEFAULTS INCLUDING IDENTITY)'))
        cls._orig_table = db_mobility.TOUR_BF_TABLE
        db_mobility.TOUR_BF_TABLE = TABLE

    @classmethod
    def tearDownClass(cls):
        cls.db_mobility.TOUR_BF_TABLE = cls._orig_table
        with cls.engine.begin() as conn:
            conn.execute(cls.text('DROP TABLE IF EXISTS ' + TABLE))
        cls.db_mobility.engine = cls._orig_engine
        cls.engine.dispose()

    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute(self.text('TRUNCATE TABLE ' + TABLE))
        sql = self.text(
            "INSERT INTO " + TABLE + " (sido_code, fclt_name, addr_road, base_dt, created_by)"
            " VALUES (:sido, :name, :road, '2026-10-04', 'SYS-BACH') RETURNING fclt_id")
        seeds = [
            ('9410000', '안양문화원', '경기도 안양시 만안구 현충로 53'),
            ('9410000', '평촌중앙공원', '경기도 안양시 동안구 시민대로 1'),
            ('9410000', '사라진 게스트하우스', '경기도 안양시 만안구 없는로 1'),
            ('9410000', '수원 화성', '경기도 수원시 팔달구 정조로 825'),   # 범위 밖(다른 시)
            ('9110000', '안양천 산책로', '서울특별시 양천구 안양천로 1'),   # 범위 밖(다른 시도)
        ]
        self.ids = {}
        with self.engine.begin() as conn:
            for sido, name, road in seeds:
                self.ids[name] = conn.execute(sql, {'sido': sido, 'name': name, 'road': road}).scalar()

    def _state(self):
        with self.engine.begin() as conn:
            rows = conn.execute(self.text(
                "SELECT fclt_id, fclt_name, del_yn, base_dt::text, deleted_by FROM " + TABLE)).fetchall()
        return {r[1]: r for r in rows}

    def test_sync_updates_inserts_and_deletes_missing_in_scope(self):
        rows = [
            _row('안양문화원', road='경기도 안양시 만안구 현충로 53'),
            _row('평촌중앙공원', road='경기도 안양시 동안구 시민대로 1'),
            _row('안양예술공원', road='경기도 안양시 만안구 예술공원로 1'),
        ]
        n = self.db_mobility.upsert_tour_bf(rows, addr_filter='경기도 안양시')
        self.assertEqual(n, 3)
        st = self._state()
        self.assertEqual(st['안양문화원'][0], self.ids['안양문화원'])      # 제자리 갱신(ID 보존)
        self.assertEqual(st['안양문화원'][3], '2026-10-13')
        self.assertEqual(st['안양예술공원'][2], 'N')                       # 신규
        self.assertEqual(st['사라진 게스트하우스'][2], 'Y')                 # 범위 안 + 응답에 없음
        self.assertEqual(st['사라진 게스트하우스'][4], 'SYS-BACH')
        self.assertEqual(st['수원 화성'][2], 'N')                          # 범위 밖 — 그대로
        self.assertEqual(st['안양천 산책로'][2], 'N')
        # 재실행 — 같은 결과, 추가 삭제·중복 없음
        self.db_mobility.upsert_tour_bf(rows, addr_filter='경기도 안양시')
        st2 = self._state()
        self.assertEqual(len(st2), 6)
        self.assertEqual({k: v[2] for k, v in st2.items()}, {k: v[2] for k, v in st.items()})

    def test_same_name_outside_scope_is_not_overwritten(self):
        # 범위 밖(수원)에 같은 이름의 행이 있어도 안양 장소는 그 행을 덮어쓰지 않고 새로 만든다
        rows = [_row('안양문화원', road='경기도 안양시 만안구 현충로 53'),
                _row('평촌중앙공원', road='경기도 안양시 동안구 시민대로 1'),
                _row('수원 화성', road='경기도 안양시 동안구 가상로 9')]
        self.db_mobility.upsert_tour_bf(rows, addr_filter='경기도 안양시')
        with self.engine.begin() as conn:
            got = conn.execute(self.text(
                "SELECT fclt_id, addr_road, del_yn FROM " + TABLE + " WHERE fclt_name='수원 화성' ORDER BY fclt_id")).fetchall()
        self.assertEqual(len(got), 2)
        self.assertEqual(got[0][0], self.ids['수원 화성'])
        self.assertEqual(got[0][1], '경기도 수원시 팔달구 정조로 825')   # 원래 행 그대로
        self.assertEqual([g[2] for g in got], ['N', 'N'])

    def test_same_name_in_scope_kept_separately(self):
        rows = [_row('안양문화원', road='경기도 안양시 만안구 현충로 53'),
                _row('평촌중앙공원', road='경기도 안양시 동안구 시민대로 1'),
                _row('평촌중앙공원', road='경기도 안양시 동안구 다른로 2')]
        self.db_mobility.upsert_tour_bf(rows, addr_filter='경기도 안양시')
        with self.engine.begin() as conn:
            got = conn.execute(self.text(
                "SELECT fclt_id, addr_road FROM " + TABLE + " WHERE fclt_name='평촌중앙공원' AND del_yn='N' ORDER BY fclt_id")).fetchall()
        self.assertEqual([g[1] for g in got], ['경기도 안양시 동안구 시민대로 1', '경기도 안양시 동안구 다른로 2'])
        self.assertEqual(got[0][0], self.ids['평촌중앙공원'])

    def test_reappearing_place_is_reactivated(self):
        base = [_row('안양문화원', road='경기도 안양시 만안구 현충로 53'),
                _row('평촌중앙공원', road='경기도 안양시 동안구 시민대로 1')]
        self.db_mobility.upsert_tour_bf(base, addr_filter='경기도 안양시')
        self.assertEqual(self._state()['사라진 게스트하우스'][2], 'Y')
        again = base + [_row('사라진 게스트하우스', road='경기도 안양시 만안구 없는로 1')]
        self.db_mobility.upsert_tour_bf(again, addr_filter='경기도 안양시')
        with self.engine.begin() as conn:
            got = conn.execute(self.text(
                "SELECT fclt_id, del_yn, deleted_at FROM " + TABLE + " WHERE fclt_name='사라진 게스트하우스'")).fetchall()
        self.assertEqual(len(got), 1)                      # 새 행을 만들지 않고
        self.assertEqual(got[0][0], self.ids['사라진 게스트하우스'])  # 같은 ID 를
        self.assertEqual(got[0][1], 'N')                   # 되살린다
        self.assertIsNone(got[0][2])

    def test_partial_response_keeps_rows(self):
        # 범위 안 기존 활성 3건 중 1건만 수집 — 50% 미만이라 삭제를 보류한다
        rows = [_row('안양문화원', road='경기도 안양시 만안구 현충로 53')]
        with self.assertLogs('db', level='WARNING') as cm:
            self.db_mobility.upsert_tour_bf(rows, addr_filter='경기도 안양시')
        self.assertTrue(any('논리삭제' in m and '보류' in m for m in cm.output))
        st = self._state()
        self.assertEqual(st['평촌중앙공원'][2], 'N')
        self.assertEqual(st['사라진 게스트하우스'][2], 'N')

    def test_empty_response_does_nothing(self):
        self.assertEqual(self.db_mobility.upsert_tour_bf([], addr_filter='경기도 안양시'), 0)
        self.assertTrue(all(v[2] == 'N' for v in self._state().values()))


if __name__ == '__main__':
    unittest.main()
