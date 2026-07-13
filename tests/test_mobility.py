"""이동편의 어댑터 단위 테스트 — Issue #76.

네트워크/DB 없이 매핑·병합·플래그 파싱 로직만 검증한다.
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors.gbis import GbisCollector  # noqa: E402
from collectors.korail_conv import KorailConvCollector, ANYANG_STATIONS  # noqa: E402
from collectors.kowsi_facl import parse_eval_flags  # noqa: E402
from collectors.tour_bf import TourBfCollector, flag_from_text  # noqa: E402


class GbisMappingTests(unittest.TestCase):
    SAMPLE = {
        'routeId': 208000073, 'routeName': '안양똑버스01', 'routeTypeCd': 50,
        'routeTypeName': '수요응답형버스', 'regionName': '안양', 'adminName': '경기도 안양시',
        'startStationId': 208000212, 'startStationName': '수리산등산로입구',
        'endStationId': 208000069, 'endStationName': '안양역',
        'companyName': '보영운수DRT', 'companyTel': '070-7330-1076',
        'peekAlloc': 0, 'nPeekAlloc': 0, 'satPeekAlloc': 0, 'satNPeekAlloc': 0,
        'sunPeekAlloc': 0, 'sunNPeekAlloc': 0, 'wePeekAlloc': 0, 'weNPeekAlloc': 0,
        'upFirstTime': '06:00', 'upLastTime': '23:00',
        'downFirstTime': '06:00', 'downLastTime': '23:00',
    }

    def test_map_route_columns(self):
        row = GbisCollector.map_route(self.SAMPLE)
        self.assertEqual(row['route_id'], 208000073)
        self.assertEqual(row['route_name'], '안양똑버스01')
        self.assertEqual(row['admin_name'], '경기도 안양시')
        self.assertEqual(row['peek_alloc'], 0)
        self.assertEqual(row['up_first_time'], '06:00')
        self.assertIn('base_dt', row)

    def test_map_route_missing_fields_are_none(self):
        row = GbisCollector.map_route({'routeId': '1', 'routeName': 'x'})
        self.assertIsNone(row['peek_alloc'])
        self.assertIsNone(row['start_station_id'])


class KorailMergeTests(unittest.TestCase):
    STATION = [{'stn_cd': '3900001', 'stn_nm': '안양', 'elevt_cnt': 4, 'esclt_cnt': 6,
                'gen_tolt_estnc': 'Y', 'nrsrm_estnc': 'Y', 'altm_lead_cntr_estnc': 'N'}]
    WEAK = [{'stn_cd': '3900001', 'stn_nm': '안양', 'whlch_liftt_cnt': 0,
             'pwdbs_slwy_estnc': 'Y', 'pwdbs_tolt_estnc': 'Y'},
            {'stn_cd': '3900002', 'stn_nm': '가야', 'whlch_liftt_cnt': 1,
             'pwdbs_slwy_estnc': 'N', 'pwdbs_tolt_estnc': 'N'}]

    def test_merge_by_stn_cd(self):
        rows = KorailConvCollector.merge(self.STATION, self.WEAK)
        by_cd = {r['stn_cd']: r for r in rows}
        self.assertEqual(len(rows), 2)
        anyang = by_cd['3900001']
        self.assertEqual(anyang['elevator_cnt'], 4)
        self.assertEqual(anyang['wheelchair_lift_cnt'], 0)
        self.assertEqual(anyang['dis_slope_yn'], 'Y')
        self.assertEqual(anyang['anyang_yn'], 'Y')

    def test_weak_only_station_still_row(self):
        rows = KorailConvCollector.merge(self.STATION, self.WEAK)
        by_cd = {r['stn_cd']: r for r in rows}
        gaya = by_cd['3900002']
        self.assertIsNone(gaya['elevator_cnt'])
        self.assertEqual(gaya['wheelchair_lift_cnt'], 1)
        self.assertEqual(gaya['anyang_yn'], 'N')

    def test_anyang_station_list(self):
        self.assertIn('범계', ANYANG_STATIONS)
        self.assertEqual(len(ANYANG_STATIONS), 7)


class KowsiEvalFlagTests(unittest.TestCase):
    def test_full_eval_info(self):
        flags = parse_eval_flags('승강기, 장애인사용가능화장실, 주출입구 접근로')
        self.assertEqual(flags['elevator_yn'], 'Y')
        self.assertEqual(flags['dis_toilet_yn'], 'Y')
        self.assertEqual(flags['approach_road_yn'], 'Y')
        self.assertEqual(flags['dis_parking_yn'], 'N')
        self.assertEqual(flags['entrance_ramp_yn'], 'N')

    def test_empty_eval_info_all_none(self):
        flags = parse_eval_flags(None)
        self.assertTrue(all(v is None for v in flags.values()))


class TourBfFlagTests(unittest.TestCase):
    def test_flag_heuristics(self):
        self.assertEqual(flag_from_text('장애인 화장실 있음'), 'Y')
        self.assertEqual(flag_from_text('엘리베이터 없음'), 'N')
        self.assertIsNone(flag_from_text(''))
        self.assertIsNone(flag_from_text(None))

    def test_map_row(self):
        area = {'title': '김중업건축박물관', 'addr1': '경기도 안양시 만안구', 'addr2': '',
                'mapx': '126.9166', 'mapy': '37.4111', 'contentid': '2464432'}
        detail = {'restroom': '장애인 화장실 있음', 'elevator': '엘리베이터 없음',
                  'parking': '장애인 주차장 있음', 'publictransport': '지하철 안양역 하차 후 버스 환승'}
        row = TourBfCollector.map_row(area, detail)
        self.assertEqual(row['fclt_name'], '김중업건축박물관')
        self.assertEqual(row['sido_code'], '9410000')
        self.assertEqual(row['toilet_yn'], 'Y')
        self.assertEqual(row['elevator_yn'], 'N')
        self.assertEqual(row['subway_yn'], 'Y')
        self.assertEqual(row['bus_stop_yn'], 'Y')
        self.assertAlmostEqual(row['latitude'], 37.4111)


class RegistryTests(unittest.TestCase):
    def test_mobility_registry(self):
        from mobility_pipeline import MOBILITY_EXT_SYS, MOBILITY_COLLECTORS
        for key in ('GBIS', 'KORAIL_CONV', 'KOWSI_FACL', 'TOUR_BF_API'):
            self.assertIn(key, MOBILITY_EXT_SYS)
            self.assertTrue(callable(MOBILITY_COLLECTORS[key]))

    def test_main_registry_contains_mobility(self):
        import main
        for key in ('GBIS', 'KORAIL_CONV', 'KOWSI_FACL', 'TOUR_BF_API'):
            self.assertIn(key, main._COLLECTOR_REGISTRY)


if __name__ == '__main__':
    unittest.main()
