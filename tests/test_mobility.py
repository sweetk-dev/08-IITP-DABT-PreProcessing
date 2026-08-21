"""이동편의 어댑터 단위 테스트 — Issue #76, #85.

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
from collectors.mobility_base import to_float, to_yn  # noqa: E402
from collectors.korail_conv import KorailConvCollector, ANYANG_STATIONS  # noqa: E402
from collectors.kowsi_facl import parse_eval_flags, check_api_error, KowsiFaclCollector  # noqa: E402
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


class GbisStationMappingTests(unittest.TestCase):
    """경유정류소 목록조회 응답 매핑 — Issue #85. 실응답 형태 기준."""

    SAMPLE = {
        'centerYn': 'N', 'districtCd': 2, 'mobileNo': ' 09327', 'regionName': '안양',
        'stationId': 208000363, 'stationName': '안양박물관.김중업건축박물관',
        'x': 126.9188611, 'y': 37.4176389, 'adminName': '경기도 안양시',
        'stationSeq': 2, 'turnSeq': 25, 'turnYn': 'N',
    }

    def test_map_station_columns(self):
        row = GbisCollector.map_station(self.SAMPLE)
        self.assertEqual(row['station_id'], 208000363)
        self.assertEqual(row['station_name'], '안양박물관.김중업건축박물관')
        self.assertEqual(row['admin_name'], '경기도 안양시')
        self.assertEqual(row['center_yn'], 'N')
        self.assertEqual(row['district_cd'], 2)
        self.assertIn('base_dt', row)

    def test_mobile_no_is_trimmed(self):
        """응답의 mobileNo 는 선행 공백이 붙어 온다 — 저장 전 trim 되어야 한다."""
        row = GbisCollector.map_station(self.SAMPLE)
        self.assertEqual(row['mobile_no'], '09327')

    def test_x_is_longitude_y_is_latitude(self):
        """GBIS 는 x=경도, y=위도. 뒤바뀌면 안양 좌표가 국외로 나간다."""
        row = GbisCollector.map_station(self.SAMPLE)
        self.assertAlmostEqual(row['latitude'], 37.4176389)
        self.assertAlmostEqual(row['longitude'], 126.9188611)
        self.assertTrue(33.0 < row['latitude'] < 39.0)
        self.assertTrue(124.0 < row['longitude'] < 132.0)

    def test_map_station_missing_fields_are_none(self):
        row = GbisCollector.map_station({'stationId': '1', 'stationName': 'x'})
        self.assertIsNone(row['latitude'])
        self.assertIsNone(row['longitude'])
        self.assertIsNone(row['center_yn'])
        self.assertIsNone(row['mobile_no'])

    def test_map_route_station_columns(self):
        row = GbisCollector.map_route_station(241253001, self.SAMPLE)
        self.assertEqual(row['route_id'], 241253001)
        self.assertEqual(row['station_id'], 208000363)
        self.assertEqual(row['station_seq'], 2)
        self.assertEqual(row['turn_seq'], 25)
        self.assertEqual(row['turn_yn'], 'N')

    def test_collect_stations_dedupes_and_builds_links(self):
        """두 노선이 같은 정류장을 경유해도 정류장은 1건, 관계는 2건이어야 한다."""
        other = dict(self.SAMPLE, stationId=208000364, stationSeq=3)
        pages = {
            '1': [self.SAMPLE, other],
            '2': [self.SAMPLE],
        }

        class Stub(GbisCollector):
            def __init__(self):
                pass

            @property
            def api_key(self):
                return 'k'

            def _route_station_url(self, route_id):
                return str(route_id)

            def get_json(self, url):
                return {'response': {'msgBody': {'busRouteStationList': pages[url]}}}

            @staticmethod
            def pause():
                return None

        stations, links = Stub().collect_stations(['1', '2'])
        self.assertEqual(len(stations), 2)
        self.assertEqual(len(links), 3)
        self.assertEqual({s['station_id'] for s in stations}, {208000363, 208000364})
        self.assertEqual(sorted(l['route_id'] for l in links), [1, 1, 2])

    def test_collect_stations_skips_rows_without_seq(self):
        """station_seq 는 노선-정류장 자연키 구성요소 — 없으면 관계에서 제외한다."""
        no_seq = dict(self.SAMPLE)
        no_seq.pop('stationSeq')

        class Stub(GbisCollector):
            def __init__(self):
                pass

            def _route_station_url(self, route_id):
                return str(route_id)

            def get_json(self, url):
                return {'response': {'msgBody': {'busRouteStationList': no_seq}}}

            @staticmethod
            def pause():
                return None

        stations, links = Stub().collect_stations(['1'])
        self.assertEqual(len(stations), 1)
        self.assertEqual(links, [])


class MobilityHelperTests(unittest.TestCase):
    def test_to_float(self):
        self.assertAlmostEqual(to_float('37.5'), 37.5)
        self.assertIsNone(to_float(''))
        self.assertIsNone(to_float(None))
        self.assertIsNone(to_float('abc'))

    def test_to_yn(self):
        self.assertEqual(to_yn('Y'), 'Y')
        self.assertEqual(to_yn('n'), 'N')
        self.assertEqual(to_yn(True), 'Y')
        self.assertIsNone(to_yn(None))
        self.assertIsNone(to_yn('maybe'))


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


class KowsiChunkedScanTests(unittest.TestCase):
    """Issue #78 — 에러 감지 + 분할 수집 상태 전이."""

    def _collector(self, tmpdir, max_pages, pages):
        os.environ['KOWSI_STATE_PATH'] = os.path.join(tmpdir, 'state.json')
        os.environ['KOWSI_MAX_PAGES'] = str(max_pages)
        os.environ['KOWSI_PAGE_SIZE'] = '2'
        os.environ['KOWSI_FETCH_EVAL'] = 'OFF'
        os.environ['DATA_GO_KR_API_KEY'] = 'test-key'
        c = KowsiFaclCollector(api_info={}, stats_src={})
        import xml.etree.ElementTree as ET

        def fake_get_xml(url):
            import re as _re
            page = int(_re.search(r'pageNo=(\d+)', url).group(1))
            body = pages[page - 1]
            return ET.fromstring(body)

        c.get_xml = fake_get_xml
        return c

    @staticmethod
    def _page_xml(total, items):
        servs = ''.join(
            '<servList><faclInfId>%s</faclInfId><faclNm>%s</faclNm><lcMnad>%s</lcMnad></servList>'
            % it for it in items
        )
        return ('<facInfoList><totalCount>%d</totalCount>%s</facInfoList>' % (total, servs))

    def test_error_response_raises(self):
        import xml.etree.ElementTree as ET
        root = ET.fromstring(
            '<facInfoList><resultCode>10</resultCode>'
            '<resultMessage>INVALID_REQUEST_PARAMETER_ERROR</resultMessage></facInfoList>'
        )
        with self.assertRaises(RuntimeError):
            check_api_error(root)

    def test_chunk_advances_state(self):
        import json, tempfile
        with tempfile.TemporaryDirectory() as tmp:
            total = 6  # page_size 2 -> 3 pages
            pages = [
                self._page_xml(total, [('1', 'A', '경기도 안양시 1'), ('2', 'B', '서울')]),
                self._page_xml(total, [('3', 'C', '부산'), ('4', 'D', '경기도 안양시 2')]),
                self._page_xml(total, [('5', 'E', '대구'), ('6', 'F', '인천')]),
            ]
            c = self._collector(tmp, max_pages=2, pages=pages)
            rows = c.collect()
            self.assertEqual([r['facl_inf_id'] for r in rows], ['1', '4'])
            state = json.load(open(os.path.join(tmp, 'state.json'), encoding='utf-8'))
            self.assertEqual(state['next_page'], 3)
            self.assertIsNone(state['cycle_completed_at'])
            # 2회차: p3 스캔 후 완주 처리
            rows2 = c.collect()
            self.assertEqual(rows2, [])
            state = json.load(open(os.path.join(tmp, 'state.json'), encoding='utf-8'))
            self.assertEqual(state['next_page'], 1)
            self.assertIsNotNone(state['cycle_completed_at'])
            # 3회차: 재스캔 주기 미도래 -> skip
            rows3 = c.collect()
            self.assertEqual(rows3, [])


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


class KorailCoordTests(unittest.TestCase):
    """Issue #80 — 역위치 CSV 좌표 보강."""

    def _write_csv(self, tmpdir, name, header, rows, enc='cp949'):
        import os as _os
        path = _os.path.join(tmpdir, name)
        with open(path, 'w', encoding=enc, newline='') as f:
            f.write(header + '\n')
            for r in rows:
                f.write(r + '\n')
        return path

    def test_norm_station_name(self):
        from collectors.korail_conv import norm_station_name
        self.assertEqual(norm_station_name('안양역'), '안양')
        self.assertEqual(norm_station_name('총신대입구(이수)'), '총신대입구')
        self.assertEqual(norm_station_name(' 인 덕 원 '), '인덕원')
        self.assertEqual(norm_station_name('역'), '역')     # 한 글자는 접미 제거 안 함
        self.assertEqual(norm_station_name(None), '')

    def test_load_station_coords_krna_header(self):
        import tempfile
        from collectors.korail_conv import load_station_coords
        with tempfile.TemporaryDirectory() as d:
            p = self._write_csv(d, 'line1.csv', '철도운영기관,선명,역명,경도,위도', [
                '코레일,1호선,안양,126.922647,37.401929',
                '코레일,1호선,석수,126.902233,37.435161',
            ])
            coords = load_station_coords([p])
        self.assertEqual(coords['안양'], (37.401929, 126.922647))
        self.assertIn('석수', coords)

    def test_load_station_coords_korail_header_and_merge_files(self):
        import tempfile
        from collectors.korail_conv import load_station_coords
        with tempfile.TemporaryDirectory() as d:
            p1 = self._write_csv(d, 'a.csv', '지역본부,역명,위도,경도,출입구 개수', [
                '수도권광역,수원,37.26608,126.999231,6',
            ])
            p2 = self._write_csv(d, 'b.csv', '철도운영기관,선명,역명,경도,위도', [
                '코레일,4호선,범계,126.950752,37.389783',
                '코레일,4호선,수원,126.999999,37.266100',   # 중복 역명 -> 최초값 유지
            ])
            coords = load_station_coords([p1, p2])
        self.assertEqual(coords['수원'], (37.26608, 126.999231))
        self.assertEqual(coords['범계'], (37.389783, 126.950752))

    def test_load_station_coords_missing_file_skipped(self):
        from collectors.korail_conv import load_station_coords
        self.assertEqual(load_station_coords(['/no/such/file.csv']), {})

    def test_enrich_coords_sets_and_preserves_none(self):
        rows = [
            {'stn_cd': '1', 'stn_name': '안양'},
            {'stn_cd': '2', 'stn_name': '미지역'},
        ]
        hit, miss = KorailConvCollector.enrich_coords(
            rows, {'안양': (37.401929, 126.922647)})
        self.assertEqual((hit, miss), (1, 1))
        self.assertEqual(rows[0]['latitude'], 37.401929)
        self.assertIsNone(rows[1]['latitude'])     # 미매칭은 None (COALESCE 보존 대상)
