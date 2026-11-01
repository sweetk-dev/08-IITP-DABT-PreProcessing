"""STD_CALLTAXI(전국교통약자이동지원센터 표준데이터) 단위 테스트 — 네트워크·DB 없이.

원천 행 예시는 2026-11-02 OpenAPI 실측 응답의 칸 구성을 따른다(값은 시험용).
"""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors import std_calltaxi as S  # noqa: E402


def src(**kw):
    base = {
        'tfcwkerMvmnCnterNm': '서천군 교통약자 이동지원센터',
        'rdnmadr': '충청남도 서천군 서천읍 충절로81번길 7, 청림주상복합빌딩 1층7호',
        'lnmadr': '충청남도 서천군 서천읍 군사리 649-2',
        'latitude': '36.07484589', 'longitude': '126.6914382',
        'carHoldCo': '9', 'carHoldKnd': '카니발', 'slopeVhcleCo': '9', 'liftVhcleCo': '0',
        'rceptPhoneNumber': '041-951-0774', 'rceptItnadr': '없음', 'appSvcNm': '없음',
        'weekdayRceptOpenHhmm': '09:00', 'weekdayRceptColseHhmm': '18:00',
        'wkendRceptOpenHhmm': '00:00', 'wkendRceptCloseHhmm': '00:00',
        'weekdayOperOpenHhmm': '09:00', 'weekdayOperColseHhmm': '18:00',
        'wkendOperOpenHhmm': '00:00', 'wkendOperCloseHhmm': '00:00',
        'beffatResvePd': '실시간', 'useLmtt': '비장애인, 65세미만', 'insideOpratArea': '서천군 전역',
        'outsideOpratArea': '충청남도 및 인접시도', 'useTrget': '중증보행장애인+동반가족및보호자',
        'useCharge': '관내 기본 1300원', 'institutionNm': '(사)한국지체장애인충남협회서천군지회',
        'phoneNumber': '041-951-0774', 'referenceDate': '2025-12-19', 'insttCode': '4580000',
        'insttNm': '충청남도 서천군',
    }
    base.update(kw)
    return base


class MapRowTests(unittest.TestCase):
    def test_basic_mapping(self):
        r = S.map_row(src())
        self.assertEqual(r['support_type'], 'calltaxi')
        self.assertEqual(r['source'], 'STD_CALLTAXI')
        self.assertEqual(r['sido_code'], '9440000')
        self.assertEqual(r['sgg_name'], '서천군')
        self.assertEqual(r['tel'], '041-951-0774')
        self.assertIsNone(r['homepage'])
        self.assertEqual(r['base_dt'], '2025-12-19')
        self.assertAlmostEqual(r['latitude'], 36.07484589)
        self.assertIsNone(r['install_desc'])
        # 주말 00:00~00:00 은 적지 않는다
        self.assertEqual(r['open_hours'], '접수 평일 09:00-18:00 / 운행 평일 09:00-18:00')
        self.assertIn('차량 9대(슬로프 9, 카니발)', r['note'])
        self.assertIn('사전예약 실시간', r['note'])
        self.assertNotIn('앱', r['note'])                   # '없음' 은 버린다
        self.assertNotIn('(041-951-0774)', r['note'])       # 관리기관 전화가 접수 전화와 같으면 다시 적지 않음

    def test_24h_and_org_phone(self):
        r = S.map_row(src(weekdayRceptOpenHhmm='00:00', weekdayRceptColseHhmm='23:59',
                          wkendRceptOpenHhmm='07:00', wkendRceptCloseHhmm='22:00', phoneNumber='031-000-0000'))
        self.assertTrue(r['open_hours'].startswith('접수 평일 24시간·주말 07:00-22:00'))
        self.assertIn('(031-000-0000)', r['note'])

    def test_homepage_extraction(self):
        cases = {
            'https://ggsts.gg.go.kr/': 'https://ggsts.gg.go.kr/',
            'call.gwd.go.kr': 'call.gwd.go.kr',
            '충청북도광역이동지원센터 (essetel.co.kr)': 'essetel.co.kr',
            '인터넷 예약 불가': None,
            '해당없음': None,
            '전남광역콜 앱': None,
        }
        for raw, want in cases.items():
            self.assertEqual(S.extract_homepage(raw), want, raw)

    def test_unknown_sido_is_null_and_sgg_from_instt(self):
        # 사무실은 목포, 담당은 신안군 — 시군구는 제공기관명 기준
        r = S.map_row(src(rdnmadr='전남광주통합특별시 목포시 수강로3번길 14', insttNm='전남광주통합특별시 신안군'))
        self.assertIsNone(r['sido_code'])
        self.assertEqual(r['sgg_name'], '신안군')

    def test_sgg_falls_back_to_address(self):
        r = S.map_row(src(rdnmadr='경기도 의왕시 왕소나무길 29-15', insttNm='의왕도시공사'))
        self.assertEqual(r['sido_code'], '9410000')
        self.assertEqual(r['sgg_name'], '의왕시')

    def test_old_sido_alias_and_jibun_only(self):
        r = S.map_row(src(rdnmadr='', lnmadr='전라북도 전주시 덕진구 1', insttNm='전라북도'))
        self.assertEqual(r['sido_code'], '9520000')
        self.assertIsNone(r['addr_road'])
        self.assertEqual(r['sgg_name'], '전주시')

    def test_bad_coords_and_date_are_null(self):
        r = S.map_row(src(latitude='0', longitude='abc', referenceDate='미상'))
        self.assertIsNone(r['latitude'])
        self.assertIsNone(r['longitude'])
        self.assertIsNone(r['base_dt'])

    def test_note_truncated(self):
        r = S.map_row(src(useTrget='가' * 2000))
        self.assertEqual(len(r['note']), S.NOTE_MAX)
        self.assertTrue(r['note'].endswith('…'))

    def test_no_name_skipped(self):
        self.assertIsNone(S.map_row(src(tfcwkerMvmnCnterNm=' ')))


class BuildRowsTests(unittest.TestCase):
    def test_duplicate_center_keeps_latest(self):
        a = src(referenceDate='2024-12-06', carHoldCo='9')
        b = src(referenceDate='2026-09-18', carHoldCo='13')
        rows = S.build_rows([a, b, src(tfcwkerMvmnCnterNm='다른 센터')])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['base_dt'], '2026-09-18')
        self.assertIn('차량 13대', rows[0]['note'])
        self.assertNotIn('_sido', rows[0])


def page(items, total, code='00'):
    return {'header': {'resultCode': code, 'resultMsg': 'NORMAL SERVICE.'},
            'body': {'items': items, 'totalCount': str(total), 'numOfRows': '100', 'pageNo': '1'}}


class FetchTests(unittest.TestCase):
    def collector(self):
        return S.StdCalltaxiCollector(api_info={'auth': 'k+ey/=', 'ext_url': S.StdCalltaxiCollector.DEFAULT_BASE_URL})

    def test_pages_until_total_and_key_encoded(self):
        c = self.collector()
        urls = []
        pages = [page([src(tfcwkerMvmnCnterNm='A%d' % i) for i in range(100)], 150),
                 page([src(tfcwkerMvmnCnterNm='B%d' % i) for i in range(50)], 150)]

        def fake(url):
            urls.append(url)
            return pages[len(urls) - 1]
        with mock.patch.object(c, 'get_json', side_effect=fake), mock.patch.object(c, 'pause'):
            items = c.fetch_items()
        self.assertEqual(len(items), 150)
        self.assertIn('serviceKey=k%2Bey%2F%3D', urls[0])
        self.assertIn('pageNo=2', urls[1])
        self.assertIn('type=json', urls[0])

    def test_response_wrapper_and_single_item(self):
        c = self.collector()
        payload = {'response': page({'item': src()}, 1)}
        with mock.patch.object(c, 'get_json', return_value=payload):
            self.assertEqual(len(c.fetch_items()), 1)

    def test_error_code_fails(self):
        c = self.collector()
        with mock.patch.object(c, 'get_json', return_value=page([], 0, code='30')):
            with self.assertRaisesRegex(RuntimeError, 'STD_CALLTAXI 응답 오류 resultCode=30'):
                c.fetch_items()

    def test_zero_and_mismatch_fail(self):
        c = self.collector()
        with mock.patch.object(c, 'get_json', return_value=page([], 0)):
            with self.assertRaisesRegex(RuntimeError, '0건'):
                c.fetch_items()
        with mock.patch.object(c, 'get_json', side_effect=[page([src()], 3), page([], 3)]), \
                mock.patch.object(c, 'pause'):
            with self.assertRaisesRegex(RuntimeError, '다름'):
                c.fetch_items()

    def test_collect_keeps_raw(self):
        c = self.collector()
        with mock.patch.object(c, 'get_json', return_value=page([src(), src()], 2)):
            rows = c.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(c._raw_details), 2)


class RegistrationTests(unittest.TestCase):
    def test_pipeline_and_alert(self):
        import db_mobility
        import mobility_pipeline as mp
        import ops_alert
        self.assertIs(mp.MOBILITY_COLLECTORS['STD_CALLTAXI'], S.StdCalltaxiCollector)
        self.assertIs(mp._UPSERT_DISPATCH['STD_CALLTAXI'], db_mobility.sync_std_calltaxi)
        self.assertIn('STD_CALLTAXI', ops_alert.JOB_LABELS)

    def test_sync_rejects_empty(self):
        import db_mobility
        with self.assertRaisesRegex(RuntimeError, '적재할 행이 없음'):
            db_mobility.sync_std_calltaxi([])


if __name__ == '__main__':
    unittest.main()
