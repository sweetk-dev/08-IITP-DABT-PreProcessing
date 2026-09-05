"""GG_TOILET 어댑터 단위 테스트 — 네트워크/DB 없이 응답 파싱·컬럼 대응·지역 필터만 검증.

실응답 형태(2026-09-05 실측) 기준. 컬럼 대응은 기존 행안부 적재분과 이름 일치 168건
전수 대조로 확정한 것이다(EMBEL=비상벨, INSTL_YN/PLC_NM=기저귀교환대, TOILET_YN=CCTV).
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors.gg_toilet import (GgToiletCollector, map_row, parse_response, in_region,  # noqa: E402
                                  yyyymm_to_dash, yyyymmdd_to_date)

SAMPLE = {
    'DATA_STD_DE': '20250717', 'PUBLFACLT_DIV_NM': '공중화장실',
    'PBCTLT_PLC_NM': '부안어린이공원 공중화장실', 'MALE_FEMALE_CMNUSE_TOILET_YN': 'N',
    'MALE_WTRCLS_CNT': 2, 'MALE_UIL_CNT': 2, 'MALE_DSPSN_WTRCLS_CNT': 1, 'MALE_DSPSN_UIL_CNT': 0,
    'MALE_KID_WTRCLS_CNT': 0, 'MALE_KID_UIL_CNT': 0, 'FEMALE_WTRCLS_CNT': 2,
    'FEMALE_DSPSN_WTRCLS_CNT': 1, 'FEMALE_KID_WTRCLS_CNT': 0,
    'MANAGE_INST_NM': '안양시 공원관리과', 'MNGINST_TELNO': '031-8045-5021',
    'OPEN_TM_INFO': '상시', 'INSTL_YM': None,
    'REFINE_LOTNO_ADDR': '경기도 안양시 동안구 관양동 1588-4',
    'REFINE_ROADNM_ADDR': '경기도 안양시 동안구 달안로 160',
    'REFINE_WGS84_LOGT': 126.9602422857, 'REFINE_WGS84_LAT': 37.3983700279,
    'EMBEL_INSTL_YN': 'Y', 'EMBEL_INSTL_PLC': '여자화장실',
    'INSTL_YN': 'Y', 'PLC_NM': '여자화장실', 'RE_BUILD_YM': None,
    'PROC_CONT': '수세식', 'TOILET_POSESN_DIV': '공공기관-지방자치단체', 'TOILET_YN': 'N',
}


def ok_payload(rows, total=None):
    return {'Publtolt': [
        {'head': [{'list_total_count': len(rows) if total is None else total},
                  {'RESULT': {'CODE': 'INFO-000', 'MESSAGE': '정상 처리되었습니다.'}},
                  {'api_version': '1.0'}]},
        {'row': rows},
    ]}


class ParseResponseTests(unittest.TestCase):
    def test_ok(self):
        total, rows = parse_response(ok_payload([SAMPLE], total=243))
        self.assertEqual(total, 243)
        self.assertEqual(len(rows), 1)

    def test_no_data_is_not_error(self):
        total, rows = parse_response({'RESULT': {'CODE': 'INFO-200', 'MESSAGE': '해당하는 데이터가 없습니다.'}})
        self.assertEqual((total, rows), (0, []))

    def test_bad_key_raises(self):
        with self.assertRaises(RuntimeError):
            parse_response({'RESULT': {'CODE': 'ERROR-290', 'MESSAGE': '인증키가 유효하지 않습니다.'}})

    def test_unexpected_shape_raises(self):
        with self.assertRaises(RuntimeError):
            parse_response({'Other': []})


class MapRowTests(unittest.TestCase):
    def test_columns(self):
        row = map_row(SAMPLE)
        self.assertEqual(row['sido_code'], '9410000')
        self.assertEqual(row['toilet_name'], '부안어린이공원 공중화장실')
        self.assertEqual(row['toilet_type'], '공중화장실')
        self.assertEqual(row['addr_road'], '경기도 안양시 동안구 달안로 160')
        self.assertEqual(row['m_dis_toilet_count'], 1)
        self.assertEqual(row['f_dis_toilet_count'], 1)
        self.assertEqual(row['open_time'], '상시')
        self.assertIsNone(row['open_time_detail'])
        self.assertAlmostEqual(row['latitude'], 37.3983700279)
        self.assertAlmostEqual(row['longitude'], 126.9602422857)
        self.assertEqual(row['base_dt'], '2025-07-17')

    def test_flag_mapping_established_by_168_row_crosscheck(self):
        row = map_row(SAMPLE)
        self.assertEqual(row['emg_bell_yn'], 'Y')
        self.assertEqual(row['emg_bell_location'], '여자화장실')
        self.assertEqual(row['diaper_table_yn'], 'Y')
        self.assertEqual(row['diaper_table_location'], '여자화장실')
        self.assertEqual(row['cctv_yn'], 'N')
        self.assertIsNone(row['safety_target_yn'])
        self.assertIsNone(row['basis'])

    def test_yyyymm_normalization(self):
        self.assertEqual(yyyymm_to_dash('202503'), '2025-03')
        self.assertEqual(yyyymm_to_dash('2025-03'), '2025-03')
        self.assertIsNone(yyyymm_to_dash(''))
        self.assertIsNone(yyyymm_to_dash('2025'))
        self.assertEqual(yyyymmdd_to_date('20250717'), '2025-07-17')
        self.assertIsNone(yyyymmdd_to_date('2025-7-17'))
        row = map_row(dict(SAMPLE, INSTL_YM='199212', RE_BUILD_YM='202311'))
        self.assertEqual(row['install_dt'], '1992-12')
        self.assertEqual(row['remodeled_dt'], '2023-11')

    def test_missing_fields_are_none(self):
        row = map_row({'PBCTLT_PLC_NM': 'x', 'PUBLFACLT_DIV_NM': '개방화장실'})
        self.assertIsNone(row['latitude'])
        self.assertIsNone(row['m_toilet_count'])
        self.assertIsNone(row['emg_bell_yn'])
        self.assertIsNone(row['base_dt'])


class RegionFilterTests(unittest.TestCase):
    def test_prefix_not_substring(self):
        self.assertTrue(in_region({'addr_road': '경기도 안양시 만안구 안양로 1'}, '경기도 안양시'))
        self.assertTrue(in_region({'addr_road': None, 'addr_jibun': '경기도 안양시 동안구 관양동 1'}, '경기도 안양시'))
        # 안양천로(서울 양천구)·안양면(전남 장흥군) 은 잡히면 안 된다
        self.assertFalse(in_region({'addr_road': '서울특별시 양천구 안양천로 1'}, '경기도 안양시'))
        self.assertFalse(in_region({'addr_jibun': '전라남도 장흥군 안양면 1'}, '경기도 안양시'))
        self.assertTrue(in_region({'addr_road': '서울특별시 양천구 안양천로 1'}, ''))


class CollectTests(unittest.TestCase):
    def _stub(self, pages, page_size=2, addr_filter='경기도 안양시'):
        class Stub(GgToiletCollector):
            def __init__(self):
                self.api_info = {}

            @property
            def api_key(self):
                return 'k'

            @property
            def page_size(self):
                return page_size

            @property
            def addr_filter(self):
                return addr_filter

            @property
            def max_pages(self):
                return 10

            def get_json(self, url):
                import re
                page = int(re.search(r'pIndex=(\d+)', url).group(1))
                return pages.get(page, {'RESULT': {'CODE': 'INFO-200', 'MESSAGE': 'no data'}})

            @staticmethod
            def pause():
                return None
        return Stub()

    def test_paginates_until_short_page(self):
        other = dict(SAMPLE, PBCTLT_PLC_NM='B', REFINE_ROADNM_ADDR='경기도 안양시 만안구 x 1')
        third = dict(SAMPLE, PBCTLT_PLC_NM='C', REFINE_ROADNM_ADDR='경기도 안양시 만안구 y 2')
        pages = {1: ok_payload([SAMPLE, other], total=3), 2: ok_payload([third], total=3)}
        rows = self._stub(pages).collect()
        self.assertEqual([r['toilet_name'] for r in rows], ['부안어린이공원 공중화장실', 'B', 'C'])

    def test_post_filters_neighbouring_rows(self):
        leak = dict(SAMPLE, PBCTLT_PLC_NM='누수', REFINE_ROADNM_ADDR='서울특별시 양천구 안양천로 1',
                    REFINE_LOTNO_ADDR='서울특별시 양천구 x')
        pages = {1: ok_payload([SAMPLE, leak], total=2)}
        rows = self._stub(pages, page_size=1000).collect()
        self.assertEqual(len(rows), 1)

    def test_drops_rows_without_name(self):
        pages = {1: ok_payload([dict(SAMPLE, PBCTLT_PLC_NM='  ')], total=1)}
        self.assertEqual(self._stub(pages, page_size=1000).collect(), [])

    def test_api_error_propagates(self):
        pages = {1: {'RESULT': {'CODE': 'ERROR-290', 'MESSAGE': 'bad key'}}}
        with self.assertRaises(RuntimeError):
            self._stub(pages).collect()


if __name__ == '__main__':
    unittest.main()
