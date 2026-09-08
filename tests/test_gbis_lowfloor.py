"""저상버스 노선현황 페이지 파서 단위 테스트 — Issue #97.

네트워크·DB 없이 고정 HTML(2026-09-07 페이지에서 잘라낸 5행)으로 파싱·기준일 계산을 검증한다.
"""
from __future__ import annotations

import datetime
import os
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors.gbis_lowfloor import (GbisLowFloorCollector, base_date_for,  # noqa: E402
                                      parse_lowfloor_rows)

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'gbis_lowfloor_sample.html')


def _load():
    with open(FIXTURE, encoding='utf-8') as fp:
        return fp.read()


class ParseTests(unittest.TestCase):
    def setUp(self):
        self.rows = parse_lowfloor_rows(_load())

    def test_row_count_and_route_ids(self):
        self.assertEqual(len(self.rows), 5)
        ids = {r['route_id'] for r in self.rows}
        self.assertIn(208000024, ids)   # 1-1 삼영운수
        self.assertIn(208000016, ids)   # 5 삼영운수
        self.assertIn(241245001, ids)   # 5 안양-편안운수(마을)
        self.assertIn(208000071, ids)   # AY01 (주말만 운행)

    def test_same_route_name_different_company_keeps_both(self):
        fives = [r for r in self.rows if r['route_name'] == '5']
        self.assertEqual(len(fives), 2)
        self.assertEqual({r['company'] for r in fives}, {'삼영운수', '안양-편안운수'})
        self.assertNotEqual(fives[0]['route_id'], fives[1]['route_id'])

    def test_cells_are_mapped(self):
        r = next(r for r in self.rows if r['route_id'] == 208000024)
        self.assertEqual(r['region'], '경기도 안양시')
        self.assertEqual(r['start_station'], '월암공영차고지(미정차)')
        self.assertEqual(r['end_station'], '사당역(중)')
        self.assertEqual(r['up_hours']['평일'], '04:40 ~ 22:30')
        self.assertEqual(r['alloc']['토요일'], '21 ~ 21 분')
        self.assertEqual(r['company'], '삼영운수')

    def test_empty_hours_become_none(self):
        r = next(r for r in self.rows if r['route_id'] == 208000071)
        self.assertIsNone(r['up_hours']['평일'])
        self.assertEqual(r['up_hours']['토요일'], '10:00 ~ 22:00')
        self.assertIsNone(r['alloc']['공휴일'])

    def test_non_anyang_rows_are_kept_for_matching(self):
        regions = {r['region'] for r in self.rows}
        self.assertIn('경기도 가평군', regions)

    def test_garbage_html_yields_nothing(self):
        self.assertEqual(parse_lowfloor_rows('<html><table><tr><td>x</td></tr></table></html>'), [])


class BaseDateTests(unittest.TestCase):
    def test_yesterday(self):
        self.assertEqual(base_date_for(datetime.date(2026, 9, 7)), '2026-09-06')


class CollectorTests(unittest.TestCase):
    def _collector(self, body: str):
        c = GbisLowFloorCollector(api_info={}, stats_src={})
        resp = mock.Mock(); resp.content = body.encode('utf-8'); resp.text = body
        c.http_get = mock.Mock(return_value=resp)
        return c

    def test_collect_all_rows_with_base_dt(self):
        c = self._collector(_load())
        rows = c.collect()
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(r['low_bus_base_dt'] for r in rows))
        self.assertEqual(len(c._all_route_ids), 5)

    def test_region_filter_keeps_matching_ids_for_all(self):
        c = self._collector(_load())
        with mock.patch.dict(os.environ, {'GBIS_LOWFLOOR_REGION_FILTER': '안양'}):
            rows = c.collect()
        self.assertEqual(len(rows), 4)
        self.assertEqual(len(c._all_route_ids), 5)

    def test_unexpected_page_raises(self):
        c = self._collector('<html><body>점검 중</body></html>')
        with self.assertRaises(RuntimeError):
            c.collect()

    def test_no_api_key_required(self):
        c = GbisLowFloorCollector(api_info={}, stats_src={})
        self.assertEqual(c.api_key, '')


if __name__ == '__main__':
    unittest.main()
