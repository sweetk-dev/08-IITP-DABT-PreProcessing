"""GG_CULTURE 수집기 단위 테스트 — 네트워크·DB 없이 돈다.

응답 해석(gg_openapi), 행 변환·정제(gg_culture), 종류별 격리 수집, 매칭 키(db_mobility)를 본다.
"""
from __future__ import annotations

import os
import re
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors.gg_culture import (  # noqa: E402
    SERVICES, GgCultureCollector, clean_coords, map_row, parse_report_date,
)
from collectors.gg_openapi import parse_gg_response  # noqa: E402

SAMPLE = {
    'SIGUN_CD': '41190', 'SIGUN_NM': '부천시', 'INST_NM': '해밀도서관', 'TELNO': '032-612-2950',
    'HMPG_URL': 'www.hmlib.or.kr', 'INSTL_STATMNT_DE': '20080601', 'COPRTN_NM_INFO': '인천가톨릭사회복지회',
    'REFINE_LOTNO_ADDR': '경기도 부천시 원미구 중동 1', 'REFINE_ROADNM_ADDR': '경기도 부천시 원미구 옥산로10번길 4',
    'REFINE_ZIP_CD': '14545', 'REFINE_WGS84_LOGT': '126.77', 'REFINE_WGS84_LAT': '37.50',
}


def ok_payload(service, rows, total=None):
    return {service: [
        {'head': [{'list_total_count': len(rows) if total is None else total},
                  {'RESULT': {'CODE': 'INFO-000', 'MESSAGE': '정상 처리되었습니다.'}},
                  {'api_version': '1.0'}]},
        {'row': rows},
    ]}


NO_DATA = {'RESULT': {'CODE': 'INFO-200', 'MESSAGE': '해당하는 데이터가 없습니다.'}}


class ParseResponseTests(unittest.TestCase):
    def test_ok(self):
        total, rows = parse_gg_response(ok_payload('Svc', [SAMPLE], total=1), 'Svc')
        self.assertEqual((total, len(rows)), (1, 1))

    def test_no_data_is_not_error(self):
        self.assertEqual(parse_gg_response(NO_DATA, 'Svc'), (0, []))

    def test_error_code_raises(self):
        with self.assertRaises(RuntimeError):
            parse_gg_response({'RESULT': {'CODE': 'ERROR-290', 'MESSAGE': 'bad key'}}, 'Svc')

    def test_head_error_raises(self):
        bad = ok_payload('Svc', [SAMPLE])
        bad['Svc'][0]['head'][1]['RESULT']['CODE'] = 'ERROR-500'
        with self.assertRaises(RuntimeError):
            parse_gg_response(bad, 'Svc')

    def test_wrong_service_key_raises(self):
        with self.assertRaises(RuntimeError):
            parse_gg_response(ok_payload('Other', [SAMPLE]), 'Svc')


class MapRowTests(unittest.TestCase):
    def test_columns(self):
        row = map_row('braille_lib', SAMPLE)
        self.assertEqual(row['facility_type'], 'braille_lib')
        self.assertEqual(row['sido_code'], '9410000')
        self.assertEqual(row['name'], '해밀도서관')
        self.assertEqual(row['addr_road'], '경기도 부천시 원미구 옥산로10번길 4')
        self.assertEqual((row['latitude'], row['longitude']), (37.5, 126.77))
        self.assertEqual(row['install_report_dt'], '2008-06-01')
        self.assertEqual(row['corp_name'], '인천가톨릭사회복지회')
        self.assertEqual(row['source'], 'GG_CULTURE')

    def test_missing_optional_fields_are_none(self):
        row = map_row('sports_org', {'INST_NM': ' 수원시장애인체육회 '})
        self.assertEqual(row['name'], '수원시장애인체육회')
        for col in ('addr_road', 'tel', 'install_report_dt', 'corp_name', 'latitude', 'longitude'):
            self.assertIsNone(row[col], col)

    def test_report_date(self):
        self.assertEqual(parse_report_date('20240808'), '2024-08-08')
        self.assertEqual(parse_report_date('2024-08-08'), '2024-08-08')
        for bad in ('20150000', '99999999', '미상', '', None, '2024080'):
            self.assertIsNone(parse_report_date(bad), bad)

    def test_coords(self):
        self.assertEqual(clean_coords('37.5', '127.0'), (37.5, 127.0))
        self.assertEqual(clean_coords('0', '0'), (None, None))
        self.assertEqual(clean_coords('127.0', '37.5'), (None, None))      # 뒤바뀜
        self.assertEqual(clean_coords('37.5', None), (None, None))         # 한쪽만


class StubCollector(GgCultureCollector):
    """서비스명 → {페이지: 응답} 사전으로 응답을 흉내 낸다. 값이 Exception 이면 그 예외를 던진다."""

    def __init__(self, responses, page_size=1000):
        super().__init__(api_info={})
        self._responses = responses
        self._ps = page_size

    @property
    def api_key(self):
        return 'k'

    @property
    def page_size(self):
        return self._ps

    def get_json(self, url):
        service = re.search(r'gg\.go\.kr/(\w+)\?', url).group(1)
        page = int(re.search(r'pIndex=(\d+)', url).group(1))
        resp = self._responses.get(service, {}).get(page, NO_DATA)
        if isinstance(resp, Exception):
            raise resp
        return resp

    @staticmethod
    def pause():
        return None


def all_ok(n_each=1):
    out = {}
    for ftype, service, _ in SERVICES:
        rows = [dict(SAMPLE, INST_NM='%s-%d' % (ftype, i)) for i in range(n_each)]
        out[service] = {1: ok_payload(service, rows)}
    return out


class CollectTests(unittest.TestCase):
    def test_collects_all_four(self):
        c = StubCollector(all_ok(2))
        rows = c.collect()
        self.assertEqual(len(rows), 8)
        self.assertEqual(c.failures, [])
        self.assertEqual(set(c.collected), {s[0] for s in SERVICES})

    def test_paginates(self):
        resp = all_ok(1)
        svc = 'Gglibrryprovsnservc'
        r1 = [dict(SAMPLE, INST_NM='A'), dict(SAMPLE, INST_NM='B')]
        resp[svc] = {1: ok_payload(svc, r1, total=3), 2: ok_payload(svc, [dict(SAMPLE, INST_NM='C')], total=3)}
        c = StubCollector(resp, page_size=2)
        rows = c.collect()
        self.assertEqual([r['name'] for r in rows if r['facility_type'] == 'booknarae_lib'], ['A', 'B', 'C'])

    def test_one_service_error_is_isolated(self):
        resp = all_ok(1)
        resp['Ggdspsnphstrnfaclt'] = {1: {'RESULT': {'CODE': 'ERROR-500', 'MESSAGE': 'server'}}}
        c = StubCollector(resp)
        rows = c.collect()
        self.assertEqual({r['facility_type'] for r in rows}, {'braille_lib', 'booknarae_lib', 'sports_org'})
        self.assertEqual([f['id'] for f in c.failures], ['GG_CULTURE:sports_fac'])
        self.assertEqual(c.failures[0]['stage'], 'collect')

    def test_network_error_is_isolated(self):
        resp = all_ok(1)
        resp['Ggbrlllibrry'] = {1: ConnectionError('timeout')}
        c = StubCollector(resp)
        c.collect()
        self.assertEqual([f['id'] for f in c.failures], ['GG_CULTURE:braille_lib'])

    def test_total_mismatch_is_failure(self):
        resp = all_ok(1)
        svc = 'Ggsigundspsnphstrn'
        resp[svc] = {1: ok_payload(svc, [SAMPLE], total=32)}
        c = StubCollector(resp)
        c.collect()
        self.assertEqual([f['id'] for f in c.failures], ['GG_CULTURE:sports_org'])

    def test_zero_rows_is_failure_and_keeps_existing(self):
        resp = all_ok(1)
        resp['Ggbrlllibrry'] = {1: NO_DATA}
        c = StubCollector(resp)
        rows = c.collect()
        self.assertNotIn('braille_lib', {r['facility_type'] for r in rows})
        self.assertIn('원천 0건', c.failures[0]['reason'])

    def test_all_fail_raises(self):
        resp = {s[1]: {1: {'RESULT': {'CODE': 'ERROR-290', 'MESSAGE': 'bad key'}}} for s in SERVICES}
        with self.assertRaises(RuntimeError):
            StubCollector(resp).collect()

    def test_duplicate_name_and_address_dropped(self):
        resp = all_ok(1)
        svc = 'Gglibrryprovsnservc'
        resp[svc] = {1: ok_payload(svc, [SAMPLE, dict(SAMPLE)])}
        rows = StubCollector(resp).collect()
        self.assertEqual(sum(1 for r in rows if r['facility_type'] == 'booknarae_lib'), 1)

    def test_base_url_strips_service_path(self):
        c = GgCultureCollector(api_info={'ext_url': 'https://openapi.gg.go.kr/Publtolt', 'auth': 'k'})
        self.assertEqual(c.base_url, 'https://openapi.gg.go.kr')


class MatchKeyTests(unittest.TestCase):
    def test_keys(self):
        from db_mobility import culture_match_keys
        self.assertEqual(culture_match_keys('', 'x 1', None), [])          # 이름 없으면 키 없음
        a = culture_match_keys('경기도 시각장애인 도서관', '경기도 수원시 팔달구 수성로157번길 27-4 (화서동)', None)
        b = culture_match_keys('경기도시각장애인도서관', '경기도 수원시 팔달구 수성로157번길 27-4', None)
        self.assertEqual(a, b)
        self.assertEqual(len(culture_match_keys('A', 'x 1', 'y 2')), 2)

    def test_parenthesised_names_stay_distinct(self):
        from db_mobility import culture_match_keys
        self.assertNotEqual(culture_match_keys('희망도서관(본관)', 'x 1', None),
                            culture_match_keys('희망도서관(분관)', 'x 1', None))

    def test_addressless_row_gets_empty_address_key(self):
        from db_mobility import culture_match_keys
        self.assertEqual(culture_match_keys('경기도장애인체육회', None, ' '), [('경기도장애인체육회', '')])


class PipelineTests(unittest.TestCase):
    """run_mobility — 수집기 실패 목록이 요약으로 옮겨지고, 실패가 있으면 동기화 시각을 갱신하지 않는다."""

    def _run(self, collector_failures, upsert_result):
        import mobility_pipeline as mp
        touched = []

        class C:
            def __init__(self, api_info=None, stats_src=None):
                self.failures = list(collector_failures)
            def collect(self):
                return [{'facility_type': 'braille_lib', 'name': 'A'}]
            def save_response(self, *a, **k):
                return None

        saved = (dict(mp.MOBILITY_COLLECTORS), dict(mp._UPSERT_DISPATCH), mp.get_api_info, mp.db_mobility.touch_latest_sync)
        try:
            mp.MOBILITY_COLLECTORS['GG_CULTURE'] = C
            mp._UPSERT_DISPATCH['GG_CULTURE'] = lambda rows: upsert_result
            mp.get_api_info = lambda ext: {}
            mp.db_mobility.touch_latest_sync = lambda ext: touched.append(ext)
            out = mp.run_mobility('GG_CULTURE', 'db')
        finally:
            mp.MOBILITY_COLLECTORS.clear(); mp.MOBILITY_COLLECTORS.update(saved[0])
            mp._UPSERT_DISPATCH.clear(); mp._UPSERT_DISPATCH.update(saved[1])
            mp.get_api_info = saved[2]; mp.db_mobility.touch_latest_sync = saved[3]
        return out, touched

    def test_failures_propagate_and_hold_sync_time(self):
        fail = {'id': 'GG_CULTURE:sports_fac', 'stage': 'collect', 'reason': 'x'}
        dbfail = {'id': 'GG_CULTURE:booknarae_lib', 'stage': 'db', 'reason': 'y'}
        out, touched = self._run([fail], {'db_ok': 1, 'db_fail': 260, 'failures': [dbfail], 'by_type': {}})
        self.assertEqual([f['id'] for f in out['failures']], ['GG_CULTURE:sports_fac', 'GG_CULTURE:booknarae_lib'])
        self.assertEqual((out['db_ok'], out['db_fail']), (1, 260))
        self.assertEqual(touched, [])

    def test_clean_run_touches_sync_time(self):
        out, touched = self._run([], {'db_ok': 1, 'db_fail': 0, 'failures': [], 'by_type': {}})
        self.assertEqual(out['failures'], [])
        self.assertEqual(touched, ['GG_CULTURE'])


if __name__ == '__main__':
    unittest.main()
