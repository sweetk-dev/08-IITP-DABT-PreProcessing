"""GG_WELFARE(경기 장애인복지시설 5종) 수집기 단위 테스트 — 네트워크·DB 없이 돈다."""
from __future__ import annotations

import os
import re
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors.gg_welfare import SERVICES, GgWelfareCollector, map_row, to_count  # noqa: E402
from tests.test_gg_culture import NO_DATA, ok_payload  # noqa: E402

RESIDENCE = {
    'SIGUN_CD': '41130', 'SIGUN_NM': '성남시', 'OBSTCL_TYPE_NM': '지적장애인시설', 'INST_NM': '예가원',
    'HMPG_URL': 'www.rainbowhill.org', 'ENTRNC_PSN_CAPA_CNT': 53, 'ENTRNC_PSTPSN_CNT': 49,
    'ENFLPSN_PSN_CAPA_CNT': 49, 'ENFLPSN_PSTPSN_CNT': 44, 'INSTL_STATMNT_DE': '20011123',
    'COPRTN_NM_INFO': '무지개동산', 'REFINE_LOTNO_ADDR': '경기도 성남시 분당구 야탑동 83번지',
    'REFINE_ROADNM_ADDR': '경기도 성남시 분당구 새나리로38번길 13', 'REFINE_ZIP_CD': '13509',
    'REFINE_WGS84_LOGT': 127.159819, 'REFINE_WGS84_LAT': 37.4018258, 'TELNO': '031-000-0000',
}
WORKSHOP = {
    'SIGUN_NM': '이천시', 'INST_NM': '한마음일터', 'HMPG_URL': 'http://daprint.kr/',
    'LABOR_DSPSN_PSN_CAPA_CNT': 22, 'LABOR_DSPSN_PSTPSN_CNT': 16, 'ENFLPSN_PSN_CAPA_CNT': 12,
    'ENFLPSN_PSTPSN_CNT': 16, 'INSTL_STATMNT_DE': '20011107', 'COPRTN_NM': '엘리엘동산',
    'REFINE_ROADNM_ADDR': '경기도 이천시 마장면 이장로 256',
    'REFINE_WGS84_LOGT': 127.3872996, 'REFINE_WGS84_LAT': 37.2735968,
}
RELATED = {
    'SIGUN_NM': '고양시', 'INST_NM': '고양시발달장애인지원센터', 'HMPG_URL': 'www.', 'RM': '발달장애인지원센터',
    'REFINE_ROADNM_ADDR': '경기도 고양시 덕양구 원당로 59번길 92', 'TELNO_INFO': '031-111-2222',
    'REFINE_WGS84_LOGT': '', 'REFINE_WGS84_LAT': '',
}


class MapRowTests(unittest.TestCase):
    def test_residence(self):
        row = map_row('residence', RESIDENCE)
        self.assertEqual(row['sub_type'], '지적장애인시설')
        self.assertEqual((row['capacity_cnt'], row['current_cnt']), (53, 49))
        self.assertEqual((row['staff_capacity_cnt'], row['staff_current_cnt']), (49, 44))
        self.assertEqual(row['corp_name'], '무지개동산')
        self.assertEqual(row['install_report_dt'], '2001-11-23')
        self.assertEqual(row['source'], 'GG_WELFARE')
        self.assertEqual(row['sido_code'], '9410000')

    def test_workshop_uses_worker_counts(self):
        row = map_row('protect_workshop', WORKSHOP)
        self.assertEqual((row['capacity_cnt'], row['current_cnt']), (22, 16))
        self.assertEqual(row['corp_name'], '엘리엘동산')
        self.assertIsNone(row['sub_type'])

    def test_related_facility(self):
        row = map_row('support_center', RELATED)
        self.assertEqual(row['sub_type'], '발달장애인지원센터')
        self.assertEqual(row['tel'], '031-111-2222')
        self.assertIsNone(row['homepage'])                         # 'www.' 은 빈 값
        self.assertEqual((row['latitude'], row['longitude']), (None, None))
        self.assertIsNone(row['capacity_cnt'])

    def test_sub_type_only_for_residence_and_related(self):
        self.assertIsNone(map_row('protect_workshop', dict(WORKSHOP, RM='비고', OBSTCL_TYPE_NM='x'))['sub_type'])
        self.assertIsNone(map_row('il_center', dict(RELATED))['sub_type'])

    def test_counts(self):
        self.assertEqual([to_count(v) for v in (12, '12', '12.0', 12.0, '1,234', 0)], [12, 12, 12, 12, 1234, 0])
        for bad in (-1, 'x', None, '', 1.5):
            self.assertIsNone(to_count(bad), bad)


class StubCollector(GgWelfareCollector):
    def __init__(self, responses):
        super().__init__(api_info={})
        self._responses = responses

    @property
    def api_key(self):
        return 'k'

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


class CollectTests(unittest.TestCase):
    def _all_ok(self):
        out = {}
        for ftype, service, _ in SERVICES:
            out[service] = {1: ok_payload(service, [dict(RESIDENCE, INST_NM='%s-1' % ftype)])}
        return out

    def test_collects_five_types(self):
        c = StubCollector(self._all_ok())
        rows = c.collect()
        self.assertEqual(sorted(c.collected), sorted(s[0] for s in SERVICES))
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(r['source'] == 'GG_WELFARE' for r in rows))

    def test_one_type_failure_isolated(self):
        resp = self._all_ok()
        resp['Ggdspsnprotectwrkshp'] = {1: {'RESULT': {'CODE': 'ERROR-300', 'MESSAGE': 'x'}}}
        c = StubCollector(resp)
        rows = c.collect()
        self.assertEqual(len(rows), 4)
        self.assertEqual([f['id'] for f in c.failures], ['GG_WELFARE:protect_workshop'])

    def test_all_fail_raises(self):
        with self.assertRaises(RuntimeError) as cm:
            StubCollector({}).collect()
        self.assertIn('GG_WELFARE: 5종 모두 수집 실패', str(cm.exception))

    def test_same_name_and_address_keeps_first(self):
        dup = [dict(RESIDENCE, ENTRNC_PSTPSN_CNT=15), dict(RESIDENCE, ENTRNC_PSTPSN_CNT=10)]
        resp = self._all_ok()
        resp['Ggobstcltyperesidnc'] = {1: ok_payload('Ggobstcltyperesidnc', dup)}
        rows = StubCollector(resp).collect()
        res = [r for r in rows if r['facility_type'] == 'residence']
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['current_cnt'], 15)

    def test_env_prefix(self):
        os.environ['GG_WELFARE_PAGE_SIZE'] = '50'
        try:
            self.assertEqual(GgWelfareCollector(api_info={}).page_size, 50)
        finally:
            del os.environ['GG_WELFARE_PAGE_SIZE']


class RegistryTests(unittest.TestCase):
    def test_registered(self):
        import mobility_pipeline as mp
        import ops_alert
        for ext in ('GG_WELFARE', 'GG_DIS_REG'):
            self.assertIn(ext, mp.MOBILITY_COLLECTORS)
            self.assertIn(ext, mp._UPSERT_DISPATCH)
            self.assertIn(ext, ops_alert.JOB_LABELS)


if __name__ == '__main__':
    unittest.main()
