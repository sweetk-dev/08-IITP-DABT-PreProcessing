"""GG_DIS_REG(경기 읍면동 등록장애인) 수집기 단위 테스트 — 네트워크·DB 없이 돈다."""
from __future__ import annotations

import os
import re
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors.gg_dis_reg import (  # noqa: E402
    GgDisRegCollector, map_row, merge_duplicates, normalize_dis_type, parse_base_date,
)
from tests.test_gg_culture import NO_DATA, ok_payload  # noqa: E402


def src(sigun='안양시', gu='만안구', emd='안양1동', t='지체', total=10.0, dt='20250807', **kw):
    base = {'SIGUN_NM': sigun, 'OLD_NM': gu, 'EMD_NM': emd, 'OBSTCL_TYPE': t, 'SUM': total,
            'DSPSN_MALE_CNT': 6, 'DSPSN_FEMALE_CNT': 4, 'SEVR_DSPSN_SUM': 3.0, 'SEVR_DSPSN_MALE_CNT': 2,
            'SEVR_DSPSN_FEMALE_CNT': 1, 'NSEVR_DSPSN_SUM': 7.0, 'NSEVR_DSPSN_MALE_CNT': 4,
            'NSEVR_DSPSN_FEMALE_CNT': 3, 'DATA_COLCT_DE': dt}
    base.update(kw)
    return base


class MapRowTests(unittest.TestCase):
    def test_columns(self):
        row = map_row(src())
        self.assertEqual(row['base_dt'], '2025-08-07')
        self.assertEqual((row['sigun_nm'], row['gu_nm'], row['emd_nm'], row['dis_type']),
                         ('안양시', '만안구', '안양1동', '지체'))
        self.assertEqual((row['tot_cnt'], row['sev_cnt'], row['nsev_female_cnt']), (10, 3, 3))

    def test_total_rows_skipped(self):
        self.assertIsNone(map_row(src(t='소계')))
        self.assertIsNone(map_row(src(t='전체')))
        self.assertIsNone(map_row(src(t='합계')))
        self.assertIsNone(map_row(src(sigun='김포시', gu=None, emd='전체', t='신장')))   # 시군 합계 행

    def test_gu_same_as_sigun_is_null(self):
        self.assertIsNone(map_row(src(sigun='광명시', gu='광명시'))['gu_nm'])
        self.assertIsNone(map_row(src(sigun='의왕시', gu=None))['gu_nm'])

    def test_dis_type_normalized(self):
        for v in ('장루.요루', '장루, 요루', ' 장루·요루 '):
            self.assertEqual(normalize_dis_type(v), '장루·요루', v)
        self.assertEqual(normalize_dis_type(' 지 체 '), '지체')

    def test_bad_date_dropped(self):
        self.assertIsNone(parse_base_date('20250230'))
        self.assertIsNone(map_row(src(dt='2025')))
        self.assertEqual(parse_base_date('2025-08-07'), '2025-08-07')

    def test_null_counts_kept(self):
        row = map_row(src(SUM=None, DSPSN_MALE_CNT=''))
        self.assertIsNone(row['tot_cnt'])
        self.assertIsNone(row['male_cnt'])


class MergeTests(unittest.TestCase):
    def test_duplicates_summed(self):
        a = map_row(src(emd='정자1동', total=43.0))
        b = map_row(src(emd='정자1동', total=61.0, SEVR_DSPSN_SUM=None))
        rows, dups = merge_duplicates([a, b, map_row(src(emd='정자2동'))])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['tot_cnt'], 104)
        self.assertEqual(rows[0]['sev_cnt'], 3)                     # 한쪽만 값 → 그 값
        self.assertEqual(list(dups.values()), [2])


class StubCollector(GgDisRegCollector):
    def __init__(self, pages, page_size=1000):
        super().__init__(api_info={})
        self._pages = pages
        self._ps = page_size

    @property
    def api_key(self):
        return 'k'

    @property
    def page_size(self):
        return self._ps

    def get_json(self, url):
        page = int(re.search(r'pIndex=(\d+)', url).group(1))
        resp = self._pages.get(page, NO_DATA)
        if isinstance(resp, Exception):
            raise resp
        return resp

    @staticmethod
    def pause():
        return None


class CollectTests(unittest.TestCase):
    def test_paginates_and_cleans(self):
        rows = [src(emd='동%d' % i) for i in range(3)] + [src(t='소계'), src(emd='동0')]
        pages = {1: ok_payload('GGREDISPERST', rows[:2], total=5), 2: ok_payload('GGREDISPERST', rows[2:4], total=5),
                 3: ok_payload('GGREDISPERST', rows[4:], total=5)}
        c = StubCollector(pages, page_size=2)
        out = c.collect()
        self.assertEqual(len(out), 3)                                # 소계 제외, 동0 두 행 합침
        self.assertEqual(out[0]['tot_cnt'], 20)
        self.assertEqual(c.stats['total_rows_skipped'], 1)
        self.assertEqual(c.stats['merged_keys'], 1)

    def test_total_mismatch_raises(self):
        with self.assertRaises(RuntimeError):
            StubCollector({1: ok_payload('GGREDISPERST', [src()], total=3)}).collect()

    def test_all_rows_cleaned_away_raises(self):
        rows = [src(t='소계'), src(emd='전 체', t='지체')]
        with self.assertRaises(RuntimeError) as cm:
            StubCollector({1: ok_payload('GGREDISPERST', rows)}).collect()
        self.assertIn('정제 뒤 0행', str(cm.exception))

    def test_spaced_total_emd_skipped(self):
        self.assertIsNone(map_row(src(emd='합 계', t='지체')))

    def test_zero_rows_raises(self):
        with self.assertRaises(RuntimeError):
            StubCollector({}).collect()

    def test_error_code_raises(self):
        with self.assertRaises(RuntimeError):
            StubCollector({1: {'RESULT': {'CODE': 'ERROR-290', 'MESSAGE': 'key'}}}).collect()


class PipelineSummaryTests(unittest.TestCase):
    """run_mobility — 실패가 없을 때만 요약표 기준일을 갱신한다."""

    def _run(self, failures):
        import mobility_pipeline as mp
        calls = []

        class C:
            def __init__(self, api_info=None, stats_src=None):
                self.failures = []
            def collect(self):
                return [{'sigun_nm': '안양시'}]
            def save_response(self, *a, **k):
                return None

        saved = (dict(mp.MOBILITY_COLLECTORS), dict(mp._UPSERT_DISPATCH), mp.get_api_info,
                 mp.db_mobility.touch_latest_sync, mp.db_mobility.refresh_data_summary)
        try:
            mp.MOBILITY_COLLECTORS['GG_DIS_REG'] = C
            mp._UPSERT_DISPATCH['GG_DIS_REG'] = lambda rows: {'db_ok': 1, 'db_fail': 0, 'failures': failures,
                                                               'by_type': {}}
            mp.get_api_info = lambda ext: {}
            mp.db_mobility.touch_latest_sync = lambda ext: calls.append(('touch', ext))
            mp.db_mobility.refresh_data_summary = lambda ext: calls.append(('summary', ext))
            mp.run_mobility('GG_DIS_REG', 'db')
        finally:
            mp.MOBILITY_COLLECTORS.clear(); mp.MOBILITY_COLLECTORS.update(saved[0])
            mp._UPSERT_DISPATCH.clear(); mp._UPSERT_DISPATCH.update(saved[1])
            mp.get_api_info = saved[2]
            mp.db_mobility.touch_latest_sync = saved[3]
            mp.db_mobility.refresh_data_summary = saved[4]
        return calls

    def test_clean_run_refreshes_summary(self):
        self.assertEqual(self._run([]), [('touch', 'GG_DIS_REG'), ('summary', 'GG_DIS_REG')])

    def test_partial_run_holds_summary(self):
        self.assertEqual(self._run([{'id': 'GG_DIS_REG:안양시', 'stage': 'db', 'reason': 'x'}]), [])

    def test_refresh_mapping_covers_jobs(self):
        import db_mobility
        import mobility_pipeline as mp
        for ext in mp.MOBILITY_COLLECTORS:
            self.assertIn(ext, db_mobility.SUMMARY_REFRESH, ext)

    def test_refresh_without_engine_is_noop(self):
        import db_mobility
        saved = db_mobility.engine
        db_mobility.engine = None
        try:
            self.assertIsNone(db_mobility.refresh_data_summary('GG_DIS_REG'))
            self.assertIsNone(db_mobility.refresh_data_summary('UNKNOWN'))
        finally:
            db_mobility.engine = saved


if __name__ == '__main__':
    unittest.main()
