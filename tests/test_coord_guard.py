"""적재 전 좌표 보정(coord_guard) 단위 테스트 — v1.25.0. 네트워크·DB 없이 동작만 본다."""
from __future__ import annotations

import io
import os
import sys
import unittest
import urllib.error
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import coord_guard  # noqa: E402
import db_mobility  # noqa: E402


def _geo(table):
    return lambda addr: table.get(addr)


class FixRowTests(unittest.TestCase):
    def test_in_range_kept(self):
        row = {'latitude': 37.39, 'longitude': 126.95}
        self.assertEqual(coord_guard.fix_row(row), 'ok')
        self.assertEqual((row['latitude'], row['longitude']), (37.39, 126.95))

    def test_no_coord_left_alone(self):
        row = {'latitude': None, 'longitude': None}
        self.assertEqual(coord_guard.fix_row(row), 'none')
        self.assertEqual(coord_guard.fix_row({'name': 'x'}), 'none')

    def test_swapped(self):
        row = {'latitude': 128.0333, 'longitude': 35.44909}
        self.assertEqual(coord_guard.fix_row(row), 'swapped')
        self.assertEqual((row['latitude'], row['longitude']), (35.44909, 128.0333))

    def test_typo_geocoded_from_address(self):
        row = {'latitude': '335.814838', 'longitude': '126.900999', 'addr_road': '전북 김제시 도작로 224-32 (검산동, 청소년 수련관)'}
        geo = _geo({'전북 김제시 도작로 224-32': (35.814409, 126.900577)})
        self.assertEqual(coord_guard.fix_row(row, geo), 'geocoded')
        self.assertEqual((row['latitude'], row['longitude']), (35.814409, 126.900577))

    def test_zero_zero_geocoded(self):
        row = {'latitude': 0.0, 'longitude': 0.0, 'addr': '경기도 안양시 동안구 경수대로 539'}
        geo = _geo({'경기도 안양시 동안구 경수대로 539': (37.370968, 126.957392)})
        self.assertEqual(coord_guard.fix_row(row, geo), 'geocoded')

    def test_unfixable_nulled(self):
        row = {'latitude': 9.9999999, 'longitude': 9.9999999, 'addr': '어딘가'}
        self.assertEqual(coord_guard.fix_row(row, _geo({})), 'nulled')
        self.assertEqual((row['latitude'], row['longitude']), (None, None))
        row = {'latitude': 0, 'longitude': 0}
        self.assertEqual(coord_guard.fix_row(row, None), 'nulled')

    def test_geocoder_result_out_of_range_rejected(self):
        row = {'latitude': 1.0, 'longitude': 1.0, 'addr': 'a'}
        self.assertEqual(coord_guard.fix_row(row, _geo({'a': (10.0, 10.0)})), 'nulled')

    def test_clean_address(self):
        self.assertEqual(coord_guard.clean_address('부산광역시 동구 자성로133번길 16, 시민회관 (범일동)'),
                         '부산광역시 동구 자성로133번길 16')


class GuardRowsTests(unittest.TestCase):
    def test_counts_and_examples(self):
        rows = [{'latitude': 37.4, 'longitude': 127.0, 'name': 'a'},
                {'latitude': 127.0, 'longitude': 37.4, 'name': 'b'},
                {'latitude': 0, 'longitude': 0, 'name': 'c'},
                {'latitude': None, 'longitude': None, 'name': 'd'}]
        st = coord_guard.guard_rows(rows, None, 'T')
        self.assertEqual((st['ok'], st['swapped'], st['nulled'], st['none']), (1, 1, 1, 1))
        self.assertEqual([e['name'] for e in st['examples']], ['b', 'c'])

    def test_disabled_by_env(self):
        with mock.patch.dict(os.environ, {'COORD_GUARD': 'false'}):
            self.assertIsNone(coord_guard.guard([{'latitude': 0, 'longitude': 0}], 'T'))

    def test_no_key_no_geocoder(self):
        with mock.patch.dict(os.environ, {'KAKAO_REST_API_KEY': '', 'KAKAO_KEY_FILE': ''}):
            self.assertIsNone(coord_guard.geocoder_from_env())

    def test_key_read_from_shared_file(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, 'shared.env')
            with open(p, 'w', encoding='utf-8') as f:
                f.write('OTHER=1\nKAKAO_REST_API_KEY=abc123\n')
            with mock.patch.dict(os.environ, {'KAKAO_REST_API_KEY': '', 'KAKAO_KEY_FILE': p}):
                g = coord_guard.geocoder_from_env()
            self.assertIsNotNone(g)
            self.assertEqual(g._key, 'abc123')

    def test_env_key_wins_over_file(self):
        with mock.patch.dict(os.environ, {'KAKAO_REST_API_KEY': 'envkey', 'KAKAO_KEY_FILE': '/nonexistent'}):
            self.assertEqual(coord_guard.geocoder_from_env()._key, 'envkey')

    def test_missing_key_file_no_geocoder(self):
        with mock.patch.dict(os.environ, {'KAKAO_REST_API_KEY': '', 'KAKAO_KEY_FILE': '/nonexistent/x.env'}):
            self.assertIsNone(coord_guard.geocoder_from_env())


class KakaoGeocoderTests(unittest.TestCase):
    def test_auth_error_disables_and_caches(self):
        g = coord_guard.KakaoGeocoder('k', delay=0)
        err = urllib.error.HTTPError('u', 401, 'unauthorized', {}, io.BytesIO(b''))
        with mock.patch('urllib.request.urlopen', side_effect=err) as m:
            self.assertIsNone(g('경기도 안양시 동안구 경수대로 539'))
            self.assertTrue(g.disabled)
            self.assertIsNone(g('다른 주소'))
            self.assertEqual(m.call_count, 1)

    def test_hit(self):
        g = coord_guard.KakaoGeocoder('k', delay=0)
        body = io.BytesIO('{"documents":[{"y":"37.37","x":"126.95"}]}'.encode())
        with mock.patch('urllib.request.urlopen', return_value=mock.MagicMock(__enter__=lambda s: body, __exit__=lambda *a: None)):
            self.assertEqual(g('경기도 안양시 동안구 경수대로 539'), (37.37, 126.95))
            self.assertEqual(g('경기도 안양시 동안구 경수대로 539'), (37.37, 126.95))
            self.assertEqual(g.calls, 1)


class FacilitySqlTests(unittest.TestCase):
    def test_coords_kept_when_new_value_null(self):
        captured = {}
        with mock.patch.object(db_mobility, '_execute_batch', side_effect=lambda sql, rows: captured.setdefault('sql', sql) and 0):
            db_mobility.upsert_facilities([])
        sql = captured['sql']
        self.assertIn('latitude=COALESCE(EXCLUDED.latitude, poi_facility_accessibility.latitude)', sql)
        self.assertIn('longitude=COALESCE(EXCLUDED.longitude, poi_facility_accessibility.longitude)', sql)


if __name__ == '__main__':
    unittest.main()
