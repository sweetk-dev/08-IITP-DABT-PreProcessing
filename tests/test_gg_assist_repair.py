# -*- coding: utf-8 -*-
"""경기데이터드림 보조기기 수리 서비스센터 CSV 적재 (v1.15.0)."""
import io
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, 'scripts'))

from load_gg_assist_repair_csv import build, map_row  # noqa: E402

HEADER = ('번호,시군명,기관명,소재지지번주소,소재지도로명주소,소재지우편번호,'
          'WGS84위도,WGS84경도,전화번호정보,홈페이지URL,비고\n')
ROWS = (
    '38,안양시,밀알보장구수리센터,경기도 안양시 동안구 관양동 1390-3번지,'
    '경기도 안양시 동안구 관평로327번길 29,13936,37.4056041,126.9560092,031-8068-6100,,\n'
    '31,수원시,자립원,경기도 수원시 팔달구 우만동 471-28번지,'
    '경기도 수원시 팔달구 세지로399번길 94-2,16233,37.2889746,127.0267293,000-000-0000,www.,\n'
)


def _csv(text):
    fd, path = tempfile.mkstemp(suffix='.csv')
    os.close(fd)
    with io.open(path, 'w', encoding='utf-8') as handle:
        handle.write(text)
    return path


class MapRowTests(unittest.TestCase):
    def test_map_row(self):
        src = {'시군명': '안양시', '기관명': '밀알보장구수리센터',
               '소재지도로명주소': '경기도 안양시 동안구 관평로327번길 29',
               '소재지지번주소': '경기도 안양시 동안구 관양동 1390-3번지',
               '소재지우편번호': '13936', 'WGS84위도': '37.4056041',
               'WGS84경도': '126.9560092', '전화번호정보': '031-8068-6100',
               '홈페이지URL': '', '비고': ''}
        row = map_row(src, '2026-06-19')
        self.assertEqual(row['support_type'], 'repair')
        self.assertEqual(row['sido_code'], '9410000')
        self.assertEqual(row['source'], 'GG_ASSIST_REPAIR')
        self.assertEqual(row['confidence'], 'H')
        self.assertAlmostEqual(row['latitude'], 37.4056041)
        self.assertEqual(row['tel'], '031-8068-6100')
        # 원천에 운영시간 항목이 없다 — 추정치를 채우지 않는다
        self.assertIsNone(row['open_hours'])

    def test_placeholder_values_become_null(self):
        """전화 '000-000-0000' 과 홈페이지 'www.' 는 값이 아니라 자리표시자다."""
        src = {'시군명': '수원시', '기관명': '자립원', 'WGS84위도': '37.2',
               'WGS84경도': '127.0', '전화번호정보': '000-000-0000', '홈페이지URL': 'www.'}
        row = map_row(src, '2026-06-19')
        self.assertIsNone(row['tel'])
        self.assertIsNone(row['homepage'])


class BuildTests(unittest.TestCase):
    def test_region_filter(self):
        path = _csv(HEADER + ROWS)
        try:
            rows = build(path, {'안양시'}, '2026-06-19')
            self.assertEqual([r['name'] for r in rows], ['밀알보장구수리센터'])
            rows_all = build(path, None, '2026-06-19')
            self.assertEqual(len(rows_all), 2)
        finally:
            os.unlink(path)

    def test_header_mismatch_raises(self):
        path = _csv('a,b,c\n1,2,3\n')
        try:
            with self.assertRaises(RuntimeError):
                build(path, None, '2026-06-19')
        finally:
            os.unlink(path)


if __name__ == '__main__':
    unittest.main()
