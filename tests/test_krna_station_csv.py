"""국가철도공단 역사 설비 CSV 적재 스크립트 테스트 — v1.10.0 (KRNA_STN).

DB 없이 헤더 판별·행 매핑·순번 부여·이격거리 요약·인코딩 처리만 검증한다.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for p in (ROOT, os.path.join(ROOT, 'scripts')):
    if p not in sys.path:
        sys.path.insert(0, p)

import load_krna_station_csv as m  # noqa: E402

EV = ('철도운영기관명,선명,역명,출입구번호,상세위치,정원_인원,정원_중량\n'
      '코레일,1호선,안양,2,(2F) 1번출구 맞이방 서쪽,11,1000\n'
      '코레일,1호선,안양,내부,(1F) 관악역 방향 승강장 4-3 출입문앞,11,1001\n'
      '코레일,1호선,석수,1,(1F) 1번 출구 쪽 엘리베이터,11,750\n'
      '코레일,4호선,인덕원,내부,(B1) 4번/5번 출입구 방향,,900\n')

TOILET = ('철도운영기관명,선명,역명,지상구분,역층,게이트내외,출구번호,상세위치,화장실구분\n'
          '코레일,1호선,명학,지상,2,외,1,2층 맞이방 게이트 옆,남자\n'
          '코레일,1호선,명학,지상,2,외,1,2층 맞이방 게이트 옆,여자\n')

PLATFORM = ('철도운영기관명,선명,역명,승강장번호,상하행,지상구분,역층,승강장연결 여부,스크린도어 유무,안전발판 유무\n'
            '코레일,4호선,범계,2,하행,지하,2,N,Y,N\n'
            '코레일,4호선,범계,1,상행,지하,2,N,Y,N\n'
            '코레일,1호선,지행,1,상행,지상,2,Y,N,Y\n')

GAP = ('철도운영기관명,선명,역명,승강장번호,차량순서,차량출입문번호,안전거리\n'
       '코레일,1호선,안양,1,1,1,9.5\n'
       '코레일,1호선,안양,1,1,2,10.6\n'
       '코레일,1호선,안양,1,1,3,11\n'
       '코레일,1호선,안양,2,1,1,8\n'
       '코레일,1호선,안양,2,1,2,\n')


class DetectTests(unittest.TestCase):
    def test_detect_each_kind(self):
        for text, kind in ((EV, 'ev'), (TOILET, 'toilet'), (PLATFORM, 'platform'), (GAP, 'gap')):
            self.assertEqual(m.detect_kind(text.splitlines()[0].split(',')), kind)

    def test_unknown_header_raises(self):
        with self.assertRaises(ValueError):
            m.detect_kind(['a', 'b'])

    def test_bom_in_header_is_ignored(self):
        hdr = ('﻿' + EV.splitlines()[0]).split(',')
        self.assertEqual(m.detect_kind(hdr), 'ev')


class ParseTests(unittest.TestCase):
    def test_ev_rows_and_unit_seq_per_station(self):
        kind, rows = m.parse_rows(EV, '2025-06-30')
        self.assertEqual(kind, 'ev')
        self.assertEqual(len(rows), 4)
        anyang = [r for r in rows if r['stn_name'] == '안양']
        self.assertEqual([r['unit_seq'] for r in anyang], [1, 2])
        self.assertEqual(anyang[0]['exit_no'], '2')
        self.assertEqual(anyang[1]['exit_no'], '내부')
        self.assertEqual(anyang[0]['capacity_person'], 11)
        idw = [r for r in rows if r['stn_name'] == '인덕원'][0]
        self.assertIsNone(idw['capacity_person'])        # 빈 칸 -> None
        self.assertEqual(idw['capacity_kg'], 900)
        self.assertEqual(idw['unit_seq'], 1)             # 역·선명 별로 1부터
        self.assertEqual(rows[0]['base_dt'], '2025-06-30')

    def test_toilet_rows_disabled_flag(self):
        kind, rows = m.parse_rows(TOILET, '2025-06-30', disabled=True)
        self.assertEqual(kind, 'toilet')
        self.assertEqual({r['disabled_yn'] for r in rows}, {'Y'})
        self.assertEqual([r['unit_seq'] for r in rows], [1, 2])
        self.assertEqual(rows[0]['gate_inout'], '외')
        self.assertEqual(rows[0]['exit_no'], '1')
        self.assertEqual(rows[1]['toilet_kind'], '여자')
        _, rows_n = m.parse_rows(TOILET, '2025-06-30', disabled=False)
        self.assertEqual({r['disabled_yn'] for r in rows_n}, {'N'})

    def test_platform_rows_yn(self):
        kind, rows = m.parse_rows(PLATFORM, '2024-09-13')
        self.assertEqual(kind, 'platform')
        bg = [r for r in rows if r['stn_name'] == '범계' and r['platform_no'] == '1'][0]
        self.assertEqual((bg['updown'], bg['screen_door_yn'], bg['safety_plate_yn'],
                          bg['platform_connect_yn']), ('상행', 'Y', 'N', 'N'))
        jh = [r for r in rows if r['stn_name'] == '지행'][0]
        self.assertEqual(jh['safety_plate_yn'], 'Y')

    def test_gap_aggregation_per_platform(self):
        kind, rows = m.parse_rows(GAP, '2025-06-30')
        self.assertEqual(kind, 'gap')
        self.assertEqual(len(rows), 4)                   # 안전거리 빈 행은 제외
        agg = m.aggregate_gaps(rows)
        by = {a['platform_no']: a for a in agg}
        self.assertEqual((by['1']['gap_min_cm'], by['1']['gap_max_cm'], by['1']['gap_avg_cm'],
                          by['1']['door_cnt']), (9.5, 11.0, 10.4, 3))
        self.assertEqual((by['2']['gap_min_cm'], by['2']['door_cnt']), (8.0, 1))
        self.assertEqual(by['1']['line_name'], '1호선')
        self.assertNotIn('vals', by['1'])


class FileTests(unittest.TestCase):
    def _write(self, d, name, text, enc):
        p = os.path.join(d, name)
        with open(p, 'w', encoding=enc, newline='') as f:
            f.write(text)
        return p

    def test_read_text_cp949_and_utf8_sig(self):
        with tempfile.TemporaryDirectory() as d:
            p1 = self._write(d, 'a.csv', EV, 'cp949')
            p2 = self._write(d, 'b.csv', EV, 'utf-8-sig')
            self.assertEqual(m.read_text(p1).splitlines()[0], EV.splitlines()[0])
            self.assertEqual(m.read_text(p2).splitlines()[0], EV.splitlines()[0])

    def test_load_file_dry_run_detects_disabled_from_filename(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._write(d, '국가철도공단_수도권1호선_장애인화장실_20250630.csv', TOILET, 'cp949')
            kind, n = m.load_file(p, '2025-06-30', dry_run=True)
            self.assertEqual((kind, n), ('toilet', 2))
            _, rows = m.parse_rows(m.read_text(p), '2025-06-30', disabled='장애인' in os.path.basename(p))
            self.assertEqual(rows[0]['disabled_yn'], 'Y')

    def test_load_file_gap_dry_run_counts_platforms(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._write(d, 'gap.csv', GAP, 'cp949')
            self.assertEqual(m.load_file(p, '2025-06-30', dry_run=True), ('gap', 2))


class DbSqlTests(unittest.TestCase):
    """SQL 문자열의 컬럼·충돌키가 01 v1.3.0 DDL 과 맞는지."""

    def test_functions_exist_and_target_tables(self):
        import db_mobility
        self.assertTrue(callable(db_mobility.replace_station_elevators))
        self.assertTrue(callable(db_mobility.replace_station_toilets))
        self.assertTrue(callable(db_mobility.upsert_station_platforms))
        self.assertTrue(callable(db_mobility.update_platform_gaps))

    def test_replace_by_group_deletes_then_inserts(self):
        import db_mobility
        calls = []

        class _Conn:
            def execute(self, sql, params):
                calls.append((str(sql), dict(params)))

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class _Engine:
            def begin(self):
                return _Conn()

        saved = db_mobility.engine
        db_mobility.engine = _Engine()
        try:
            rows = [{'line_name': '1호선', 'stn_name': '안양', 'unit_seq': 1, 'oper_org': '코레일',
                     'exit_no': '2', 'detail_loc': 'x', 'capacity_person': 11, 'capacity_kg': 1000,
                     'base_dt': '2025-06-30'},
                    {'line_name': '4호선', 'stn_name': '범계', 'unit_seq': 1, 'oper_org': '코레일',
                     'exit_no': '6', 'detail_loc': 'y', 'capacity_person': 15, 'capacity_kg': 1000,
                     'base_dt': '2025-06-30'}]
            n = db_mobility.replace_station_elevators(rows)
        finally:
            db_mobility.engine = saved
        self.assertEqual(n, 2)
        deletes = [c for c in calls if c[0].startswith('DELETE')]
        inserts = [c for c in calls if c[0].startswith('INSERT')]
        self.assertEqual([c[1]['line_name'] for c in deletes], ['1호선', '4호선'])
        self.assertEqual(len(inserts), 2)
        self.assertEqual(inserts[0][1]['created_by'], db_mobility.CREATED_BY)
        # 삭제가 삽입보다 먼저
        self.assertLess(calls.index(deletes[-1]), calls.index(inserts[0]))


if __name__ == '__main__':
    unittest.main()
