"""KOSIS 통계표 구조 변경 대응 (v1.17.0) — DB 없이 도는 단위 테스트.

대상
  - db_processing.parse_intg_col_map      : 통합 테이블 칸 배치 규칙 해석·검증
  - db_processing.check_response_structure : 응답 구조가 규칙과 맞는지(분류 단계 증가·항목 증가 감지)
  - db_processing.remap_meta_rows          : 메타의 item_obj 분류를 '항목'으로 바꾸기
  - kosis_api.resolve_collect_years        : 수집 종료연도를 올해까지 넓히는 규칙
  - kosis_api 기간 분할 수집의 err=30 처리   : 미공표 연도만 담긴 구간만 빈 결과로 인정
  - kosis_api.KosisApiError                : 오류 코드 원인 안내

응답·메타 예시는 2026-10-13 KOSIS 실측 구조를 따른다.
  DT_11761_N007: 항목 T001(인원) / 분류 YRE(연령)·GRA(장애정도)·D(성별 T001 계·T002 남자·T003 여자)
  DT_11761_N008: 항목 T001(인원) / 분류 CHU(장애유형)·HJG(시도)·GRA·D → 4단계
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import db_processing  # noqa: E402
import kosis_api  # noqa: E402
from db_processing import (  # noqa: E402
    KosisDataValidationError, check_response_structure, parse_intg_col_map, remap_meta_rows,
)

N007_RULE = {'c3': None, 'itm_id': 'C3', 'item_obj': 'D'}
N008_RULE = {'itm_id': 'C4', 'item_obj': 'D'}


def n007_rows(years=(2024, 2025), ages=('000', '020'), sev=('TT', '01'), sex=('T001', 'T002', 'T003')):
    """새 구조 N007 응답(분류 3단계, 항목 1개)."""
    out = []
    for y in years:
        for a in ages:
            for g in sev:
                for x in sex:
                    out.append({'PRD_DE': str(y), 'TBL_ID': 'DT_11761_N007', 'DT': '10',
                                'C1': a, 'C1_OBJ_NM': '연령별', 'C2': g, 'C2_OBJ_NM': '장애정도별',
                                'C3': x, 'C3_OBJ_NM': '성별', 'ITM_ID': 'T001', 'ITM_NM': '인원'})
    return out


def n008_rows():
    """새 구조 N008 응답(분류 4단계)."""
    return [{'PRD_DE': '2025', 'TBL_ID': 'DT_11761_N008', 'DT': '5',
             'C1': 'CHU01', 'C1_OBJ_NM': '장애유형별', 'C2': '11', 'C2_OBJ_NM': '시도별',
             'C3': '01', 'C3_OBJ_NM': '장애정도별', 'C4': x, 'C4_OBJ_NM': '성별', 'ITM_ID': 'T001'}
            for x in ('T001', 'T002', 'T003')]


class ParseIntgColMapTests(unittest.TestCase):

    def test_none_and_empty_mean_legacy_rule(self):
        for raw in (None, '', '   '):
            rule = parse_intg_col_map(raw)
            self.assertEqual({k: rule[k] for k in ('c1', 'c2', 'c3', 'itm_id')},
                             {'c1': 'C1', 'c2': 'C2', 'c3': 'C3', 'itm_id': 'ITM'})
            self.assertFalse(rule['custom'])
            self.assertIsNone(rule['item_obj'])

    def test_json_text_from_db_is_parsed(self):
        rule = parse_intg_col_map(json.dumps(N007_RULE))
        self.assertEqual((rule['c1'], rule['c2'], rule['c3'], rule['itm_id'], rule['item_obj']),
                         ('C1', 'C2', None, 'C3', 'D'))
        self.assertTrue(rule['custom'])

    def test_source_names_are_case_insensitive(self):
        self.assertEqual(parse_intg_col_map({'itm_id': 'c4', 'item_obj': 'D'})['itm_id'], 'C4')

    def test_invalid_rules_are_rejected_with_reason(self):
        bad = [
            ('[1]', 'JSON 객체가 아님'),
            ('{not json', 'JSON 해석 실패'),
            ({'c5': 'C1'}, '알 수 없는 키'),
            ({'c1': 'C9'}, '허용되지 않음'),
            ({'c1': 'x; DROP TABLE t'}, '허용되지 않음'),
            ({'c1': None}, 'c1 는 비울 수 없음'),
            ({'itm_id': None}, 'itm_id 는 비울 수 없음'),
            ({'itm_id': 'C3', 'c3': None}, 'item_obj 를 지정해야 함'),
            ({'itm_id': 'C2', 'item_obj': 'D'}, '같은 원본 칼럼'),
            ({'itm_id': 'C4', 'item_obj': "D'; --"}, 'item_obj'),
        ]
        for raw, expect in bad:
            with self.subTest(raw=raw):
                with self.assertRaises(KosisDataValidationError) as cm:
                    parse_intg_col_map(raw, 'DT_X')
                self.assertIn(expect, str(cm.exception))
                self.assertIn('[DT_X]', str(cm.exception))

    def test_select_expressions_use_whitelisted_columns_only(self):
        exprs = db_processing._intg_select_exprs(parse_intg_col_map(N007_RULE))
        self.assertEqual(exprs, {'c1': 'c1', 'c2': 'c2', 'c3': "''", 'itm_id': 'c3'})
        exprs = db_processing._intg_select_exprs(parse_intg_col_map(N008_RULE))
        self.assertEqual(exprs, {'c1': 'c1', 'c2': 'c2', 'c3': 'c3', 'itm_id': 'c4'})


class CheckResponseStructureTests(unittest.TestCase):

    def test_matching_rule_passes(self):
        check_response_structure(n007_rows(), parse_intg_col_map(N007_RULE), 'DT_11761_N007')
        check_response_structure(n008_rows(), parse_intg_col_map(N008_RULE), 'DT_11761_N008')

    def test_extra_classification_level_without_rule_is_detected(self):
        """분류가 4단계로 늘었는데 규칙이 없으면 4번째 분류(성별)가 버려진다 → 원인이 드러나는 사유로 실패."""
        with self.assertRaises(KosisDataValidationError) as cm:
            check_response_structure(n008_rows(), parse_intg_col_map(None), 'DT_11761_N008')
        msg = str(cm.exception)
        self.assertIn('구조 변경 감지', msg)
        self.assertIn('C4(성별)', msg)
        self.assertIn('기존 데이터 유지', msg)

    def test_more_than_one_item_blocks_pivot_rule(self):
        rows = n007_rows()
        rows[0] = dict(rows[0], ITM_ID='T002')   # KOSIS 가 항목(예: 구성비)을 하나 더 내기 시작한 상황
        with self.assertRaises(KosisDataValidationError) as cm:
            check_response_structure(rows, parse_intg_col_map(N007_RULE), 'DT_11761_N007')
        self.assertIn('항목이 2개', str(cm.exception))

    def test_empty_source_for_not_null_column_is_rejected(self):
        rows = n007_rows()
        rows[3] = dict(rows[3], C3='')
        with self.assertRaises(KosisDataValidationError) as cm:
            check_response_structure(rows, parse_intg_col_map(N007_RULE), 'DT_11761_N007')
        self.assertIn('itm_id 에 넣을 C3 값이 빈 행 1건', str(cm.exception))

    def test_legacy_stats_with_unused_levels_still_pass(self):
        """종전 통계(분류 1단계 + 항목 여러 개)는 규칙 없이 그대로 통과한다(회귀 없음)."""
        rows = [{'PRD_DE': '2023', 'C1': 'A', 'C2': '', 'C3': '', 'ITM_ID': t} for t in ('T1', 'T2')]
        check_response_structure(rows, parse_intg_col_map(None), 'DT_LEGACY')


class RemapMetaRowsTests(unittest.TestCase):

    META = [
        {'obj_id': 'ITEM', 'obj_nm': '항목', 'itm_id': 'T001', 'itm_nm': '인원'},
        {'obj_id': 'YRE', 'obj_nm': '연령별', 'itm_id': '000', 'itm_nm': '합계'},
        {'obj_id': 'D', 'obj_nm': '성별', 'itm_id': 'T001', 'itm_nm': '계'},
        {'obj_id': 'D', 'obj_nm': '성별', 'itm_id': 'T002', 'itm_nm': '남자'},
    ]

    def test_item_obj_becomes_item_and_original_item_is_dropped(self):
        out = remap_meta_rows([dict(r) for r in self.META], 'D', 'DT_11761_N007')
        items = [(r['itm_id'], r['itm_nm']) for r in out if r['obj_nm'] == '항목']
        self.assertEqual(items, [('T001', '계'), ('T002', '남자')])
        self.assertTrue(all(r['obj_id'] == 'ITEM' for r in out if r['obj_nm'] == '항목'))
        self.assertIn(('YRE', '000'), [(r['obj_id'], r['itm_id']) for r in out])
        self.assertNotIn('인원', [r['itm_nm'] for r in out])

    def test_no_item_obj_returns_rows_unchanged(self):
        rows = [dict(r) for r in self.META]
        self.assertEqual(remap_meta_rows(rows, None), rows)

    def test_missing_item_obj_in_meta_is_rejected(self):
        with self.assertRaises(KosisDataValidationError) as cm:
            remap_meta_rows([dict(r) for r in self.META], 'B', 'DT_X')
        self.assertIn("item_obj 'B'", str(cm.exception))


class ResolveCollectYearsTests(unittest.TestCase):

    @staticmethod
    def _src(prd_se='Y'):
        url = f"https://kosis.kr/openapi/x?prdSe={prd_se}&startPrdDe={{from}}&endPrdDe={{to}}"
        return {'api_data_url': json.dumps({'url': url, 'format': 'json'})}

    INFO = {'collect_start_dt': '2019', 'collect_end_dt': '2024'}

    def test_yearly_stat_extends_to_this_year(self):
        self.assertEqual(kosis_api.resolve_collect_years(self._src('Y'), self.INFO, today=date(2026, 10, 13)),
                         (2019, 2026, 2024))

    def test_multi_year_stat_also_extends(self):
        self.assertEqual(kosis_api.resolve_collect_years(self._src('F'), self.INFO, today=date(2026, 1, 2))[1],
                         2026)

    def test_monthly_stat_keeps_registered_end(self):
        self.assertEqual(kosis_api.resolve_collect_years(self._src('M'), self.INFO, today=date(2026, 10, 13)),
                         (2019, 2024, 2024))

    def test_registered_end_in_future_is_kept(self):
        info = {'collect_start_dt': '2019', 'collect_end_dt': '2027'}
        self.assertEqual(kosis_api.resolve_collect_years(self._src('Y'), info, today=date(2026, 10, 13))[1], 2027)

    def test_build_url_uses_extended_range(self):
        url, _ = kosis_api.build_kosis_url({'auth': 'K'}, self._src('Y'), self.INFO, 'api_data_url')
        self.assertIn('startPrdDe=2019', url)
        self.assertRegex(url, r'endPrdDe=20(2[6-9]|[3-9]\d)')


class SplitNoDataTests(unittest.TestCase):
    """err=31 로 기간을 나눌 때 err=30(자료 없음) 처리."""

    SRC = {'stat_tbl_id': 'DT_T', 'api_data_url': json.dumps(
        {'url': 'https://kosis.kr/x?prdSe=Y&startPrdDe={from}&endPrdDe={to}', 'format': 'json'})}

    @staticmethod
    def _resp(payload):
        r = MagicMock()
        r.status_code = 200
        r.json.return_value = payload
        return r

    def _fake_get(self, data_years):
        """data_years 에 해당하는 연도만 자료가 있는 원천. 3년 넘게 한 번에 요청하면 err=31."""
        def get(url, timeout=None):
            frm = int(url.split('startPrdDe=')[1][:4])
            to = int(url.split('endPrdDe=')[1][:4])
            if to - frm + 1 > 3:
                return self._resp({'err': '31', 'errMsg': '40,000셀 초과'})
            rows = [{'PRD_DE': str(y), 'DT': '1'} for y in range(frm, to + 1) if y in data_years]
            return self._resp(rows if rows else {'err': '30', 'errMsg': '데이터가 존재하지 않습니다.'})
        return get

    def test_future_only_segment_becomes_empty(self):
        info = {'collect_start_dt': '2019', 'collect_end_dt': '2024'}
        # 2025·2026 은 아직 공표 전 → 분할 구간 2025~2026 이 err=30 이지만 빈 결과로 통과해야 한다.
        with patch.object(kosis_api.requests, 'get', side_effect=self._fake_get(set(range(2019, 2025)))), \
                patch.object(kosis_api, 'resolve_collect_years', return_value=(2019, 2026, 2024)):
            rows = kosis_api.fetch_kosis_data({'auth': 'K'}, self.SRC, info)
        self.assertEqual(sorted({r['PRD_DE'] for r in rows}), [str(y) for y in range(2019, 2025)])

    def test_no_data_in_past_segment_still_fails(self):
        """자료가 있어야 할 기간(등록 종료연도 이전)이 비면 성공으로 삼키지 않는다."""
        info = {'collect_start_dt': '2019', 'collect_end_dt': '2024'}
        have = set(range(2019, 2027)) - {2021, 2022}
        with patch.object(kosis_api.requests, 'get', side_effect=self._fake_get(have)), \
                patch.object(kosis_api, 'resolve_collect_years', return_value=(2019, 2026, 2024)):
            with self.assertRaises(kosis_api.KosisApiError) as cm:
                kosis_api.fetch_kosis_data({'auth': 'K'}, self.SRC, info)
        self.assertEqual(cm.exception.err_code, '30')


class KosisErrorHintTests(unittest.TestCase):

    def test_structure_change_hint_in_message(self):
        e = kosis_api.KosisApiError('data', '20', '필수요청변수값이 누락되었습니다. (objL)')
        self.assertIn('err=20(필수 요청변수 누락 — 통계표 구조(분류 단계) 변경 의심', str(e))
        self.assertIn('errMsg=필수요청변수값이 누락되었습니다.', str(e))

    def test_unknown_code_keeps_plain_format(self):
        self.assertEqual(str(kosis_api.KosisApiError('meta', '99', 'x')),
                         'KOSIS meta API 오류 응답: err=99, errMsg=x')


if __name__ == '__main__':
    unittest.main()
