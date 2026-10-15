"""ODCLOUD_EMP(장애인 고용 파일데이터 14종) 단위 테스트 — 네트워크·DB 없이 돈다.

값 변환기, 판(edition) 고르기, 칸 검사, 행 변환, 쪽 단위 수집, 파이프라인의 데이터셋별 격리·건너뛰기·
동기화 시각 규칙, 적재 전 검사(validate_rows·check_drop), main.py 의 부분 완료 처리를 본다.
명세·데이터 예시는 tests/fixtures/odcloud_emp_samples.json(실제 응답을 판 2~3개·행 3개로 줄인 것).
"""
from __future__ import annotations

import copy
import datetime
import json
import os
import re
import sys
import unittest
from decimal import Decimal
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors import odcloud_emp as oe  # noqa: E402
from collectors.odcloud_emp import (  # noqa: E402
    DATASET_BY_TABLE, DATASETS, DatasetError, Edition, OdcloudEmpCollector, build_rows, conv_bool,
    conv_date, conv_decimal, conv_int, conv_int_round, conv_salary, editions_from_spec,
    parse_edition_date, pick_edition, resolve_columns,
)

with open(os.path.join(ROOT, 'tests', 'fixtures', 'odcloud_emp_samples.json'), encoding='utf-8') as _f:
    FIXTURE = json.load(_f)['datasets']


def fixture_edition(table):
    """픽스처의 데이터가 나온 판(data_path)과 그 판의 원천 행."""
    fx = FIXTURE[table]
    ed = next(e for e in editions_from_spec(fx['spec']) if e.path == fx['data_path'])
    return ed, copy.deepcopy(fx['data']['data'])


def edition(fields, date='2025-12-31', path='/1/v1/uddi:aaaa'):
    return Edition(path=path, summary='x_' + date.replace('-', ''),
                   base_date=datetime.date.fromisoformat(date), fields=tuple(fields))


class ConverterTests(unittest.TestCase):
    def test_int(self):
        self.assertEqual(conv_int(5), 5)
        self.assertEqual(conv_int('1,234'), 1234)
        self.assertEqual(conv_int('12.0'), 12)
        self.assertIsNone(conv_int(' '))
        for bad in ('1.5', 'abc', True):
            with self.assertRaises(ValueError, msg=bad):
                conv_int(bad)

    def test_int_round(self):
        self.assertEqual([conv_int_round(v) for v in ('8.4', '8.5', '12.0', 3)], [8, 9, 12, 3])
        self.assertIsNone(conv_int_round(''))
        with self.assertRaises(ValueError):
            conv_int_round('여덟')

    def test_decimal(self):
        self.assertEqual(conv_decimal('3.53'), Decimal('3.53'))
        self.assertEqual(conv_decimal('3.5%'), Decimal('3.5'))
        self.assertEqual(conv_decimal(2), Decimal(2))
        for bad in ('x', 'NaN', 'Infinity'):
            with self.assertRaises(ValueError, msg=bad):
                conv_decimal(bad)

    def test_date_formats(self):
        d = datetime.date(2025, 1, 31)
        for v in ('2025-01-31', '20250131', '2025.1.31', '2025/01/31', '2025-01-31 00:00:00',
                  '2025년 1월 31일', '01/31/2025'):
            self.assertEqual(conv_date(v), d, v)
        self.assertIsNone(conv_date(None))
        for bad in ('2025-02-30', '25-01-31', '미정'):
            with self.assertRaises(ValueError, msg=bad):
                conv_date(bad)

    def test_bool(self):
        self.assertEqual([conv_bool(v) for v in ('O', '○', 'y', '예', 'X', '×', 'N', '-')],
                         [True, True, True, True, False, False, False, False])
        self.assertIsNone(conv_bool(''))
        with self.assertRaises(ValueError):
            conv_bool('△')          # 뜻을 추측하지 않는다

    def test_salary(self):
        self.assertEqual(conv_salary('(시급)10030'), ('시급', 10030))
        self.assertEqual(conv_salary('(월급) 2,000,000'), ('월급', 2000000))
        self.assertIsNone(conv_salary('()0'))            # 미기재 — 두 칸 NULL
        self.assertIsNone(conv_salary('()'))
        self.assertEqual(conv_salary('(시급)'), ('시급', None))     # 금액만 미기재
        self.assertEqual(conv_salary('(월급)0'), ('월급', None))    # 0원은 금액이 아니다
        for bad in ('()5', '10030', '시급 10030', '(시급)일만원'):
            with self.assertRaises(ValueError, msg=bad):
                conv_salary(bad)


class EditionTests(unittest.TestCase):
    def test_parse_date(self):
        self.assertEqual(parse_edition_date('a_b_20251231'), datetime.date(2025, 12, 31))
        for bad in ('x_08/13/2020', 'x(2018년)', 'x_20251332', ''):
            self.assertIsNone(parse_edition_date(bad), bad)

    def test_latest_by_date_not_by_uddi(self):
        eds = editions_from_spec(FIXTURE['emp_dis_obligation_status']['spec'])
        self.assertEqual(len(eds), 3)
        latest = pick_edition(eds)
        self.assertEqual(latest.base_date_str, '2025-12-31')
        self.assertEqual(latest.fields, ('기관명', '대상 사업체(개소)', '상시근로자수', '장애인수', '고용률'))

    def test_unparsed_editions_are_skipped_and_all_unparsed_fails(self):
        eds = [edition([], '2025-12-31', '/1/v1/uddi:a'), Edition('/1/v1/uddi:b', 'x_08/13/2020', None, ())]
        self.assertEqual(pick_edition(eds).uddi, 'uddi:a')
        with self.assertRaises(DatasetError):
            pick_edition([Edition('/1/v1/uddi:b', 'x(2018년)', None, ())])

    def test_two_editions_with_same_latest_date_fail(self):
        eds = [edition([], '2025-12-31', '/1/v1/uddi:a'), edition([], '2025-12-31', '/1/v1/uddi:b')]
        with self.assertRaisesRegex(DatasetError, '2개'):
            pick_edition(eds)

    def test_pinned_edition(self):
        eds = editions_from_spec(FIXTURE['emp_dis_regional_status']['spec'])
        self.assertEqual(pick_edition(eds, pinned_uddi='99d5ad7e-0487-4b08-adca-f7da5d8859a4').base_date_str,
                         '2024-12-31')
        with self.assertRaises(DatasetError):
            pick_edition(eds, pinned_uddi='uddi:none')

    def test_spec_without_paths_fails(self):
        with self.assertRaises(DatasetError):
            editions_from_spec({'swagger': '2.0'})


class DatasetDefinitionTests(unittest.TestCase):
    def test_fourteen_tables_without_one_off_dataset(self):
        tables = [d.table for d in DATASETS]
        self.assertEqual(len(tables), 14)
        self.assertEqual(len(set(tables)), 14)
        self.assertNotIn('emp_dis_obligation_by_type', tables)
        self.assertEqual(len({d.data_id for d in DATASETS}), 14)

    def test_targets_and_aliases_are_unique_per_dataset(self):
        for ds in DATASETS:
            targets = ds.target_columns
            self.assertEqual(len(targets), len(set(targets)), ds.table)
            aliases = [a for c in ds.cols for a in c.aliases]
            self.assertEqual(len(aliases), len(set(aliases)), ds.table)
            self.assertFalse(set(aliases) & set(ds.ignore), ds.table)
            if ds.mode == oe.MODE_YEAR:
                self.assertIn('year', targets, ds.table)


class GateTests(unittest.TestCase):
    def test_renamed_columns_resolve_through_aliases(self):
        ds = DATASET_BY_TABLE['emp_dis_obligation_status']
        old = resolve_columns(ds, ['기관', '대상 사업체(개소)', '상시 근로자(명)', '장애인(명)', '고용률'])
        new = resolve_columns(ds, ['기관명', '대상 사업체(개소)', '상시근로자수', '장애인수', '고용률'])
        self.assertEqual(old[0], '기관')
        self.assertEqual(new[0], '기관명')

    def test_missing_required_column_fails(self):
        ds = DATASET_BY_TABLE['emp_dis_obligation_status']
        with self.assertRaisesRegex(DatasetError, "필수 칸 없음: '고용률'"):
            resolve_columns(ds, ['기관명', '대상 사업체(개소)', '상시근로자수', '장애인수'])

    def test_unknown_column_fails_instead_of_being_dropped(self):
        ds = DATASET_BY_TABLE['emp_dis_obligation_status']
        with self.assertRaisesRegex(DatasetError, "대응표에 없는 칸: '비고'"):
            resolve_columns(ds, ['기관명', '대상 사업체(개소)', '상시근로자수', '장애인수', '고용률', '비고'])

    def test_both_old_and_new_name_is_ambiguous(self):
        ds = DATASET_BY_TABLE['emp_dis_obligation_status']
        with self.assertRaisesRegex(DatasetError, '둘 이상'):
            resolve_columns(ds, ['기관명', '기관', '대상 사업체(개소)', '상시근로자수', '장애인수', '고용률'])

    def test_ignored_column_is_accepted(self):
        ds = DATASET_BY_TABLE['emp_dis_center_usage']
        resolve_columns(ds, ['기관구분명', '훈련과정명', '수료인원수', '취업자수'])

    def test_column_only_in_data_rows_is_checked_too(self):
        ed, raw = fixture_edition('emp_dis_regional_status')
        raw[0]['연도'] = 2024          # 명세에는 없고 응답에만 나온 칸
        with self.assertRaisesRegex(DatasetError, "대응표에 없는 칸: '연도'"):
            build_rows(DATASET_BY_TABLE['emp_dis_regional_status'], ed, raw)


class BuildRowsTests(unittest.TestCase):
    def test_jobseeker_salary_is_split(self):
        ed, raw = fixture_edition('emp_dis_jobseeker_status')
        raw.append(dict(raw[0], 연번=4, 희망임금='()0'))
        raw.append(dict(raw[0], 연번=5, 희망임금='(시급)'))
        rows, notes = build_rows(DATASET_BY_TABLE['emp_dis_jobseeker_status'], ed, raw)
        self.assertEqual((rows[0]['salary_type'], rows[0]['salary_amount']), ('월급', 2000000))
        self.assertEqual((rows[1]['salary_type'], rows[1]['salary_amount']), ('시급', 9860))
        self.assertEqual((rows[3]['salary_type'], rows[3]['salary_amount']), (None, None))
        self.assertEqual((rows[4]['salary_type'], rows[4]['salary_amount']), ('시급', None))
        self.assertEqual(rows[0]['reg_date'], datetime.date(2024, 1, 1))
        self.assertEqual(notes['nulled'], {})

    def test_strict_column_fails_on_bad_value_even_if_nullable(self):
        ed, raw = fixture_edition('emp_dis_jobseeker_status')
        raw[1]['희망임금'] = '협의'
        with self.assertRaisesRegex(DatasetError, 'salary_type·salary_amount 1건'):
            build_rows(DATASET_BY_TABLE['emp_dis_jobseeker_status'], ed, raw)

    def test_old_edition_names_map_to_same_columns(self):
        ed, raw = fixture_edition('emp_dis_obligation_status')     # 08/13/2020 판(옛 칸 이름)
        rows, _ = build_rows(DATASET_BY_TABLE['emp_dis_obligation_status'], ed, raw)
        self.assertEqual(rows[0], {'org_name': '중앙행정기관', 'workplace_count': 52, 'worker_count': 187439,
                                   'disabled_count': 6880, 'emp_rate': Decimal('3.67')})

    def test_regional_year_comes_from_edition(self):
        ed, raw = fixture_edition('emp_dis_regional_status')        # _20241231 판
        rows, _ = build_rows(DATASET_BY_TABLE['emp_dis_regional_status'], ed, raw)
        self.assertEqual({r['year'] for r in rows}, {2024})
        self.assertEqual(rows[0]['severe_2x_rate'], Decimal('2.81'))

    def test_regional_duplicate_region_means_multi_year_and_fails(self):
        ed, raw = fixture_edition('emp_dis_regional_status')
        raw.append(dict(raw[0]))
        with self.assertRaisesRegex(DatasetError, '겹침'):
            build_rows(DATASET_BY_TABLE['emp_dis_regional_status'], ed, raw)

    def test_dev_support_booleans_and_optional_new_column(self):
        ds = DATASET_BY_TABLE['emp_dis_dev_support_org']
        ed, raw = fixture_edition('emp_dis_dev_support_org')        # 2023 판 — 최중증 통합돌봄 칸 없음
        rows, _ = build_rows(ds, ed, raw)
        self.assertEqual((rows[0]['afterschool'], rows[0]['day_activity']), (True, False))
        self.assertIsNone(rows[0]['severe_integrated_care'])
        new_fields = next(e for e in editions_from_spec(FIXTURE['emp_dis_dev_support_org']['spec'])
                          if e.base_date_str == '2025-09-30').fields
        new_raw = [{'일련번호': 1, '시군구명': '강원도 강릉시', '수행기관명': 'A센터', '최중증 통합돌봄': 'O',
                    **{k: 'X' for k in ('주간활동', '청소년방과후활동', '개인별지원계획', '부모교육', '가족휴식',
                                        '부모상담', '권리구제', '공공후견', '장애아가족양육지원', '긴급돌봄')}}]
        rows, _ = build_rows(ds, edition(new_fields, '2025-09-30'), new_raw)
        self.assertIs(rows[0]['severe_integrated_care'], True)

    def test_unknown_bool_symbol_fails_not_null_column(self):
        ed, raw = fixture_edition('emp_dis_dev_support_org')
        raw[0]['주간활동'] = '△'
        with self.assertRaisesRegex(DatasetError, 'day_activity 1건'):
            build_rows(DATASET_BY_TABLE['emp_dis_dev_support_org'], ed, raw)

    def test_startup_hours_rounded_and_counted(self):
        ed, raw = fixture_edition('emp_dis_startup_lecture')
        raw[1]['교육시간'] = '8.4'
        rows, notes = build_rows(DATASET_BY_TABLE['emp_dis_startup_lecture'], ed, raw)
        self.assertEqual([r['hours'] for r in rows], [12, 8, 8])
        self.assertEqual(notes['rounded'], {'hours': 1})
        self.assertEqual({r['year'] for r in rows}, {2018})

    def test_blank_in_not_null_column_fails_with_samples(self):
        ed, raw = fixture_edition('emp_dis_startup_lecture')
        raw[0]['교육제목'] = ' '
        with self.assertRaisesRegex(DatasetError, "title 1건 예: '교육제목'"):
            build_rows(DATASET_BY_TABLE['emp_dis_startup_lecture'], ed, raw)

    def test_nullable_column_bad_value_is_nulled_and_counted(self):
        ds = DATASET_BY_TABLE['emp_dis_staff_train_crs']
        fields = [a for c in ds.cols for a in c.aliases[:1]]
        raw = [{'구분': 'a', '교육연수과정명': 'b', '교육내용': 'c', '교육방법': 'd', '교육시작일자': '2026-03-26',
                '교육종료일자': '2026-03-27', '모집인원': 40, '수강신청 시작일자': '상시', '수강신청 종료일자': None,
                '수강신청방법': None, '교육장소명': None, '교육대상': None}]
        rows, notes = build_rows(ds, edition(fields), raw)
        self.assertIsNone(rows[0]['apply_start_date'])
        self.assertEqual(notes['nulled'], {'apply_start_date': 1})


class StubCollector(OdcloudEmpCollector):
    """URL → 응답 사전으로 odcloud 를 흉내 낸다. 값이 Exception 이면 던진다."""

    def __init__(self, pages, key='k', page_size=2):
        super().__init__(api_info={'ext_url': 'https://api.odcloud.kr/api', 'auth': key})
        self._pages = pages
        self._ps = page_size
        self.urls = []

    @property
    def page_size(self):
        return self._ps

    def get_json(self, url):
        self.urls.append(url)
        page = int(re.search(r'[?&]page=(\d+)', url).group(1))
        resp = self._pages.get(page)
        if isinstance(resp, Exception):
            raise resp
        return resp

    @staticmethod
    def pause():
        return None


def page(rows, total):
    return {'page': 1, 'perPage': 2, 'currentCount': len(rows), 'matchCount': total, 'totalCount': total,
            'data': rows}


class FetchTests(unittest.TestCase):
    DS = DATASET_BY_TABLE['emp_dis_obligation_fulfillment']
    ED = edition(['연도', '사업체수', '이행사업체수'], path='/15028087/v1/uddi:x')

    def test_paginates_until_total(self):
        rows = [{'연도': 2010 + i} for i in range(5)]
        c = StubCollector({1: page(rows[:2], 5), 2: page(rows[2:4], 5), 3: page(rows[4:], 5)})
        self.assertEqual(len(c.fetch_rows(self.DS, self.ED)), 5)
        self.assertEqual(len(c.urls), 3)
        self.assertTrue(c.urls[0].startswith('https://api.odcloud.kr/api/15028087/v1/uddi:x?page=1&perPage=2'))

    def test_count_mismatch_fails(self):
        c = StubCollector({1: page([{'연도': 1}], 3), 2: page([], 3)})
        with self.assertRaisesRegex(DatasetError, '받은 행 1'):
            c.fetch_rows(self.DS, self.ED)

    def test_zero_rows_fails_and_keeps_existing(self):
        with self.assertRaisesRegex(DatasetError, '원천 0건'):
            StubCollector({1: page([], 0)}).fetch_rows(self.DS, self.ED)

    def test_unexpected_shape_fails(self):
        with self.assertRaisesRegex(DatasetError, '형식이 다름'):
            StubCollector({1: {'code': -4, 'msg': '등록되지 않은 인증키'}}).fetch_rows(self.DS, self.ED)

    def test_service_key_encoding(self):
        c = StubCollector({1: page([{'연도': 1}], 1)}, key='ab+c/d=')
        c.fetch_rows(self.DS, self.ED)
        self.assertIn('serviceKey=ab%2Bc%2Fd%3D', c.urls[0])
        c = StubCollector({1: page([{'연도': 1}], 1)}, key='ab%2Bc')      # 이미 인코딩된 키는 그대로
        c.fetch_rows(self.DS, self.ED)
        self.assertIn('serviceKey=ab%2Bc', c.urls[0])
        self.assertNotIn('%252B', c.urls[0])


class ValidateRowsTests(unittest.TestCase):
    INFO = {
        'name': {'type': 'character varying', 'max_len': 5, 'nullable': False, 'precision': None, 'scale': None},
        'cnt': {'type': 'integer', 'max_len': None, 'nullable': True, 'precision': 32, 'scale': 0},
        'rate': {'type': 'numeric', 'max_len': None, 'nullable': False, 'precision': 5, 'scale': 2},
    }

    def test_ok(self):
        import db_odcloud_emp as D
        D.validate_rows('t', [{'name': '가나다', 'cnt': None, 'rate': Decimal('999.99')}], self.INFO)

    def test_reports_each_problem(self):
        import db_odcloud_emp as D
        rows = [{'name': None, 'cnt': 2 ** 31, 'rate': Decimal('1000')},
                {'name': '가나다라마바', 'cnt': 1, 'rate': Decimal('1')}]
        with self.assertRaises(ValueError) as cm:
            D.validate_rows('t', rows, self.INFO)
        msg = str(cm.exception)
        self.assertIn('name NOT NULL 칸 빈 값 1건', msg)
        self.assertIn('name 글자 수 5 초과 1건', msg)
        self.assertIn('cnt 숫자 범위(integer) 초과 1건', msg)
        self.assertIn('rate 숫자 범위(numeric) 초과 1건', msg)

    def test_unknown_column(self):
        import db_odcloud_emp as D
        with self.assertRaisesRegex(ValueError, '에 없는 칸: severe_integrated_care'):
            D.validate_rows('t', [{'name': 'a', 'cnt': 1, 'rate': 1, 'severe_integrated_care': True}], self.INFO)

    def test_drop_guard(self):
        import db_odcloud_emp as D
        D.check_drop('t', 50, 100, set())                 # 정확히 절반은 통과
        D.check_drop('t', 1, 0, set())                    # 바꿔치울 행이 없으면(새 연도) 검사 안 함
        with self.assertRaisesRegex(ValueError, 'ODCLOUD_EMP_ACCEPT_DROP=t'):
            D.check_drop('t', 49, 100, set())
        with self.assertRaisesRegex(ValueError, r't\(2024년\): 새 판 0행'):
            D.check_drop('t', 0, 30, set(), scope='2024년')
        D.check_drop('t', 49, 100, {'t'})
        D.check_drop('t', 49, 100, {'ALL'})

    def test_drop_guard_per_year(self):
        import db_odcloud_emp as D
        rows = [{'year': 2023}] * 10 + [{'year': 2024}] * 2 + [{'year': 2025}] * 5
        # 합계 17/20 은 통과지만 2024년 2/10 은 중단
        with self.assertRaisesRegex(ValueError, r'\(2024년\)'):
            D.check_drop_plan('t', 'year', rows, 20, {2023: 10, 2024: 10}, set())
        D.check_drop_plan('t', 'year', rows, 20, {2023: 10, 2024: 10}, {'t'})
        D.check_drop_plan('t', 'year', rows, 14, {2023: 10, 2024: 4}, set())      # 새 연도 2025 는 검사 없음
        with self.assertRaises(ValueError):
            D.check_drop_plan('t', 'replace', rows, 40, {}, set())


class FakeDb:
    """파이프라인이 쓰는 db_odcloud_emp 함수 대역."""

    def __init__(self, tracking, fail_tables=()):
        self.tracking = tracking
        self.fail_tables = set(fail_tables)
        self.loaded, self.planned = [], []
        self.pinned = {}

    def load_tracking(self, ext_api_id):
        return copy.deepcopy(self.tracking)

    def load_dataset(self, table, mode, path, base_date, rows, track, accept, pinned=False):
        if table in self.fail_tables:
            raise ValueError('적재 오류(시험) https://api.odcloud.kr/api/x?page=1&serviceKey=SECRETKEY')
        self.loaded.append((table, path, len(rows)))
        self.pinned[table] = pinned
        return {'deleted': 1, 'inserted': len(rows), 'years': [], 'tracking_updated': True}

    def plan_dataset(self, table, mode, rows, accept):
        self.planned.append(table)
        if table in self.fail_tables:
            return {'replace': 9, 'insert': len(rows), 'years': [], 'drop_ok': False, 'problem': '급감(시험)'}
        return {'replace': 1, 'insert': len(rows), 'years': [], 'drop_ok': True, 'problem': None}


class PipelineTests(unittest.TestCase):
    """run_odcloud_emp — 데이터셋별 격리, 판 변경 없음 건너뛰기, 동기화 시각 규칙, 수동 실행 옵션."""

    TABLES = ('emp_dis_regional_status', 'emp_dis_obligation_status')

    def _spec_for(self, table):
        return FIXTURE[table]['spec']

    def _run(self, mode='db', tracking=None, fail_tables=(), fetch_fail=(), env=None):
        import odcloud_emp_pipeline as P
        tracking = tracking if tracking is not None else {
            t: {'stat_api_id': i, 'api_meta_url': '', 'api_data_url': '', 'src_latest_chn_dt': None}
            for i, t in enumerate(self.TABLES)}
        fake = FakeDb(tracking, fail_tables)
        touched = []
        spec_for = self._spec_for

        def fetch_spec(self_, ds, meta_url=None):
            return spec_for(ds.table)

        def fetch_rows(self_, ds, ed):
            if ds.table in fetch_fail:
                raise RuntimeError('ODCLOUD_EMP GET failed status=500')
            if ds.table == 'emp_dis_obligation_status':
                return [{'기관명': '중앙행정기관', '대상 사업체(개소)': 56, '상시근로자수': 1, '장애인수': 1, '고용률': '3.53'}]
            return copy.deepcopy(FIXTURE['emp_dis_regional_status']['data']['data'])

        datasets = tuple(DATASET_BY_TABLE[t] for t in self.TABLES)
        environ = {k: v for k, v in os.environ.items() if not k.startswith('ODCLOUD_EMP_')}
        environ.update(env or {})
        with patch.dict(os.environ, environ, clear=True), \
                patch.object(P, 'DATASETS', datasets), \
                patch.object(P, 'get_api_info', return_value={'ext_api_id': 9, 'ext_url': 'u', 'auth': 'k'}), \
                patch.object(P.db_odcloud_emp, 'load_tracking', fake.load_tracking), \
                patch.object(P.db_odcloud_emp, 'load_dataset', fake.load_dataset), \
                patch.object(P.db_odcloud_emp, 'plan_dataset', fake.plan_dataset), \
                patch.object(P.db_mobility, 'touch_latest_sync', side_effect=lambda e: touched.append(e)), \
                patch.object(OdcloudEmpCollector, 'fetch_spec', fetch_spec), \
                patch.object(OdcloudEmpCollector, 'fetch_rows', fetch_rows), \
                patch.object(OdcloudEmpCollector, 'save_response', return_value=None), \
                patch.object(OdcloudEmpCollector, 'pause', staticmethod(lambda: None)):
            out = P.run_odcloud_emp(mode)
        return out, fake, touched

    def test_clean_run_loads_all_and_touches_sync(self):
        out, fake, touched = self._run()
        self.assertEqual((out['targets'], out['files_ok'], out['db_ok'], out['failures']), (2, 2, 2, []))
        self.assertEqual([t for t, _, _ in fake.loaded], list(self.TABLES))
        self.assertTrue(fake.loaded[0][1].endswith('uddi:96ce0ca6-582b-4244-a745-94c1220ba0fe'))   # 2025 판
        self.assertEqual(touched, ['ODCLOUD_EMP'])
        self.assertEqual(out['run_scope'], 'ALL')

    def test_unchanged_edition_is_skipped_but_counted_ok(self):
        tracking = {t: {'stat_api_id': i, 'api_meta_url': '', 'api_data_url': '', 'src_latest_chn_dt': None}
                    for i, t in enumerate(self.TABLES)}
        tracking['emp_dis_regional_status']['api_data_url'] = '/15080750/v1/uddi:96ce0ca6-582b-4244-a745-94c1220ba0fe'
        out, fake, touched = self._run(tracking=tracking)
        self.assertEqual([t for t, _, _ in fake.loaded], ['emp_dis_obligation_status'])
        self.assertEqual(out['by_table']['emp_dis_regional_status']['result'], 'unchanged')
        self.assertEqual(out['db_ok'], 2)
        # FORCE 면 같은 판도 다시 적재
        out, fake, _ = self._run(tracking=tracking, env={'ODCLOUD_EMP_FORCE': 'emp_dis_regional_status'})
        self.assertEqual(len(fake.loaded), 2)

    def test_one_dataset_failure_is_isolated_and_holds_sync(self):
        out, fake, touched = self._run(fail_tables=('emp_dis_regional_status',))
        self.assertEqual([t for t, _, _ in fake.loaded], ['emp_dis_obligation_status'])
        self.assertEqual(out['failures'][0]['id'], 'ODCLOUD_EMP:emp_dis_regional_status')
        self.assertEqual(out['failures'][0]['stage'], 'db')
        self.assertEqual((out['db_ok'], out['db_fail']), (1, 1))
        self.assertEqual(touched, [])
        # 사유에 URL 이 섞여 들어와도 인증키는 가려진다
        self.assertNotIn('SECRETKEY', out['failures'][0]['reason'])
        self.assertIn('serviceKey=***', out['failures'][0]['reason'])

    def test_collect_failure_is_recorded_with_stage(self):
        out, _, touched = self._run(fetch_fail=('emp_dis_obligation_status',))
        self.assertEqual(out['failures'][0]['stage'], 'collect')
        self.assertIn('status=500', out['failures'][0]['reason'])
        self.assertEqual(touched, [])

    def test_missing_tracking_row_fails_that_dataset(self):
        tracking = {'emp_dis_obligation_status': {'stat_api_id': 1, 'api_meta_url': '', 'api_data_url': '',
                                                  'src_latest_chn_dt': None}}
        out, fake, _ = self._run(tracking=tracking)
        self.assertIn('01 v1.11.0', out['failures'][0]['reason'])
        self.assertEqual(len(fake.loaded), 1)

    def test_all_fail_raises(self):
        with self.assertRaisesRegex(RuntimeError, '모두 실패'):
            self._run(fetch_fail=self.TABLES)

    def test_only_option_is_partial_scope_without_sync(self):
        out, fake, touched = self._run(env={'ODCLOUD_EMP_ONLY': 'emp_dis_obligation_status'})
        self.assertEqual((out['targets'], out['run_scope']), (1, 'PARTIAL'))
        self.assertEqual(touched, [])

    def test_file_mode_only_plans(self):
        out, fake, touched = self._run(mode='file')
        self.assertEqual(fake.loaded, [])
        self.assertEqual(fake.planned, list(self.TABLES))
        self.assertEqual(touched, [])

    def test_file_mode_all_plans_failing_is_total_failure(self):
        # 수집은 됐어도 적재 점검이 전부 실패하면 '전부 실패' 다(files_ok 로 판정하지 않는다)
        with self.assertRaisesRegex(RuntimeError, '모두 실패'):
            self._run(mode='file', fail_tables=self.TABLES)

    def test_bad_options_stop_the_run(self):
        for env in ({'ODCLOUD_EMP_ONLY': 'emp_dis_typo'}, {'ODCLOUD_EMP_FORCE': 'x'},
                    {'ODCLOUD_EMP_EDITION': 'emp_dis_obligation_status:uddi:a'},   # 전체 교체 표는 판 지정 불가
                    {'ODCLOUD_EMP_EDITION': 'emp_dis_regional_status'}):
            with self.assertRaises(ValueError, msg=env):
                self._run(env=env)

    def test_pinned_edition_for_year_table(self):
        out, fake, _ = self._run(env={'ODCLOUD_EMP_EDITION':
                                      'emp_dis_regional_status:uddi:99d5ad7e-0487-4b08-adca-f7da5d8859a4'})
        self.assertEqual(out['by_table']['emp_dis_regional_status']['base_date'], '2024-12-31')
        self.assertEqual(fake.pinned, {'emp_dis_regional_status': True, 'emp_dis_obligation_status': False})

    def test_missing_ext_row_is_fatal(self):
        import odcloud_emp_pipeline as P
        with patch.object(P, 'get_api_info', return_value={}):
            with self.assertRaisesRegex(RuntimeError, '01 v1.11.0'):
                P.run_odcloud_emp('db')


class MainRoutingTests(unittest.TestCase):
    """main.py — ODCLOUD_EMP 가 행 단위 수집 갈래로 가고, 실패 목록이 있으면 부분 완료(2)."""

    def _main(self, mob):
        import main as M

        class _Args:
            mode = 'db'
            ext_sys = 'odcloud_emp'
        calls, captured = [], {}
        with patch.object(M, 'setup_logging'), patch.object(M, 'parse_args', return_value=_Args), \
                patch.object(M, 'check_required_env_and_args'), \
                patch('odcloud_emp_pipeline.run_odcloud_emp', return_value=mob), \
                patch.object(M, 'write_run_summary', side_effect=captured.update), \
                patch.object(M.ops_alert, 'record_and_notify', side_effect=lambda *a, **k: calls.append((a, k))):
            with self.assertRaises(SystemExit) as cm:
                M.main()
        return cm.exception.code, captured, calls

    def test_partial(self):
        fail = {'id': 'ODCLOUD_EMP:emp_dis_job_posting', 'stage': 'collect', 'reason': '칸 검사 실패: x'}
        code, summary, calls = self._main({'targets': 14, 'files_ok': 13, 'db_ok': 13, 'db_fail': 0,
                                           'failures': [fail], 'run_scope': 'ALL'})
        self.assertEqual((code, summary['status'], summary['ext_sys']), (2, 'PARTIAL', 'ODCLOUD_EMP'))
        self.assertEqual(calls[0][0][1], [fail])
        self.assertEqual(calls[0][1]['run_scope'], 'ALL')

    def test_other_jobs_do_not_import_the_new_module(self):
        """KOSIS 등 다른 작업의 시작은 고용 수집 모듈을 불러오지 않는다(새 모듈 문제가 번지지 않게)."""
        import subprocess
        code = ("import sys, main; print('odcloud_emp_pipeline' in sys.modules, "
                "'collectors.odcloud_emp' in sys.modules)")
        env = {k: v for k, v in os.environ.items() if k != 'DB_URL'}
        out = subprocess.run([sys.executable, '-c', code], cwd=ROOT, env=env, capture_output=True, text=True)
        self.assertEqual(out.stdout.strip(), 'False False', out.stderr[-500:])

    def test_success_and_partial_scope_passthrough(self):
        code, summary, calls = self._main({'targets': 1, 'files_ok': 1, 'db_ok': 1, 'db_fail': 0,
                                           'failures': [], 'run_scope': 'PARTIAL'})
        self.assertEqual((code, summary['status']), (0, 'SUCCESS'))
        self.assertEqual(calls[0][1]['run_scope'], 'PARTIAL')


class AlertTextTests(unittest.TestCase):
    def test_job_label_and_hints(self):
        import ops_alert
        self.assertEqual(ops_alert.JOB_LABELS['ODCLOUD_EMP'], '장애인 고용 파일데이터 수집')
        hints = ops_alert.matching_action_hints(
            [{'reason': '구인 정보 수집 실패: 칸 검사 실패: 대응표에 없는 칸'},
             {'reason': '... ODCLOUD_EMP_ACCEPT_DROP=emp_dis_job_posting 로 수동 실행'}])
        self.assertEqual(len(hints), 2)


if __name__ == '__main__':
    unittest.main()
