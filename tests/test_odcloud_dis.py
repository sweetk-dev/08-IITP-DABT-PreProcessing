"""ODCLOUD_DIS(장애인 파일데이터 22종) 단위 테스트 — 네트워크·DB 없이.

SPEC_FIELDS 는 2026-11-01 odcloud 명세(infuser.odcloud.kr/oas/docs)의 최신 판 칸 목록이다. 원천 칸이 바뀌면
수집기가 그 데이터셋을 실패로 멈추는데, 이 테스트는 지금 대응표가 최신 판을 빠짐없이 받는지 확인한다.
"""
from __future__ import annotations

import datetime
import json
import os
import sys
import unittest
from decimal import Decimal
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collectors import odcloud_dis as D  # noqa: E402
from collectors.odcloud_emp import DatasetError, Edition, resolve_columns  # noqa: E402

SPEC_FIELDS = {
    15088956: ['순번', '취업일자', '근무지역', '취업직종대분류', '연령', '장애유형', '중증여부'],
    15046221: ['계획년도', '계획기관', '훈련구분', '위탁기관', '산업분류대분류', '성별', '장애유형', '중증여부', '훈련결과', '최종학력', '연령'],
    3072377: ['접수번호', '사업장명', '사업자등록번호', '접수일자', '신청금액', '결정일자', '결정금액', '지사', '산업분류명', '산업분류대분류'],
    15014780: ['연번', '지사', '기관명', '신청일자', '신청금액', '최초등록', '추가경증', '추가중증', '결정일자', '결정금액', '취소일자', '취소금액', '상세사유'],
    15053954: ['지사명', '성별', '장애유형', '중경증여부', '산업분류', '중분류', '결정금액'],
    3072380: ['지사', '지급(총액)', '사업장 주소', '산업분류(대)', '산업분류명', '지원품목'],
    3072370: ['지역', '업종', '사업체수', '부담금(백만원)'],
    15054713: ['지역', '업종', '사업체수'],
    15014779: ['사업체구분', '사업체명', '산업대분류', '주소', '상시근로자수', '고용의무 인원', '장애인 근로자 수(2배수적용)', '장애인고용률(2배수적용)', '관할지사'],
    15119194: ['기준연도', '구분', '기관구분명', '사업체명', '시도명', '시군구명'],
    15137610: ['교육일련번호', '교육연도', '교육구분명', '교육과정명'],
    15127844: ['연도', '시도', '활동지원서비스_남', '활동지원서비스_여', '응급안전서비스_남', '응급안전서비스_여'],
    15001632: ['연도', '시도', '기초생활보장 생계_의료급여 수급자(일반)', '기초생활보장 생계_의료급여 수급자(시설)', '차상위 및 기초생활보장 교육_주거급여 수급자'],
    15044286: ['연도', '시도', '기초생활보장 생계-의료급여 수급자_일반', '기초생활보장 생계-의료급여 수급자_시설', '차상위 및 기초생활보장 교육-주거급여 수급자', '차상위 초과'],
    15014781: ['연번', '인증번호', '인증구분', '인증등급', '지역코드', '시군구', '지역(시설)명', '인증대상', '신청기관', '면적', '용도구분', '인증일자', '인증만료일', '인증기관'],
    15118857: ['연번', '소속기관', '교육기관명', '소재지', '교육신청(연락처)'],
    15104256: ['연번', '기준년도', '교육기관 지정번호', '교육기관명', '대표자', '사업수행이력', '교육구분', '강사수', '관할지사'],
    15037836: ['교육기관명', '교육종목', '활동지역', '보유강사', '지정일자', '전화번호'],
    3051090: ['번호', '지역', '기관유형', '기관명', '일자리유형'],
    3051092: ['연번', '기관유형', '기관명', '연락처', '시도', '시군구', '상세주소', '직업상담', '직업평가', '직업적응훈련', '지원고용', '취업알선', '취업 후 적응지원', '직업능력개발훈련', '직업재활프로그램'],
    15066368: ['연번', '청(지청)', '센터명', '관할구역', '소재지', '대표전화'],
    3057850: ['기관명', '기관종류', '우편번호', '지역', '상세주소', '대표전화번호', '팩스번호', '누리집', '기준일자'],
}

# 01 v1.17.0 odcloud_dis_collect_v1170.sql 의 판 추적 키와 같아야 한다.
EXPECTED_KEYS = {
    'emp_dis_employment_info', 'emp_dis_supported_emp', 'emp_dis_work_support_cost', 'emp_dis_placement_subsidy',
    'emp_dis_vehicle_support', 'emp_dis_loan_support', 'emp_dis_levy_report', 'emp_dis_incentive_company',
    'emp_dis_noncompliance_list', 'emp_dis_counselor_obligation', 'emp_dis_job_program_edu',
    'stats_dis_alone_service_sido', 'stats_dis_allowance_sido', 'stats_dis_pension_sido', 'poi_bf_certification',
    'poi_dis_support_org:workaid_edu', 'poi_dis_support_org:aware_instructor', 'poi_dis_support_org:aware_edu',
    'poi_dis_support_org:job_program', 'poi_dis_support_org:voc_rehab', 'poi_dis_support_org:job_center',
    'poi_dis_support_org:lifelong_edu',
}
ORG_TYPES = {'workaid_edu', 'aware_instructor', 'aware_edu', 'job_program', 'voc_rehab', 'job_center', 'lifelong_edu'}
BD = datetime.date(2026, 6, 30)


def edition(ds, fields=None):
    return Edition(path='/%d/v1/uddi:x' % ds.data_id, summary='x_%s' % BD.strftime('%Y%m%d'), base_date=BD,
                   fields=tuple(fields if fields is not None else SPEC_FIELDS[ds.data_id]))


class DefinitionTests(unittest.TestCase):
    def test_keys_match_migration(self):
        self.assertEqual({d.key for d in D.DATASETS}, EXPECTED_KEYS)
        self.assertEqual(len(D.DATASETS), 22)

    def test_data_ids_unique_and_known(self):
        ids = [d.data_id for d in D.DATASETS]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(set(ids), set(SPEC_FIELDS))

    def test_targets_and_aliases_unique_per_dataset(self):
        for ds in D.DATASETS:
            targets = [t for c in ds.cols for t in c.targets]
            self.assertEqual(len(targets), len(set(targets)), ds.key)
            aliases = [a for c in ds.cols for a in c.aliases] + list(ds.detail)
            self.assertEqual(len(aliases), len(set(aliases)), ds.key)

    def test_org_datasets(self):
        orgs = [d for d in D.DATASETS if d.table == D.ORG_TABLE]
        self.assertEqual({d.const['org_type'] for d in orgs}, ORG_TYPES)
        for d in orgs:
            self.assertEqual(d.mode, D.MODE_REPLACE)
            self.assertEqual(d.scope, ('org_type',))
            self.assertEqual(d.key, '%s:%s' % (D.ORG_TABLE, d.const['org_type']))
            self.assertEqual(d.const['src_data_id'], str(d.data_id))
            self.assertIn('name', [t for c in d.cols for t in c.targets])
            # 2026 판 기준 key 길이(stat_tbl_id varchar(40))
            self.assertLessEqual(len(d.key), 40)

    def test_modes(self):
        for d in D.DATASETS:
            self.assertIn(d.mode, (D.MODE_EDITION, D.MODE_YEAR, D.MODE_REPLACE))
            if d.mode == D.MODE_YEAR:
                self.assertIn('year', [t for c in d.cols for t in c.targets], d.key)

    def test_latest_spec_columns_all_resolve(self):
        """2026-11-01 명세의 칸이 대응표·detail 로 모두 받아진다(모르는 칸·필수 칸 누락 없음)."""
        for ds in D.DATASETS:
            with self.subTest(ds=ds.key):
                resolve_columns(ds.base(), SPEC_FIELDS[ds.data_id])


class BuildRowsTests(unittest.TestCase):
    def test_org_detail_and_const(self):
        ds = D.DATASET_BY_KEY['poi_dis_support_org:voc_rehab']
        raw = [{'연번': 1, '기관유형': '장애인복지관', '기관명': ' 안양시관악장애인종합복지관 ', '연락처': '031-000-0000',
                '시도': '경기', '시군구': '안양시', '상세주소': '경기도 안양시 …', '직업상담': 'O', '직업평가': '',
                '직업적응훈련': 'O', '지원고용': 'X', '취업알선': 'O', '취업 후 적응지원': 'O', '직업능력개발훈련': None,
                '직업재활프로그램': 'O'}]
        rows, _ = D.build_rows(ds, edition(ds), raw)
        r = rows[0]
        self.assertEqual(r['name'], '안양시관악장애인종합복지관')
        self.assertEqual(r['org_type'], 'voc_rehab')
        self.assertEqual(r['src_data_id'], '3051092')
        self.assertEqual(r['src_base_dt'], BD)
        detail = json.loads(r['detail'])
        self.assertEqual(detail['직업상담'], 'O')
        self.assertNotIn('직업평가', detail)          # 빈 값은 담지 않는다
        self.assertNotIn('직업능력개발훈련', detail)
        self.assertEqual(detail['연번'], 1)

    def test_detail_column_missing_in_edition_is_ok(self):
        ds = D.DATASET_BY_KEY['poi_dis_support_org:job_center']
        fields = [f for f in SPEC_FIELDS[ds.data_id] if f != '관할구역']
        raw = [{'연번': 1, '청(지청)': '중부청', '센터명': '안양고용복지플러스센터', '소재지': '경기도 안양시', '대표전화': '1350'}]
        rows, _ = D.build_rows(ds, edition(ds, fields), raw)
        self.assertEqual(json.loads(rows[0]['detail']), {'연번': 1})

    def test_unknown_column_still_fails(self):
        ds = D.DATASET_BY_KEY['poi_dis_support_org:aware_edu']
        fields = SPEC_FIELDS[ds.data_id] + ['새칸']
        with self.assertRaises(DatasetError):
            D.build_rows(ds, edition(ds, fields), [])

    def test_empty_detail_is_null(self):
        ds = D.DATASET_BY_KEY['poi_dis_support_org:aware_edu']
        raw = [{'교육기관명': 'A교육원', '교육종목': '직장 내 장애인 인식개선', '활동지역': '전국', '보유강사': '',
                '지정일자': None, '전화번호': '02-0'}]
        rows, _ = D.build_rows(ds, edition(ds), raw)
        self.assertIsNone(rows[0]['detail'])
        self.assertEqual(rows[0]['sido'], '전국')

    def test_stats_year_rows_and_unique(self):
        ds = D.DATASET_BY_KEY['stats_dis_pension_sido']
        raw = [{'연도': 2024, '시도': '경기', '기초생활보장 생계-의료급여 수급자_일반': '1,234',
                '기초생활보장 생계-의료급여 수급자_시설': 10, '차상위 및 기초생활보장 교육-주거급여 수급자': 5,
                '차상위 초과': 7}]
        rows, _ = D.build_rows(ds, edition(ds), raw)
        self.assertEqual(rows[0]['year'], 2024)
        self.assertEqual(rows[0]['basic_living_general'], 1234)
        with self.assertRaises(DatasetError):
            D.build_rows(ds, edition(ds), raw + raw)      # 같은 (연도, 시도) 두 번

    def test_strict_nullable_column_fails_on_garbage(self):
        ds = D.DATASET_BY_KEY['emp_dis_vehicle_support']
        raw = [{'지사명': '경기', '성별': '남', '장애유형': '지체', '중경증여부': '경증', '산업분류': 'C', '중분류': 'C10',
                '결정금액': '금액 미상'}]
        with self.assertRaises(DatasetError):
            D.build_rows(ds, edition(ds), raw)

    def test_bf_prelim_expire_is_null(self):
        ds = D.DATASET_BY_KEY['poi_bf_certification']
        base = {'연번': 1, '인증번호': 'A-1', '인증구분': '본', '인증등급': '우수', '지역코드': '경기', '시군구': '안양시',
                '지역(시설)명': '시청', '인증대상': '건축물', '신청기관': '안양시', '면적': '1000', '용도구분': '업무시설',
                '인증일자': '2024-03-01', '인증만료일': '2034-02-28', '인증기관': '한국장애인개발원'}
        prelim = dict(base, 연번=2, 인증구분='예비', 인증만료일='본인증 교부전 또는 사용승인 후 1년 이내')
        rows, notes = D.build_rows(ds, edition(ds), [base, prelim])
        self.assertEqual(rows[0]['expire_date'], datetime.date(2034, 2, 28))
        self.assertIsNone(rows[1]['expire_date'])
        self.assertEqual(notes['nulled'], {})
        with self.assertRaises(DatasetError):
            D.build_rows(ds, edition(ds), [dict(base, 인증만료일='추후 결정')])

    def test_noncompliance_rates(self):
        ds = D.DATASET_BY_KEY['emp_dis_noncompliance_list']
        raw = [{'사업체구분': '민간', '사업체명': '가나', '산업대분류': '제조업', '주소': '서울', '상시근로자수': 300,
                '고용의무 인원': 9, '장애인 근로자 수(2배수적용)': 2, '장애인고용률(2배수적용)': '0.67%', '관할지사': '서울'}]
        rows, _ = D.build_rows(ds, edition(ds), raw)
        self.assertEqual(rows[0]['emp_rate_2x'], Decimal('0.67'))
        self.assertEqual(rows[0]['disabled_count_2x'], Decimal('2'))
        self.assertNotIn('detail', rows[0])

    def test_business_no_kept_as_text(self):
        ds = D.DATASET_BY_KEY['emp_dis_work_support_cost']
        raw = [{'접수번호': 12, '사업장명': 'A', '사업자등록번호': 1234567890, '접수일자': '2025-01-02', '신청금액': 100,
                '결정일자': '2025-02-03', '결정금액': 90, '지사': '서울', '산업분류명': '소매', '산업분류대분류': 'G'}]
        rows, _ = D.build_rows(ds, edition(ds), raw)
        self.assertEqual(rows[0]['business_no'], '1234567890')
        self.assertEqual(rows[0]['receipt_date'], datetime.date(2025, 1, 2))


class OptionsTests(unittest.TestCase):
    def test_unknown_key_fails(self):
        import odcloud_dis_pipeline as P
        with mock.patch.dict(os.environ, {'ODCLOUD_DIS_ONLY': 'poi_dis_support_org:nope'}):
            with self.assertRaises(ValueError):
                P.read_options()

    def test_known_keys(self):
        import odcloud_dis_pipeline as P
        with mock.patch.dict(os.environ, {'ODCLOUD_DIS_ONLY': 'poi_bf_certification,poi_dis_support_org:job_center',
                                          'ODCLOUD_DIS_FORCE': 'ALL'}):
            opts = P.read_options()
        self.assertEqual(opts['only'], {'poi_bf_certification', 'poi_dis_support_org:job_center'})
        self.assertEqual(opts['force'], {'ALL'})

    def test_file_name_has_no_colon(self):
        import odcloud_dis_pipeline as P
        self.assertEqual(P._file_name('poi_dis_support_org:job_center', BD), 'poi_dis_support_org__job_center_20260630.json')


class ScopeSqlTests(unittest.TestCase):
    def test_scope_and_whole_table(self):
        import db_odcloud_dis as L
        where, params = L._scope_sql(D.DATASET_BY_KEY['poi_dis_support_org:aware_edu'])
        self.assertEqual(where, ' WHERE org_type = :s0')
        self.assertEqual(params, {'s0': 'aware_edu'})
        self.assertEqual(L._scope_sql(D.DATASET_BY_KEY['poi_bf_certification']), ('', {}))

    def test_replace_drop_guard_message_names_dis_env(self):
        import db_odcloud_emp as base
        with self.assertRaises(ValueError) as cm:
            base.check_drop('poi_bf_certification', 10, 100, set(), env_name='ODCLOUD_DIS_ACCEPT_DROP')
        self.assertIn('ODCLOUD_DIS_ACCEPT_DROP=poi_bf_certification', str(cm.exception))
        base.check_drop('poi_bf_certification', 10, 100, {'poi_bf_certification'}, env_name='ODCLOUD_DIS_ACCEPT_DROP')


class AlertTests(unittest.TestCase):
    def test_label_and_hint(self):
        import ops_alert
        self.assertIn('ODCLOUD_DIS', ops_alert.JOB_LABELS)
        hints = [h for keys, h in ops_alert._ACTION_HINTS if any(k in 'ODCLOUD_DIS GET failed status=401' for k in keys)]
        self.assertTrue(hints and '활용신청' in hints[0])


if __name__ == '__main__':
    unittest.main()
