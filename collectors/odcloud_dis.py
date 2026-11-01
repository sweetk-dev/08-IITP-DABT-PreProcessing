"""공공데이터포털 장애인 파일데이터 22종 정기 수집 어댑터 — ODCLOUD_DIS (v1.22.0).

배경
    1차년도(2024)에 공공데이터포털에서 파일로 한 번 받아 둔 장애인 관련 자료 가운데 DB 에 없던 것 22종을,
    같은 원천의 최신 판으로 매월 확인해 적재한다. 원천 구조(odcloud 명세·판·칸 검사·값 변환)는 고용 14종
    수집기(collectors/odcloud_emp, ODCLOUD_EMP)와 같아서 그 부품을 그대로 쓰고, 이 모듈은 데이터셋 정의와
    두 가지 확장만 더한다.

    · 고정 값(const)      : 모든 행에 같은 값을 넣는 칸. 한 표에 원천 여러 개를 넣는 지원기관 목록
                            (poi_dis_support_org)의 종류(org_type)·데이터 ID(src_data_id).
    · 원천별 고유 칸(detail): 표에 칸을 따로 두지 않고 JSON 칸 하나에 {원천 칸 이름: 값} 으로 담는 칸.
                            원천에 없는 판이어도 실패하지 않는다(있는 것만 담는다). 대응표·detail·무시 목록
                            어디에도 없는 칸이 새로 생기면 고용 수집기와 같이 그 데이터셋을 실패시킨다.

데이터셋 묶음과 적재 방식(적재는 db_odcloud_dis)
    고용 기록·실적 11종   emp_dis_*                판 보존(MODE_EDITION) 또는 연도별 교체(MODE_YEAR)
    시도별 복지 통계 3종  stats_dis_*_sido         연도별 교체(MODE_YEAR)
    BF 인증 시설          poi_bf_certification     최신 판으로 표 교체(MODE_REPLACE)
    지원기관 목록 7종     poi_dis_support_org      종류(org_type)별로 최신 판 교체(MODE_REPLACE, scope=org_type)

    판 추적 키(key) = sys_stats_src_api_info.stat_tbl_id. 표 하나에 원천 하나면 표 이름, 지원기관 목록은
    'poi_dis_support_org:<종류>' 다(01 v1.17.0 odcloud_dis_collect_v1170.sql).

활용신청
    odcloud 데이터 API 는 공공데이터포털에서 데이터셋마다 활용신청이 돼 있어야 응답한다(안 돼 있으면
    '유효하지 않은 인증키'). 그 데이터셋만 실패로 기록되고 나머지는 계속한다.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from collectors.odcloud_emp import (
    MODE_EDITION, MODE_YEAR, YEAR_FROM_COLUMN, Col, Dataset, DatasetError, Edition, OdcloudEmpCollector,
    _blank, _c, build_rows as _emp_build_rows, conv_date, conv_decimal, conv_int, conv_str,
)

EXT_SYS = 'ODCLOUD_DIS'
MODE_REPLACE = 'replace'   # 최신 판으로 범위(표 전체 또는 scope 칸 값) 교체 — 목록형 원천

ORG_TABLE = 'poi_dis_support_org'


def _n(target, *aliases, conv=conv_str, required=True) -> Col:
    """NULL 허용 칸. 변환이 안 되는 값은 NULL 로 덮지 않고 실패시킨다(strict) — 원천 형식이 바뀐 것을 놓치지 않게."""
    return _c(target, *aliases, conv=conv, required=required, null_ok=True, strict=True)


# BF 인증 시설 원천(15014781)은 예비인증 행의 '인증만료일' 에 날짜 대신 예비인증 유효기간 규정 문구를 적는다
# (2026-06-30 판: 예비 13,966행 전부 이 문구, 본인증 8,218행은 날짜). 기한은 인증구분 '예비' 로 알 수 있어
# 문구는 싣지 않고 NULL 로 둔다. 그 밖의 날짜가 아닌 값은 여전히 실패(원천 형식 변경을 놓치지 않게).
_BF_PRELIM_EXPIRE = '본인증교부전또는사용승인후1년이내'


def conv_bf_expire(value):
    """BF 인증만료일 — 날짜면 date, 예비인증 유효기간 문구면 None, 그 밖은 ValueError."""
    if not _blank(value) and re.sub(r'\s+', '', str(value)) == _BF_PRELIM_EXPIRE:
        return None
    return conv_date(value)


@dataclass(frozen=True)
class DisDataset:
    """수집 대상 1종.

    key     : 판 추적 키(sys_stats_src_api_info.stat_tbl_id). 표에 원천이 하나면 표 이름.
    table   : 적재 표.
    mode    : MODE_EDITION | MODE_YEAR | MODE_REPLACE
    const   : 모든 행에 넣는 고정 값 {표 칸: 값}.
    detail  : detail(JSON) 칸에 담을 원천 칸 이름들.
    scope   : MODE_REPLACE 에서 교체 범위를 정하는 칸(const 의 키). 비우면 표 전체.
    """
    key: str
    table: str
    data_id: int
    label: str
    mode: str
    cols: Tuple[Col, ...]
    year_from: Optional[str] = None
    ignore: Dict[str, str] = field(default_factory=dict)
    unique: Tuple[str, ...] = ()
    const: Dict[str, object] = field(default_factory=dict)
    detail: Tuple[str, ...] = ()
    scope: Tuple[str, ...] = ()

    def base(self) -> Dataset:
        """고용 수집기의 칸 검사·행 변환에 넘길 정의. detail 칸은 무시 목록으로 넘겨 '모르는 칸' 으로 보지 않게 한다."""
        ignore = dict(self.ignore)
        for name in self.detail:
            ignore.setdefault(name, 'detail(JSON) 칸에 담음')
        return Dataset(table=self.table, data_id=self.data_id, label=self.label,
                       mode=MODE_YEAR if self.mode == MODE_YEAR else MODE_EDITION,
                       cols=self.cols, year_from=self.year_from, ignore=ignore, unique=self.unique)

    @property
    def scope_values(self) -> Dict[str, object]:
        return {k: self.const[k] for k in self.scope}


def _org(org_type: str, data_id: int, label: str, cols, detail=()) -> DisDataset:
    return DisDataset(key='%s:%s' % (ORG_TABLE, org_type), table=ORG_TABLE, data_id=data_id, label=label,
                      mode=MODE_REPLACE, cols=tuple(cols), detail=tuple(detail),
                      const={'org_type': org_type, 'src_data_id': str(data_id)}, scope=('org_type',))


def _emp(table, data_id, label, cols, mode=MODE_EDITION, year_from=None) -> DisDataset:
    return DisDataset(key=table, table=table, data_id=data_id, label=label, mode=mode,
                      year_from=year_from, cols=tuple(cols))


_SIDO_YEAR = (_c('year', '연도', conv=conv_int), _c('sido', '시도'))

DATASETS: Tuple[DisDataset, ...] = (
    # ---- 고용 기록·실적 ---------------------------------------------------------
    _emp('emp_dis_employment_info', 15088956, '장애인 취업 정보', (
        _n('seq_no', '순번', conv=conv_int),
        _c('employ_date', '취업일자', conv=conv_date),
        _n('work_region', '근무지역'),
        _n('job_category', '취업직종대분류'),
        _n('age', '연령', conv=conv_int),
        _n('disability_type', '장애유형'),
        _n('severity', '중증여부'),
    )),
    _emp('emp_dis_supported_emp', 15046221, '중증장애인 지원고용', (
        _c('year', '계획년도', conv=conv_int),
        _n('plan_org', '계획기관'),
        _n('train_type', '훈련구분'),
        _n('consign_org', '위탁기관'),
        _n('industry_major', '산업분류대분류'),
        _n('gender', '성별'),
        _n('disability_type', '장애유형'),
        _n('severity', '중증여부'),
        _n('train_result', '훈련결과'),
        _n('education', '최종학력'),
        _n('age', '연령', conv=conv_int),
    ), mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN),
    _emp('emp_dis_work_support_cost', 3072377, '근로지원비용 지원', (
        _n('receipt_no', '접수번호', conv=conv_int),
        _n('company_name', '사업장명'),
        _n('business_no', '사업자등록번호'),
        _n('receipt_date', '접수일자', conv=conv_date),
        _n('apply_amount', '신청금액', conv=conv_int),
        _n('decision_date', '결정일자', conv=conv_date),
        _n('decision_amount', '결정금액', conv=conv_int),
        _n('branch', '지사'),
        _n('industry', '산업분류명'),
        _n('industry_major', '산업분류대분류'),
    )),
    _emp('emp_dis_placement_subsidy', 15014780, '취업알선지원금', (
        _n('seq_no', '연번', conv=conv_int),
        _n('branch', '지사'),
        _n('org_name', '기관명'),
        _n('apply_date', '신청일자', conv=conv_date),
        _n('apply_amount', '신청금액', conv=conv_int),
        _n('first_reg_count', '최초등록', conv=conv_int),
        _n('add_mild_count', '추가경증', conv=conv_int),
        _n('add_severe_count', '추가중증', conv=conv_int),
        _n('decision_date', '결정일자', conv=conv_date),
        _n('decision_amount', '결정금액', conv=conv_int),
        _n('cancel_date', '취소일자', conv=conv_date),
        _n('cancel_amount', '취소금액', conv=conv_int),
        _n('detail_reason', '상세사유'),
    )),
    _emp('emp_dis_vehicle_support', 15053954, '차량지원결정', (
        _n('branch', '지사명'),
        _n('gender', '성별'),
        _n('disability_type', '장애유형'),
        _n('severity', '중경증여부'),
        _n('industry', '산업분류'),
        _n('industry_mid', '중분류'),
        _n('decision_amount', '결정금액', conv=conv_int),
    )),
    _emp('emp_dis_loan_support', 3072380, '사업주 융자지원', (
        _n('branch', '지사'),
        _n('paid_total', '지급(총액)', conv=conv_int),
        _n('address', '사업장 주소'),
        _n('industry_major', '산업분류(대)'),
        _n('industry', '산업분류명'),
        _n('support_item', '지원품목'),
    )),
    _emp('emp_dis_levy_report', 3072370, '고용부담금 신고 사업체', (
        _n('region', '지역'),
        _n('industry', '업종'),
        _n('company_count', '사업체수', conv=conv_int),
        _n('levy_amount_mil', '부담금(백만원)', conv=conv_int),
    )),
    _emp('emp_dis_incentive_company', 15054713, '장려금 지원 사업체', (
        _n('region', '지역'),
        _n('industry', '업종'),
        _n('company_count', '사업체수', conv=conv_int),
    )),
    _emp('emp_dis_noncompliance_list', 15014779, '고용의무 불이행 명단공표', (
        _n('company_type', '사업체구분'),
        _n('company_name', '사업체명'),
        _n('industry_major', '산업대분류'),
        _n('address', '주소'),
        _n('worker_count', '상시근로자수', conv=conv_int),
        _n('obligation_count', '고용의무 인원', conv=conv_int),
        _n('disabled_count_2x', '장애인 근로자 수(2배수적용)', conv=conv_decimal),
        _n('emp_rate_2x', '장애인고용률(2배수적용)', conv=conv_decimal),
        _n('branch', '관할지사'),
    )),
    _emp('emp_dis_counselor_obligation', 15119194, '직업생활상담원 선임의무사업체', (
        _c('year', '기준연도', conv=conv_int),
        _n('category', '구분'),
        _n('org_type', '기관구분명'),
        _n('company_name', '사업체명'),
        _n('sido', '시도명'),
        _n('sigungu', '시군구명'),
    ), mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN),
    _emp('emp_dis_job_program_edu', 15137610, '일자리사업 참여자 교육', (
        _n('edu_seq', '교육일련번호', conv=conv_int),
        _c('year', '교육연도', conv=conv_int),
        _n('edu_type', '교육구분명'),
        _n('course_name', '교육과정명'),
    ), mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN),
    # ---- 시도별 복지 통계 -------------------------------------------------------
    DisDataset(key='stats_dis_alone_service_sido', table='stats_dis_alone_service_sido', data_id=15127844,
               label='독거 장애인 서비스 시도별', mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN,
               unique=('year', 'sido'), cols=_SIDO_YEAR + (
                   _n('act_support_male', '활동지원서비스_남', conv=conv_int),
                   _n('act_support_female', '활동지원서비스_여', conv=conv_int),
                   _n('emg_safety_male', '응급안전서비스_남', conv=conv_int),
                   _n('emg_safety_female', '응급안전서비스_여', conv=conv_int),
               )),
    DisDataset(key='stats_dis_allowance_sido', table='stats_dis_allowance_sido', data_id=15001632,
               label='장애수당 수급자 시도별', mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN,
               unique=('year', 'sido'), cols=_SIDO_YEAR + (
                   _n('basic_living_general', '기초생활보장 생계_의료급여 수급자(일반)', conv=conv_int),
                   _n('basic_living_facility', '기초생활보장 생계_의료급여 수급자(시설)', conv=conv_int),
                   _n('near_poor_edu_housing', '차상위 및 기초생활보장 교육_주거급여 수급자', conv=conv_int),
               )),
    DisDataset(key='stats_dis_pension_sido', table='stats_dis_pension_sido', data_id=15044286,
               label='장애인연금 수급자 시도별', mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN,
               unique=('year', 'sido'), cols=_SIDO_YEAR + (
                   _n('basic_living_general', '기초생활보장 생계-의료급여 수급자_일반', conv=conv_int),
                   _n('basic_living_facility', '기초생활보장 생계-의료급여 수급자_시설', conv=conv_int),
                   _n('near_poor_edu_housing', '차상위 및 기초생활보장 교육-주거급여 수급자', conv=conv_int),
                   _n('above_near_poor', '차상위 초과', conv=conv_int),
               )),
    # ---- BF 인증 시설 -------------------------------------------------------------
    DisDataset(key='poi_bf_certification', table='poi_bf_certification', data_id=15014781,
               label='BF 인증 시설', mode=MODE_REPLACE, cols=(
                   _n('seq_no', '연번', conv=conv_int),
                   _n('cert_no', '인증번호'),
                   _n('cert_type', '인증구분'),
                   _n('cert_grade', '인증등급'),
                   _n('region', '지역코드'),
                   _n('sigungu', '시군구'),
                   _c('facility_name', '지역(시설)명'),
                   _n('cert_target', '인증대상'),
                   _n('applicant', '신청기관'),
                   _n('area', '면적'),
                   _n('usage_type', '용도구분'),
                   _n('cert_date', '인증일자', conv=conv_date),
                   _n('expire_date', '인증만료일', conv=conv_bf_expire),   # 예비인증은 NULL
                   _n('cert_body', '인증기관'),
               )),
    # ---- 지원기관 목록 (poi_dis_support_org) -------------------------------------------
    _org('workaid_edu', 15118857, '근로지원인 양성교육기관', (
        _c('name', '교육기관명'),
        _n('category', '소속기관'),
        _n('address', '소재지'),
        _n('tel', '교육신청(연락처)'),
    ), detail=('연번',)),
    _org('aware_instructor', 15104256, '인식개선 교육 강사지원 수행기관', (
        _c('name', '교육기관명'),
        _n('category', '교육구분'),
    ), detail=('연번', '기준년도', '교육기관 지정번호', '대표자', '사업수행이력', '강사수', '관할지사')),
    _org('aware_edu', 15037836, '인식개선 교육 지정 교육기관', (
        _c('name', '교육기관명'),
        _n('category', '교육종목'),
        _n('sido', '활동지역'),
        _n('tel', '전화번호'),
    ), detail=('보유강사', '지정일자')),
    _org('job_program', 3051090, '장애인일자리사업 수행기관', (
        _c('name', '기관명'),
        _n('category', '기관유형'),
        _n('sido', '지역'),
    ), detail=('번호', '일자리유형')),
    _org('voc_rehab', 3051092, '중증장애인직업재활지원사업 수행기관', (
        _c('name', '기관명'),
        _n('category', '기관유형'),
        _n('tel', '연락처'),
        _n('sido', '시도'),
        _n('sigungu', '시군구'),
        _n('address', '상세주소'),
    ), detail=('연번', '직업상담', '직업평가', '직업적응훈련', '지원고용', '취업알선', '취업 후 적응지원',
               '직업능력개발훈련', '직업재활프로그램')),
    _org('job_center', 15066368, '고용복지플러스센터', (
        _c('name', '센터명'),
        _n('category', '청(지청)'),
        _n('address', '소재지'),
        _n('tel', '대표전화'),
    ), detail=('연번', '관할구역')),
    _org('lifelong_edu', 3057850, '장애인평생교육기관', (
        _c('name', '기관명'),
        _n('category', '기관종류'),
        _n('sido', '지역'),
        _n('address', '상세주소'),
        _n('tel', '대표전화번호'),
        _n('homepage', '누리집'),
    ), detail=('우편번호', '팩스번호', '기준일자')),
)

DATASET_BY_KEY: Dict[str, DisDataset] = {d.key: d for d in DATASETS}


def _detail_value(value):
    """detail JSON 에 담을 값 — 숫자는 그대로, 문자열은 앞뒤 공백 제거. 빈 값은 담지 않는다(None)."""
    if _blank(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value.strip() if isinstance(value, str) else value


def build_rows(ds: DisDataset, edition: Edition, raw_rows: Sequence[dict]) -> Tuple[List[dict], dict]:
    """원천 행 → 표 행. 칸 검사·값 변환은 고용 수집기와 같고, 고정 값·detail(JSON)을 더한다.

    detail 은 JSON 문자열로 넣는다(PostgreSQL 이 jsonb 로 받는다). 담을 값이 하나도 없으면 NULL.
    """
    rows, notes = _emp_build_rows(ds.base(), edition, raw_rows, log_label=EXT_SYS)
    if len(rows) != len(raw_rows):   # 행 변환은 1:1 이어야 detail 을 같은 순서로 붙일 수 있다
        raise DatasetError('행 변환 결과 수 %d 가 원천 %d 와 다름' % (len(rows), len(raw_rows)))
    for row, raw in zip(rows, raw_rows):
        if ds.detail:
            packed = {}
            for name in ds.detail:
                v = _detail_value(raw.get(name))
                if v is not None:
                    packed[name] = v
            row['detail'] = json.dumps(packed, ensure_ascii=False, sort_keys=True) if packed else None
        row.update(ds.const)
    return rows, notes


class OdcloudDisCollector(OdcloudEmpCollector):
    """네트워크(명세·데이터) — 고용 수집기와 같고 이름·환경변수(ODCLOUD_DIS_*)만 다르다."""
    EXT_SYS = EXT_SYS

    def collect(self) -> List[dict]:
        raise NotImplementedError('ODCLOUD_DIS 는 odcloud_dis_pipeline.run_odcloud_dis 로 실행한다')
