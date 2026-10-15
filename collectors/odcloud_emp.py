"""공공데이터포털 장애인 고용 파일데이터 14종 정기 수집 어댑터 — ODCLOUD_EMP (v1.19.0 · 판 보존 v1.20.0).

배경
    emp_dis_* 표 15종은 2025-07 에 공공데이터포털(data.go.kr) 파일데이터를 한 번 받아 넣은 뒤
    갱신되지 않았다. 파일데이터는 포털이 자동으로 OpenAPI(odcloud)로도 열어 주므로, 같은 원천을
    API 로 정기 수집해 새 판(edition)이 올라오면 표를 갱신한다.
    제외: emp_dis_obligation_by_type(15074236) — 2023-12-31 판 1회로 끝난 데이터라 수집하지 않는다.

원천 구조 (odcloud)
    명세(키 불필요) : https://infuser.odcloud.kr/oas/docs?namespace={데이터ID}/v1
        → paths 에 판마다 경로 1개: /{데이터ID}/v1/uddi:xxxxxxxx-...
          각 경로의 summary 가 '..._YYYYMMDD' 로 끝난다(그 판의 기준일).
          definitions['uddi:..._model'].properties 가 그 판의 칸 이름(한글) 목록이다.
    데이터          : https://api.odcloud.kr/api/{경로}?page=N&perPage=M&serviceKey=KEY
        → {currentCount, data[], matchCount, page, perPage, totalCount}

판 고르기 — 가장 큰 기준일
    uddi 는 날짜 순서가 아니다. summary 끝의 _YYYYMMDD 를 날짜로 읽어 가장 늦은 판을 쓴다.
    옛 판에는 '_08/13/2020', '(2018년)' 처럼 읽을 수 없는 summary 가 섞여 있어 그런 판은 건너뛴다.
    읽히는 판이 하나도 없거나, 가장 늦은 기준일의 판이 둘 이상이면(어느 쪽이 맞는지 모름) 실패.

칸 검사(column gate) — 조용히 버리지 않는다
    데이터셋마다 '원천 칸 이름 → 표 칸' 대응표를 코드에 명시한다(DATASETS). 판이 바뀌며 칸 이름이
    바뀐 사례가 있어(예: '기관' → '기관명') 표 칸마다 알려진 이름을 여러 개 둘 수 있다.
        · 필수 칸이 판에 없음                  → 그 데이터셋 실패
        · 알려진 이름이 한 판에 둘 이상 있음     → 실패(어느 쪽을 쓸지 모름)
        · 대응표에도 무시 목록에도 없는 칸이 있음 → 실패(새 정보를 말없이 버리지 않는다)
    값 변환: 날짜(여러 형식) · 숫자(천 단위 쉼표, '12.0') · O/X·Y/N·○ → 참/거짓 · 빈 값 → NULL.
        NOT NULL 칸에서 변환이 안 되거나 비어 있으면 그 데이터셋 실패, NULL 허용 칸이면 NULL 로 두고
        건수를 기록한다(로그·실행 결과).

적재 방식(이 모듈은 행을 만들 뿐, 적재는 db_odcloud_emp)
    모든 행에 src_base_dt = 판 기준일을 넣는다(01 v1.12.0). 지난 판을 지우지 않고 남기기 위해서다.
    MODE_YEAR    연도 칸이 있는 표 — 새 판에 들어 있는 연도의 행만 지우고 다시 넣는다(다른 연도 보존).
                 연도 칸이 원천에 없는 지역별 고용 현황은 판의 기준일 연도를 연도로 쓴다.
    MODE_EDITION 연도 칸이 없는 표 — 같은 판 기준일의 행만 지우고 다시 넣는다(다른 판 보존, v1.20.0).
                 (v1.19.0 의 MODE_REPLACE — 표 전체 교체 — 는 지난 판을 지워 버려 바꿨다.)

인증키 마스킹
    serviceKey 는 URL 쿼리에 실린다. 로그·예외 메시지는 BaseCollector.http_get 과 file_utils 의
    마스킹 규칙(serviceKey=***)을 거친다. 이 모듈이 직접 만드는 메시지에는 URL 을 넣지 않는다.
"""
from __future__ import annotations

import datetime
import logging
import os
import re
import urllib.parse
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from collectors.mobility_base import MobilityCollector

logger = logging.getLogger(__name__)

EXT_SYS = 'ODCLOUD_EMP'
# 명세 주소 형식. sys_stats_src_api_info.api_meta_url 에 데이터셋별로 같은 값이 등록돼 있고,
# 그 값이 비어 있을 때만 이 형식으로 만든다.
SPEC_URL_FMT = 'https://infuser.odcloud.kr/oas/docs?namespace={data_id}/v1'
DEFAULT_BASE_URL = 'https://api.odcloud.kr/api'
# odcloud 는 perPage 를 크게 받아 주지만, 한 번 응답이 너무 커지지 않게 1,000 행씩 받는다
# (가장 큰 구직자 현황 약 4.5만 행 → 45회 호출).
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 10000

MODE_EDITION = 'edition'   # v1.20.0 — 판 단위 교체(이전 이름 MODE_REPLACE: 표 전체 교체)
MODE_YEAR = 'year'
# MODE_YEAR 에서 연도를 어디서 얻는가
YEAR_FROM_COLUMN = 'column'    # 원천의 연도 칸(대응표의 'year')
YEAR_FROM_EDITION = 'edition'  # 판의 기준일 연도(원천에 연도 칸이 없는 지역별 고용 현황)


class DatasetError(RuntimeError):
    """데이터셋 하나를 이번 실행에서 포기해야 하는 문제(원천 이상·칸 검사·변환 오류).

    다른 데이터셋 처리에는 영향을 주지 않는다(파이프라인이 데이터셋 단위로 잡는다).
    메시지는 실행 기록·알림 메일에 그대로 실리므로 운영자가 읽을 수 있는 문장으로 쓴다.
    """


# ---------------------------------------------------------------------------
# 값 변환기 — 실패하면 ValueError. 빈 값(None·공백)은 None 을 돌려준다.
# ---------------------------------------------------------------------------
def _blank(value) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def conv_str(value) -> Optional[str]:
    """문자열 — 앞뒤 공백 제거, 빈 문자열은 None. 숫자로 온 값도 문자열로 바꾼다."""
    if _blank(value):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value).strip()


def conv_int(value) -> Optional[int]:
    """정수 — 1234 · '1,234' · '12.0' 을 받는다. 소수부가 0 이 아닌 값('1.5')은 오류(반올림하지 않는다)."""
    if _blank(value):
        return None
    if isinstance(value, bool):
        raise ValueError('참/거짓 값은 정수가 아님')
    if isinstance(value, int):
        return value
    token = str(value).strip().replace(',', '')
    if re.fullmatch(r'[+-]?\d+', token):
        return int(token)
    try:
        dec = Decimal(token)
    except InvalidOperation:
        raise ValueError('정수가 아님') from None
    if dec != dec.to_integral_value():
        raise ValueError('소수부가 있음')
    return int(dec)


def conv_int_round(value) -> Optional[int]:
    """정수 칸이지만 원천에 소수가 드물게 섞이는 값(창업넷 교육시간 '8.4') — 반올림해 정수로.

    표 칸(hours integer)을 실수로 바꾸면 공개 API(09)의 정수 필드가 그 값을 읽지 못한다. 그래서 값을
    반올림하되, 원래 값과 달라진 건수는 build_rows 가 notes['rounded'] 와 경고 로그로 남긴다(조용히 바꾸지 않음).
    """
    if _blank(value):
        return None
    if isinstance(value, bool):
        raise ValueError('참/거짓 값은 숫자가 아님')
    dec = conv_decimal(value)
    return int(dec.quantize(Decimal(1), rounding=ROUND_HALF_UP))


def conv_decimal(value) -> Optional[Decimal]:
    """실수(고용률 등) — '3.53' · '3,53'(X) · '3.5%' 를 받는다. 천 단위 쉼표와 % 기호는 지운다."""
    if _blank(value):
        return None
    if isinstance(value, bool):
        raise ValueError('참/거짓 값은 숫자가 아님')
    token = str(value).strip().replace(',', '').rstrip('%').strip()
    try:
        dec = Decimal(token)
    except InvalidOperation:
        raise ValueError('숫자가 아님') from None
    if not dec.is_finite():
        raise ValueError('숫자가 아님')
    return dec


_DATE_PATTERNS = (
    # (정규식, 연·월·일 그룹 순서)
    (re.compile(r'(\d{4})[-./](\d{1,2})[-./](\d{1,2})(?:[ T].*)?'), (1, 2, 3)),   # 2025-01-31, 2025.1.31, 2025-01-31 00:00:00
    (re.compile(r'(\d{4})(\d{2})(\d{2})'), (1, 2, 3)),                            # 20250131
    (re.compile(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일'), (1, 2, 3)),             # 2025년 1월 31일
    (re.compile(r'(\d{1,2})/(\d{1,2})/(\d{4})'), (3, 1, 2)),                       # 01/31/2025 (미국식, odcloud 옛 판)
)


def conv_date(value) -> Optional[datetime.date]:
    """날짜 — 위 형식들을 받아 date 로. 달력에 없는 날짜('2025-02-30')는 오류."""
    if _blank(value):
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    token = str(value).strip()
    for pattern, order in _DATE_PATTERNS:
        m = pattern.fullmatch(token)
        if m:
            y, mo, d = (int(m.group(i)) for i in order)
            try:
                return datetime.date(y, mo, d)
            except ValueError:
                raise ValueError('달력에 없는 날짜') from None
    raise ValueError('날짜 형식이 아님')


_TRUE_TOKENS = {'O', 'Ｏ', '○', '●', 'Y', 'YES', 'TRUE', 'T', '1', '예', '유', '있음', '해당', 'V', '√'}
_FALSE_TOKENS = {'X', 'Ｘ', '×', '✕', 'N', 'NO', 'FALSE', 'F', '0', '아니오', '무', '없음', '미해당', '-'}


def conv_bool(value) -> Optional[bool]:
    """참/거짓 — O·○·Y·예 → True, X·×·N·- → False. 그 밖의 기호는 오류(뜻을 추측하지 않는다)."""
    if _blank(value):
        return None
    if isinstance(value, bool):
        return value
    token = str(value).strip().upper()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    raise ValueError('참/거짓 기호가 아님')


_SALARY_RE = re.compile(r'\(\s*([^()]*?)\s*\)\s*([\d,]+(?:\.0+)?)?\s*(?:원)?')


def conv_salary(value) -> Optional[Tuple[Optional[str], Optional[int]]]:
    """구직자 현황의 희망임금 '(시급)10030' → ('시급', 10030). 표에는 임금형태·금액 두 칸으로 들어간다.

    원천이 '미기재' 를 적는 두 가지 방식(2025-12-31 판 44,588행 실측)
        '()0'    형태·금액 모두 없음(323행) → (None, None) 대신 None — 두 칸 모두 NULL
        '(시급)' 형태만 있고 금액 없음(89행) → ('시급', None) — 금액만 NULL ('(시급)0' 도 같게 본다)
    0원을 금액으로 넣으면 평균·분포 집계가 틀어지므로 NULL 로 둔다(01 v1.11.0 에서 두 칸의 NOT NULL 을
    풀었다). 형태 없이 0 이 아닌 금액이 오거나 형식이 다르면 오류 — 뜻을 추측하지 않는다
    (대응표에서 strict 로 두어 NULL 로 덮지 않고 그 데이터셋을 실패시킨다).
    """
    if _blank(value):
        return None
    m = _SALARY_RE.fullmatch(str(value).strip())
    if not m:
        raise ValueError("'(형태)금액' 형식이 아님")
    kind = m.group(1).strip()
    amount = conv_int(m.group(2)) if m.group(2) else None
    if not kind:
        if not amount:
            return None
        raise ValueError('임금형태 없이 금액만 있음')
    # '(시급)0' 처럼 형태만 있고 금액이 0 이면 금액 미기재로 본다(0원은 실제 희망임금이 아니다).
    return kind, (amount or None)


# ---------------------------------------------------------------------------
# 데이터셋 정의
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Col:
    """원천 칸 → 표 칸 대응 하나.

    target   : 표 칸 이름. 한 원천 칸이 두 표 칸으로 나뉘면 튜플(희망임금 → 임금형태·금액).
    aliases  : 이 칸으로 알려진 원천 칸 이름들(판마다 이름이 바뀐 사례를 모두 적는다). 첫 이름이 최신 판.
    conv     : 값 변환기(위 conv_*).
    required : 판에 이 칸이 반드시 있어야 하는가. False 면 없을 때 표 칸을 NULL 로 둔다.
    null_ok  : 표 칸이 NULL 을 받는가(01 DDL 과 같게 적는다). False 인데 값이 비거나 변환이 안 되면 실패.
               True 면 빈 값은 NULL, 변환이 안 되는 값도 NULL 로 두고 건수를 기록한다.
    strict   : True 면 null_ok 여도 변환이 안 되는 값은 실패(빈 값·변환기가 None 을 돌려준 값만 NULL).
    """
    target: object
    aliases: Tuple[str, ...]
    conv: Callable
    required: bool = True
    null_ok: bool = False
    strict: bool = False

    @property
    def targets(self) -> Tuple[str, ...]:
        return self.target if isinstance(self.target, tuple) else (self.target,)


@dataclass(frozen=True)
class Dataset:
    """수집 대상 데이터셋 1종 = 표 1개.

    table      : 적재 표(emp_dis_*). sys_stats_src_api_info.stat_tbl_id 와 같은 값.
    data_id    : 공공데이터포털 데이터 ID(odcloud namespace). 데이터셋 이름이 바뀌어도 ID 는 그대로다.
    label      : 화면·메일용 짧은 이름.
    mode       : MODE_EDITION | MODE_YEAR
    year_from  : MODE_YEAR 일 때 연도 출처(YEAR_FROM_COLUMN | YEAR_FROM_EDITION).
    cols       : 원천 칸 대응표.
    ignore     : 알고 있지만 표에 칸이 없어 싣지 않는 원천 칸 {이름: 사유}.
    unique     : 판 안에서 겹치면 안 되는 표 칸 조합(겹치면 실패). 버리는 칸이 있을 때 그 칸 때문에
                 행이 갈라지는지 확인하거나, 표의 유니크 인덱스를 미리 검사하는 데 쓴다.
    """
    table: str
    data_id: int
    label: str
    mode: str
    cols: Tuple[Col, ...]
    year_from: Optional[str] = None
    ignore: Dict[str, str] = field(default_factory=dict)
    unique: Tuple[str, ...] = ()

    @property
    def target_columns(self) -> Tuple[str, ...]:
        out = []
        for c in self.cols:
            out.extend(c.targets)
        if self.mode == MODE_YEAR and self.year_from == YEAR_FROM_EDITION:
            out.append('year')
        out.append('src_base_dt')       # 모든 행에 판 기준일(01 v1.12.0)
        return tuple(out)


def _c(target, *aliases, conv=conv_str, required=True, null_ok=False, strict=False) -> Col:
    return Col(target=target, aliases=tuple(aliases), conv=conv, required=required, null_ok=null_ok,
               strict=strict)


# 순서 = 수집 순서. 큰 데이터셋(구직자·구인)을 앞에 두어 일일 호출 한도 문제를 먼저 드러낸다.
DATASETS: Tuple[Dataset, ...] = (
    Dataset(
        table='emp_dis_jobseeker_status', data_id=15014774, label='구직자 현황', mode=MODE_EDITION,
        cols=(
            _c('seq_no', '연번', conv=conv_int),
            _c('reg_date', '구직등록일', conv=conv_date),
            _c('age', '연령', conv=conv_int),
            _c('region', '희망지역'),
            _c('job_type', '희망직종'),
            # '(시급)10030' 한 칸 → salary_type '시급' + salary_amount 10030. '()0' 은 두 칸, '(시급)' 은 금액만 NULL.
            _c(('salary_type', 'salary_amount'), '희망임금', conv=conv_salary, null_ok=True,
               strict=True),
            _c('disability_type', '장애유형'),
            _c('severity', '중증여부'),
            _c('org_type', '기관분류'),
        ),
    ),
    Dataset(
        table='emp_dis_job_posting', data_id=3072637, label='구인 정보', mode=MODE_EDITION,
        cols=(
            _c('seq_no', '연번', conv=conv_int),
            _c('apply_date', '구인신청일자', conv=conv_date),
            _c('recruit_period', '모집기간'),
            _c('company_name', '사업장명'),
            # 2026-06-30 판에 모집직종 빈 행이 있다(7,085행 중 2행) — 01 v1.11.0 에서 NOT NULL 해제.
            _c('job_type', '모집직종', null_ok=True),
            _c('emp_type', '고용형태'),
            _c('salary_type', '임금형태'),
            _c('salary_amount', '임금', conv=conv_int, null_ok=True),
            _c('hire_type', '입사형태'),
            _c('experience', '요구경력'),
            _c('education', '요구학력'),
            _c('major', '전공계열', null_ok=True),
            _c('license', '요구자격증', null_ok=True),
            _c('address', '사업장 주소', '사업장주소'),
            _c('company_type', '기업형태'),
            _c('office', '담당기관'),
            _c('reg_date', '등록일', conv=conv_date),
            _c('tel', '연락처'),
        ),
    ),
    Dataset(
        table='emp_dis_center_usage', data_id=15118552, label='발달장애인훈련센터 이용', mode=MODE_EDITION,
        cols=(
            _c('train_org', '기관구분명'),
            # 2025-07 첫 적재 때부터 '수료인원수' 를 user_count 에 넣어 왔다(칸 이름은 이용자 수).
            _c('user_count', '수료인원수', '이용자수', conv=conv_int),
            _c('employed_count', '취업자수', conv=conv_int),
        ),
        # 표에 과정 칸이 없다. 센터당 1행일 때만 뜻이 보존되므로 unique 로 센터 중복을 막는다.
        ignore={'훈련과정명': '표에 과정 칸 없음(센터당 1행 확인 후 적재)'},
        unique=('train_org',),
    ),
    Dataset(
        table='emp_dis_obligation_status', data_id=15014785, label='고용의무 현황', mode=MODE_EDITION,
        cols=(
            _c('org_name', '기관명', '기관'),
            _c('workplace_count', '대상 사업체(개소)', conv=conv_int),
            _c('worker_count', '상시근로자수', '상시 근로자(명)', conv=conv_int),
            _c('disabled_count', '장애인수', '장애인(명)', conv=conv_int),
            _c('emp_rate', '고용률', conv=conv_decimal),
        ),
    ),
    Dataset(
        table='emp_dis_consulting_his', data_id=15014777, label='고용컨설팅', mode=MODE_EDITION,
        cols=(
            _c('seq_no', '순번', conv=conv_int),
            _c('diagnosis_no', '진단번호'),
            _c('receive_date', '접수일자', conv=conv_date),
            _c('business_no', '사업자등록번호'),
            _c('company_name', '사업체명'),
            _c('address', '소재지주소'),
            _c('business_type', '사업체유형'),
            _c('regional_office', '관할 지역본부 및 지사'),
            _c('office_tel', '관할 지역본부 및 지사 대표번호'),
        ),
    ),
    Dataset(
        table='emp_dis_burden_workplace', data_id=15067959, label='부담금감면 연계고용사업장',
        mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN,
        cols=(
            _c('year', '연도', conv=conv_int),
            _c('company_name', '연계고용대상사업장명', '사업장명'),
            _c('facility_type', '사업장유형구분', '시설구분'),
            _c('address', '지역', '사업장소재지'),
            _c('work_item', '도급품목'),
            _c('worker_count', '상시근로자수', conv=conv_int),
            _c('disabled_count', '장애인근로자수', '장애인수', conv=conv_int),
            _c('severe_count', '중증장애인수', conv=conv_int),
        ),
    ),
    Dataset(
        # 원천에 연도 칸이 없다 — 판의 기준일 연도를 연도로 쓴다(_20251231 판 → 2025).
        # 한 판이 한 해치라는 것은 unique(region) 로 확인한다(여러 해가 섞이면 시군구가 겹쳐 실패).
        table='emp_dis_regional_status', data_id=15080750, label='지역별 고용 현황',
        mode=MODE_YEAR, year_from=YEAR_FROM_EDITION,
        cols=(
            _c('region', '시군구명'),
            _c('company_count', '사업체수', conv=conv_int),
            _c('worker_count', '적용대상 근로자수', conv=conv_int),
            _c('obligation_count', '의무고용 인원', conv=conv_int),
            _c('severe_2x_count', '중증2배수 적용 장애인 고용인원', conv=conv_int),
            _c('severe_2x_rate', '중증2배수 적용 장애인 고용률', conv=conv_decimal),
        ),
        unique=('region',),
    ),
    Dataset(
        # 데이터셋 이름이 '고용개발원 교육정보' → '장애인고용 전문인력 양성교육정보' 로 바뀌었다(ID 는 같음).
        table='emp_dis_staff_train_crs', data_id=15004132, label='전문인력 양성교육', mode=MODE_EDITION,
        cols=(
            _c('course_type', '구분'),
            _c('course_name', '교육연수과정명'),
            _c('course_content', '교육내용'),
            _c('method', '교육방법'),
            _c('start_date', '교육시작일자', conv=conv_date),
            _c('end_date', '교육종료일자', conv=conv_date),
            _c('recruit_count', '모집인원', conv=conv_int),
            _c('apply_start_date', '수강신청 시작일자', conv=conv_date, null_ok=True),
            _c('apply_end_date', '수강신청 종료일자', conv=conv_date, null_ok=True),
            _c('apply_method', '수강신청방법', null_ok=True),
            _c('location', '교육장소명', null_ok=True),
            _c('target', '교육대상', null_ok=True),
        ),
    ),
    Dataset(
        table='emp_dis_dev_support_org', data_id=15072489, label='발달장애인 지원기관', mode=MODE_EDITION,
        cols=(
            _c('seq_no', '일련번호', '연번', conv=conv_int),
            _c('region', '시군구명', '시군구'),
            _c('org_name', '수행기관명', '기관명'),
            _c('day_activity', '주간활동', conv=conv_bool),
            _c('afterschool', '청소년방과후활동', conv=conv_bool),
            _c('indiv_plan', '개인별지원계획', conv=conv_bool),
            _c('parent_edu', '부모교육', conv=conv_bool),
            _c('family_rest', '가족휴식', conv=conv_bool),
            _c('parent_counsel', '부모상담', conv=conv_bool),
            _c('rights_relief', '권리구제', conv=conv_bool),
            _c('public_guardian', '공공후견', conv=conv_bool),
            _c('child_family_sup', '장애아가족양육지원', conv=conv_bool),
            _c('emergency_care', '긴급돌봄', conv=conv_bool),
            # 2025-09-30 판에 새로 생긴 칸(01 v1.11.0 에서 표 칸 추가). 옛 판에는 없으므로 필수 아님.
            _c('severe_integrated_care', '최중증 통합돌봄', conv=conv_bool, required=False, null_ok=True),
        ),
    ),
    Dataset(
        # 누적 데이터(2018~) — 판에 실린 연도만 바꾸고 나머지 연도는 그대로 둔다.
        table='emp_dis_startup_lecture', data_id=15035266, label='창업넷 일반강좌',
        mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN,
        cols=(
            _c('year', '교육년도', conv=conv_int),
            _c('online_type', '온오프라인'),
            _c('category', '교육구분'),
            _c('title', '교육제목'),
            _c('start_date', '교육시작일', conv=conv_date),
            _c('end_date', '교육종료일', conv=conv_date),
            # '12.0' 이 대부분이고 '8.4' 같은 소수가 드물다(2025-12-31 판 328행 중 2행) — 반올림, 건수 기록.
            _c('hours', '교육시간', conv=conv_int_round),
            _c('recruit_count', '모집인원', conv=conv_int),
            _c('apply_start_date', '신청시작일', conv=conv_date),
            _c('apply_end_date', '신청종료일', conv=conv_date),
            _c('org_name', '교육기관'),
            _c('apply_count', '신청인원', conv=conv_int),
            _c('complete_count', '수료인원', conv=conv_int),
        ),
    ),
    Dataset(
        table='emp_dis_std_workplace', data_id=3033670, label='표준사업장', mode=MODE_EDITION,
        cols=(
            _c('cert_no', '인증번호'),
            _c('company_name', '사업체명'),
            _c('branch', '관할지사'),
            _c('ceo', '대표자'),
            _c('business_no', '사업자등록번호'),
            _c('address', '소재지'),
            _c('cert_date', '인증일자', conv=conv_date),
            # 2025-12-31 판에 전화번호 빈 행이 있다(877행 중 98행) — 01 v1.11.0 에서 NOT NULL 해제.
            _c('tel', '전화번호', null_ok=True),
            _c('business_item', '업종 및 주요생산품'),
            _c('type', '구분'),
        ),
    ),
    Dataset(
        table='emp_dis_emp_incentive', data_id=15121124, label='신규고용장려금', mode=MODE_EDITION,
        cols=(
            _c('region', '지역명', '시도구분'),
            _c('industry', '업종명', '업종'),
            _c('company_count', '사업체수', conv=conv_int),
            _c('amount', '지급액', conv=conv_int),
            _c('paid_person', '지급순인원', conv=conv_int),
            _c('paid_year_person', '지급연인원', conv=conv_int),
        ),
    ),
    Dataset(
        table='emp_dis_obligation_by_indust', data_id=15131080, label='산업별 의무고용',
        mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN,
        cols=(
            _c('year', '해당연도', '연도', conv=conv_int),
            _c('industry', '한국표준산업분류명', '산업구분'),
            _c('company_count', '사업체수', conv=conv_int),
            _c('worker_count', '적용대상근로자수', conv=conv_int),
            _c('obligation_count', '의무고용인원', conv=conv_int),
            _c('emp_rate', '장애인 고용률', conv=conv_decimal),
        ),
    ),
    Dataset(
        # 누적 데이터(2010~) — 연도별 1행.
        table='emp_dis_obligation_fulfillment', data_id=15028087, label='의무고용 이행 사업체',
        mode=MODE_YEAR, year_from=YEAR_FROM_COLUMN,
        cols=(
            _c('year', '연도', conv=conv_int),
            _c('company_count', '사업체수', conv=conv_int),
            _c('fulfilled_count', '이행사업체수', conv=conv_int),
        ),
        unique=('year',),
    ),
)

DATASET_BY_TABLE: Dict[str, Dataset] = {d.table: d for d in DATASETS}


# ---------------------------------------------------------------------------
# 판(edition) 고르기
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Edition:
    """판 하나. path 는 '/{데이터ID}/v1/uddi:...' (sys_stats_src_api_info.api_data_url 에 기록하는 값)."""
    path: str
    summary: str
    base_date: Optional[datetime.date]   # summary 끝 _YYYYMMDD. 읽을 수 없으면 None
    fields: Tuple[str, ...]              # 명세가 밝힌 칸 이름(순서 유지)

    @property
    def uddi(self) -> str:
        return self.path.rsplit('/', 1)[-1]

    @property
    def base_date_str(self) -> Optional[str]:
        return self.base_date.strftime('%Y-%m-%d') if self.base_date else None


_EDITION_DATE_RE = re.compile(r'_(\d{8})\s*$')


def parse_edition_date(summary: str) -> Optional[datetime.date]:
    """'..._20251231' → date(2025, 12, 31). 형식이 다르거나 달력에 없는 날짜면 None."""
    m = _EDITION_DATE_RE.search(str(summary or ''))
    if not m:
        return None
    try:
        return datetime.datetime.strptime(m.group(1), '%Y%m%d').date()
    except ValueError:
        return None


def editions_from_spec(spec: dict) -> List[Edition]:
    """명세 JSON → 판 목록. 판마다 칸 이름은 definitions['<uddi>_model'].properties 에서 읽는다."""
    if not isinstance(spec, dict) or not isinstance(spec.get('paths'), dict):
        raise DatasetError('명세 응답에 paths 가 없음')
    defs = spec.get('definitions') or {}
    out = []
    for path, item in spec['paths'].items():
        summary = ((item or {}).get('get') or {}).get('summary') or ''
        uddi = str(path).rsplit('/', 1)[-1]
        props = ((defs.get(uddi + '_model') or {}).get('properties')) or {}
        out.append(Edition(path=str(path), summary=str(summary),
                           base_date=parse_edition_date(summary), fields=tuple(props.keys())))
    return out


def pick_edition(editions: Sequence[Edition], pinned_uddi: Optional[str] = None) -> Edition:
    """가장 늦은 기준일의 판. pinned_uddi 가 있으면 그 판(수동 보충 적재용).

    · 기준일을 읽을 수 없는 판은 건너뛴다(옛 판의 '08/13/2020', '(2018년)' 같은 표기).
    · 읽히는 판이 없으면 DatasetError.
    · 가장 늦은 기준일의 판이 둘 이상이면 DatasetError — 어느 쪽이 맞는 판인지 알 수 없다.
      (운영자가 확인 뒤 ODCLOUD_EMP_EDITION 으로 지정해 적재할 수 있다.)
    """
    if pinned_uddi:
        want = pinned_uddi if pinned_uddi.startswith('uddi:') else 'uddi:' + pinned_uddi
        for e in editions:
            if e.uddi == want:
                if e.base_date is None:
                    raise DatasetError('지정한 판 %s 의 기준일을 읽을 수 없음(%s)' % (want, e.summary))
                return e
        raise DatasetError('지정한 판 %s 이 명세에 없음' % want)
    dated = [e for e in editions if e.base_date is not None]
    if not dated:
        raise DatasetError('기준일(_YYYYMMDD)을 읽을 수 있는 판이 없음 — 판 %d개' % len(editions))
    latest_date = max(e.base_date for e in dated)
    latest = [e for e in dated if e.base_date == latest_date]
    if len(latest) > 1:
        raise DatasetError('기준일 %s 인 판이 %d개 — 어느 판인지 정할 수 없음(%s)'
                           % (latest_date, len(latest), ', '.join(e.uddi for e in latest)))
    return latest[0]


# ---------------------------------------------------------------------------
# 칸 검사 + 행 변환
# ---------------------------------------------------------------------------
def resolve_columns(ds: Dataset, fields: Sequence[str]) -> Dict[int, Optional[str]]:
    """판의 칸 이름 목록 → {대응표 순번: 원천 칸 이름 | None}. 규칙 위반이면 DatasetError.

    fields 는 명세의 칸 목록과 실제 응답 행에 나온 키의 합집합을 넘긴다(명세에 없는 칸이 응답에만
    나오는 경우까지 잡기 위해서다).
    """
    names = [str(f) for f in fields]
    present = set(names)
    out: Dict[int, Optional[str]] = {}
    problems = []
    for i, col in enumerate(ds.cols):
        hits = [a for a in col.aliases if a in present]
        if len(hits) > 1:
            problems.append('같은 칸으로 알려진 이름이 둘 이상(%s)' % '·'.join(hits))
        elif not hits:
            if col.required:
                problems.append("필수 칸 없음: '%s'" % col.aliases[0])
            out[i] = None
        else:
            out[i] = hits[0]
    known = {a for col in ds.cols for a in col.aliases} | set(ds.ignore)
    unknown = [n for n in names if n not in known]
    if unknown:
        problems.append('대응표에 없는 칸: %s — 표에 칸을 추가하거나 대응표·무시 목록을 고친 뒤 적재'
                        % ', '.join("'%s'" % u for u in unknown))
    if problems:
        raise DatasetError('칸 검사 실패: ' + ' / '.join(problems))
    return out


def build_rows(ds: Dataset, edition: Edition, raw_rows: Sequence[dict], max_samples: int = 3
               ) -> Tuple[List[dict], dict]:
    """원천 행 → 표 행. 반환 (rows, notes).

    notes = {'nulled': {표 칸: 건수}, 'rounded': {표 칸: 건수}, 'source_columns': {표 칸: 원천 칸}}
        nulled  — NULL 허용 칸에서 변환이 안 돼 NULL 로 둔 값의 건수(값 예시는 로그에 남긴다).
        rounded — conv_int_round 칸에서 소수를 반올림한 건수.
    NOT NULL 칸의 빈 값·변환 오류(strict 칸은 NULL 허용이어도 변환 오류), unique 위반, 연도 누락은
    DatasetError(오류 예시 max_samples 건 포함).
    """
    fields = list(edition.fields)
    for r in raw_rows:                    # 명세에 없는 키가 응답에만 나오는 경우까지 검사한다
        for k in r.keys():
            if k not in fields:
                fields.append(k)
    colmap = resolve_columns(ds, fields)

    rows: List[dict] = []
    errors: Dict[str, List[str]] = {}
    error_counts: Dict[str, int] = {}
    nulled: Dict[str, int] = {}
    nulled_samples: Dict[str, List[str]] = {}
    rounded: Dict[str, int] = {}

    def _err(target, src, value, why):
        error_counts[target] = error_counts.get(target, 0) + 1
        bucket = errors.setdefault(target, [])
        if len(bucket) < max_samples:
            bucket.append("'%s'=%r(%s)" % (src, value, why))

    edition_year = edition.base_date.year if edition.base_date else None
    for raw in raw_rows:
        row = {}
        for i, col in enumerate(ds.cols):
            src = colmap.get(i)
            value = raw.get(src) if src else None
            try:
                converted = col.conv(value)
            except ValueError as exc:
                converted = None
                if not col.null_ok or col.strict:
                    _err('·'.join(col.targets), src, value, exc)
                    for t in col.targets:
                        row[t] = None
                    continue
                key = '·'.join(col.targets)
                nulled[key] = nulled.get(key, 0) + 1
                if len(nulled_samples.setdefault(key, [])) < max_samples:
                    nulled_samples[key].append(repr(value))
            if converted is None and not col.null_ok:
                _err('·'.join(col.targets), src, value, '빈 값')
            if col.conv is conv_int_round and converted is not None and conv_decimal(value) != converted:
                rounded[col.target] = rounded.get(col.target, 0) + 1
            if len(col.targets) > 1:
                parts = converted if converted is not None else (None,) * len(col.targets)
                for t, v in zip(col.targets, parts):
                    row[t] = v
            else:
                row[col.target] = converted
        if ds.mode == MODE_YEAR and ds.year_from == YEAR_FROM_EDITION:
            row['year'] = edition_year
        # 이 행을 넣은 판의 기준일 — 판 단위 교체(MODE_EDITION)의 기준이고, 연도 표에서도 연도별 출처가 된다.
        row['src_base_dt'] = edition.base_date
        rows.append(row)

    if errors:
        detail = ' / '.join('%s %d건 예: %s' % (t, error_counts[t], ', '.join(s)) for t, s in errors.items())
        raise DatasetError('값 변환 실패(NOT NULL 칸): ' + detail)
    if ds.mode == MODE_YEAR:
        if any(r.get('year') is None for r in rows):
            raise DatasetError('연도를 정할 수 없는 행이 있음')
    if ds.unique:
        seen, dups = set(), []
        for r in rows:
            key = tuple(r.get(k) for k in ds.unique)
            if key in seen and len(dups) < max_samples:
                dups.append(key)
            seen.add(key)
        if dups:
            raise DatasetError('판 안에서 %s 이(가) 겹침 — 예: %s' % ('+'.join(ds.unique), dups))
    for key, n in nulled.items():
        logger.warning('%s:%s %s 칸 변환 불가 %d건을 NULL 로 둠(예: %s)',
                       EXT_SYS, ds.table, key, n, ', '.join(nulled_samples.get(key, [])))
    for key, n in rounded.items():
        logger.warning('%s:%s %s 칸 소수 값 %d건을 반올림해 정수로 둠', EXT_SYS, ds.table, key, n)
    notes = {
        'nulled': nulled,
        'rounded': rounded,
        'source_columns': {'·'.join(c.targets): colmap.get(i) for i, c in enumerate(ds.cols)},
    }
    return rows, notes


# ---------------------------------------------------------------------------
# 수집기 — 네트워크(명세·데이터)만 맡는다. DB 는 db_odcloud_emp, 흐름은 odcloud_emp_pipeline.
# ---------------------------------------------------------------------------
class OdcloudEmpCollector(MobilityCollector):
    EXT_SYS = EXT_SYS
    DEFAULT_BASE_URL = DEFAULT_BASE_URL

    @property
    def page_size(self) -> int:
        try:
            size = int(os.getenv('ODCLOUD_EMP_PAGE_SIZE', str(DEFAULT_PAGE_SIZE)))
        except ValueError:
            size = DEFAULT_PAGE_SIZE
        return max(1, min(size, MAX_PAGE_SIZE))

    @property
    def max_pages(self) -> int:
        """무한 반복 방어 상한. 가장 큰 데이터셋도 1,000 행씩 100쪽이면 10만 행이다."""
        try:
            return int(os.getenv('ODCLOUD_EMP_MAX_PAGES', '200'))
        except ValueError:
            return 200

    def _service_key(self) -> str:
        """serviceKey 쿼리 값. 포털의 '디코딩' 키(특수문자 그대로)는 URL 인코딩하고,
        이미 인코딩된 '인코딩' 키(% 포함)는 그대로 쓴다(이중 인코딩 방지)."""
        key = self.api_key
        return key if '%' in key else urllib.parse.quote(key, safe='')

    def fetch_spec(self, ds: Dataset, meta_url: Optional[str] = None) -> dict:
        """명세(JSON). meta_url 은 sys_stats_src_api_info.api_meta_url(비어 있으면 기본 형식)."""
        url = (meta_url or '').strip() or SPEC_URL_FMT.format(data_id=ds.data_id)
        payload = self.get_json(url)
        if not isinstance(payload, dict):
            raise DatasetError('명세 응답이 JSON 객체가 아님')
        return payload

    def _page_url(self, edition: Edition, page: int) -> str:
        return (self.base_url + edition.path
                + '?page=' + str(page) + '&perPage=' + str(self.page_size)
                + '&serviceKey=' + self._service_key())

    def fetch_rows(self, ds: Dataset, edition: Edition) -> List[dict]:
        """판의 모든 행을 쪽 단위로 받는다. 받은 행 수가 totalCount 와 다르면 DatasetError.

        odcloud 는 인증 실패·미승인도 HTTP 401·403 등으로 돌려준다 — http_get 이 재시도 뒤 RuntimeError
        (메시지에 인증키 없음)를 올리고, 파이프라인이 그 데이터셋만 실패로 기록한다.
        """
        rows: List[dict] = []
        total = None
        page = 1
        while page <= self.max_pages:
            payload = self.get_json(self._page_url(edition, page))
            if not isinstance(payload, dict) or 'data' not in payload:
                keys = list(payload) if isinstance(payload, dict) else type(payload).__name__
                raise DatasetError('데이터 응답 형식이 다름 — 키: %s' % (keys,))
            if total is None:
                total = payload.get('totalCount')
                if not isinstance(total, int) or total < 0:
                    raise DatasetError('데이터 응답에 totalCount 가 없음')
            part = payload.get('data') or []
            if not isinstance(part, list):
                raise DatasetError('데이터 응답의 data 가 목록이 아님')
            rows.extend(r for r in part if isinstance(r, dict))
            if not part or len(rows) >= total:
                break
            page += 1
            self.pause()
        else:
            raise DatasetError('쪽 상한 %d 에 도달 — 수집이 잘렸을 수 있음(받은 %d / 전체 %s)'
                               % (self.max_pages, len(rows), total))
        if total == 0 or not rows:
            # 판이 통째로 비었다는 응답은 원천 장애일 가능성이 크다. 기존 데이터를 지킨다.
            raise DatasetError('원천 0건 — 기존 데이터를 유지함')
        if len(rows) != total:
            raise DatasetError('받은 행 %d 이 응답 전체 건수 %d 와 다름' % (len(rows), total))
        logger.info('%s:%s 판 %s(%s) %d행 수집', EXT_SYS, ds.table, edition.uddi[5:13],
                    edition.base_date_str, len(rows))
        return rows

    def collect(self) -> List[dict]:
        """MobilityCollector 계약용 — 이 소스는 odcloud_emp_pipeline.run_odcloud_emp 가 흐름을 맡는다."""
        raise NotImplementedError('ODCLOUD_EMP 는 odcloud_emp_pipeline.run_odcloud_emp 로 실행한다')
