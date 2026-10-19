"""경기데이터드림 장애인 문화·체육 시설 4종 수집 어댑터 — 적재: poi_culture_sports_facility.

원천(모두 https://openapi.gg.go.kr/<서비스명>, 호출 제한 없음, 이용허락 제한 없음, 원천 갱신 연 1회):
    braille_lib    점자도서관 현황             Ggbrlllibrry          (2026-10-14 실측   5건)
    booknarae_lib  책나래서비스 제공도서관 현황 Gglibrryprovsnservc   (            260건)
    sports_fac     장애인체육시설 현황          Ggdspsnphstrnfaclt    (              5건)
    sports_org     시군별 장애인체육회 현황      Ggsigundspsnphstrn    (             32건)
수집 범위: 경기도 전체(시군 필터 없음). 같은 성격의 전국 위치 목록은 공개 원천이 없다.

공통 응답 필드: SIGUN_CD, SIGUN_NM, INST_NM, TELNO, HMPG_URL, REFINE_LOTNO_ADDR,
REFINE_ROADNM_ADDR, REFINE_ZIP_CD, REFINE_WGS84_LAT, REFINE_WGS84_LOGT
종류별 추가: INSTL_STATMNT_DE(설치신고일 — 점자도서관·체육시설), COPRTN_NM_INFO(법인명 — 점자도서관)

종류별 격리
    네 서비스는 서로 독립된 엔드포인트다. 한 서비스가 실패(ERROR-*, 네트워크, 총건수 불일치,
    0건)해도 나머지는 그대로 수집해 반환한다. 실패한 종류는 반환 행에 들어가지 않으므로 적재기가
    그 종류의 기존 행을 건드리지 않는다. 실패 내역은 self.failures 에 남아 실행 기록·이상
    알림(부분 완료)으로 이어진다. 네 종류가 모두 실패하면 예외를 올려 실행 전체를 실패로 둔다.

값 정제(원천 오류가 적재 실패로 번지지 않게)
    · 설치신고일: YYYYMMDD 이면서 실제 달력 날짜일 때만 'YYYY-MM-DD', 아니면 None
    · 좌표: 국내 범위(위도 33~39, 경도 124~132) 밖이거나 한쪽만 있으면 둘 다 None
    · 같은 종류 안에서 (이름, 주소) 가 같은 행이 둘 이상이면 첫 행만 쓴다(활성 행 유일성 보장)
"""
from __future__ import annotations

import datetime
import logging
import os
import re
from typing import Dict, List, Optional

from collectors.gg_openapi import parse_gg_response
from collectors.mobility_base import MobilityCollector, to_float

logger = logging.getLogger(__name__)

EXT_SYS = 'GG_CULTURE'
GYEONGGI_INTERNAL_SIDO = '9410000'  # sys_common_code sido_code (내부용, prefix 9)
MAX_PAGE_SIZE = 1000

# (종류 코드, 서비스명, 화면·메일용 이름). 순서가 수집 순서다.
SERVICES = (
    ('braille_lib', 'Ggbrlllibrry', '점자도서관'),
    ('booknarae_lib', 'Gglibrryprovsnservc', '책나래서비스 제공도서관'),
    ('sports_fac', 'Ggdspsnphstrnfaclt', '장애인체육시설'),
    ('sports_org', 'Ggsigundspsnphstrn', '시군별 장애인체육회'),
)
FACILITY_TYPES = tuple(s[0] for s in SERVICES)
TYPE_LABELS: Dict[str, str] = {s[0]: s[2] for s in SERVICES}

# 국내 좌표 범위(제주·울릉·독도 포함 여유). 경기도 원천이지만 범위를 경기로 좁히지 않는다 —
# 원천 오기로 경계 밖 좌표가 오면 위치를 틀리게 안내하는 것보다 비워 두는 편이 낫지만,
# 경기 경계 바로 바깥의 정상 시설(서울 소재 법인 사무국 등)을 지우지 않기 위해서다.
LAT_RANGE = (33.0, 39.0)
LON_RANGE = (124.0, 132.0)


def _clean(value) -> Optional[str]:
    """앞뒤 공백 제거, 빈 문자열은 None."""
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def parse_report_date(value) -> Optional[str]:
    """설치신고일 'YYYYMMDD' → 'YYYY-MM-DD'. 형식이 다르거나 달력에 없는 날짜면 None.

    '20150000'·'99999999'·'미상' 같은 값이 date 칸에 그대로 들어가면 적재 SQL 이 실패하므로
    여기서 걸러 낸다(해당 행의 다른 정보는 그대로 적재된다).
    """
    token = _clean(value)
    if token is None:
        return None
    token = token.replace('-', '').replace('.', '')
    if not re.fullmatch(r'\d{8}', token):
        return None
    try:
        return datetime.datetime.strptime(token, '%Y%m%d').strftime('%Y-%m-%d')
    except ValueError:
        return None


def clean_coords(lat, lon):
    """(위도, 경도) — 숫자가 아니거나 국내 범위 밖이면 (None, None). 한쪽만 있어도 (None, None)."""
    lat_f, lon_f = to_float(lat), to_float(lon)
    if lat_f is None or lon_f is None:
        return None, None
    if not (LAT_RANGE[0] <= lat_f <= LAT_RANGE[1] and LON_RANGE[0] <= lon_f <= LON_RANGE[1]):
        return None, None
    return lat_f, lon_f


def map_row(facility_type: str, src: dict) -> dict:
    """원천 1행 → poi_culture_sports_facility 컬럼 dict."""
    lat, lon = clean_coords(src.get('REFINE_WGS84_LAT'), src.get('REFINE_WGS84_LOGT'))
    return {
        'facility_type': facility_type,
        'sido_code': GYEONGGI_INTERNAL_SIDO,
        'sigun_cd': _clean(src.get('SIGUN_CD')),
        'sigun_nm': _clean(src.get('SIGUN_NM')),
        'name': _clean(src.get('INST_NM')) or '',
        'addr_road': _clean(src.get('REFINE_ROADNM_ADDR')),
        'addr_jibun': _clean(src.get('REFINE_LOTNO_ADDR')),
        'zip_code': _clean(src.get('REFINE_ZIP_CD')),
        'latitude': lat,
        'longitude': lon,
        'tel': _clean(src.get('TELNO')),
        'homepage': _clean(src.get('HMPG_URL')),
        'install_report_dt': parse_report_date(src.get('INSTL_STATMNT_DE')),
        'corp_name': _clean(src.get('COPRTN_NM_INFO')),
        'source': EXT_SYS,
    }


def raw_key(row: dict) -> tuple:
    """DB 부분 유니크 인덱스와 같은 키 — (종류, 이름, 도로명→없으면 지번)."""
    return (row['facility_type'], row['name'], row.get('addr_road') or row.get('addr_jibun') or '')


class GgCultureCollector(MobilityCollector):
    """경기데이터드림 여러 서비스(종류) 수집기. 장애인복지시설(GG_WELFARE)도 이 클래스를 이어받아
    서비스 목록(SERVICES)·행 변환(map_source_row)·환경변수 머리(ENV_PREFIX)만 바꿔 쓴다."""
    EXT_SYS = EXT_SYS
    ENV_PREFIX = 'GG_CULTURE'
    SERVICES = SERVICES
    DEFAULT_BASE_URL = 'https://openapi.gg.go.kr'

    def __init__(self, api_info: dict = None, stats_src: dict = None):
        super().__init__(api_info, stats_src)
        # 종류별 실패 — mobility_pipeline 이 실행 요약으로 옮겨 실행 기록·알림 실패 목록이 된다.
        #   [{'id': 'GG_CULTURE:braille_lib', 'stage': 'collect', 'reason': '...'}]
        self.failures: List[dict] = []
        # 종류별 수집 결과 {'braille_lib': 5, ...} — 실패 종류는 들어가지 않는다.
        self.collected: Dict[str, int] = {}

    @property
    def base_url(self) -> str:
        """서비스 공통 주소. 예전 형식처럼 서비스명까지 붙은 주소가 와도 공통 부분만 쓴다."""
        url = super().base_url
        match = re.match(r'(https?://[^/]+)', url)
        return match.group(1) if match else url

    @property
    def page_size(self) -> int:
        return min(int(os.getenv(self.ENV_PREFIX + '_PAGE_SIZE', str(MAX_PAGE_SIZE))), MAX_PAGE_SIZE)

    @property
    def max_pages(self) -> int:
        """무한 루프 방어 상한. 가장 큰 종류(책나래 260건)도 1페이지면 끝난다."""
        return int(os.getenv(self.ENV_PREFIX + '_MAX_PAGES', '20'))

    def _page_url(self, service: str, page_no: int) -> str:
        return (self.base_url + '/' + service
                + '?KEY=' + self.api_key
                + '&Type=json'
                + '&pIndex=' + str(page_no)
                + '&pSize=' + str(self.page_size))

    @staticmethod
    def map_source_row(facility_type: str, src: dict) -> dict:
        """원천 1행 → 적재 행. 하위 클래스가 바꾼다."""
        return map_row(facility_type, src)

    def collect_service(self, facility_type: str, service: str) -> List[dict]:
        """서비스 하나를 끝까지 페이징 수집. 총건수와 받은 행 수가 다르면 RuntimeError."""
        label = '%s:%s' % (self.EXT_SYS, facility_type)
        raw: List[dict] = []
        total = None
        page = 1
        while page <= self.max_pages:
            page_total, page_rows = parse_gg_response(self.get_json(self._page_url(service, page)),
                                                      service, label)
            if total is None:
                total = page_total
            if not page_rows:
                break
            raw.extend(page_rows)
            if len(page_rows) < self.page_size or (total and len(raw) >= total):
                break
            page += 1
            self.pause()
        else:
            raise RuntimeError('%s: 페이지 상한 %d 도달 — 수집이 잘렸을 수 있음' % (label, self.max_pages))

        if not raw:
            # 원천이 비었다는 응답(INFO-200)도 이 데이터에서는 비정상이다(연 1회 갱신 목록이
            # 통째로 사라지는 경우는 원천 장애 가능성이 크다). 기존 행을 지키고 알린다.
            raise RuntimeError('%s: 원천 0건 — 기존 행을 유지함' % label)
        if total and len(raw) != total:
            raise RuntimeError('%s: 응답 총건수 %d 와 받은 행 %d 이 다름' % (label, total, len(raw)))

        rows: List[dict] = []
        seen = set()
        dropped_name = dup = bad_coord = bad_date = 0
        for src in raw:
            row = self.map_source_row(facility_type, src)
            if not row['name']:
                dropped_name += 1
                continue
            key = raw_key(row)
            if key in seen:
                dup += 1
                continue
            seen.add(key)
            if row['latitude'] is None and (_clean(src.get('REFINE_WGS84_LAT')) or _clean(src.get('REFINE_WGS84_LOGT'))):
                bad_coord += 1
            if row['install_report_dt'] is None and _clean(src.get('INSTL_STATMNT_DE')):
                bad_date += 1
            rows.append(row)
        if bad_coord or bad_date:
            logger.warning('%s: 정제 — 범위 밖·형식 오류 좌표 %d건, 설치신고일 형식 오류 %d건을 빈 값으로 둠',
                           label, bad_coord, bad_date)
        if dropped_name or dup:
            logger.warning('%s: 이름 없는 행 %d건, (이름·주소) 중복 행 %d건 제외', label, dropped_name, dup)
        logger.info('%s(%s): 원천 %d건 → 적재 대상 %d건', label, service, total or len(raw), len(rows))
        return rows

    def collect(self) -> List[dict]:
        """종류(서비스)를 차례로 수집. 실패한 종류는 건너뛰고 self.failures 에 남긴다."""
        self.failures = []
        self.collected = {}
        rows: List[dict] = []
        for facility_type, service, title in self.SERVICES:
            try:
                part = self.collect_service(facility_type, service)
            except Exception as exc:   # 종류별 격리 — 다른 종류 수집은 계속한다
                reason = '%s 수집 실패: %s' % (title, exc)
                logger.error('%s:%s %s', self.EXT_SYS, facility_type, reason)
                self.failures.append({'id': '%s:%s' % (self.EXT_SYS, facility_type),
                                      'stage': 'collect', 'reason': reason[:300]})
                continue
            self.collected[facility_type] = len(part)
            rows.extend(part)
            self.pause()
        if not self.collected:
            raise RuntimeError('%s: %d종 모두 수집 실패 — ' % (self.EXT_SYS, len(self.SERVICES)) +
                               ' / '.join(f['reason'] for f in self.failures))
        logger.info('%s: 종류별 수집 %s, 실패 %d종', self.EXT_SYS, self.collected, len(self.failures))
        return rows
