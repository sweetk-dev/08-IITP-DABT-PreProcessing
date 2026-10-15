"""경기데이터드림 장애인복지시설 5종 수집 어댑터 — 적재: poi_welfare_facility (01 v1.14.0).

원천(모두 https://openapi.gg.go.kr/<서비스명>, 인증키는 GG_CULTURE 와 같은 경기데이터드림 키,
제공 경기도장애인복지종합지원센터(누림센터), 원천 갱신 연 1회):
    residence        장애유형별 거주시설 현황          Ggobstcltyperesidnc     (2026-10-15 실측  88건)
    short_residence  장애인 단기거주시설 현황          Ggdspsnshtpedresidnc    (              29건)
    protect_workshop 장애인보호작업장 현황              Ggdspsnprotectwrkshp    (             158건)
    il_center        중증장애인자립생활지원센터 현황    Ggsersillnsdspsncenter  (              61건)
    support_center   장애인 관련 시설 현황              Ggdspsnrelatefaclt      (              28건)
                     (가족지원센터·인권센터·발달장애인지원센터·장애인권익옹호기관 — 구분은 비고 RM)
수집 범위: 경기도 전체(시군 필터 없음).

칸 대응
    공통      SIGUN_CD, SIGUN_NM, INST_NM, REFINE_ROADNM_ADDR, REFINE_LOTNO_ADDR, REFINE_ZIP_CD,
              REFINE_WGS84_LAT/LOGT, HMPG_URL, TELNO(관련 시설은 TELNO_INFO)
    구분      sub_type ← OBSTCL_TYPE_NM(거주시설 장애유형) 또는 RM(관련 시설 기관 구분)
    정원·현원 capacity/current ← ENTRNC_PSN_CAPA_CNT/ENTRNC_PSTPSN_CNT(입소)
                                 또는 LABOR_DSPSN_PSN_CAPA_CNT/LABOR_DSPSN_PSTPSN_CNT(보호작업장 근로장애인)
              staff_capacity/staff_current ← ENFLPSN_PSN_CAPA_CNT/ENFLPSN_PSTPSN_CNT(종사자)
    설치신고일 INSTL_STATMNT_DE, 법인명 COPRTN_NM_INFO 또는 COPRTN_NM

수집·정제·종류별 격리 규칙은 GG_CULTURE(collectors/gg_culture.py)와 같다. 같은 종류 안에서
(이름, 주소)가 같은 행은 첫 행만 쓴다 — 2026-10-15 원천에 거주시설 1곳이 같은 이름·주소로 두 번
(현원만 다름) 실려 있다(운영자 확인 대상으로 경고 로그를 남긴다).
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

from collectors.gg_culture import (
    GYEONGGI_INTERNAL_SIDO, GgCultureCollector, _clean, clean_coords, parse_report_date,
)
from collectors.mobility_base import to_float

logger = logging.getLogger(__name__)

EXT_SYS = 'GG_WELFARE'

SERVICES = (
    ('residence', 'Ggobstcltyperesidnc', '장애유형별 거주시설'),
    ('short_residence', 'Ggdspsnshtpedresidnc', '장애인 단기거주시설'),
    ('protect_workshop', 'Ggdspsnprotectwrkshp', '장애인보호작업장'),
    ('il_center', 'Ggsersillnsdspsncenter', '중증장애인자립생활지원센터'),
    ('support_center', 'Ggdspsnrelatefaclt', '장애인 관련 시설'),
)
FACILITY_TYPES = tuple(s[0] for s in SERVICES)
TYPE_LABELS: Dict[str, str] = {s[0]: s[2] for s in SERVICES}


def to_count(value) -> Optional[int]:
    """인원 수 — 정수. 원천은 숫자·숫자 문자열·'12.0'·'1,234' 로 온다. 음수·소수·숫자 아님은 None."""
    if isinstance(value, str):
        value = value.replace(',', '').strip()
    f = to_float(value)
    if f is None or f < 0 or f != int(f):
        return None
    return int(f)


def _first(src: dict, *keys):
    for key in keys:
        value = _clean(src.get(key))
        if value is not None:
            return value
    return None


def _sub_type(facility_type: str, src: dict) -> Optional[str]:
    """세부 구분 — 거주시설은 장애유형(OBSTCL_TYPE_NM), 관련 시설은 기관 구분(비고 RM). 나머지 종류는 없음."""
    if facility_type == 'residence':
        return _clean(src.get('OBSTCL_TYPE_NM'))
    if facility_type == 'support_center':
        return _clean(src.get('RM'))
    return None


def map_row(facility_type: str, src: dict) -> dict:
    """원천 1행 → poi_welfare_facility 컬럼 dict."""
    lat, lon = clean_coords(src.get('REFINE_WGS84_LAT'), src.get('REFINE_WGS84_LOGT'))
    homepage = _clean(src.get('HMPG_URL'))
    if homepage and homepage.rstrip('/').lower() in ('www.', 'www', 'http://www.', 'https://www.', 'http:', 'https:'):
        homepage = None   # 원천의 빈 자리 표기('www.')는 주소가 아니다
    if facility_type == 'protect_workshop':
        capacity, current = src.get('LABOR_DSPSN_PSN_CAPA_CNT'), src.get('LABOR_DSPSN_PSTPSN_CNT')
    else:
        capacity, current = src.get('ENTRNC_PSN_CAPA_CNT'), src.get('ENTRNC_PSTPSN_CNT')
    return {
        'facility_type': facility_type,
        'sub_type': _sub_type(facility_type, src),
        'sido_code': GYEONGGI_INTERNAL_SIDO,
        'sigun_cd': _clean(src.get('SIGUN_CD')),
        'sigun_nm': _clean(src.get('SIGUN_NM')),
        'name': _clean(src.get('INST_NM')) or '',
        'addr_road': _clean(src.get('REFINE_ROADNM_ADDR')),
        'addr_jibun': _clean(src.get('REFINE_LOTNO_ADDR')),
        'zip_code': _clean(src.get('REFINE_ZIP_CD')),
        'latitude': lat,
        'longitude': lon,
        'tel': _first(src, 'TELNO', 'TELNO_INFO'),
        'homepage': homepage,
        'capacity_cnt': to_count(capacity),
        'current_cnt': to_count(current),
        'staff_capacity_cnt': to_count(src.get('ENFLPSN_PSN_CAPA_CNT')),
        'staff_current_cnt': to_count(src.get('ENFLPSN_PSTPSN_CNT')),
        'install_report_dt': parse_report_date(src.get('INSTL_STATMNT_DE')),
        'corp_name': _first(src, 'COPRTN_NM_INFO', 'COPRTN_NM'),
        'source': EXT_SYS,
    }


class GgWelfareCollector(GgCultureCollector):
    EXT_SYS = EXT_SYS
    ENV_PREFIX = 'GG_WELFARE'
    SERVICES = SERVICES

    @staticmethod
    def map_source_row(facility_type: str, src: dict) -> dict:
        return map_row(facility_type, src)
