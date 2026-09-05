"""경기데이터드림 공중화장실 현황(제공표준) 수집 어댑터 — 적재: poi_public_toilet_info.

원천: https://openapi.gg.go.kr/Publtolt (호출 제한 없음, 이용허락 제한 없음)
배경: 행안부 전국표준데이터가 2025년 2월부터 좌표 제공을 중단해 기존 적재분은
      좌표 보유율이 17%(안양 174건 중 29건)였다. 경기도 API 는 좌표를 그대로 준다
      (안양 243건 중 237건, 98%). 실측 2026-09-05.

- 페이징: pIndex/pSize(최대 1000). 데이터가 없으면 head 없이 RESULT.CODE=INFO-200
  응답이 온다 → 정상 종료. 그 외 ERROR-* 는 RuntimeError (조용한 실패 금지).
- 필터: 요청 인자 REFINE_ROADNM_ADDR 는 부분일치다. 응답도 다시 주소 접두어로
  걸러 인접 지역·'안양천로' 류의 오염을 막는다(LIKE '%안양%' 오류의 재발 방지).
- 컬럼 대응(2026-09-05, 기존 행안부 적재분과 이름 일치 168건 전수 대조 — 168/168 일치):
    EMBEL_INSTL_YN / EMBEL_INSTL_PLC → emg_bell_yn / emg_bell_location (비상벨)
    INSTL_YN / PLC_NM               → diaper_table_yn / diaper_table_location (기저귀교환대)
    TOILET_YN                       → cctv_yn (화장실 입구 CCTV)
    INSTL_YM / RE_BUILD_YM (YYYYMM) → install_dt / remodeled_dt (YYYY-MM)
  원천에 없는 컬럼: basis, open_time_detail, safety_target_yn → None.
  MALE_FEMALE_CMNUSE_TOILET_YN(남녀공용) → unisex_yn (01 v1.4.0 에서 추가).
- OPEN_TM_INFO 는 '상시'/'정시' 두 값뿐이다. 개방시간 상세(예: 09:00~18:00)는 이 원천에
  없으므로 적재 시 이름+주소가 같은 기존 행의 open_time_detail 을 유지한다(db_mobility 참조).
"""
from __future__ import annotations

import logging
import os
import re
from typing import List, Optional
from urllib.parse import quote

from collectors.mobility_base import MobilityCollector, to_float, to_int, to_yn

logger = logging.getLogger(__name__)

GYEONGGI_INTERNAL_SIDO = '9410000'  # sys_common_code sido_code (내부용, prefix 9)
DEFAULT_ADDR_FILTER = '경기도 안양시'
MAX_PAGE_SIZE = 1000

# 응답에 데이터가 없을 때의 코드(에러가 아니다).
NO_DATA_CODE = 'INFO-200'
OK_CODE = 'INFO-000'


def yyyymm_to_dash(value: Optional[str]) -> Optional[str]:
    """'202503' → '2025-03'. 이미 '2025-03' 이면 그대로. 형식이 다르면 None."""
    if value is None:
        return None
    token = str(value).strip()
    if re.fullmatch(r'\d{6}', token):
        return token[:4] + '-' + token[4:]
    if re.fullmatch(r'\d{4}-\d{2}', token):
        return token
    return None


def yyyymmdd_to_date(value: Optional[str]) -> Optional[str]:
    """'20250717' → '2025-07-17'. 형식이 다르면 None."""
    if value is None:
        return None
    token = str(value).strip()
    if re.fullmatch(r'\d{8}', token):
        return token[:4] + '-' + token[4:6] + '-' + token[6:]
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', token):
        return token
    return None


def _clean(value) -> Optional[str]:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def parse_response(payload: dict) -> tuple:
    """응답 JSON → (total_count, rows). 데이터 없음(INFO-200)은 (0, []).

    정상 응답: {"Publtolt": [{"head": [{"list_total_count": N}, {"RESULT": {...}}, ...]},
                             {"row": [...]}]}
    오류·데이터 없음: {"RESULT": {"CODE": "...", "MESSAGE": "..."}}
    """
    if not isinstance(payload, dict):
        raise RuntimeError('GG_TOILET API: 응답이 JSON 객체가 아님')
    top_result = payload.get('RESULT')
    if top_result is not None:
        code = str(top_result.get('CODE', '')).strip()
        if code == NO_DATA_CODE:
            return 0, []
        raise RuntimeError('GG_TOILET API error code=%s message=%s'
                           % (code, top_result.get('MESSAGE')))
    body = payload.get('Publtolt')
    if not isinstance(body, list) or not body:
        raise RuntimeError('GG_TOILET API: Publtolt 본문이 없음 — 응답 키: %s' % list(payload))
    total = 0
    rows: List[dict] = []
    for part in body:
        if not isinstance(part, dict):
            continue
        for head in part.get('head', []) or []:
            if 'list_total_count' in head:
                total = to_int(head['list_total_count']) or 0
            result = head.get('RESULT')
            if result and str(result.get('CODE', '')).strip() not in ('', OK_CODE):
                raise RuntimeError('GG_TOILET API error code=%s message=%s'
                                   % (result.get('CODE'), result.get('MESSAGE')))
        if 'row' in part:
            rows = list(part['row'] or [])
    return total, rows


def map_row(src: dict) -> dict:
    """원천 1행 → poi_public_toilet_info 컬럼 dict."""
    return {
        'sido_code': GYEONGGI_INTERNAL_SIDO,
        'toilet_name': _clean(src.get('PBCTLT_PLC_NM')) or '',
        'toilet_type': _clean(src.get('PUBLFACLT_DIV_NM')) or '',
        'basis': None,
        'addr_road': _clean(src.get('REFINE_ROADNM_ADDR')),
        'addr_jibun': _clean(src.get('REFINE_LOTNO_ADDR')),
        'm_toilet_count': to_int(src.get('MALE_WTRCLS_CNT')),
        'm_urinal_count': to_int(src.get('MALE_UIL_CNT')),
        'm_dis_toilet_count': to_int(src.get('MALE_DSPSN_WTRCLS_CNT')),
        'm_dis_urinal_count': to_int(src.get('MALE_DSPSN_UIL_CNT')),
        'm_child_toilet_count': to_int(src.get('MALE_KID_WTRCLS_CNT')),
        'm_child_urinal_count': to_int(src.get('MALE_KID_UIL_CNT')),
        'f_toilet_count': to_int(src.get('FEMALE_WTRCLS_CNT')),
        'f_dis_toilet_count': to_int(src.get('FEMALE_DSPSN_WTRCLS_CNT')),
        'f_child_toilet_count': to_int(src.get('FEMALE_KID_WTRCLS_CNT')),
        'managing_org': _clean(src.get('MANAGE_INST_NM')),
        'phone_number': _clean(src.get('MNGINST_TELNO')),
        'open_time': _clean(src.get('OPEN_TM_INFO')),
        'open_time_detail': None,          # 원천에 없음 — 적재 시 매칭된 기존 행 값 유지
        'install_dt': yyyymm_to_dash(_clean(src.get('INSTL_YM'))),
        'latitude': to_float(src.get('REFINE_WGS84_LAT')),
        'longitude': to_float(src.get('REFINE_WGS84_LOGT')),
        'owner_type': _clean(src.get('TOILET_POSESN_DIV')),
        'waste_process_type': _clean(src.get('PROC_CONT')),
        'safety_target_yn': None,          # 원천에 없음
        'emg_bell_yn': to_yn(src.get('EMBEL_INSTL_YN')),
        'emg_bell_location': _clean(src.get('EMBEL_INSTL_PLC')),
        'cctv_yn': to_yn(src.get('TOILET_YN')),
        'diaper_table_yn': to_yn(src.get('INSTL_YN')),
        'diaper_table_location': _clean(src.get('PLC_NM')),
        'remodeled_dt': yyyymm_to_dash(_clean(src.get('RE_BUILD_YM'))),
        'unisex_yn': to_yn(src.get('MALE_FEMALE_CMNUSE_TOILET_YN')),   # 01 v1.4.0 컬럼
        'base_dt': yyyymmdd_to_date(_clean(src.get('DATA_STD_DE'))),
    }


def in_region(row: dict, addr_filter: str) -> bool:
    """도로명 또는 지번 주소가 필터 접두어로 시작하는가(부분일치 금지)."""
    if not addr_filter:
        return True
    for key in ('addr_road', 'addr_jibun'):
        value = row.get(key) or ''
        if value.startswith(addr_filter):
            return True
    return False


class GgToiletCollector(MobilityCollector):
    EXT_SYS = 'GG_TOILET'
    DEFAULT_BASE_URL = 'https://openapi.gg.go.kr/Publtolt'

    @property
    def addr_filter(self) -> str:
        """주소 접두어. 빈 문자열이면 경기도 전체(약 11,217건)."""
        return os.getenv('GG_TOILET_ADDR_FILTER', DEFAULT_ADDR_FILTER).strip()

    @property
    def page_size(self) -> int:
        return min(int(os.getenv('GG_TOILET_PAGE_SIZE', str(MAX_PAGE_SIZE))), MAX_PAGE_SIZE)

    @property
    def max_pages(self) -> int:
        """무한 루프 방어 상한. 경기도 전체 11,217건이면 12페이지."""
        return int(os.getenv('GG_TOILET_MAX_PAGES', '50'))

    def _page_url(self, page_no: int) -> str:
        url = (self.base_url
               + '?KEY=' + self.api_key
               + '&Type=json'
               + '&pIndex=' + str(page_no)
               + '&pSize=' + str(self.page_size))
        if self.addr_filter:
            url += '&REFINE_ROADNM_ADDR=' + quote(self.addr_filter)
        return url

    def collect(self) -> List[dict]:
        matched: List[dict] = []
        seen_raw = 0
        dropped = 0
        total = None
        page = 1
        while page <= self.max_pages:
            page_total, raw_rows = parse_response(self.get_json(self._page_url(page)))
            if total is None:
                total = page_total
            if not raw_rows:
                break
            seen_raw += len(raw_rows)
            for src in raw_rows:
                row = map_row(src)
                if not row['toilet_name']:
                    dropped += 1
                    continue
                if not in_region(row, self.addr_filter):
                    dropped += 1
                    continue
                matched.append(row)
            if len(raw_rows) < self.page_size or (total and seen_raw >= total):
                break
            page += 1
            self.pause()
        else:
            logger.warning('GG_TOILET: 페이지 상한 %d 도달 — 수집이 잘렸을 수 있음', self.max_pages)

        with_coord = sum(1 for r in matched if r['latitude'] is not None and r['longitude'] is not None)
        logger.info('GG_TOILET: 원천 %s건(응답 %d) → 필터 %r 통과 %d건, 제외 %d건, 좌표 보유 %d건',
                    total, seen_raw, self.addr_filter, len(matched), dropped, with_coord)
        return matched
