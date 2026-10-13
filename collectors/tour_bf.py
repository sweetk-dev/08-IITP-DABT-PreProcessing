"""한국관광공사 무장애여행(KorWithService2) 수집 어댑터 — 적재: poi_tour_bf_facility.

Issue #76. areaBasedList2(지역 목록) + detailWithTour2(무장애 편의정보) 를 결합해
기존 poi_tour_bf_facility 스키마의 _yn 플래그로 파생 매핑한다.
기본 대상: 경기(31) 안양시(17) — 2026-09-07 실측 원본 11건.
편의정보 원문은 ext_data 파일(raw json)로 보존한다.

2026-09-08: 편의정보 서술 원문을 detail_raw 로 함께 적재한다(01 v1.6.0).

2026-09-07 정정 2건 (실측 근거):
1. ``flag_from_text`` 가 '없' 만 보고 N 을 반환해 **긍정 서술을 부정으로 뒤집었다**.
   "주출입구는 단차가 없어 휠체어 접근 가능함" -> 종전 N. 안양문화원·안양아트센터·
   평촌중앙공원·산타루치아 4건이 실제로 뒤집혀 있었다.
2. 접근로 정본인 ``route`` 필드를 아예 매핑하지 않았다. 안양 11건 중 route 는 10건,
   exit 는 8건 보유로 route 쪽이 더 잘 채워져 있다. slope_yn 은 route 우선으로 본다.

2026-10-13 (v1.17.1, #120): 목록 조회를 법정동 코드로 바꿨다.
   종전에는 옛 지역 코드(areaCode=31·sigunguCode=17)로 조회했다. 원천이 법정동 코드
   (lDongRegnCd·lDongSignguCd)로 옮겨 가면서 새로 등록·수정된 장소는 옛 시군구 코드가
   비어 이 조건에 걸리지 않았고, 수집 건수가 10 → 9 → 8 로 줄었다. 같은 날 법정동 코드로
   조회하면 만안구(171) 10건 · 동안구(173) 25건이다. 시는 구 단위 코드만 있으므로 구마다
   조회해 합치고 contentid 로 중복을 없앤다. 분류(관광지·음식점·쇼핑 등)는 거르지 않는다 —
   이 표는 관광 POI 에 무장애 속성을 덧씌우고 시설 내 장애인화장실을 찾는 데 쓰이며,
   추천 분류 거르기는 소비처(경로 서비스 category)가 한다.
"""
from __future__ import annotations

import datetime
import json
import os
import re
from typing import List, Optional

from collectors.mobility_base import MobilityCollector

GYEONGGI_INTERNAL_SIDO = '9410000'  # sys_common_code sido_code (내부용, prefix 9)

# detailWithTour2 응답 필드 → poi_tour_bf_facility 컬럼 (1:1 매핑분)
DETAIL_FIELD_MAP = {
    'restroom': 'toilet_yn',
    'elevator': 'elevator_yn',
    'parking': 'parking_yn',
    'wheelchair': 'wheelchair_rent_yn',
    'braileblock': 'tactile_map_yn',
    'lactationroom': 'nursing_room_yn',
    'stroller': 'stroller_rent_yn',
}

# 한 컬럼을 여러 응답 필드로 채우는 경우 — 앞의 필드가 우선한다.
# slope_yn: route(출입구까지의 접근로)가 정본, 없으면 exit(주출입구 단차/경사로)
MULTI_FIELD_MAP = {
    'slope_yn': ('route', 'exit'),
    'accessible_room_yn': ('room', 'auditorium'),
    'audio_guide_yn': ('audioguide', 'videoguide', 'signguide', 'guidehuman'),
}

# 부정 단서가 먼저다 — "이용 어려움" 처럼 '없' 이 없는 부정 서술을 잡는다.
NEGATIVE_HINTS = ('불가', '미설치', '없습니다', '해당없음', '해당 없음', '어려움', '어렵')
# 긍정 단서 — "단차가 없어 ... 가능함" 류를 부정으로 오판하지 않게 한다.
POSITIVE_HINTS = ('가능', '있음', '있어', '있는', '설치', '완비', '보유', '넓음', '완만')


def flag_from_text(text: Optional[str]) -> Optional[str]:
    """편의정보 서술 텍스트 → Y/N/None (정보 없음).

    판정 순서 — 2026-09-07 정정. 종전에는 "'없' 이 있고 '있' 이 없으면 N" 이었는데,
    원문이 "주출입구는 단차가 없어 휠체어 접근 가능함" 처럼 **없다는 사실이 긍정인**
    서술이라 결과가 뒤집혔다.

    1. 부정 단서(불가·미설치·어려움…)가 있고 '가능' 이 없으면 N
    2. 긍정 단서(가능·있음·설치·완만…)가 있으면 Y
    3. 그 밖에 '없' 이 있으면 N  (예: "엘리베이터 없음")
    4. 서술이 존재한다는 것 자체를 존재로 본다 → Y
    """
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    if any(n in s for n in NEGATIVE_HINTS) and '가능' not in s:
        return 'N'
    if any(p in s for p in POSITIVE_HINTS):
        return 'Y'
    if '없' in s:
        return 'N'
    return 'Y'


def first_flag(detail_item: dict, fields) -> Optional[str]:
    """여러 응답 필드 중 값이 있는 첫 필드로 플래그를 정한다(앞이 우선)."""
    for f in fields:
        flag = flag_from_text((detail_item or {}).get(f))
        if flag is not None:
            return flag
    return None


class TourBfCollector(MobilityCollector):
    EXT_SYS = 'TOUR_BF_API'
    DEFAULT_BASE_URL = 'https://apis.data.go.kr/B551011/KorWithService2'

    @property
    def ldong_regn_cd(self) -> str:
        """법정동 시도 코드(경기 41)."""
        code = os.getenv('TOUR_LDONG_REGN_CD', '41').strip()
        if not re.fullmatch(r'\d{1,5}', code):
            raise ValueError('TOUR_LDONG_REGN_CD 는 숫자 코드여야 합니다: %r' % code)
        return code

    @property
    def ldong_signgu_cds(self) -> List[str]:
        """법정동 시군구 코드 목록(안양 만안구 171 · 동안구 173). 쉼표로 여러 개."""
        raw = os.getenv('TOUR_LDONG_SIGNGU_CDS', '171,173')
        codes = [c.strip() for c in raw.split(',') if c.strip()]
        bad = [c for c in codes if not re.fullmatch(r'\d{1,5}', c)]
        if bad or not codes:
            raise ValueError('TOUR_LDONG_SIGNGU_CDS 는 쉼표로 구분한 숫자 코드여야 합니다: %r' % raw)
        return codes

    def _common_qs(self) -> str:
        return ('serviceKey=' + self.api_key
                + '&MobileOS=ETC&MobileApp=iitp-dabt&_type=json')

    def _list_url(self, signgu_cd: str, page_no: int) -> str:
        return (self.base_url + '/areaBasedList2?' + self._common_qs()
                + '&lDongRegnCd=' + self.ldong_regn_cd
                + '&lDongSignguCd=' + signgu_cd
                + '&numOfRows=100&pageNo=' + str(page_no))

    def _detail_url(self, content_id) -> str:
        return (self.base_url + '/detailWithTour2?' + self._common_qs()
                + '&contentId=' + str(content_id))

    @staticmethod
    def _body(data: dict) -> dict:
        """응답 본문. 결과 코드가 정상(0000)이 아니면 예외 — 빈 목록으로 오인해 적재·논리삭제
        판단을 하지 않도록 수집 실패로 올린다."""
        response = (data or {}).get('response') or {}
        header = response.get('header') or {}
        code = str(header.get('resultCode') or '').strip()
        if code and code not in ('0000', '00'):
            raise RuntimeError('TOUR_BF_API 오류 응답: resultCode=%s %s'
                               % (code, str(header.get('resultMsg') or '')[:100]))
        return response.get('body') or {}

    def _fetch_signgu(self, signgu_cd: str) -> List[dict]:
        items: List[dict] = []
        page = 1
        while True:
            body = self._body(self.get_json(self._list_url(signgu_cd, page)))
            chunk = (body.get('items') or {}).get('item') or []
            if isinstance(chunk, dict):
                chunk = [chunk]
            items.extend(chunk)
            total = int(body.get('totalCount') or 0)
            if len(items) >= total or not chunk:
                break
            page += 1
            self.pause()
        return items

    def fetch_area_list(self) -> List[dict]:
        """시군구 코드마다 목록을 받아 합친다. 같은 contentid 는 처음 것만 남긴다."""
        items: List[dict] = []
        seen = set()
        for i, signgu_cd in enumerate(self.ldong_signgu_cds):
            if i:
                self.pause()
            for item in self._fetch_signgu(signgu_cd):
                cid = str(item.get('contentid') or '').strip()
                if cid:
                    if cid in seen:
                        continue
                    seen.add(cid)
                items.append(item)
        return items

    def fetch_detail(self, content_id) -> dict:
        body = self._body(self.get_json(self._detail_url(content_id)))
        item = (body.get('items') or {}).get('item') or {}
        if isinstance(item, list):
            item = item[0] if item else {}
        return item

    @staticmethod
    def map_row(area_item: dict, detail_item: dict) -> dict:
        """areaBasedList2 + detailWithTour2 → poi_tour_bf_facility 컬럼 매핑."""
        row = {
            'sido_code': GYEONGGI_INTERNAL_SIDO,
            'fclt_name': str(area_item.get('title') or ''),
            'addr_road': area_item.get('addr1'),
            'addr_jibun': area_item.get('addr2'),
            'latitude': float(area_item['mapy']) if area_item.get('mapy') else None,
            'longitude': float(area_item['mapx']) if area_item.get('mapx') else None,
            'base_dt': datetime.date.today().isoformat(),
        }
        for src_field, col in DETAIL_FIELD_MAP.items():
            row[col] = flag_from_text(detail_item.get(src_field))
        for col, fields in MULTI_FIELD_MAP.items():
            row[col] = first_flag(detail_item, fields)
        # 서술 원문을 그대로 남긴다(01 v1.6.0 detail_raw). 플래그로 접으면
        # "본관 옆 부스" 같은 위치 설명이 사라지고, 판정 규칙이 바뀌었을 때
        # 원천을 다시 호출하지 않고는 재파싱할 수 없다.
        row['detail_raw'] = (json.dumps(detail_item, ensure_ascii=False)
                             if detail_item else None)
        public_transport = str(detail_item.get('publictransport') or '')
        row['subway_yn'] = 'Y' if '지하철' in public_transport else (None if not public_transport else 'N')
        row['bus_stop_yn'] = 'Y' if '버스' in public_transport else (None if not public_transport else 'N')
        return row

    def collect(self) -> List[dict]:
        rows = []
        self._raw_details = []
        for area_item in self.fetch_area_list():
            detail = self.fetch_detail(area_item.get('contentid'))
            self._raw_details.append({'area': area_item, 'detail': detail})
            rows.append(self.map_row(area_item, detail))
            self.pause()
        return rows
