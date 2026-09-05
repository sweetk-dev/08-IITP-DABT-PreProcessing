"""한국사회보장정보원 장애인편의시설 수집 어댑터 — 적재: poi_facility_accessibility.

Issue #76 최초 구현, Issue #78 분할 수집 개선.
B554287/DisabledPersonConvenientFacility (XML 전용).

- 목록: getDisConvFaclList 페이징 → 주소(lcMnad) 필터(기본 '안양시')
- 에러 감지: 응답 resultCode 가 0 이 아니면 RuntimeError (조용한 실패 금지, #78)
- 분할 수집(#78): numOfRows 상한 1000 + 일 트래픽 한도로 단일 실행 완주 불가
  → 상태 파일(ext_data/KOWSI_FACL/state.json)에 next_page 를 기록하고
    실행당 최대 KOWSI_MAX_PAGES 페이지만 스캔, 다음 실행이 이어받는다.
    전 페이지 완주 시 cycle_completed_at 기록, KOWSI_RESCAN_DAYS(기본 28일)
    이내 재실행은 skip. 일 단위 cron 으로 돌리면 월 1회 전국 리프레시가 된다.
- 기구표: getFacInfoOpenApiJpEvalInfoList?wfcltId= — 시설 1건당 1호출
  (KOWSI_FETCH_EVAL=ON 일 때만 이번 실행에서 필터 통과한 시설에 한해 호출)
  개발계정(일 100건)에서는 완주 불가라 기본 OFF 였다. 운영계정 전환(2026-09-04) 후
  안양 1,617개 시설 전수 수집을 확인했다(429 없음, 실패 0).
- 기구표 원문 처리(2026-09-04 실측 반영):
  · 토큰 표기가 흔들려 정규화 후 매핑한다(_norm). 예: '주출입구 높이차이 제거'
  · 기구표 미작성 시 원천이 전 항목을 채운 **더미 응답**을 내보낸다(1.2%).
    이를 'N'(부재)으로 적재하면 없는 시설이 있는 것으로 기록되므로 None(미확인)으로 둔다.
  · 처음 보는 토큰은 경고로 남긴다 — 원천이 표기를 바꿔도 묻히지 않게.
"""
from __future__ import annotations

import datetime
import json
import logging
import math
import os
import re
from typing import List, Optional, Tuple
import xml.etree.ElementTree as ET

from collectors.mobility_base import MobilityCollector

logger = logging.getLogger(__name__)

# 기구표 원문 토큰 -> 컬럼. 키는 _norm() 을 거친 형태(공백 제거)로 둔다.
# 원천이 같은 항목을 여러 표기로 내보내므로 정규화 없이 비교하면 놓친다.
EVAL_FLAG_MAP = {
    '승강기': 'elevator_yn',
    '장애인사용가능화장실': 'dis_toilet_yn',
    '장애인전용주차구역': 'dis_parking_yn',
    '주출입구높이차이제거': 'entrance_ramp_yn',
    '주출입구(문)': 'entrance_door_yn',
    '주출입구접근로': 'approach_road_yn',
    # 2026-09-05 컬럼 추가(01 v1.4.0). 실측 2026-09-04(안양 1,617개 시설): 37건 / 23건.
    '유도및안내설비': 'guide_facility_yn',
    '장애인사용가능객실': 'accessible_room_yn',
}

# 정상 기구표에 항목이 없어도 'N' 이 아니라 None 으로 두는 컬럼.
# 장애인사용가능객실은 숙박시설에만 해당한다 — 청사·어린이집에 '객실 없음(N)' 을 기록하면
# 의무 대상이 아닌 시설을 미설치 시설로 왜곡한다. 시설 유형으로 판정하지 않고 항목이 있을 때만 'Y'.
EVAL_ABSENT_IS_UNKNOWN = ('accessible_room_yn',)

# 정상 어휘이지만 담을 컬럼이 없는 항목(현재 없음) — 경고 대상이 아니다(eval_info_raw 에 보존).
EVAL_NO_COLUMN = ()

# 기구표 미작성 시 원천이 내보내는 "전 항목 채움" 더미 응답의 표식.
# 실측 2026-09-04: 안양 1,617개 시설 중 20건(1.2%)이 아래 8항목을 담은 동일 문자열 1종이었다.
#   주출입구 접근로, 장애인전용주차구역, 주출입구높이차이제거(경사로), 주출입문,
#   승강기, 장애인사용가능화장실, 안내설비, 장애인사용가능객실
# 어린이공원 화장실에 승강기·장애인사용가능객실이 붙어 있어 실제 값으로 볼 수 없다.
# '주출입문' 은 이 더미에서만 나타나고 정상 응답과 교차 0 이라 판별자로 쓴다.
DUMMY_EVAL_MARKER = '주출입문'

DEFAULT_STATE_PATH = os.path.join('ext_data', 'KOWSI_FACL', 'state.json')


def _norm(text: Optional[str]) -> str:
    """토큰 비교용 정규화 — 공백을 모두 제거한다."""
    return re.sub(r'\s+', '', text or '')


def is_dummy_eval(eval_info: Optional[str]) -> bool:
    """기구표 더미(전 항목 채움) 응답인가."""
    return DUMMY_EVAL_MARKER in _norm(eval_info)


def parse_eval_flags(eval_info: Optional[str], facl_id: Optional[str] = None) -> dict:
    """기구표 원문(콤마 구분) → _yn 플래그 dict.

    - 원문이 없으면 전부 None (미확인)
    - 더미 응답이면 전부 None (미확인) — 'N'(부재)으로 두면 없는 시설이 있는 것으로 기록된다
    - 그 외에는 매핑된 항목 'Y', 나머지 'N' (EVAL_ABSENT_IS_UNKNOWN 컬럼은 None)

    처음 보는 토큰은 경고로 남긴다. 접근성 데이터에서 위양성('없는데 있다')은
    위음성보다 위험하므로, 판단이 서지 않으면 'N' 이 아니라 None 으로 둔다.
    """
    flags = {col: None for col in EVAL_FLAG_MAP.values()}
    if not eval_info:
        return flags
    if is_dummy_eval(eval_info):
        logger.warning('KOWSI_FACL 기구표 더미 응답 — 플래그 미판정 (facl=%s): %s',
                       facl_id or '?', eval_info)
        return flags
    for col in EVAL_FLAG_MAP.values():
        flags[col] = None if col in EVAL_ABSENT_IS_UNKNOWN else 'N'
    for token in str(eval_info).split(','):
        name = _norm(token)
        if not name:
            continue
        col = EVAL_FLAG_MAP.get(name)
        if col:
            flags[col] = 'Y'
        elif name not in EVAL_NO_COLUMN:
            logger.warning('KOWSI_FACL 기구표 미매핑 토큰 (facl=%s): %r',
                           facl_id or '?', token.strip())
    return flags


def check_api_error(root: ET.Element) -> None:
    """응답 resultCode 검사 — 0/미존재 외에는 예외 (#78, 조용한 실패 금지)."""
    code = root.findtext('resultCode')
    if code is None:
        return
    code = code.strip()
    if code not in ('', '0'):
        message = (root.findtext('resultMessage') or '').strip()
        raise RuntimeError(f'KOWSI_FACL API error resultCode={code} message={message}')


class KowsiFaclCollector(MobilityCollector):
    EXT_SYS = 'KOWSI_FACL'
    DEFAULT_BASE_URL = 'https://apis.data.go.kr/B554287/DisabledPersonConvenientFacility'

    @property
    def addr_filter(self) -> str:
        return os.getenv('KOWSI_ADDR_FILTER', '안양시')

    @property
    def page_size(self) -> int:
        # API 상한 1000 (2026-07-13 실측: 초과 시 INVALID_REQUEST_PARAMETER_ERROR)
        return min(int(os.getenv('KOWSI_PAGE_SIZE', '1000')), 1000)

    @property
    def max_pages(self) -> int:
        """실행당 스캔 페이지 수 — 일 트래픽 한도(개발계정 100건/일) 방어."""
        return int(os.getenv('KOWSI_MAX_PAGES', '80'))

    @property
    def rescan_days(self) -> int:
        return int(os.getenv('KOWSI_RESCAN_DAYS', '28'))

    @property
    def state_path(self) -> str:
        return os.getenv('KOWSI_STATE_PATH', DEFAULT_STATE_PATH)

    @property
    def fetch_eval(self) -> bool:
        return os.getenv('KOWSI_FETCH_EVAL', 'OFF').upper() == 'ON'

    # --- 상태 파일 (#78) ----------------------------------------------------
    def load_state(self) -> dict:
        try:
            with open(self.state_path, encoding='utf-8') as f:
                state = json.load(f)
            if not isinstance(state, dict):
                raise ValueError('state must be dict')
        except (OSError, ValueError):
            state = {}
        state.setdefault('next_page', 1)
        state.setdefault('cycle_completed_at', None)
        return state

    def save_state(self, state: dict) -> None:
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        with open(self.state_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    def _recently_completed(self, state: dict) -> bool:
        completed = state.get('cycle_completed_at')
        if not completed or int(state.get('next_page', 1)) != 1:
            return False
        try:
            completed_dt = datetime.datetime.fromisoformat(completed)
        except ValueError:
            return False
        age_days = (datetime.datetime.now() - completed_dt).days
        return age_days < self.rescan_days

    # --- URL ---------------------------------------------------------------
    def _list_url(self, page_no: int) -> str:
        return (self.base_url + '/getDisConvFaclList'
                + '?serviceKey=' + self.api_key
                + '&pageNo=' + str(page_no)
                + '&numOfRows=' + str(self.page_size))

    def _eval_url(self, wfclt_id: str) -> str:
        return (self.base_url + '/getFacInfoOpenApiJpEvalInfoList'
                + '?serviceKey=' + self.api_key
                + '&wfcltId=' + str(wfclt_id))

    # --- 파싱 ---------------------------------------------------------------
    @staticmethod
    def _text(elem: ET.Element, tag: str) -> Optional[str]:
        node = elem.find(tag)
        if node is None or node.text is None:
            return None
        return node.text.strip()

    @staticmethod
    def _to_float(value):
        try:
            if value is None or value == '':
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def parse_list_page(self, root: ET.Element) -> Tuple[int, List[dict]]:
        check_api_error(root)
        total = int((root.findtext('totalCount') or '0').strip() or 0)
        rows = []
        today = datetime.date.today().isoformat()
        for serv in root.iter('servList'):
            addr = self._text(serv, 'lcMnad') or ''
            rows.append({
                'facl_inf_id': self._text(serv, 'faclInfId'),
                'facl_name': self._text(serv, 'faclNm') or '',
                'facl_type': self._text(serv, 'faclTyCd'),
                'addr': addr,
                'latitude': self._to_float(self._text(serv, 'faclLat')),
                'longitude': self._to_float(self._text(serv, 'faclLng')),
                'estb_date': self._text(serv, 'estbDate'),
                'eval_info_raw': None,
                'wfclt_id': self._text(serv, 'wfcltId'),
                'base_dt': today,
            })
        return total, rows

    # --- 수집 (#78 분할) ------------------------------------------------------
    def collect(self) -> List[dict]:
        state = self.load_state()
        if self._recently_completed(state):
            logger.info(
                'KOWSI_FACL: 최근 %s 완주(재스캔 주기 %d일 미도래) — skip',
                state.get('cycle_completed_at'), self.rescan_days,
            )
            return []

        start_page = int(state.get('next_page', 1))
        end_page = start_page + self.max_pages - 1
        matched: List[dict] = []
        completed = False
        page = start_page
        while page <= end_page:
            root = self.get_xml(self._list_url(page))
            total, rows = self.parse_list_page(root)
            for row in rows:
                if row['facl_inf_id'] and self.addr_filter in (row['addr'] or ''):
                    matched.append(row)
            total_pages = max(1, math.ceil(total / self.page_size)) if total else page
            if page >= total_pages or not rows:
                completed = True
                break
            page += 1
            self.pause()

        if completed:
            state['next_page'] = 1
            state['cycle_completed_at'] = datetime.datetime.now().isoformat(timespec='seconds')
            logger.info('KOWSI_FACL: 전 페이지 스캔 완료(~p%d) — cycle 종료', page)
        else:
            state['next_page'] = end_page + 1
            logger.info('KOWSI_FACL: p%d~p%d 스캔 — 다음 실행 p%d부터 이어받기', start_page, end_page, end_page + 1)
        self.save_state(state)

        if self.fetch_eval:
            # 기구표는 wfcltId(건물) 단위다. 한 건물에 여러 시설이 등록돼 있으면
            # (아파트 + 경로당 + 어린이집 같은 경우) 같은 기구표를 공유하므로
            # 건물당 1회만 호출하고 결과를 돌려쓴다.
            # 실측 2026-09-04(안양): 시설 1,649건이 건물 1,617개에 걸쳐 있어 32회를 아낀다.
            eval_cache: dict = {}
            for row in matched:
                key = row.get('wfclt_id') or row.get('facl_inf_id')
                if key in eval_cache:
                    eval_info = eval_cache[key]
                else:
                    try:
                        root = self.get_xml(self._eval_url(key))
                        check_api_error(root)
                        eval_info = root.findtext('.//evalInfo')
                    except RuntimeError:
                        eval_info = None
                    eval_cache[key] = eval_info
                    self.pause()
                row['eval_info_raw'] = eval_info
                row.update(parse_eval_flags(eval_info, key))
            logger.info('KOWSI_FACL 기구표: 시설 %d건 / 건물 %d개 호출',
                        len(matched), len(eval_cache))
        else:
            for row in matched:
                row.update(parse_eval_flags(None))
        return matched
