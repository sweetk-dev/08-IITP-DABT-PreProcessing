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
- 기구표: getFacInfoOpenApiJpEvalInfoList?wfcltId= — 트래픽 한도 고려 기본 OFF
  (KOWSI_FETCH_EVAL=ON 일 때만 이번 실행에서 필터 통과한 시설에 한해 호출)
"""
from __future__ import annotations

import datetime
import json
import logging
import math
import os
from typing import List, Optional, Tuple
import xml.etree.ElementTree as ET

from collectors.mobility_base import MobilityCollector

logger = logging.getLogger(__name__)

EVAL_FLAG_MAP = {
    '승강기': 'elevator_yn',
    '장애인사용가능화장실': 'dis_toilet_yn',
    '장애인전용주차구역': 'dis_parking_yn',
    '주출입구 높이차이 제거': 'entrance_ramp_yn',
    '주출입구(문)': 'entrance_door_yn',
    '주출입구 접근로': 'approach_road_yn',
}

DEFAULT_STATE_PATH = os.path.join('ext_data', 'KOWSI_FACL', 'state.json')


def parse_eval_flags(eval_info: Optional[str]) -> dict:
    """기구표 원문(콤마 구분) → _yn 플래그 dict. 원문 없으면 전부 None."""
    flags = {col: None for col in EVAL_FLAG_MAP.values()}
    if not eval_info:
        return flags
    for col in EVAL_FLAG_MAP.values():
        flags[col] = 'N'
    for token in str(eval_info).split(','):
        name = token.strip()
        col = EVAL_FLAG_MAP.get(name)
        if col:
            flags[col] = 'Y'
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
            for row in matched:
                key = row.get('wfclt_id') or row.get('facl_inf_id')
                try:
                    root = self.get_xml(self._eval_url(key))
                    check_api_error(root)
                    eval_info = root.findtext('.//evalInfo')
                except RuntimeError:
                    eval_info = None
                row['eval_info_raw'] = eval_info
                row.update(parse_eval_flags(eval_info))
                self.pause()
        else:
            for row in matched:
                row.update(parse_eval_flags(None))
        return matched
