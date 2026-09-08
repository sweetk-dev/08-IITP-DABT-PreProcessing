"""경기버스정보 저상버스 노선현황(전일 기준) 수집 어댑터 — Issue #97.

원천: https://www.gbis.go.kr/service/busInfo/lowfloorBus.action?cmd=lowfloorAuto
      "저상버스 정보는 어제 기준" 표. 경기도 전 시군 노선(1,030행, 2026-09-07 실측)이
      한 페이지에 전부 들어 있고, 노선번호 셀의 busInfoView('번호','routeId') 링크에
      GBIS routeId 가 함께 있다. 따라서 매칭은 routeId 로 하고, 노선번호·운수사는
      원본 보존과 검증용으로만 쓴다(같은 번호가 여러 운수사에 있어 번호 매칭은 위험 —
      5번 삼영운수/안양-편안운수, 6번 삼영운수/안양-학운교통, 9번 안양-신안운수/삼영운수).

적재(db_mobility.update_bus_route_low_floor):
  · 표에 있는 routeId              → tran_bus_route_info.low_bus_yn = 'Y'
  · 경기도 관할 노선 중 표에 없는 것 → 'N'
  · low_bus_base_dt = 기준일(수집일 전일). 페이지를 못 읽으면 기존 값을 건드리지 않는다.

정적 노선 API(getBusRouteInfoItemv2)에는 저상 항목이 없어 이 컬럼이 비어 있었다.
실시간 저상 여부(차량 단위 lowPlate)는 서비스가 직접 호출하며 여기서 다루지 않는다.
"""
from __future__ import annotations

import datetime
import html
import logging
import os
import re
from typing import List, Optional

from collectors.mobility_base import MobilityCollector, to_int

logger = logging.getLogger(__name__)

DEFAULT_PAGE_URL = 'https://www.gbis.go.kr/service/busInfo/lowfloorBus.action?cmd=lowfloorAuto'

# 표 한 행에서 기대하는 셀 수(시/군·노선번호·기점·종점·상행·하행·배차·운수사)
MIN_CELLS = 7

_TR = re.compile(r'<tr[^>]*>(.*?)</tr>', re.S | re.I)
_TD = re.compile(r'<td[^>]*>(.*?)</td>', re.S | re.I)
_TAG = re.compile(r'<[^>]+>')
_BR = re.compile(r'<br\s*/?>', re.I)
_VIEW = re.compile(r"busInfoView\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)")
_DAY = re.compile(r'(평일|토요일|일요일|공휴일)\s*:\s*([^\n]*)')


def _cell_text(raw: str) -> str:
    """셀 HTML → 줄바꿈(br)은 유지, 태그 제거, 공백 정리."""
    txt = _BR.sub('\n', raw)
    txt = _TAG.sub(' ', txt)
    txt = html.unescape(txt)
    lines = [re.sub(r'[ \t\r\f\v]+', ' ', ln).strip() for ln in txt.split('\n')]
    return '\n'.join(ln for ln in lines if ln)


def _day_map(cell: str) -> dict:
    """'평일 : 04:40 ~ 22:30\n토요일 : ...' → {'평일': '04:40 ~ 22:30', ...}.

    값이 비어 있는 요일('평일 : ~', '~ 분')은 None 으로 둔다(주말만 운행하는 노선 등).
    """
    out = {}
    for day, val in _DAY.findall(cell):
        v = val.strip()
        v = re.sub(r'\s+', ' ', v)
        if v in ('', '~', '~ 분', '분') or re.fullmatch(r'~\s*분?', v):
            out[day] = None
        else:
            out[day] = v
    return out


def parse_lowfloor_rows(page_html: str) -> List[dict]:
    """페이지 HTML → 행 dict 목록(전 시군). 표 구조가 바뀌어 routeId 를 못 찾으면 그 행은 버린다."""
    rows = []
    for tr in _TR.findall(page_html):
        cells = _TD.findall(tr)
        if len(cells) < MIN_CELLS:
            continue
        m = _VIEW.search(tr)
        if not m:
            continue
        texts = [_cell_text(c) for c in cells]
        rows.append({
            'region': texts[0],
            'route_name': m.group(1).strip() or texts[1].replace('\n', ' ').strip(),
            'route_id': to_int(m.group(2)),
            'start_station': texts[2],
            'end_station': texts[3],
            'up_hours': _day_map(texts[4]),
            'down_hours': _day_map(texts[5]),
            'alloc': _day_map(texts[6]),
            'company': texts[-1].replace('\n', ' ').strip(),
        })
    return [r for r in rows if r['route_id'] is not None]


def base_date_for(collected: Optional[datetime.date] = None) -> str:
    """페이지 문구 "어제 기준" — 수집일 전일을 기준일로 기록한다."""
    d = (collected or datetime.date.today()) - datetime.timedelta(days=1)
    return d.isoformat()


class GbisLowFloorCollector(MobilityCollector):
    """저상버스 노선현황 페이지 수집. 인증키 불필요(공개 페이지)."""

    EXT_SYS = 'GBIS_LOWFLOOR'
    DEFAULT_BASE_URL = DEFAULT_PAGE_URL

    @property
    def api_key(self) -> str:      # 공개 페이지 — 키가 없어도 동작
        return ''

    @property
    def region_filter(self) -> str:
        """원본 보존 대상 시군(빈 문자열이면 전 시군). 매칭은 routeId 라 전 시군 행을 쓴다."""
        return os.getenv('GBIS_LOWFLOOR_REGION_FILTER', '')

    def fetch_page(self) -> str:
        resp = self.http_get(self.base_url, timeout=40, retries=2, backoff_sec=2.0)
        raw = resp.content or b''
        text = raw.decode('utf-8', 'replace')
        if '저상' not in text:
            text = raw.decode('cp949', 'replace')
        if '저상' not in text or 'busInfoView' not in text:
            raise RuntimeError('저상버스 노선현황 페이지 형식이 예상과 다릅니다 — 적재를 중단합니다')
        return text

    def collect(self) -> List[dict]:
        page = self.fetch_page()
        rows = parse_lowfloor_rows(page)
        if not rows:
            raise RuntimeError('저상버스 노선현황 표에서 행을 하나도 읽지 못했습니다 — 적재를 중단합니다')
        base_dt = base_date_for()
        region = self.region_filter
        out = []
        for r in rows:
            if region and region not in r['region']:
                continue
            r['low_bus_base_dt'] = base_dt
            out.append(r)
        logger.info('GBIS_LOWFLOOR: 표 %d행, 보존 %d행(필터=%r), 기준일 %s',
                    len(rows), len(out), region, base_dt)
        # 매칭용 전체 routeId 는 별도로 보관(필터와 무관하게 전 시군)
        self._all_route_ids = sorted({r['route_id'] for r in rows})
        self._base_dt = base_dt
        return out
