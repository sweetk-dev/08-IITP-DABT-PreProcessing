"""전국교통약자이동지원센터정보표준데이터 수집 어댑터 — 적재: poi_emergency_support (support_type='calltaxi').

원천: 공공데이터포털 표준데이터 「전국교통약자이동지원센터정보표준데이터」(데이터 ID 15028207)
      OpenAPI https://api.data.go.kr/openapi/tn_pubr_public_tfcwker_mvmn_cnter_api (활용신청 필요)
      센터 1곳 = 1행. 2026-11-02 실측 178행(전국), 31개 칸.

적재 규칙 (v1.23.0)
  - 자연키는 poi_emergency_support 공통 (유형, 이름, 도로명주소, 설치지점). 이 원천은 설치지점이 없다.
  - 원천이 같은 센터(이름+도로명주소)를 두 번 올린 경우가 있다(2026-11-02: 칠곡·의왕·광주 3쌍).
    데이터기준일자가 늦은 행을 남긴다.
  - tel 은 이용자가 거는 접수 전화(rceptPhoneNumber). 관리기관 전화가 다르면 note 에 남긴다.
  - homepage 는 인터넷 접수 주소 칸에서 주소 모양만 뽑는다('없음'·'해당없음'·앱 이름은 버린다).
  - open_hours 는 접수·운행 시각을 평일/주말로 한 줄에. 00:00~23:59 는 24시간, 00:00~00:00 은
    운영 여부를 알 수 없어 적지 않는다(추정하지 않는다 — 충전기 표준데이터 로더와 같은 규칙).
  - 시도 코드는 주소 첫 단어로 정한다. 01 공통코드(sido_code)에 없는 이름(2026-11 실측:
    '전남광주통합특별시')은 NULL 로 두고 경고만 남긴다 — 코드를 지어내지 않는다.
  - 시군구는 제공기관명(insttNm)의 둘째 단어를 먼저 쓴다. 센터 사무실이 이웃 시에 있는 경우
    (신안군 센터 → 목포시 주소)에도 담당 시군이 맞게 들어간다. 없으면 주소 둘째 단어.
  동기화(사라진 센터 논리삭제·다른 원천과 주소가 겹치는 행 건너뜀)는 db_mobility.sync_std_calltaxi.
"""
from __future__ import annotations

import datetime
import logging
import re
import urllib.parse
from typing import Dict, List, Optional

from collectors.mobility_base import MobilityCollector, to_float

logger = logging.getLogger(__name__)

EXT_SYS = 'STD_CALLTAXI'
SOURCE = EXT_SYS
SUPPORT_TYPE = 'calltaxi'
PAGE_SIZE = 100
# 전국 센터는 200곳 안팎이다. 이 상한을 넘으면 응답 이상으로 본다.
MAX_PAGES = 20

# 내부 시도 코드 — 01 sys_common_code(grp_id='sido_code')
SIDO_CODE = {
    '서울특별시': '9110000', '부산광역시': '9260000', '대구광역시': '9270000',
    '인천광역시': '9280000', '광주광역시': '9290000', '대전광역시': '9300000',
    '울산광역시': '9310000', '세종특별자치시': '9360000', '경기도': '9410000',
    '강원특별자치도': '9510000', '충청북도': '9430000', '충청남도': '9440000',
    '전북특별자치도': '9520000', '전라남도': '9460000', '경상북도': '9470000',
    '경상남도': '9480000', '제주특별자치도': '9500000',
}
# 원천에 개편 전 명칭이 섞여 들어온다
SIDO_ALIAS = {'전라북도': '전북특별자치도', '강원도': '강원특별자치도', '제주도': '제주특별자치도',
              '세종시': '세종특별자치시'}

# 값이 없다는 뜻으로 원천이 적는 표기
_NONE_TOKENS = {'', '-', '없음', '해당없음', '해당 없음', '미정', '미운영', 'null', 'none'}
_URL_RE = re.compile(r'(https?://[^\s()]+|(?:www\.)?[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}(?:/[^\s()]*)?)')
_SGG_RE = re.compile(r'.+(시|군|구)$')

NOTE_MAX = 1000
OPEN_HOURS_MAX = 200


def _clean(value) -> Optional[str]:
    if value is None:
        return None
    token = re.sub(r'\s+', ' ', str(value)).strip()
    return None if token.lower() in _NONE_TOKENS else token


def normalize_sido(name) -> str:
    token = (name or '').strip()
    return SIDO_ALIAS.get(token, token)


def sido_and_sgg(addr: Optional[str], instt_nm: Optional[str]):
    """(시도명, 시도코드 또는 None, 시군구명 또는 None)."""
    words = (addr or '').split()
    sido = normalize_sido(words[0]) if words else None
    code = SIDO_CODE.get(sido) if sido else None
    sgg = None
    instt = (instt_nm or '').split()
    if len(instt) >= 2 and _SGG_RE.match(instt[1]):
        sgg = instt[1]
    elif len(words) >= 2 and _SGG_RE.match(words[1]):
        sgg = words[1]
    return sido, code, sgg


def extract_homepage(value) -> Optional[str]:
    token = _clean(value)
    if not token:
        return None
    m = _URL_RE.search(token)
    return m.group(1).rstrip('.,')[:400] if m else None


def _span(open_hm, close_hm) -> Optional[str]:
    o, c = _clean(open_hm), _clean(close_hm)
    if not o or not c:
        return None
    if o in ('00:00', '0:00') and c in ('23:59', '24:00'):
        return '24시간'
    if o == c:
        return None        # 00:00~00:00 등 — 운영 여부를 알 수 없어 적지 않는다
    return '%s-%s' % (o, c)


def build_open_hours(src: dict) -> Optional[str]:
    def pair(label, wd_o, wd_c, we_o, we_c):
        parts = []
        wd = _span(src.get(wd_o), src.get(wd_c))
        we = _span(src.get(we_o), src.get(we_c))
        if wd:
            parts.append('평일 ' + wd)
        if we:
            parts.append('주말 ' + we)
        return (label + ' ' + '·'.join(parts)) if parts else None

    bits = [b for b in (
        pair('접수', 'weekdayRceptOpenHhmm', 'weekdayRceptColseHhmm', 'wkendRceptOpenHhmm', 'wkendRceptCloseHhmm'),
        pair('운행', 'weekdayOperOpenHhmm', 'weekdayOperColseHhmm', 'wkendOperOpenHhmm', 'wkendOperCloseHhmm'),
    ) if b]
    return ' / '.join(bits)[:OPEN_HOURS_MAX] or None


def build_note(src: dict, tel: Optional[str]) -> Optional[str]:
    """이용에 필요한 정보를 중요한 것부터 담는다. 넘치면 뒤쪽(이용대상·이용제한 원문)부터 잘린다."""
    bits = []
    cars = _clean(src.get('carHoldCo'))
    if cars and cars != '0':
        detail = []
        slope, lift = _clean(src.get('slopeVhcleCo')), _clean(src.get('liftVhcleCo'))
        if slope and slope != '0':
            detail.append('슬로프 %s' % slope)
        if lift and lift != '0':
            detail.append('리프트 %s' % lift)
        kind = _clean(src.get('carHoldKnd'))
        if kind:
            detail.append(kind)
        bits.append('차량 %s대' % cars + ('(%s)' % ', '.join(detail) if detail else ''))
    for label, key in (('사전예약', 'beffatResvePd'), ('관내 운행', 'insideOpratArea'),
                       ('관외 운행', 'outsideOpratArea'), ('요금', 'useCharge'), ('앱', 'appSvcNm')):
        v = _clean(src.get(key))
        if v:
            bits.append('%s %s' % (label, v))
    org = _clean(src.get('institutionNm'))
    org_tel = _clean(src.get('phoneNumber'))
    if org or (org_tel and org_tel != tel):
        bits.append('운영 %s' % ' '.join(x for x in (org, '(%s)' % org_tel if org_tel and org_tel != tel else None) if x))
    for label, key in (('이용대상', 'useTrget'), ('이용제한', 'useLmtt')):
        v = _clean(src.get(key))
        if v:
            bits.append('%s %s' % (label, v))
    note = ' | '.join(bits)
    if len(note) > NOTE_MAX:
        note = note[:NOTE_MAX - 1] + '…'
    return note or None


def _base_dt(value) -> Optional[str]:
    token = _clean(value)
    if not token:
        return None
    try:
        return datetime.date.fromisoformat(token[:10]).isoformat()
    except ValueError:
        return None


def _coord(value, lo, hi) -> Optional[float]:
    v = to_float(_clean(value))
    return v if v is not None and lo <= v <= hi else None


def map_row(src: dict) -> Optional[dict]:
    name = _clean(src.get('tfcwkerMvmnCnterNm'))
    if not name:
        return None
    addr_road = _clean(src.get('rdnmadr'))
    addr_jibun = _clean(src.get('lnmadr'))
    sido, code, sgg = sido_and_sgg(addr_road or addr_jibun, src.get('insttNm'))
    tel = (_clean(src.get('rceptPhoneNumber')) or _clean(src.get('phoneNumber')) or '')[:40] or None
    return {
        'support_type': SUPPORT_TYPE,
        'sido_code': code,
        'sgg_name': sgg,
        'name': name[:200],
        'addr_road': addr_road[:600] if addr_road else None,
        'addr_jibun': addr_jibun[:600] if addr_jibun else None,
        'zip_code': None,           # 표준데이터에 우편번호 항목이 없다. 지어내지 않는다.
        'latitude': _coord(src.get('latitude'), 33.0, 39.0),
        'longitude': _coord(src.get('longitude'), 124.0, 132.0),
        'tel': tel,
        'homepage': extract_homepage(src.get('rceptItnadr')),
        'open_hours': build_open_hours(src),
        'install_desc': None,
        'note': build_note(src, tel),
        'source': SOURCE,
        'confidence': 'H',
        'base_dt': _base_dt(src.get('referenceDate')),
        '_sido': sido,
    }


def build_rows(items: List[dict]) -> List[dict]:
    """원천 행 → 적재 행. 같은 (이름, 도로명주소) 는 데이터기준일자가 늦은 행 하나만 남긴다."""
    merged: Dict[tuple, dict] = {}
    order = []
    unknown_sido = {}
    for src in items:
        row = map_row(src)
        if row is None:
            continue
        if row['sido_code'] is None and row['_sido']:
            unknown_sido[row['_sido']] = unknown_sido.get(row['_sido'], 0) + 1
        key = (row['name'], row['addr_road'] or '')
        head = merged.get(key)
        if head is None:
            merged[key] = row
            order.append(key)
        elif (row['base_dt'] or '') > (head['base_dt'] or ''):
            merged[key] = row
    if unknown_sido:
        logger.warning('%s: 시도 코드가 없는 시도명 — %s (sido_code NULL 로 적재)', EXT_SYS,
                       ', '.join('%s %d건' % kv for kv in sorted(unknown_sido.items())))
    dup = len(items) - len(order)
    if dup:
        logger.info('%s: 같은 센터 중복 %d행을 데이터기준일자가 늦은 행으로 접음', EXT_SYS, dup)
    rows = [merged[k] for k in order]
    for row in rows:
        row.pop('_sido', None)
    return rows


def _items_of(payload: dict):
    """표준데이터 응답 → (resultCode, resultMsg, totalCount, items). 'response' 껍데기가 있든 없든 받는다."""
    root = payload.get('response', payload) if isinstance(payload, dict) else {}
    header = root.get('header') or {}
    body = root.get('body') or {}
    items = body.get('items') or []
    if isinstance(items, dict):
        items = items.get('item') or []
    if isinstance(items, dict):
        items = [items]
    total = body.get('totalCount')
    try:
        total = int(total)
    except (TypeError, ValueError):
        total = None
    return str(header.get('resultCode', '')), header.get('resultMsg'), total, [i for i in items if isinstance(i, dict)]


class StdCalltaxiCollector(MobilityCollector):
    EXT_SYS = EXT_SYS
    DEFAULT_BASE_URL = 'https://api.data.go.kr/openapi/tn_pubr_public_tfcwker_mvmn_cnter_api'

    def _service_key(self) -> str:
        """포털의 '디코딩' 키는 URL 인코딩하고, 이미 인코딩된 키(% 포함)는 그대로 쓴다."""
        key = self.api_key
        return key if '%' in key else urllib.parse.quote(key, safe='')

    def _page_url(self, page: int) -> str:
        return (self.base_url + '?serviceKey=' + self._service_key()
                + '&pageNo=%d&numOfRows=%d&type=json' % (page, PAGE_SIZE))

    def fetch_items(self) -> List[dict]:
        items: List[dict] = []
        total = None
        page = 1
        while page <= MAX_PAGES:
            code, msg, page_total, part = _items_of(self.get_json(self._page_url(page)))
            if code not in ('00', '0'):
                raise RuntimeError('%s 응답 오류 resultCode=%s %s' % (EXT_SYS, code, msg or ''))
            if total is None:
                if page_total is None:
                    raise RuntimeError('%s 응답에 totalCount 가 없음' % EXT_SYS)
                total = page_total
            items.extend(part)
            if not part or len(items) >= total:
                break
            page += 1
            self.pause()
        else:
            raise RuntimeError('%s 쪽 상한 %d 에 도달 — 수집이 잘렸을 수 있음(받은 %d / 전체 %s)'
                               % (EXT_SYS, MAX_PAGES, len(items), total))
        if not items:
            raise RuntimeError('%s 원천 0건 — 기존 데이터를 유지함' % EXT_SYS)
        if len(items) != total:
            raise RuntimeError('%s 받은 행 %d 이 응답 전체 건수 %d 와 다름' % (EXT_SYS, len(items), total))
        return items

    def collect(self) -> List[dict]:
        items = self.fetch_items()
        self._raw_details = items          # run_mobility 가 ext_data 에 원본으로 남긴다
        rows = build_rows(items)
        logger.info('%s: 원천 %d행 → 적재 대상 %d건', EXT_SYS, len(items), len(rows))
        return rows
