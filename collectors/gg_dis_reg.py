"""경기데이터드림 「경기도 내 등록장애인 집계현황(읍면동별·유형별·정도별)」 수집 어댑터.

원천: https://openapi.gg.go.kr/GGREDISPERST (인증키는 GG_CULTURE 와 같은 경기데이터드림 키)
적재: stats_gg_dis_reg_emd (01 v1.14.0) — 시군별 원천 기준일(DATA_COLCT_DE) 판을 쌓는다.

응답 칸
    SIGUN_NM 시군 · OLD_NM 일반구 · EMD_NM 읍면동 · OBSTCL_TYPE 장애유형 · DATA_COLCT_DE 기준일(YYYYMMDD)
    SUM / DSPSN_MALE_CNT / DSPSN_FEMALE_CNT                           전체·남·여
    SEVR_DSPSN_SUM / SEVR_DSPSN_MALE_CNT / SEVR_DSPSN_FEMALE_CNT      심한 장애
    NSEVR_DSPSN_SUM / NSEVR_DSPSN_MALE_CNT / NSEVR_DSPSN_FEMALE_CNT   심하지 않은 장애

원천 특성(2026-10-15 실측 7,503행, 시군 29곳)과 정제 규칙
    · 시군마다 기준일이 다르다(2021-06 ~ 2026-05). 한 시군 안에서는 기준일이 하나다.
    · 합계 행이 섞여 있다 — 적재하지 않는다(넣으면 합산이 두 배가 된다).
        유형 '소계'(읍면동 합계)·'전체'·'합계', 읍면동 '전체'(시군 합계 — 김포·남양주 등)
    · 장애유형 표기 '장루.요루'·'장루, 요루' → '장루·요루' 하나로 맞춘다.
    · 구 칸에 시군명이 그대로 온 행(광명시·남양주시)은 구 없음(NULL)으로 둔다.
    · 같은 (시군, 구, 읍면동, 유형, 기준일) 이 여러 행인 곳이 있다(수원 정자1동·영통3동 등 —
      원천에서 서로 다른 동이 같은 이름으로 실린 것으로 보인다). 버리면 시군·구 합계가 줄어들므로
      값을 더해 한 행으로 만들고 경고 로그를 남긴다(읍면동 값은 원천 오기가 섞인 값).
    · 인원 칸은 실수(61.0)로 온다 → 정수. 빈 값은 NULL 그대로 둔다.
    · 기준일이 YYYYMMDD 실제 날짜가 아니면 그 행은 버린다(기준일은 판을 가르는 키다).
"""
from __future__ import annotations

import datetime
import logging
import os
import re
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from collectors.gg_culture import _clean
from collectors.gg_openapi import parse_gg_response
from collectors.gg_welfare import to_count
from collectors.mobility_base import MobilityCollector

logger = logging.getLogger(__name__)

EXT_SYS = 'GG_DIS_REG'
SERVICE = 'GGREDISPERST'
MAX_PAGE_SIZE = 1000

# 합계 행 표지 — 유형 칸 또는 읍면동 칸이 이 값이면 적재하지 않는다.
TOTAL_TYPES = frozenset({'소계', '전체', '합계', '계'})
TOTAL_EMDS = frozenset({'전체', '합계', '소계', '계'})

COUNT_FIELDS = (
    ('tot_cnt', 'SUM'), ('male_cnt', 'DSPSN_MALE_CNT'), ('female_cnt', 'DSPSN_FEMALE_CNT'),
    ('sev_cnt', 'SEVR_DSPSN_SUM'), ('sev_male_cnt', 'SEVR_DSPSN_MALE_CNT'),
    ('sev_female_cnt', 'SEVR_DSPSN_FEMALE_CNT'),
    ('nsev_cnt', 'NSEVR_DSPSN_SUM'), ('nsev_male_cnt', 'NSEVR_DSPSN_MALE_CNT'),
    ('nsev_female_cnt', 'NSEVR_DSPSN_FEMALE_CNT'),
)
COUNT_COLUMNS = tuple(c for c, _ in COUNT_FIELDS)


def _squash(value) -> str:
    """공백을 모두 뺀 값 — '전 체'·'합 계' 처럼 띄어 쓴 합계 표지도 걸러 내기 위해 쓴다."""
    return re.sub(r'\s+', '', value or '')


def normalize_dis_type(value) -> Optional[str]:
    """장애유형 표기 통일. 공백 제거, 장루·요루 구분자 통일."""
    token = _clean(value)
    if token is None:
        return None
    token = re.sub(r'\s+', '', token)
    if '장루' in token and '요루' in token:
        return '장루·요루'
    return token


def parse_base_date(value) -> Optional[str]:
    """기준일 'YYYYMMDD' → 'YYYY-MM-DD'. 실제 날짜가 아니면 None."""
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


def map_row(src: dict) -> Optional[dict]:
    """원천 1행 → 적재 행. 합계 행·기준일 오류 행·이름 없는 행은 None."""
    sigun = _clean(src.get('SIGUN_NM'))
    emd = _clean(src.get('EMD_NM'))
    dis_type = normalize_dis_type(src.get('OBSTCL_TYPE'))
    base_dt = parse_base_date(src.get('DATA_COLCT_DE'))
    if not sigun or not emd or not dis_type or not base_dt:
        return None
    if dis_type in TOTAL_TYPES or _squash(emd) in TOTAL_EMDS:
        return None
    gu = _clean(src.get('OLD_NM'))
    if gu == sigun:
        gu = None
    row = {'base_dt': base_dt, 'sigun_nm': sigun, 'gu_nm': gu, 'emd_nm': emd, 'dis_type': dis_type}
    for col, field in COUNT_FIELDS:
        row[col] = to_count(src.get(field))
    return row


def row_key(row: dict) -> tuple:
    """DB 유니크 인덱스와 같은 키 (시군, 구, 읍면동, 유형, 기준일)."""
    return (row['sigun_nm'], row.get('gu_nm') or '', row['emd_nm'], row['dis_type'], row['base_dt'])


def merge_duplicates(rows: List[dict]) -> Tuple[List[dict], Dict[tuple, int]]:
    """같은 키 행을 더해 하나로 만든다. 반환 (행, {키: 원래 행 수} — 2 이상만).

    한쪽만 값이 있으면 그 값, 둘 다 비면 NULL.
    """
    merged: 'OrderedDict[tuple, dict]' = OrderedDict()
    counts: Dict[tuple, int] = {}
    for row in rows:
        key = row_key(row)
        if key not in merged:
            merged[key] = dict(row)
            counts[key] = 1
            continue
        counts[key] += 1
        acc = merged[key]
        for col in COUNT_COLUMNS:
            a, b = acc.get(col), row.get(col)
            acc[col] = b if a is None else (a if b is None else a + b)
    return list(merged.values()), {k: n for k, n in counts.items() if n > 1}


class GgDisRegCollector(MobilityCollector):
    EXT_SYS = EXT_SYS
    DEFAULT_BASE_URL = 'https://openapi.gg.go.kr'

    def __init__(self, api_info: dict = None, stats_src: dict = None):
        super().__init__(api_info, stats_src)
        self.failures: List[dict] = []
        self.stats: Dict[str, int] = {}

    @property
    def base_url(self) -> str:
        url = super().base_url
        match = re.match(r'(https?://[^/]+)', url)
        return match.group(1) if match else url

    @property
    def page_size(self) -> int:
        return min(int(os.getenv('GG_DIS_REG_PAGE_SIZE', str(MAX_PAGE_SIZE))), MAX_PAGE_SIZE)

    @property
    def max_pages(self) -> int:
        """무한 루프 방어 상한. 2026-10 원천 7,503행 = 8페이지."""
        return int(os.getenv('GG_DIS_REG_MAX_PAGES', '40'))

    def _page_url(self, page_no: int) -> str:
        return (self.base_url + '/' + SERVICE + '?KEY=' + self.api_key + '&Type=json'
                + '&pIndex=' + str(page_no) + '&pSize=' + str(self.page_size))

    def collect(self) -> List[dict]:
        """전체 페이지를 받아 정제한다. 총건수 불일치·0건은 RuntimeError(기존 판을 지킨다)."""
        raw: List[dict] = []
        total = None
        page = 1
        while page <= self.max_pages:
            page_total, page_rows = parse_gg_response(self.get_json(self._page_url(page)), SERVICE, EXT_SYS)
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
            raise RuntimeError('%s: 페이지 상한 %d 도달 — 수집이 잘렸을 수 있음' % (EXT_SYS, self.max_pages))
        if not raw:
            raise RuntimeError('%s: 원천 0건 — 기존 행을 유지함' % EXT_SYS)
        if total and len(raw) != total:
            raise RuntimeError('%s: 응답 총건수 %d 와 받은 행 %d 이 다름' % (EXT_SYS, total, len(raw)))

        mapped, skipped_total, skipped_bad = [], 0, 0
        for src in raw:
            row = map_row(src)
            if row is None:
                t = normalize_dis_type(src.get('OBSTCL_TYPE'))
                if t in TOTAL_TYPES or _squash(_clean(src.get('EMD_NM'))) in TOTAL_EMDS:
                    skipped_total += 1
                else:
                    skipped_bad += 1
                continue
            mapped.append(row)
        rows, dups = merge_duplicates(mapped)
        if not rows:
            # 원천은 왔는데 정제 뒤 남은 행이 없다 — 칸·형식이 바뀐 것으로 보고 기존 판을 지킨다
            # (그냥 끝내면 성공으로 기록되고 동기화 시각·요약표가 갱신된다).
            raise RuntimeError('%s: 원천 %d행이 정제 뒤 0행 — 칸·형식 변경 의심, 기존 판을 유지함' % (EXT_SYS, len(raw)))
        if skipped_bad:
            logger.warning('%s: 시군·읍면동·유형·기준일이 비었거나 기준일 형식 오류인 행 %d건 제외', EXT_SYS, skipped_bad)
        if dups:
            sample = ', '.join('%s %s %s %s(%d행)' % (k[0], k[1], k[2], k[3], n) for k, n in list(dups.items())[:5])
            logger.warning('%s: 같은 읍면동·유형이 여러 행인 %d곳은 값을 더해 한 행으로 적재 — 예: %s',
                           EXT_SYS, len(dups), sample)
        self.stats = {'source': len(raw), 'total_rows_skipped': skipped_total, 'bad_rows_skipped': skipped_bad,
                      'merged_keys': len(dups), 'rows': len(rows),
                      'siguns': len({r['sigun_nm'] for r in rows})}
        logger.info('%s: 원천 %d행 → 합계 행 %d 제외 · 적재 대상 %d행(시군 %d)',
                    EXT_SYS, len(raw), skipped_total, len(rows), self.stats['siguns'])
        return rows
