"""경기데이터드림(openapi.gg.go.kr) 공통 응답 해석.

경기데이터드림 OpenAPI 는 서비스마다 응답 최상위 키가 서비스명이다.

    정상    : {"<서비스명>": [{"head": [{"list_total_count": N},
                                        {"RESULT": {"CODE": "INFO-000", ...}},
                                        {"api_version": "1.0"}]},
                             {"row": [ {...}, ... ]}]}
    데이터 없음·마지막 페이지 이후 : {"RESULT": {"CODE": "INFO-200", ...}}   ← 오류 아님
    키 오류 등                      : {"RESULT": {"CODE": "ERROR-290", ...}}

모두 HTTP 200 으로 오므로 본문 코드로 판정해야 한다(조용한 실패 금지).
GG_TOILET(gg_toilet.parse_response)도 같은 규칙으로 해석한다. 이 모듈은 서비스명을
인자로 받는 일반형으로, GG_CULTURE 처럼 여러 서비스를 부르는 수집기가 쓴다.
"""
from __future__ import annotations

from typing import List, Tuple

from collectors.mobility_base import to_int

NO_DATA_CODE = 'INFO-200'   # 데이터 없음(오류 아님)
OK_CODE = 'INFO-000'        # 정상


def parse_gg_response(payload, service: str, label: str = 'GG') -> Tuple[int, List[dict]]:
    """응답 JSON → (list_total_count, rows). 데이터 없음(INFO-200)은 (0, []).

    service : 응답 최상위 키가 되는 서비스명(예: 'Ggbrlllibrry').
    label   : 오류 메시지 앞머리(어느 작업·서비스에서 났는지 운영자가 바로 알 수 있게).
    """
    if not isinstance(payload, dict):
        raise RuntimeError('%s API: 응답이 JSON 객체가 아님' % label)
    top_result = payload.get('RESULT')
    if top_result is not None:
        code = str(top_result.get('CODE', '')).strip()
        if code == NO_DATA_CODE:
            return 0, []
        raise RuntimeError('%s API error code=%s message=%s'
                           % (label, code, top_result.get('MESSAGE')))
    body = payload.get(service)
    if not isinstance(body, list) or not body:
        raise RuntimeError('%s API: %s 본문이 없음 — 응답 키: %s' % (label, service, list(payload)))
    total = 0
    rows: List[dict] = []
    for part in body:
        if not isinstance(part, dict):
            continue
        for head in part.get('head', []) or []:
            if 'list_total_count' in head:
                total = to_int(head['list_total_count']) or 0
            result = head.get('RESULT')
            code = str((result or {}).get('CODE', '')).strip()
            if code == NO_DATA_CODE:          # 머리 안에 '데이터 없음' 을 싣는 경우도 오류가 아니다
                return 0, []
            if result and code not in ('', OK_CODE):
                raise RuntimeError('%s API error code=%s message=%s'
                                   % (label, result.get('CODE'), result.get('MESSAGE')))
        if 'row' in part:
            rows = list(part['row'] or [])
    return total, rows
