"""kosis_api 단위 테스트 — KOSIS 오류 본문 처리와 인증키 마스킹.

네트워크를 쓰지 않는다. ``requests.get`` 을 가짜 응답으로 바꿔 끼운다.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import traceback
import unittest
from unittest.mock import patch

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import kosis_api  # noqa: E402

SECRET = 'SECRETKEY123'
API_INFO = {'ext_url': 'https://kosis.example', 'auth': SECRET}
STATS_SRC = {
    'stat_tbl_id': 'DT_X',
    'use_base_url_yn': 'Y',
    'api_data_url': json.dumps({'url': '/data?apiKey={API_AUTH_KEY}&startPrdDe={from}&endPrdDe={to}',
                                'format': 'json'}),
    'api_meta_url': json.dumps({'url': '/meta?apiKey={API_AUTH_KEY}', 'format': 'json'}),
    'api_latest_chn_dt_url': json.dumps({'url': '/latest?apiKey={API_AUTH_KEY}', 'format': 'json'}),
}
DATA_INFO = {'collect_start_dt': '2020', 'collect_end_dt': '2023'}


class _Resp:
    """requests.Response 대역 — status_code / json() / text 만 흉내 낸다."""

    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload, ensure_ascii=False)

    def json(self):
        return self._payload


class KosisErrorBodyTests(unittest.TestCase):
    """HTTP 200 + {"err": ...} 본문은 수집 실패다(err=31 은 기간 분할 재시도)."""

    def test_error_dict_raises_with_code_and_message(self):
        body = {'err': '20', 'errMsg': '필수요청변수값이 누락되었습니다.'}
        with patch.object(kosis_api.requests, 'get', return_value=_Resp(body)):
            with self.assertRaises(kosis_api.KosisApiError) as cm:
                kosis_api.fetch_kosis_data(API_INFO, STATS_SRC, DATA_INFO)
        message = str(cm.exception)
        self.assertEqual(cm.exception.err_code, '20')
        self.assertIn('err=20', message)
        self.assertIn('필수요청변수값', message)
        self.assertNotIn(SECRET, message)

    def test_normal_list_is_returned_unchanged(self):
        rows = [{'PRD_DE': '2020', 'DT': '1'}]
        with patch.object(kosis_api.requests, 'get', return_value=_Resp(rows)):
            self.assertEqual(kosis_api.fetch_kosis_data(API_INFO, STATS_SRC, DATA_INFO), rows)

    def test_err_31_still_splits_period(self):
        """건수 초과(31)는 예외가 아니라 기간을 나눠 다시 호출한다."""
        calls = []

        def fake_get(url, timeout=None):
            calls.append(url)
            if 'startPrdDe=2020&endPrdDe=2023' in url:
                return _Resp({'err': '31', 'errMsg': '조회결과 초과'})
            return _Resp([{'PRD_DE': url.split('startPrdDe=')[1][:4], 'DT': '1'}])

        with patch.object(kosis_api.requests, 'get', side_effect=fake_get):
            rows = kosis_api.fetch_kosis_data(API_INFO, STATS_SRC, DATA_INFO)
        self.assertEqual(len(calls), 3)          # 전체 1회 + 반씩 2회
        self.assertEqual([r['PRD_DE'] for r in rows], ['2020', '2022'])

    def test_error_in_one_split_range_raises_instead_of_mixing_into_rows(self):
        """분할 구간 하나가 오류를 돌려주면 정상 행 사이에 오류 dict 를 끼워 반환하지 않는다."""

        def fake_get(url, timeout=None):
            if 'startPrdDe=2020&endPrdDe=2023' in url:
                return _Resp({'err': '31', 'errMsg': '조회결과 초과'})
            if 'startPrdDe=2022' in url:
                return _Resp({'err': '30', 'errMsg': '데이터가 존재하지 않습니다.'})
            return _Resp([{'PRD_DE': '2020', 'DT': '1'}])

        with patch.object(kosis_api.requests, 'get', side_effect=fake_get):
            with self.assertRaises(kosis_api.KosisApiError) as cm:
                kosis_api.fetch_kosis_data(API_INFO, STATS_SRC, DATA_INFO)
        self.assertEqual(cm.exception.err_code, '30')

    def test_err_31_at_one_year_range_fails_instead_of_returning_error_dict(self):
        """1년 단위까지 나눠도 건수 초과(31)면 실패한다 — 오류 dict 가 행 목록에 담겨 반환되지 않는다."""
        calls = []

        def fake_get(url, timeout=None):
            calls.append(url)
            if 'startPrdDe=2020&endPrdDe=2020' in url:
                return _Resp([{'PRD_DE': '2020', 'DT': '1'}])
            return _Resp({'err': '31', 'errMsg': '조회결과 초과'})

        with patch.object(kosis_api.requests, 'get', side_effect=fake_get), patch('builtins.print'):
            with self.assertRaises(RuntimeError):
                kosis_api.fetch_kosis_data(API_INFO, STATS_SRC, DATA_INFO)
        # 2020~2023 → 2020~2021 → 2020(정상), 2021(31) 에서 중단. 뒤 구간은 호출하지 않는다.
        self.assertTrue(any('startPrdDe=2021&endPrdDe=2021' in u for u in calls))

    def test_latest_and_meta_error_dict_raise(self):
        body = {'err': '11', 'errMsg': '인증키 기간만료'}
        with patch.object(kosis_api.requests, 'get', return_value=_Resp(body)):
            with self.assertRaises(kosis_api.KosisApiError):
                kosis_api.fetch_kosis_latest(API_INFO, STATS_SRC, DATA_INFO)
            with self.assertRaises(kosis_api.KosisApiError):
                kosis_api.fetch_kosis_meta(API_INFO, STATS_SRC, DATA_INFO)


class KosisAuthMaskingTests(unittest.TestCase):
    """요청 예외 문자열(URL 포함)이 로그·예외·트레이스백에 인증키를 남기지 않는다."""

    def _connection_error(self, url, timeout=None):
        raise requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='kosis.example', port=443): Max retries exceeded with url: "
            + url.split('kosis.example')[1] + " (Caused by NewConnectionError('refused'))")

    def test_request_exception_does_not_leak_key(self):
        for fetch in (kosis_api.fetch_kosis_data, kosis_api.fetch_kosis_meta, kosis_api.fetch_kosis_latest):
            with self.subTest(fetch=fetch.__name__):
                with patch.object(kosis_api.requests, 'get', side_effect=self._connection_error), \
                        patch('builtins.print') as printed, \
                        self.assertLogs(level=logging.ERROR) as logs:
                    try:
                        fetch(API_INFO, STATS_SRC, DATA_INFO)
                        self.fail('예외가 올라와야 한다')
                    except RuntimeError:
                        # 상위(main.save_single_file)는 exc_info=True 로 트레이스백을 남긴다.
                        # 원인 예외가 연결돼 있으면 여기에 원본 URL 이 출력된다.
                        rendered = traceback.format_exc()
                log_text = '\n'.join(
                    logging.Formatter('%(message)s').format(r) for r in logs.records)
                printed_text = ' '.join(str(a) for call in printed.call_args_list for a in call.args)
                for where, text in (('log', log_text), ('print', printed_text), ('traceback', rendered)):
                    self.assertNotIn(SECRET, text, where)
                self.assertIn('apiKey=***', log_text)
                self.assertIn('ConnectionError', log_text)

    def test_mask_covers_three_parameter_names(self):
        self.assertEqual(kosis_api.mask_auth_in_url('https://h/p?apiKey=abc&x=1'), 'https://h/p?apiKey=***&x=1')
        self.assertEqual(kosis_api.mask_auth_in_url('https://h/p?x=1&serviceKey=a%2Bb=='),
                         'https://h/p?x=1&serviceKey=***')
        self.assertEqual(kosis_api.mask_auth_in_url('https://h/p?KEY=abc&Type=json'), 'https://h/p?KEY=***&Type=json')
        self.assertIsNone(kosis_api.mask_auth_in_url(None))


if __name__ == '__main__':
    unittest.main()
