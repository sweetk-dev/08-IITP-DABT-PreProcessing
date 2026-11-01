"""MobilityCollector.get_json — 끊기거나 깨진 응답 본문 재수신 (v1.23.2).

원천이 200 응답에 중간이 끊긴 JSON 을 실어 보낼 때가 있다(2026-11 경기데이터드림 읍면동 등록장애인).
같은 주소를 다시 받으면 정상이므로 몇 번 다시 받고, 끝내 깨져 있으면 알림이 알아볼 문구로 멈춘다.
네트워크 없이 http_get 을 바꿔 끼워 검증한다.
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import ops_alert  # noqa: E402
from collectors import mobility_base  # noqa: E402
from collectors.gg_dis_reg import GgDisRegCollector  # noqa: E402

GOOD = '{"GGREDISPERST":[{"head":[{"list_total_count":1}]},{"row":[]}]}'
CUT = '{"GGREDISPERST":[{"head":[{"list_total_count":1}]},{"row":[{"SIGUN_NM":"수원시","OLD_NM"'


class _Resp:
    def __init__(self, text):
        self.text = text
        self.content = text.encode('utf-8')

    def json(self):
        return json.loads(self.text)


class _Collector(GgDisRegCollector):
    def __init__(self, bodies):
        super().__init__(api_info={}, stats_src={})
        self.bodies = list(bodies)
        self.calls = 0

    def http_get(self, url, timeout=None, retries=None, backoff_sec=None):
        self.calls += 1
        return _Resp(self.bodies.pop(0))


@patch.object(mobility_base, 'JSON_REFETCH_WAIT_SEC', 0)
class GetJsonRefetchTests(unittest.TestCase):

    def test_good_body_single_call(self):
        c = _Collector([GOOD])
        self.assertIn('GGREDISPERST', c.get_json('http://x/?KEY=k'))
        self.assertEqual(c.calls, 1)

    def test_cut_body_refetched_then_ok(self):
        c = _Collector([CUT, GOOD])
        self.assertIn('GGREDISPERST', c.get_json('http://x/?KEY=k'))
        self.assertEqual(c.calls, 2)

    def test_cut_body_every_time_raises_with_marker(self):
        c = _Collector([CUT] * (mobility_base.JSON_REFETCH + 1))
        with self.assertRaises(RuntimeError) as cm:
            c.get_json('http://x/?KEY=k')
        self.assertEqual(c.calls, mobility_base.JSON_REFETCH + 1)
        self.assertIn('GG_DIS_REG: 원천 응답 JSON 깨짐', str(cm.exception))

    def test_non_json_body_not_refetched(self):
        c = _Collector(['<OpenAPI_ServiceResponse><returnAuthMsg>SERVICE_KEY_IS_NOT_REGISTERED_ERROR'
                        '</returnAuthMsg></OpenAPI_ServiceResponse>'])
        with self.assertRaises(RuntimeError) as cm:
            c.get_json('http://x/?KEY=k')
        self.assertEqual(c.calls, 1)
        self.assertIn('JSON 이 아님', str(cm.exception))
        self.assertNotIn('JSON 깨짐', str(cm.exception))

    def test_non_json_head_masks_key(self):
        c = _Collector(['<error>http://x/?serviceKey=SECRET123&pageNo=1</error>'])
        with self.assertRaises(RuntimeError) as cm:
            c.get_json('http://x/')
        self.assertNotIn('SECRET123', str(cm.exception))


class AlertHintTests(unittest.TestCase):

    def _mail(self, ext_sys, error):
        ctx = {'ext_sys': ext_sys, 'status': 'ERROR', 'targets': 0, 'failures': [], 'error_summary': error,
               'consecutive': 1, 'console_url': ''}
        return ops_alert.compose_mail('FAIL', ctx)[1]

    def test_broken_body_hint_and_no_kosis_runbook(self):
        body = self._mail('GG_DIS_REG', 'GG_DIS_REG: 원천 응답 JSON 깨짐 — 3번 받아도 읽을 수 없음')
        self.assertIn('원천 응답 깨짐', body)
        self.assertIn('--ext-sys <작업>', body)
        self.assertNotIn('kosis-structure-change-runbook', body)

    def test_raw_decoder_message_also_matches(self):
        body = self._mail('GG_DIS_REG', "Expecting ',' delimiter: line 1 column 153620 (char 153619)")
        self.assertIn('원천 응답 깨짐', body)

    def test_kosis_mail_keeps_runbook(self):
        body = self._mail('KOSIS', 'KOSIS API 처리 중단(meta): HTTP 503')
        self.assertIn('kosis-structure-change-runbook', body)
        self.assertNotIn('원천 응답 깨짐', body)


if __name__ == '__main__':
    unittest.main()
