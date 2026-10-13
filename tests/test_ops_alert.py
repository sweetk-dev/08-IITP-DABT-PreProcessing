"""ops_alert — 수집 배치 실행 기록·이상 알림 (v1.17.0).

1. 단위 테스트(DB 없음): 수신 주소 해석, 알림 판단, 메일 내용, 발신 설정 선택, 발신 방식별 전송.
2. 통합 테스트(ITEST_DB_URL 이 있을 때만): sys_batch_run_log 기록 → 실패 알림 → 정상화 알림,
   수동 실행·수신자 없음·알림 꺼짐 처리. 01 v1.9.0 이 적용된 DB 가 필요하다.
   운영 행은 건드리지 않는다 — 전용 ext_sys('ITEST_ALERT')의 기록만 만들고 지우며,
   sys_ops_setting 은 테스트 전 값을 기억했다가 끝나면 되돌린다.
"""
from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import ops_alert  # noqa: E402

ITEST_URL = os.getenv('ITEST_DB_URL') or None

FAIL_CTX = {
    'ext_sys': 'KOSIS', 'status': 'PARTIAL',
    'started_at': datetime(2026, 10, 18, 3, 0, tzinfo=ops_alert.KST),
    'finished_at': datetime(2026, 10, 18, 3, 7, tzinfo=ops_alert.KST),
    'targets': 24, 'ok_cnt': 20, 'fail_cnt': 4,
    'failures': [{'id': 'DT_11761_N007', 'title': '전국 연령별,장애정도별,성별 등록장애인수', 'stage': 'collect',
                  'reason': 'KOSIS data API 오류 응답: err=20(필수 요청변수 누락 — 통계표 구조(분류 단계) 변경 의심, '
                            '조회 주소 objL 점검 필요), errMsg=필수요청변수값이 누락되었습니다.'}],
    'consecutive': 3, 'last_success_at': datetime(2026, 9, 18, 3, 5, tzinfo=ops_alert.KST),
    'console_url': '',
}


class RecipientTests(unittest.TestCase):

    def test_parse_valid_dedup_and_limit(self):
        raw = 'a@x.co.kr, A@x.co.kr; b@x.co.kr  bad@  c@x.co.kr,d@x.co.kr,e@x.co.kr,f@x.co.kr'
        self.assertEqual(ops_alert.parse_recipients(raw),
                         ['a@x.co.kr', 'b@x.co.kr', 'c@x.co.kr', 'd@x.co.kr', 'e@x.co.kr'])

    def test_empty(self):
        self.assertEqual(ops_alert.parse_recipients(''), [])
        self.assertEqual(ops_alert.parse_recipients(None), [])


class DecideAlertTests(unittest.TestCase):

    def test_matrix(self):
        cases = [
            (('file', 'ALL', 'PARTIAL', 'SUCCESS'), None),       # 수동 시험
            (('db', 'PARTIAL', 'ERROR', 'SUCCESS'), None),       # 일부 통계 수동 실행
            (('db', 'ALL', 'PARTIAL', 'SUCCESS'), 'FAIL'),
            (('db', 'ALL', 'ERROR', 'PARTIAL'), 'FAIL'),         # 연속 실패도 매번 알린다
            (('db', 'ALL', 'SUCCESS', 'PARTIAL'), 'RECOVER'),
            (('db', 'ALL', 'SUCCESS', 'SUCCESS'), None),
            (('db', 'ALL', 'SUCCESS', None), None),              # 첫 실행 성공
        ]
        for args, expect in cases:
            with self.subTest(args=args):
                self.assertEqual(ops_alert.decide_alert(*args)[0], expect)


class ComposeMailTests(unittest.TestCase):

    def test_failure_mail_has_summary_list_and_matching_hint(self):
        subject, body = ops_alert.compose_mail('FAIL', FAIL_CTX)
        self.assertEqual(subject, '[IITP 데이터 수집] KOSIS 통계 수집 이상 — 4건 실패 (2026-10-18 03:00)')
        self.assertIn('대상 24건 중 성공 20건, 실패 4건', body)
        self.assertIn('연속 실패: 3회째 (마지막 성공: 2026-09-18 03:05)', body)
        self.assertIn('1. DT_11761_N007 전국 연령별,장애정도별,성별 등록장애인수', body)
        self.assertIn('단계: 수집', body)
        self.assertIn('KOSIS 오류 20·21', body)              # err=20 에 맞는 조치 안내
        self.assertNotIn('인증키 문제', body)                 # 해당 없는 안내는 싣지 않는다
        self.assertIn('기존 데이터가 그대로 남아 있습니다', body)
        self.assertIn('정책 운영 콘솔 [운영] 탭', body)

    def test_total_failure_subject(self):
        ctx = dict(FAIL_CTX, status='ERROR', failures=[], error_summary='KOSIS API 처리 중단(meta): HTTP 503',
                   consecutive=1)
        subject, body = ops_alert.compose_mail('FAIL', ctx)
        self.assertIn('전체 실패', subject)
        self.assertIn('DB 에 바뀐 데이터는 없습니다', body)
        self.assertIn('원천 서버·네트워크 오류', body)
        self.assertNotIn('연속 실패', body)

    def test_recovery_mail(self):
        ctx = dict(FAIL_CTX, status='SUCCESS', prev_fail_since=datetime(2026, 9, 3, 3, 0, tzinfo=ops_alert.KST))
        subject, body = ops_alert.compose_mail('RECOVER', ctx)
        self.assertEqual(subject, '[IITP 데이터 수집] KOSIS 통계 수집 정상화 (2026-10-18 03:00)')
        self.assertIn('직전 실패 시작: 2026-09-03 03:00', body)

    def test_unknown_ext_sys_uses_identifier(self):
        subject, _ = ops_alert.compose_mail('FAIL', dict(FAIL_CTX, ext_sys='NEW_SRC'))
        self.assertIn('NEW_SRC 이상', subject)


class MailConfigTests(unittest.TestCase):

    def _cfg(self, env):
        keys = ['MAIL_BACKEND', 'MAIL_MX_HOST', 'MAIL_FROM', 'GRAPH_TENANT_ID', 'GRAPH_CLIENT_ID',
                'GRAPH_CLIENT_SECRET', 'GRAPH_SENDER', 'SMTP_HOST', 'SMTP_USER', 'SMTP_PASS', 'SMTP_PORT']
        clean = {k: '' for k in keys}
        clean.update(env)
        with patch.dict(os.environ, clean):
            return ops_alert.mail_config()

    def test_mx_selected_when_mx_host_set(self):
        cfg, _ = self._cfg({'MAIL_MX_HOST': 'mx.example.com', 'MAIL_FROM': 'noreply@alert.example.org'})
        self.assertEqual((cfg['backend'], cfg['port'], cfg['helo']), ('mx', 25, 'alert.example.org'))

    def test_graph_missing_keys_reported(self):
        cfg, problem = self._cfg({'MAIL_BACKEND': 'graph', 'GRAPH_TENANT_ID': 't'})
        self.assertIsNone(cfg)
        self.assertIn('GRAPH_CLIENT_ID', problem)

    def test_smtp_default_and_ssl_port(self):
        cfg, _ = self._cfg({'SMTP_HOST': 'h', 'SMTP_USER': 'u@x.kr', 'SMTP_PASS': 'p', 'SMTP_PORT': '465'})
        self.assertEqual((cfg['backend'], cfg['security']), ('smtp', 'ssl'))

    def test_nothing_configured(self):
        cfg, problem = self._cfg({})
        self.assertIsNone(cfg)
        self.assertIn('SMTP 발신 설정 없음', problem)


class SendTests(unittest.TestCase):

    def test_mx_send_uses_starttls_without_login(self):
        server = MagicMock()
        server.has_extn.return_value = True
        server.sendmail.return_value = {}
        cfg = {'backend': 'mx', 'host': 'mx.example.com', 'port': 25, 'from': 'noreply@a.org',
               'from_name': '알림', 'helo': 'a.org'}
        with patch.object(ops_alert.smtplib, 'SMTP', return_value=server) as smtp:
            ops_alert.send_mail(cfg, ['u@x.kr'], '제목', '본문')
        smtp.assert_called_once_with('mx.example.com', 25, timeout=20, local_hostname='a.org')
        server.starttls.assert_called_once()
        server.login.assert_not_called()
        self.assertEqual(server.sendmail.call_args[0][1], ['u@x.kr'])

    def test_mx_refused_recipient_raises(self):
        server = MagicMock()
        server.has_extn.return_value = False
        server.sendmail.return_value = {'other@y.com': (550, b'relay denied')}
        cfg = {'backend': 'mx', 'host': 'h', 'port': 25, 'from': 'n@a.org', 'from_name': 'x', 'helo': 'a.org'}
        with patch.object(ops_alert.smtplib, 'SMTP', return_value=server):
            with self.assertRaises(ops_alert.MailSendError) as cm:
                ops_alert.send_mail(cfg, ['u@x.kr', 'other@y.com'], 's', 'b')
        self.assertIn('other@y.com', str(cm.exception))

    def test_graph_send_flow_and_error_has_no_secret(self):
        cfg = {'backend': 'graph', 'tenant': 'tid', 'client_id': 'cid', 'client_secret': 'S3CRET',
               'from': 'noreply@x.kr', 'from_name': '알림'}
        tok = MagicMock(status_code=200)
        tok.json.return_value = {'access_token': 'AT'}
        ok = MagicMock(status_code=202)
        with patch.object(ops_alert.requests, 'post', side_effect=[tok, ok]) as post:
            ops_alert.send_mail(cfg, ['u@x.kr'], '제목', '본문')
        self.assertIn('/users/noreply@x.kr/sendMail', post.call_args_list[1][0][0])
        payload = post.call_args_list[1][1]['json']
        self.assertEqual(payload['message']['toRecipients'], [{'emailAddress': {'address': 'u@x.kr'}}])
        self.assertFalse(payload['saveToSentItems'])

        bad = MagicMock(status_code=401)
        bad.json.return_value = {'error': 'invalid_client', 'error_description': 'S3CRET wrong'}
        with patch.object(ops_alert.requests, 'post', return_value=bad):
            with self.assertRaises(ops_alert.MailSendError) as cm:
                ops_alert.send_mail(cfg, ['u@x.kr'], 's', 'b')
        self.assertIn('invalid_client', str(cm.exception))
        self.assertNotIn('S3CRET', str(cm.exception))


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL 미설정 — DB 통합 테스트 생략')
class RecordAndNotifyDbTests(unittest.TestCase):
    EXT = 'ITEST_ALERT'

    @classmethod
    def setUpClass(cls):
        from sqlalchemy import create_engine, text
        cls.text = staticmethod(text)
        cls.engine = create_engine(ITEST_URL)
        with cls.engine.begin() as conn:
            cls.saved = dict(conn.execute(text(
                "SELECT setting_key, setting_value FROM sys_ops_setting")).all())

    @classmethod
    def tearDownClass(cls):
        with cls.engine.begin() as conn:
            conn.execute(cls.text("DELETE FROM sys_batch_run_log WHERE ext_sys = :e"), {'e': cls.EXT})
            for k, v in cls.saved.items():
                conn.execute(cls.text("UPDATE sys_ops_setting SET setting_value = :v WHERE setting_key = :k"),
                             {'k': k, 'v': v})
        cls.engine.dispose()

    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute(self.text("DELETE FROM sys_batch_run_log WHERE ext_sys = :e"), {'e': self.EXT})
        self._set('Y', 'ops@example.co.kr')
        self.sent = []

    def _set(self, enabled, emails):
        with self.engine.begin() as conn:
            conn.execute(self.text("UPDATE sys_ops_setting SET setting_value = :v WHERE setting_key = :k"),
                         {'k': ops_alert.SETTING_ENABLED, 'v': enabled})
            conn.execute(self.text("UPDATE sys_ops_setting SET setting_value = :v WHERE setting_key = :k"),
                         {'k': ops_alert.SETTING_EMAILS, 'v': emails})

    def _run(self, status, failures=(), mode='db', scope='ALL', start='2026-10-18 03:00:00'):
        summary = {'start': start, 'end': start, 'ext_sys': self.EXT, 'mode': mode, 'targets': 3,
                   'files_ok': 3, 'db_ok': 3 - len(failures), 'db_fail': len(failures),
                   'status': status, 'error': None}
        env = {'MAIL_BACKEND': 'mx', 'MAIL_MX_HOST': 'mx.example.com', 'MAIL_FROM': 'noreply@a.org'}
        with patch.dict(os.environ, env):
            return ops_alert.record_and_notify(
                summary, list(failures), run_scope=scope, titles={'DT_A': '시험 통계'},
                engine=self.engine, sender=lambda cfg, rcpt, subj, body: self.sent.append((rcpt, subj, body)))

    def _rows(self):
        with self.engine.begin() as conn:
            return conn.execute(self.text(
                "SELECT status, alert_status, alert_detail, fail_cnt, failures FROM sys_batch_run_log"
                " WHERE ext_sys = :e ORDER BY run_id"), {'e': self.EXT}).all()

    def test_failure_then_recovery(self):
        fail = [{'id': 'DT_A', 'stage': 'collect', 'reason': 'err=20'}]
        self.assertEqual(self._run('PARTIAL', fail, start='2026-10-03 03:00:00')[0], ops_alert.ALERT_SENT)
        self.assertEqual(self._run('PARTIAL', fail, start='2026-10-18 03:00:00')[0], ops_alert.ALERT_SENT)
        self.assertIn('연속 실패: 2회째', self.sent[1][2])
        self.assertIn('DT_A 시험 통계', self.sent[1][2])
        self.assertEqual(self._run('SUCCESS', start='2026-11-03 03:00:00')[0], ops_alert.ALERT_SENT)
        self.assertIn('정상화', self.sent[2][1])
        self.assertEqual(self._run('SUCCESS', start='2026-11-18 03:00:00')[0], ops_alert.ALERT_NONE)
        rows = self._rows()
        self.assertEqual([r[0] for r in rows], ['PARTIAL', 'PARTIAL', 'SUCCESS', 'SUCCESS'])
        self.assertEqual(rows[0][3], 1)
        self.assertEqual(rows[0][4], [{'id': 'DT_A', 'stage': 'collect', 'reason': 'err=20'}])

    def test_manual_runs_are_recorded_but_not_alerted(self):
        self.assertEqual(self._run('ERROR', mode='file')[0], ops_alert.ALERT_NONE)
        self.assertEqual(self._run('ERROR', scope='PARTIAL')[0], ops_alert.ALERT_NONE)
        self.assertEqual(self.sent, [])
        self.assertEqual(len(self._rows()), 2)

    def test_disabled_and_no_recipient_are_skipped(self):
        self._set('N', 'ops@example.co.kr')
        self.assertEqual(self._run('ERROR')[0], ops_alert.ALERT_SKIP)
        self._set('Y', '')
        status, detail = self._run('ERROR')
        self.assertEqual(status, ops_alert.ALERT_SKIP)
        self.assertIn('수신 주소 없음', detail)
        self.assertEqual(self.sent, [])

    def test_send_error_is_recorded_not_raised(self):
        def boom(*a):
            raise ops_alert.MailSendError('일부 수신자 거부: x')
        summary = {'start': '2026-10-18 03:00:00', 'end': '2026-10-18 03:01:00', 'ext_sys': self.EXT,
                   'mode': 'db', 'targets': 1, 'files_ok': 0, 'db_ok': 0, 'db_fail': 0,
                   'status': 'ERROR', 'error': 'x'}
        with patch.dict(os.environ, {'MAIL_BACKEND': 'mx', 'MAIL_MX_HOST': 'h', 'MAIL_FROM': 'n@a.org'}):
            status, detail = ops_alert.record_and_notify(summary, [], engine=self.engine, sender=boom)
        self.assertEqual(status, ops_alert.ALERT_FAIL)
        self.assertEqual(self._rows()[-1][1], 'FAIL')


if __name__ == '__main__':
    unittest.main()
