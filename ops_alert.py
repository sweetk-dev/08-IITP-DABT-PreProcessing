"""데이터 수집 배치 실행 기록 + 이상 알림 메일 (v1.17.0).

배경
  2026-10 KOSIS 통계 4종이 원천 구조 변경으로 수개월 동안 매 실행 실패했지만, 실패는 종료 코드(2)와
  서버 로그에만 남아 아무도 알아채지 못했다. 이 모듈은 실행 1회마다 결과를 DB 에 남기고
  (sys_batch_run_log), 실패하거나 실패 뒤 정상으로 돌아오면 운영자에게 메일을 보낸다.

하는 일 (main.py 의 마지막 단계에서 record_and_notify 1회 호출)
  1. sys_batch_run_log 에 실행 1행 기록 — 운영 콘솔 [운영] 탭이 이 표로 최근 실행 상태를 보여 준다.
  2. 알림 판단
       - 정기 실행(db 모드 · 전체 범위)만 알린다. file 모드(수동 시험)와 일부 통계 실행(PARTIAL)은
         기록만 하고 알리지 않는다 — 운영자가 직접 지켜보는 실행이기 때문이다.
       - 실패(PARTIAL·ERROR)면 매번 보낸다. 쿨다운을 두지 않는다 — 원인을 고친 뒤 재실행했는데 또
         실패한 경우가 "직전에 보냈다"는 이유로 묻히면 안 된다(정기 실행 간격은 하루~보름이다).
       - 실패 뒤 처음 성공하면 정상화 메일을 1회 보낸다.
  3. 수신자·사용 여부는 sys_ops_setting(운영 콘솔에서 수정), 발신 수단은 서버 .env 에서 읽는다.

설계 원칙
  - 이 모듈의 어떤 실패도 배치 결과(종료 코드)에 영향을 주지 않는다. 모든 예외를 잡아 로그로만 남긴다.
  - 01 v1.9.0 마이그레이션 전 DB(표가 없음)에서는 아무것도 하지 않는다.
  - 메일·로그에 SMTP 비밀번호·DB 접속 문자열·내부 주소를 넣지 않는다.

발신 수단 (.env) — MAIL_BACKEND = mx | graph | smtp
  (지정하지 않으면 MAIL_MX_HOST 가 있으면 mx, GRAPH_* 가 있으면 graph, 아니면 smtp)
  mx    : 수신 도메인의 메일 서버(MX)에 직접 건넨다. 외부에서 회사로 오는 일반 메일과 같은 경로라
          계정·비밀번호가 필요 없다. 수신 주소는 그 메일 서버가 받는 도메인이어야 한다.
          MAIL_MX_HOST(예: <회사도메인>.mail.protection.outlook.com), MAIL_FROM(발신 표시 주소),
          MAIL_HELO(선택, 기본 MAIL_FROM 의 도메인). 포트 25, 서버가 지원하면 STARTTLS.
          수신 측에서 정크로 분류될 수 있으므로 처음 받은 메일에서 "정크 아님"/수신 허용을 설정한다.
  graph : Microsoft 365(Exchange Online) 회사 메일. Entra ID 앱(애플리케이션 권한 Mail.Send)으로
          Graph API sendMail 을 호출한다. Exchange Online 은 아이디·비밀번호 SMTP(Basic 인증)를
          단계적으로 폐지하고 있으므로 Microsoft 365 를 쓰면 이 방식을 쓴다.
          GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, GRAPH_SENDER(발신 메일함 주소)
  smtp  : 일반 SMTP 서버. SMTP_HOST, SMTP_PORT(기본 587), SMTP_USER, SMTP_PASS,
          SMTP_FROM(기본 SMTP_USER), SMTP_SECURITY = starttls(기본) | ssl | none
  공통  : MAIL_FROM_NAME(기본 'IITP 데이터 수집 알림'),
          OPS_CONSOLE_URL(선택) — 메일 본문에 넣을 운영 콘솔 주소.
"""
from __future__ import annotations

import logging
import os
import re
import smtplib
import socket

import requests
from datetime import datetime, timedelta, timezone
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid

from sqlalchemy import text

from file_utils import mask_auth_in_text

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

SETTING_ENABLED = 'batch_alert_enabled'
SETTING_EMAILS = 'batch_alert_emails'
MAX_RECIPIENTS = 5
# 실용적인 이메일 형식 검사(RFC 전체가 아니라 오타·구분자 실수를 거르는 수준). 운영 콘솔과 같은 규칙.
EMAIL_RE = re.compile(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$')

# 알림 처리 결과 (sys_batch_run_log.alert_status)
ALERT_SENT = 'SENT'   # 발송함
ALERT_SKIP = 'SKIP'   # 보낼 상황이지만 설정 때문에 보내지 않음(꺼짐·수신자 없음)
ALERT_FAIL = 'FAIL'   # 보내려 했으나 실패(SMTP 설정 없음·전송 오류)
ALERT_NONE = 'NONE'   # 보낼 상황이 아님(정상, 수동 실행)

# 메일 제목·본문에 쓸 작업 이름. 등록되지 않은 ext_sys 는 식별자를 그대로 쓴다.
JOB_LABELS = {
    'KOSIS': 'KOSIS 통계 수집',
    'GBIS': '경기 버스 노선(GBIS) 수집',
    'GBIS_LOWFLOOR': '저상버스 운행 노선 수집',
    'KORAIL_CONV': '철도역 편의시설 수집',
    'KOWSI_FACL': '건물 편의시설 수집',
    'TOUR_BF_API': '무장애 관광지 수집',
    'GG_TOILET': '경기 공중화장실 수집',
    'GG_CULTURE': '경기 문화·체육 시설 수집',
}

_STAGE_LABELS = {'collect': '수집', 'db': '적재', 'batch': '배치'}

# 실패 사유에 들어 있는 표지 → 운영자 조치 안내. 메일에는 이번 실패에 해당하는 것만 싣는다.
_ACTION_HINTS = [
    (('err=20', 'err=21'),
     'KOSIS 오류 20·21(요청변수 누락·오류): KOSIS 가 통계표 분류 단계나 코드를 바꿨을 가능성이 큽니다. '
     'KOSIS 메타(분류 목록)와 sys_stats_src_api_info 의 조회 주소(objL·itmId)를 비교해 주소와 '
     '칸 배치 규칙(intg_col_map)을 고칩니다.'),
    (('구조 변경 감지',),
     '구조 변경 감지: 응답의 분류 단계·항목 수가 칸 배치 규칙과 맞지 않습니다. 조회 주소와 '
     'intg_col_map 을 원천 구조에 맞춰 고칩니다.'),
    (('키가 일치', '기존 연도'),
     '기존 데이터와 모양이 다름: 원천의 분류 체계·제공 기간이 바뀌었는지 KOSIS 화면에서 확인합니다. '
     '바뀐 것이 맞으면 KOSIS_ACCEPT_RESHAPE=<통계표ID> 를 붙여 해당 통계만 수동 실행합니다.'),
    (('메타에 이름이 없는',),
     '메타 이름 누락: 데이터에 나온 코드가 KOSIS 메타에 없습니다. 메타 응답과 칸 배치 규칙(item_obj)을 점검합니다. '
     '그대로 적재하면 공개 API 조회가 오류로 끝나므로 적재를 멈췄습니다.'),
    (('err=10', 'err=11'),
     'KOSIS 인증키 문제: 인증키를 재발급해 sys_ext_api_info.auth 를 갱신합니다.'),
    (('HTTP 5', 'ConnectionError', 'Timeout', 'err=50'),
     '원천 서버·네트워크 오류: 일시 장애일 수 있습니다. 다음 정기 실행 결과를 확인하고, 반복되면 원천 상태를 확인합니다.'),
    (('err=31',),
     'KOSIS 4만 셀 초과: 1년 단위로 나눠도 셀 수가 많습니다. 조회 주소의 분류 범위를 줄여야 합니다.'),
]


def now_kst():
    return datetime.now(KST)


# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
def parse_recipients(raw):
    """쉼표·세미콜론·공백으로 구분된 주소 문자열 → 형식이 맞는 주소 목록(중복 제거, 최대 5개).

    형식이 틀린 주소는 버리고 로그만 남긴다(나머지 주소로는 발송한다).
    """
    out = []
    for part in re.split(r'[,;\s]+', str(raw or '')):
        addr = part.strip()
        if not addr:
            continue
        if not EMAIL_RE.match(addr):
            logger.warning("알림 수신 주소 형식 오류로 제외: %s", addr)
            continue
        if addr.lower() not in [a.lower() for a in out]:
            out.append(addr)
    return out[:MAX_RECIPIENTS]


def load_settings(conn):
    """sys_ops_setting 에서 (사용 여부, 수신자 목록)을 읽는다. 값이 없으면 (True, [])."""
    rows = conn.execute(text(
        "SELECT setting_key, setting_value FROM sys_ops_setting WHERE setting_key IN (:k1, :k2)"),
        {'k1': SETTING_ENABLED, 'k2': SETTING_EMAILS}).all()
    values = {r[0]: r[1] for r in rows}
    enabled = str(values.get(SETTING_ENABLED, 'Y')).strip().upper() != 'N'
    return enabled, parse_recipients(values.get(SETTING_EMAILS, ''))


_SECRET_ENV = ('SMTP_PASS', 'GRAPH_CLIENT_SECRET')


def mail_config():
    """.env 의 발신 설정. 선택된 방식의 필수값이 하나라도 없으면 (None, 빠진 항목 안내)."""
    from_name = (os.getenv('MAIL_FROM_NAME', '') or os.getenv('SMTP_FROM_NAME', '')).strip() \
        or 'IITP 데이터 수집 알림'
    graph_keys = ('GRAPH_TENANT_ID', 'GRAPH_CLIENT_ID', 'GRAPH_CLIENT_SECRET', 'GRAPH_SENDER')
    backend = os.getenv('MAIL_BACKEND', '').strip().lower()
    if not backend:
        if os.getenv('MAIL_MX_HOST', '').strip():
            backend = 'mx'
        elif any(os.getenv(k) for k in graph_keys):
            backend = 'graph'
        else:
            backend = 'smtp'
    if backend == 'mx':
        host = os.getenv('MAIL_MX_HOST', '').strip()
        sender = os.getenv('MAIL_FROM', '').strip()
        if not (host and sender and EMAIL_RE.match(sender)):
            return None, 'MX 발신 설정 없음(.env MAIL_MX_HOST·MAIL_FROM)'
        return {
            'backend': 'mx', 'host': host, 'port': int(os.getenv('MAIL_MX_PORT', '25') or 25),
            'from': sender, 'from_name': from_name,
            'helo': os.getenv('MAIL_HELO', '').strip() or sender.split('@')[-1],
        }, ''
    if backend == 'graph':
        missing = [k for k in graph_keys if not os.getenv(k, '').strip()]
        if missing:
            return None, f"Graph 발신 설정 없음(.env {', '.join(missing)})"
        return {
            'backend': 'graph',
            'tenant': os.getenv('GRAPH_TENANT_ID').strip(),
            'client_id': os.getenv('GRAPH_CLIENT_ID').strip(),
            'client_secret': os.getenv('GRAPH_CLIENT_SECRET').strip(),
            'from': os.getenv('GRAPH_SENDER').strip(),
            'from_name': from_name,
        }, ''
    host = os.getenv('SMTP_HOST', '').strip()
    user = os.getenv('SMTP_USER', '').strip()
    password = os.getenv('SMTP_PASS', '')
    if not (host and user and password):
        return None, 'SMTP 발신 설정 없음(.env SMTP_HOST·SMTP_USER·SMTP_PASS)'
    port = int(os.getenv('SMTP_PORT', '587') or 587)
    security = (os.getenv('SMTP_SECURITY', '') or ('ssl' if port == 465 else 'starttls')).strip().lower()
    return {
        'backend': 'smtp',
        'host': host, 'port': port, 'user': user, 'password': password,
        'from': os.getenv('SMTP_FROM', '').strip() or user,
        'from_name': from_name,
        'security': security,
    }, ''


# ---------------------------------------------------------------------------
# 판단
# ---------------------------------------------------------------------------
def decide_alert(run_mode, run_scope, status, prev_status):
    """알림 종류를 정한다. 반환: ('FAIL'|'RECOVER'|None, 사유 문구)."""
    if run_mode != 'db':
        return None, 'file 모드(수동 시험) — 알림 대상 아님'
    if run_scope == 'PARTIAL':
        return None, '일부 통계 수동 실행(PARTIAL) — 알림 대상 아님'
    if status != 'SUCCESS':
        return 'FAIL', ''
    if prev_status and prev_status != 'SUCCESS':
        return 'RECOVER', ''
    return None, '정상'


def matching_action_hints(failures, error_summary=''):
    """실패 사유에 해당하는 조치 안내 목록(중복 없이, 정의 순서대로)."""
    blob = ' '.join([str(f.get('reason') or '') for f in failures] + [str(error_summary or '')])
    return [hint for keys, hint in _ACTION_HINTS if any(k in blob for k in keys)]


# ---------------------------------------------------------------------------
# 메일 내용
# ---------------------------------------------------------------------------
def _fmt(dt):
    if dt is None:
        return '-'
    if dt.tzinfo is not None:
        dt = dt.astimezone(KST)
    return dt.strftime('%Y-%m-%d %H:%M')


def compose_mail(kind, ctx):
    """알림 메일 (제목, 본문). DB·네트워크를 쓰지 않는 순수 함수.

    ctx 키: ext_sys, status, started_at, finished_at, targets, ok_cnt, fail_cnt, failures(list of dict
    id/title/stage/reason), error_summary, consecutive(int), last_success_at(datetime|None), console_url.
    """
    ext_sys = ctx.get('ext_sys') or '-'
    job = JOB_LABELS.get(ext_sys, ext_sys)
    when = _fmt(ctx.get('started_at'))
    console = (ctx.get('console_url') or '').strip()
    lines = []

    if kind == 'RECOVER':
        subject = f"[IITP 데이터 수집] {job} 정상화 ({when})"
        lines += [
            f"{job} 배치가 다시 정상으로 끝났습니다.",
            "",
            "■ 요약",
            f"- 작업: {job} ({ext_sys})",
            f"- 실행: {when} ~ {_fmt(ctx.get('finished_at'))} (KST)",
            f"- 결과: 성공 — 대상 {ctx.get('targets', 0)}건 모두 처리",
        ]
        if ctx.get('prev_fail_since'):
            lines.append(f"- 직전 실패 시작: {_fmt(ctx.get('prev_fail_since'))}")
    else:
        status = ctx.get('status')
        if status == 'ERROR':
            result = f"전체 실패 — 대상 {ctx.get('targets', 0)}건 중 처리된 건 없음"
            title_tail = '전체 실패'
            impact = '이번 실행에서 DB 에 바뀐 데이터는 없습니다(기존 데이터 유지).'
        else:
            result = (f"일부 실패 — 대상 {ctx.get('targets', 0)}건 중 성공 {ctx.get('ok_cnt', 0)}건, "
                      f"실패 {ctx.get('fail_cnt', 0)}건")
            title_tail = f"{ctx.get('fail_cnt', 0)}건 실패"
            impact = '실패한 항목은 적재 전 단계에서 멈춰 기존 데이터가 그대로 남아 있습니다. 성공한 항목만 갱신됐습니다.'
        subject = f"[IITP 데이터 수집] {job} 이상 — {title_tail} ({when})"
        lines += [
            f"{job} 배치에서 이상이 확인되어 알려 드립니다.",
            "",
            "■ 요약",
            f"- 작업: {job} ({ext_sys})",
            f"- 실행: {when} ~ {_fmt(ctx.get('finished_at'))} (KST)",
            f"- 결과: {result}",
        ]
        consecutive = int(ctx.get('consecutive') or 1)
        if consecutive > 1:
            last_ok = ctx.get('last_success_at')
            tail = f" (마지막 성공: {_fmt(last_ok)})" if last_ok else " (기록상 성공 이력 없음)"
            lines.append(f"- 연속 실패: {consecutive}회째{tail}")
        lines.append(f"- 데이터 영향: {impact}")

        failures = ctx.get('failures') or []
        if failures:
            lines += ["", "■ 실패 목록"]
            for i, f in enumerate(failures[:30], 1):
                title = f" {f['title']}" if f.get('title') else ''
                stage = _STAGE_LABELS.get(f.get('stage'), f.get('stage') or '-')
                lines.append(f"{i}. {f.get('id')}{title}")
                lines.append(f"   단계: {stage} / 사유: {f.get('reason') or '-'}")
            if len(failures) > 30:
                lines.append(f"... 외 {len(failures) - 30}건 (운영 콘솔에서 전체 확인)")
        elif ctx.get('error_summary'):
            lines += ["", "■ 오류", f"- {ctx.get('error_summary')}"]

        hints = matching_action_hints(failures, ctx.get('error_summary'))
        lines += ["", "■ 권장 조치"]
        for h in hints:
            lines.append(f"- {h}")
        lines.append("- 상세 절차: 08-IITP-DABT-PreProcessing 문서 docs/operations/kosis-structure-change-runbook.md")

    lines += ["", "■ 확인 위치"]
    lines.append("- 정책 운영 콘솔 [운영] 탭 › 데이터 수집" + (f": {console}" if console else ""))
    lines.append("- 수집 서버 로그: logs/run_summary.log, logs/<날짜>.log")
    lines += ["", "이 메일은 데이터 수집 배치가 자동으로 보냈습니다. 수신 주소와 알림 사용 여부는 "
                  "정책 운영 콘솔 [운영] 탭에서 바꿀 수 있습니다."]
    return subject, "\n".join(lines) + "\n"


class MailSendError(RuntimeError):
    """메일 전송 실패(Graph 응답 오류 등). 메시지에 비밀값을 담지 않는다."""


def send_mail(cfg, recipients, subject, body, timeout=20):
    """설정된 방식(graph/smtp)으로 평문 메일 1통을 보낸다. 실패하면 예외를 올린다(호출자가 기록)."""
    if cfg.get('backend') == 'graph':
        return send_mail_graph(cfg, recipients, subject, body, timeout=timeout)
    if cfg.get('backend') == 'mx':
        return send_mail_mx(cfg, recipients, subject, body, timeout=timeout)
    return send_mail_smtp(cfg, recipients, subject, body, timeout=timeout)


def _build_message(cfg, recipients, subject, body):
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = formataddr((cfg['from_name'], cfg['from']), charset='utf-8')
    msg['To'] = ', '.join(recipients)
    msg['Date'] = formatdate(localtime=True)
    msg['Message-ID'] = make_msgid(domain=cfg['from'].split('@')[-1])
    return msg


def send_mail_mx(cfg, recipients, subject, body, timeout=20):
    """수신 메일 서버(MX)에 인증 없이 직접 건넨다. 서버가 지원하면 STARTTLS 로 암호화한다.

    일부 수신자가 거부되면(다른 도메인 주소 등) MailSendError 로 알린다 — 나머지 수신자에게는 이미 전달됐다.
    """
    msg = _build_message(cfg, recipients, subject, body)
    server = smtplib.SMTP(cfg['host'], cfg['port'], timeout=timeout, local_hostname=cfg['helo'])
    try:
        server.ehlo()
        if server.has_extn('starttls'):
            server.starttls()
            server.ehlo()
        refused = server.sendmail(cfg['from'], recipients, msg.as_string())
    finally:
        try:
            server.quit()
        except Exception:  # noqa: BLE001
            pass
    if refused:
        raise MailSendError(f"일부 수신자 거부: {', '.join(sorted(refused))}")


def send_mail_graph(cfg, recipients, subject, body, timeout=20):
    """Microsoft Graph sendMail(애플리케이션 권한 Mail.Send)로 보낸다.

    1) 클라이언트 자격 증명으로 토큰 발급 → 2) /users/{발신 메일함}/sendMail 호출(성공 202).
    보낸편지함에는 남기지 않는다(saveToSentItems=false) — 알림 메일함이 쌓이지 않게.
    오류 메시지에는 HTTP 상태와 Microsoft 오류 코드만 싣는다(비밀값·토큰 제외).
    """
    tok = requests.post(
        f"https://login.microsoftonline.com/{cfg['tenant']}/oauth2/v2.0/token",
        data={'client_id': cfg['client_id'], 'client_secret': cfg['client_secret'],
              'scope': 'https://graph.microsoft.com/.default', 'grant_type': 'client_credentials'},
        timeout=timeout)
    if tok.status_code != 200:
        raise MailSendError(f"Graph 토큰 발급 실패 HTTP {tok.status_code} {_ms_error_code(tok)}")
    access = tok.json().get('access_token')
    payload = {
        'message': {
            'subject': subject,
            'body': {'contentType': 'Text', 'content': body},
            'toRecipients': [{'emailAddress': {'address': a}} for a in recipients],
        },
        'saveToSentItems': False,
    }
    r = requests.post(f"https://graph.microsoft.com/v1.0/users/{cfg['from']}/sendMail",
                      headers={'Authorization': f'Bearer {access}'}, json=payload, timeout=timeout)
    if r.status_code != 202:
        raise MailSendError(f"Graph sendMail 실패 HTTP {r.status_code} {_ms_error_code(r)}")


def _ms_error_code(resp):
    """Microsoft 오류 응답에서 코드만 뽑는다(예: invalid_client, ErrorAccessDenied)."""
    try:
        j = resp.json()
    except ValueError:
        return ''
    err = j.get('error')
    if isinstance(err, dict):
        return str(err.get('code') or '')
    return str(err or '')


def send_mail_smtp(cfg, recipients, subject, body, timeout=20):
    """SMTP 로 평문 메일 1통을 보낸다. 실패하면 예외를 그대로 올린다(호출자가 기록)."""
    msg = _build_message(cfg, recipients, subject, body)
    if cfg['security'] == 'ssl':
        server = smtplib.SMTP_SSL(cfg['host'], cfg['port'], timeout=timeout)
    else:
        server = smtplib.SMTP(cfg['host'], cfg['port'], timeout=timeout)
    try:
        server.ehlo()
        if cfg['security'] == 'starttls':
            server.starttls()
            server.ehlo()
        server.login(cfg['user'], cfg['password'])
        server.sendmail(cfg['from'], recipients, msg.as_string())
    finally:
        try:
            server.quit()
        except Exception:  # noqa: BLE001 — 종료 실패는 결과에 영향 없음
            pass


def _safe_error(e):
    """예외를 기록용 한 줄로. 비밀번호가 메시지에 섞여 들어가는 경우를 막는다."""
    msg = f"{type(e).__name__}: {e}"
    for key in _SECRET_ENV:
        secret = os.getenv(key, '')
        if secret:
            msg = msg.replace(secret, '***')
    return mask_auth_in_text(msg).replace('\n', ' ')[:300]


# ---------------------------------------------------------------------------
# 진입점
# ---------------------------------------------------------------------------
def _tables_ready(conn):
    r = conn.execute(text(
        "SELECT to_regclass('public.sys_batch_run_log') IS NOT NULL, "
        "to_regclass('public.sys_ops_setting') IS NOT NULL")).one()
    return bool(r[0]) and bool(r[1])


def _history(conn, ext_sys):
    """같은 ext_sys 의 정기 실행(db·ALL) 이력에서 (직전 상태, 연속 실패 수, 마지막 성공 시각, 실패 시작 시각)."""
    rows = conn.execute(text("""
        SELECT status, started_at FROM sys_batch_run_log
         WHERE ext_sys = :ext_sys AND run_mode = 'db' AND COALESCE(run_scope, 'ALL') = 'ALL'
         ORDER BY started_at DESC, run_id DESC LIMIT 200"""), {'ext_sys': ext_sys}).all()
    prev_status = rows[0][0] if rows else None
    fails, fail_since, last_ok = 0, None, None
    for status, started in rows:
        if status == 'SUCCESS':
            last_ok = started
            break
        fails += 1
        fail_since = started
    return prev_status, fails, last_ok, fail_since


def record_and_notify(summary, failures, *, run_scope=None, titles=None, engine=None,
                      sender=None, now=None):
    """실행 1회를 기록하고 필요하면 알림 메일을 보낸다. 예외를 밖으로 내지 않는다.

    인자:
        summary   : main.py 의 실행 요약 dict(start/end 문자열, ext_sys, mode, targets, files_ok,
                    db_ok, db_fail, status, error).
        failures  : [{'id', 'stage'('collect'|'db'), 'reason'}] — 통계·소스 단위 실패 목록.
        run_scope : 'ALL' | 'PARTIAL' | None(=ALL).
        titles    : {통계표 ID: 통계명} — 메일에 이름을 함께 싣는다.
        engine    : SQLAlchemy 엔진(테스트 주입용). None 이면 db.engine.
        sender    : 메일 전송 함수(테스트 주입용). None 이면 send_mail.
    반환:
        (alert_status, alert_detail) — 기록하지 못했으면 (None, 사유).
    """
    try:
        if engine is None:
            from db import engine as default_engine
            engine = default_engine
        if engine is None:
            return None, 'DB 엔진 없음'
        ext_sys = summary.get('ext_sys') or 'UNKNOWN'
        run_mode = summary.get('mode')
        scope = (run_scope or 'ALL').upper()
        status = summary.get('status') or 'ERROR'
        started = _parse_local(summary.get('start'))
        finished = _parse_local(summary.get('end'))
        titles = titles or {}
        fail_list = [dict(f, title=titles.get(f.get('id'), '')) for f in (failures or [])]
        targets = int(summary.get('targets') or 0)
        ok_cnt = int((summary.get('db_ok') if run_mode == 'db' else summary.get('files_ok')) or 0)
        fail_cnt = len(fail_list) or (targets - ok_cnt if status != 'SUCCESS' else 0)
        exit_code = {'SUCCESS': 0, 'PARTIAL': 2}.get(status, 1)

        with engine.begin() as conn:
            if not _tables_ready(conn):
                logger.warning("sys_batch_run_log/sys_ops_setting 없음(01 v1.9.0 미적용) — 실행 기록·알림 생략")
                return None, '기록 표 없음'
            prev_status, prev_fails, last_ok, fail_since = _history(conn, ext_sys)
            import json as _json
            run_id = conn.execute(text("""
                INSERT INTO sys_batch_run_log (ext_sys, run_mode, run_scope, started_at, finished_at,
                    exit_code, status, targets, ok_cnt, fail_cnt, failures, error_summary)
                VALUES (:ext_sys, :run_mode, :scope, :started, :finished, :exit_code, :status,
                    :targets, :ok_cnt, :fail_cnt, CAST(:failures AS jsonb), :error_summary)
                RETURNING run_id"""), {
                'ext_sys': ext_sys, 'run_mode': run_mode, 'scope': scope,
                'started': started, 'finished': finished, 'exit_code': exit_code,
                'status': status, 'targets': targets, 'ok_cnt': ok_cnt, 'fail_cnt': max(fail_cnt, 0),
                'failures': _json.dumps(
                    [{k: f.get(k) for k in ('id', 'stage', 'reason')} for f in fail_list], ensure_ascii=False),
                'error_summary': (summary.get('error') or '')[:1000] or None,
            }).scalar_one()
            enabled, recipients = load_settings(conn)

        kind, why = decide_alert(run_mode, scope, status, prev_status)
        if kind is None:
            return _finish(engine, run_id, ALERT_NONE, why)
        if not enabled:
            return _finish(engine, run_id, ALERT_SKIP, '알림 꺼짐(batch_alert_enabled=N)')
        if not recipients:
            return _finish(engine, run_id, ALERT_SKIP, '수신 주소 없음 — 운영 콘솔에서 입력 필요')
        cfg, cfg_problem = mail_config()
        if cfg is None:
            return _finish(engine, run_id, ALERT_FAIL, cfg_problem)
        ctx = {
            'ext_sys': ext_sys, 'status': status, 'started_at': started, 'finished_at': finished,
            'targets': targets, 'ok_cnt': ok_cnt, 'fail_cnt': max(fail_cnt, 0), 'failures': fail_list,
            'error_summary': summary.get('error'),
            'consecutive': prev_fails + 1 if kind == 'FAIL' else 0,
            'last_success_at': last_ok, 'prev_fail_since': fail_since,
            'console_url': os.getenv('OPS_CONSOLE_URL', ''),
        }
        subject, body = compose_mail(kind, ctx)
        try:
            (sender or send_mail)(cfg, recipients, subject, body)
        except (smtplib.SMTPException, OSError, socket.timeout, requests.RequestException, MailSendError) as e:
            return _finish(engine, run_id, ALERT_FAIL, f"전송 실패: {_safe_error(e)}")
        label = '정상화' if kind == 'RECOVER' else '이상'
        logger.info("수집 %s 알림 발송: %d명", label, len(recipients))
        return _finish(engine, run_id, ALERT_SENT, f"{label} 알림 {len(recipients)}명")
    except Exception as e:  # noqa: BLE001 — 기록·알림 실패가 배치 결과를 바꾸면 안 된다
        logger.error("실행 기록·알림 처리 실패(배치 결과에는 영향 없음): %s", _safe_error(e))
        return None, _safe_error(e)


def _finish(engine, run_id, status, detail):
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE sys_batch_run_log SET alert_status = :s, alert_detail = :d WHERE run_id = :id"),
            {'s': status, 'd': (detail or '')[:500], 'id': run_id})
    if status == ALERT_FAIL:
        logger.error("수집 알림 실패: %s", detail)
    else:
        logger.info("수집 알림: %s — %s", status, detail)
    return status, detail


def _parse_local(value):
    """main.py 요약의 'YYYY-MM-DD HH:MM:SS'(서버 로컬 시각) → aware datetime. 실패하면 지금(KST)."""
    try:
        return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S').astimezone()
    except (TypeError, ValueError):
        return now_kst()
