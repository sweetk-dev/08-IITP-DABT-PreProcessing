"""수집 이상 알림 메일 발송 시험 (v1.17.0).

현재 설정(수신자: DB sys_ops_setting, 발신: .env 의 MAIL_*/GRAPH_*/SMTP_*)으로 시험 메일 1통을 보낸다.
실행 기록(sys_batch_run_log)은 남기지 않는다.

사용법 (프로젝트 루트에서):
    python scripts/send_test_alert.py              # DB 의 수신 주소로
    python scripts/send_test_alert.py a@x.co.kr    # 지정한 주소로(DB 설정 무시)

종료 코드: 0 발송함 / 1 설정 없음·발송 실패
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

import config  # noqa: E402,F401 — .env 를 환경변수로 올린다
import ops_alert  # noqa: E402


def main(argv):
    if len(argv) > 1:
        recipients = ops_alert.parse_recipients(','.join(argv[1:]))
    else:
        from db import engine
        if engine is None:
            print('DB_URL 이 없어 수신 주소를 읽을 수 없습니다. 주소를 인자로 주세요.')
            return 1
        with engine.begin() as conn:
            enabled, recipients = ops_alert.load_settings(conn)
        if not enabled:
            print('참고: 알림이 꺼져 있습니다(batch_alert_enabled=N). 시험 메일은 그대로 보냅니다.')
    if not recipients:
        print('수신 주소가 없습니다 — 운영 콘솔 [운영] 탭에서 입력하거나 인자로 주세요.')
        return 1
    cfg, problem = ops_alert.mail_config()
    if cfg is None:
        print(problem)
        return 1
    subject = '[IITP 데이터 수집] 알림 메일 발송 시험'
    body = ('데이터 수집 이상 알림의 발송 경로를 확인하는 시험 메일입니다.\n'
            f'발신 방식: {cfg["backend"]}\n\n'
            '이 메일이 정크 메일함에 들어갔다면 "정크 아님"으로 표시해 두세요. '
            '실제 알림도 같은 발신 주소로 갑니다.\n')
    try:
        ops_alert.send_mail(cfg, recipients, subject, body)
    except Exception as e:  # noqa: BLE001
        print('발송 실패:', ops_alert._safe_error(e))
        return 1
    print(f'발송함: {len(recipients)}명 ({cfg["backend"]})')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
