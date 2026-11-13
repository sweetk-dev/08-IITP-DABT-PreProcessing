"""갱신·오류 레포트 정확도(지표 7) — (나) 오류 주입 시험.

검증용 DB(운영 DB 를 복제한 별도 DB)에서 일부러 장애를 만든 뒤 수집 명령을 실행하고, 실행 기록
(sys_batch_run_log)과 알림 판단이 그 장애를 맞게 보고했는지 본다. **운영 DB 에서는 실행하지 않는다** —
접속 DB 이름이 --protect 목록(기본 iitp_db)에 있으면 시작하지 않는다.

시나리오 파일(JSON) 한 항목
    {"name": "KOSIS 인증키 오류",
     "ext_sys": "KOSIS",
     "setup_sql": ["UPDATE sys_ext_api_info SET auth = 'INVALID' WHERE ext_sys = 'KOSIS'"],
     "restore_sql": ["UPDATE sys_ext_api_info SET auth = :saved WHERE ext_sys = 'KOSIS'"],
     "save_sql": {"saved": "SELECT auth FROM sys_ext_api_info WHERE ext_sys = 'KOSIS'"},
     "command": ["python", "main.py", "--mode", "db", "--ext-sys", "KOSIS"],
     "env": {"KOSIS_SAMPLE_LIMIT": "1"},
     "expect": {"status_in": ["ERROR", "PARTIAL"], "hint_any": ["err=10", "err=11"], "alert": "FAIL"}}

- save_sql 로 바꾸기 전 값을 읽어 두고, 끝나면 restore_sql 로 되돌린다(실패해도 되돌린다).
- expect.status_in : 기록된 상태가 이 중 하나여야 한다. 정상 대조 시나리오는 ["SUCCESS"].
- expect.hint_any  : 실패 사유·오류 요약에 이 표지 중 하나가 있어야 한다(운영자 조치 안내가 맞게 붙는지).
- expect.alert     : ops_alert.decide_alert 판단이 이 값이어야 한다("FAIL"·"RECOVER"·null).
- 명령은 접속 주소를 DB_URL 환경변수로 넘겨 검증용 DB 에만 쓰게 한다.

    정확도(%) = 기대대로 보고된 시나리오 수 / 실행한 시나리오 수 × 100
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass, field, asdict
from typing import List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


@dataclass
class ScenarioResult:
    name: str
    ext_sys: str
    ok: bool
    status: Optional[str] = None
    reasons: List[str] = field(default_factory=list)
    rc: Optional[int] = None

    def as_dict(self):
        return asdict(self)


def _decide_alert(run_mode, run_scope, status, prev_status):
    """ops_alert.decide_alert 와 같은 판단. 모듈을 그대로 쓰되, 가져오지 못하면 같은 규칙으로 계산한다."""
    try:
        import ops_alert
        return ops_alert.decide_alert(run_mode, run_scope, status, prev_status)[0]
    except Exception:  # noqa: BLE001
        if run_mode != "db" or run_scope == "PARTIAL":
            return None
        if status != "SUCCESS":
            return "FAIL"
        return "RECOVER" if prev_status and prev_status != "SUCCESS" else None


def run_scenarios(db_url: str, scenarios: List[dict], workdir: str, protect=("iitp_db",),
                  timeout: int = 900) -> List[ScenarioResult]:
    url = make_url(db_url)
    if url.database in set(protect):
        raise SystemExit(f"보호 대상 DB({url.database})에서는 오류 주입 시험을 하지 않는다 — 검증용 DB 를 지정한다")
    eng = create_engine(db_url)
    out: List[ScenarioResult] = []
    for sc in scenarios:
        name, ext = sc["name"], sc["ext_sys"]
        saved = {}
        with eng.begin() as c:
            for k, q in (sc.get("save_sql") or {}).items():
                saved[k] = c.execute(text(q)).scalar()
            prev = c.execute(text("SELECT status FROM sys_batch_run_log WHERE ext_sys = :e ORDER BY started_at DESC LIMIT 1"),
                             {"e": ext}).scalar()
            # 이번 시나리오의 실행 기록은 "시험 직전 마지막 run_id 보다 큰 기록" 으로 찾는다.
            # 시각으로 찾지 않는 이유: 수집 명령은 started_at 을 초 단위로 잘라 기록한다(main.py 의 strftime).
            # DB 의 now() 는 소수점 이하까지 있으므로, 같은 초 안에 시작한 실행은 started_at < now() 가 되어
            # "실행 기록이 남지 않음" 으로 잘못 판정된다(시나리오를 연달아 돌리면 대부분 이 경우다).
            last_id = c.execute(text("SELECT COALESCE(max(run_id), 0) FROM sys_batch_run_log")).scalar()
            for q in sc.get("setup_sql") or []:
                c.execute(text(q), saved)
        env = dict(os.environ, DB_URL=db_url, **{k: str(v) for k, v in (sc.get("env") or {}).items()})
        rc = None
        try:
            p = subprocess.run(sc["command"], cwd=workdir, env=env, capture_output=True, text=True, timeout=timeout)
            rc = p.returncode
        except subprocess.TimeoutExpired:
            rc = -1
        finally:
            with eng.begin() as c:
                for q in sc.get("restore_sql") or []:
                    c.execute(text(q), saved)
        time.sleep(1)
        with eng.connect() as c:
            row = c.execute(text("""
                SELECT run_mode, run_scope, status, failures::text AS failures, error_summary, alert_status
                  FROM sys_batch_run_log WHERE ext_sys = :e AND run_id > :last ORDER BY run_id DESC LIMIT 1"""),
                {"e": ext, "last": last_id}).mappings().first()
        exp = sc.get("expect") or {}
        reasons = []
        if row is None:
            reasons.append("실행 기록이 남지 않음")
            out.append(ScenarioResult(name, ext, False, None, reasons, rc))
            continue
        if exp.get("status_in") and row["status"] not in exp["status_in"]:
            reasons.append(f"상태 {row['status']} (기대 {exp['status_in']})")
        if exp.get("hint_any"):
            blob = (row["failures"] or "") + " " + (row["error_summary"] or "")
            if not any(h in blob for h in exp["hint_any"]):
                reasons.append(f"사유에 표지 없음(기대 {exp['hint_any']})")
        if "alert" in exp:
            got = _decide_alert(row["run_mode"], row["run_scope"], row["status"], prev)
            if got != exp["alert"]:
                reasons.append(f"알림 판단 {got} (기대 {exp['alert']})")
        out.append(ScenarioResult(name, ext, not reasons, row["status"], reasons, rc))
    return out


def load(path: str) -> List[dict]:
    return json.loads(open(path, encoding="utf-8").read())


def summarize(results: List[ScenarioResult]) -> dict:
    n = len(results)
    ok = sum(1 for r in results if r.ok)
    return {"scenarios": n, "matched": ok, "pass_rate": round(100.0 * ok / n, 2) if n else None}
