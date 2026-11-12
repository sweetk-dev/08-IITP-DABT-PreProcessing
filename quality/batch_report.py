"""갱신·오류 레포트 정확도(지표 7) — (가) 정상 실행 대조.

수집 배치는 실행마다 sys_batch_run_log 에 한 줄을 남기고(ops_alert.record_and_notify), 실패하거나
실패 뒤 정상으로 돌아오면 운영자에게 알림을 보낸다. 이 모듈은 기록된 실행 한 건 한 건이 실제와 맞는지
아래 항목으로 대조한다. 한 건이라도 어긋나면 그 실행은 '불일치'다.

    R1 종료 코드와 상태가 맞는가          SUCCESS=0, PARTIAL=2, 그 밖=1 (ops_alert 의 규칙)
    R2 실패 건수와 상태가 맞는가          SUCCESS 면 실패 0, SUCCESS 가 아니면 실패 1 이상 또는 오류 요약 있음
    R3 건수가 대상 수를 넘지 않는가       ok_cnt + fail_cnt <= targets (targets 가 있을 때)
    R4 실패 목록과 실패 건수가 맞는가     failures 목록 길이 == fail_cnt (목록이 있을 때)
    R5 시간이 앞뒤가 맞는가              finished_at >= started_at
    R6 알림 판단이 맞는가                정기 실행(db, 전체)에서 실패면 알림 처리(SENT/SKIP/FAIL)가 남아 있어야 함
    R7 갱신 시각이 반영됐는가            각 수집원의 가장 최근 성공 실행 뒤에 sys_ext_api_info 의 동기화 시각이
                                       그 실행 시작 이후로 갱신돼 있어야 함(그 수집원이 등록돼 있을 때만)

    정확도(%) = 모든 항목이 맞은 실행 수 / 대조한 실행 수 × 100

(나) 오류 주입 시험은 quality/fault_injection.py — 두 결과를 합쳐 지표 7 을 낸다.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import timedelta
from typing import List, Optional

from sqlalchemy import text

EXIT_BY_STATUS = {"SUCCESS": 0, "PARTIAL": 2}
ALERT_HANDLED = {"SENT", "SKIP", "FAIL"}


@dataclass
class RunCheck:
    run_id: int
    ext_sys: str
    status: str
    started_at: str
    failed_rules: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed_rules

    def as_dict(self):
        d = asdict(self)
        d["ok"] = self.ok
        return d


def _as_list(v):
    if v is None:
        return None
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            x = json.loads(v)
            return x if isinstance(x, list) else None
        except ValueError:
            return None
    return None


def check_runs(conn, days: int = 90, tolerance_sec: int = 120) -> List[RunCheck]:
    """최근 days 일의 실행 기록을 대조한다. tolerance_sec: R7 에서 시각 비교 여유(초)."""
    rows = conn.execute(text("""
        SELECT run_id, ext_sys, run_mode, run_scope, started_at, finished_at, exit_code, status,
               targets, ok_cnt, fail_cnt, failures, error_summary, alert_status
          FROM sys_batch_run_log
         WHERE started_at >= now() - make_interval(days => :d)
         ORDER BY started_at"""), {"d": days}).mappings().all()
    sync = {r[0]: r[1] for r in conn.execute(text(
        "SELECT ext_sys, GREATEST(COALESCE(last_sync_time, latest_sync_time), COALESCE(latest_sync_time, last_sync_time)) "
        "FROM sys_ext_api_info WHERE COALESCE(del_yn, 'N') = 'N'")).fetchall()}
    last_ok = {}
    for r in rows:
        if r["status"] == "SUCCESS" and r["run_mode"] == "db":
            last_ok[r["ext_sys"]] = r["run_id"]
    out: List[RunCheck] = []
    for r in rows:
        bad = []
        st = r["status"] or ""
        if r["exit_code"] is not None and r["exit_code"] != EXIT_BY_STATUS.get(st, 1):
            bad.append("R1")
        fc = r["fail_cnt"] or 0
        if st == "SUCCESS" and fc != 0:
            bad.append("R2")
        if st != "SUCCESS" and fc == 0 and not (r["error_summary"] or "").strip():
            bad.append("R2")
        if r["targets"] is not None and (r["ok_cnt"] or 0) + fc > r["targets"]:
            bad.append("R3")
        fl = _as_list(r["failures"])
        if fl is not None and len(fl) != fc:
            bad.append("R4")
        if r["finished_at"] is not None and r["started_at"] is not None and r["finished_at"] < r["started_at"]:
            bad.append("R5")
        if r["run_mode"] == "db" and (r["run_scope"] or "") != "PARTIAL" and st != "SUCCESS" \
                and (r["alert_status"] or "") not in ALERT_HANDLED:
            bad.append("R6")
        if last_ok.get(r["ext_sys"]) == r["run_id"] and r["ext_sys"] in sync:
            # 수집원이 sys_ext_api_info 에 따로 등록된 경우만 본다(예: GBIS_LOWFLOOR 는 GBIS 등록을 함께 쓴다)
            s = sync.get(r["ext_sys"])
            if s is None or s < r["started_at"] - timedelta(seconds=tolerance_sec):
                bad.append("R7")
        out.append(RunCheck(r["run_id"], r["ext_sys"], st, str(r["started_at"]), bad))
    return out


def summarize(checks: List[RunCheck]) -> dict:
    n = len(checks)
    ok = sum(1 for c in checks if c.ok)
    by_rule = {}
    for c in checks:
        for rule in c.failed_rules:
            by_rule[rule] = by_rule.get(rule, 0) + 1
    return {"runs": n, "matched": ok, "pass_rate": round(100.0 * ok / n, 2) if n else None, "failed_by_rule": by_rule}
