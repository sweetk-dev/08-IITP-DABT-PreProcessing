"""quality 모듈 단위 테스트 — DB·네트워크 없이 규칙 해석·집계·파일 분포·안전장치를 확인한다."""
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quality import spec, checks, files, batch_report, fault_injection  # noqa: E402


def _write_spec(tmp_path, rows):
    p = tmp_path / "spec.csv"
    with open(p, "w", encoding="utf-8-sig", newline="") as f:
        f.write("seq,table,column,type,key,not_null,range_min,range_max,rule_format,rule_date,unique,desc\n")
        for r in rows:
            f.write(r + "\n")
    return p


def test_spec_load_and_rules(tmp_path):
    p = _write_spec(tmp_path, [
        '1,t,id,숫자,PK,O,,,,,O,식별자',
        '2,t,prd_de,숫자,,O,"2,019",2024,,,,연도',
        '3,t,c1,문자,,O,,,"A[0-9]{2,4}",,,분류',
        '4,t,ymd,문자,,,,,,YYYY-MM-DD,,날짜',
        ',,,,,,,,,,,',
    ])
    s = spec.load(p)
    assert len(s) == 4                                  # 빈 줄은 건너뛴다
    assert s[0].unique and s[0].not_null and s[0].key == "PK"
    assert (s[1].range_min, s[1].range_max) == (2019.0, 2024.0)   # 천 단위 쉼표 허용
    assert s[2].pattern_regex == "^(A[0-9]{2,4})$"     # 값 전체 일치로 감싼다
    assert s[3].date_regex.startswith("^(") and s[3].date_regex.endswith(")$")
    assert s[0].type_class == "number" and s[2].type_class == "text"


def test_spec_save_roundtrip_keeps_extra_columns(tmp_path):
    s = [spec.ColumnSpec("t", "c", "문자", extra={"obs_rows": "10"})]
    out = tmp_path / "o.csv"
    spec.save(s, out)
    back = spec.load(out)
    assert back[0].extra["obs_rows"] == "10"


def test_pg_type_class():
    assert checks.pg_type_class("integer") == "number"
    assert checks.pg_type_class("character varying") == "text"
    assert checks.pg_type_class("timestamp with time zone") == "date"
    assert checks.pg_type_class("USER-DEFINED") is None


def test_column_types_missing_and_mismatch():
    specs = [spec.ColumnSpec("t", "a", "숫자"), spec.ColumnSpec("t", "b", "숫자"), spec.ColumnSpec("t", "z", "문자"),
             spec.ColumnSpec("t", "x", "")]
    db = {("t", "a"): "integer", ("t", "b"): "character varying"}
    r = checks.check_column_types(specs, db)
    assert [(x.column, x.failed) for x in r] == [("a", 0), ("b", 1), ("z", 1)]   # 타입 빈칸(x)은 검사 제외


def test_summarize_weights_by_counts_and_floors_preprocessing():
    R = checks.CheckResult
    res = [R("t", "a", "pattern", 900, 0), R("t", "b", "pattern", 100, 90),       # 합산 1 - 90/1000
           R("t", "a", "unique", 100, 1), R("t", "a", "not_null", 100, 2),
           R("t", "a", "column_type", 1, 0)]
    s = checks.summarize(res)
    assert s["profiling_pattern"]["pass_rate"] == pytest.approx(91.0)
    assert s["preprocessing"]["pass_rate_raw"] == pytest.approx(98.5)
    assert s["preprocessing"]["pass_rate"] == 98                                   # 소수점 내림


def test_range_expr_text_column_counts_non_numeric_as_out():
    sp = spec.ColumnSpec("t", "c", "문자", range_min=0, range_max=10)
    e = checks._range_expr('"c"', "character varying", sp)
    assert "IS NULL" in e and "< 0" in e and "> 10" in e


def _mk(root, layout):
    for rel, content in layout.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)


def test_files_diversity_and_dedup(tmp_path):
    g = tmp_path / "basic"
    layout = {}
    # 하위 폴더 4개: a 2개(20%), b 2개(20%), c 5개(50% → 범위 밖), d 1개(10%)
    for i in range(2): layout[f"a/{i}.bin"] = f"a{i}".encode()
    for i in range(2): layout[f"b/{i}.bin"] = f"b{i}".encode()
    for i in range(5): layout[f"c/{i}.bin"] = f"c{i}".encode()
    layout["d/0.bin"] = b"d0"
    layout["d/copy.bin"] = b"a0"      # a/0 과 같은 내용 → 중복으로 빠진다
    _mk(g, layout)
    r = files.count_group(g, name="기초")
    assert r.total == 10 and r.duplicates == 1
    assert r.diversity_pct == pytest.approx(50.0)     # a+b+d = 5 / 10
    r2 = files.count_group(g, dedup=False)
    assert r2.total == 11


def test_files_run_automation(tmp_path):
    _mk(tmp_path / "x", {"a/1": b"1", "b/1": b"2"})
    _mk(tmp_path / "y", {"a/1": b"3", "a/2": b"4"})
    cfg = tmp_path / "f.json"
    cfg.write_text(json.dumps({"groups": [
        {"name": "X", "path": str(tmp_path / "x"), "auto": True},
        {"name": "Y", "path": str(tmp_path / "y"), "auto": False}]}), encoding="utf-8")
    out = files.run(cfg)
    assert out["automation"] == {"auto_files": 2, "total_files": 4, "pass_rate": 50.0}


def test_batch_as_list():
    assert batch_report._as_list('[{"a":1}]') == [{"a": 1}]
    assert batch_report._as_list(None) is None
    assert batch_report._as_list("x") is None


def test_fault_injection_refuses_protected_db():
    with pytest.raises(SystemExit):
        fault_injection.run_scenarios("postgresql://u:p@localhost:5432/iitp_db", [], ".")


def test_fault_injection_alert_rule_matches_ops_alert():
    assert fault_injection._decide_alert("db", "ALL", "ERROR", "SUCCESS") == "FAIL"
    assert fault_injection._decide_alert("db", "ALL", "SUCCESS", "ERROR") == "RECOVER"
    assert fault_injection._decide_alert("file", "ALL", "ERROR", None) is None


@pytest.mark.skipif(not os.getenv("ITEST_DB_URL"), reason="ITEST_DB_URL 미설정 — DB 통합 테스트 생략")
def test_fault_injection_finds_run_started_in_same_second():
    """수집 명령은 started_at 을 초 단위로 잘라 기록한다. 시험 시작과 같은 초에 기록된 실행도 찾아야 한다.

    명령 대신 실행 기록 1행을 직접 넣는 작은 스크립트를 돌린다(started_at = 이번 초의 시작).
    검증용 DB(ITEST_DB_URL)에서만 돌고, 표식 ext_sys 의 행은 끝나면 지운다.
    """
    from sqlalchemy import create_engine, text
    url = os.environ["ITEST_DB_URL"]
    ext = "ITEST_FAULT_SAME_SEC"
    code = ("import os, sqlalchemy as sa\n"
            "e = sa.create_engine(os.environ['DB_URL'])\n"
            "with e.begin() as c:\n"
            "    c.execute(sa.text(\"INSERT INTO sys_batch_run_log (ext_sys, run_mode, run_scope, started_at, finished_at,"
            " exit_code, status, targets, ok_cnt, fail_cnt, failures) VALUES ('" + ext + "', 'db', 'ALL',"
            " date_trunc('second', now()), now(), 0, 'SUCCESS', 1, 1, 0, '[]'::jsonb)\"))\n")
    sc = [{"name": "same-second", "ext_sys": ext, "command": [sys.executable, "-c", code],
           "expect": {"status_in": ["SUCCESS"]}}]
    eng = create_engine(url)
    try:
        res = fault_injection.run_scenarios(url, sc, ".")
        assert res[0].ok, res[0].reasons
        assert res[0].status == "SUCCESS"
    finally:
        with eng.begin() as c:
            c.execute(text("DELETE FROM sys_batch_run_log WHERE ext_sys = :e"), {"e": ext})
        eng.dispose()
