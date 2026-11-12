"""품질 측정 실행 명령.

    python -m quality profile --out spec_draft.csv [--base spec.csv] [--include 'stats_*'] [--exclude ...]
    python -m quality check   --spec spec.csv --out result_dir
    python -m quality files   --config files.json --out result_dir
    python -m quality runs    --days 90 --out result_dir
    python -m quality faults  --scenarios scenarios.json --db-url <검증용 DB 주소> --workdir . --out result_dir

DB 주소는 --db-url 이 없으면 .env 의 DB_URL(config.get_db_url)을 쓴다. check·runs 는 조회만 한다.
결과는 out 폴더에 summary.json 과 상세 CSV 로 남긴다(측정 시각·명세서 파일 해시 포함).
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine


def _engine(db_url):
    if not db_url:
        from dotenv import load_dotenv
        load_dotenv()
        from config import get_db_url
        db_url = get_db_url()
    if not db_url:
        raise SystemExit("DB 주소가 없다 — --db-url 또는 .env 의 DB_URL")
    return create_engine(db_url, connect_args={"options": "-c default_transaction_read_only=on"})


def _write(out: Path, name: str, rows, summary: dict, extra_meta=None):
    out.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    if rows:
        with open(out / f"{name}.csv", "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    meta = {"measured_at": datetime.now().isoformat(timespec="seconds"), **(extra_meta or {})}
    (out / f"{name}_summary.json").write_text(json.dumps({"meta": meta, "summary": summary}, ensure_ascii=False, indent=2),
                                             encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m quality")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("profile"); p.add_argument("--out", required=True); p.add_argument("--base")
    p.add_argument("--include", nargs="*"); p.add_argument("--exclude", nargs="*"); p.add_argument("--db-url")
    p.add_argument("--no-observe", action="store_true")
    c = sub.add_parser("check"); c.add_argument("--spec", required=True); c.add_argument("--out", required=True)
    c.add_argument("--db-url"); c.add_argument("--blank-as-null", action="store_true")
    fl = sub.add_parser("files"); fl.add_argument("--config", required=True); fl.add_argument("--out", required=True)
    r = sub.add_parser("runs"); r.add_argument("--days", type=int, default=90); r.add_argument("--out", required=True)
    r.add_argument("--db-url")
    fi = sub.add_parser("faults"); fi.add_argument("--scenarios", required=True); fi.add_argument("--db-url", required=True)
    fi.add_argument("--workdir", default="."); fi.add_argument("--out", required=True)
    a = ap.parse_args(argv)

    if a.cmd == "profile":
        from quality import profile, spec
        with _engine(a.db_url).connect() as conn:
            tables = profile.list_tables(conn, include=a.include, exclude=a.exclude)
            base = spec.load(a.base) if a.base else None
            specs = profile.draft(conn, tables, base=base, with_observation=not a.no_observe)
        spec.save(specs, a.out)
        print(f"{len(tables)} tables, {len(specs)} columns -> {a.out}")
    elif a.cmd == "check":
        from quality import checks, spec
        specs = spec.load(a.spec)
        with _engine(a.db_url).connect() as conn:
            res = checks.run_all(conn, specs, blank_as_null=a.blank_as_null)
        sha = hashlib.sha256(Path(a.spec).read_bytes()).hexdigest()[:16]
        _write(Path(a.out), "check", [x.as_dict() for x in res], checks.summarize(res),
               {"spec": str(a.spec), "spec_sha256_16": sha, "columns": len(specs), "blank_as_null": a.blank_as_null})
    elif a.cmd == "files":
        from quality import files
        res = files.run(a.config)
        rows = [{"group": g["name"], **f} for g in res["groups"] for f in g["folders"]]
        _write(Path(a.out), "files", rows, {"diversity": res["diversity"], "automation": res["automation"],
                                            "rule": res["rule"]})
    elif a.cmd == "runs":
        from quality import batch_report
        with _engine(a.db_url).connect() as conn:
            res = batch_report.check_runs(conn, a.days)
        _write(Path(a.out), "runs", [x.as_dict() for x in res], batch_report.summarize(res), {"days": a.days})
    elif a.cmd == "faults":
        from quality import fault_injection
        res = fault_injection.run_scenarios(a.db_url, fault_injection.load(a.scenarios), a.workdir)
        _write(Path(a.out), "faults", [x.as_dict() for x in res], fault_injection.summarize(res))
    return 0


if __name__ == "__main__":
    sys.exit(main())
