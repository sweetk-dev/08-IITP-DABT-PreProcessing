"""poi_facility_accessibility 신규 플래그 백필 — 저장된 eval_info_raw 를 다시 파싱한다 (2026-09-05).

01 v1.4.0 에서 guide_facility_yn·accessible_room_yn 컬럼이 추가됐다. 원천 재호출 없이
이미 저장된 기구표 원문(eval_info_raw)을 collectors.kowsi_facl.parse_eval_flags 로 다시 파싱해
두 컬럼만 채운다(더미 응답 판별·정규화 규칙을 수집기와 공유하므로 SQL 문자열 검색으로 하지 않는다).

    python scripts/backfill_facility_eval_flags.py --dry-run
    python scripts/backfill_facility_eval_flags.py            # 500건 단위 커밋

재실행해도 같은 결과(멱등). 다른 플래그 6종은 건드리지 않는다.
"""
from __future__ import annotations

import argparse
import collections
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sqlalchemy import text  # noqa: E402

from collectors.kowsi_facl import parse_eval_flags  # noqa: E402
from db import engine  # noqa: E402
from db_mobility import CREATED_BY  # noqa: E402

TARGET_COLS = ('guide_facility_yn', 'accessible_room_yn')


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--chunk', type=int, default=500)
    args = ap.parse_args()
    if engine is None:
        print('DB_URL 미설정', file=sys.stderr)
        return 1

    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT facl_id, wfclt_id, eval_info_raw, guide_facility_yn, accessible_room_yn"
            " FROM poi_facility_accessibility WHERE del_yn='N' ORDER BY facl_id")).fetchall()

    updates = []
    stats = collections.Counter()
    for facl_id, wfclt_id, raw, cur_guide, cur_room in rows:
        flags = parse_eval_flags(raw, wfclt_id or str(facl_id))
        new = {c: flags[c] for c in TARGET_COLS}
        stats['guide=' + str(new['guide_facility_yn'])] += 1
        stats['room=' + str(new['accessible_room_yn'])] += 1
        if (cur_guide, cur_room) != (new['guide_facility_yn'], new['accessible_room_yn']):
            updates.append(dict(new, facl_id=facl_id, created_by=CREATED_BY))

    print('대상 %d행 / 변경 %d행' % (len(rows), len(updates)))
    for k in sorted(stats):
        print('  %s: %d' % (k, stats[k]))
    if args.dry_run or not updates:
        return 0

    sql = text(
        "UPDATE poi_facility_accessibility SET guide_facility_yn=:guide_facility_yn,"
        " accessible_room_yn=:accessible_room_yn, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
        " WHERE facl_id=:facl_id")
    done = 0
    for i in range(0, len(updates), args.chunk):
        chunk = updates[i:i + args.chunk]
        with engine.begin() as conn:
            conn.execute(sql, chunk)
        done += len(chunk)
        print('  커밋 %d/%d' % (done, len(updates)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
