#!/usr/bin/env bash
# 08-IITP-DABT-PreProcessing 수집/적재 실행 래퍼 (스케줄러용)
# - 단일 인스턴스 보장(flock), 프로젝트 루트 자체 탐지, venv 자동 활성화(있으면)
# - crontab 예시(매월 3일/18일 03:00):  0 3 3,18 * *  <PROJECT_DIR>/scripts/run_collect.sh >> <PROJECT_DIR>/logs/cron.log 2>&1
# - 사용법:  run_collect.sh [mode]   (mode 기본값: db)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

MODE="${1:-db}"
LOCKFILE="$PROJECT_DIR/.run_collect.lock"

# 단일 인스턴스: 이미 실행 중이면 이번 트리거 건너뜀
exec 9>"$LOCKFILE"
if ! flock -n 9; then
  echo "[$(date '+%F %T')] already running, skip this trigger" >&2
  exit 0
fi

# 가상환경 활성화(있을 때만)
if [ -f "$PROJECT_DIR/.venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  . "$PROJECT_DIR/.venv/bin/activate"
fi

echo "[$(date '+%F %T')] run_collect start (mode=$MODE)"
python main.py --mode "$MODE"
rc=$?
echo "[$(date '+%F %T')] run_collect end exit=$rc"
exit $rc
