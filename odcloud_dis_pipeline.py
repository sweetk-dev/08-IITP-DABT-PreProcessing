"""공공데이터포털 장애인 파일데이터 22종 수집·적재 흐름 — ODCLOUD_DIS (v1.22.0).

main.py 가 ext_sys == 'ODCLOUD_DIS' 이면 이 모듈의 run_odcloud_dis() 로 위임한다. 반환 요약은
고용 14종(odcloud_emp_pipeline.run_odcloud_emp)과 같은 모양이라 main.py 의 부분 완료(PARTIAL)·실행 기록·
알림 처리를 그대로 쓴다.

데이터셋 1종 처리 순서 (데이터셋끼리 격리 — 하나가 실패해도 나머지는 계속한다)
    1. 명세 조회 → 가장 늦은 기준일의 판 선택
    2. 기록된 판(sys_stats_src_api_info.api_data_url)과 같으면 건너뜀(변경 없음)
    3. 판 전체 수집(받은 행 수 = totalCount 확인) → 칸 검사·값 변환 → 고정 값·detail(JSON)
    4. 원본 보존: ext_data/ODCLOUD_DIS/<YYYYMMDD>/<키>_<기준일>.json
    5. db 모드: 한 트랜잭션으로 적재 + 추적 행 갱신 / file 모드: 무엇이 바뀔지만 계산(DB 쓰기 없음)

활용신청이 안 된 데이터셋은 데이터 API 가 거부해 그 데이터셋만 실패로 남는다(메일·실행 기록의 실패 목록).
sys_ext_api_info.latest_sync_time 은 db 모드·전체 실행에서 실패가 하나도 없을 때만 갱신한다.

수동 실행용 환경변수 (.env 에 넣지 말고 명령줄에만 붙인다) — 키 = stat_tbl_id(표 이름 또는 '표:종류')
    ODCLOUD_DIS_ONLY=<키>[,<키>]           이 데이터셋만 실행(알림 없음, 전체 동기화 시각 갱신 안 함)
    ODCLOUD_DIS_FORCE=<키>[,<키>]|ALL      판이 그대로여도 다시 적재
    ODCLOUD_DIS_ACCEPT_DROP=<키>[,..]|ALL  행 수 급감(50% 미만) 방어를 이번만 허용
"""
from __future__ import annotations

import datetime
import logging
import os

import db_mobility
import db_odcloud_dis
import db_odcloud_emp
from collectors.odcloud_dis import DATASET_BY_KEY, DATASETS, EXT_SYS, OdcloudDisCollector, build_rows
from collectors.odcloud_emp import editions_from_spec, pick_edition
from db import get_api_info
from file_utils import mask_auth_in_text

logger = logging.getLogger(__name__)

GENERIC_EXT_DATA_ROOT = 'ext_data'


def read_options() -> dict:
    """수동 실행 옵션. 모르는 키가 있으면 ValueError(오타로 엉뚱한 데이터셋을 건너뛰지 않게)."""
    opts = {
        'only': db_odcloud_emp.env_table_set('ODCLOUD_DIS_ONLY'),
        'force': db_odcloud_emp.env_table_set('ODCLOUD_DIS_FORCE'),
        'accept_drop': db_odcloud_emp.env_table_set('ODCLOUD_DIS_ACCEPT_DROP'),
    }
    for key in ('only', 'force', 'accept_drop'):
        bad = sorted(t for t in opts[key] if t != 'ALL' and t not in DATASET_BY_KEY)
        if bad or (key == 'only' and 'ALL' in opts[key]):
            raise ValueError('ODCLOUD_DIS_%s 에 모르는 키: %s' % (key.upper(), ', '.join(bad) or 'ALL'))
    return opts


def _file_name(key: str, base_date: datetime.date) -> str:
    return '%s_%s.json' % (key.replace(':', '__'), base_date.strftime('%Y%m%d'))


def run_odcloud_dis(mode: str) -> dict:
    """22종 수집·적재 1회. 반환: main.py 가 읽는 요약 dict."""
    api_info = get_api_info(EXT_SYS) or {}
    if not api_info.get('ext_api_id'):
        raise RuntimeError('sys_ext_api_info 에 ODCLOUD_DIS 행이 없음 — 01 v1.17.0 마이그레이션 적용 필요')
    opts = read_options()
    tracking = db_odcloud_dis.load_tracking(api_info['ext_api_id'])
    collector = OdcloudDisCollector(api_info=api_info, stats_src={})
    datasets = [d for d in DATASETS if not opts['only'] or d.key in opts['only']]
    run_scope = 'PARTIAL' if opts['only'] else 'ALL'
    save_dir = os.path.join(GENERIC_EXT_DATA_ROOT, EXT_SYS, datetime.datetime.now().strftime('%Y%m%d'))

    summary = {'targets': len(datasets), 'files_ok': 0, 'db_ok': 0, 'db_fail': 0,
               'failures': [], 'by_table': {}, 'run_scope': run_scope}
    for ds in datasets:
        label = '%s:%s' % (EXT_SYS, ds.key)
        stage = 'collect'
        info = {'label': ds.label}
        try:
            track = tracking.get(ds.key)
            if track is None:
                raise RuntimeError('sys_stats_src_api_info 에 %s 행이 없음 — 01 v1.17.0 마이그레이션 확인' % ds.key)
            edition = pick_edition(editions_from_spec(collector.fetch_spec(ds, track.get('api_meta_url'))))
            info.update(edition=edition.path, base_date=edition.base_date_str)
            forced = 'ALL' in opts['force'] or ds.key in opts['force']
            if edition.path == (track.get('api_data_url') or '') and not forced:
                info['result'] = 'unchanged'
                summary['files_ok'] += 1
                summary['db_ok'] += 1
                logger.info('%s: 판 변경 없음(%s) — 건너뜀', label, edition.base_date_str)
                continue
            raw = collector.fetch_rows(ds, edition)
            rows, notes = build_rows(ds, edition, raw)
            info.update(fetched=len(raw), nulled=notes['nulled'], rounded=notes['rounded'])
            collector.save_response(raw, save_dir, _file_name(ds.key, edition.base_date))
            summary['files_ok'] += 1
            stage = 'db'
            if mode == 'db':
                res = db_odcloud_dis.load_dataset(ds, edition.path, edition.base_date, rows, track,
                                                  opts['accept_drop'])
                info.update(result='loaded', **res)
            else:
                plan = db_odcloud_dis.plan_dataset(ds, rows, opts['accept_drop'], edition.base_date)
                info.update(result='planned', **plan)
                if plan['problem']:
                    raise RuntimeError(plan['problem'])
            summary['db_ok'] += 1
        except Exception as exc:  # noqa: BLE001 — 데이터셋별 격리
            # 사유는 로그·실행 기록·알림 메일로 나간다. URL 이 섞여도 인증키는 가린다(serviceKey=***).
            reason = mask_auth_in_text('%s %s 실패: %s' % (ds.label, '수집' if stage == 'collect' else '적재', exc))
            logger.error('%s %s', label, reason)
            summary['failures'].append({'id': label, 'stage': stage, 'reason': reason[:300]})
            if stage == 'db':
                summary['db_fail'] += 1
            info.update(result='failed', reason=reason[:300])
        finally:
            summary['by_table'][ds.key] = info
            collector.pause()

    try:
        collector.save_response(summary['by_table'], save_dir, 'run_summary.json')
    except OSError as exc:
        logger.warning('%s 실행 요약 파일 저장 실패: %s', EXT_SYS, exc)
    for key, info in summary['by_table'].items():
        logger.info('%s %-40s %-9s 기준일 %s %s', EXT_SYS, key, info.get('result'), info.get('base_date'),
                    info.get('reason') or '')

    if datasets and summary['db_ok'] == 0 and summary['failures']:
        raise RuntimeError('ODCLOUD_DIS: %d종 모두 실패 — %s' % (
            len(datasets), ' / '.join(f['reason'] for f in summary['failures'])[:900]))
    if mode == 'db':
        if summary['failures'] or run_scope != 'ALL':
            logger.warning('%s: 실패 %d건·실행 범위 %s — 전체 동기화 시각 갱신 보류', EXT_SYS,
                           len(summary['failures']), run_scope)
        else:
            db_mobility.touch_latest_sync(EXT_SYS)
    return summary
