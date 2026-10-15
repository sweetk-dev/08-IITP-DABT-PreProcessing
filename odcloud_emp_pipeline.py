"""장애인 고용 파일데이터 14종 수집·적재 흐름 — ODCLOUD_EMP (v1.19.0).

main.py 가 ext_sys == 'ODCLOUD_EMP' 이면 이 모듈의 run_odcloud_emp() 로 위임한다.
반환하는 요약은 이동편의 파이프라인(mobility_pipeline.run_mobility)과 같은 모양이라 main.py 의
부분 완료(PARTIAL)·실행 기록·알림 처리를 그대로 쓴다.

데이터셋 1종 처리 순서 (데이터셋끼리 격리 — 하나가 실패해도 나머지는 계속한다)
    1. 명세 조회 → 가장 늦은 기준일의 판 선택
    2. 기록된 판(sys_stats_src_api_info.api_data_url)과 같으면 건너뜀(변경 없음)
    3. 판 전체 수집(받은 행 수 = totalCount 확인) → 칸 검사·값 변환
    4. 원본 보존: ext_data/ODCLOUD_EMP/<YYYYMMDD>/<표>_<기준일>.json
    5. db 모드: 한 트랜잭션으로 적재 + 추적 행 갱신 / file 모드: 무엇이 바뀔지만 계산(DB 쓰기 없음)
       급감 방어는 표 전체(전체 교체 표) 또는 연도마다(연도별 표) 본다.

실행 결과
    targets = 대상 데이터셋 수, db_ok(file 모드는 files_ok) = 적재했거나 이미 최신인 데이터셋 수.
    실패한 데이터셋은 failures 에 {'id': 'ODCLOUD_EMP:<표>', 'stage', 'reason'} 로 실려
    main.py 가 부분 완료(종료코드 2)로 기록·알림한다. 모든 데이터셋이 실패하면 예외 → 전체 실패(1).
    sys_ext_api_info.latest_sync_time 은 db 모드·전체 실행에서 실패가 하나도 없을 때만 갱신한다
    (KOSIS·GG_CULTURE 와 같은 규칙 — 일부 실패에도 갱신하면 운영 화면이 전부 최신인 것처럼 보인다).
    표별 시각은 적재에 성공한 표만 sys_stats_src_api_info.latest_sync_time 에 남는다.

수동 실행용 환경변수 (.env 에 넣지 말고 명령줄에만 붙인다)
    ODCLOUD_EMP_ONLY=<표>[,<표>]          이 표들만 실행. 일부 실행이므로 알림을 보내지 않고 전체 동기화
                                           시각도 갱신하지 않는다(run_scope=PARTIAL).
    ODCLOUD_EMP_FORCE=<표>[,<표>]|ALL     판이 그대로여도 다시 적재.
    ODCLOUD_EMP_ACCEPT_DROP=<표>[,..]|ALL 행 수 급감(50% 미만) 방어를 이번만 허용.
    ODCLOUD_EMP_EDITION=<표>:<uddi>[,..]  최신 판 대신 지정한 판을 적재(연도별 표만 — 빠진 연도 보충용).
                                           기록된 기준일보다 이른 판이면 표에 없는 연도만 넣고(이미 있는
                                           연도는 옛 값으로 덮지 않음) 추적 행은 바꾸지 않는다.
"""
from __future__ import annotations

import datetime
import logging
import os
from typing import Dict

import db_mobility
import db_odcloud_emp
from collectors.odcloud_emp import (
    DATASET_BY_TABLE, DATASETS, EXT_SYS, MODE_YEAR, DatasetError, OdcloudEmpCollector,
    build_rows, editions_from_spec, pick_edition,
)
from db import get_api_info
from file_utils import mask_auth_in_text

logger = logging.getLogger(__name__)

GENERIC_EXT_DATA_ROOT = 'ext_data'


def _parse_editions(raw: str) -> Dict[str, str]:
    """'표:uddi,표:uddi' → {표: uddi}. 형식이 틀리면 ValueError(실행 전체를 멈춘다 — 오타로 엉뚱한 판을
    넣거나 지정이 조용히 무시되는 것을 막는다)."""
    out = {}
    for part in (raw or '').split(','):
        part = part.strip()
        if not part:
            continue
        if ':' not in part:
            raise ValueError("ODCLOUD_EMP_EDITION 형식 오류(표:uddi): %r" % part)
        tbl, uddi = part.split(':', 1)
        out[tbl.strip()] = uddi.strip()
    return out


def read_options() -> dict:
    """수동 실행 옵션을 읽고 표 이름을 검사한다. 모르는 표 이름이 있으면 ValueError."""
    opts = {
        'only': db_odcloud_emp.env_table_set('ODCLOUD_EMP_ONLY'),
        'force': db_odcloud_emp.env_table_set('ODCLOUD_EMP_FORCE'),
        'accept_drop': db_odcloud_emp.env_table_set('ODCLOUD_EMP_ACCEPT_DROP'),
        'editions': _parse_editions(os.getenv('ODCLOUD_EMP_EDITION', '')),
    }
    for key in ('only', 'force', 'accept_drop'):
        bad = sorted(t for t in opts[key] if t != 'ALL' and t not in DATASET_BY_TABLE)
        if bad or (key == 'only' and 'ALL' in opts[key]):
            raise ValueError('ODCLOUD_EMP_%s 에 모르는 표 이름: %s' % (key.upper(), ', '.join(bad) or 'ALL'))
    for tbl in opts['editions']:
        ds = DATASET_BY_TABLE.get(tbl)
        if ds is None:
            raise ValueError('ODCLOUD_EMP_EDITION 에 모르는 표 이름: %s' % tbl)
        if ds.mode != MODE_YEAR:
            # 전체 교체 표에 옛 판을 넣으면 표가 통째로 과거로 돌아가고, 추적 행은 최신 판을 가리키는
            # 상태로 남아 다음 실행도 바로잡지 않는다. 연도별 표(빠진 연도 보충)만 허용한다.
            raise ValueError('ODCLOUD_EMP_EDITION 은 연도별 표에만 쓸 수 있음: %s' % tbl)
    return opts


def run_odcloud_emp(mode: str) -> dict:
    """14종 수집·적재 1회. 반환: main.py 가 읽는 요약 dict."""
    api_info = get_api_info(EXT_SYS) or {}
    if not api_info.get('ext_api_id'):
        raise RuntimeError('sys_ext_api_info 에 ODCLOUD_EMP 행이 없음 — 01 v1.11.0 마이그레이션 적용 필요')
    opts = read_options()
    tracking = db_odcloud_emp.load_tracking(api_info['ext_api_id'])
    collector = OdcloudEmpCollector(api_info=api_info, stats_src={})
    datasets = [d for d in DATASETS if not opts['only'] or d.table in opts['only']]
    run_scope = 'PARTIAL' if opts['only'] else 'ALL'
    save_dir = os.path.join(GENERIC_EXT_DATA_ROOT, EXT_SYS, datetime.datetime.now().strftime('%Y%m%d'))

    summary = {'targets': len(datasets), 'files_ok': 0, 'db_ok': 0, 'db_fail': 0,
               'failures': [], 'by_table': {}, 'run_scope': run_scope}
    for ds in datasets:
        label = '%s:%s' % (EXT_SYS, ds.table)
        stage = 'collect'
        info = {'label': ds.label}
        try:
            track = tracking.get(ds.table)
            if track is None:
                raise DatasetError('sys_stats_src_api_info 에 %s 행이 없음 — 01 v1.11.0 마이그레이션 확인' % ds.table)
            pinned = opts['editions'].get(ds.table)
            edition = pick_edition(editions_from_spec(collector.fetch_spec(ds, track.get('api_meta_url'))),
                                   pinned_uddi=pinned)
            info.update(edition=edition.path, base_date=edition.base_date_str)
            forced = 'ALL' in opts['force'] or ds.table in opts['force']
            if edition.path == (track.get('api_data_url') or '') and not forced and not pinned:
                info['result'] = 'unchanged'
                summary['files_ok'] += 1
                summary['db_ok'] += 1
                logger.info('%s: 판 변경 없음(%s) — 건너뜀', label, edition.base_date_str)
                continue
            raw = collector.fetch_rows(ds, edition)
            rows, notes = build_rows(ds, edition, raw)
            info.update(fetched=len(raw), nulled=notes['nulled'], rounded=notes['rounded'])
            collector.save_response(raw, save_dir, '%s_%s.json' % (ds.table, edition.base_date.strftime('%Y%m%d')))
            summary['files_ok'] += 1
            stage = 'db'
            if mode == 'db':
                res = db_odcloud_emp.load_dataset(ds.table, ds.mode, edition.path, edition.base_date, rows,
                                                  track, opts['accept_drop'], pinned=bool(pinned))
                info.update(result='loaded', **res)
            else:
                plan = db_odcloud_emp.plan_dataset(ds.table, ds.mode, rows, opts['accept_drop'])
                info.update(result='planned', **plan)
                if plan['problem']:
                    raise DatasetError(plan['problem'])
            summary['db_ok'] += 1
        except Exception as exc:  # noqa: BLE001 — 데이터셋별 격리: 이 데이터셋만 실패로 기록하고 다음으로
            # 사유는 로그·실행 기록·알림 메일로 나간다. 수집기 예외는 이미 인증키를 가리지만, 어느 경로로든
            # URL 이 섞여 들어올 수 있으므로 한 번 더 가린다(serviceKey=***).
            reason = mask_auth_in_text('%s %s 실패: %s' % (ds.label, '수집' if stage == 'collect' else '적재', exc))
            logger.error('%s %s', label, reason)
            summary['failures'].append({'id': label, 'stage': stage, 'reason': reason[:300]})
            if stage == 'db':
                summary['db_fail'] += 1
            info.update(result='failed', reason=reason[:300])
        finally:
            summary['by_table'][ds.table] = info
            collector.pause()

    try:
        collector.save_response(summary['by_table'], save_dir, 'run_summary.json')
    except OSError as exc:  # 요약 파일 저장 실패가 적재 결과를 바꾸지 않게 한다
        logger.warning('%s 실행 요약 파일 저장 실패: %s', EXT_SYS, exc)
    for tbl, info in summary['by_table'].items():
        logger.info('%s %-32s %-9s 기준일 %s %s', EXT_SYS, tbl, info.get('result'), info.get('base_date'),
                    info.get('reason') or '')

    # 성공 = 적재(db) 또는 적재 계획 점검(file)까지 통과했거나 판이 그대로인 데이터셋. file 모드에서도
    # files_ok(수집만 성공)가 아니라 이 값으로 '전부 실패' 를 판정한다.
    if datasets and summary['db_ok'] == 0 and summary['failures']:
        raise RuntimeError('ODCLOUD_EMP: %d종 모두 실패 — %s' % (
            len(datasets), ' / '.join(f['reason'] for f in summary['failures'])[:900]))
    if mode == 'db':
        if summary['failures'] or run_scope != 'ALL':
            logger.warning('%s: 실패 %d건·실행 범위 %s — 전체 동기화 시각 갱신 보류', EXT_SYS,
                           len(summary['failures']), run_scope)
        else:
            db_mobility.touch_latest_sync(EXT_SYS)
    return summary
