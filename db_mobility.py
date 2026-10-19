"""이동편의 데이터 적재 모듈 — Issue #76.

대상 테이블(01-IITP-DABT-Database):
- tran_bus_route_info          (GBIS)          ON CONFLICT (route_id)
- tran_bus_route_info.low_bus_yn (GBIS_LOWFLOOR) routeId 매칭 UPDATE (Issue #97)
- tran_bus_station_info        (GBIS)          ON CONFLICT (station_id)
- tran_bus_route_station       (GBIS)          ON CONFLICT (route_id, station_id, station_seq)
- poi_station_access_status    (KORAIL_CONV)   ON CONFLICT (stn_cd)
- poi_station_wheelchair_lift  (KRNA_LIFT CSV) ON CONFLICT (line_name, stn_name, mng_no)
- poi_station_elevator_unit    (KRNA_STN CSV)  선명 단위 전체 교체 (연 1회 파일)
- poi_station_toilet_unit      (KRNA_STN CSV)  선명·disabled_yn 단위 전체 교체
- poi_station_platform         (KRNA_STN CSV)  ON CONFLICT (line_name, stn_name, platform_no) + 이격거리 요약 UPDATE
- poi_facility_accessibility   (KOWSI_FACL)    ON CONFLICT (facl_inf_id)
- poi_tour_bf_facility         (TOUR_BF_API)   자연키 없음 → (fclt_name, sido_code) 조회 후 UPDATE/INSERT / 범위 안 사라진 행 논리삭제
- poi_public_toilet_info       (GG_TOILET)     자연키 없음 → (이름+주소) 매칭 UPDATE / 미매칭 INSERT / 사라진 행 논리삭제
- poi_emergency_support        (GG_ASSIST_REPAIR) ON CONFLICT (support_type, name, coalesce(addr_road,''))
- poi_culture_sports_facility  (GG_CULTURE)    자연키 없음 → 종류별 (이름+주소) 매칭 UPDATE / 되살림 / INSERT / 논리삭제, 종류별 트랜잭션
- poi_welfare_facility         (GG_WELFARE)    GG_CULTURE 와 같은 방식(장애인복지시설 5종, v1.21.0)
- stats_gg_dis_reg_emd         (GG_DIS_REG)    (시군, 기준일) 판 단위 교체 — 다른 기준일 판은 남김, 시군별 트랜잭션 (v1.21.0)
- sys_data_summary_info        수집 작업이 실패 없이 끝나면 원천 기준일·확인일 갱신 (refresh_data_summary, v1.21.0)

created_by 는 DB 공통코드(sys_work_type) 시드 정본 'SYS-BACH' 를 따른다.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from sqlalchemy import text

from db import engine

logger = logging.getLogger('db')

CREATED_BY = 'SYS-BACH'  # sys_work_type 시드 정본 표기


def _execute_batch(sql: str, rows: List[dict]) -> int:
    if not rows:
        return 0
    if engine is None:
        raise RuntimeError('DB engine not configured (DB_URL)')
    with engine.begin() as conn:
        for row in rows:
            conn.execute(text(sql), dict(row, created_by=CREATED_BY))
    return len(rows)


def upsert_bus_routes(rows: List[dict]) -> int:
    sql = (
        "INSERT INTO tran_bus_route_info ("
        " route_id, route_name, route_type_cd, route_type_name, region_name, admin_name,"
        " start_station_id, start_station_name, end_station_id, end_station_name,"
        " company_name, company_tel,"
        " peek_alloc, npeek_alloc, sat_peek_alloc, sat_npeek_alloc,"
        " sun_peek_alloc, sun_npeek_alloc, we_peek_alloc, we_npeek_alloc,"
        " up_first_time, up_last_time, down_first_time, down_last_time,"
        " base_dt, created_by)"
        " VALUES (:route_id, :route_name, :route_type_cd, :route_type_name, :region_name, :admin_name,"
        " :start_station_id, :start_station_name, :end_station_id, :end_station_name,"
        " :company_name, :company_tel,"
        " :peek_alloc, :npeek_alloc, :sat_peek_alloc, :sat_npeek_alloc,"
        " :sun_peek_alloc, :sun_npeek_alloc, :we_peek_alloc, :we_npeek_alloc,"
        " :up_first_time, :up_last_time, :down_first_time, :down_last_time,"
        " CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (route_id) DO UPDATE SET"
        " route_name=EXCLUDED.route_name, route_type_cd=EXCLUDED.route_type_cd,"
        " route_type_name=EXCLUDED.route_type_name, region_name=EXCLUDED.region_name,"
        " admin_name=EXCLUDED.admin_name,"
        " start_station_id=EXCLUDED.start_station_id, start_station_name=EXCLUDED.start_station_name,"
        " end_station_id=EXCLUDED.end_station_id, end_station_name=EXCLUDED.end_station_name,"
        " company_name=EXCLUDED.company_name, company_tel=EXCLUDED.company_tel,"
        " peek_alloc=EXCLUDED.peek_alloc, npeek_alloc=EXCLUDED.npeek_alloc,"
        " sat_peek_alloc=EXCLUDED.sat_peek_alloc, sat_npeek_alloc=EXCLUDED.sat_npeek_alloc,"
        " sun_peek_alloc=EXCLUDED.sun_peek_alloc, sun_npeek_alloc=EXCLUDED.sun_npeek_alloc,"
        " we_peek_alloc=EXCLUDED.we_peek_alloc, we_npeek_alloc=EXCLUDED.we_npeek_alloc,"
        " up_first_time=EXCLUDED.up_first_time, up_last_time=EXCLUDED.up_last_time,"
        " down_first_time=EXCLUDED.down_first_time, down_last_time=EXCLUDED.down_last_time,"
        " base_dt=EXCLUDED.base_dt, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    return _execute_batch(sql, rows)


def update_bus_route_low_floor(rows: List[dict]) -> int:
    """tran_bus_route_info.low_bus_yn / low_bus_base_dt 갱신 — Issue #97.

    rows: collectors.gbis_lowfloor 행(route_id, low_bus_base_dt). 표에 있는 노선은 'Y',
    경기도 관할(admin_name '경기도%') 노선 중 표에 없는 노선은 'N'. 그 밖의 노선은 손대지 않는다.
    빈 rows 는 원천 장애로 보고 아무것도 갱신하지 않는다(기존 값 보존).
    반환: 갱신 행 수(Y + N).
    """
    ids = sorted({int(r['route_id']) for r in rows if r.get('route_id') is not None})
    if not ids:
        logger.warning('GBIS_LOWFLOOR: 갱신할 routeId 가 없어 low_bus_yn 을 건드리지 않습니다')
        return 0
    base_dt = next((r.get('low_bus_base_dt') for r in rows if r.get('low_bus_base_dt')), None)
    if engine is None:
        return 0
    with engine.begin() as conn:
        y = conn.execute(text(
            "UPDATE tran_bus_route_info SET low_bus_yn='Y', low_bus_base_dt=CAST(:base_dt AS date),"
            " updated_at=CURRENT_TIMESTAMP, updated_by=:by"
            " WHERE route_id = ANY(:ids) AND COALESCE(del_yn,'N')='N'"
        ), {'ids': ids, 'base_dt': base_dt, 'by': CREATED_BY}).rowcount
        n = conn.execute(text(
            "UPDATE tran_bus_route_info SET low_bus_yn='N', low_bus_base_dt=CAST(:base_dt AS date),"
            " updated_at=CURRENT_TIMESTAMP, updated_by=:by"
            " WHERE NOT (route_id = ANY(:ids)) AND COALESCE(del_yn,'N')='N'"
            "   AND COALESCE(admin_name,'') LIKE '경기도%'"
        ), {'ids': ids, 'base_dt': base_dt, 'by': CREATED_BY}).rowcount
    logger.info('GBIS_LOWFLOOR: low_bus_yn Y=%d N=%d (기준일 %s, 표 routeId %d건)', y, n, base_dt, len(ids))
    return int(y or 0) + int(n or 0)


def upsert_bus_stations(rows: List[dict]) -> int:
    """tran_bus_station_info — GBIS 경유정류소 마스터 (Issue #85).

    좌표는 GBIS 가 항상 채워 보내지만, 결측 응답이 기존 좌표를 지우지 않도록
    COALESCE 로 보존한다(poi_station_access_status 와 동일한 방어).
    """
    sql = (
        "INSERT INTO tran_bus_station_info ("
        " station_id, mobile_no, station_name, region_name, admin_name,"
        " latitude, longitude, center_yn, district_cd, base_dt, created_by)"
        " VALUES (:station_id, :mobile_no, :station_name, :region_name, :admin_name,"
        " :latitude, :longitude, :center_yn, :district_cd, CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (station_id) DO UPDATE SET"
        " mobile_no=EXCLUDED.mobile_no, station_name=EXCLUDED.station_name,"
        " region_name=EXCLUDED.region_name, admin_name=EXCLUDED.admin_name,"
        " latitude=COALESCE(EXCLUDED.latitude, tran_bus_station_info.latitude),"
        " longitude=COALESCE(EXCLUDED.longitude, tran_bus_station_info.longitude),"
        " center_yn=EXCLUDED.center_yn, district_cd=EXCLUDED.district_cd,"
        " base_dt=EXCLUDED.base_dt, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    return _execute_batch(sql, rows)


def upsert_bus_route_stations(rows: List[dict]) -> int:
    """tran_bus_route_station — 노선-정류장 경유 관계 (Issue #85)."""
    sql = (
        "INSERT INTO tran_bus_route_station ("
        " route_id, station_id, station_seq, turn_seq, turn_yn, base_dt, created_by)"
        " VALUES (:route_id, :station_id, :station_seq, :turn_seq, :turn_yn,"
        " CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (route_id, station_id, station_seq) DO UPDATE SET"
        " turn_seq=EXCLUDED.turn_seq, turn_yn=EXCLUDED.turn_yn,"
        " base_dt=EXCLUDED.base_dt, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    return _execute_batch(sql, rows)


def upsert_station_access(rows: List[dict]) -> int:
    """좌표(latitude/longitude)는 Issue #80 보강분 — 역위치 CSV 미매칭 역(None)은
    COALESCE 로 기존값을 보존한다(임시 보강 좌표를 재수집이 지우지 않도록)."""
    sql = (
        "INSERT INTO poi_station_access_status ("
        " stn_cd, stn_name, elevator_cnt, escalator_cnt, wheelchair_lift_cnt,"
        " dis_slope_yn, dis_toilet_yn, gen_toilet_yn, nursing_room_yn, info_center_yn,"
        " latitude, longitude, anyang_yn, base_dt, created_by)"
        " VALUES (:stn_cd, :stn_name, :elevator_cnt, :escalator_cnt, :wheelchair_lift_cnt,"
        " :dis_slope_yn, :dis_toilet_yn, :gen_toilet_yn, :nursing_room_yn, :info_center_yn,"
        " :latitude, :longitude, :anyang_yn, CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (stn_cd) DO UPDATE SET"
        " stn_name=EXCLUDED.stn_name, elevator_cnt=EXCLUDED.elevator_cnt,"
        " escalator_cnt=EXCLUDED.escalator_cnt, wheelchair_lift_cnt=EXCLUDED.wheelchair_lift_cnt,"
        " dis_slope_yn=EXCLUDED.dis_slope_yn, dis_toilet_yn=EXCLUDED.dis_toilet_yn,"
        " gen_toilet_yn=EXCLUDED.gen_toilet_yn, nursing_room_yn=EXCLUDED.nursing_room_yn,"
        " info_center_yn=EXCLUDED.info_center_yn,"
        " latitude=COALESCE(EXCLUDED.latitude, poi_station_access_status.latitude),"
        " longitude=COALESCE(EXCLUDED.longitude, poi_station_access_status.longitude),"
        " anyang_yn=EXCLUDED.anyang_yn,"
        " base_dt=EXCLUDED.base_dt, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    for row in rows:
        row.setdefault('latitude', None)
        row.setdefault('longitude', None)
    return _execute_batch(sql, rows)


def fetch_station_names() -> List[dict]:
    """poi_station_access_status 의 (stn_cd, stn_name, latitude) — 좌표 갱신 매칭용."""
    if engine is None:
        raise RuntimeError('DB engine not configured (DB_URL)')
    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT stn_cd, stn_name, latitude FROM poi_station_access_status"
            " WHERE del_yn = 'N'"
        )).mappings().all()
    return [dict(r) for r in rows]


def update_station_coords(rows: List[dict]) -> int:
    """좌표만 갱신 (전체 재수집 없이) — rows: [{stn_cd, latitude, longitude}]."""
    sql = (
        "UPDATE poi_station_access_status SET"
        " latitude=:latitude, longitude=:longitude,"
        " updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
        " WHERE stn_cd=:stn_cd"
    )
    return _execute_batch(sql, rows)


def upsert_wheelchair_lifts(rows: List[dict]) -> int:
    sql = (
        "INSERT INTO poi_station_wheelchair_lift ("
        " oper_org, line_name, stn_name, mng_no, exit_no, detail_loc,"
        " length_mm, width_mm, start_floor, end_floor, base_dt, created_by)"
        " VALUES (:oper_org, :line_name, :stn_name, :mng_no, :exit_no, :detail_loc,"
        " :length_mm, :width_mm, :start_floor, :end_floor, CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (line_name, stn_name, mng_no) DO UPDATE SET"
        " oper_org=EXCLUDED.oper_org, exit_no=EXCLUDED.exit_no, detail_loc=EXCLUDED.detail_loc,"
        " length_mm=EXCLUDED.length_mm, width_mm=EXCLUDED.width_mm,"
        " start_floor=EXCLUDED.start_floor, end_floor=EXCLUDED.end_floor,"
        " base_dt=EXCLUDED.base_dt, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    return _execute_batch(sql, rows)


def _replace_by_group(table: str, group_cols: tuple, insert_sql: str, rows: List[dict]) -> int:
    """파일 단위 전체 교체 — 그룹(선명 등)에 해당하는 기존 행을 지우고 다시 넣는다.

    연 1회 갱신되는 파일 자료는 행 자연키가 없어(같은 역에 '내부' 승강기가 여럿) UPSERT 로
    낡은 행을 걷어낼 수 없다. 한 트랜잭션에서 그룹 삭제 → 삽입한다.
    """
    if not rows:
        return 0
    if engine is None:
        raise RuntimeError('DB engine not configured (DB_URL)')
    groups = sorted({tuple(r[c] for c in group_cols) for r in rows})
    where = ' AND '.join('%s = :%s' % (c, c) for c in group_cols)
    with engine.begin() as conn:
        for g in groups:
            conn.execute(text('DELETE FROM %s WHERE %s' % (table, where)),
                         dict(zip(group_cols, g)))
        for row in rows:
            conn.execute(text(insert_sql), dict(row, created_by=CREATED_BY))
    return len(rows)


def replace_station_elevators(rows: List[dict]) -> int:
    sql = (
        "INSERT INTO poi_station_elevator_unit ("
        " oper_org, line_name, stn_name, unit_seq, exit_no, detail_loc,"
        " capacity_person, capacity_kg, base_dt, created_by)"
        " VALUES (:oper_org, :line_name, :stn_name, :unit_seq, :exit_no, :detail_loc,"
        " :capacity_person, :capacity_kg, CAST(:base_dt AS date), :created_by)"
    )
    return _replace_by_group('poi_station_elevator_unit', ('line_name',), sql, rows)


def replace_station_toilets(rows: List[dict]) -> int:
    sql = (
        "INSERT INTO poi_station_toilet_unit ("
        " oper_org, line_name, stn_name, disabled_yn, unit_seq, ground_dv, floor_no,"
        " gate_inout, exit_no, detail_loc, toilet_kind, base_dt, created_by)"
        " VALUES (:oper_org, :line_name, :stn_name, :disabled_yn, :unit_seq, :ground_dv, :floor_no,"
        " :gate_inout, :exit_no, :detail_loc, :toilet_kind, CAST(:base_dt AS date), :created_by)"
    )
    return _replace_by_group('poi_station_toilet_unit', ('line_name', 'disabled_yn'), sql, rows)


def upsert_station_platforms(rows: List[dict]) -> int:
    sql = (
        "INSERT INTO poi_station_platform ("
        " oper_org, line_name, stn_name, platform_no, updown, ground_dv, floor_no,"
        " platform_connect_yn, screen_door_yn, safety_plate_yn, base_dt, created_by)"
        " VALUES (:oper_org, :line_name, :stn_name, :platform_no, :updown, :ground_dv, :floor_no,"
        " :platform_connect_yn, :screen_door_yn, :safety_plate_yn, CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (line_name, stn_name, platform_no) DO UPDATE SET"
        " oper_org=EXCLUDED.oper_org, updown=EXCLUDED.updown, ground_dv=EXCLUDED.ground_dv,"
        " floor_no=EXCLUDED.floor_no, platform_connect_yn=EXCLUDED.platform_connect_yn,"
        " screen_door_yn=EXCLUDED.screen_door_yn, safety_plate_yn=EXCLUDED.safety_plate_yn,"
        " base_dt=EXCLUDED.base_dt, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    return _execute_batch(sql, rows)


def update_platform_gaps(rows: List[dict]) -> int:
    """이격거리 요약을 승강장 행에 붙인다. 승강장 행이 없으면(승강장 정보 파일 미적재) 요약만으로
    행을 만든다 — 상하행·안전발판은 NULL 로 남고 다음 승강장 파일 적재 때 채워진다."""
    sql = (
        "INSERT INTO poi_station_platform ("
        " oper_org, line_name, stn_name, platform_no, gap_min_cm, gap_max_cm, gap_avg_cm, door_cnt,"
        " base_dt, created_by)"
        " VALUES (:oper_org, :line_name, :stn_name, :platform_no, :gap_min_cm, :gap_max_cm, :gap_avg_cm,"
        " :door_cnt, CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (line_name, stn_name, platform_no) DO UPDATE SET"
        " gap_min_cm=EXCLUDED.gap_min_cm, gap_max_cm=EXCLUDED.gap_max_cm,"
        " gap_avg_cm=EXCLUDED.gap_avg_cm, door_cnt=EXCLUDED.door_cnt,"
        " updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    return _execute_batch(sql, rows)


def upsert_facilities(rows: List[dict]) -> int:
    sql = (
        "INSERT INTO poi_facility_accessibility ("
        " facl_inf_id, wfclt_id, facl_name, facl_type, addr, latitude, longitude, estb_date,"
        " elevator_yn, dis_toilet_yn, dis_parking_yn, entrance_ramp_yn, entrance_door_yn,"
        " approach_road_yn, guide_facility_yn, accessible_room_yn, eval_info_raw, base_dt, created_by)"
        " VALUES (:facl_inf_id, :wfclt_id, :facl_name, :facl_type, :addr, :latitude, :longitude, :estb_date,"
        " :elevator_yn, :dis_toilet_yn, :dis_parking_yn, :entrance_ramp_yn, :entrance_door_yn,"
        " :approach_road_yn, :guide_facility_yn, :accessible_room_yn, :eval_info_raw, CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (facl_inf_id) DO UPDATE SET"
        " wfclt_id=EXCLUDED.wfclt_id, facl_name=EXCLUDED.facl_name, facl_type=EXCLUDED.facl_type,"
        " addr=EXCLUDED.addr, latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,"
        " estb_date=EXCLUDED.estb_date,"
        # 기구표에서 나오는 플래그 8종과 원문은 "새 값이 NULL 이면 기존 값 유지"로 갱신한다.
        #   새 값이 NULL 이 되는 경우: (1) 기구표 조회를 끈 실행(KOWSI_FETCH_EVAL=OFF — 전 플래그 None)
        #   (2) 기구표 호출 실패 (3) 원천이 더미 응답을 준 경우 (4) 해당 없음으로 판정을 보류한 항목.
        #   EXCLUDED 값으로 그대로 덮으면 이런 실행 한 번으로 이전에 수집해 둔 Y/N 과 원문이 NULL 이 된다.
        #   새 값이 있으면(Y/N, 원문 문자열) 그 값으로 갱신된다.
        #   테이블명을 붙여(poi_facility_accessibility.x) 기존 행의 값을 가리킨다.
        " elevator_yn=COALESCE(EXCLUDED.elevator_yn, poi_facility_accessibility.elevator_yn),"
        " dis_toilet_yn=COALESCE(EXCLUDED.dis_toilet_yn, poi_facility_accessibility.dis_toilet_yn),"
        " dis_parking_yn=COALESCE(EXCLUDED.dis_parking_yn, poi_facility_accessibility.dis_parking_yn),"
        " entrance_ramp_yn=COALESCE(EXCLUDED.entrance_ramp_yn, poi_facility_accessibility.entrance_ramp_yn),"
        " entrance_door_yn=COALESCE(EXCLUDED.entrance_door_yn, poi_facility_accessibility.entrance_door_yn),"
        " approach_road_yn=COALESCE(EXCLUDED.approach_road_yn, poi_facility_accessibility.approach_road_yn),"
        " guide_facility_yn=COALESCE(EXCLUDED.guide_facility_yn, poi_facility_accessibility.guide_facility_yn),"
        " accessible_room_yn=COALESCE(EXCLUDED.accessible_room_yn, poi_facility_accessibility.accessible_room_yn),"
        " eval_info_raw=COALESCE(EXCLUDED.eval_info_raw, poi_facility_accessibility.eval_info_raw),"
        " base_dt=EXCLUDED.base_dt, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    return _execute_batch(sql, rows)


TOUR_BF_TABLE = 'poi_tour_bf_facility'
# 원천에서 사라진 행을 논리삭제할 때의 안전선 — 이번 수집 건수가 범위 안 기존 활성 행의
# 이 비율 미만이면 원천 일부 장애로 보고 삭제하지 않는다(갱신·추가는 그대로 한다).
TOUR_BF_MIN_RATIO = 0.5


def upsert_tour_bf(rows: List[dict], addr_filter: str = None) -> int:
    """poi_tour_bf_facility — 자연키(UNIQUE) 부재로 (fclt_name, sido_code) 조회 후 분기.

    v1.17.1 (#120): 수집 범위 안에서 원천에서 사라진 장소를 논리삭제(del_yn='Y')한다.
      - 범위: sido_code 일치 + 도로명·지번 주소가 addr_filter(기본 '경기도 안양시')로 시작.
        2025년에 다른 경로로 넣은 경기도 다른 시군 행은 범위 밖이라 건드리지 않는다.
      - 이번 실행에서 갱신·추가한 행이 아닌 범위 안 활성 행이 대상이다.
      - 방어: 범위 안 수집 건수가 기존 활성 행의 TOUR_BF_MIN_RATIO 미만이면 삭제하지 않고
        경고만 남긴다. 수집 0건이면 아무것도 하지 않는다.
      - 매칭: 같은 이름 후보 중 범위가 같은 행만(범위 밖 동명 행을 덮어쓰지 않음), 활성·주소
        일치 순. 한 실행에서 같은 행을 두 번 쓰지 않는다. 논리삭제된 행이 다시 나타나면 되살린다.
    전체가 한 트랜잭션. 반환은 갱신·추가한 행 수(종전과 같음).
    """
    import os
    from sqlalchemy import bindparam

    if not rows:
        logger.warning('%s: 수집 0건 — 기존 행을 유지하고 적재를 건너뜀', TOUR_BF_TABLE)
        return 0
    if engine is None:
        raise RuntimeError('DB engine not configured (DB_URL)')
    if addr_filter is None:
        addr_filter = os.getenv('TOUR_BF_ADDR_FILTER', '경기도 안양시').strip()
    table = TOUR_BF_TABLE
    # 같은 이름의 기존 행 후보 — 활성 행 우선, 그다음 최근에 논리삭제된 행(원천에 다시 나타난
    # 장소는 새 행을 만들지 않고 되살려 ID 를 유지한다).
    select_sql = text(
        "SELECT fclt_id, del_yn, addr_road, addr_jibun FROM " + table +
        " WHERE fclt_name=:fclt_name AND sido_code=:sido_code"
        " ORDER BY (del_yn='N') DESC, deleted_at DESC NULLS LAST, fclt_id"
    )
    insert_sql = text(
        "INSERT INTO " + table + " ("
        " sido_code, fclt_name, toilet_yn, elevator_yn, parking_yn, slope_yn,"
        " subway_yn, bus_stop_yn, wheelchair_rent_yn, tactile_map_yn, audio_guide_yn,"
        " nursing_room_yn, accessible_room_yn, stroller_rent_yn,"
        " addr_road, addr_jibun, latitude, longitude, detail_raw, base_dt, created_by)"
        " VALUES (:sido_code, :fclt_name, :toilet_yn, :elevator_yn, :parking_yn, :slope_yn,"
        " :subway_yn, :bus_stop_yn, :wheelchair_rent_yn, :tactile_map_yn, :audio_guide_yn,"
        " :nursing_room_yn, :accessible_room_yn, :stroller_rent_yn,"
        " :addr_road, :addr_jibun, :latitude, :longitude, CAST(:detail_raw AS jsonb),"
        " CAST(:base_dt AS date), :created_by)"
        " RETURNING fclt_id"
    )
    update_sql = text(
        "UPDATE " + table + " SET"
        " toilet_yn=:toilet_yn, elevator_yn=:elevator_yn, parking_yn=:parking_yn,"
        " slope_yn=:slope_yn, subway_yn=:subway_yn, bus_stop_yn=:bus_stop_yn,"
        " wheelchair_rent_yn=:wheelchair_rent_yn, tactile_map_yn=:tactile_map_yn,"
        " audio_guide_yn=:audio_guide_yn, nursing_room_yn=:nursing_room_yn,"
        " accessible_room_yn=:accessible_room_yn, stroller_rent_yn=:stroller_rent_yn,"
        " addr_road=:addr_road, addr_jibun=:addr_jibun, latitude=:latitude, longitude=:longitude,"
        " detail_raw=CAST(:detail_raw AS jsonb),"
        " base_dt=CAST(:base_dt AS date), updated_at=CURRENT_TIMESTAMP, updated_by=:created_by,"
        " del_yn='N', deleted_at=NULL, deleted_by=NULL"
        " WHERE fclt_id=:fclt_id"
    )
    scope_sql = text(
        "SELECT fclt_id, fclt_name FROM " + table +
        " WHERE del_yn='N' AND sido_code=:sido_code"
        "   AND (addr_road LIKE :prefix OR addr_jibun LIKE :prefix)"
    )
    delete_sql = text(
        "UPDATE " + table + " SET del_yn='Y', deleted_at=CURRENT_TIMESTAMP,"
        " deleted_by=:created_by, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
        " WHERE fclt_id IN :ids"
    ).bindparams(bindparam('ids', expanding=True))

    def _addr_in_scope(*addrs) -> bool:
        return bool(addr_filter) and any(str(a or '').strip().startswith(addr_filter) for a in addrs)

    def _in_scope(row: dict) -> bool:
        return _addr_in_scope(row.get('addr_road'), row.get('addr_jibun'))

    def _pick(candidates, row):
        """같은 이름 후보 중 하나. 이번 실행에서 이미 쓴 행은 제외(동명 장소가 한 행으로
        합쳐지지 않게), 범위가 같은 행만, 그 안에서 활성·주소 일치 순으로 고른다.
        범위 안 장소가 범위 밖(경기도 다른 시군) 동명 행을 덮어쓰지 않게 한다."""
        row_scope = _in_scope(row)
        road = str(row.get('addr_road') or '').strip()
        best = None
        for cand_id, del_yn, c_road, c_jibun in candidates:
            if cand_id in touched or _addr_in_scope(c_road, c_jibun) != row_scope:
                continue
            key = (del_yn != 'N', str(c_road or '').strip() != road)
            if best is None or key < best[0]:
                best = (key, cand_id)
        return best[1] if best else None

    sido_codes = {r['sido_code'] for r in rows}
    count = 0
    touched = set()
    with engine.begin() as conn:
        legacy = {}
        if addr_filter:
            for sido in sido_codes:
                for _id, name in conn.execute(scope_sql, {'sido_code': sido, 'prefix': addr_filter + '%'}):
                    legacy[_id] = name
        for row in rows:
            params = dict(row, created_by=CREATED_BY)
            fclt_id = _pick(conn.execute(select_sql, params).fetchall(), row)
            if fclt_id is not None:
                params['fclt_id'] = fclt_id
                conn.execute(update_sql, params)
                touched.add(fclt_id)
            else:
                touched.add(conn.execute(insert_sql, params).scalar())
            count += 1

        stale = sorted(i for i in legacy if i not in touched)
        in_scope = sum(1 for r in rows if _in_scope(r))
        if stale and legacy and in_scope < len(legacy) * TOUR_BF_MIN_RATIO:
            logger.warning(
                '%s: 범위(%s) 안 수집 %d건이 기존 활성 %d건의 %.0f%% 미만 — 원천 일부 장애로 보고 '
                '논리삭제 %d건을 보류', table, addr_filter, in_scope, len(legacy),
                TOUR_BF_MIN_RATIO * 100, len(stale))
        elif stale:
            conn.execute(delete_sql, {'ids': stale, 'created_by': CREATED_BY})
            logger.info('%s: 원천에서 사라진 %d건 논리삭제 — %s', table, len(stale),
                        ', '.join(legacy[i] for i in stale[:10]))
    return count


def upsert_emergency_support(rows: List[dict]) -> int:
    """poi_emergency_support — (유형, 이름, 도로명주소, 설치지점) 복합 자연키.

    같은 이름의 지회가 시군마다 있어 이름만으로는 구분되지 않는다(예: 경기도지체
    장애인협회 OO시지회). 주소를 키에 넣어야 시군 간 충돌이 없다.

    install_desc 는 01 v1.7.0 에서 키에 추가됐다. 같은 건물에 충전기가 여러 대
    설치된 경우(김포시청 제3별관 로비 / 민원동 장애인화장실 옆)를 구분한다.
    좌표까지 다른 사례가 있어(운정중앙역 환승센터 북측·남측 대합실) 병합하면
    정보가 사라진다. 수리센터·콜택시는 NULL 이라 종전과 동일하게 동작한다.
    """
    sql = (
        "INSERT INTO poi_emergency_support ("
        " support_type, sido_code, sgg_name, name, addr_road, addr_jibun, zip_code,"
        " latitude, longitude, tel, homepage, open_hours, install_desc, note, source,"
        " confidence, base_dt, created_by)"
        " VALUES (:support_type, :sido_code, :sgg_name, :name, :addr_road, :addr_jibun, :zip_code,"
        " :latitude, :longitude, :tel, :homepage, :open_hours, :install_desc, :note, :source,"
        " :confidence, CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (support_type, name, coalesce(addr_road, ''), coalesce(install_desc, ''))"
        " DO UPDATE SET"
        " sido_code=EXCLUDED.sido_code, sgg_name=EXCLUDED.sgg_name,"
        " addr_jibun=EXCLUDED.addr_jibun, zip_code=EXCLUDED.zip_code,"
        " latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,"
        " tel=EXCLUDED.tel, homepage=EXCLUDED.homepage, open_hours=EXCLUDED.open_hours,"
        " note=EXCLUDED.note, source=EXCLUDED.source, confidence=EXCLUDED.confidence,"
        " base_dt=EXCLUDED.base_dt, del_yn='N', deleted_at=NULL, deleted_by=NULL,"
        " updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    # install_desc 를 안 넣는 기존 호출부(수리센터·콜택시 로더)도 그대로 돌도록 채운다
    for row in rows:
        row.setdefault('install_desc', None)
    return _execute_batch(sql, rows)


def touch_latest_sync(ext_sys: str) -> None:
    """sys_ext_api_info.latest_sync_time 갱신 (수집 성공 후 1회)."""
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE sys_ext_api_info SET latest_sync_time=CURRENT_TIMESTAMP,"
            " updated_at=CURRENT_TIMESTAMP, updated_by=:by"
            " WHERE ext_sys=:ext_sys AND del_yn='N'"
        ), {'ext_sys': ext_sys, 'by': CREATED_BY})


# poi_public_toilet_info 적재 대상 테이블 — 통합 테스트가 임시 테이블로 바꿔 끼운다.
PUBLIC_TOILET_TABLE = 'poi_public_toilet_info'

# 수집 건수가 기존 활성 행의 이 비율 미만이면 원천 장애로 보고 적재를 중단한다.
PUBLIC_TOILET_MIN_RATIO = 0.7

PUBLIC_TOILET_COLUMNS = (
    'toilet_type', 'basis', 'addr_road', 'addr_jibun',
    'm_toilet_count', 'm_urinal_count', 'm_dis_toilet_count', 'm_dis_urinal_count',
    'm_child_toilet_count', 'm_child_urinal_count', 'f_toilet_count', 'f_dis_toilet_count',
    'f_child_toilet_count', 'managing_org', 'phone_number', 'open_time',
    'install_dt', 'latitude', 'longitude', 'owner_type', 'waste_process_type', 'safety_target_yn',
    'emg_bell_yn', 'emg_bell_location', 'cctv_yn', 'diaper_table_yn', 'diaper_table_location',
    'remodeled_dt', 'unisex_yn',
)


def _norm_addr(value) -> str:
    """주소 매칭 키 — 공백·괄호 병기 제거. '동안로 66 (호계동, 도서관)' 과 '동안로 66' 을 같게 본다."""
    import re
    text = re.sub(r'\([^)]*\)', '', str(value or ''))
    return re.sub(r'\s+', '', text)


def public_toilet_match_keys(name: str, addr_road, addr_jibun, region_prefix: str = '') -> list:
    """한 행이 가질 수 있는 매칭 키 목록 — (이름, 정규화 도로명) 과 (이름, 정규화 지번).

    이 테이블엔 원천 고유키가 없다. 이름만으로는 동명 시설(어린이공원 화장실 등)이 섞이므로
    주소를 반드시 같이 본다. 도로명·지번 중 하나만 같아도 같은 시설로 본다(과거 행은 둘 중
    하나가 비어 있는 경우가 많다 — 2026-09-05 실측: 안양 174행 중 도로명 결측 27).
    region_prefix(예: '경기도 안양시')가 주소 앞에 있으면 떼고 비교한다 — 과거 행에
    '만안구 안양로 317' 처럼 시 이름 없이 적재된 것이 있다(안양 7건 실측).
    """
    name = (name or '').strip()
    prefix = _norm_addr(region_prefix)
    keys = []
    for addr in (addr_road, addr_jibun):
        key = _norm_addr(addr)
        if prefix and key.startswith(prefix):
            key = key[len(prefix):]
        if name and key:
            keys.append((name, key))
    return keys


def sync_public_toilets(rows: List[dict], addr_filter: str = None) -> dict:
    """poi_public_toilet_info — 지역 단위 동기화 (GG_TOILET, 2026-09-05).

    원천 고유키가 없어 (이름 + 도로명 또는 지번 주소) 로 기존 행을 찾는다.
      · 매칭   → 제자리 UPDATE (toilet_id 보존, 소비자 참조 안정)
      · 미매칭 신규 → INSERT
      · 미매칭 기존 활성 행 → 논리삭제 (del_yn='Y') — 원천에서 사라진 시설
    전량 논리삭제 후 재적재 방식은 실행마다 PK 가 바뀌고 삭제 행이 누적되므로 채택하지 않았다.

    - 지역 판정: sido_code 일치 + (도로명·지번이 addr_filter 로 시작 OR 새 행에서 뽑은 '○○구'
      접두어로 시작 — '만안구 안양로 317' 처럼 시 이름 없이 적재된 과거 행 대응).
      LIKE '%안양%' 은 안양면·안양천로가 섞이므로 쓰지 않는다.
    - 개방시간 상세(open_time_detail)는 이 원천에 없다. UPDATE 시 새 값이 없으면 기존 값을
      유지한다(이름+주소가 같은 행에서만 — 이름만 같은 행으로의 복제는 하지 않는다).
    - 방어: 수집 0건이면 무동작. 수집 건수가 기존 활성 행의 PUBLIC_TOILET_MIN_RATIO 미만이면
      원천 장애로 보고 RuntimeError (부분 응답으로 정상 행을 지우는 사고 방지).
    - 전체가 한 트랜잭션. 반환: {'matched','inserted','deleted','kept_detail'}.
    """
    import os
    import re
    from sqlalchemy import bindparam

    result = {'matched': 0, 'inserted': 0, 'deleted': 0, 'kept_detail': 0}
    if not rows:
        logger.warning('%s: 수집 0건 — 기존 행을 유지하고 적재를 건너뜀', PUBLIC_TOILET_TABLE)
        return result
    if engine is None:
        raise RuntimeError('DB engine not configured (DB_URL)')
    if addr_filter is None:
        addr_filter = os.getenv('GG_TOILET_ADDR_FILTER', '경기도 안양시').strip()
    sido_code = rows[0]['sido_code']
    table = PUBLIC_TOILET_TABLE

    gu_names = set()
    for row in rows:
        for addr in (row.get('addr_road'), row.get('addr_jibun')):
            if not addr or (addr_filter and not addr.startswith(addr_filter)):
                continue
            tokens = addr.split()
            if len(tokens) >= 3 and tokens[2].endswith('구'):
                gu_names.add(tokens[2])
    gu_regex = '^(' + '|'.join(re.escape(g) for g in sorted(gu_names)) + r')(\s|$)' if gu_names else '^$'

    select_sql = text(
        "SELECT toilet_id, toilet_name, addr_road, addr_jibun, open_time_detail FROM " + table +
        " WHERE del_yn='N' AND sido_code=:sido_code"
        "   AND (addr_road LIKE :prefix OR addr_jibun LIKE :prefix"
        "        OR addr_road ~ :gu_regex OR addr_jibun ~ :gu_regex)"
    )
    set_clause = ', '.join('%s=:%s' % (c, c) for c in PUBLIC_TOILET_COLUMNS)
    update_sql = text(
        "UPDATE " + table + " SET " + set_clause +
        ", toilet_name=:toilet_name, sido_code=:sido_code,"
        " open_time_detail=COALESCE(:open_time_detail, open_time_detail),"
        " base_dt=CAST(:base_dt AS date), updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
        " WHERE toilet_id=:toilet_id"
    )
    delete_sql = text(
        "UPDATE " + table + " SET del_yn='Y', deleted_at=CURRENT_TIMESTAMP,"
        " deleted_by=:created_by, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
        " WHERE toilet_id IN :ids"
    ).bindparams(bindparam('ids', expanding=True))
    cols = ('sido_code', 'toilet_name') + PUBLIC_TOILET_COLUMNS + ('open_time_detail',)
    insert_sql = text(
        "INSERT INTO " + table + " (" + ', '.join(cols) + ", base_dt, created_by)"
        " VALUES (" + ', '.join(':' + c for c in cols) + ", CAST(:base_dt AS date), :created_by)"
    )

    with engine.begin() as conn:
        legacy = conn.execute(select_sql, {
            'sido_code': sido_code, 'prefix': addr_filter + '%', 'gu_regex': gu_regex,
        }).fetchall()
        if legacy and len(rows) < len(legacy) * PUBLIC_TOILET_MIN_RATIO:
            raise RuntimeError(
                '%s: 수집 %d건이 기존 활성 %d건의 %.0f%% 미만 — 원천 장애로 보고 적재 중단'
                % (table, len(rows), len(legacy), PUBLIC_TOILET_MIN_RATIO * 100))

        by_key: dict = {}
        ambiguous = set()
        for _id, name, road, jibun, detail in legacy:
            for key in public_toilet_match_keys(name, road, jibun, addr_filter):
                if key in by_key and by_key[key][0] != _id:
                    ambiguous.add(key)
                by_key.setdefault(key, (_id, detail))
        for key in ambiguous:
            logger.warning('%s: 이름+주소가 같은 기존 행이 둘 이상 — 매칭 제외: %r', table, key)
            by_key.pop(key, None)

        matched_ids = set()
        updates, inserts = [], []
        for row in rows:
            hit = None
            for key in public_toilet_match_keys(row['toilet_name'], row.get('addr_road'), row.get('addr_jibun'), addr_filter):
                if key in by_key and by_key[key][0] not in matched_ids:
                    hit = by_key[key]
                    break
            params = dict(row, created_by=CREATED_BY)
            if hit:
                matched_ids.add(hit[0])
                params['toilet_id'] = hit[0]
                if not row.get('open_time_detail') and (hit[1] or '').strip():
                    result['kept_detail'] += 1
                updates.append(params)
            else:
                inserts.append(params)

        stale_ids = [r[0] for r in legacy if r[0] not in matched_ids]
        if updates:
            conn.execute(update_sql, updates)
        if inserts:
            conn.execute(insert_sql, inserts)
        if stale_ids:
            conn.execute(delete_sql, {'ids': stale_ids, 'created_by': CREATED_BY})

    result.update(matched=len(updates), inserted=len(inserts), deleted=len(stale_ids))
    logger.info('%s(%s, sido=%s, 구=%s): 기존 활성 %d행 → 갱신 %d · 신규 %d · 논리삭제 %d '
                '(개방시간 상세 유지 %d)', table, addr_filter or '전체', sido_code,
                sorted(gu_names) or '-', len(legacy), len(updates), len(inserts), len(stale_ids),
                result['kept_detail'])
    return result


def upsert_public_toilets(rows: List[dict]) -> int:
    """mobility_pipeline 디스패치용 — 적재 행 수(갱신+신규)를 돌려준다."""
    summary = sync_public_toilets(rows)
    return summary['matched'] + summary['inserted']


# ============================================================================
# poi_culture_sports_facility — 경기 문화·체육 시설 4종 (GG_CULTURE, 2026-10-14)
# ============================================================================

# 적재 대상 테이블 — 통합 테스트가 임시 테이블로 바꿔 끼운다.
CULTURE_TABLE = 'poi_culture_sports_facility'

# 급감 방어: 한 번에 논리삭제될 행이 max(CULTURE_DROP_MIN_ABS, 기존 활성 × CULTURE_DROP_MAX_RATIO)
# 를 넘으면 원천 장애(부분 응답)로 보고 그 종류의 적재를 중단한다.
#   · 비율만 쓰면 5건짜리 종류는 1곳만 문을 닫아도(20%) 매번 중단된다 → 절대 하한 3건을 둔다.
#   · 절대 건수만 쓰면 32건짜리 종류가 1건으로 줄어도(31건 삭제) 성공으로 끝난다 → 비율 상한 30%.
#   예) 5건 → 3곳까지 삭제 허용, 32건 → 9곳, 260건 → 78곳.
# 실제로 대량 정리된 것이 맞으면 GG_CULTURE_ACCEPT_DROP=<종류>[,<종류>] 를 붙여 수동 실행한다.
CULTURE_DROP_MIN_ABS = 3
CULTURE_DROP_MAX_RATIO = 0.3

CULTURE_COLUMNS = (
    'sido_code', 'sigun_cd', 'sigun_nm', 'name', 'addr_road', 'addr_jibun', 'zip_code',
    'latitude', 'longitude', 'tel', 'homepage', 'install_report_dt', 'corp_name', 'source',
)


def _norm_name(value) -> str:
    """이름 매칭 키 — 공백·가운뎃점·하이픈 차이만 무시한다.

    괄호 안은 지우지 않는다. 공공시설은 '희망도서관(본관)'·'희망도서관(분관)' 처럼 괄호로 서로 다른
    시설을 구분하는 일이 많아, 지우면 다른 시설을 같은 시설로 이어 붙이게 된다.
    """
    import re
    return re.sub(r'[\s·\-]+', '', str(value or ''))


def culture_match_keys(name, addr_road, addr_jibun) -> list:
    """(정규화 이름, 정규화 도로명) 과 (정규화 이름, 정규화 지번). 둘 중 하나만 같아도 같은 시설로 본다.

    주소가 둘 다 없으면 (정규화 이름, '') 하나를 돌려준다 — DB 부분 유니크 인덱스가
    coalesce(addr_road, addr_jibun, '') 로 같은 키를 만들기 때문이다. 키가 없으면 매번 신규로
    분류돼 기존 활성 행과 유일성 제약에서 충돌한다.
    """
    n = _norm_name(name)
    if not n:
        return []
    keys = []
    for addr in (addr_road, addr_jibun):
        a = _norm_addr(addr)
        if a:
            keys.append((n, a))
    if not keys:
        keys.append((n, ''))
    return keys


def _sync_culture_type(conn, facility_type: str, rows: List[dict]) -> dict:
    """문화·체육 시설 한 종류 동기화 — 규칙은 _sync_facility_type 참고."""
    return _sync_facility_type(conn, _culture_spec(), facility_type, rows)


def _culture_spec() -> dict:
    # 표 이름은 부를 때 읽는다 — 통합 테스트가 CULTURE_TABLE 을 임시 표로 바꿔 끼운다.
    return {'table': CULTURE_TABLE, 'columns': CULTURE_COLUMNS, 'accept_env': 'GG_CULTURE_ACCEPT_DROP'}


def _sync_facility_type(conn, spec: dict, facility_type: str, rows: List[dict]) -> dict:
    """(이름+주소) 시설 목록 표 한 종류를 동기화한다(호출자가 트랜잭션을 연다).

    spec: {'table': 표, 'columns': facility_type 밖의 적재 칸, 'accept_env': 급감 허용 환경변수}
    반환 {'matched','revived','inserted','deleted'}.

    매칭 순서
      1) (이름, 주소) 가 같은 활성 행 → 제자리 UPDATE
      2) (이름, 주소) 가 같은 논리삭제 행 → 되살림 UPDATE(del_yn='N', facility_id 유지)
      3) 이름만 같고 양쪽 모두 짝이 하나뿐 → 주소가 바뀐 같은 시설로 보고 UPDATE
         (원천의 주소 정정·도로명 부여로 행이 끊기는 것을 막는다. 동명 시설이 둘 이상이면 하지 않는다)
      나머지 신규 → INSERT, 짝을 못 찾은 기존 활성 행 → 논리삭제
    적용 순서는 논리삭제 → UPDATE → INSERT 다. 활성 행 부분 유니크 인덱스가 문장마다 검사되므로,
    사라진 행이 키를 쥐고 있는 상태에서 다른 행이 같은 키로 바뀌면 충돌하기 때문이다.
    """
    import os
    from sqlalchemy import bindparam

    table = spec['table']
    columns = spec['columns']
    # 같은 표·종류를 두 실행(정기 + 수동)이 동시에 바꾸지 않게 트랜잭션 잠금을 건다.
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {'k': table + ':' + facility_type})
    existing = conn.execute(text(
        "SELECT facility_id, name, addr_road, addr_jibun, del_yn FROM " + table +
        " WHERE facility_type=:t ORDER BY facility_id"), {'t': facility_type}).fetchall()
    active = [r for r in existing if (r[4] or 'N') == 'N']

    def build_index(candidates):
        index, ambiguous = {}, set()
        for fid, name, road, jibun, _ in candidates:
            for key in culture_match_keys(name, road, jibun):
                if key in index and index[key] != fid:
                    ambiguous.add(key)
                index.setdefault(key, fid)
        for key in ambiguous:      # 같은 키를 가진 기존 행이 둘 이상이면 어느 쪽인지 모르므로 매칭하지 않는다
            index.pop(key, None)
        return index

    active_idx = build_index(active)
    deleted_idx = build_index([r for r in existing if (r[4] or 'N') != 'N'])

    used = set()
    plan = []            # (row, facility_id or None, kind)
    for row in rows:
        keys = culture_match_keys(row['name'], row.get('addr_road'), row.get('addr_jibun'))
        hit = next((active_idx[k] for k in keys if k in active_idx and active_idx[k] not in used), None)
        kind = 'matched'
        if hit is None:
            hit = next((deleted_idx[k] for k in keys if k in deleted_idx and deleted_idx[k] not in used), None)
            kind = 'revived'
        if hit is not None:
            used.add(hit)
        plan.append([row, hit, kind if hit is not None else None])

    # 3) 이름만 같은 1:1 짝 — 주소 변경
    left_new = {}
    for item in plan:
        if item[1] is None:
            left_new.setdefault(_norm_name(item[0]['name']), []).append(item)
    left_old = {}
    for fid, name, *_ in active:
        if fid not in used:
            left_old.setdefault(_norm_name(name), []).append(fid)
    moved = 0
    for nname, items in left_new.items():
        olds = left_old.get(nname, [])
        if nname and len(items) == 1 and len(olds) == 1:
            items[0][1], items[0][2] = olds[0], 'matched'
            used.add(olds[0])
            moved += 1

    stale_ids = [r[0] for r in active if r[0] not in used]
    # 급감 방어 — 매칭이 끝난 뒤 실제로 지워질 행 수로 판정한다(수집 건수만 보면 이름이 바뀐 행 등이 섞인다).
    drop_limit = max(CULTURE_DROP_MIN_ABS, int(len(active) * CULTURE_DROP_MAX_RATIO))
    accepted = {t.strip() for t in os.getenv(spec['accept_env'], '').split(',') if t.strip()}
    if len(stale_ids) > drop_limit and facility_type not in accepted:
        raise RuntimeError('%s(%s): 논리삭제 예정 %d건이 허용 %d건(기존 활성 %d건)을 넘음 — 원천 장애로 보고 적재 중단.'
                           ' 실제 정리가 맞으면 %s=%s 로 수동 실행'
                           % (table, facility_type, len(stale_ids), drop_limit, len(active),
                              spec['accept_env'], facility_type))
    # install_report_dt 는 문자열('YYYY-MM-DD')로 오므로 CAST 를 붙여 따로 지정한다(같은 칸을 두 번 쓰지 않게 제외).
    set_clause = ', '.join('%s=:%s' % (c, c) for c in columns if c != 'install_report_dt')
    update_sql = text(
        "UPDATE " + table + " SET " + set_clause + ", install_report_dt=CAST(:install_report_dt AS date),"
        " del_yn='N', deleted_at=NULL, deleted_by=NULL,"
        " updated_at=CURRENT_TIMESTAMP, updated_by=:created_by WHERE facility_id=:facility_id")
    insert_cols = ('facility_type',) + tuple(columns)
    insert_sql = text(
        "INSERT INTO " + table + " (" + ', '.join(insert_cols) + ", created_by) VALUES (" +
        ', '.join('CAST(:install_report_dt AS date)' if c == 'install_report_dt' else ':' + c
                  for c in insert_cols) + ", :created_by)")
    delete_sql = text(
        "UPDATE " + table + " SET del_yn='Y', deleted_at=CURRENT_TIMESTAMP, deleted_by=:created_by,"
        " updated_at=CURRENT_TIMESTAMP, updated_by=:created_by WHERE facility_id IN :ids"
    ).bindparams(bindparam('ids', expanding=True))

    def params(row, **extra):
        out = {c: row.get(c) for c in columns}
        out.update(extra)
        return out

    updates = [params(r, facility_id=fid, created_by=CREATED_BY) for r, fid, k in plan if fid is not None]
    inserts = [params(r, facility_type=facility_type, created_by=CREATED_BY) for r, fid, k in plan if fid is None]
    if stale_ids:
        conn.execute(delete_sql, {'ids': stale_ids, 'created_by': CREATED_BY})
    if updates:
        conn.execute(update_sql, updates)
    if inserts:
        conn.execute(insert_sql, inserts)

    result = {
        'matched': sum(1 for _, fid, k in plan if fid is not None and k == 'matched'),
        'revived': sum(1 for _, fid, k in plan if fid is not None and k == 'revived'),
        'inserted': len(inserts),
        'deleted': len(stale_ids),
    }
    logger.info('%s(%s): 기존 활성 %d행 → 갱신 %d(주소 변경 %d) · 되살림 %d · 신규 %d · 논리삭제 %d',
                table, facility_type, len(active), result['matched'], moved, result['revived'],
                result['inserted'], result['deleted'])
    return result


def sync_culture_facilities(rows: List[dict]) -> dict:
    """poi_culture_sports_facility 동기화 — 종류별로 트랜잭션을 따로 연다(종류별 격리).

    rows 에 없는 종류(수집 실패·0건)는 건드리지 않는다. 한 종류의 적재가 실패해도 다른 종류는
    반영되며, 실패 종류는 failures 로 돌려준다.
    반환: {'db_ok', 'db_fail', 'failures': [...], 'by_type': {종류: {...}}}
    """
    return _sync_facilities_by_type(rows, _culture_spec(), 'GG_CULTURE')


def _sync_facilities_by_type(rows: List[dict], spec: dict, ext_sys: str) -> dict:
    """종류별 트랜잭션으로 _sync_facility_type 을 돌린다(문화·체육·장애인복지시설 공통)."""
    table = spec['table']
    out = {'db_ok': 0, 'db_fail': 0, 'failures': [], 'by_type': {}}
    if not rows:
        logger.warning('%s: 수집 0건 — 적재를 건너뜀', table)
        return out
    if engine is None:
        raise RuntimeError('DB engine not configured (DB_URL)')
    by_type: dict = {}
    for row in rows:
        by_type.setdefault(row['facility_type'], []).append(row)
    for facility_type, part in by_type.items():
        try:
            with engine.begin() as conn:
                res = _sync_facility_type(conn, spec, facility_type, part)
        except Exception as exc:     # 종류별 격리 — 이 종류만 롤백되고 다음 종류로 간다
            logger.error('%s(%s) 적재 실패: %s', table, facility_type, exc)
            out['db_fail'] += len(part)
            out['failures'].append({'id': ext_sys + ':' + facility_type, 'stage': 'db',
                                    'reason': ('적재 실패: %s' % exc)[:300]})
            continue
        out['by_type'][facility_type] = res
        out['db_ok'] += res['matched'] + res['revived'] + res['inserted']
    return out


def upsert_culture_facilities(rows: List[dict]) -> dict:
    """mobility_pipeline 디스패치용 — 종류별 결과 dict 를 그대로 돌려준다(실패 목록 포함)."""
    return sync_culture_facilities(rows)


# ============================================================================
# poi_welfare_facility — 경기 장애인복지시설 5종 (GG_WELFARE, 01 v1.14.0)
# 키·매칭·급감 방어·종류별 격리는 문화·체육 시설과 같다(_sync_facility_type).
# ============================================================================

WELFARE_TABLE = 'poi_welfare_facility'

WELFARE_COLUMNS = (
    'sub_type', 'sido_code', 'sigun_cd', 'sigun_nm', 'name', 'addr_road', 'addr_jibun', 'zip_code',
    'latitude', 'longitude', 'tel', 'homepage', 'capacity_cnt', 'current_cnt', 'staff_capacity_cnt',
    'staff_current_cnt', 'install_report_dt', 'corp_name', 'source',
)


def _welfare_spec() -> dict:
    return {'table': WELFARE_TABLE, 'columns': WELFARE_COLUMNS, 'accept_env': 'GG_WELFARE_ACCEPT_DROP'}


def sync_welfare_facilities(rows: List[dict]) -> dict:
    """poi_welfare_facility 동기화 — 종류별 트랜잭션. 반환 형식은 sync_culture_facilities 와 같다."""
    return _sync_facilities_by_type(rows, _welfare_spec(), 'GG_WELFARE')


# ============================================================================
# stats_gg_dis_reg_emd — 경기 읍면동별 등록장애인 (GG_DIS_REG, 01 v1.14.0)
# ============================================================================

GG_DIS_REG_TABLE = 'stats_gg_dis_reg_emd'

# 급감 방어: 한 시군의 새 판 행 수가 비교 대상(같은 판을 다시 넣으면 그 판, 아니면 그 시군의 직전 최신 판)의
# 이 비율 미만이면 원천 장애(부분 응답)로 보고 그 시군을 건너뛴다. 확인 뒤 GG_DIS_REG_ACCEPT_DROP=<시군>.
GG_DIS_REG_MIN_RATIO = 0.5

GG_DIS_REG_COLUMNS = ('base_dt', 'sigun_nm', 'gu_nm', 'emd_nm', 'dis_type',
                      'tot_cnt', 'male_cnt', 'female_cnt', 'sev_cnt', 'sev_male_cnt', 'sev_female_cnt',
                      'nsev_cnt', 'nsev_male_cnt', 'nsev_female_cnt')


def _sync_gg_dis_reg_edition(conn, sigun: str, base_dt: str, rows: List[dict]) -> dict:
    """한 시군의 한 판(기준일)을 교체한다(호출자가 트랜잭션을 연다).

    같은 (시군, 기준일) 행만 지우고 새로 넣는다. 다른 기준일의 판은 그대로 남는다.
    반환 {'deleted', 'inserted', 'compare', 'kind'} — kind: same(같은 판 다시)·new(새 판)·first(첫 판)
    """
    import os
    table = GG_DIS_REG_TABLE
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {'k': table + ':' + sigun})
    same = conn.execute(text(
        "SELECT count(*) FROM " + table + " WHERE sigun_nm=:s AND base_dt=CAST(:d AS date)"),
        {'s': sigun, 'd': base_dt}).scalar() or 0
    if same:
        compare, kind = same, 'same'
    else:
        prev = conn.execute(text(
            "SELECT count(*) FROM " + table + " WHERE sigun_nm=:s AND base_dt = ("
            " SELECT max(base_dt) FROM " + table + " WHERE sigun_nm=:s AND base_dt < CAST(:d AS date))"),
            {'s': sigun, 'd': base_dt}).scalar() or 0
        compare, kind = prev, ('new' if prev else 'first')
    accepted = {t.strip() for t in os.getenv('GG_DIS_REG_ACCEPT_DROP', '').split(',') if t.strip()}
    if compare and len(rows) < compare * GG_DIS_REG_MIN_RATIO and sigun not in accepted and 'ALL' not in accepted:
        raise RuntimeError('%s(%s %s): 새 판 %d행이 비교 대상 %d행의 %d%% 미만 — 원천 장애로 보고 적재 중단.'
                           ' 실제 축소가 맞으면 GG_DIS_REG_ACCEPT_DROP=%s 로 수동 실행'
                           % (table, sigun, base_dt, len(rows), compare, int(GG_DIS_REG_MIN_RATIO * 100), sigun))
    deleted = conn.execute(text(
        "DELETE FROM " + table + " WHERE sigun_nm=:s AND base_dt=CAST(:d AS date)"),
        {'s': sigun, 'd': base_dt}).rowcount or 0
    insert_sql = text(
        "INSERT INTO " + table + " (" + ', '.join(GG_DIS_REG_COLUMNS) + ", created_by) VALUES (" +
        ', '.join('CAST(:base_dt AS date)' if c == 'base_dt' else ':' + c for c in GG_DIS_REG_COLUMNS) +
        ", :created_by)")
    payload = [dict({c: r.get(c) for c in GG_DIS_REG_COLUMNS}, created_by=CREATED_BY) for r in rows]
    if payload:
        conn.execute(insert_sql, payload)
    return {'deleted': deleted, 'inserted': len(payload), 'compare': compare, 'kind': kind}


def sync_gg_dis_reg(rows: List[dict]) -> dict:
    """stats_gg_dis_reg_emd 동기화 — (시군, 기준일) 판마다 트랜잭션을 따로 연다(시군별 격리).

    원천에 없는 시군·기준일의 기존 판은 건드리지 않는다(원천에서 시군이 빠져도 지난 판은 남는다).
    반환: {'db_ok', 'db_fail', 'failures': [...], 'by_type': {시군: {...}}}
    """
    out = {'db_ok': 0, 'db_fail': 0, 'failures': [], 'by_type': {}}
    if not rows:
        logger.warning('%s: 수집 0건 — 적재를 건너뜀', GG_DIS_REG_TABLE)
        return out
    if engine is None:
        raise RuntimeError('DB engine not configured (DB_URL)')
    groups: dict = {}
    for row in rows:
        groups.setdefault((row['sigun_nm'], row['base_dt']), []).append(row)
    for (sigun, base_dt), part in sorted(groups.items()):
        try:
            with engine.begin() as conn:
                res = _sync_gg_dis_reg_edition(conn, sigun, base_dt, part)
        except Exception as exc:
            logger.error('%s(%s %s) 적재 실패: %s', GG_DIS_REG_TABLE, sigun, base_dt, exc)
            out['db_fail'] += len(part)
            out['failures'].append({'id': 'GG_DIS_REG:' + sigun, 'stage': 'db',
                                    'reason': ('적재 실패: %s' % exc)[:300]})
            continue
        out['by_type'][sigun + ' ' + base_dt] = res
        out['db_ok'] += res['inserted']
    news = sum(1 for r in out['by_type'].values() if r['kind'] in ('new', 'first'))
    logger.info('%s: 판 %d개 반영(새 판 %d) · 실패 %d', GG_DIS_REG_TABLE, len(out['by_type']), news,
                len(out['failures']))
    return out


# ============================================================================
# 공개 데이터 요약표(sys_data_summary_info) 기준일 갱신 — 06 플랫폼 목록·상세의 '원천 갱신일'
# ============================================================================
# 수집 작업 → (요약표 sys_tbl_id, 원천 기준일을 구하는 SQL). SQL 이 None 이면 원천이 기준일을 주지 않는
# 목록(경기데이터드림 시설 목록 — 연 1회 갱신)이라 '원천과 대조해 확인한 날'(오늘)을 쓴다.
# 요약표에 행이 없으면(01 마이그레이션 미적용) 경고만 남긴다. KOSIS·ODCLOUD_EMP 는 각자 적재기가 갱신한다.
_ACTIVE = "coalesce(del_yn, 'N') = 'N'"
_BUS_DT_SQL = ("SELECT max(greatest(base_dt, low_bus_base_dt)) FROM tran_bus_route_info WHERE " + _ACTIVE)
SUMMARY_REFRESH = {
    # 버스는 두 작업(노선 GBIS·저상버스 GBIS_LOWFLOOR)이 한 표를 갱신한다 — 같은 식을 써서 어느 쪽이 늦게
    # 돌아도 같은 값이 되게 한다.
    'GBIS': ('tran_bus_route_info', _BUS_DT_SQL),
    'GBIS_LOWFLOOR': ('tran_bus_route_info', _BUS_DT_SQL),
    'KORAIL_CONV': ('poi_station_access_status',
                    "SELECT max(base_dt) FROM poi_station_access_status WHERE " + _ACTIVE),
    'KOWSI_FACL': ('poi_facility_accessibility',
                   "SELECT max(base_dt) FROM poi_facility_accessibility WHERE " + _ACTIVE),
    'TOUR_BF_API': ('poi_tour_bf_facility', "SELECT max(base_dt) FROM poi_tour_bf_facility WHERE " + _ACTIVE),
    'GG_TOILET': ('poi_public_toilet_info', "SELECT max(base_dt) FROM poi_public_toilet_info WHERE " + _ACTIVE),
    'GG_CULTURE': ('poi_culture_sports_facility', None),
    'GG_WELFARE': ('poi_welfare_facility', None),
    'GG_DIS_REG': ('stats_gg_dis_reg_emd', "SELECT max(base_dt) FROM stats_gg_dis_reg_emd"),
}


def refresh_data_summary(ext_sys: str) -> Optional[str]:
    """수집 작업이 실패 없이 끝난 뒤 요약표의 src_latest_chn_dt(원천 기준일)·sys_data_ref_dt(확인일)를 갱신한다.

    원천 기준일은 적재된 행에서 다시 구한 값으로 덮어쓴다(마이그레이션이 표가 비어 있을 때 넣은 임시 날짜도
    첫 적재 뒤 실제 기준일로 바뀐다). 실패해도 예외를 올리지 않는다
    (수집·적재 결과는 이미 반영됐고, 요약표는 다음 실행에서 다시 맞춰진다).
    반환: 기록한 원천 기준일 'YYYY-MM-DD' 또는 None(대상 아님·행 없음·실패)
    """
    import datetime
    spec = SUMMARY_REFRESH.get((ext_sys or '').upper())
    if spec is None or engine is None:
        return None
    table, date_sql = spec
    today = datetime.date.today().strftime('%Y-%m-%d')
    try:
        with engine.begin() as conn:
            src_dt = None
            if date_sql:
                value = conn.execute(text(date_sql)).scalar()
                if value is not None:
                    src_dt = value.strftime('%Y-%m-%d') if hasattr(value, 'strftime') else str(value)[:10]
            else:
                src_dt = today
            if not src_dt:
                logger.warning('요약표 갱신 건너뜀(%s): %s 에 기준일 값이 없음', ext_sys, table)
                return None
            n = conn.execute(text(
                "UPDATE sys_data_summary_info"
                "   SET src_latest_chn_dt = :d,"
                "       sys_data_ref_dt = :today, updated_at = CURRENT_TIMESTAMP"
                " WHERE sys_tbl_id = :t AND del_yn = 'N'"), {'d': src_dt, 'today': today, 't': table}).rowcount
        if not n:
            logger.warning('요약표 갱신 건너뜀(%s): sys_data_summary_info 에 sys_tbl_id=%s 행이 없음', ext_sys, table)
            return None
        logger.info('요약표 갱신(%s): %s 원천 기준일 %s · 확인일 %s', ext_sys, table, src_dt, today)
        return src_dt
    except Exception as exc:   # 요약표 갱신 실패가 수집 결과를 실패로 만들지 않는다
        logger.warning('요약표 갱신 실패(%s, %s): %s', ext_sys, table, exc)
        return None
