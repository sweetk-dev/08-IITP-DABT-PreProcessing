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
- poi_tour_bf_facility         (TOUR_BF_API)   자연키 없음 → (fclt_name, sido_code) 조회 후 UPDATE/INSERT
- poi_public_toilet_info       (GG_TOILET)     자연키 없음 → (이름+주소) 매칭 UPDATE / 미매칭 INSERT / 사라진 행 논리삭제

created_by 는 DB 공통코드(sys_work_type) 시드 정본 'SYS-BACH' 를 따른다.
"""
from __future__ import annotations

import logging
from typing import List

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
        " estb_date=EXCLUDED.estb_date, elevator_yn=EXCLUDED.elevator_yn,"
        " dis_toilet_yn=EXCLUDED.dis_toilet_yn, dis_parking_yn=EXCLUDED.dis_parking_yn,"
        " entrance_ramp_yn=EXCLUDED.entrance_ramp_yn, entrance_door_yn=EXCLUDED.entrance_door_yn,"
        " approach_road_yn=EXCLUDED.approach_road_yn, guide_facility_yn=EXCLUDED.guide_facility_yn,"
        " accessible_room_yn=EXCLUDED.accessible_room_yn, eval_info_raw=EXCLUDED.eval_info_raw,"
        " base_dt=EXCLUDED.base_dt, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
    )
    return _execute_batch(sql, rows)


def upsert_tour_bf(rows: List[dict]) -> int:
    """poi_tour_bf_facility — 자연키(UNIQUE) 부재로 (fclt_name, sido_code) 조회 후 분기."""
    if not rows:
        return 0
    if engine is None:
        raise RuntimeError('DB engine not configured (DB_URL)')
    select_sql = text(
        "SELECT fclt_id FROM poi_tour_bf_facility"
        " WHERE fclt_name=:fclt_name AND sido_code=:sido_code AND del_yn='N'"
    )
    insert_sql = text(
        "INSERT INTO poi_tour_bf_facility ("
        " sido_code, fclt_name, toilet_yn, elevator_yn, parking_yn, slope_yn,"
        " subway_yn, bus_stop_yn, wheelchair_rent_yn, tactile_map_yn, audio_guide_yn,"
        " nursing_room_yn, accessible_room_yn, stroller_rent_yn,"
        " addr_road, addr_jibun, latitude, longitude, base_dt, created_by)"
        " VALUES (:sido_code, :fclt_name, :toilet_yn, :elevator_yn, :parking_yn, :slope_yn,"
        " :subway_yn, :bus_stop_yn, :wheelchair_rent_yn, :tactile_map_yn, :audio_guide_yn,"
        " :nursing_room_yn, :accessible_room_yn, :stroller_rent_yn,"
        " :addr_road, :addr_jibun, :latitude, :longitude, CAST(:base_dt AS date), :created_by)"
    )
    update_sql = text(
        "UPDATE poi_tour_bf_facility SET"
        " toilet_yn=:toilet_yn, elevator_yn=:elevator_yn, parking_yn=:parking_yn,"
        " slope_yn=:slope_yn, subway_yn=:subway_yn, bus_stop_yn=:bus_stop_yn,"
        " wheelchair_rent_yn=:wheelchair_rent_yn, tactile_map_yn=:tactile_map_yn,"
        " audio_guide_yn=:audio_guide_yn, nursing_room_yn=:nursing_room_yn,"
        " accessible_room_yn=:accessible_room_yn, stroller_rent_yn=:stroller_rent_yn,"
        " addr_road=:addr_road, addr_jibun=:addr_jibun, latitude=:latitude, longitude=:longitude,"
        " base_dt=CAST(:base_dt AS date), updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
        " WHERE fclt_id=:fclt_id"
    )
    count = 0
    with engine.begin() as conn:
        for row in rows:
            params = dict(row, created_by=CREATED_BY)
            found = conn.execute(select_sql, params).fetchone()
            if found:
                params['fclt_id'] = found[0]
                conn.execute(update_sql, params)
            else:
                conn.execute(insert_sql, params)
            count += 1
    return count


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
