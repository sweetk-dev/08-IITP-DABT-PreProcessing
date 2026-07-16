"""이동편의 데이터 적재 모듈 — Issue #76.

대상 테이블(01-IITP-DABT-Database v1.1.0, PR #28):
- tran_bus_route_info          (GBIS)          ON CONFLICT (route_id)
- poi_station_access_status    (KORAIL_CONV)   ON CONFLICT (stn_cd)
- poi_station_wheelchair_lift  (KRNA_LIFT CSV) ON CONFLICT (line_name, stn_name, mng_no)
- poi_facility_accessibility   (KOWSI_FACL)    ON CONFLICT (facl_inf_id)
- poi_tour_bf_facility         (TOUR_BF_API)   자연키 없음 → (fclt_name, sido_code) 조회 후 UPDATE/INSERT

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


def upsert_station_access(rows: List[dict]) -> int:
    sql = (
        "INSERT INTO poi_station_access_status ("
        " stn_cd, stn_name, elevator_cnt, escalator_cnt, wheelchair_lift_cnt,"
        " dis_slope_yn, dis_toilet_yn, gen_toilet_yn, nursing_room_yn, info_center_yn,"
        " anyang_yn, base_dt, created_by)"
        " VALUES (:stn_cd, :stn_name, :elevator_cnt, :escalator_cnt, :wheelchair_lift_cnt,"
        " :dis_slope_yn, :dis_toilet_yn, :gen_toilet_yn, :nursing_room_yn, :info_center_yn,"
        " :anyang_yn, CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (stn_cd) DO UPDATE SET"
        " stn_name=EXCLUDED.stn_name, elevator_cnt=EXCLUDED.elevator_cnt,"
        " escalator_cnt=EXCLUDED.escalator_cnt, wheelchair_lift_cnt=EXCLUDED.wheelchair_lift_cnt,"
        " dis_slope_yn=EXCLUDED.dis_slope_yn, dis_toilet_yn=EXCLUDED.dis_toilet_yn,"
        " gen_toilet_yn=EXCLUDED.gen_toilet_yn, nursing_room_yn=EXCLUDED.nursing_room_yn,"
        " info_center_yn=EXCLUDED.info_center_yn, anyang_yn=EXCLUDED.anyang_yn,"
        " base_dt=EXCLUDED.base_dt, updated_at=CURRENT_TIMESTAMP, updated_by=:created_by"
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


def upsert_facilities(rows: List[dict]) -> int:
    sql = (
        "INSERT INTO poi_facility_accessibility ("
        " facl_inf_id, wfclt_id, facl_name, facl_type, addr, latitude, longitude, estb_date,"
        " elevator_yn, dis_toilet_yn, dis_parking_yn, entrance_ramp_yn, entrance_door_yn,"
        " approach_road_yn, eval_info_raw, base_dt, created_by)"
        " VALUES (:facl_inf_id, :wfclt_id, :facl_name, :facl_type, :addr, :latitude, :longitude, :estb_date,"
        " :elevator_yn, :dis_toilet_yn, :dis_parking_yn, :entrance_ramp_yn, :entrance_door_yn,"
        " :approach_road_yn, :eval_info_raw, CAST(:base_dt AS date), :created_by)"
        " ON CONFLICT (facl_inf_id) DO UPDATE SET"
        " wfclt_id=EXCLUDED.wfclt_id, facl_name=EXCLUDED.facl_name, facl_type=EXCLUDED.facl_type,"
        " addr=EXCLUDED.addr, latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,"
        " estb_date=EXCLUDED.estb_date, elevator_yn=EXCLUDED.elevator_yn,"
        " dis_toilet_yn=EXCLUDED.dis_toilet_yn, dis_parking_yn=EXCLUDED.dis_parking_yn,"
        " entrance_ramp_yn=EXCLUDED.entrance_ramp_yn, entrance_door_yn=EXCLUDED.entrance_door_yn,"
        " approach_road_yn=EXCLUDED.approach_road_yn, eval_info_raw=EXCLUDED.eval_info_raw,"
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
