# 장애인 이동편의 공공데이터 멀티소스 수집·전처리 툴

![version](https://img.shields.io/badge/version-v1.16.1-blue)

## 개요
장애인 이동편의·편의시설 관련 공공데이터를 소스별 수집 어댑터로 수집해, 파일 저장·정제·DB 적재까지 처리하는 Python 기반 툴입니다.
`--ext-sys` 로 수집 대상 소스를 선택하며, 미지정 시 KOSIS 가 default 입니다(후방호환).

- 수집 어댑터 7종 — KOSIS 통계 / 한국관광공사 무장애여행(TOUR_BF_API) / 경기버스정보 노선·저상버스(GBIS·GBIS_LOWFLOOR) / 경기데이터드림 공중화장실(GG_TOILET) / 철도 역사 편의시설(KORAIL_CONV) / 장애인편의시설(KOWSI_FACL)
- 옵션에 따라 파일 저장만 또는 DB 적재까지 수행
- 통합 테이블 자동 이관 · 과거 데이터 정리
- 보조기기 수리센터·충전기 등 CSV 원천 적재 스크립트 별도 제공
- 현행 아키텍처: [docs/design/107-current-architecture.md](docs/design/107-current-architecture.md)

> 이슈 #29 (v1.5.0) — 멀티 외부 API 소스 지원. `--ext-sys` CLI 또는 `EXT_SYS` 환경변수로 수집 대상 소스를 선택. 미지정 시 KOSIS 가 default (후방호환).

> v1.16.0 — 긴급대응 수리센터 전국 원천 2종 추가와 충전기 설치 지점 보존. 국민건강보험공단 **보조기기 급여 등록업소**(`scripts/load_nhis_assist_store_csv.py`, 전동휠체어 취급 전국 2,022건, 주소·전화 보유)와 중앙보조기기센터 **전국 보조기기센터 33곳**(`scripts/load_knat_center_csv.py`)을 넣는다. 지정업체 명부의 시군구는 사업장 위치가 아니라 수리비를 지원하는 관할이라, 등록업소 주소로 실제 위치를 채운다. 충전기는 01 v1.7.0 `install_desc` 를 채워 같은 건물의 설치 지점을 개별 행으로 보존한다(병합 시 195행 소실되던 문제).

> v1.15.0 — 무장애여행 편의정보 **원문 보존**(`poi_tour_bf_facility.detail_raw`, 01 v1.6.0)과 **보조기기 수리 서비스센터 적재**(`scripts/load_gg_assist_repair_csv.py` → `poi_emergency_support`) 추가. 원문을 남기면 판정 규칙이 바뀌어도 원천 재호출 없이 다시 파싱할 수 있고, "본관 옆 부스" 같은 위치 설명을 안내에 쓸 수 있다.
>
> v1.14.0 — 한국관광공사 무장애여행 파싱 정정. `flag_from_text` 가 '없' 만 보고 부정으로 판정해 "주출입구는 단차가 없어 휠체어 접근 가능함" 같은 **긍정 서술을 뒤집던** 문제를 고치고(안양 4건 실측), 접근로 정본인 `route` 필드와 `auditorium`(장애인 관람석)을 매핑에 추가했다. 안양 13건의 `slope_yn` 이 Y 4건에서 Y 11건으로 정정된다.
>
> v1.13.0 — 경기버스정보 **저상버스 노선현황(전일 기준)** 수집기 `GBIS_LOWFLOOR` 추가. 정적 노선 API 에 없던 `tran_bus_route_info.low_bus_yn` 을 페이지 표의 routeId 로 매일 갱신한다(01 v1.5.0 `low_bus_base_dt`). 경로 서비스의 저상버스 우선 모드가 1차 필터로 쓴다.
>
> v1.12.0 — 01 v1.4.0 컬럼 반영. 기구표 항목 「유도 및 안내 설비」→`guide_facility_yn`, 「장애인사용가능객실」→`accessible_room_yn`(숙박시설 외에는 판정하지 않아 NULL), 화장실 남녀공용 →`unisex_yn`. 기존 행은 `scripts/backfill_facility_eval_flags.py` 로 저장된 기구표 원문을 다시 파싱해 채운다(재수집 불필요).
>
> v1.11.0 — 경기데이터드림 **공중화장실 현황(제공표준)** 수집기 `GG_TOILET` 추가. 행안부 표준데이터가 좌표 제공을 중단(2025-02)해 17%였던 안양 좌표 보유율을 98%로 올린다. 원천 고유키가 없어 (이름+주소) 매칭으로 제자리 갱신·신규·논리삭제를 한 트랜잭션에서 처리한다.
>
> v1.10.0 — 국가철도공단 **역사 설비 CSV**(엘리베이터·화장실·장애인화장실·승강장·승강장이격거리, 수도권 1/4호선)를 설비 단위로 적재하는 `scripts/load_krna_station_csv.py` 추가. 헤더로 파일 종류를 자동 판별하며 01 v1.3.0 테이블 `poi_station_elevator_unit` / `poi_station_toilet_unit` / `poi_station_platform` 에 넣는다. 연 1회 파일 갱신이므로 배치 대상이 아니다.

> 이슈 #85 (v1.9.0) — GBIS 수집이 노선 메타에 이어 **경유정류소(정류장 좌표 + 노선-정류장 관계)** 까지 처리한다. 적재 테이블은 `tran_bus_route_info` / `tran_bus_station_info` / `tran_bus_route_station` 3종. 노선 메타만 갱신하려면 `GBIS_COLLECT_STATIONS=false`.

## 주요 기능
- DB에서 KOSIS API 연동 정보 조회
- KOSIS API로 데이터/메타 수집
- 날짜별 폴더 및 규칙에 맞는 파일명으로 저장
- 옵션에 따라 파일 저장만 또는 DB 삽입까지 수행
- 통계 데이터 통합 테이블 자동 이관
- 과거 데이터 자동 정리

## 실행 방법
```bash
# KOSIS (default, 후방호환)
python main.py --mode file
python main.py --mode db

# 다른 외부 소스 (예: 공공데이터포털)
python main.py --mode file --ext-sys DATA_GO_KR
# 또는 환경변수로
EXT_SYS=DATA_GO_KR python main.py --mode db
```

## 실행 옵션
- `--mode file` : API 데이터 파일로만 저장
- `--mode db`   : API 데이터 파일 저장 후 DB 삽입
- `--ext-sys <KEY>` : 외부 시스템 식별자 (예: `KOSIS`, `DATA_GO_KR`). 미지정 시 `EXT_SYS` 환경변수, 그래도 없으면 `KOSIS`.

### ext_sys 우선순위
```
CLI(--ext-sys)  >  env(EXT_SYS)  >  default 'KOSIS'
```
값은 항상 대문자로 정규화되어 `sys_ext_api_info.ext_sys` 와 매칭됩니다.

## 파일 저장 규칙
- 실행 위치 기준, 오늘 날짜(YYYYMMDD) 폴더 생성
- 디렉터리 패턴 (이슈 #29):
    - KOSIS (default): `kosis_data/<YYYYMMDD>/{data,meta,latest}` (기존 패턴 그대로 유지 — 후방호환)
    - 그 외 ext_sys: `ext_data/<EXT_SYS>/<YYYYMMDD>/{data,meta,latest}`
- 파일명 예시:
    - Data: `{순서}-{stat_title}-{from_year}-{to_year}_{yyyyMMddHHmmss}.json`
    - Meta: `{순서}-{stat_title}-{from_year}-{to_year}_{yyyyMMddHHmmss}.xml`
    - Latest: `{순서}-{stat_title}-{from_year}-{to_year}_{yyyyMMddHHmmss}.xml`
- 같은 데이터의 Data/Meta/Latest 파일은 순서 일치

## DB 연동 정보

### 조회 대상 테이블
- `sys_ext_api_info`: 외부 API 정보 조회
- `sys_stats_src_api_info`: 통계 소스 API 정보 조회  
- `stats_src_data_info`: 통계 소스 데이터 정보 조회

### 업데이트 대상 테이블
- `stats_kosis_origin_data`: 원본 데이터 저장
- `stats_kosis_metadata_code`: 메타데이터 저장
- `stats_*` (통합 테이블들): 통계별 통합 데이터 저장
  - 예: `stats_dis_hlth_disease_cost_sub`, `stats_dis_reg_natl_by_new` 등
- `stats_src_data_info`: 통계 소스 정보 업데이트
- `sys_data_summary_info`: 시스템 데이터 요약 정보 업데이트
- `sys_stats_src_api_info`: API 동기화 정보 업데이트
- `sys_ext_api_info`: 외부 API 동기화 정보 업데이트

### DB 처리 과정
1. **데이터 수집**: KOSIS API에서 데이터/메타 수집
2. **원본 저장**: `stats_kosis_origin_data` 테이블에 원본 데이터 저장
3. **통합 이관**: 통계별 통합 테이블로 데이터 이관
4. **메타데이터 저장**: `stats_kosis_metadata_code` 테이블에 메타데이터 저장
5. **정보 업데이트**: 관련 테이블들의 최신화 정보 업데이트
6. **과거 데이터 정리**: 이전 버전 데이터 자동 삭제

## 폴더/파일 구조 예시
```
02.kosisDatApiLoader/
├── main.py
├── db.py
├── db_processing.py
├── kosis_api.py
├── file_utils.py
├── config.py
├── requirements.txt
├── logs/
│   ├── 20250925.log
│   └── db_20250925.log
├── kosis_data/
│   └── 20250925/
│       ├── data/
│       ├── meta/
│       └── latest/
└── README.md
```

## 필요 패키지 설치
```bash
pip install -r requirements.txt
```

## 환경설정

환경 변수는 `.env` 파일로 관리합니다. 

```bash
cp .env.example .env
```

> `DB_URL`만 필수입니다. 나머지는 기본값이 설정되어 있어 생략 가능합니다.
### 환경변수 목록

| 변수명 | 필수 | 기본값 | 허용값 | 설명 |
|--------|:----:|--------|--------|------|
| `EXT_SYS` | — | `KOSIS` | `KOSIS` `DATA_GO_KR` ... | 수집 대상 외부 시스템 (CLI `--ext-sys` 가 우선) |
| `DB_URL` | ✅ | — | `postgresql://...` | PostgreSQL 접속 URL |
| `DB_BATCH_SIZE` | — | `100` | 정수 | DB 배치 삽입 크기 |
| `LOG_LEVEL` | — | `INFO` | `DEBUG` `INFO` `WARNING` `ERROR` | 로그 출력 레벨 |
| `EXT_API_INFO_KOSIS_SYS` | — | `KOSIS` | 문자열 | KOSIS 시스템 구분 코드 |
| `PARALLEL_WORKERS_FILE` | — | `4` | 정수 | 파일 저장 병렬 워커 수 |
| `PARALLEL_WORKERS_DB` | — | `2` | 정수 | DB 삽입 병렬 워커 수 |
| `DATA_COLLECTION_SCOPE` | — | `ALL` | `ALL` `PARTIAL` | 데이터 수집 범위 |
| `CHECK_DATA_LATEST_DATE_MODE` | — | `OFF` | `ON` `OFF` | KOSIS 최신 변경일 기준 업데이트 여부 |
### 빠른 시작 예시

```env
# 최소 필수 설정 (DB_URL 은 반드시 입력)
DB_URL=postgresql://myuser:mypassword@localhost:5432/iitp_dabt

# 성능 튜닝 (선택)
PARALLEL_WORKERS_FILE=8
PARALLEL_WORKERS_DB=4
DB_BATCH_SIZE=200
```

> **주의**: `DB_URL` 미설정 시 임포트 단계에서 `create_engine()` 호출로 비정상 종료됩니다 (이슈 #17 참조).

## 주요 유의사항
- **트랜잭션 처리**: 통계 단위 커밋/롤백 (오류 발생 통계만 개별 롤백·실패 집계). 실패가 있으면 동기화 시각 갱신과 과거 데이터 cleanup 을 보류하고 종료코드 2로 종료
- **데이터 중복 방지**: 동일한 날짜 데이터는 기존 데이터 삭제 후 신규 삽입
- **과거 데이터 관리**: 자동으로 이전 버전 데이터 정리
- **에러 처리**: 필수 테이블 누락 시 프로그램 중단
- **로그 관리**: 실행 로그는 `logs/` 폴더에 날짜별 저장

## 공중화장실 적재 (경기데이터드림 `GG_TOILET`, 2026-09-05)

행안부 전국표준데이터가 2025년 2월부터 좌표를 빼면서 `poi_public_toilet_info` 의 안양 좌표 보유율이 17%였다.
경기데이터드림 `Publtolt` API(호출 제한 없음, 좌표 제공)로 대체한다. 인증키는 경기데이터드림에서 따로 발급한다.

```bash
python main.py --mode file --ext-sys GG_TOILET   # ext_data/GG_TOILET/<날짜>/rows.json 만 저장
python main.py --mode db   --ext-sys GG_TOILET   # (이름+주소) 매칭 동기화
```

- 이 테이블엔 원천 고유키가 없어 **(이름+주소) 로 기존 행을 찾아 제자리 갱신(id 보존)·미매칭 신규 INSERT·원천에서 사라진 행 논리삭제**를 한 트랜잭션에서 처리한다. 대상 지역은 `GG_TOILET_ADDR_FILTER`(기본 `경기도 안양시`) 로 정한다. 수집 0건이면 무동작, 기존 활성 행의 70% 미만이면 중단
- 원천에 없는 개방시간 상세(`open_time_detail`)는 매칭된 기존 행의 값을 유지한다
- 필드 대응 근거·한계는 `docs/poi_public_toilet_info_안양_갱신_GG_TOILET_2026-09-05.md`

## 보조기기 수리 서비스센터 적재 (경기데이터드림 `GG_ASSIST_REPAIR`, v1.15.0)

원천은 경기데이터드림 「경기도_시군별 보조기기 수리 서비스센터 현황」(제공: 경기도장애인복지종합지원센터(누림센터))이다.
**OpenAPI 가 아니라 파일로 제공되고 갱신 주기가 연 1회**라, KRNA_STN 과 같이 배치 등록 없이 갱신 시 수동 재실행한다.

```bash
python scripts/load_gg_assist_repair_csv.py --csv data/gg_assist_repair_20260619.csv --base-dt 2026-06-19
python scripts/load_gg_assist_repair_csv.py --csv <파일> --dry-run      # 파싱 결과만 확인
python scripts/load_gg_assist_repair_csv.py --csv <파일> --region ""     # 전 시군 적재
```

- 적재 대상: `poi_emergency_support` (`support_type='repair'`)
- 기본 범위는 안양 생활권(안양·도 단위 광역 센터·군포·의왕·과천·광명·안산). `--region` 으로 조정한다
- 원천에 **운영시간 항목이 없다.** 야간 대응 가능 여부 판단에 필요한 값이므로 추정치를 넣지 않고 비워 둔다
- 전화 `000-000-0000`, 홈페이지 `www.` 는 자리표시자라 빈 값으로 적재한다

## 저상버스 운행 노선 적재 (경기버스정보 `GBIS_LOWFLOOR`, v1.13.0)

GBIS 정적 노선 API(`getBusRouteInfoItemv2`)에는 저상버스 항목이 없어 `tran_bus_route_info.low_bus_yn` 이 비어 있었다.
경기버스정보 저상버스 노선현황 페이지(`lowfloorBus.action?cmd=lowfloorAuto`, "어제 기준" 집계)는 경기도 전 노선의
저상 운행 목록을 한 페이지에 주고, 노선번호 셀에 GBIS `routeId` 가 함께 있다. 인증키는 필요 없다.

```bash
python main.py --mode file --ext-sys GBIS_LOWFLOOR   # ext_data/GBIS_LOWFLOOR/<날짜>/rows.json 만 저장
python main.py --mode db   --ext-sys GBIS_LOWFLOOR   # low_bus_yn / low_bus_base_dt 갱신
```

- 매칭은 **routeId** 로 한다. 같은 노선번호가 여러 운수사에 있어(5번 삼영운수/안양-편안운수, 6번 삼영운수/안양-학운교통, 9번 안양-신안운수/삼영운수) 번호 매칭은 쓰지 않는다. 노선번호·기점·종점·운행시간대·배차간격·운수사는 원본 보존용
- 표에 있는 노선 `Y`, 경기도 관할(`admin_name`) 노선 중 표에 없는 노선 `N`, `low_bus_base_dt` = 수집일 전일. 페이지를 못 읽거나 행이 0건이면 기존 값을 건드리지 않는다
- 운행시간대·배차가 비어 있는 행(주말만 운행하는 노선 등)은 해당 요일만 `null` 로 둔다
- 표는 어제 기준이므로 **매일 1회** 실행한다. 실시간 차량 단위 저상 여부(lowPlate)는 경로 서비스가 직접 조회한다

```bash
# crontab 예시 — 매일 04:30
30 4 * * * cd <PROJECT_DIR> && EXT_SYS=GBIS_LOWFLOOR <PROJECT_DIR>/scripts/run_collect.sh >> <PROJECT_DIR>/logs/cron.log 2>&1
```

## 건물 편의시설 플래그 백필 (`KOWSI_FACL`, v1.12.0)

01 v1.4.0 에서 `poi_facility_accessibility` 에 `guide_facility_yn`·`accessible_room_yn` 이 추가됐다. 이미 저장된 기구표 원문(`eval_info_raw`)을 수집기와 같은 파서로 다시 읽어 두 컬럼만 채운다.

```bash
python scripts/backfill_facility_eval_flags.py --dry-run   # 건수만
python scripts/backfill_facility_eval_flags.py             # 500건 단위 커밋, 재실행 안전
```

- `accessible_room_yn` 은 항목이 있을 때만 `Y`, 없으면 `N` 이 아니라 NULL — 숙박시설이 아닌 청사·어린이집을 "객실 미설치"로 기록하지 않기 위해서다. 안양 실측: `Y` 3건(기숙사 2·일반숙박시설 1)
- 기구표 미작성(더미 응답)은 두 컬럼 모두 NULL 로 남는다

## 역사 설비 CSV 적재 (국가철도공단, v1.10.0)

공공데이터포털에서 받은 파일(cp949)을 그대로 넣는다. 파일명은 자유 — 종류는 헤더로 판별한다.
화장실·장애인화장실은 헤더가 같으므로 파일명에 `장애인` 이 있거나 `--disabled` 를 주면 `disabled_yn='Y'` 로 적재한다.
이격거리 파일은 출입문 단위 원자료를 승강장 단위(min/max/avg/출입문 수)로 요약해 `poi_station_platform` 에 붙인다.

```bash
python scripts/load_krna_station_csv.py --dir ext_data/krna_20260902 --base-dt 2025-06-30
python scripts/load_krna_station_csv.py --csv 국가철도공단_수도권1호선_엘리베이터_20250630.csv --dry-run
```

| 파일(포털 ID) | 대상 테이블 | 적재 방식 |
|---|---|---|
| 수도권1/4호선_엘리베이터 (15041389 / 15041392) | `poi_station_elevator_unit` | 선명 단위 전체 교체 |
| 수도권1/4호선_화장실 (15041254 / 15041257) · 장애인화장실 (15041222 / 15041225) | `poi_station_toilet_unit` | 선명·disabled_yn 단위 전체 교체 |
| 수도권1/4호선_승강장_정보 (15041192 / 15041194) | `poi_station_platform` | UPSERT (line, stn, platform_no) |
| 수도권1/4호선_승강장이격거리 (15041514 / 15041517) | `poi_station_platform` gap_* | 승강장 단위 요약 UPDATE |

## 스케줄러 실행 (정기 수집)

정기 수집은 OS 스케줄러(cron)로 `scripts/run_collect.sh` 를 호출합니다. 래퍼는 단일 인스턴스(flock)를 보장하고, 프로젝트 루트를 자동 탐지하며, `.venv` 가 있으면 활성화한 뒤 `python main.py --mode db` 를 실행합니다.

```bash
# crontab 예시 — 매월 3일/18일 03:00 (시스템 타임존 기준)
0 3 3,18 * * <PROJECT_DIR>/scripts/run_collect.sh >> <PROJECT_DIR>/logs/cron.log 2>&1
```

- 종료 코드: `0` 성공 / `2` 일부 통계 적재 실패(부분 완료) / `1` 치명적 오류
- 실행 요약: 매 실행 1줄이 `logs/run_summary.log` 에 누적됩니다(기존 날짜별 로그는 그대로 유지).

## 참고
- Python 3.8 이상 권장
- DB 종류: PostgreSQL 권장
- 메모리: 대용량 데이터 처리 시 충분한 메모리 필요 

## 라이선스

이 프로젝트는 MIT 라이선스로 배포됩니다. 전문은 [LICENSE](LICENSE) 파일을 참고하십시오.

본 연구는 정부(과학기술정보통신부)의 재원으로 정보통신기획평가원의 지원을 받아 수행된 연구입니다.
(연구개발과제번호 RS-2024-003976, 데이터 기반 장애인 데이터 탐색·활용 해결기술 개발)
