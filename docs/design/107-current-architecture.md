# 현행 아키텍처 설계 — 이동편의 공공데이터 멀티소스 수집·전처리

> 이슈 #107 설계 문서 | 기준 버전: v1.16.0
> 선행 문서: `docs/design/26-multi-source-architecture.md` (이슈 #26, v1.2.0 시점 확장 설계)

## 목차

1. [문서의 목적과 범위](#1-문서의-목적과-범위)
2. [시스템 개요 — 두 개의 수집 계열](#2-시스템-개요--두-개의-수집-계열)
3. [실행 구조](#3-실행-구조)
4. [수집기 계층](#4-수집기-계층)
5. [어댑터별 규격](#5-어댑터별-규격)
6. [적재 계층](#6-적재-계층)
7. [CSV 원천 적재 계열](#7-csv-원천-적재-계열)
8. [설정 체계](#8-설정-체계)
9. [실행·운영](#9-실행운영)
10. [오류 처리와 재시도 원칙](#10-오류-처리와-재시도-원칙)
11. [신규 소스 확장 절차](#11-신규-소스-확장-절차)
12. [테스트 구성](#12-테스트-구성)
13. [선행 설계(#26) 대비 변경 요약](#13-선행-설계26-대비-변경-요약)

---

## 1. 문서의 목적과 범위

### 1.1 목적

이 문서는 `08-IITP-DABT-PreProcessing` 의 **현행 아키텍처 정본**이다. 이슈 #26 설계 문서는 KOSIS 단일 소스를 멀티소스로 확장하기 위한 당시의 설계안이며, 그 설계가 실제로 구현된 뒤 이동편의 데이터 수집·CSV 원천 적재로 범위가 넓어진 결과는 담고 있지 않다. 두 문서의 역할은 다음과 같이 나뉜다.

| 문서 | 성격 | 다루는 것 |
|---|---|---|
| `26-multi-source-architecture.md` | 이슈 #26 시점 설계 기록 | BaseCollector 추상화를 도입한 이유, 마이그레이션 계획, 당시 작업 분해 |
| 이 문서 | 현행 구조 정본 | 지금 동작하는 실행 경로, 어댑터 7종 규격, 적재 전략, 확장 절차 |

### 1.2 범위

- 포함: 수집 대상 원천, 실행 진입점과 라우팅, 수집기 계층 구조, 적재 대상 테이블과 충돌 전략, 설정 체계, 운영 절차, 확장 절차
- 제외: 적재 대상 테이블의 DDL(스키마 정본은 `01-IITP-DABT-Database` 레포), 수집한 데이터의 분석·활용, 배포 환경 구성

### 1.3 이 툴의 위치

이 툴은 공공데이터 원천과 서비스 DB 사이에 서는 **배치형 수집·전처리 계층**이다. 외부 원천을 호출해 원본을 파일로 보존하고, 서비스가 바로 조회할 수 있는 형태로 정규화해 DB에 적재한다. 서비스 레포는 이 툴이 적재한 테이블만 읽으며, 외부 API 를 직접 호출하지 않는다. 예외는 실시간성이 본질인 정보(버스 실시간 위치 등)로, 이런 항목은 수집·적재하지 않고 서비스가 직접 호출한다.

---

## 2. 시스템 개요 — 두 개의 수집 계열

현재 이 툴에는 성격이 다른 두 계열이 공존한다. 같은 진입점(`main.py`)을 쓰지만 그 뒤의 흐름이 갈라진다.

```
                        main.py  --mode {file|db}  --ext-sys <EXT_SYS>
                                        |
                     ext_sys 가 MOBILITY_EXT_SYS 에 속하는가?
                          |                             |
                         아니오                          예
                          |                             |
            [통계 계열: KOSIS]              [이동편의 계열: 6종]
                          |                             |
        stats_src 목록 조회 (DB)              collector.collect() -> list[dict]
        -> 어댑터 fetch_meta/latest/data       -> ext_data/<EXT_SYS>/<날짜>/rows.json
        -> kosis_data/<날짜>/{meta,latest,data} -> db_mobility.upsert_*()
        -> db_processing.process_db_insertion   -> sys_ext_api_info.latest_sync_time 갱신
        -> 원본 적재 -> 통합 테이블 이관
```

이와 별개로, OpenAPI 가 없거나 갱신 주기가 길어 배치에 올릴 실익이 없는 원천은 **CSV 적재 계열**(`scripts/`)로 처리한다. 이 계열은 `main.py` 를 거치지 않고 스크립트를 직접 실행한다(7절).

### 2.1 계열이 갈라진 이유

통계 계열은 "통계표 → 메타 + 최신 변경일 + 데이터" 라는 KOSIS 형 3단 구조를 전제로 만들어졌다. 관리 테이블(`sys_ext_api_info`, `stats_src_api_info`, `stats_src_data_info`)에 수집 대상이 행으로 등록되어 있고, 수집기는 그 행을 읽어 URL 템플릿을 채운다.

이동편의 계열의 원천은 통계표가 아니라 **행 단위 기준 데이터**다. 정류장 목록, 역사 편의시설 목록, 시설 접근성 목록에는 "메타"나 "최신 변경일" 개념이 없다. 이 차이를 통계 계열의 추상에 억지로 맞추면 어댑터마다 빈 구현이 쌓이므로, 공통 베이스(`MobilityCollector`)를 한 단계 더 두어 통계용 추상 메서드를 그 자리에서 흡수하고 어댑터에는 `collect()` 하나만 남겼다.

---

## 3. 실행 구조

### 3.1 진입점과 인자

```
python main.py --mode {file|db} [--ext-sys <EXT_SYS>]
```

| 인자 | 필수 | 의미 |
|---|:---:|---|
| `--mode file` | ✅ | 원본 파일 저장까지만 수행 |
| `--mode db` | ✅ | 파일 저장 후 DB 적재까지 수행 |
| `--ext-sys` | | 수집 대상 외부 시스템 식별자. 미지정 시 환경변수 `EXT_SYS`, 그래도 없으면 `KOSIS` |

`--mode` 는 필수이고 두 값 외에는 즉시 종료한다. `DB_URL` 미설정도 즉시 종료한다.

### 3.2 ext_sys 해석과 라우팅

`resolve_ext_sys()` 의 우선순위는 **CLI `--ext-sys` > 환경변수 `EXT_SYS` > 기본값 `KOSIS`** 이며, 값은 항상 대문자로 정규화해 `sys_ext_api_info.ext_sys` 와 일치시킨다.

`_COLLECTOR_REGISTRY` 는 ext_sys 식별자를 수집기 클래스에 매핑한다. 미등록 식별자는 `ValueError` 로 즉시 실패하며, 등록된 목록을 함께 알린다.

```python
_COLLECTOR_REGISTRY = {
    'KOSIS': KosisCollector,
    'GBIS': GbisCollector,
    'GBIS_LOWFLOOR': GbisLowFloorCollector,
    'GG_TOILET': GgToiletCollector,
    'KORAIL_CONV': KorailConvCollector,
    'KOWSI_FACL': KowsiFaclCollector,
    'TOUR_BF_API': TourBfCollector,
}
```

이 중 KOSIS 를 제외한 6종은 `mobility_pipeline.MOBILITY_EXT_SYS` 에도 속하므로, `main()` 이 `run_mobility()` 로 실행을 위임한다.

### 3.3 저장 경로

원본 보존 경로는 소스별로 갈린다. KOSIS 는 기존 운영 경로를 그대로 유지하고(후방호환), 그 외는 일반화 패턴을 쓴다.

| 계열 | 경로 |
|---|---|
| KOSIS | `kosis_data/<YYYYMMDD>/{meta,latest,data}/` |
| 그 외 통계 소스 | `ext_data/<EXT_SYS>/<YYYYMMDD>/{meta,latest,data}/` |
| 이동편의 | `ext_data/<EXT_SYS>/<YYYYMMDD>/rows.json` (+ 소스에 따라 `raw_details.json`) |

원본 보존은 `--mode db` 에서도 건너뛰지 않는다. 파싱 규칙이 바뀌었을 때 원천을 다시 호출하지 않고 재처리할 수 있어야 하고, 적재 결과가 의심스러울 때 대조할 기준이 있어야 하기 때문이다.

### 3.4 병렬 처리

통계 계열의 파일 저장은 `ThreadPoolExecutor` 로 통계표 단위 병렬 처리한다. 워커 수는 `PARALLEL_WORKERS_FILE`(기본 4, 상한 10), DB 적재는 `PARALLEL_WORKERS_DB`(기본 2, 상한 5)로 제한한다. 상한을 코드에 둔 이유는 설정 실수로 원천에 과도한 동시 호출을 보내는 것을 막기 위해서다.

이동편의 계열은 소스 단위로 순차 실행한다. 대부분의 원천이 페이지 단위 순회를 요구하고 호출 간 간격(기본 0.15초)을 두기 때문에 병렬화 이득이 없다.

---

## 4. 수집기 계층

```
BaseCollector (ABC)                     collectors/base.py
├─ KosisCollector                       collectors/kosis.py
└─ MobilityCollector                    collectors/mobility_base.py
   ├─ GbisCollector                     collectors/gbis.py
   ├─ GbisLowFloorCollector             collectors/gbis_lowfloor.py
   ├─ GgToiletCollector                 collectors/gg_toilet.py
   ├─ KorailConvCollector               collectors/korail_conv.py
   ├─ KowsiFaclCollector                collectors/kowsi_facl.py
   └─ TourBfCollector                   collectors/tour_bf.py
```

### 4.1 BaseCollector

모든 수집기의 공통 인터페이스다. 서브클래스는 클래스 속성 `EXT_SYS` 에 소스 식별자를 선언한다.

**추상 메서드 4종**

| 메서드 | 책임 |
|---|---|
| `fetch_meta(data_info)` | 통계표 메타 조회 |
| `fetch_latest(data_info)` | 최신 변경 시점 조회 |
| `fetch_data(data_info)` | 실데이터 조회. 소스별 재시도·분할 규칙 적용 |
| `is_retryable_error(response)` | 재시도 대상 오류인지 판정 |

**공통 구현 2종**

- `http_get(url, timeout, retries, backoff_sec)` — 재시도·타임아웃을 갖춘 GET. 기본값은 타임아웃 30초, **재시도 0회**, 백오프 1초다. 재시도 기본값이 0인 것은 의도적이다. KOSIS 경로가 이 베이스로 옮겨 오기 전 단일 호출이었으므로, 기본값을 0으로 두어야 어댑터가 명시적으로 재시도를 켜기 전까지 기존 동작이 유지된다. 재시도 소진 시 `RuntimeError` 를 던지고, 성공 시 `Response` 를 가공 없이 반환해 어댑터가 `status_code`·`json()`·`text` 를 직접 다루게 한다.
- `save_response(response, save_dir, filename)` — 응답을 파일로 보존. `dict`/`list` 는 JSON(들여쓰기 2, `ensure_ascii=False`), 그 외는 문자열로 기록한다. 소스별로 달라질 이유가 없으므로 어댑터가 재정의하지 않는다.

**로그 마스킹** — `_mask_url()` 이 URL 의 `apiKey=` 값을 `***` 로 치환한다. 실패 로그에 요청 URL 이 그대로 남으면 인증키가 로그 파일에 평문으로 쌓이기 때문이다.

### 4.2 MobilityCollector

이동편의 소스의 공통 베이스다. 통계용 추상 메서드를 여기서 흡수한다 — `fetch_meta`/`fetch_latest` 는 빈 문자열을 반환하고, `fetch_data` 는 `collect()` 로 위임한다. 따라서 어댑터가 구현할 것은 `collect() -> list[dict]` 하나다.

**설정 해석** — 두 속성이 해석 순서를 고정한다.

| 속성 | 우선순위 |
|---|---|
| `base_url` | `sys_ext_api_info.ext_url` > 환경변수 `<EXT_SYS>_BASE_URL` > 클래스 기본값 `DEFAULT_BASE_URL` |
| `api_key` | `sys_ext_api_info.auth` > 환경변수 `<EXT_SYS>_API_KEY` > 환경변수 `DATA_GO_KR_API_KEY` |

DB 등록 값을 최우선에 두면 운영 중 원천 URL 이나 키가 바뀌어도 배포 없이 바꿀 수 있다. 키가 어디에도 없으면 빈 호출을 보내지 않고 `RuntimeError` 로 즉시 실패한다.

**HTTP 헬퍼** — `get_json()`/`get_xml()` 은 타임아웃 30초·재시도 2회·백오프 1초로 고정한다. 이동편의 원천은 공공데이터포털 계열이라 일시적 5xx 가 드물지 않고, 배치 1회 실패가 그날 데이터 전체 결손으로 이어지기 때문에 통계 계열보다 공격적으로 재시도한다.

**값 정규화 3종** — `to_int`/`to_float`/`to_yn`. 외부 API 가 빈 문자열·`-`·`null` 을 섞어 보내는 경우가 많아, 변환 실패를 예외로 올리지 않고 `None` 으로 떨어뜨린다. `to_yn` 은 `Y/YES/TRUE/1` 과 `N/NO/FALSE/0` 을 한 글자로 정규화하고, 판별 불가는 `None` 으로 남긴다 — **판별 불가를 `N` 으로 적지 않는 것이 원칙이다.** 접근성 정보에서 "없음"과 "모름"은 이용자에게 전혀 다른 의미이기 때문이다.

### 4.3 이동편의 오케스트레이션

`mobility_pipeline.run_mobility(ext_sys, mode)` 의 흐름은 다음과 같다.

1. `MOBILITY_COLLECTORS[ext_sys]` 로 수집기 생성. `sys_ext_api_info` 행이 없으면 경고를 남기고 환경변수 설정만으로 진행한다
2. `collector.collect()` 실행
3. `ext_data/<EXT_SYS>/<YYYYMMDD>/rows.json` 로 원본 보존. 어댑터가 `_raw_details` 를 들고 있으면 `raw_details.json` 도 함께 보존
4. `--mode db` 이면 `_UPSERT_DISPATCH[ext_sys]` 가 가리키는 `db_mobility` 함수로 적재
5. GBIS 는 같은 실행에서 경유정류소(정류장 마스터 + 노선-정류장 관계)까지 이어서 수집
6. `--mode db` 이면 `db_mobility.touch_latest_sync(ext_sys)` 로 동기화 시각 기록

수집기 선택과 적재 함수 선택을 **두 개의 매핑 테이블**(`MOBILITY_COLLECTORS`, `_UPSERT_DISPATCH`)로 분리한 것은, 한 소스가 여러 테이블에 적재되거나(GBIS) 기존 테이블의 특정 컬럼만 갱신하는 경우(GBIS_LOWFLOOR)를 같은 흐름에 담기 위해서다.

GBIS 경유정류소 수집은 노선 메타 수집 결과의 `route_id` 를 재사용하므로 노선 열거를 다시 하지 않는다. 정류소 수집이 실패해도 이미 끝난 노선 메타 적재 결과는 유지한다. 노선 메타만 갱신할 때는 `GBIS_COLLECT_STATIONS=false` 로 끌 수 있다.

---

## 5. 어댑터별 규격

### 5.1 KOSIS — 국가통계포털

| 항목 | 내용 |
|---|---|
| 식별자 | `KOSIS` (기본값) |
| 계열 | 통계 |
| 대상 | `stats_src_api_info` 에 등록된 통계표 |
| 적재 | 원본 테이블 → 통합 테이블 (`db_processing`) |

URL 은 DB 에 저장된 템플릿을 어댑터가 채운다 — `{API_AUTH_KEY}`, `{from}`, `{to}` 치환. 응답 형식은 `url_info['format']` 에 따라 JSON 과 텍스트를 구분해 처리한다.

**오류 31(조회 기간 초과) 처리** — KOSIS 는 요청 기간이 넓으면 데이터 대신 오류 31 을 돌려준다. 어댑터는 이 응답을 감지해 기간을 연도 단위로 쪼개 재귀 재시도하고 결과를 합친다. 이 규칙은 KOSIS 에만 있는 제약이므로 베이스가 아니라 어댑터 안에 격리되어 있다.

### 5.2 TOUR_BF_API — 한국관광공사 무장애여행

| 항목 | 내용 |
|---|---|
| 엔드포인트 | `KorWithService2` — `areaBasedList2`(지역 목록) + `detailWithTour2`(무장애 편의정보) |
| 대상 | 지역코드 기반 (기본 경기 31 / 안양 17) |
| 적재 | `poi_tour_bf_facility` |

지역 목록으로 대상을 얻고 각 건의 무장애 편의정보를 결합해, 스키마의 `_yn` 플래그로 파생 매핑한다.

**서술 원문 보존** — 편의정보는 자유 서술 문장이다. 판정 규칙이 바뀔 때마다 원천을 다시 호출하지 않도록 원문을 `detail_raw` 에 함께 적재하고, 원본 JSON 도 파일로 남긴다. "본관 옆 부스" 같은 위치 설명은 플래그로 환원되지 않으므로 원문 자체가 안내 자료가 된다.

**긍정·부정 판정** — 텍스트에서 플래그를 유도할 때 부정어 한 글자만 보고 판정하면 "단차가 없어 휠체어 접근 가능함" 같은 긍정 서술이 부정으로 뒤집힌다. 판정은 문장 단위로 한다. 접근로 정본인 `route` 필드와 장애인 관람석(`auditorium`)도 매핑 대상에 포함한다.

### 5.3 GBIS — 경기버스정보(노선·정류장)

| 항목 | 내용 |
|---|---|
| 엔드포인트 | 공공데이터포털 경기버스정보 서비스 |
| 적재 | `tran_bus_route_info`, `tran_bus_station_info`, `tran_bus_route_station` |

노선 메타(노선번호·유형·기점종점·운수사·배차간격·첫차막차)를 수집하고, 같은 노선 목록으로 경유정류소를 이어서 수집한다.

**노선 열거 방식** — 노선 목록 API 가 키워드 검색만 지원해 "이 지역의 전체 노선"을 직접 얻을 수 없다. 숫자 0~9 를 키워드로 스캔해 합집합을 만든 뒤 `regionName` 부분일치(기본값 `안양`)로 거른다.

**수집하지 않는 것** — 실시간 위치는 수집·저장하지 않는다. 배치 주기와 실시간성이 맞지 않아 저장하는 순간 낡은 값이 되기 때문이며, 서비스가 필요 시 직접 호출한다.

### 5.4 GBIS_LOWFLOOR — 저상버스 노선현황

| 항목 | 내용 |
|---|---|
| 원천 | 경기버스정보 저상버스 노선현황(전일 기준) 페이지 |
| 적재 | `tran_bus_route_info.low_bus_yn` 갱신 |

정적 노선 API 에 없는 저상버스 운행 여부를 채우는 전용 수집기다. 경기도 전 시군 노선이 한 페이지에 들어 있고, 노선번호 셀의 링크에 `routeId` 가 함께 있다.

**매칭 키는 `routeId` 다.** 같은 노선번호가 여러 운수사에 존재하므로(예: 동일 번호가 서로 다른 운수사에 중복) 번호 매칭은 구조적으로 틀린다. 노선번호·운수사는 원본 보존과 검증용으로만 쓴다.

적재 규칙은 두 방향이다 — 표에 있는 `routeId` 는 `Y`, 경기도 관할 노선 중 표에 없는 것은 `N`. 한쪽만 갱신하면 폐지된 저상 노선이 `Y` 로 남는다. 전일 기준 표이므로 일 1회 실행을 전제로 한다.

### 5.5 GG_TOILET — 경기데이터드림 공중화장실 현황

| 항목 | 내용 |
|---|---|
| 원천 | 경기데이터드림 공중화장실 현황(제공표준) |
| 적재 | `poi_public_toilet_info` |

**이 소스를 쓰는 이유** — 전국 표준데이터가 좌표 제공을 중단해 기존 적재분의 좌표 보유율이 17%까지 떨어졌다. 경기도 제공 API 는 좌표를 그대로 주어 98%로 회복된다.

- 페이징: `pIndex`/`pSize`(상한 1000)
- 종료 판정: 데이터가 없으면 `RESULT.CODE=INFO-200` 이 오며 이를 정상 종료로 처리한다. 그 외 `ERROR-*` 는 `RuntimeError` — 조용한 실패를 만들지 않는다
- 필터: 요청 인자의 주소 필터가 부분일치이므로, 응답을 받은 뒤 주소 접두어로 다시 거른다. 그렇지 않으면 인접 지역이나 유사 도로명이 섞인다

### 5.6 KORAIL_CONV — 철도 역사 편의시설

| 항목 | 내용 |
|---|---|
| 엔드포인트 | 편의시설정보 서비스 — `stationFacilities`(역사 내) + `weekPersonFacilities`(교통약자) |
| 적재 | `poi_station_access_status` |

두 엔드포인트를 전 페이지 수집해 역 코드(`stn_cd`) 기준으로 병합한다. 역명 필터 파라미터를 API 가 지원하지 않아 전 역을 적재하고, 실증 대상 역은 `anyang_yn='Y'` 로 표시한다.

**좌표 보강** — 이 API 는 좌표를 제공하지 않는다. `KORAIL_LOC_CSV`(콤마 구분 다중 경로)로 지정한 노선별 역위치 CSV 를 병합 시점에 결합한다. 별도 재수집 없이 좌표만 갱신해야 할 때는 `scripts/load_station_coords.py` 를 쓴다.

### 5.7 KOWSI_FACL — 장애인편의시설

| 항목 | 내용 |
|---|---|
| 엔드포인트 | 장애인편의시설 서비스 (XML 전용) |
| 적재 | `poi_facility_accessibility` |

목록 API 를 페이징하며 주소 필터(기본 `안양시`)로 대상을 좁힌다. 응답 `resultCode` 가 0 이 아니면 `RuntimeError` 로 즉시 실패한다.

**분할 수집** — 페이지 상한(1000)과 일 트래픽 한도 때문에 단일 실행으로 전 페이지를 완주할 수 없다. 상태 파일(`ext_data/KOWSI_FACL/state.json`)에 다음 페이지를 기록하고, 실행당 `KOWSI_MAX_PAGES` 만큼만 스캔한 뒤 다음 실행이 이어받는다. 전 페이지를 완주하면 완료 시각을 기록하고 `KOWSI_RESCAN_DAYS`(기본 28일) 이내 재실행은 건너뛴다. 일 단위 스케줄로 돌리면 결과적으로 월 1회 전수 갱신이 된다.

**기구표 상세** — 시설 1건당 1회 추가 호출이 필요해 트래픽을 크게 쓴다. 코드 기본값은 `KOWSI_FETCH_EVAL=OFF` 이며, OFF 로 두면 접근성 플래그가 NULL 로 남는다. 트래픽 한도가 충분한 환경에서만 ON 으로 둔다.

---

## 6. 적재 계층

### 6.1 통계 계열 — `db_processing.py`

`process_db_insertion()` 이 파일 저장 결과를 받아 통계표 단위로 처리한다.

1. `_insert_origin_data()` — 원본 데이터 적재
2. `_transfer_to_integration_table()` — 통합 테이블로 이관
3. `_insert_metadata()` — 메타 적재
4. `_update_stats_src_data_info()` / `_update_sys_data_summary_info()` — 수집 이력·요약 갱신
5. `_update_sys_ext_api_info()` — 외부 시스템 동기화 시각 갱신
6. `cleanup_old_data()` — 보존 기간이 지난 과거 데이터 정리

통계표 일부가 실패해도 나머지는 진행하며, 실패 목록을 모아 종료 코드 2(부분 완료)로 알린다.

### 6.2 이동편의 계열 — `db_mobility.py`

적재 대상과 충돌 전략은 원천이 **자연키를 주는지**에 따라 갈린다.

| 테이블 | 소스 | 충돌 전략 |
|---|---|---|
| `tran_bus_route_info` | GBIS | `ON CONFLICT (route_id)` |
| `tran_bus_route_info.low_bus_yn` | GBIS_LOWFLOOR | `routeId` 매칭 UPDATE (양방향 Y/N) |
| `tran_bus_station_info` | GBIS | `ON CONFLICT (station_id)` |
| `tran_bus_route_station` | GBIS | `ON CONFLICT (route_id, station_id, station_seq)` |
| `poi_station_access_status` | KORAIL_CONV | `ON CONFLICT (stn_cd)` |
| `poi_station_wheelchair_lift` | CSV | `ON CONFLICT (line_name, stn_name, mng_no)` |
| `poi_station_elevator_unit` | CSV | 선명 단위 전체 교체 |
| `poi_station_toilet_unit` | CSV | 선명·구분 단위 전체 교체 |
| `poi_station_platform` | CSV | `ON CONFLICT (line_name, stn_name, platform_no)` + 요약 UPDATE |
| `poi_facility_accessibility` | KOWSI_FACL | `ON CONFLICT (facl_inf_id)` |
| `poi_tour_bf_facility` | TOUR_BF_API | 자연키 없음 → (시설명, 시도코드) 조회 후 UPDATE/INSERT |
| `poi_public_toilet_info` | GG_TOILET | 자연키 없음 → (이름+주소) 매칭 UPDATE / 미매칭 INSERT / 사라진 행 논리삭제 |
| `poi_emergency_support` | CSV | `ON CONFLICT (support_type, name, coalesce(addr_road,''))` |

**자연키가 없는 원천의 처리 원칙** — 전량 삭제 후 재적재를 하지 않는다. 그렇게 하면 기존 행의 id 가 매번 바뀌어, 그 id 를 참조하는 파생 데이터(사용자 제보, 보정값)가 끊어진다. 대신 (이름+주소) 정규화 매칭으로 제자리 갱신하고, 매칭되지 않은 새 행만 INSERT 하며, 이번 수집분에서 사라진 행은 물리 삭제가 아니라 논리 삭제(`del_yn`)로 남긴다. 세 동작을 한 트랜잭션에서 처리한다.

**매칭 안전장치** — 매칭 성공 비율이 임계치(0.7) 아래로 떨어지면 원천 스키마나 주소 표기가 바뀐 것으로 보고 적재를 중단한다. 그대로 진행하면 대량 중복 INSERT 와 대량 논리삭제가 동시에 일어난다.

**주소 컬럼 취급** — 지오코딩 결과로 원천 주소 컬럼을 덮어쓰지 않는다. 주소가 자연키의 일부이므로, 덮어쓰면 다음 실행에서 같은 행을 다시 찾지 못해 전량 중복 INSERT 가 된다. 보강 좌표는 좌표 컬럼에만 기록한다.

**작성자 표기** — 적재 행의 `created_by` 는 DB 공통코드 시드 정본인 `SYS-BACH` 로 통일한다.

### 6.3 동기화 시각

적재가 끝나면 `touch_latest_sync(ext_sys)` 가 `sys_ext_api_info.latest_sync_time` 을 갱신한다. 이 값이 갱신되지 않았다는 것은 그 소스의 배치가 돌지 않았거나 실패했다는 뜻이므로, 운영 점검의 1차 지표로 쓴다.

---

## 7. CSV 원천 적재 계열

### 7.1 이 계열이 존재하는 이유

공공데이터 원천이 전부 OpenAPI 로 제공되지는 않는다. 다음 세 경우는 배치 수집기로 만들 실익이 없다.

- OpenAPI 가 아예 없고 조회 화면만 있는 원천
- 갱신 주기가 연 1회·반기 1회여서 매일 호출할 이유가 없는 원천
- 파일(CSV) 배포가 정본인 원천

이런 원천은 **수집(도구) 과 적재(스크립트) 를 분리**한다. 수집은 `tools/` 의 스크립트가 원천 화면·다운로드 채널을 그대로 재현해 CSV 를 만들고, 적재는 `scripts/` 의 Python 스크립트가 그 CSV 를 읽어 DB 에 넣는다. 분리해 두면 원천 화면이 바뀌어도 적재 로직은 그대로 쓸 수 있고, 적재 규칙이 바뀌어도 원천을 다시 긁지 않는다.

### 7.2 적재 스크립트

| 스크립트 | 원천 | 적재 대상 | 주기 |
|---|---|---|---|
| `load_national_charger_csv.py` | 전국 전동휠체어 급속충전기 표준데이터 | `poi_emergency_support` (`charge`) | 반기 |
| `load_gg_assist_repair_csv.py` | 경기도 시군별 보조기기 수리 서비스센터 | `poi_emergency_support` (`repair`) | 연 1회 |
| `load_knat_center_csv.py` | 전국 보조기기센터 명부 | `poi_emergency_support` (`repair`) | 부정기 |
| `load_knat_repair_csv.py` | 전국 보조기기 수리센터 명부 | `poi_emergency_support` (`repair`) | 부정기 |
| `load_nhis_assist_store_csv.py` | 보조기기 급여 등록업소 | `poi_emergency_support` (`repair`) | 부정기 |
| `load_krna_station_csv.py` | 철도 역사 설비(승강기·화장실·승강장) | 역사 설비 테이블 3종 | 연 1회 |
| `load_lift_csv.py` | 철도 휠체어리프트 | `poi_station_wheelchair_lift` | 분기 |

보정용 스크립트 2종이 더 있다.

| 스크립트 | 용도 |
|---|---|
| `load_station_coords.py` | 역위치 CSV 로 `poi_station_access_status` 좌표만 갱신 (전체 재수집 없이) |
| `backfill_facility_eval_flags.py` | 저장된 기구표 원문을 다시 파싱해 신규 플래그 컬럼만 채움 (원천 재호출 없이) |

**백필 스크립트의 원칙** — 저장된 원문을 수집기와 **같은 파서**로 다시 읽는다. SQL 문자열 검색으로 채우지 않는다. 더미 응답 판별·정규화 규칙이 수집기 안에 있으므로, 같은 규칙을 쓰지 않으면 수집 경로로 들어온 행과 백필로 채운 행이 서로 다른 기준을 갖게 된다.

**공통 인자 규약** — 적재 스크립트는 `--csv <파일>` 과 `--dry-run` 을 공통으로 받고, 지오코딩 결과를 별도 CSV 로 받는 스크립트는 `--geocoded` 를 추가로 받는다. `--dry-run` 은 파싱·정규화 결과만 출력하고 DB 를 건드리지 않는다. 재실행해도 같은 결과가 되도록(멱등) 설계한다.

**신뢰도 등급** — `poi_emergency_support` 는 여러 원천에서 같은 성격의 행이 들어온다. 원천이 주소·연락처를 직접 주는 명부와, 상호명만 주어 외부 지오코딩으로 위치를 붙인 명부는 정확도가 다르므로 `confidence` 로 구분해 적재한다. 서비스는 이 값을 안내 문구 수위 조절에 쓴다.

### 7.3 수집 도구

| 도구 | 채널 |
|---|---|
| `fetch_std_dataset.ps1` | 표준데이터셋 통합본 — 상세 페이지의 그리드 다운로드 엔드포인트를 그대로 사용(인증키 불필요) |
| `fetch_nhis_assist_store.ps1` | 등록업소 조회 화면(POST 폼) 재현 |
| `fetch_knat_repair.ps1` | 수리센터 명부 — 시도별 POST 순회, EUC-KR 직접 디코딩 |
| `fetch_knat_center.ps1` | 보조기기센터 명부 — 표 단위 파싱, EUC-KR 직접 디코딩 |
| `fetch_gg_theme_sheet.ps1` | 테마 데이터 시트 — 상세 페이지에서 쿠키·토큰을 얻은 뒤 시트 JSON 엔드포인트 호출 |
| `enrich_address_kakao.ps1` | 도로명/지번 주소 → 좌표 보강(주소 검색). 실패 시 뒤쪽 상세를 잘라 가며 재시도 |
| `enrich_repair_kakao_v2.ps1` | 상호명 기반 주소·좌표 보강 + 검증 게이트 |

**인증키 취급** — 좌표 보강 도구는 키를 스크립트에 담지 않는다. 키 파일 경로를 필수 인자로 받아 실행 시점에 읽는다.

**상호명 검색의 검증 게이트** — 첫 검색 결과를 그대로 채택하면 안 된다. 명부에는 같은 체인 상호가 지역마다 반복 등장하므로 첫 결과 채택은 구조적으로 틀린 값을 만든다(다른 자치구 매장이 붙는 식). 후보를 받아 행정구역·상호 일치도로 걸러 통과한 것만 채택하고, 통과하지 못하면 좌표를 비운 채 남긴다.

**행정구역 중심점 배제** — 주소 검색 결과가 지번 단위로 해석되지 않으면 지오코딩 서비스가 행정구역 중심점을 돌려준다. 이 값을 좌표로 채택하면 실제 위치에서 수 km 떨어진 지점이 저장되므로, 결과 타입을 확인해 도로명·지번 주소로 해석된 것만 채택한다.

---

## 8. 설정 체계

### 8.1 원칙

- 설정은 `.env` 에 두고 `.env.example` 이 그 목록의 정본이다
- 원천 URL·인증키는 **DB 등록 값이 최우선**이다. 배포 없이 교체할 수 있어야 하기 때문이다
- 코드 기본값은 "설정이 하나도 없어도 안전한 쪽"으로 잡는다. 트래픽을 많이 쓰는 옵션은 기본 OFF 다

### 8.2 주요 설정

| 구분 | 키 | 기본값 | 비고 |
|---|---|---|---|
| DB | `DB_URL` | — | 필수. 미설정 시 즉시 종료 |
| DB | `DB_BATCH_SIZE` | 100 | 배치 삽입 크기 |
| 로깅 | `LOG_LEVEL` | INFO | |
| 라우팅 | `EXT_SYS` | KOSIS | CLI `--ext-sys` 가 우선 |
| 수집 범위 | `DATA_COLLECTION_SCOPE` | ALL | `ALL` / `PARTIAL` 만 허용 |
| 병렬 | `PARALLEL_WORKERS_FILE` | 4 | 코드 상한 10 |
| 병렬 | `PARALLEL_WORKERS_DB` | 2 | 코드 상한 5 |
| 인증 | `DATA_GO_KR_API_KEY` | — | 소스별 키가 없을 때의 공통 폴백 |
| 소스별 | `<EXT_SYS>_BASE_URL` / `<EXT_SYS>_API_KEY` | — | DB 등록 값 다음 순위 |

소스별 동작 조절 키는 각 어댑터 절(5절)에 정리되어 있다. 대표적으로 `GBIS_REGION_FILTER`(노선 필터), `GBIS_COLLECT_STATIONS`(경유정류소 동시 수집), `KOWSI_MAX_PAGES`·`KOWSI_RESCAN_DAYS`·`KOWSI_FETCH_EVAL`(분할 수집·상세 호출), `GG_TOILET_ADDR_FILTER`(주소 접두어), `KORAIL_LOC_CSV`(좌표 보강 CSV)가 있다.

### 8.3 부분 수집 대상 지정

`DATA_COLLECTION_SCOPE=PARTIAL` 이면 `.env` 의 `[TARGET_SRC_TBL_ID_LIST]` 섹션에 적힌 통계표만 수집한다. 목록에 있는 통계표가 DB 에 없으면 **경고 후 진행이 아니라 즉시 종료**한다. 오타 하나로 수집 대상이 조용히 비는 것을 막기 위해서다. 허용값(`ALL`/`PARTIAL`) 외의 값도 같은 이유로 즉시 종료한다 — 잘못된 값을 `ALL` 로 폴백시키면 의도하지 않은 전량 수집이 일어난다.

---

## 9. 실행·운영

### 9.1 실행 래퍼

`scripts/run_collect.sh` 가 스케줄 실행용 래퍼다.

- **단일 인스턴스 보장** — `flock` 으로 잠금. 이미 실행 중이면 이번 트리거를 건너뛴다. 수집이 예상보다 길어졌을 때 다음 트리거가 겹쳐 같은 원천을 동시에 호출하는 것을 막는다
- 프로젝트 루트 자체 탐지, 가상환경이 있으면 자동 활성화
- 시작·종료 시각과 종료 코드를 표준 출력에 남겨 스케줄러 로그에 그대로 쌓이게 한다

### 9.2 종료 코드

| 코드 | 의미 |
|---|---|
| 0 | 정상 완료 |
| 1 | 실행 실패 (설정 누락, 예외) |
| 2 | 부분 완료 — 일부 통계표 적재 실패 |

부분 실패를 0 으로 처리하지 않는 이유는, 스케줄러가 성공으로 기록해 버리면 결손을 아무도 모르기 때문이다. 반대로 1 로 뭉뚱그리지 않는 이유는 전면 실패와 부분 실패의 대응이 다르기 때문이다.

### 9.3 실행 기록

- `logs/<YYYYMMDD>.log` — 전체 로그
- `logs/db_<YYYYMMDD>.log` — DB 전용 로그
- `logs/run_summary.log` — **실행 1회를 한 줄로 누적**

`run_summary.log` 한 줄에는 시작·종료 시각, `ext_sys`, 모드, 대상 수, 파일 저장 성공 수, DB 성공·실패 수, 소요 시간, 상태가 들어간다. 배치가 며칠에 걸쳐 어떻게 돌았는지를 이 파일 하나로 훑을 수 있게 하려는 것이며, 기존 로그는 그대로 유지한다.

### 9.4 소스별 실행 주기 설계

| 계열 | 주기 근거 |
|---|---|
| 통계(KOSIS) | 원천 갱신이 월 단위. 월 2회 실행 |
| GBIS 노선·정류장 | 노선 개편 반영. 주 단위면 충분 |
| GBIS_LOWFLOOR | 원천이 전일 기준 표. **일 1회** |
| KOWSI_FACL | 분할 수집. 일 1회 실행이 월 1회 전수 갱신이 됨 |
| GG_TOILET / KORAIL_CONV / TOUR_BF_API | 기준 정보. 월 단위 |
| CSV 계열 | 원천 갱신 시 수동 실행 |

---

## 10. 오류 처리와 재시도 원칙

### 10.1 조용한 실패를 만들지 않는다

수집기가 지켜야 할 첫 번째 규칙이다. 원천이 오류를 **정상 응답 형태로** 돌려주는 경우가 흔하다 — 결과 코드 필드에만 오류가 담기고 HTTP 는 200 인 식이다. 이를 그대로 파싱하면 0건 수집이 정상 완료로 기록된다. 모든 어댑터는 응답의 결과 코드를 먼저 확인하고, 오류면 `RuntimeError` 로 즉시 실패한다.

"데이터 없음"과 "오류"는 구분한다. 전자는 정상 종료, 후자는 실패다. 두 가지를 같은 코드로 돌려주는 원천에서는 코드 값으로 명시 분기한다.

### 10.2 재시도는 소스 성격에 맞춘다

| 계열 | 타임아웃 | 재시도 | 근거 |
|---|---|---|---|
| 통계(기본) | 30초 | 0회 | 기존 동작 보존. 어댑터가 필요 시 명시적으로 켠다 |
| 이동편의 | 30초 | 2회 (백오프 1초) | 일시적 5xx 가 드물지 않고, 1회 실패가 당일 결손으로 직결 |

재시도 대상은 `is_retryable_error()` 가 판정한다. 이동편의 계열은 429·5xx 계열만 재시도하고, 4xx 는 재시도하지 않는다 — 잘못된 요청을 반복해 보내면 트래픽 한도만 소모한다.

### 10.3 부분 실패의 경계

- **통계 계열** — 통계표 단위로 독립적이므로 일부 실패를 허용하고 종료 코드 2 로 알린다
- **이동편의 계열** — 한 소스의 수집은 전부 성공하거나 전부 실패한다. 페이지 순회 중 끊긴 결과를 적재하면 "사라진 것으로 보이는 행"이 대량 논리삭제되기 때문이다
- **GBIS 경유정류소** — 예외적으로 부분 성공을 허용한다. 노선 메타 적재가 이미 끝난 뒤의 추가 단계이므로, 정류소 수집 실패가 노선 적재를 되돌릴 이유가 없다

### 10.4 로그에 남기지 않는 것

인증키는 어떤 경로로도 로그에 남기지 않는다. 요청 URL 을 로그에 남길 때는 인증키 파라미터를 마스킹한다. 실패 로그일수록 URL 전체를 남기고 싶어지지만, 실패 로그야말로 오래 보관되고 공유되는 대상이다.

---

## 11. 신규 소스 확장 절차

### 11.1 통계형 소스

1. `sys_ext_api_info` 에 소스 행 등록 (`ext_sys`, `ext_url`, `auth`)
2. `stats_src_api_info` 에 대상 통계표와 URL 템플릿 등록
3. `collectors/<source>.py` 에 `BaseCollector` 서브클래스 작성 — `EXT_SYS` 선언, 추상 메서드 4종 구현
4. `main.py` 의 `_COLLECTOR_REGISTRY` 에 한 줄 추가
5. `.env.example` 에 소스별 설정 키 추가

`collectors/base.py` 는 수정하지 않는다. 베이스를 고쳐야 한다면 그 요구는 소스별 특수 규칙일 가능성이 높고, 특수 규칙은 어댑터 안에 있어야 한다.

### 11.2 이동편의형 소스

1. `sys_ext_api_info` 에 소스 행 등록
2. `collectors/<source>.py` 에 `MobilityCollector` 서브클래스 작성 — `EXT_SYS`·`DEFAULT_BASE_URL` 선언, `collect() -> list[dict]` 구현. 반환 dict 의 키는 **적재 대상 DB 컬럼명** 에 맞춘다
3. `db_mobility.py` 에 `upsert_*()` 함수 추가 — 자연키 유무에 따라 충돌 전략 선택(6.2 참조)
4. `mobility_pipeline.py` 의 `MOBILITY_COLLECTORS` 와 `_UPSERT_DISPATCH` 에 각각 한 줄 추가
5. `main.py` 의 `_COLLECTOR_REGISTRY` 에 한 줄 추가
6. `.env.example` 에 소스별 설정 키 추가
7. 테스트 추가 — 최소한 응답 파싱과 오류 응답 판정

### 11.3 CSV 원천

1. `tools/` 에 수집 도구 추가 (원천 채널 재현)
2. `scripts/load_*.py` 에 적재 스크립트 추가 — `--csv`/`--dry-run` 공통 인자, 멱등 적재
3. 적재 완료 후 `sys_ext_api_info.latest_sync_time` 갱신

### 11.4 새 소스를 붙이기 전에 확인할 것

- 원천이 **자연키를 주는가** — 없으면 6.2 의 매칭 전략과 안전장치가 필요하다
- 원천이 오류를 **어떤 형태로** 돌려주는가 — HTTP 상태와 결과 코드가 어긋나는지 실제 호출로 확인한다
- 페이지 상한과 **일 트래픽 한도** — 단일 실행 완주가 불가능하면 분할 수집 설계가 필요하다
- 좌표를 주는가 — 주지 않으면 보강 경로와 검증 게이트가 필요하다
- 자유 서술 필드가 있는가 — 있으면 **원문을 함께 보존**한다. 플래그로 환원하면 규칙 변경 시 되돌릴 수 없다

---

## 12. 테스트 구성

| 파일 | 대상 |
|---|---|
| `test_collectors.py` | BaseCollector 추상 동작, KOSIS 어댑터 동등성 |
| `test_db.py` | DB 조회 계층 |
| `test_main_integration.py` | 진입점 라우팅, KOSIS 경로 동등성 |
| `test_mobility.py` | 이동편의 어댑터·적재 전반 |
| `test_gbis_lowfloor.py` | 저상버스 표 파싱·Y/N 양방향 갱신 |
| `test_gg_toilet.py` | 공중화장실 응답 파싱·종료 판정·주소 재필터 |
| `test_gg_toilet_db.py` | 매칭 갱신·논리삭제·안전장치 |
| `test_krna_station_csv.py` | 헤더 기반 파일 종류 자동 판별, 단위 교체 적재 |

테스트가 고정하는 것은 **되돌아가기 쉬운 결정**이다. 후방호환(KOSIS 경로 동등성), 오류 응답을 실패로 판정하는 규칙, 자연키 없는 테이블의 매칭 전략, 판별 불가를 `N` 으로 적지 않는 규칙이 여기에 해당한다.

---

## 13. 선행 설계(#26) 대비 변경 요약

| 항목 | #26 설계 시점 (v1.2.0) | 현재 (v1.16.0) |
|---|---|---|
| 수집 계열 | 통계 1종 | 통계 1종 + 이동편의 1종 + CSV 적재 1종 |
| 수집 어댑터 | KOSIS 1종 | 7종 |
| 공통 베이스 | `BaseCollector` | `BaseCollector` + `MobilityCollector` |
| 오케스트레이션 | `main.py` 단일 흐름 | `main.py` + `mobility_pipeline.py` 위임 |
| 적재 모듈 | `db_processing.py` | `db_processing.py` + `db_mobility.py` |
| 적재 대상 | 통계 원본·통합 테이블 | 좌측 + 이동편의 계열 테이블 13종 |
| 원본 보존 | `kosis_data/<날짜>/` | 좌측 + `ext_data/<EXT_SYS>/<날짜>/` |
| 부가 스크립트 | 없음 | 적재 7종 + 보정 2종, 수집 도구 7종 |

**#26 설계에서 그대로 유지된 것** — ext_sys 기반 라우팅, 어댑터 등록 한 줄로 소스를 추가하는 확장 모델, KOSIS 후방호환 원칙, 원본 파일 보존.

**#26 설계가 예상하지 못한 것** — 통계형이 아닌 행 단위 원천의 비중이 커지면서 공통 베이스가 한 단계 더 필요해졌고, OpenAPI 가 없는 원천을 위한 CSV 적재 계열이 별도로 생겼다. 자연키를 주지 않는 원천의 갱신 전략은 #26 설계에 없던 문제였고, 6.2 의 매칭·논리삭제·안전장치가 그 답으로 자리 잡았다.
