# KOSIS 단일 소스 의존성 현황 정리

> 이슈 #25 분석 문서 | 버전: v1.1.3

## 목차

1. [개요](#1-개요)
2. [파일별 KOSIS 의존부 인벤토리](#2-파일별-kosis-의존부-인벤토리)
   - 2.1 [db.py](#21-dbpy)
   - 2.2 [kosis_api.py](#22-kosis_apipy)
   - 2.3 [main.py](#23-mainpy)
3. [데이터 흐름과 의존 핫스팟](#3-데이터-흐름과-의존-핫스팟)
4. [결론](#4-결론)
5. [권장 방향 (#26 설계 입력)](#5-권장-방향-26-설계-입력)

---

## 1. 개요

본 문서는 `08-IITP-DABT-PreProcessing` 레포의 데이터 수집 구조가
KOSIS(국가통계포털) API 단일 소스에 얼마나 깊이 의존하고 있는지를
코드 수준에서 체계적으로 정리한 분석 보고서입니다.

### 1.1 분석 목적

- KOSIS 외 추가 데이터 소스(공공데이터포털, 마이크로데이터 등) 연동 가능성 평가
- 단일 소스 의존으로 인한 리스크 식별
- 이슈 #26(다중 소스 설계) 작업의 입력 자료 제공

### 1.2 분석 대상 파일

| 파일 | 역할 |
|------|------|
| `config.py` | 환경변수 로딩, KOSIS 설정값 관리 |
| `db.py` | DB 연결, KOSIS 메타/통계 소스 조회 |
| `kosis_api.py` | KOSIS REST API 클라이언트 |
| `main.py` | 수집 진입점, 흐름 제어 |

---

## 2. 파일별 KOSIS 의존부 인벤토리

*각 섹션은 순차적으로 채워집니다.*
### 2.1 db.py

`db.py`는 KOSIS 관련 DB 조회를 담당하는 데이터 접근 계층입니다.
KOSIS 단일 소스 식별자를 모듈 최상위에서 고정하고, 3개의 조회 함수를 통해 수집 대상 정보를 제공합니다.

#### 2.1.1 모듈 레벨 상수 및 초기화

| 위치 | 코드 | 역할 |
|------|------|------|
| 모듈 최상위 | `EXT_SYS_KOSIS = get_kosis_sys()` | KOSIS 시스템 식별자(`'KOSIS'`)를 모듈 로드 시 고정 |
| 모듈 최상위 | `engine = create_engine(DB_URL)` | DB 엔진 단일 인스턴스 생성 |

> **핫스팟**: `EXT_SYS_KOSIS` 상수가 모듈 레벨에서 한 번 결정됩니다.
> 추후 다중 소스 지원 시 이 상수에 의존하는 모든 쿼리를 파라미터화해야 합니다.

#### 2.1.2 KOSIS 의존 함수 목록

| 함수 | 테이블 | WHERE 조건 | 반환 구조 | KOSIS 하드코딩 여부 |
|------|--------|-----------|-----------|:-------------------:|
| `get_api_info()` | `sys_ext_api_info` | `ext_sys = EXT_SYS_KOSIS` (= `'KOSIS'`) | `dict` (1건) | ✅ |
| `get_stats_src_api_info(ext_api_id)` | `sys_stats_src_api_info` | `ext_api_id` (KOSIS로부터 조회된 ID) | `list[dict]` | 간접 의존 |
| `get_stats_src_data_info(ext_api_id, stat_tbl_id_list)` | `stats_src_data_info` | `ext_api_id`, `stat_tbl_id_list` | `dict[stat_tbl_id → dict]` | 간접 의존 |

#### 2.1.3 `get_api_info()` 상세 분석

```python
# db.py:37-57
query = text("""
    SELECT ext_api_id, if_name, ext_sys, ext_url, auth, data_format, ...
    FROM sys_ext_api_info 
    WHERE ext_sys = :ext_sys AND del_yn = 'N' AND status = 'A'
""")
result = session.execute(query, {'ext_sys': EXT_SYS_KOSIS})
row = result.fetchone()  # ← 단 1건만 가져옴
```

**문제점**: `fetchone()` 사용으로 KOSIS 소스가 1건이라고 암묵적으로 가정합니다.
복수 외부 API가 존재해도 첫 번째 행만 사용합니다.

#### 2.1.4 반환 딕셔너리 키 (api_info 구조)

`get_api_info()`가 반환하는 `api_info` 딕셔너리는 이후 모든 흐름에서 참조됩니다.

| 키 | 설명 | 하위 사용처 |
|----|------|-------------|
| `ext_api_id` | KOSIS 외부 API ID | `get_stats_src_api_info()`, `get_stats_src_data_info()` |
| `ext_url` | KOSIS API 기본 URL | `kosis_api.py: build_kosis_url()` |
| `auth` | KOSIS API 인증키 | `kosis_api.py: build_kosis_url()` |
| `data_format` | 기본 응답 형식 | 참조 |
### 2.2 kosis_api.py

`kosis_api.py`는 KOSIS REST API와 직접 통신하는 전용 클라이언트 모듈입니다.
모든 함수가 KOSIS 전용 파라미터 구조에 완전히 의존하며, 다른 외부 API와 공유할 수 있는 추상화가 존재하지 않습니다.

#### 2.2.1 공개 함수 목록

| 함수 | KOSIS 엔드포인트 키 | 반환 타입 | 비고 |
|------|---------------------|-----------|------|
| `build_kosis_url(...)` | `api_data_url` / `api_meta_url` / `api_latest_chn_dt_url` | `(url, format)` | URL 빌더 |
| `fetch_kosis_meta(...)` | `api_meta_url` | JSON 또는 텍스트 | 통계 메타정보 |
| `fetch_kosis_latest(...)` | `api_latest_chn_dt_url` | JSON 또는 텍스트 | 최신 변경일 |
| `fetch_kosis_data(...)` | `api_data_url` | JSON 또는 텍스트 | 실제 통계 데이터 (retry 포함) |
| `fetch_kosis_data_single(...)` | `api_data_url` | JSON 또는 텍스트 | 단일 기간 수집 |
| `fetch_kosis_data_split(...)` | `api_data_url` | `list` | 기간 분할 재귀 수집 |
| `fetch_kosis_data_with_retry(...)` | `api_data_url` | JSON 또는 `list` | Error 31 자동 분할 진입점 |
| `is_error_31(response)` | — | `bool` | KOSIS 전용 오류 코드 판별 |

#### 2.2.2 `build_kosis_url()` 파라미터 의존 분석

```python
def build_kosis_url(api_info, stats_src, stats_src_data_info, url_key, from_year=None, to_year=None):
    base_url  = api_info.get('ext_url', '')       # DB: sys_ext_api_info.ext_url
    url_info  = stats_src.get(url_key)             # DB: sys_stats_src_api_info.<url_key>
    url = url.replace('{API_AUTH_KEY}', api_info.get('auth', ''))  # DB: sys_ext_api_info.auth
    url = url.replace('{from}', str(from_year))   # DB: stats_src_data_info.collect_start_dt
    url = url.replace('{to}', str(to_year))       # DB: stats_src_data_info.collect_end_dt
```

모든 URL 구성 요소(base URL, 인증키, 기간 파라미터)가 DB에서 조회된 KOSIS 전용 딕셔너리로부터 추출됩니다.
다른 API를 위한 URL 빌더를 추가하려면 별도 함수 또는 전략 패턴이 필요합니다.

#### 2.2.3 KOSIS Error 31 전용 로직

KOSIS API는 조회 데이터 건수가 한계를 초과하면 `err: '31'`을 반환합니다.
이를 처리하기 위한 재귀 분할 수집 로직이 내장되어 있습니다.

```
fetch_kosis_data()
  └─ fetch_kosis_data_with_retry()  : 전체 기간 1차 시도
       └─ fetch_kosis_data_split()  : Error 31 시 재귀 분할 (1/2 씩)
            └─ fetch_kosis_data_single()  : 단일 기간 실제 HTTP 요청
```

이 로직은 KOSIS Error 코드 체계에 특화되어 있어 다른 데이터 소스에 직접 재사용할 수 없습니다.

#### 2.2.4 KOSIS 전용 3개 엔드포인트 구조

| 엔드포인트 키 | 용도 | 저장 위치 |
|---------------|------|-----------|
| `api_meta_url` | 통계표 메타정보 (항목 구조, 분류 등) | `kosis_data/<date>/meta/` |
| `api_latest_chn_dt_url` | 통계표 최신 변경일 조회 | `kosis_data/<date>/latest/` |
| `api_data_url` | 실제 통계 데이터 (기간 기반) | `kosis_data/<date>/data/` |

3개 엔드포인트 모두 `sys_stats_src_api_info` 테이블의 JSON 컬럼에 URL 템플릿으로 저장되어 있으며,
다른 API 소스는 동일한 컬럼 구조를 가져야 호환이 가능합니다.
### 2.3 main.py

`main.py`는 데이터 수집의 진입점으로, KOSIS 전용 모듈들을 직접 import하고
수집 흐름 전체를 조율합니다.

#### 2.3.1 KOSIS 관련 import 구조

```python
# main.py 상단 import 선언
from db import get_db_url, get_api_info, get_stats_src_api_info, get_stats_src_data_info
from kosis_api import fetch_kosis_data, fetch_kosis_meta, fetch_kosis_latest
from config import load_target_src_tbl_id_list, get_log_level, get_data_collection_scope, get_parallel_workers_file
```

KOSIS 관련 함수 3개(`fetch_kosis_data`, `fetch_kosis_meta`, `fetch_kosis_latest`)가
모두 `kosis_api` 모듈에서 직접 import됩니다. 추상화 계층 없이 구현에 직접 결합되어 있습니다.

#### 2.3.2 호출 진입점 함수별 KOSIS 의존 현황

| 함수 | KOSIS 의존 방식 | 세부 내용 |
|------|----------------|-----------|
| `check_required_env_and_args()` | `config.get_db_url()` | DB_URL 미설정 시 `sys.exit(1)` |
| `get_filtered_stats_src_list()` | `get_api_info()` → KOSIS `ext_sys='KOSIS'` 조회 | 수집 대상 목록 구성 전체가 KOSIS API 정보 기반 |
| `save_single_file(args)` | `fetch_kosis_meta`, `fetch_kosis_latest`, `fetch_kosis_data` 직접 호출 | 통계소스 1건당 3개 KOSIS 엔드포인트 순차 호출 |
| `save_all_files(...)` | `save_single_file` 병렬 실행 | ThreadPoolExecutor로 병렬화, 내부는 동일 KOSIS 의존 |
| `main()` | 위 모든 함수 순차 호출 | KOSIS → 파일 → (선택) DB 삽입 전체 흐름 |

#### 2.3.3 저장 경로 패턴 (KOSIS 전용 디렉터리 구조)

```python
# main.py: create_data_save_directory()
data_root = "kosis_data"          # 최상위 디렉터리명에 'kosis' 고정
root_dir  = os.path.join(data_root, today_str)
data_dir  = os.path.join(root_dir, "data")
meta_dir  = os.path.join(root_dir, "meta")
latest_dir = os.path.join(root_dir, "latest")
```

저장 디렉터리명 `kosis_data/`가 KOSIS에 하드코딩되어 있습니다.
다중 소스 지원 시 소스별로 디렉터리를 분리해야 합니다 (예: `ext_data/<source>/<date>/`).

#### 2.3.4 `save_single_file()` 호출 흐름

```
save_single_file(api_info, stats_src, dirs, data_info)
    │
    ├─ fetch_kosis_meta(api_info, stats_src, data_info)
    │       └─ build_kosis_url(..., 'api_meta_url')   → GET 요청
    │
    ├─ fetch_kosis_latest(api_info, stats_src, data_info)
    │       └─ build_kosis_url(..., 'api_latest_chn_dt_url') → GET 요청
    │
    └─ fetch_kosis_data(api_info, stats_src, data_info)
            └─ fetch_kosis_data_with_retry(...)
                 └─ fetch_kosis_data_single / fetch_kosis_data_split (재귀)
```

통계소스 1건 처리 시 최소 3회, Error 31 발생 시 추가 분할 요청으로 KOSIS 서버에 의존합니다.

#### 2.3.5 config.py KOSIS 설정값 핫스팟 (참고)

`config.py`에는 `main.py`와 `db.py`에서 직접 참조되는 KOSIS 전용 환경변수 4개가 있습니다.

| 환경변수 키 | 기본값 | 사용처 |
|-------------|--------|--------|
| `EXT_API_INFO_KOSIS_SYS` | `'KOSIS'` | `db.py: EXT_SYS_KOSIS` — DB 조회 조건 |
| `MAX_KOSIS_API_GET_DATA_CNT` | `40000` | (현재 `kosis_api.py`에서 미사용, 설정값으로만 존재) |
| `DATA_COLLECTION_SCOPE` | `'ALL'` | `main.py: get_filtered_stats_src_list()` |
| `CHECK_DATA_LATEST_DATE_MODE` | `'OFF'` | `db_processing.py` 연동 예정 |
---

## 3. 데이터 흐름과 의존 핫스팟

### 3.1 전체 데이터 흐름도

```
[.env / DB]
    │
    ├─ config.py ─── EXT_API_INFO_KOSIS_SYS='KOSIS'
    │                MAX_KOSIS_API_GET_DATA_CNT=40000
    │
    └─ db.py
         │
         ├─ get_api_info()
         │    SQL: sys_ext_api_info WHERE ext_sys='KOSIS'
         │    → api_info { ext_api_id, ext_url, auth, ... }
         │
         ├─ get_stats_src_api_info(ext_api_id)
         │    SQL: sys_stats_src_api_info WHERE ext_api_id=<KOSIS ID>
         │    → stats_src_list [ { stat_tbl_id, api_data_url, api_meta_url, ... } ]
         │
         └─ get_stats_src_data_info(ext_api_id, stat_tbl_id_list)
              SQL: stats_src_data_info WHERE ext_api_id=<KOSIS ID>
              → { stat_tbl_id → { collect_start_dt, collect_end_dt, ... } }

                          ↓ (api_info + stats_src + data_info)

main.py: save_all_files() [ThreadPoolExecutor]
    └─ save_single_file() ×N (통계소스 건수)
         │
         ├─ kosis_api.fetch_kosis_meta()
         │    build_kosis_url(..., 'api_meta_url')
         │    → GET https://kosis.kr/openapi/... → 메타 파일 저장
         │
         ├─ kosis_api.fetch_kosis_latest()
         │    build_kosis_url(..., 'api_latest_chn_dt_url')
         │    → GET https://kosis.kr/openapi/... → latest 파일 저장
         │
         └─ kosis_api.fetch_kosis_data()
              fetch_kosis_data_with_retry()
                └─ fetch_kosis_data_single() → GET https://kosis.kr/openapi/...
                     Error 31? → fetch_kosis_data_split() [재귀]
              → 데이터 파일 저장

                          ↓ (--mode db 시)

db_processing.process_db_insertion()
    → 파일 파싱 → DB 삽입
```

### 3.2 단일 소스 가정이 박힌 위치 (핫스팟 목록)

| # | 위치 | 코드/SQL | 의존 유형 | 심각도 |
|---|------|---------|-----------|:------:|
| H1 | `config.py:15` | `_EXT_API_INFO_KOSIS_SYS = os.getenv('EXT_API_INFO_KOSIS_SYS', 'KOSIS')` | 환경변수 기본값 하드코딩 | 중 |
| H2 | `db.py:18` | `EXT_SYS_KOSIS = get_kosis_sys()` | 모듈 레벨 상수 — 런타임 변경 불가 | 높음 |
| H3 | `db.py:44` | `WHERE ext_sys = :ext_sys` + `fetchone()` | 단일 API 소스만 조회 가능 | 높음 |
| H4 | `kosis_api.py` 전체 | 모든 함수 시그니처: `(api_info, stats_src, stats_src_data_info)` | KOSIS 전용 딕셔너리 구조 의존 | 높음 |
| H5 | `kosis_api.py:is_error_31()` | `response.get('err') == '31'` | KOSIS 전용 오류 코드 | 중 |
| H6 | `main.py:import` | `from kosis_api import ...` | 구현 직접 결합, 추상화 없음 | 높음 |
| H7 | `main.py:create_data_save_directory()` | `data_root = "kosis_data"` | 저장 경로에 소스명 하드코딩 | 중 |
| H8 | `main.py:save_single_file()` | `fetch_kosis_meta`, `fetch_kosis_latest`, `fetch_kosis_data` 직접 호출 | 3개 엔드포인트 패턴이 KOSIS 전용 | 높음 |

### 3.3 다중 소스 연동 시 영향 범위 추정

현재 구조에서 새 데이터 소스(예: 공공데이터포털)를 추가하려면 수정이 필요한 파일과 범위:

| 파일 | 수정 필요 이유 | 예상 난이도 |
|------|--------------|:-----------:|
| `config.py` | 소스별 환경변수 구조화 | 낮음 |
| `db.py` | `fetchone()` → `fetchall()`, `ext_sys` 파라미터화 | 중간 |
| `kosis_api.py` | 소스 독립적 클라이언트 인터페이스 추출 또는 래퍼 분리 | 높음 |
| `main.py` | import 추상화, 저장 경로 소스별 분리, 흐름 제어 범용화 | 높음 |
| `db_processing.py` | 파일 파싱 로직 소스별 분기 (JSON/XML 이미 일부 지원) | 중간 |


