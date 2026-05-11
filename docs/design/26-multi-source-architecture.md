# 멀티 외부 API 소스 지원 확장 아키텍처 설계

> 이슈 #26 설계 문서 | 버전: v1.2.0 | 입력 자료: `docs/analysis/25-kosis-dependency-status.md`

## 목차

1. [배경 및 설계 목표](#1-배경-및-설계-목표)
2. [BaseCollector 추상화](#2-basecollector-추상화)
3. [DB 스키마 확장](#3-db-스키마-확장)
4. [설정 스키마](#4-설정-스키마)
5. [마이그레이션 플랜](#5-마이그레이션-플랜)
6. [예제 어댑터 구현 개요](#6-예제-어댑터-구현-개요)
7. [#27/#28/#29 작업 분해 매핑](#7-272829-작업-분해-매핑)

---

## 1. 배경 및 설계 목표

본 설계 문서는 이슈 #25 의 분석 결과(`docs/analysis/25-kosis-dependency-status.md`)를
설계 입력으로 받아, `08-IITP-DABT-PreProcessing` 레포가 KOSIS 단일 소스에서
**멀티 외부 API 소스** 를 지원하도록 확장하는 아키텍처를 정의합니다.

### 1.1 분석 결과 요약 (분석 #25 인용)

분석 #25 의 §4.1 표에 따르면 현 구조의 KOSIS 단일 소스 의존 현황은 다음과 같습니다.

| 지표 | 분석 #25 §4.1 결과 |
|---|---|
| KOSIS 의존 핫스팟 수 | 8개 (H1~H8) |
| 추상화 계층 존재 여부 | 없음 — 구현에 직접 결합 |
| 다른 소스 추가 시 수정 필요 파일 수 | 5개 전체 (config.py / db.py / kosis_api.py / main.py / db_processing.py) |
| KOSIS 전용 오류 처리 로직 | Error 31 재귀 분할 수집 (kosis_api.py 전용) |
| 저장 경로 소스명 하드코딩 | `kosis_data/` 디렉터리 |

핫스팟 분류 (분석 #25 §3.2):

- **데이터 접근 계층 (H1~H3)**: `config.py` 환경변수, `db.py` 모듈 상수 `EXT_SYS_KOSIS`, `get_api_info() fetchone()`
- **API 클라이언트 계층 (H4~H5)**: `kosis_api.py` 전체 함수 시그니처, KOSIS Error 31 전용 로직
- **수집 진입점 계층 (H6~H8)**: `main.py` 직접 import, `kosis_data/` 경로 하드코딩, 3개 엔드포인트 직접 호출

### 1.2 설계 목표

| # | 목표 | 비기능 요건 |
|---|---|---|
| G1 | 외부 API 소스를 **플러그인 식** 으로 추가 가능 (KOSIS / 공공데이터포털 / 마이크로데이터 등) | 신규 소스 1건 추가 시 단일 파일 추가로 완결 |
| G2 | 기존 KOSIS 수집 동작 **후방호환 보장** | 마이그레이션 중·후 KOSIS 수집 정상 동작 |
| G3 | DB 스키마 변경 **최소화** (기존 컬럼 활용) | `sys_ext_api_info` 기존 `ext_sys` 컬럼 활용 |
| G4 | `.env` 키 명명 규칙 **일관성** | `<EXT_SYS>_<KEY>` 형식 |
| G5 | 저장 경로 **소스별 분리** | `ext_data/<ext_sys>/<date>/...` |

### 1.3 비범위 (Out of scope)

- 본 설계 문서는 **구현 PR 이 아닙니다** — 구현은 #27 (인터페이스 추출), #28 (DB·설정 일반화), #29 (신규 소스 어댑터 추가) 에서 분담합니다.
- 본 설계 문서는 **API 응답 형식 표준화** (JSON ↔ XML 통합) 를 포함하지 않습니다 — 별도 이슈로 분리 권장.
- 본 설계 문서는 **DB 적재 단계 (`db_processing.py`)** 의 소스별 파싱 분기를 상위 수준에서만 다룹니다 — 상세는 #28 본문에서.

---

## 2. BaseCollector 추상화

본 절은 외부 API 소스 추가의 핵심 추상화인 `BaseCollector` 인터페이스를 정의합니다.
설계 목표 G1 (플러그인식 추가) 과 G2 (후방호환) 를 충족합니다.

### 2.1 클래스 계층 구조

```
BaseCollector (abstract)
    │
    ├─ KosisCollector(BaseCollector)        ← 기존 kosis_api.py 함수들을 래핑
    ├─ DataGoKrCollector(BaseCollector)     ← 공공데이터포털 (향후 #29)
    └─ <NewSource>Collector(BaseCollector)  ← 신규 소스
```

### 2.2 BaseCollector 인터페이스 시그니처

```python
# trader/collectors/base.py (신규)
from abc import ABC, abstractmethod
from typing import Any

class BaseCollector(ABC):
    """외부 API 소스 수집기 공통 인터페이스.

    구현체는 ext_sys 식별자(예: 'KOSIS', 'DATA_GO_KR') 를 클래스 속성으로 가집니다.
    """

    EXT_SYS: str  # 구현체에서 'KOSIS' 등으로 설정

    def __init__(self, api_info: dict, stats_src: dict):
        """
        api_info: db.get_api_info(ext_sys) 가 반환한 단건 dict
        stats_src: db.get_stats_src_api_info(ext_api_id) 결과 1행
        """
        self.api_info = api_info
        self.stats_src = stats_src

    @abstractmethod
    def fetch_meta(self, data_info: dict) -> dict | str:
        """통계표 메타정보 조회. 응답을 dict (JSON) 또는 str (텍스트) 로 반환."""

    @abstractmethod
    def fetch_latest(self, data_info: dict) -> dict | str:
        """통계표 최신 변경일 조회."""

    @abstractmethod
    def fetch_data(self, data_info: dict) -> dict | list | str:
        """실제 통계 데이터 조회. 소스 고유 재시도/분할 로직은 구현체 내부에서 처리."""

    @abstractmethod
    def is_retryable_error(self, response: Any) -> bool:
        """소스별 재시도 가능 오류 판별 (KOSIS Error 31 등)."""

    # --- 공통 유틸 ---
    def save_response(self, response, save_dir: str, filename: str) -> str:
        """응답을 파일로 저장 (소스별 동일 동작). 반환: 저장된 절대경로."""
        # 구현체에서 override 불필요 — 공통 구현
        ...
```

### 2.3 책임 경계

| 항목 | BaseCollector (공통) | 구현체 (예: KosisCollector) |
|---|---|---|
| 파일 저장 | ✅ `save_response()` | — |
| URL 빌드 | — | ✅ 내부 helper |
| 인증키 주입 | — | ✅ 내부 helper |
| 재시도/분할 로직 | — | ✅ `fetch_data()` 내부 |
| 오류 판별 | 인터페이스만 | ✅ `is_retryable_error()` 구현 |
| 응답 파싱 | — | ✅ `fetch_*()` 반환 형식 통일 책임 |

### 2.4 후방호환 전략

기존 `kosis_api.py` 의 함수형 API (`fetch_kosis_meta`, `fetch_kosis_latest`, `fetch_kosis_data`) 는
**삭제하지 않고 thin wrapper 로 유지** 합니다.

```python
# kosis_api.py — 후방호환 wrapper (마이그레이션 기간 동안 유지)
def fetch_kosis_meta(api_info, stats_src, data_info):
    """후방호환 wrapper. 신규 코드는 KosisCollector.fetch_meta() 사용 권장."""
    return KosisCollector(api_info, stats_src).fetch_meta(data_info)
```

이로써 `main.py` 의 import 와 호출부를 단계적으로 마이그레이션할 수 있습니다 (§5 참조).


---

## 3. DB 스키마 확장

본 절은 멀티 소스 지원을 위한 DB 스키마 변경안을 정의합니다. 설계 목표 G3 (스키마 변경 최소화) 를 충족합니다.

### 3.1 변경 원칙

| 원칙 | 설명 |
|---|---|
| **기존 컬럼 활용** | `sys_ext_api_info.ext_sys` 컬럼이 이미 존재 — 새 컬럼 추가 없이 활용 |
| **default 'KOSIS'** | 기존 행은 `ext_sys='KOSIS'` 로 가정 — 후방호환 |
| **신규 행 추가 방식** | 신규 소스는 `ext_sys='DATA_GO_KR'` 등으로 row 추가 |
| **마이그레이션 SQL** | 데이터 백필 1회 + 인덱스 추가 (필요 시) |

### 3.2 영향받는 테이블 — `sys_ext_api_info`

분석 #25 §2.1.2 표에서 식별된 KOSIS 의존 함수 `get_api_info()` 의 SQL 조건이 본 스키마와 직접 결합합니다.

| 컬럼 | 기존 | 변경 후 | 비고 |
|---|---|---|---|
| `ext_api_id` | PK | (변경 없음) | |
| `ext_sys` | varchar (값: 'KOSIS') | varchar (값: 'KOSIS' / 'DATA_GO_KR' / ...) | **소스 구분자** |
| `ext_url` | varchar | (변경 없음) | base URL |
| `auth` | varchar | (변경 없음) | 인증키 |
| `data_format` | varchar | (변경 없음) | 기본 응답 형식 |
| `del_yn`, `status` | char | (변경 없음) | 조회 조건 |

> **인덱스 추가 권장**: `idx_sys_ext_api_info_ext_sys ON sys_ext_api_info(ext_sys, del_yn, status)` — `get_api_info()` 조회 성능 보장.

### 3.3 `db.py` 함수 시그니처 변경안

분석 #25 §2.1.2 표의 3개 함수 시그니처를 다음과 같이 일반화합니다.

#### 3.3.1 `get_api_info()` — `ext_sys` 파라미터 추가

```python
# db.py (변경 후)
def get_api_info(ext_sys: str = 'KOSIS') -> dict:
    """외부 API 정보 1건 조회.

    ext_sys: 'KOSIS' / 'DATA_GO_KR' / ... (default 'KOSIS' — 후방호환)
    반환: 단건 dict (분석 #25 §2.1.4 의 키 구조 그대로 유지)
    """
    query = text("""
        SELECT ext_api_id, if_name, ext_sys, ext_url, auth, data_format, ...
        FROM sys_ext_api_info
        WHERE ext_sys = :ext_sys AND del_yn = 'N' AND status = 'A'
    """)
    result = session.execute(query, {'ext_sys': ext_sys})
    row = result.fetchone()  # 분석 #25 §2.1.3 의 fetchone() 패턴 유지 — 소스당 1건 가정 변경 X
    return dict(row._mapping) if row else None
```

**후방호환**: `ext_sys` 가 default='KOSIS' 이므로 기존 호출부 `get_api_info()` 는 변경 없이 동작.

> ⚠️ **fetchone() 패턴 유지 결정**: 분석 #25 §4.1 에서 `fetchone()` 의 "복수 API 소스 등록 시 첫 행만 사용" 문제가 지적되었으나, 본 설계에서는 **소스당 단건** 정책을 유지합니다 (소스별 row 1개, ext_sys 로 라우팅). 향후 동일 소스의 복수 등록이 필요할 경우 별도 이슈로 분리.

#### 3.3.2 `get_stats_src_api_info(ext_api_id)` — 변경 없음

분석 #25 §2.1.2 표 그대로 `ext_api_id` 파라미터만 받으므로 소스 일반화 영향 없음. 시그니처 유지.

#### 3.3.3 `get_stats_src_data_info(ext_api_id, stat_tbl_id_list)` — 변경 없음

마찬가지로 `ext_api_id` 기반이므로 소스 일반화 영향 없음. 시그니처 유지.

### 3.4 `EXT_SYS_KOSIS` 모듈 상수 처리 (H2 핫스팟)

분석 #25 §3.2 의 H2 핫스팟 — `db.py` 의 모듈 레벨 상수 `EXT_SYS_KOSIS = get_kosis_sys()` 는 다음과 같이 처리합니다.

| 시점 | 처리 |
|---|---|
| 구현 #27 직후 | 모듈 상수 유지 (후방호환 wrapper 가 default 로 사용) |
| 구현 #28 완료 후 | 모듈 상수 제거 검토 — `config.py` 의 `get_ext_sys_list()` 가 대체 |
| 신규 소스 #29 추가 시 | `config.get_ext_sys_list()` 가 반환하는 리스트로 루프 — 모듈 상수 의존 사라짐 |

### 3.5 마이그레이션 SQL 스크립트 (참고)

```sql
-- 기존 KOSIS 행 명시 (이미 ext_sys='KOSIS' 라면 no-op)
UPDATE sys_ext_api_info SET ext_sys = 'KOSIS' WHERE ext_sys IS NULL OR ext_sys = '';

-- 인덱스 추가
CREATE INDEX IF NOT EXISTS idx_sys_ext_api_info_ext_sys
    ON sys_ext_api_info (ext_sys, del_yn, status);

-- 신규 소스 행 추가 예시 (#29 에서 실행)
INSERT INTO sys_ext_api_info (ext_sys, if_name, ext_url, auth, data_format, del_yn, status, ...)
VALUES ('DATA_GO_KR', '공공데이터포털', 'https://api.data.go.kr/...', 'API_KEY_HERE', 'json', 'N', 'A', ...);
```

> 본 SQL 은 설계 참고용 — 실제 마이그레이션은 #28 PR 에서 검토·실행.


---

## 4. 설정 스키마

본 절은 멀티 소스에 따른 `.env` 환경변수 명명 규칙 및 `config.py` 매핑 변경안을 정의합니다. 설계 목표 G4 (키 명명 일관성) 를 충족합니다.

### 4.1 현재 상태 (분석 #25 §2.3.5 인용)

분석 #25 §2.3.5 에서 식별된 KOSIS 전용 환경변수 4개:

| 환경변수 키 (현재) | 기본값 | 사용처 |
|---|---|---|
| `EXT_API_INFO_KOSIS_SYS` | `'KOSIS'` | `db.py: EXT_SYS_KOSIS` |
| `MAX_KOSIS_API_GET_DATA_CNT` | `40000` | (현재 미사용, 설정값으로만 존재) |
| `DATA_COLLECTION_SCOPE` | `'ALL'` | `main.py: get_filtered_stats_src_list()` |
| `CHECK_DATA_LATEST_DATE_MODE` | `'OFF'` | `db_processing.py` 연동 예정 |

### 4.2 새 명명 규칙 — `<EXT_SYS>_<KEY>`

소스별 환경변수는 모두 **`<대문자 EXT_SYS>_<KEY>`** 형식으로 통일합니다.

| 카테고리 | 명명 규칙 | 예시 |
|---|---|---|
| 소스 활성화 | `<EXT_SYS>_ENABLED` | `KOSIS_ENABLED=true`, `DATA_GO_KR_ENABLED=false` |
| 인증키 (DB 미저장 시 fallback) | `<EXT_SYS>_API_KEY` | `KOSIS_API_KEY=xxx` |
| 최대 데이터 건수 | `<EXT_SYS>_MAX_DATA_CNT` | `KOSIS_MAX_DATA_CNT=40000` (기존 `MAX_KOSIS_API_GET_DATA_CNT` 대체) |
| 타임아웃 (초) | `<EXT_SYS>_TIMEOUT_SEC` | `KOSIS_TIMEOUT_SEC=30` |
| 재시도 횟수 | `<EXT_SYS>_RETRY_COUNT` | `KOSIS_RETRY_COUNT=3` |

### 4.3 글로벌 설정 (소스 무관)

| 환경변수 키 | 기본값 | 비고 |
|---|---|---|
| `ENABLED_EXT_SYS_LIST` | `'KOSIS'` | 콤마 구분. 예: `'KOSIS,DATA_GO_KR'` — `main.py` 가 이 목록을 순회 |
| `DATA_COLLECTION_SCOPE` | `'ALL'` | (변경 없음 — 분석 #25 §2.3.5) |
| `CHECK_DATA_LATEST_DATE_MODE` | `'OFF'` | (변경 없음) |

### 4.4 `config.py` 함수 시그니처 변경안

```python
# config.py (변경 후)
def get_enabled_ext_sys_list() -> list[str]:
    """활성화된 외부 API 소스 식별자 리스트.

    예: 'KOSIS' 또는 'KOSIS,DATA_GO_KR' 형태의 환경변수를 파싱.
    main.py 가 이 리스트를 순회하며 소스별 수집을 수행.
    """
    raw = os.getenv('ENABLED_EXT_SYS_LIST', 'KOSIS')
    return [s.strip() for s in raw.split(',') if s.strip()]

def get_ext_sys_config(ext_sys: str) -> dict:
    """특정 소스의 설정값 묶음 반환.

    반환 키: 'enabled', 'api_key', 'max_data_cnt', 'timeout_sec', 'retry_count'
    환경변수가 없으면 합리적 기본값 사용.
    """
    return {
        'enabled':       os.getenv(f'{ext_sys}_ENABLED', 'true').lower() == 'true',
        'api_key':       os.getenv(f'{ext_sys}_API_KEY', ''),
        'max_data_cnt':  int(os.getenv(f'{ext_sys}_MAX_DATA_CNT', '40000')),
        'timeout_sec':   int(os.getenv(f'{ext_sys}_TIMEOUT_SEC', '30')),
        'retry_count':   int(os.getenv(f'{ext_sys}_RETRY_COUNT', '3')),
    }

# 후방호환 helper — 기존 함수명 유지
def get_kosis_sys() -> str:
    """후방호환. 신규 코드는 get_enabled_ext_sys_list() 사용."""
    return os.getenv('EXT_API_INFO_KOSIS_SYS', 'KOSIS')
```

### 4.5 환경변수 마이그레이션 매핑 (구->신)

기존 `.env` 운영자가 알아야 할 변경 매핑:

| 기존 키 | 신규 키 | 처리 방안 |
|---|---|---|
| `EXT_API_INFO_KOSIS_SYS` | (제거 권장) | 신규 코드는 `get_enabled_ext_sys_list()` 사용. 후방호환 wrapper `get_kosis_sys()` 가 당분간 유지 |
| `MAX_KOSIS_API_GET_DATA_CNT` | `KOSIS_MAX_DATA_CNT` | 둘 다 인식하도록 `config.py` 에서 fallback 처리 |
| (없음) | `ENABLED_EXT_SYS_LIST` | 신규 추가. 기본값 `'KOSIS'` — 기존 운영에 영향 없음 |
| (없음) | `KOSIS_API_KEY` | 선택 — DB 의 `sys_ext_api_info.auth` 가 우선. fallback 용도 |

### 4.6 `.env.example` 추가 권장

`#27` 또는 `#28` PR 에서 다음과 같은 `.env.example` 추가 권장 (실제 키 값은 빈 문자열).

```ini
# 멀티 소스 설정
ENABLED_EXT_SYS_LIST=KOSIS
DATA_COLLECTION_SCOPE=ALL
CHECK_DATA_LATEST_DATE_MODE=OFF

# KOSIS 소스
KOSIS_ENABLED=true
KOSIS_API_KEY=
KOSIS_MAX_DATA_CNT=40000
KOSIS_TIMEOUT_SEC=30
KOSIS_RETRY_COUNT=3

# 공공데이터포털 (예시 — #29 에서 활성화)
# DATA_GO_KR_ENABLED=false
# DATA_GO_KR_API_KEY=
# DATA_GO_KR_MAX_DATA_CNT=10000
```


---

## 5. 마이그레이션 플랜

*다음 commit 에서 채워집니다.*

---

## 6. 예제 어댑터 구현 개요

*다음 commit 에서 채워집니다.*

---

## 7. #27/#28/#29 작업 분해 매핑

*다음 commit 에서 채워집니다.*
