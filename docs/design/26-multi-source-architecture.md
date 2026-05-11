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

*다음 commit 에서 채워집니다.*

---

## 4. 설정 스키마

*다음 commit 에서 채워집니다.*

---

## 5. 마이그레이션 플랜

*다음 commit 에서 채워집니다.*

---

## 6. 예제 어댑터 구현 개요

*다음 commit 에서 채워집니다.*

---

## 7. #27/#28/#29 작업 분해 매핑

*다음 commit 에서 채워집니다.*
