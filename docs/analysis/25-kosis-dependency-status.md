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


