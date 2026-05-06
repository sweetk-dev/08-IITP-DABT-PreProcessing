# KOSIS 데이터 API 연동 및 파일/DB 저장 툴

## 개요
KOSIS(국가통계포털) 데이터를 API를 통해 수집하여, 옵션에 따라 파일로 저장하거나 파일 저장 후 DB에 삽입하는 Python 기반 툴입니다.

## 주요 기능
- DB에서 KOSIS API 연동 정보 조회
- KOSIS API로 데이터/메타 수집
- 날짜별 폴더 및 규칙에 맞는 파일명으로 저장
- 옵션에 따라 파일 저장만 또는 DB 삽입까지 수행
- 통계 데이터 통합 테이블 자동 이관
- 과거 데이터 자동 정리

## 실행 방법
```bash
python main.py --mode file   # 파일 저장만
python main.py --mode db     # 파일 저장 + DB 삽입
```

## 실행 옵션
- `--mode file` : API 데이터 파일로만 저장
- `--mode db`   : API 데이터 파일 저장 후 DB 삽입

## 파일 저장 규칙
- 실행 위치 기준, 오늘 날짜(YYYYMMDD) 폴더 생성
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
| `DB_URL` | ✅ | — | `postgresql://...` | PostgreSQL 접속 URL |
| `DB_BATCH_SIZE` | — | `100` | 정수 | DB 배치 삽입 크기 |
| `LOG_LEVEL` | — | `INFO` | `DEBUG` `INFO` `WARNING` `ERROR` | 로그 출력 레벨 |
| `EXT_API_INFO_KOSIS_SYS` | — | `KOSIS` | 문자열 | KOSIS 시스템 구분 코드 |
| `MAX_KOSIS_API_GET_DATA_CNT` | — | `40000` | 정수 | KOSIS API 1회 최대 조회 건수 |
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
- **트랜잭션 처리**: DB 처리 중 오류 발생 시 전체 트랜잭션 롤백
- **데이터 중복 방지**: 동일한 날짜 데이터는 기존 데이터 삭제 후 신규 삽입
- **과거 데이터 관리**: 자동으로 이전 버전 데이터 정리
- **에러 처리**: 필수 테이블 누락 시 프로그램 중단
- **로그 관리**: 실행 로그는 `logs/` 폴더에 날짜별 저장

## 참고
- Python 3.8 이상 권장
- DB 종류: PostgreSQL 권장
- 메모리: 대용량 데이터 처리 시 충분한 메모리 필요 
