# KOSIS 데이터 API 연동 및 파일/DB 저장 툴

## 개요
KOSIS(국가통계포털) 데이터를 API를 통해 수집하여, 옵션에 따라 파일로 저장하거나 파일 저장 후 DB에 삽입하는 Python 기반 툴입니다.

## 주요 기능
- DB에서 KOSIS API 연동 정보 조회
- KOSIS API로 데이터/메타 수집
- 날짜별 폴더 및 규칙에 맞는 파일명으로 저장
- 옵션에 따라 파일 저장만 또는 DB 삽입까지 수행

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
    - Data: `{순서}.data_{stat_title}_{stat_tbl_id}_{yyyy.mm.dd.hhMMss}.json`
    - Meta: `{순서}.meta_{stat_title}_{stat_tbl_id}_{yyyymmddss}.xml`
- 같은 데이터의 Data/Meta 파일은 순서 일치

## DB 연동 정보
- API 연동 정보: `sys_ext_api_info`, `sys_stats_src_api_info` 테이블에서 조회
- DB 종류/접속 정보는 `.env` 또는 `config.py`에서 관리

## 폴더/파일 구조 예시
```
02.kosisDatApiLoader/
├── main.py
├── db.py
├── kosis_api.py
├── file_utils.py
├── db_insert.py
├── requirements.txt
└── README.md
```

## 필요 패키지 설치
```bash
pip install -r requirements.txt
```

## 환경설정
- DB 접속 정보 등은 `.env` 또는 `config.py`에 설정

## 참고
- Python 3.8 이상 권장
- DB 종류: (예시) MySQL, PostgreSQL 등 