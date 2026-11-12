# 수집 데이터 품질 측정 (`quality/`)

수집 DB 와 비정형 파일을 데이터 명세서의 규칙으로 검사해 연구개발계획서의 데이터 품질 지표를 산출한다.
DB 는 읽기 전용 세션으로 연결하고(`check`·`runs`·`profile`), 오류 주입 시험(`faults`)은 검증용 DB 에서만 실행한다.

## 지표와 산식

| # | 지표 | 산식 | 명령 |
|---|---|---|---|
| 1 | 프로파일링 정확도(컬럼 적합성) | 명세서 타입(숫자/문자/날짜)과 DB 타입이 일치한 컬럼 수 ÷ 검사한 컬럼 수 | `check` |
| 2 | 프로파일링 정확도(패턴) | `규칙(형식)`·`규칙(날짜)` 정규식 구문 유효성 통과율(값 단위) | `check` |
| 3 | 프로파일링 정확도(범위) | `범위(Min~Max)` 유효성 통과율(값 단위) | `check` |
| 4 | 수집 데이터 통계적 다양성 | 비정형 구분별로 하위 폴더 파일 비율이 3~30% 인 폴더의 파일 합 ÷ 구분 파일 수 | `files` |
| 5 | 전처리 품질 정확도 | (값 유일성 통과율 + 값 완전성 통과율) ÷ 2, 소수점 첫째 자리 내림 | `check` |
| 6 | 수집 데이터 자동화 비율 | 자동 수집한 비정형 파일 수 ÷ 전체 비정형 파일 수 | `files` |
| 7 | 갱신·오류 레포트 정확도 | 실행 기록이 실제와 맞은 실행 수 ÷ 대조한 실행 수(정상 실행 대조), 기대대로 보고된 시나리오 ÷ 실행 시나리오(오류 주입 시험) | `runs`·`faults` |

통과율은 컬럼별 비율의 평균이 아니라 어긋난 수와 검사한 수를 각각 더해서 나눈다.

## 데이터 명세서

CSV 한 장. 열: `seq,table,column,type,key,not_null,range_min,range_max,rule_format,rule_date,unique,desc`

- `type` 숫자/문자/날짜 · `not_null`·`unique` 는 O 표시 · `rule_format` 은 정규식(값 전체 일치) · `rule_date` 는 `YYYY`·`YYYYMM`·`YYYYMMDD`·`YYYY-MM`·`YYYY-MM-DD` 또는 정규식
- `profile` 이 DB 카탈로그와 실제 값 분포로 초안을 만든다. 범위·정규식은 비워 두고 관측값(`obs_*`)을 붙인다 — 작업자가 관측값을 보고 정한 뒤 측정 전에 확정한다.

## 실행

```bash
python -m quality profile --out spec_draft.csv [--base 기존명세서.csv]      # 명세서 초안
python -m quality check   --spec spec.csv --out result/                    # 지표 1·2·3·5
python -m quality files   --config files.json --out result/                # 지표 4·6
python -m quality runs    --days 90 --out result/                          # 지표 7 (가)
python -m quality faults  --scenarios scenarios.json --db-url <검증용 DB> --out result/   # 지표 7 (나)
```

- DB 주소는 `--db-url` 이 없으면 `.env` 의 `DB_URL` 을 쓴다.
- 결과는 `result/` 에 `<명령>.csv`(컬럼·실행·시나리오별 상세)와 `<명령>_summary.json`(지표 값, 측정 시각, 명세서 해시)으로 남는다.
- `files` 설정·`faults` 시나리오 형식은 `quality/files.py`·`quality/fault_injection.py` 머리말에 있다.

## 실행 기록 대조 항목 (`runs`)

| 항목 | 확인 내용 |
|---|---|
| R1 | 종료 코드와 상태가 맞음(SUCCESS=0, PARTIAL=2, 그 밖=1) |
| R2 | SUCCESS 면 실패 0, 아니면 실패 1 이상 또는 오류 요약 있음 |
| R3 | 성공+실패 건수가 대상 수를 넘지 않음 |
| R4 | 실패 목록 길이 = 실패 건수 |
| R5 | 끝난 시각 ≥ 시작 시각 |
| R6 | 정기 실행(db, 전체) 실패에 알림 처리 결과가 남아 있음 |
| R7 | 수집원별 최근 성공 실행 뒤 `sys_ext_api_info` 동기화 시각이 갱신됨 |
