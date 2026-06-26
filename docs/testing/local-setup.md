# 검증용 테스트 서버 — 로컬 setup 가이드

> 본 문서는 검증용 테스트 서버에서 본 봇의 동작을 검증하기 위한 1회성 setup 절차입니다.
> 운영 환경과 분리된 별도 환경에서 진행합니다. 운영 DB 에 영향이 가지 않도록 격리된 PostgreSQL 인스턴스 또는 별도 데이터베이스를 사용하세요.

---

## 0. 사전 요구사항

| 항목 | 권장 사양 |
|------|-----------|
| OS | Linux (RHEL / CentOS / Rocky / Ubuntu / Debian 계열) |
| Python | 3.9 이상 |
| PostgreSQL | 13 이상 |
| Git | 2.x 이상 |
| 디스크 | 10GB 이상 가용 (수집 데이터 + 로그) |
| 인터넷 | KOSIS API (`https://kosis.kr/`) 도달 가능 |

본 가이드의 명령은 일반 사용자 권한을 전제로 합니다 (`<USER>` 로 표기). PostgreSQL 관련 일부 명령은 sudo 가 필요할 수 있습니다.

---

## 1. PostgreSQL — 앱 전용 사용자 / DB 준비

운영과 비슷한 형태를 위해 **봇 전용 DB 사용자 (`iitp_app`)** 를 생성하고 비밀번호 인증을 사용합니다. 슈퍼유저 (`postgres`) 로 봇을 실행하지 않습니다.

### 1-1. 검증용 데이터베이스 준비

이미 운영 DB schema 가 복원된 검증용 DB 가 있으면 그것을 사용합니다. 없다면 새로 생성 후 schema 복원이 필요합니다 (별도 절차).

```bash
# 슈퍼유저로 접속
sudo -u postgres psql

-- 검증용 DB 확인 (없으면 생성)
-- \l 로 목록 확인 후, 필요 시:
-- CREATE DATABASE <TEST_DB> WITH ENCODING = 'UTF8' LC_COLLATE = 'ko_KR.UTF-8' LC_CTYPE = 'ko_KR.UTF-8';

\q
```

### 1-2. 앱 전용 사용자 생성 + 권한 부여

```sql
-- 슈퍼유저 (postgres) 로 접속 후 실행
sudo -u postgres psql -d <TEST_DB>

CREATE USER iitp_app WITH PASSWORD '<APP_DB_PASSWORD>';

-- 데이터베이스 접속 권한
GRANT CONNECT ON DATABASE <TEST_DB> TO iitp_app;

-- 스키마 권한 (public 스키마 기준)
GRANT USAGE, CREATE ON SCHEMA public TO iitp_app;

-- 기존 모든 테이블/시퀀스 권한
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO iitp_app;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO iitp_app;

-- 향후 추가될 테이블/시퀀스 권한
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO iitp_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO iitp_app;

\q
```

> `<APP_DB_PASSWORD>` 는 임의의 강력한 비밀번호로 교체. `.env` 파일에 동일하게 기입합니다.

### 1-3. 인증 정책 확인 (pg_hba.conf)

봇이 TCP 로 PostgreSQL 에 비밀번호로 접속할 수 있도록 `pg_hba.conf` 에 다음 항목이 있는지 확인합니다.

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    <TEST_DB>       iitp_app        127.0.0.1/32            scram-sha-256
```

> 기존 항목이 `ident` 또는 `trust` 로 되어 있다면 위 줄을 추가하는 편이 깔끔합니다. 변경 후 PostgreSQL 재시작 또는 reload:
> ```bash
> sudo systemctl reload postgresql
> ```

### 1-4. 접속 검증

```bash
psql "postgresql://iitp_app:<APP_DB_PASSWORD>@127.0.0.1:5432/<TEST_DB>" -c "SELECT current_user, current_database();"
```

`iitp_app | <TEST_DB>` 가 출력되면 정상.

---

## 2. 봇 코드 clone

검증용 테스트 서버의 작업 디렉터리를 미리 정해 둡니다. 본 가이드는 `<PROJECT_DIR>` 로 표기합니다 (예: `~/projects/08-IITP-DABT-PreProcessing`).

```bash
# 작업 디렉터리 생성
mkdir -p <PROJECT_DIR>
cd "$(dirname <PROJECT_DIR>)"

# subproject 브랜치 (일상 작업 브랜치) clone
git clone -b subproject <REPO_URL> "$(basename <PROJECT_DIR>)"
cd <PROJECT_DIR>

# 현재 위치 확인
git status
git log -1 --oneline
```

> `<REPO_URL>` 은 본 레포의 HTTPS 또는 SSH URL.

---

## 3. Python 가상환경 + 의존성 설치

```bash
cd <PROJECT_DIR>

# venv 생성
python3 -m venv venv
source venv/bin/activate

# pip 업그레이드 + 의존성 설치
pip install --upgrade pip
pip install -r requirements.txt

# 설치 확인
python -c "import requests, dotenv, sqlalchemy, psycopg2; print('deps OK')"
```

---

## 4. `.env` 작성

본 레포의 `.env.test.example` 을 복사해 시작합니다.

```bash
cd <PROJECT_DIR>
cp .env.test.example .env

# 에디터로 값 채우기
vi .env   # 또는 nano .env
```

채워야 할 핵심 값:

| 변수 | 설명 | 예 |
|------|------|-----|
| `DB_URL` | 검증용 DB 접속 URL | `postgresql://iitp_app:<APP_DB_PASSWORD>@127.0.0.1:5432/<TEST_DB>` |
| `LOG_LEVEL` | 검증 시 자세히 보려면 `DEBUG`, 운영처럼 보려면 `INFO` | `DEBUG` |
| `DB_BATCH_SIZE` | DB 배치 삽입 크기 | `100` |
| `PARALLEL_WORKERS_FILE` | 파일 저장 병렬 워커 (테스트는 작게) | `2` |
| `PARALLEL_WORKERS_DB` | DB 삽입 병렬 워커 (테스트는 작게) | `1` |
| `EXT_SYS` | 외부 시스템 식별자 (기본 KOSIS) | `KOSIS` |
| `DATA_COLLECTION_SCOPE` | 검증 시 부분 수집 권장 | `PARTIAL` |

> KOSIS API 키 는 본 봇에서 `sys_ext_api_info.auth` 컬럼에서 조회합니다. `.env` 에 별도 키를 두지 않습니다. 검증용 DB 의 `sys_ext_api_info` 에 KOSIS 항목이 등록되어 있어야 합니다 (`SELECT * FROM sys_ext_api_info WHERE ext_sys='KOSIS' AND status='A' AND del_yn='N';` 로 확인).

`PARTIAL` 수집 시 수집 대상 통계표 ID 는 `.env` 의 `[TARGET_SRC_TBL_ID_LIST]` 섹션에 추가합니다 (예시는 `.env.test.example` 참고).

---

## 5. 단위 테스트 — L1

코드가 검증용 서버의 Python 환경에서도 정상 동작하는지 확인.

```bash
cd <PROJECT_DIR>
source venv/bin/activate

# unittest (pytest 미설치 환경에서도 동작)
python -m unittest discover -s tests -v 2>&1 | tail -10

# 모든 케이스 OK 가 출력되면 통과
```

기대 출력:
```
Ran N tests in 0.XXXs
OK
```

---

## 6. 1차 검증 — `--mode file` (DB 조회 / KOSIS API / 파일 저장)

DB 에서 API 정보를 조회한 뒤 KOSIS API 를 호출하여 데이터를 **파일로만 저장**합니다 (DB 삽입은 일어나지 않음).

```bash
cd <PROJECT_DIR>
source venv/bin/activate

# 기본 (KOSIS)
python main.py --mode file

# 또는 명시적
python main.py --mode file --ext-sys KOSIS
```

검증 포인트:

| 확인 항목 | 방법 |
|-----------|------|
| 로그에 DB 조회 성공 | `외부 API 정보 조회 시작 (ext_sys=KOSIS)` 같은 라인 |
| KOSIS API 호출 성공 | 4xx/5xx 에러 없음 |
| 파일 저장 경로 | `<PROJECT_DIR>/kosis_data/<YYYYMMDD>/{data,meta,latest}/` 아래 |
| 파일 내용 | data/*.json 이 정상 JSON, meta/*.xml 이 정상 XML |

---

## 7. 2차 검증 — `--mode db` (DB 삽입 포함)

파일 저장 이후 DB 삽입까지 진행합니다.

```bash
cd <PROJECT_DIR>
source venv/bin/activate

python main.py --mode db --ext-sys KOSIS
```

검증 포인트:

| 확인 항목 | 방법 |
|-----------|------|
| `stats_kosis_origin_data` 새 row | `SELECT count(*), max(created_at) FROM stats_kosis_origin_data;` |
| `stats_kosis_metadata_code` 새 row | 동일 패턴 |
| 통합 테이블 (`stats_*`) row 증가 | 수집 대상 통계표에 해당하는 `stats_*` 테이블 |
| `stats_src_data_info` 갱신 | `last_sync_time` 같은 컬럼 갱신 여부 |
| `sys_data_summary_info` 갱신 | 시스템 요약 정보 |
| `sys_ext_api_info.last_sync_time` 갱신 | `SELECT ext_sys, last_sync_time FROM sys_ext_api_info;` |

---

## 8. 멀티소스 라우팅 검증 (v1.5.0 핵심)

KOSIS 가 아닌 다른 `ext_sys` 값을 지정했을 때의 동작:

```bash
# 미등록 ext_sys 는 ValueError 가 발생해야 정상 (Collector 레지스트리 안 됨)
python main.py --mode file --ext-sys DATA_GO_KR
```

기대: `ValueError: Unsupported ext_sys: DATA_GO_KR` 또는 비슷한 메시지로 즉시 종료. 무한 루프나 silent fail 이 일어나면 회귀 의심.

---

## 9. 검증 결과 보고 형식

검증 종료 후 다음 항목을 정리합니다.

```
[검증 결과 — vX.Y.Z]
- 단위 테스트: PASS / FAIL (실패 시 케이스 명)
- --mode file: 수집 통계표 N건 / 파일 M개 생성 / 에러 0건
- --mode db: 통합 테이블 row 증가 N건 / origin_data row 증가 M건 / 에러 0건
- 회귀 항목 (멀티소스 라우팅 등): PASS / FAIL
- 추가 관찰 사항: (있으면)
```

---

## 10. 트러블슈팅

| 증상 | 원인 | 조치 |
|------|------|------|
| `psycopg2.OperationalError: FATAL: password authentication failed` | `.env` 의 비밀번호 불일치 또는 pg_hba.conf 인증 정책 | §1-3 확인, 비밀번호 재설정 |
| `외부 API 정보가 DB에 없거나 삭제된 상태입니다 (ext_sys=KOSIS)` | `sys_ext_api_info` 에 KOSIS 행 없음 또는 `status≠'A'` / `del_yn≠'N'` | DB 에 직접 INSERT 또는 운영에서 schema dump 복원 시 데이터까지 포함 |
| `KOSIS API` 4xx 응답 | API 키 만료 또는 형식 오류 | `sys_ext_api_info.auth` 컬럼의 값 확인 |
| `IntegrityError` (UNIQUE / FK) | 검증 중 동일 데이터 재삽입 시도 | 정상 동작 — 이미 수집된 데이터는 skip 됨 |
| 파일 저장은 되는데 DB 삽입이 안 됨 | `--mode db` 가 아닌 `--mode file` 로 실행 | 모드 확인 |

---

## 11. 검증 후 데이터 정리

본 검증으로 들어간 데이터는 **그대로 유지**합니다 (재검증 시 누적 데이터를 비교 가능). 다음 검증 사이클이 시작될 때만 별도 정리.

정리가 필요하면:

```sql
-- 검증 데이터만 삭제 (이번 실행 분 — created_at 기준)
DELETE FROM stats_kosis_origin_data WHERE created_at >= '<검증시작시각>';
-- 통합 테이블·메타데이터 등 동일 패턴
```

또는 검증용 DB 전체 재생성. 단 운영 환경과 분리된 검증용 DB 인지 반드시 확인 후.

---

## 12. 변경 이력

- v1.5.2: 신규 — 검증용 테스트 서버 setup 가이드 추가 (이슈 #<N>).
