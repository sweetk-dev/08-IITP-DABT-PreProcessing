# Contributing — 08-IITP-DABT-PreProcessing

> KOSIS(국가통계포털) API 연동 데이터 전처리 모듈

이 문서는 **08-IITP-DABT-PreProcessing** 레포에 기여하는 모든 개발자를 위한 거버넌스 가이드입니다. IITP DABT (정보통신기획평가원 데이터 활용 비즈니스 트랜스포메이션) 국책과제의 일부로 운영되며, **유의적 버전 관리(SemVer)** 표준과 **forward-only** 정책을 따릅니다.

---

## 1. 버전 관리 정책

이 레포는 `vMAJOR.MINOR.PATCH` 형식의 [Semantic Versioning](https://semver.org/lang/ko/) 을 따릅니다.

### 1-1. 버전 단계 의미

| 단계 | 트리거 |
|------|--------|
| **MAJOR** | 외부 인터페이스(API/DB 스키마/CLI) 호환성 깨짐, 아키텍처 대규모 재설계 |
| **MINOR** | 신규 기능/모듈 추가, 기존 기능 확장, 운영 정책의 의미있는 변경 |
| **PATCH** | 버그 수정, 미세 조정, 리팩토링, 문서 변경, 의존성 패치 |

### 1-2. 시작 버전 — `v1.0.0`

신규 레포 또는 작업 시작 시점에 `v1.0.0` 을 부여합니다 (오픈소스 첫 정식 공개 의미). 0.x 라인은 사용하지 않습니다.

### 1-3. Forward-only 원칙

정책 변경 이후에도 이전 commit / PR / 문서의 버전 표기는 **그대로 유지**합니다 (소급 변경 X). 정책 변경 시점부터 새 정책이 적용됩니다.

### 1-4. 전략적 마일스톤

| 마일스톤 | 의미 | 권장 시점 |
|---------|------|-----------|
| **v1.0.0** | 레포 정식 공개 / 1.x 라인 시작 | 작업 시작 시점 |
| **v2.0.0** | 핵심 고도화 / 외부 인터페이스 재설계 | 과제 중반 |
| **v3.0.0+** | 추가 고도화 (선택) | 과제 후반 |

---

## 2. 브랜치 정책

### 2-1. 절대 규칙

- ❌ `main` 브랜치 직접 push 금지 (모든 협업자, Owner 포함)
- ✅ 모든 작업은 작업 브랜치에서 진행 → PR 생성 → Owner 머지

### 2-2. 작업 브랜치 명명 규칙

```
<github_username>/<type>/<issue_no>-<short-desc>
```

| 요소 | 설명 | 예 |
|------|------|-----|
| `github_username` | 작업자 GitHub 계정 | `alice` |
| `type` | 작업 종류 (아래 §2-3) | `feat`, `fix` |
| `issue_no` | GitHub 이슈 번호 | `12` |
| `short-desc` | 영문 소문자, 하이픈 구분, 3~5단어 | `add-new-strategy` |

명명 예시:
```
alice/feat/12-add-momentum-strategy
bob/fix/8-prevent-zero-division
charlie/refactor/15-extract-config-loader
```

### 2-3. 작업 종류 (type)

| type | 용도 | 라벨 |
|------|------|------|
| `feat` | 신규 기능 추가 | `enhancement` |
| `fix` | 버그 수정 | `bug` |
| `refactor` | 코드 정리 (기능 변화 없음) | `refactor` |
| `docs` | 문서 변경 | `documentation` |
| `chore` | 빌드/설정/잡무 | `chore` |
| `hotfix` | 운영 긴급 수정 (사전 승인 필요) | `hotfix` |

---

## 3. 이슈(Issue) 관리

모든 작업은 이슈 등록부터 시작합니다.

- 사소한 기능 개선이나 버그도 반드시 **이슈 탭에 먼저 등록**
- 라벨(`enhancement` / `bug` / `documentation` / `refactor` / `chore` / `hotfix`) 부착
- 이슈 본문에 배경 / 변경 방향 / 작업 범위 / 비고 명시

### 3-1. 이슈 close 규칙

작업 완료 후 PR 본문에 `Closes #<이슈번호>` 를 작성하면 머지 시 이슈가 자동으로 close 됩니다.

---

## 4. PR (Pull Request) 규칙

### 4-1. PR 제목 형식

커밋 메시지 첫 줄과 동일:

```
vX.Y.Z - 변경 요약 한 줄
```

예: `v1.1.0 - KOSIS API retry 로직 추가`

### 4-2. PR 본문 필수 항목

- **이슈 연결**: `Closes #<이슈번호>` (머지 시 자동 close)
- **변경 사항**: 수정된 파일 / 로직 요약
- **테스트 방법**: 검증 절차 (해당 시)

### 4-3. 베이스 / 컴페어 브랜치

- 베이스(merge target): `main`
- 컴페어(source): 작업 브랜치

### 4-4. 머지 권한

이 레포의 **Owner 만 PR 머지 권한**을 가집니다. 협업자는 작업 브랜치 push + PR 생성까지 가능합니다.

---

## 5. 커밋 메시지 형식

커밋 메시지 첫 줄에 **버전을 반드시 포함**합니다.

```
v1.0.0 - 첫 정식 공개: 모듈 X
v1.1.0 - 신규 모듈 Y 추가
v1.1.1 - 모듈 Y 버그 수정
v2.0.0 - DB 스키마 재설계 (Owner 승인)
```

본문은 변경 사유 / 영향 범위를 기술합니다 (선택).

---

## 6. Git Tag & GitHub Releases

| 변경 단계 | git tag | GitHub Release |
|-----------|:------:|:--------------:|
| **patch** | 선택 | 선택 |
| **minor** | ✅ 권장 | ✅ 권장 |
| **major** | ✅ **필수** | ✅ **필수** |

### 6-1. 태깅 명령

```bash
git tag -a v1.1.0 -m "v1.1.0 - 변경 요약"
git push origin v1.1.0
```

### 6-2. Release 노트 권장 형식

```markdown
## 변경 사항
- 신규 기능: ...
- 개선: ...
- 버그 수정: ...

## 해결된 이슈
- Closes #N
- Closes #M
```

해결된 이슈 번호를 **반드시 링크**합니다 (활동성 증빙).

---

## 7. 활동성 운영 권장사항 (참고)

이 프로젝트는 IITP 국책과제이며, GitHub 활동성이 평가에 반영됩니다. 다음을 권장합니다.

- **이슈**: 작업 단위마다 이슈 생성 (목표: 레포당 50개 이상)
- **이슈 close 비율**: PR 본문 `Closes #N` 활용 (목표: 20개 이상 close)
- **라벨 일관성**: §2-3 라벨 표 따름
- **유지 기간**: 레포 생성일로부터 24개월 이상 유지

---

## 8. 문의

이 레포 관련 문의는 GitHub Issues 로 등록하거나, Owner 에게 문의 바랍니다.

---

> 이 문서는 IITP DABT 프로젝트 공통 거버넌스 템플릿(`.claude/templates/CONTRIBUTING.md.template`) 을 기반으로 작성되었습니다. 정책 갱신은 워크스페이스 `.claude/rules/versioning.md`, `.claude/rules/branch-policy.md` 를 기준으로 동기화됩니다.
