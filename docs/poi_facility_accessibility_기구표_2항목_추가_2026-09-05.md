# `poi_facility_accessibility` 기구표 항목 2종 추가 적재 (2026-09-05)

앞 문서 `poi_facility_accessibility_백필_완료_2026-09-04.md` 의 후속. 01 v1.4.0 컬럼 추가에 맞춘 08 v1.12.0.

## 배경

기구표 원문 토큰 중 「유도 및 안내 설비」·「장애인사용가능객실」은 정상 어휘지만 담을 컬럼이 없어 버려졌다.
JSONB 한 컬럼에 담는 안은 소비자 API(JPA 엔티티)·CSV/GIS 평탄화·Y/N 제약 측면에서 기존 `bpchar(1)` 패턴보다
불리해 컬럼 2개(`guide_facility_yn`, `accessible_room_yn`)로 갔다.

## 적재 규칙 — 객실은 "없으면 NULL"

| 컬럼 | 항목 있음 | 정상 기구표에 항목 없음 | 더미 응답·미조회 |
|---|---|---|---|
| `guide_facility_yn` | Y | **N** (기존 5개 플래그와 동일) | NULL |
| `accessible_room_yn` | Y | **NULL** | NULL |

객실 항목은 숙박시설에만 해당한다. 청사·어린이집에 `N` 을 기록하면 의무 대상이 아닌 시설을 "미설치"로 왜곡한다.
시설 유형으로 판정하지 않고 항목이 있을 때만 `Y` 로 둔다.

## 백필 결과 (안양 1,649행, 저장된 `eval_info_raw` 재파싱 — 재수집 없음)

| | 건수 |
|---|---|
| `guide_facility_yn` = Y | **37** |
| `guide_facility_yn` = N | 1,577 |
| `guide_facility_yn` NULL (더미 20 + 원문 없음 14 + 기타) | 35 |
| `accessible_room_yn` = Y | **3** — 기숙사 2 · 일반숙박시설 1 |
| `accessible_room_yn` NULL | 1,646 |

> 앞 문서의 "장애인사용가능객실 23건" 은 더미 응답 20건이 포함된 수였다(더미는 8항목을 전부 채운다). 실제는 **3건**이다.
> 더미 응답 20건은 두 컬럼 모두 NULL 로 남았다(교차 0 확인).

재실행 시 변경 0행(멱등). 스크립트 `scripts/backfill_facility_eval_flags.py`, 500건 단위 커밋.

## 확인 못 한 것

- 「유도 및 안내 설비」의 원천 정의(점자블록·음성유도기 등 어떤 설비를 포괄하는지) — 항목명 그대로 적재했다
