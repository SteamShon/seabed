# seabed 진행 상황

마지막 수정: 2026-11-06. 이 문서를 마지막으로 고친 PR은 `git log -1 -- docs/STATUS.md`로, main에 릴리스된 범위는 `git log --first-parent origin/main`으로 봅니다.

마일스톤과 목표는 [PLAN.md](PLAN.md) 7장, 결정은 [DECISIONS.md](DECISIONS.md)에 있습니다. 이 문서는 "지금 무엇이 됐고 무엇이 남았는지"만 적는 스냅샷입니다. 이력은 git log에 맡기고, 끝난 것은 접거나 지웁니다(정리는 릴리스마다, CONTRIBUTING.md "릴리스" 1단계). PR 때문에 이 문서의 어떤 줄이 틀리게 되면 같은 PR에서 고칩니다. 규칙은 [AGENTS.md](../AGENTS.md)의 "진행 상황 기록"에 있습니다.

표기: ✅ 됨 · 🟡 일부 · ⬜ 안 됨

## 요약

| 마일스톤 | 단계 | 상태 | 비고 |
| --- | --- | --- | --- |
| M0 평가 기반 | - | 🟡 판정 기준만 | 코드 없음. 판정 기준은 잠정 결정 |
| M1 저장 primitive | 1 | 🟡 기능 범위 완료 | S3 어댑터는 에뮬레이터(moto)로만 검증. 실제 AWS S3, GC, 프로세스 간 멱등성은 남음 |
| M2 벡터 인덱스 | 1 → 2 | ⬜ | segment 참조에 `index` 자리만 있음 |
| M3 속성 인덱스 · 필터 · 텍스트 | 2 | ⬜ | |
| M4 업데이트 경로 | 2 → 3 | ⬜ | |
| M4.5 분산 검증 | 3 | ⬜ | |
| M5 사내 workload | 3 | ⬜ | |
| M6 논문 | - | ⬜ | |

## M0 평가 기반

- ✅ 판정 기준 초안 고정: 전체 재빌드 24시간 이내(그동안 서비스 p99 증가 10% 이내), 월 비용 turbopuffer +30% 이내, 가장 약한 구간 recall@10 ≥ 0.90(p99 ≤ 1초) (#4, PLAN 2장)
- ⬜ 벤치마크 러너(VectorDBBench 재사용), recall 하네스, 필터 생성기(HCBGen 기반)
- ⬜ 비용 모델(S3 요청 수 포함), turbopuffer 가격 확인
- ⬜ 시나리오 S1~S10
- ⬜ 기준선 측정: Chroma, Lance, Milvus, Qdrant, VectorChord

M0 결과가 "새로 만들지, 기여·재사용할지"를 정하므로, M2 이상을 크게 진행하기 전에 최소 하네스가 필요합니다.

## M1 저장 primitive

| 항목 | 상태 | PR |
| --- | --- | --- |
| ObjectStore 포트, 메모리 어댑터, 장애 주입, 요청 수 계측 | ✅ | 초기, #3 |
| 읽기 재시도 내장(`Retrying`) | ✅ | #3 |
| ObjectStore conformance 스위트 | ✅ 메모리 어댑터, S3 어댑터(moto) | 초기, #28 |
| WAL: LSN 선점(`If-None-Match`), 결과 모름 처리 | ✅ | 초기, #16 |
| WAL: 결과 모름 뒤 재시도 중복 방지 | 🟡 같은 `Wal` 인스턴스 안에서만 | #17, #19 |
| manifest CAS 커밋(`If-None-Match`), 최신 포인터(`If-Match` 전진 + 앞으로 탐색, 읽기 GET 약 3회) | ✅ | 초기, #5, #16, #19 |
| 불변 segment 포맷(footer TOC, SHA-256) | ✅ | #6 |
| 타입이 있는 manifest 내용(segment 목록, `indexed_through`, `IndexBinding`) | ✅ | #8, #10 |
| Parquet SoT, namespace 속성 스키마(추가만) | ✅ | #11, #13 |
| `Namespace` 쓰기 · flush · strong 읽기(WAL 꼬리 병합) | ✅ | #14 |
| 쓰기 경로 재시도 예산 | ✅ | #16 |
| 오류 enum `#[non_exhaustive]`, nightly 장애 주입(시드 1000개) | ✅ #25 릴리스로 기본 브랜치에 들어가 수동·예약 실행 가능. 수동 실행 1회 성공(run 42161199449, 테스트마다 시드 5개, 2026-11-06). 시드 1000개 예약 실행(매일 17:23 UTC)은 아직 관측 전 | #20, #25 |
| S3 어댑터(`object_store` crate)와 S3 호환 에뮬레이터(moto) conformance, 엔진 왕복 테스트, CI `s3` job | ✅ | #28 |
| 실제 AWS S3로 conformance와 엔진 왕복 실행 | ⬜ | |
| SlateDB 어댑터와 conformance 비교 | ⬜ | |
| 고아 SoT GC, WAL GC, 오래된 manifest 정리 · 체크포인트 | ⬜ | |
| 프로세스 간 멱등성 색인(재시작 뒤 재시도 중복 방지) | ⬜ | |
| writer fencing(epoch), group commit | ⬜ | |
| eventual 읽기(캐시 스냅샷), 쓰기 응답 LSN 토큰과 `min_lsn` | ⬜ | |
| 인제스트 정책 프리셋(realtime / balanced / bulk) | ⬜ | |

### M1 동작 메모

PLAN 7장의 "현재 상태" 문단에 있던 동작 설명을 옮기고, 리뷰에서 나온 주의점을 더했습니다.

- `Namespace::write`는 쓰기 전에 묶음을 검증하고, WAL 레코드 v1(upsert/delete 묶음)로 한 항목에 붙인다. strong 읽기는 모든 segment와 `indexed_through` 이후 WAL 꼬리를 id마다 LSN이 가장 큰 행으로 병합하고 tombstone을 반영한다.
- 쓰기 재시도 정책은 `RetryPolicy`이고, `Namespace::with_write_retry_policy`가 WAL, manifest, SoT 세 곳을 한 번에 바꾼다.
- 결과 모름 뒤 재시도 중복 방지의 규칙: 후보는 항목 전체(request id와 payload)가 같을 때만 자기 것으로 본다. 재시도가 성공하면 그 LSN을 기억(Landed)해 다음 재시도에 저장소를 보지 않고 답한다. 같은 request id의 append는 한 번에 하나씩 돌아, 동시 재시도도 한 번만 붙는다.
- 새 속성은 write가 WAL에 붙이기 전에 스키마를 넓힌 manifest를 먼저 커밋한다. 그래서 타입 충돌은 순차든 경합이든 write에서 거부되고 flush가 막히지 않는다. 스키마 커밋이 `SCHEMA_COMMIT_ATTEMPTS`번 모두 지면 `SchemaCommitContention`.
- S3 어댑터: 포트는 동기식이라 어댑터 안의 tokio 런타임에서 `block_on`으로 부른다(async 런타임 안에서 부르면 안 됨). 클라이언트 자체 재시도는 끄고 엔진의 재시도 예산에 맡긴다. S3 etag는 내용 해시라 같은 바이트를 다시 쓰면 etag가 같다(포인터는 앞으로만 움직여 ABA 없음). 키가 없을 때의 `If-Match`는 `PreconditionFailed`로 온다(포트 계약은 `NotFound`나 `PreconditionFailed`).
- `IndexBinding`은 manifest 커밋(`check_successor`)에서만 강제된다. `Namespace`는 아직 binding을 설정하거나 확인하지 않으므로, M2에서 인덱스 빌드와 질의 경로가 생길 때 확인을 연결해야 한다.
- WAL 레코드는 `float_roundtrip`으로 f32·f64를 비트 그대로 보존한다.
- 쓰기 경로(WAL append, manifest 커밋, SoT 쓰기)의 일시 장애는 쓰기 재시도 정책(기본 8회, 20 ms에서 1 s까지 지수 백오프와 jitter)으로 다시 시도하고, 다 쓰면 마지막 `Unavailable`(결과 모름)을 돌려준다. 다른 writer에게 진 경합은 이 예산을 쓰지 않는다.
- manifest는 거절된 생성 뒤 항상 버전을 다시 읽어 commit id를 비교한다. 결과 모름 뒤 같은 commit을 다시 하면 자기 버전을 알아본다. 같은 내용의 동시 commit은 같은 id라 함께 성공한다. 커밋 뒤 포인터 전진은 best effort로 최대 2회.
- 결과 모름 뒤 재시도 중복 방지는 같은 `Wal`(같은 `Namespace`) 안에서만 된다(후보 LSN을 `request_id`와 항목 해시별로 최대 `MAX_PENDING`개 기억, 오래된 것부터 잊음). 다른 프로세스나 재시작 뒤 재시도는 두 번 붙을 수 있다.
- WAL GC는 읽을 수 있는 모든 manifest의 `indexed_through` 이하만 지울 수 있다. 결과 모름 후보 LSN이 지워지면 중복 위험이 생기므로 GC 설계 때 함께 본다. writer fencing을 넣을 때는 WAL의 Landed 기억을 다시 본다.
- 알려진 읽기 비용: strong 읽기 1회에 manifest GET 3 + segment마다 GET 1 + WAL LIST 1 + 꼬리 항목마다 GET 1(테스트로 고정). 인덱스는 M2부터.

## M2 ~ M6

아직 코드가 없습니다. M2 설계 때 미리 정할 것(PLAN 7장):

- 필터와의 연결 지점: 클러스터별 속성 요약, 클러스터 ↔ doc id 매핑
- 배치 fan-out 쿼리를 위한 클러스터 읽기 묶기(요청 병합, 공유 캐시)

## 다음 후보

1. M1: 실제 AWS S3로 S3 어댑터 conformance 실행(자격 증명과 버킷 필요)
2. M0: 최소 하네스(recall, 비용)
3. M2: 정적 빌드 설계 문서 → 구현

### 남은 작은 항목 (리뷰에서 "나중에")

항목마다 (나온 PR, 그 리뷰 코멘트의 날짜 KST)를 붙입니다. 해결한 PR에서 지웁니다. 릴리스 때 문서 정리 PR이 한 릴리스 넘게 남은 항목을 처리합니다(CONTRIBUTING.md "릴리스" 1단계). #17부터 #27 리뷰 2차까지의 모든 라운드를 코드와 대조해 열린 항목을 모았습니다.

- `ManifestError::CommitConflict` 문서의 "다시 commit하면 알 수 있다"는 저장소가 그 키를 내줄 때만 성립 (#19, 2026-11-06)
- 테스트 래퍼 `ScriptedVersions`의 "첫 GET"이 어느 키인지가 호출 순서에 달린다는 점을 주석에 적기 (#19, 2026-11-06)
- `namespace.rs` 문서의 "(32)" 표기와 줄바꿈 (#20, 2026-11-06)
- manifest_content 장애 주입 테스트에서 덮어써지는 `cfg.seed` (#20, 2026-11-06)
- CONTRIBUTING에 `#[non_exhaustive]` 정책과 "새 `StoreError` variant는 engine의 분류 지점 세 곳(manifest, wal, sot)을 확인" 규칙 (#20, 2026-11-06)
- nightly: `workflow_dispatch`의 `ref` 선택, `permissions: contents: read`, `timeout-minutes` (#20, 2026-11-06)
- pr-review 스킬: 리뷰어가 PR 본문 원문(면제 줄)을 직접 읽게 하고, 리뷰 뒤 면제 줄만 바뀌면 그 줄만 다시 확인하는 규칙 (#22, 2026-11-06)
- AGENTS.md 면제 줄 문장 앞에 "검사 경로를 바꾼 PR에서"를 붙여 적용 범위를 분명히 하기 (#22, 2026-11-06)
- `status.yml`에 `concurrency` 설정(본문을 연달아 고칠 때 run이 겹침) (#22, 2026-11-06)
- AGENTS.md의 "문서 정리 PR을 먼저 넣습니다"를 "필요하면 먼저"로 (#23, 2026-11-06)
- STATUS.md 머리말의 "마지막 수정" 날짜는 STATUS를 고치는 PR마다 낡음. 빼고 git log에 맡길지 정하기 (#23, 2026-11-06)
- `status-check-test.sh`가 바뀐 파일 목록 만들기의 실패를 테스트 실패로 치지 않음(`listed && check`) (#22, 2026-11-06)
- `status.yml`에 `shellcheck .github/scripts/*.sh` 단계 추가 (#22, 2026-11-06)
- 정리 PR이 마지막 라운드뿐 아니라 모든 리뷰 라운드의 "나중에"를 대조하게 하기 (#23, 2026-11-06)
- AGENTS.md: 같은 STATUS 절을 고치는 열린 PR이 있으면 "나중에"를 그 PR에 적고 본문에 밝힌다는 예외 문장 (#22, 2026-11-06)
- CONTRIBUTING.md 릴리스 1단계의 "[결정]으로 바꾸거나"를 "사용자에게 확인받아 [결정]으로 바꾸거나, 후보를 PR 설명에 적습니다"로 (#24, 2026-11-06)
- CONTRIBUTING.md의 develop 커밋 찾는 법(`git log --grep`)이 본문 줄에도 걸림. 제목만 보게 하기 (#24, 2026-11-06)
- `within()`의 Disconnected arm에 "catch_unwind가 항상 send하므로 보통 닿지 않음" 주석 (#24, 2026-11-06)
- 저장소 ruleset에 `check`(ci.yml: fmt, clippy, test)를 필수 검사로 추가. 사용자 설정 (#24, 2026-11-06)
- 릴리스 PR 판정에 같은 저장소인지 확인을 더하기(fork의 `develop` 브랜치 이름으로 검사를 건너뛰지 않게 `HEAD_REPO` 비교) (#24, 2026-11-06)
- AGENTS.md "진행 상황 기록"의 릴리스 PR 문장을 문단 끝으로 옮기기 (#24, 2026-11-06)
- workflow의 `actions/checkout@v4`가 Node.js 20 대상이라 경고가 남. 새 버전으로 올리기 (#25 실행 로그, 2026-11-06)
- PR 본문이 AGENTS.md footer(`---` + `_Generated by [Claude Code](https://claude.ai/code)_`) 대신 다른 attribution(`🤖 Generated with…` + 세션 URL)으로 끝나거나, footer 링크가 세션 URL로 바뀌어 있음(#23, #24, #26, #27). 규칙을 실제 관행에 맞출지, 작성 절차를 바꿀지 정하기 (#26, 2026-11-06)
- STATUS의 footer 항목 중 "footer 링크가 세션 URL"은 #27 리뷰 1차 시점 본문에서 본 것(지금 본문은 바뀜). 같은 항목에 커밋 trailer의 모델 이름(`Co-Authored-By: Claude Opus 5.5`, #19~#27 커밋)이 AGENTS.md "커밋" 규칙과 어긋나는 점을 함께 정하기. #27 머지부터는 trailer에서 모델 이름을 뺌 (#27, 2026-11-06)
