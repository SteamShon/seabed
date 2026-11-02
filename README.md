# seabed — M1 기본 단위 골격

설계 문서(Rust 워크스페이스와 포트 설계 초안)의 첫 구현 단위입니다. 표준 라이브러리만 사용합니다.

프로젝트 배경과 결정 사항은 `docs/DECISIONS.md`, 설계 문서 링크는 `docs/LINKS.md`를 보세요.

**중요: 이 코드는 작성한 환경에 Rust 툴체인이 없어 한 번도 컴파일하거나 실행하지 못했습니다.**
문법과 타입은 직접 검토했지만, 첫 `cargo test`에서 컴파일 오류가 나올 수 있습니다.

## 실행

```
cargo test --workspace
```

(Rust 1.70 이상, 외부 크레이트 없음)

## 구성

| 크레이트 | 내용 |
| --- | --- |
| `seabed-domain` | TenantId, NamespaceId, Lsn, Version, Key, ETag, 키 레이아웃(`NamespaceKeys`). 모든 키에 tenant와 namespace 포함 |
| `seabed-ports` | `ObjectStore` 트레이트, 능력 선언(`ObjectStoreCaps`), `PutCondition`, `StoreError` |
| `seabed-adapter-objectstore-mem` | 메모리 ObjectStore, 요청 수 계수 데코레이터(`CountingObjectStore`), 장애 주입 데코레이터(`FaultInjecting`) |
| `seabed-engine` | `Wal`(If-None-Match로 LSN 선점), `ManifestStore`(버전 객체 생성으로 선형 이력) |
| `seabed-conformance` | `run_object_store_suite`: 어떤 ObjectStore 어댑터에도 돌릴 수 있는 공통 테스트 |

## 테스트가 확인하려는 것

- ObjectStore: put/get, range 읽기, list 정렬, 멱등 delete, If-None-Match 생성 경쟁(승자 1명), If-Match CAS 카운터(갱신 손실 없음)
- WAL: 동시 append의 LSN 유일성과 연속성, **응답 유실 후 재시도가 항목을 중복시키지 않음**
- Manifest: 동시 커밋의 선형 이력, 장애 주입 하에서도 각 커밋이 정확히 한 번
- 계수: WAL이 보낸 PUT/LIST 요청 수를 셀 수 있음 (비용 모델 입력)

## 로컬 S3 에뮬레이터 평가에 쓰는 법

`run_object_store_suite`에 에뮬레이터용 어댑터(S3 어댑터는 아직 없음)를 넘겨서, 조건부 쓰기(If-None-Match, If-Match) 테스트를 통과하는 에뮬레이터만 사용합니다. 어댑터가 `caps()`에서 지원하지 않는다고 선언한 기능의 테스트는 건너뜁니다.

## 의도적으로 빠진 것 (설계 문서의 열린 질문과 연결)

- async와 런타임 선택, `Bytes`/Arrow 버퍼 (지금은 동기 트레이트와 `Vec<u8>`)
- 그룹 커밋, writer 펜싱(epoch), 포기한 요청이 나중에 도착하는 경우의 처리
- 지연 주입 (장애 주입은 실패와 응답 유실만 다룸)
- `If-Match` 기반 CURRENT 포인터 방식의 manifest
- 실제 S3 어댑터 (`object_store` 크레이트 사용 여부는 열린 질문 6번)
- 라이선스 (공개 시점에 결정)
