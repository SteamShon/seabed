# seabed 프로젝트 계획

v0.5 · 2026-11-04 · 대화에서 합의한 내용 기준. v0.5는 계획 리뷰 지적 사항을 반영했다(마일스톤과 개발 단계 매핑, M0 판정 규칙, 일관성 비용, 최소 경로).
표기: **[결정]** 사용자가 정함, **[제안]** 문서에 적었으나 미확정, **[가설]** 측정으로 확인해야 함, **[확인 필요]** 조사하지 못함.
같은 내용의 Claude Docs 원본은 [LINKS.md](LINKS.md)에 있습니다. 결정만 모은 목록은 [DECISIONS.md](DECISIONS.md)입니다.

## 1. 배경과 문제

운영 경험에서 온 문제(자세한 표는 README의 Why):

- ES HNSW: 문서가 늘면 메모리 한계, 업데이트 비효율. Vespa: 나으나 큰 메모리 노드가 비쌈.
- 주기적 대량 재색인: 서비스 노드가 인덱싱까지 맡아서, ES 6노드에 200TB 전체 색인이 일주일(시간당 약 1.2TB). 병렬도를 올려도 노드를 늘려야만 빨라짐.
- ES 운영: master 노드 한 대 이탈로 클러스터 불안정, force merge OOM으로 노드가 죽고 복귀 못함.
- turbopuffer: 싸고 acceptable하지만 큰 namespace 비용 증가, 비오픈소스.

**목표:** object storage를 source of truth로 두고 캐시 계층을 얹는 구조의 오픈소스 대안을 억 단위 이상 규모에서 만든다. 1차 적용 후보는 새로 가는 ads DMP 조직의 feature store다.

## 2. 차별점 가설과 시장 위치

**[결정, 사용자 판단]** 차별점은 simple but efficient(비용과 성능 모두)한 분산 시스템이다. 기존 시스템은 기존 primitive 위에 올려서 기존 기능을 그대로 쓸 수 있다는 장점이 있지만, 구성이 복잡하고 문제가 생기면 운영이 어렵다.

시장 조사(2026-10, 웹 문서 기준, 성능 수치는 대부분 벤더 자체 주장이며 재현하지 않음)에서 확인한 것:

- 기본 기술은 수렴 중: HNSW → IVF 클러스터 + 강한 양자화(ES DiskBBQ, VectorChord, Milvus와 Lance의 RaBitQ, Chroma와 turbopuffer의 SPANN/SPFresh), 쓰기 경로는 오브젝트 스토리지 WAL(Milvus Woodpecker, Chroma wal3, turbopuffer). **이것만으로는 차별점이 아니다.**
- 오픈소스 분산은 구성이 무거움: Milvus(Kubernetes, etcd, 오브젝트 스토리지, WAL 계층, 여러 노드 역할), Chroma 분산(Kubernetes/Helm, Postgres), LanceDB 대규모 분산은 Enterprise, VectorChord는 단일 노드 Postgres, Loams는 TiKV 사용.
- 1억 건 이하는 VectorChord, Qdrant 같은 단순하고 싼 대안이 있음(벤더 주장).

**[가설, 미검증]** (a) 구성이 단순한 오픈소스 분산이 비어 있다, (b) 서비스 노드와 분리된 대량 재색인과 비용·시간 견적이 실질적 차이를 만든다, (c) 모든 selectivity 구간에서 예측 가능한 필터 recall을 낼 수 있다. 다른 시스템에 없다고 **확인한 것이 아니다.**

**우리의 약점:** 성숙도와 생태계, 풀텍스트·집계·보안의 풍부함(ES, OpenSearch), 혼자 개발하는 규모.

**대상 구간:** 억 단위 이상 코퍼스, 필터 포함 검색, 주기적 대량 갱신이 잦고, 서비스 노드 증설 없이 비용을 통제하려는 팀. 에이전트별 소규모 namespace(Weaviate, turbopuffer 등이 이미 맞춤)나 1천만 이하는 목표가 아니다.

**[미정]** "이거면 쓴다"는 최소 조건(전체 갱신 시간, 비용, 필터 recall 하한의 숫자). M0에서 기준선이 이미 만족하면 새로 만들지 않고 기여나 재사용으로 전환한다.

**[제안] 판정 기준은 M0 측정 전에 고정한다.** 결과를 본 뒤 기준을 정하면 사후 합리화가 되기 쉽다. 최소한 (1) 전체 갱신 시간, (2) 월 비용, (3) 가장 약한 selectivity 구간의 recall@10 세 숫자를 M0 시작 전에 이 문서에 적고, 이후 바꾸면 이유를 남긴다.

**[결정, 잠정] M0 판정 기준** (2026-11-04, 사용자가 초안대로 채택). 숫자는 이 문서에 이미 있는 목표에서 끌어왔고, 근거가 약한 것은 표시했다. M0 측정 전까지는 바꿀 수 있고, 측정을 시작한 뒤에 바꾸면 이유를 남긴다.

| 기준 | 초안 | 근거 |
| --- | --- | --- |
| (1) 전체 갱신 시간 | 1억 item × 20벡터(20억 벡터, 768차원) 전체 재빌드가 **24시간 이내**. 그동안 서비스 쿼리 p99 증가 **10% 이내** | ES 사례 200TB/일주일. bulk 목표 "10배 이상"을 일 단위로 환산. 벡터 기준 환산은 근거 약함 |
| (2) 월 비용 | 같은 규모·쿼리량에서 turbopuffer 추정치의 **+30% 이내** | 4장 비용 항목의 ±30% 제안. turbopuffer 가격은 [확인 필요] |
| (3) 필터 recall | 가장 약한 selectivity 구간(0.01% 이하 포함)에서 **recall@10 ≥ 0.90**, p99 ≤ 1초 | 3장 목표 90~95%의 하한 |

판정: 기준선(Chroma, Lance, Milvus, Qdrant, VectorChord) 중 하나라도 (1)~(3)을 모두 만족하면 새로 만들지 않고 기여나 재사용으로 전환한다. seabed는 (1)~(3)을 모두 만족하고, 그중 하나에서 가장 좋은 기준선보다 **2배 이상** 나아야 계속 진행한다(2배는 초안).

## 3. 목표와 비목표

목표:

1. 억 단위 이상 벡터를 낮은 비용으로 저장·검색 (p99 1초 이하, 비용은 turbopuffer와 comparable) [결정]
2. 운영 단순, scale-out 가능 (상태 없는 쿼리 노드 + object storage 중심) [결정]
3. 필터 first class: 모든 검색에 필터, selectivity 10% 남는 경우부터 0.01% 이하, 조건은 equality/IN/range/조합 전부 [결정]
4. 실시간 업데이트, 벡터 변경과 메타데이터 변경 분리 [결정]
5. 벡터 + BM25 하이브리드, 한국어 필수 [결정]
6. 문서당 multi-vector와 group-by [결정: item당 수 개~수십 개, 변동 가능]
7. OLTP 우선, S3의 원본 파일에 대한 OLAP 가능, OLTP에 영향 없는 bulk import/export [결정]
8. 오픈소스 컴포넌트를 적극 활용하고 결과물도 오픈소스 [결정]

비목표(초기): 수십 ms 초저지연, 온프렘 지원(AWS로 시작, 나중에), 범용 SQL 엔진(OLAP는 외부 엔진에 위임), 운영 기능(무중단 업그레이드, 오토스케일, 백업과 복구는 SaaS 단계).

## 4. 요구사항

| 항목 | 내용 |
| --- | --- |
| 대표 쿼리 | user embedding은 offline. 유저 history의 각 item에 대해 ANN(요청당 N개 fan-out). 배치 쿼리 API 필요 |
| 규모 | 억 단위 이상 item, 벡터는 수십억. 예: 1억 item × 20벡터 = 20억 벡터, 768차원 fp32로 원본 약 6TB(가정) → 양자화 필수 |
| 지연 | p99 1초 이하 (cold 쿼리 허용) |
| recall | 필터 쿼리 포함 recall@10 90~95%(turbopuffer와 같은 기준). 난이도 구간별로 측정하고 가장 약한 구간 기준 |
| 비용 | turbopuffer와 comparable. 공개 가격과 계산기로 추정(직접 측정 안 함). "comparable"의 허용 범위는 기준선 계산 후 결정(제안: ±30%). 비용 모델에는 컴퓨트와 저장뿐 아니라 쿼리당 S3 요청 수(최신 manifest 확인, 꼬리 읽기, rerank 읽기)를 포함한다 |
| 일관성 | strong 기본(쓴 직후 보임), 쿼리별 eventual 선택. eventual의 반영 지연은 몇 초~최대 1분(불확실). workload별 freshness 설정 |
| 업데이트 | 실시간. 변경 비율이 다양하므로 엔진이 측정해서 증분 / 부분 재빌드 / 전체 재빌드를 자동 선택(기본 목표 비용 최소화), 예상 시간과 비용을 알려 주고 사용자가 조정 |
| API | turbopuffer와 공통 기능 호환, 고유 기능은 확장 필드 |
| 인프라 | AWS로 시작, 다른 클라우드도 선택적, 온프렘은 나중 |
| 관측성 | OpenTelemetry 스펙 |

## 5. 설계 원칙

1. **SoT와 인덱스 분리 [제안]:** 원본은 object storage의 불변 segment + manifest, 모든 인덱스는 파생. turbopuffer v3가 ANN 주소를 primary key로 쓴 설계에서 겪은 증폭을 처음부터 피한다.
2. **문서는 한 번만 저장:** 벡터는 doc id를 참조하고 본문과 속성을 복제하지 않는다.
3. **안정적인 doc id 기반 속성 인덱스:** 클러스터 리밸런싱이 속성 인덱스를 움직이지 않게 한다.
4. **한 아키텍처로는 부족:** selectivity 구간별, 문서 모델별 전략을 두고 플래너가 비용 기반으로 고른다.
5. **Port & Adapter + DDD [결정]:** 주요 컴포넌트는 포트 뒤에 두고 기본 구현은 오픈소스, 교체 가능. 어댑터는 능력과 비용 모델을 선언하고 포트마다 conformance 테스트.
6. **일관성 [결정]:** 모든 쓰기에 단조 증가 LSN. strong은 최신 manifest 확인 + 미인덱싱 WAL 꼬리 전수 검색 병합, eventual은 캐시된 스냅샷. 벡터와 BM25 모두 "인덱스 + 꼬리 병합".
7. **tenant와 namespace 식별자를 모든 키와 manifest에 처음부터 포함 [제안].**
8. **코디네이터 노드 없음 [제안, 가설]:** 메타데이터는 오브젝트 스토리지의 조건부 쓰기(CAS)로 관리하고, 백그라운드 작업은 서비스 노드와 분리해 메모리 상한을 둔다.
9. **Rust [결정].** hot path는 제네릭으로 인라인하고 포트는 굵은 단위로.

## 6. 아키텍처 (모두 계획)

### 6.1 인덱스와 양자화

- SPANN → SPFresh 계열 클러스터(centroid) 인덱스가 기본. object storage에서는 그래프 기반(DiskANN류)보다 클러스터 단위 읽기가 왕복 횟수 면에서 유리하다(추론).
- UBIS(스트리밍 업데이트 균형)는 참고. 억 단위 샤딩은 클러스터 트리를 파티션 경계로 삼고 consistent hashing으로 캐시 지역성 확보.
- 양자화도 포트: 처음에는 무압축, SQ8, 1비트 계열 2~3개. 어댑터가 코드 포맷과 스캔 커널을 함께 제공. 재순위(rerank)는 별도 포트이며 S3 읽기 횟수가 비용 모델에 들어간다.

### 6.2 필터링

- 단순 pre/post-filter가 아니라 벡터 인덱스와 속성 인덱스가 협력하는 native filtering 지향(turbopuffer 선례).
- 플래너 전략(초안): 매우 selective하면 속성 인덱스로 후보를 구하고 id→vector 조회 후 exact 계산, 중간이면 클러스터별 매칭 수로 후보 클러스터를 고른 뒤 클러스터 안에서 bitmap 필터, 거의 다 통과하면 일반 ANN + post-filter(부족하면 over-fetch). 적응형 탐색: top-k가 찰 때까지 가까운 클러스터부터 확장.
- 연구 후보: α-Hardness(필터 실패로 인한 over-fetch 비율)를 쿼리 시점 플래너 신호로 쓸 수 있는가.

### 6.3 multi-vector 문서 모델

- 전략 A(기본): 벡터 단위 인덱싱 + 쿼리 연산자로 doc 단위 group-by.
- 전략 B(선택): MUVERA의 FDE로 item당 벡터 1개로 줄여 인덱싱하고 원본으로 재순위. FDE 차원이 커서 압축이 필요할 수 있다.
- namespace 단위로 전략 선택.

### 6.4 쓰기와 인제스트

- 쓰기는 WAL(LSN, If-None-Match로 선점) → 비동기 인덱싱. 읽기는 인덱스 + 미인덱싱 꼬리. 쓰기 응답에 LSN 일관성 토큰을 돌려주고 쿼리가 min_lsn을 받게 한다 [제안, Loams 참고].
- **인제스트 정책(namespace별 설정) [제안]:** 가시성 목표, 그룹 커밋 윈도우, 꼬리 상한, 인덱스 빌드 트리거, 우선순위. 프리셋 realtime / balanced / bulk. bulk는 꼬리 검색을 끄고 끝나면 manifest를 교체해 한 번에 공개.
- **경로 자동 선택 [제안]:** 변경 비율을 측정해(배치는 시작 전에 키와 해시 컬럼 스캔, 스트리밍은 이동 평균) 파티션 단위로 증분 / 부분 재빌드 / 전체 재빌드를 고른다. 견적은 시간과 비용 범위(가장 싼 안, 균형 안, 가장 빠른 안), 마감 시간·비용 상한·최대 워커 수로 조정. 작업은 파티션 단위 멱등 태스크라 실행 중 워커 증감과 재시도가 가능.
- 메타데이터만 바뀌면 속성 인덱스만 갱신하고 벡터 클러스터는 건드리지 않는다. 컴팩션과 빌드는 별도 워커.
- **strong 읽기의 비용 [제안]:** strong은 쿼리마다 최신 manifest를 확인해야 한다. 현재 M1 구현의 `load_latest`는 manifest prefix 전체를 LIST하므로 버전이 쌓일수록 느려지고 요청 비용이 늘어난다. 최신 버전을 가리키는 포인터 객체(`If-Match` 갱신) 또는 주기적 체크포인트와 오래된 버전 정리가 필요하다. 쿼리 노드는 짧은 TTL로 최신 버전을 캐시하고, 쓰기 응답의 LSN 토큰(min_lsn)으로 필요할 때만 강제 확인한다.

### 6.5 저장 포맷 [제안, memvid 참고]

- segment 파일 끝에 footer TOC(섹션 오프셋, 길이, SHA-256, 포맷 버전). 끝부분을 range GET으로 읽어 TOC를 얻고 필요한 섹션만 읽는다. 같은 입력이면 같은 바이트(결정성).
- 구현(M1): `seabed_engine::segment`. 섹션 이름순 배치, TOC 항목마다 SHA-256, footer에 TOC SHA-256·포맷 버전·magic. 크기는 manifest가 기록한다는 전제로 호출자가 넘긴다(HEAD 요청 없음). 끝 4KB를 한 번에 읽어 TOC가 들어 있으면 GET 1회로 열린다.
- manifest에 `IndexBinding`(임베딩 모델 id, 차원, 거리 지표, 양자화, 토크나이저 id와 사전 버전)을 기록하고 불일치하면 즉시 실패.
- 구현(M1): manifest payload는 `seabed_engine::manifest_content::ManifestContent`(JSON, 필드 순서 고정이라 결정적). tenant·namespace, `indexed_through`(이 LSN까지는 segment에 있고 그 뒤가 미인덱싱 꼬리), `IndexBinding`(처음 쓴 값으로 고정, 다른 값이면 어느 필드가 다른지와 함께 실패), segment 목록(id, 크기, TOC SHA-256)을 담는다. `SegmentStore::open_ref`는 기록된 TOC 해시와 실제 객체를 비교한다.

### 6.6 텍스트(BM25)

- Tantivy + lindera(ko-dic) 어댑터 후보 [확인 필요: 사전 라이선스]. 토크나이저는 포트이고 인덱스와 쿼리 시점이 일치해야 한다.
- 열린 문제: segment와 미인덱싱 꼬리에 걸친 코퍼스 통계(IDF, 평균 문서 길이) 일관성.

### 6.7 OLAP

자체 조인 엔진을 만들지 않는다. SoT를 표준 컬럼너 포맷과 스냅샷 메타데이터로 노출해 외부 엔진이 읽게 한다.

구현(M1): `seabed_engine::sot`. segment마다 `segments/<id>/docs.parquet`를 둔다. 이 파일은 seabed 컨테이너(`.sbsg`) 안이 아니라 독립된 Parquet 객체라서 DuckDB 같은 외부 엔진이 그대로 읽는다. 컬럼은 `id`, `lsn`, `deleted`(tombstone), `vectors`(list<fixed_size_list<f32, dims>>), 속성마다 nullable 컬럼(utf8/int64/float64/bool) 하나씩이다. 행은 id 순으로 정렬되고 id가 겹치지 않는다. 같은 Parquet writer 버전이면 같은 행은 같은 바이트로 인코딩된다. writer 버전이 바뀌면 바이트가 달라질 수 있으며, manifest는 실제로 쓴 바이트의 크기와 SHA-256을 기록한다. 파일 메타데이터에 `seabed.sot.format_version`과 `seabed.sot.dims`를 기록하고 읽을 때 확인한다. `lsn`은 Spark 등 외부 엔진 호환을 위해 int64로 저장한다(`i64::MAX` 초과 거부). manifest content v2의 segment는 `docs`(SoT 파일의 크기와 SHA-256, 필수)와 `index`(`.sbsg` 컨테이너, 선택)를 가진다. SoT가 원본이고 인덱스는 파생이라는 5.1 원칙을 그대로 따른다. 미인덱싱 꼬리 병합 읽기는 `Namespace::read_all`이 한다.

**[결정] namespace 단위 속성 스키마 (구현됨).** manifest의 `attr_schema`(이름→타입)가 모든 SoT 파일의 열 타입을 정한다. 스키마는 추가만 허용되고 기존 속성의 타입 변경이나 삭제는 커밋 단계(`check_successor`)에서 거부된다. flush가 지켜야 할 순서는 "행의 속성으로 스키마를 넓히고(최신 manifest와 충돌하면 아무것도 쓰지 않고 실패) → 그 스키마로 SoT를 쓰고 → 커밋"이다. 이 순서는 `seabed_engine::namespace::Namespace::flush`가 구현한다. segment id는 WAL 범위와, 범위 밖에서 바이트를 정하는 입력(속성 스키마, dims, Parquet writer 버전)의 지문으로 정해진다(`w<첫 LSN>-<끝 LSN>-<지문>`). 입력이 같으면 같은 id와 같은 바이트가 되어 `SotStore::write`가 성공으로 받고(재시도, 같은 범위 경합, 응답 유실), 입력이 다르면 id가 달라 서로를 막지 않는다(스키마가 커진 뒤 같은 범위를 다시 flush하거나 롤링 업그레이드). manifest 커밋 id는 커밋 내용의 해시다. 두 writer가 같은 버전에서 출발해 서로 다른 타입으로 넓히면, manifest는 안전하지만(진 쪽은 `CommitConflict` 뒤 다시 읽으면 `AttrSchemaChanged`) 진 쪽이 이미 쓴 SoT 파일은 manifest가 가리키지 않는 고아로 남는다. 같은 범위의 경합은 고아를 남기지 않는다. 다만 두 flush가 같은 manifest에서 출발해 WAL 끝을 서로 다르게 읽으면 범위가 달라 id도 달라지므로, 진 쪽 파일은 여전히 고아가 된다. 고아는 GC 대상이다(아직 없음). encode는 스키마의 모든 열을 nullable로 쓰고, decode는 스키마에 없거나 타입이 다른 열을 거부하되 스키마가 커지기 전에 쓴 파일(열이 적음)은 받아들인다. 그래서 파일마다 열 타입이 달라지는 일은 없다. 다만 오래된 파일은 새 열이 없으므로, 외부 엔진은 glob이 아니라 manifest의 파일 목록을 읽고 열을 이름으로 합쳐야(예: DuckDB `union_by_name`) 스냅샷이 된다.

### 6.8 포트 후보

저장(ObjectStore, Manifest, Wal), 포맷(SegmentFormat, AttributeIndex), 인덱스(VectorIndex, Quantizer, Reranker), 텍스트(TextIndex, Tokenizer), 쓰기 경로(MemTable, IngestPlanner), 실행(QueryPlanner, Cache, CompactionScheduler), 분산(ShardClient, Router), 관측성(Telemetry), 바인딩(IndexBinding). 포트 스케치는 Rust 설계 문서(LINKS.md)에 있다.

## 7. 개발 순서와 마일스톤

순서 [결정]: 1단계 단일 노드 correctness → 2단계 단일 노드 안의 shard와 분산 구조(shard 간 인터페이스는 초기에 고정) → 3단계 실제 분산 노드. M0를 진행하면서 M1의 기본 단위를 병행하고, 방향이 바뀌면 버려도 된다. M1의 WAL과 manifest는 직접 구현한 뒤 SlateDB 어댑터와 같은 conformance로 비교한다.

마일스톤과 개발 단계의 대응 [제안]: 차별점 가설 (a) "구성이 단순한 분산"은 3단계에서만 검증되므로, 각 마일스톤이 어느 단계에 속하는지와 어느 가설을 검증하는지 함께 적는다.

| 마일스톤 | 단계 | 검증하는 가설 | 내용 |
| --- | --- | --- | --- |
| M0 평가 기반 | - | (a)(b)(c) 기준선 | 벤치마크 재사용(VectorDBBench 러너, HCBGen 기반 필터 생성기), recall 하네스, 비용 모델, 시나리오 S1~S10, 기준선 확대 |
| M1 저장 primitive | 1 | - | 불변 segment + manifest + WAL(LSN), 컬럼너 SoT, 검색 가능한 미인덱싱 꼬리, S3 조건부 쓰기 기반 manifest 커밋, 최신 manifest 포인터 |
| M2 벡터 인덱스 | 1 → 2 | - | 클러스터 인덱스 정적 빌드 → 증분 갱신, 양자화 어댑터, Reranker. shard 간 인터페이스를 여기서 고정 |
| M3 속성 인덱스 · 필터 · 텍스트 | 2 | (c) | 속성 인덱스, native filtering과 플래너, BM25/하이브리드(한국어) |
| M4 업데이트 경로 | 2 → 3 | (b) | 벡터/메타 분리, bulk와 증분 이중 경로, 경로 자동 선택과 견적, 컴팩션 워커 분리. 빌드 워커를 별도 프로세스·노드로 띄우는 것이 첫 실제 분산 |
| M4.5 분산 검증 | 3 | (a) | 쿼리 노드 여러 대 + 빌드 워커, consistent hashing 라우팅, S10(노드 종료, 메모리 제한 하의 빌드) 실측 |
| M5 사내 workload | 3 | 전체 재검증 | 배치 쿼리 API, id→vector, multi-vector group-by, FDE 검토, OLAP 노출, 실데이터 재검증 |
| M6 논문 | - | - | M3~M5에서 실제로 새로웠던 결과만 정리 |

**앞 마일스톤에서 미리 정할 것 [제안]:**

- M2 설계 시 필터와의 연결 지점을 인터페이스에 넣는다: 클러스터별 속성 요약(값 분포, bitmap 생성 hook), 클러스터 ↔ doc id 매핑. 이게 없으면 M3의 native filtering에서 클러스터 레이아웃을 다시 설계하게 된다.
- 대표 쿼리가 배치 fan-out이므로 M2의 쿼리 경로부터 여러 쿼리의 클러스터 읽기를 묶는 구조(요청 병합, 공유 캐시)를 전제로 한다. 배치 쿼리 API 자체는 M5.

**최소 검증 경로 [제안]:** 혼자 개발하는 규모를 고려해, 가설 검증에 꼭 필요한 M1 → M2(정적 빌드) → M3(필터 플래너) → M4(bulk 분리) → M4.5를 먼저 끝낸다. BM25/한국어, OLAP 노출, FDE, 증분 클러스터 갱신의 고도화는 그 뒤로 미룰 수 있다.

현재 상태: M1의 일부(ObjectStore 포트와 메모리 어댑터, 장애 주입, WAL, manifest 커밋, conformance)만 코드가 있다. 2026-11-04 기준 `cargo test --workspace`가 컴파일되고 모든 테스트가 통과한다. 최신 manifest 포인터(`If-Match` 전진 + 앞으로 탐색, 읽기 GET 약 3회)가 있다. segment 포맷과 타입이 있는 manifest 내용(segment 목록, `indexed_through`, `IndexBinding`)이 있다. Parquet SoT 파일(쓰기·읽기·검증)과, 이를 manifest에 연결하는 segment 참조(`docs` 필수, `index` 선택)와 namespace 속성 스키마가 있다. `Namespace`가 쓰기(WAL 레코드 v1: upsert/delete 묶음, 쓰기 전에 검증), flush(WAL 꼬리 → segment SoT → manifest 커밋), strong 읽기(모든 segment + `indexed_through` 이후 WAL 꼬리를 LSN이 가장 큰 행으로 병합, tombstone 반영)를 제공한다. 이로써 M1의 기능 범위는 채워졌다. 새 속성은 write가 WAL에 붙이기 전에 스키마를 넓힌 manifest를 먼저 커밋하므로, 타입 충돌은 순차든 경합이든 항상 write에서 거부되고 flush가 막히지 않는다. WAL 레코드는 `float_roundtrip`으로 f32·f64를 비트 그대로 보존한다. 남은 것: 고아 SoT GC, 읽기 때마다 모든 segment와 꼬리를 다 읽는 비용(strong 읽기 1회에 manifest GET 3 + segment마다 GET 1 + WAL LIST 1 + 꼬리 항목마다 GET 1, 테스트로 고정; 인덱스는 M2부터), WAL GC는 읽을 수 있는 모든 manifest의 `indexed_through` 이하만 지울 수 있다.

## 8. 평가(M0) 요약

- **기준선 [결정]:** Chroma, Lance, Milvus, Qdrant, VectorChord. turbopuffer는 계산기 추정.
- **환경 [결정]:** 로컬 우선(S3 호환 에뮬레이터, 조건부 쓰기를 conformance로 통과한 것만 사용). AWS는 지연 측정에 짧게, 예산은 월 몇 달러 수준. 규모는 머신이 버티는 선까지 설정값으로 올리고 직접 측정 못 한 규모는 외삽으로 표시.
- **외삽 결과의 판정 규칙 [제안]:** 가설 (a)(b)는 규모가 커져야 드러나는 성질이라 로컬 측정만으로는 판정할 수 없다. 외삽으로만 얻은 결과는 "판정 보류"로 두고, S6(bulk 간섭)과 S10(운영 단순성)은 중간 규모(예: 1억 벡터)에서 한 번은 AWS로 실측한다. 이 실측의 예산은 별도로 잡는다.
- **시나리오:** S1 필터 없는 ANN, S2 필터 난이도 스윕, S3 배치 쿼리, S4 multi-vector, S5 업데이트 혼합과 freshness, S6 bulk 간섭, S7 규모별 비용, S8 BM25, S9 멀티테넌트(후속), **S10 운영 단순성과 장애 복구**(구성요소 수와 노드 역할 수, 노드 종료 후 복구, 메모리 제한 하의 병합·빌드에서 서비스 노드 생존).
- **지표:** recall@10 분포, p50/p99(cold와 warm), QPS, 비용, freshness, 간섭, BM25 정확도, 견적 대비 실제 오차.
- **bulk 목표 [제안]:** ES 사례(시간당 약 1.2TB) 대비 10배 이상. 단, ES 수치는 원문 문서와 HNSW를 함께 색인한 것이라 벡터 색인과 같은 일이 아니다. 비교 단위는 TB/시간 대신 시간당 처리 벡터 수, 백만 벡터당 비용, 사용한 워커 수로 정의하고, ES 사례는 같은 단위로 환산할 수 있을 때만 비교한다.

## 9. 오픈소스 활용 후보 (라이선스와 현재 상태 확인 필요)

Arrow / Parquet / DataFusion(컬럼너와 OLAP), Rust object_store, SPTAG/SPFresh(참고 구현), Lance 포맷, SlateDB(WAL과 manifest 어댑터 후보), Tantivy + lindera(BM25), Chroma(가장 가까운 선례, 소스 확인 필요), Loams(참고 사례, 메타데이터에 TiKV 사용).

## 10. 열린 연구 질문

1. 필터와 벡터의 상관관계가 강할 때(광고주별 임베딩 군집 등) recall 보장과 selectivity 추정
2. doc id 기반 속성 인덱스와 클러스터 사이의 변환 비용
3. 적응형 클러스터 확장의 S3 왕복 비용과 recall의 균형
4. multi-vector group-by의 over-fetch 최소화, 전략 A와 FDE의 교차 조건
5. 접근 로그 기반 클러스터 재배치와 SPFresh식 리밸런싱의 충돌 여부
6. bulk와 컴팩션의 OLTP 격리 방식
7. α-Hardness를 플래너 신호로 쓰는 방법
8. OR, range, IN 복합 조건의 selectivity와 난이도 추정, 인덱스 설계
9. BM25 코퍼스 통계를 segment와 꼬리에 걸쳐 일관되게 유지하는 방법
10. 한국어 토크나이저 사전 변경 시 재색인 비용
11. 오브젝트 스토리지 CAS만으로 writer 펜싱과 그룹 커밋을 구현할 때 경합과 PUT 비용의 균형, SlateDB와의 차이
12. 변경 비율 추정과 경로 자동 선택의 regret, 견적 오차 감소
13. 코디네이터 없는 구조가 노드 장애와 메모리 압박에서 실제로 더 단순하고 안정적인가 (S10)
14. 사용자가 "쓴다"고 판단할 최소 조건 (미정)

## 11. 확인하지 못한 것과 위험

- VecBench 코드 공개 여부, HCBGen 라이선스와 동작, VectorDBBench의 클라이언트 범위와 라이선스
- Chroma 분산 모드의 오픈소스 범위, Lance의 OSS와 Enterprise 경계, SlateDB의 라이선스와 성숙도, Milvus 등 일부 시스템의 최신 라이선스
- mecab-ko-dic 사전 라이선스, turbopuffer 공식 가격
- Pinecone 등 비공개 SaaS와 범용 DB의 벡터 기능은 조사하지 않음
- 합성 필터와 공개 데이터가 광고 데이터의 분포를 대표하지 않을 수 있음(M5에서 재검증)
- 프로젝트 이름 seabed는 충돌을 확인하지 못함(crates.io, GitHub, 도메인, 상표 직접 확인 필요)
- 라이선스와 SaaS 운영 주체는 공개 시점에 결정
- 범위 위험: 혼자 개발하는데 M1~M5 범위가 넓다. 최소 검증 경로(7장)를 벗어나는 작업은 가설이 확인된 뒤로 미룬다

## 12. 참고 자료

SPANN arXiv 2111.08566 · SPFresh arXiv 2410.14452 · UBIS arXiv 2602.00563 · HNSW arXiv 1603.09320 · MUVERA arXiv 2405.19504 · HCBGen / α-Hardness arXiv 2606.14193 · PathFinder arXiv 2511.00995 · VecBench doi 10.1145/3802125 · VectorDBBench(Zilliz) · turbopuffer 문서(Architecture, native filtering) · memvid MV2 포맷 명세 · Tantivy, lindera · AWS S3 조건부 쓰기 · SlateDB · Loams · Chroma
