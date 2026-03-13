# LLM RAG Chatbot

범용 질의응답 제품을 만들기 위한 LLM + Retrieval 기반 Chatbot 엔진입니다.
사용자 질문을 받아 승인된 데이터 소스(DB/API/S3/Notion)를 조회하고, 근거 기반으로 답변합니다.

## 프로젝트 포지셔닝

- 저장소 성격: **개인 프로젝트 기반의 오픈소스 `RAG LLM Bot` 코어**
- 기본 목표: 특정 회사에 종속되지 않는 범용 엔진
- 확장 전략: 회사/도메인 전용 기능은 **별도 Router/Adapter**로 붙이는 구조

## 제품 전략 (Open Core + Domain Adapter)

### 1) Open Core (공개 대상)

- 범용 `RAG LLM Bot` 엔진
- Intent/Slot Router, Policy Guard, Tool Executor, Evidence Merger, Request Logger
- 팀/도메인 무관하게 재사용 가능한 구조

### 2) Domain Adapter (회사 전용)

- 예시: `Mommybox Router`
- 회사 도메인 규칙(바코드/영상/로그), 권한 정책, 내부 데이터 소스 연결
- 코어와 분리해 운영하며 민감 정보/내부 정책은 비공개 유지

## 현재 코드 모듈 경계

- `boxer/adapters/common`
  - 채널 공통 전송 레이어
  - Slack Bolt 초기화, 이벤트 정규화, 공통 reply 래퍼 담당
  - 환경변수 검증은 Slack 필수 토큰만 수행
- `boxer/adapters/company`
  - 회사 전용 채널 핸들러
  - app_mention 정책 가드/권한/도메인 라우팅/응답 정책 담당
  - LLM/DB/S3 환경변수 검증은 이 레이어에서 수행
- `boxer/adapters/factory.py`
  - 환경변수 `ADAPTER_ENTRYPOINT`로 활성 어댑터 선택
  - 기본값: `boxer.adapters.sample.slack:create_app`
- `boxer/adapters/sample`
  - open core smoke test용 최소 Slack adapter
  - 회사 설정 없이 ping/기본 응답만 검증
- `boxer/adapters/slack.py`
  - 기존 경로 호환을 위한 legacy alias
  - 내부적으로 `ADAPTER_ENTRYPOINT`를 따라 어댑터를 재로딩(재귀 방지 포함)
- `boxer/core`
  - 설정, 공통 유틸, LLM 호출, 스레드 컨텍스트 처리
- `boxer/company`
  - 회사 전용 설정/정책/패턴/프롬프트
  - 사용자 권한 ID, 바코드/로그 규칙, 회사용 시스템 프롬프트
  - route별 token budget, 장비 파일 다운로드 설정 같은 회사 env 포함
- `boxer/company/retrieval_rules.py`
  - 회사 전용 retrieval synthesis 규칙
  - route별 프롬프트 규칙과 evidence 축약 담당
- `boxer/routers/common`
  - 제품 공통으로 재사용 가능한 저수준 기능만 포함
  - `db.py`: read-only DB 연결/검증/실행(명령 파싱/문구 제외)
  - `notion.py`: Notion API 호출, page/block 로드, 캐시
  - `s3.py`: S3 client 생성
  - `sqlite_store.py`: 로컬 SQLite 연결, WAL 설정, 스냅샷/S3 백업 helper
  - `request_log.py`: 채널 요청 로그용 SQLite schema/upsert/backup helper
  - `request_log_backup.py`: request log SQLite snapshot S3 백업 job entrypoint
- `boxer/routers/company`
  - 회사 도메인 로직 전용
  - 예: 바코드 영상 개수, app-user 조회, 장비 로그 파싱/분석, S3 요청 파싱
- `boxer/company/notion_playbooks.py`
  - 회사 전용 Notion 플레이북 선택/점수화 규칙
  - Mommybox 운영 문서 탐색, overview/reference 선택 담당

원칙:

- 타인이 이 저장소를 사용할 때 `common`은 그대로 재사용
- `company`는 각 조직 규칙에 맞춰 교체/재구현

## 프로젝트 목표

- 오픈소스 코어를 먼저 단단히 구축
- 회사용 Router를 플러그인처럼 결합해 실제 업무 챗봇으로 운영
- 이후 공개 가능한 범위에서 사례/아키텍처를 공유해 확장 가능한 제품 형태로 발전

## 제품 확장 방향

- Slack은 현재 레퍼런스 채널이며, Web/CLI/사내툴로 채널 확장 가능
- DB/API/S3/Notion 커넥터를 모듈로 분리해 제품형 확장 가능
- 권한 정책(RBAC), 개인정보 마스킹, 요청 로그를 제품 기본 기능으로 내장
- 멀티 워크스페이스/멀티 테넌트 구조로 확장 가능
- 공개 코어 위에 도메인 Router(예: Momybox 전용)를 붙여 서비스별 챗봇으로 전개

## 현재 동작 (이 저장소에서 구현됨)

- Slack 어댑터가 Socket Mode로 동작 (레퍼런스 구현)
- `@Bot ping` 멘션에 스레드로 응답
- `REQUEST_LOG_SQLITE_ENABLED=true`면 Slack 요청 메타데이터용 로컬 SQLite를 시작 시 초기화하고 요청 단위로 저장 가능
- 선택적으로 최신 SQLite snapshot을 S3에 백업하고, 앱 시작 시 최신 snapshot 복구 가능
- S3 백업은 요청 시점이 아니라 `python -m boxer.routers.common.request_log_backup` 같은 주기 job으로 실행 가능
- S3 backup object key는 기본적으로 `request-log/YYYY-MM-DD-request_log.db` 형태로 저장되며, 같은 날 다시 실행하면 같은 객체를 덮어씀
- `REQUEST_LOG_SQLITE_S3_PREFIX` 기본값은 `request-log`이며, 필요하면 다른 prefix로 바꿀 수 있음
- Slack 스레드 맥락을 읽어 LLM 프롬프트에 주입
- LLM 제공자 라우팅 지원 (`ollama`, `claude`)
- 조회형 응답은 서버가 근거를 수집한 뒤, LLM이 근거(JSON) 기반으로 최종 문장화
- app-user API를 통한 바코드 사용자 프로필 조회 지원
- 바코드 프로필 조회 권한을 owner와 Mark로 제한
- 미인가 사용자의 바코드 프로필 요청에는 다음 문구로 응답:
  - `보안 책임자 @DD 의 승인이 필요합니다.`
- DB 조회 명령은 명시적 커맨드 형태만 지원:
  - `db 조회 ...` 또는 `db조회 ...`
- DB 쿼리 실행은 읽기 전용으로 제한 (SELECT/SHOW/DESCRIBE/EXPLAIN/WITH)
- S3 조회 명령은 명시적 커맨드 형태만 지원:
  - `s3 영상 <바코드>`
  - `s3 로그 <장비명> <YYYY-MM-DD>`
- S3 요청 파싱/조회 응답은 회사 도메인 Router(`routers/company`)에서 처리
- 바코드 + 로그 자연어 요청을 부분 지원:
  - 바코드를 `recordings.fullBarcode`로 조회해 `devices.deviceName` 매핑
  - `오늘/어제/YYYY-MM-DD` 날짜 파싱 후 `장비명/log-YYYY-MM-DD.log` 분석
  - 에러/오류 키워드가 있으면 에러 라인 분석
  - 그 외에는 `Scanned:` 이벤트를 `시간: 명령` 타임라인으로 응답
- 바코드 + 영상 개수 자연어 요청을 부분 지원:
  - 바코드를 `recordings.fullBarcode`로 조회해 row 수(`COUNT(*)`) 응답
- 바코드 + 마지막 녹화 날짜 자연어 요청 지원:
  - 바코드를 `recordings.fullBarcode`로 조회해 `MAX(recordedAt)` 응답
- 바코드 + 전체 녹화 날짜 자연어 요청 지원:
  - 바코드를 `recordings.fullBarcode`로 조회해 `recordedAt`의 KST 날짜 목록 응답
- 바코드 + 특정 날짜 녹화 여부 자연어 요청 지원:
  - 바코드 + 날짜(`오늘/어제/그제/내일/YYYY-MM-DD/YYYY.MM.DD/YY.MM.DD/M.D/M/D/M월 D일`)를 `recordings.recordedAt` 범위 조건으로 조회
  - 날짜 해석은 KST(`Asia/Seoul`) 기준, DB 조회는 UTC 범위(`>= start_utc`, `< end_utc`)로 변환

## 아직 구현되지 않음 (중요)

- 자연어 의도 라우팅은 아직 부분 구현 상태 (현재는 바코드 로그 분석 의도만 우선 지원)
- 멀티 소스 조회 오케스트레이션(S3 + Notion + DB) 미완성
- 근거 메타데이터 영구 저장 경로(table/index) 미확정

## 핵심 아키텍처 (플랫폼 독립)

핵심 원칙: `LLM은 판단`, `서버는 정책 검증과 실제 실행`을 담당합니다.

1. Input Normalizer

- 멘션 텍스트, 스레드 맥락, 사용자 식별자, 타임스탬프 정규화

2. Intent/Slot Router (rule + LLM)

- 의도 분류 및 필요한 슬롯 추출
- 예시: `video_count_by_barcode`, `video_dates_by_barcode`

3. Policy Guard (server authority)

- 실행 여부를 최종 결정
- 권한, PII 정책, 허용 템플릿(allowlist) 검증

4. Tool Executor

- 승인된 도구/템플릿만 실행
- 가드레일이 적용된 DB/API/S3/Notion 조회

5. Evidence Merger

- 수집된 근거를 정규화하고 중복 제거

6. Answer Synthesizer

- 근거 기반 최종 답변 생성
- 근거가 부족하면 보강 질문 1개 또는 근거 부족 안내

7. Request Logger

- 실행 추적 정보와 요청 메타데이터 저장

## 권장 자연어 처리 구조 (Hybrid Router)

`모든 자연어를 규칙으로 처리`도 아니고, `라우터 미스 시 LLM이 임의 조회`도 아닙니다.

권장 흐름:

1. Rule Router

- 고빈도/고정형 의도(`바코드 영상 개수`, `바코드 로그 분석`)를 즉시 매핑

2. LLM Intent Parser

- Rule Router 미스 시 LLM이 `intent + slots(JSON)`만 생성
- 이 단계에서 SQL/API/S3를 직접 실행하지 않음

3. Policy Guard

- 사용자 권한, PII 정책, 허용 템플릿(allowlist) 검증

4. Tool Executor

- 검증된 템플릿만 실제 실행(DB/API/S3/Notion)

5. Answer Synthesizer

- 근거 기반으로 응답 생성, 근거 부족 시 보강 질문 1회

핵심 제약:

- LLM은 직접 조회 권한이 없음
- 서버만 실행 권한을 가짐
- 실행 근거 메타데이터를 반드시 남김

## 정책 가드 라우팅을 쓰는 이유 (일반 GPT 답변 대비)

일반 GPT 대화:

- 도구 실행 보장 없이 그럴듯한 텍스트를 생성할 수 있음
- 실제 데이터를 조회했는지 증명하기 어려움

정책 가드 라우팅:

- `LLM 판단 -> 정책 검증 -> 실제 조회 -> 근거 기반 응답`
- 환각 및 허위 조회 주장 감소
- 권한/개인정보(PII) 정책 강제
- 장애 대응 및 감사 추적이 쉬워짐

## 근거 메타데이터 (권장)

응답 단위로 아래 메타데이터를 남겨 추적성을 확보합니다:

```json
{
  "source": "db",
  "intent": "video_count_by_barcode",
  "query_key": "barcode=43032748143",
  "executed_at": "2026-02-27T18:52:10+09:00",
  "row_count": 110
}
```

## 빠른 시작 (Sample Adapter)

1. 가상환경 생성 및 의존성 설치

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. 환경 변수 설정

```bash
cp .env.example .env
```

샘플 어댑터 최소 변수:

- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`
- `SLACK_SIGNING_SECRET`
- `LLM_PROVIDER` (`ollama` 또는 `claude`)

어댑터 선택 변수(선택):

- `ADAPTER_ENTRYPOINT` (기본값 `boxer.adapters.sample.slack:create_app`)
- 예: `boxer.adapters.company.slack:create_app`

LLM 변수:

- Ollama: `OLLAMA_BASE_URL`, `OLLAMA_MODEL`, `OLLAMA_TIMEOUT_SEC`, `OLLAMA_TEMPERATURE`
- Claude: `ANTHROPIC_API_KEY_HUMANSCAPE`, `ANTHROPIC_MODEL`, `ANTHROPIC_TIMEOUT_SEC`, `ANTHROPIC_MAX_TOKENS`
- Retrieval Synthesis: `LLM_SYNTHESIS_ENABLED`, `LLM_SYNTHESIS_MAX_EVIDENCE_CHARS`, `LLM_SYNTHESIS_MASKING_ENABLED`, `RETRIEVAL_SYNTHESIS_SYSTEM_PROMPT`
- Retrieval Synthesis 추가 옵션: `LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT` (기본 `false`, 조회형 합성 시 스레드 문맥 주입 여부)

공통 connector 변수:

- DB 조회: `DB_QUERY_ENABLED=true`, `DB_HOST`, `DB_PORT`, `DB_USERNAME`, `DB_PASSWORD`, `DB_DATABASE`
- DB 제한값: `DB_QUERY_TIMEOUT_SEC`, `DB_QUERY_MAX_ROWS`, `DB_QUERY_MAX_SQL_CHARS`, `DB_QUERY_MAX_RESULT_CHARS`
- S3 조회: `S3_QUERY_ENABLED=true`, `AWS_REGION`, `S3_ULTRASOUND_BUCKET`, `S3_LOG_BUCKET`
- S3 제한값: `S3_QUERY_TIMEOUT_SEC`, `S3_QUERY_MAX_KEYS`, `S3_QUERY_MAX_ITEMS`, `S3_QUERY_MAX_RESULT_CHARS`, `S3_LOG_TAIL_BYTES`, `S3_LOG_TAIL_LINES`
- Notion 조회: `NOTION_TOKEN`, `NOTION_TEST_PAGE_ID`, `NOTION_API_TIMEOUT_SEC`
- Access Key 방식일 때만 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` 사용

회사 어댑터를 쓸 때는 `.env.company.example` 를 참고해서 같은 `.env` 에 필요한 key를 추가해.

3. sample adapter smoke test

```bash
scripts/smoke_sample_adapter.sh
```

4. 실행

```bash
python app.py
```

5. 회사 어댑터 전환

- `ADAPTER_ENTRYPOINT=boxer.adapters.company.slack:create_app`
- `.env.company.example` 의 회사 전용 key를 `.env` 에 추가
- DB/S3/Notion/company policy key는 실제 쓰는 기능만 켜서 설정

## Open Core 경계와 검증

- open core 경계 원칙:
  - `boxer/core`, `boxer/routers/common`, `boxer/adapters/common` 에 회사 고유 키워드와 정책을 넣지 않음
  - 회사 전용 규칙, env, prompt, 문서 선택 로직은 `boxer/company`, `boxer/routers/company`, `boxer/adapters/company` 에만 둠
  - `.env.example` 는 공통 key만 두고, 회사 key는 `.env.company.example` 를 참고해 실제 `.env` 에 추가
- 정적 경계 검사:

```bash
scripts/verify_open_core_boundary.sh
```

- sample adapter smoke test:

```bash
scripts/smoke_sample_adapter.sh
```

검증 기준:

- `boxer/core`, `boxer/routers/common`, `boxer/adapters/common` 에 회사 키워드가 없어야 함
- `.env.example` 는 sample/open core 기준 key만 포함해야 함
- 회사 전용 key는 `.env.company.example`, `boxer/company/settings.py` 기준으로 관리
- sample adapter는 회사 설정 없이 smoke test가 돌아야 함

## EC2 설치/실행 (Private Subnet + Session Manager)

새 EC2에서 SSH 없이 Session Manager로 설치/운영하는 기준 절차입니다.

1. EC2 준비

- 인스턴스 생성 (Amazon Linux 2023 권장)
- Private Subnet에 배치
- IAM Role 연결: `AmazonSSMManagedInstanceCore` 포함
- Session Manager 접속 확인:

```bash
aws ssm start-session --target <instance-id> --region ap-northeast-2
```

2. 서버 패키지 설치 및 코드 배치 (인스턴스 셸에서)

```bash
sudo dnf install -y git python3.11 python3.11-pip
cd /home/ec2-user
git clone https://github.com/firstquarter-J/rag-bot.git
cd rag-bot
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. 환경변수 설정

```bash
cp .env.example .env
vi .env
chmod 600 .env
```

- `.env.example`에는 키만 있고 실제 값은 넣지 않음
- 실제 비밀값은 `.env`(로컬/EC2)에서만 관리
- 공통 env는 `boxer/core/settings.py`, 회사 전용 env는 `boxer/company/settings.py` 기준으로 분리

4. systemd 서비스 등록

```bash
sudo tee /etc/systemd/system/boxer.service > /dev/null <<'EOF'
[Unit]
Description=Boxer Slack Router
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
Group=ec2-user
WorkingDirectory=/home/ec2-user/rag-bot
EnvironmentFile=/home/ec2-user/rag-bot/.env
ExecStart=/home/ec2-user/rag-bot/.venv/bin/python /home/ec2-user/rag-bot/app.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable boxer
sudo systemctl restart boxer
sudo systemctl status boxer --no-pager -l
```

5. 서비스 점검

```bash
sudo systemctl is-active boxer
sudo journalctl -u boxer -f -o short-iso
```

6. request log S3 백업 timer 등록 예시

```bash
sudo tee /etc/systemd/system/boxer-request-log-backup.service > /dev/null <<'EOF'
[Unit]
Description=Boxer Request Log SQLite Backup
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=ec2-user
Group=ec2-user
WorkingDirectory=/home/ec2-user/rag-bot
EnvironmentFile=/home/ec2-user/rag-bot/.env
ExecStart=/home/ec2-user/rag-bot/.venv/bin/python -m boxer.routers.common.request_log_backup
EOF

sudo tee /etc/systemd/system/boxer-request-log-backup.timer > /dev/null <<'EOF'
[Unit]
Description=Run Boxer Request Log Backup Daily

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now boxer-request-log-backup.timer
sudo systemctl list-timers --all | grep boxer-request-log-backup
```

문제 해결 포인트:

- `TypeError: ... str | None` 오류가 나면 Python 3.9로 실행된 상태임
- 반드시 `python3.11` 기반 `.venv`로 재생성 후 서비스 재시작

## Slack 사용 예시

- Ping
  - `@Boxer ping`

- 바코드 프로필 조회 (권한 사용자만)
  - `@Boxer 43032748143`
  - `@Boxer 바코드 43032748143 조회해줘`

- DB 명시적 조회 모드
  - `@Boxer db 조회`
  - `@Boxer db 조회 SELECT NOW() AS now_time`

- S3 명시적 조회 모드
  - `@Boxer s3 영상 43032748143`
  - `@Boxer s3 로그 <device-name> 2026-03-04`

- 바코드 로그 자연어 분석 모드
  - `@Boxer 43032748143 바코드로 오늘 로그 에러 분석해줘`
  - `@Boxer 43032748143 어제 로그에 오류 있었어?`
  - `@Boxer 43032748143 오늘 로그 단순 분석해줘`
  - `@Boxer 43032748143 오늘 로그 스캔 명령 타임라인 보여줘`

- 바코드 영상 개수 자연어 조회 모드
  - `@Boxer 43032748143 영상 몇 개야`
  - `@Boxer 바코드 43032748143 영상 개수 알려줘`

## 운영 로그

실시간 로그 확인:

```bash
sudo journalctl -u boxer -f -o short-iso
```

특정 시간대 로그 확인:

```bash
sudo journalctl -u boxer \
  --since "2026-02-27 09:45:00" \
  --until "2026-02-27 09:53:00" \
  --no-pager \
| grep -E "Received app_mention|Responded with|db query|barcode lookup"
```

## 보안 노트

- 비밀값은 `.env`로 관리하고 커밋하지 않기
- DB 조회 모드에는 읽기 전용 계정 사용
- PII 엔드포인트는 엄격한 권한 정책으로 보호
- 정책이 요구할 때 기본 응답에서 민감 정보 마스킹

## 로드맵

### Phase A - 안정 베이스라인

- [x] Slack 멘션 처리 및 스레드 응답
- [x] 제공자 라우팅 (`ollama` / `claude`)
- [x] 스레드 맥락 주입
- [x] 권한 게이트가 있는 바코드 프로필 조회
- [x] 명시적 읽기 전용 DB 조회 모드

### Phase B - 자연어 조회 라우팅

- [ ] 의도/슬롯 스키마 정의
- [ ] 의도별 정책 가드 규칙 세트
- [ ] 자연어 질문을 안전한 템플릿 조회로 자동 라우팅
- [ ] 슬롯 누락 시 보강 질문 fallback

### Phase C - 멀티 소스 조회

- [ ] 날짜/키워드/id 기반 S3 로그 조회
- [ ] Notion 페이지 검색 및 본문 추출
- [ ] DB/API/S3/Notion 통합 근거 병합
- [ ] 근거 메타데이터 저장 및 조회

### Phase D - 인터페이스와 확장

- [ ] 코어 엔진 모듈화
- [ ] FastAPI/WS 엔드포인트
- [ ] 웹 채팅 연동
- [ ] 성능 및 동시성 튜닝

## 상위 다이어그램

```text
Slack/Web -> Input Normalizer -> Intent/Slot Router -> Policy Guard -> Tool Executor
                                                         |              |- DB
                                                         |              |- API
                                                         |              |- S3
                                                         |              |- Notion
                                                         v
                              Evidence Merger -> Answer Synthesizer -> Response
                                                         \
                                                          -> Request Logger
```

## 구현 페이즈 기록 (History + Progress)

최종 업데이트: `2026-03-04`

### 큰 목차

1. Phase A - 안정 베이스라인
2. Phase B - 자연어 조회 라우팅
3. Phase C - 멀티 소스 조회
4. Phase D - 인터페이스와 확장

### 진행 상태 요약

| Phase | 상태   | 비고                                                |
| ----- | ------ | --------------------------------------------------- |
| A     | 완료   | Slack 레퍼런스 동작 + 권한 게이트 + 명시적 DB 조회  |
| B     | 진행중 | 자연어 질의 자동 라우팅 설계 정리, 실행 로직은 미완 |
| C     | 예정   | S3/Notion/DB 증거 병합 파이프라인 구현 예정         |
| D     | 예정   | 채널 확장(Web/WS/API) 및 성능/동시성 고도화 예정    |

### 상세 기록

#### Phase A - 안정 베이스라인

완료:

- Slack 멘션 처리 및 스레드 응답
- `ollama`/`claude` 제공자 라우팅
- 스레드 맥락 주입
- 바코드 프로필 조회 + 권한 게이트(owner/Mark)
- 미인가 요청 시 `보안 책임자 @DD 의 승인이 필요합니다.` 응답
- 명시적 읽기 전용 DB 조회 모드
- 모듈 경계 1차 분리 (`adapters.common` / `adapters.company` / `core` / `routers.common` / `routers.company`)
- 코어/회사 설정 분리 (`core.settings` / `company.settings`)
- 어댑터 엔트리포인트 선택 지원 (`ADAPTER_ENTRYPOINT`)

진행중:

- 없음

다음:

- 운영 관점 로그 키/메타데이터 표준화

#### Phase B - 자연어 조회 라우팅

완료:

- 정책 가드 기반 구조/흐름 문서화
- 의도/슬롯 기반 라우팅 방향 확정

진행중:

- 자연어 입력에서 안전한 템플릿 조회로 연결하는 라우터 구현
- 슬롯 누락 시 보강 질문 fallback 규칙 구현

다음:

- 의도 스키마 확정 (`video_count_by_barcode`, `video_dates_by_barcode` 우선)
- 의도별 허용 쿼리/툴 allowlist 연결

#### Phase C - 멀티 소스 조회

완료:

- 없음

진행중:

- 없음

다음:

- S3 로그 조회기 구현 (날짜/키워드/id 필터)
- Notion 검색/본문 추출 구현
- DB/API/S3/Notion 통합 근거 병합기 구현
- 근거 메타데이터 저장 경로 확정 및 적용

#### Phase D - 인터페이스와 확장

완료:

- 없음

진행중:

- 없음

다음:

- 코어 엔진 모듈화
- FastAPI/WS 엔드포인트
- 웹 채널 연동
- 성능/동시성 튜닝

### 기록 규칙

- 완료 항목만 `완료`에 기록
- 계획은 `다음`에 기록
- 변경 시 `최종 업데이트` 날짜를 함께 갱신
- 가능하면 PR/커밋 링크를 함께 남겨 추적성 확보
