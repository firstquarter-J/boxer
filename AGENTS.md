# Project Summary

- 목적: 근거 기반 운영 질의응답 봇
- 원칙: 질문에 맞는 정보를 먼저 조회(DB/S3/API)하고, 그 결과를 근거로 답변
- 우선순위: 추측보다 조회 결과, LLM fallback보다 라우터 우선
- 상세 설계와 외부 설명의 기준 문서는 `README.md`

# Project Direction

- 설치 단위 기준 핵심 패키지는 `boxer`, `boxer_adapter_slack`, `boxer_company`, `boxer_company_adapter_slack` 네 개다
- `boxer_company`는 `boxer`를 import해서 확장한다
- `boxer_company_adapter_slack`는 `boxer_adapter_slack`, `boxer_company`를 import해서 조립한다
- 회사 레이어는 공개 패키지를 복사하지 않고 import해서 쓴다
- `boxer_adapter_web`, `widget`은 아직 설치 단위가 아니라 자리만 잡아둔 상태다

# Open Source Boundary Rules

- 새 기능을 넣기 전 먼저 공개 코어(`boxer`, `boxer_adapter_slack`)인지 회사 확장(`boxer_company`, `boxer_company_adapter_slack`)인지 분류한다
- 분류가 애매하면 기본값은 회사 레이어에 둔다
- 공개 레이어에는 회사 용어, 회사 정책, 회사 전용 라우팅, 회사 전용 env key, 회사 전용 기본값을 넣지 않는다
- 공개 레이어는 회사 패키지를 import하지 않는다
- 회사 레이어는 공개 패키지를 import해서 확장하고, 공개 코드를 복사해서 들고 있지 않는다
- 재사용 가능한 조회/문맥/observability/helper만 공개 레이어로 올리고, 도메인 판단과 정책 가드는 adapter/domain 레이어에 둔다
- README, sample, examples는 공개 사용법만 다룬다
- 회사 구조, 회사 운영 메모, 회사 import 관계는 `AGENTS.md`와 `.local/README.md`에서만 관리한다
- `boxer_adapter_web`, `widget`은 placeholder로만 취급하고, 실제 설치 단위로 승격하기 전까지 공개 사용법에 포함하지 않는다
- 공개 레이어를 건드렸으면 최소 `tests/open_source`와 `scripts/verify_open_core_boundary.sh`를 기준 검증으로 본다

# Code Map

- `boxer/core`: 설정, LLM 호출, 공통 utils
- `boxer/context`: entries, builder, windowing
  - `ContextEntry` 기본 축: `kind`, `source`, `author_id`, `text`, `created_at`
- `boxer/observability`: request log, audit, sqlite snapshot helper
- `boxer/retrieval/connectors`: 공통 DB/S3/Notion connector
- `boxer/retrieval/synthesis.py`: retrieval synthesis
- `boxer_adapter_slack`: 공개용 Slack 런타임, 공통 이벤트/응답 래퍼, Slack context loader, Slack reference adapter
- `boxer_company`: 회사 도메인 패키지
- `boxer_company_adapter_slack`: 회사 전용 Slack adapter 조립부
- `boxer_adapter_web`: 웹 API / BFF adapter 자리
- `widget`: 브라우저 채팅 UI 자리
- `boxer_company/routers`: 바코드, 영상, 로그, app-user 등 회사 도메인 조회
- `pyproject.toml`: `boxer` open-core 패키지 metadata
- `boxer_adapter_slack/pyproject.toml`: 공개 Slack adapter 패키지 metadata
- `boxer_company/pyproject.toml`: 회사 도메인 패키지 metadata
- `boxer_company_adapter_slack/pyproject.toml`: 회사 Slack adapter 패키지 metadata

# Local Notes

- 이 파일에는 오래 가는 규칙만 둔다
- 자주 바뀌는 운영 메모, 진행 현황, 인프라, 배포, 검증 케이스는 `.local/README.md` 하나를 기준으로 본다
- `.local`의 나머지 파일은 실행 스크립트만 둔다

# Runtime Rules

- 날짜 기준은 `Asia/Seoul`
- `recordings.recordedAt`은 UTC 저장, 사용자 날짜는 KST로 해석 후 UTC 범위로 변환해 조회
- 로그 파일 날짜(`log-YYYY-MM-DD.log`)는 KST 기준
- 바코드가 있으면 `recordings` 기본 컨텍스트는 최대 `30건` 조회
- 조회 가능한 질문은 라우터가 직접 처리하고, 필요하면 evidence를 LLM에 넘겨 문장화
- 근거가 없으면 추측하지 않고 `없음`, `확인 필요`로 답변
- 조회 가능 대상:
  - `recordings`
  - `ultrasound_captures`
  - `hospitals`
  - `hospital_rooms`
- `recording_failure_analysis`는 운영 근거 우선으로 분석하고, 코드 참고 기준과 실제 경로는 `.local/README.md`를 따른다
- 질문에 `구버전 장비` 또는 `legacy`가 있으면 `legacy` 기준을 참고한다
- 날짜 지정 로그 조회는 매핑 장비를 먼저 보고, 필요하면 같은 병원 장비까지 확장 검색
- `recordings` 날짜 기준 row가 `0개`이고 `ffmpeg`/`stalled` 오류가 있으면 `녹화 & 업로드 실패`로 판단
- 다운로드는 장비 파일을 직접 확인하고, 링크는 요청자 DM으로만 보낸다
- 복구 라우터 코드는 남겨두고, 현재 활성 여부와 운영 메모는 `.local/README.md`에 둔다

# Security Rules

- app-user 조회는 PII 포함이므로 허용 사용자만 가능
- DB 쿼리는 read-only만 허용
- 민감 동작은 정책 가드에서 차단 후 안내 문구 응답

# Env Rules

- `.env.example`에는 키만 기록한다
- `.env.example`는 오픈소스 공통 key만 유지한다
- 회사 전용 key는 `.env.company.example` 참고용으로만 두고, 실제 값은 `.env`에만 넣는다
- 실제 값은 로컬 `.env`, EC2 `.env`에만 둔다
- 배포 때 `.env`가 바뀐 경우에만 EC2 `.env`를 동기화한다
- 운영 EC2 `.env`에는 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`를 넣지 않고 인스턴스 역할을 사용한다
- LLM 호출 대상 전환은 `LLM_PROVIDER`가 최우선 스위치다
  - `LLM_PROVIDER=ollama` 여야 Ollama 서버를 호출한다
  - `LLM_PROVIDER=claude` 면 `OLLAMA_BASE_URL`이 맞아도 Ollama를 호출하지 않는다
- 현재 운영 env 값과 서버 인벤토리는 `.local/README.md`에서 관리한다
- MDA GraphQL 인증은 `Bearer` 복붙이 아니라 `adminUser(userPassword)`로 access token을 발급받아 사용한다
- 장비 파일 기능 관련 핵심 키:
  - `MDA_GRAPHQL_URL`
  - `MDA_ADMIN_USER_PASSWORD`
  - `DEVICE_SSH_USER`
  - `DEVICE_SSH_PASSWORD`
  - `DEVICE_FILE_RECOVERY_ENABLED`
- Notion 공통 연동 키:
  - `NOTION_TOKEN`
  - `NOTION_TEST_PAGE_ID`

# Docs Rules

- README에는 공개 패키지와 공개 사용법만 둔다
- README에는 `boxer`, `boxer_adapter_slack` 중심의 오픈소스 설명만 둔다
- README에 회사 패키지명, 회사 설치 절차, 회사 운영 메모는 두지 않는다
- README는 프로젝트 설명과 사용법 중심으로 유지한다
- 구현 현황, 운영 메모, 배포 기록은 README 대신 `.local`에서 관리한다
- 지금 단계에선 `docs/architecture` 같은 별도 아키텍처 문서를 새로 만들지 않는다
- 회사 구조와 import 관계 설명은 `AGENTS.md`, `.local/README.md`에서 관리한다
- 경계 설명은 README와 검증 스크립트로 유지한다

# Commit Rules

- 커밋/푸시/배포는 사용자가 명시적으로 요청했을 때만 수행한다
- 커밋 형식:
  - `type: 이모지 제목`
  - 예: `fix: 🐛 LLM 미응답 즉시 안내`
- 자주 쓰는 타입: `feat`, `add`, `fix`, `refactor`, `docs`, `chore`, `remove`, `style`, `test`

# Deploy Rules

- 순서: `커밋 -> 푸시 -> EC2 반영`
- 앱 서버는 프라이빗 서브넷이라 SSH보다 `SSM(Session Manager)` 기준으로 작업한다
- 로컬 `.env`가 바뀌었거나 EC2 `.env`에 누락된 키가 있으면, 배포 전에 필요한 키만 EC2 `.env`에 동기화한 뒤 재시작한다
- `git pull`, `pip install`은 항상 `ec2-user`로 실행한다
- 이유: `root`로 실행하면 `dubious ownership`가 반복될 수 있다
- 기본 반영 절차:
  - 필요하면 `.env` 동기화
  - `git pull --ff-only origin main`
  - `.venv/bin/pip install -e .`
  - `.venv/bin/pip install -e ./boxer_adapter_slack`
  - `.venv/bin/pip install -e ./boxer_company`
  - `.venv/bin/pip install -e ./boxer_company_adapter_slack`
  - `sudo systemctl restart boxer`
- 현재 서버 정보, 실제 명령, 성공 기준, 로그 명령은 `.local/README.md`에서 관리한다

# DB/Schema Rules

- FK 이름이 `xxxSeq`여도 참조 대상 PK는 대부분 `seq`
- 예:
  - `recordings.deviceSeq -> devices.seq`
  - `recordingSeq -> recordings.seq`

# Log Session Rules

- 녹화 시작: `Scanned : <11자리 바코드>`
- 녹화 종료: `Scanned : C_STOPSESS`
- 특수 종료 토큰: `Scanned : SPECIAL_RECORD_START_STOP`
- 세션 범위: 시작 라인부터 종료 라인 + `20줄`
- 종료 스캔 없이 다음 11자리 바코드 스캔이 나오면 다음 세션 시작으로 간주
- 같은 바코드가 `1초` 이내에 다시 스캔되고 중복 입력으로 판단되면 같은 세션으로 병합
- 다중 세션 로그는 장비 카드 -> 세션 카드 포맷으로 응답
- 장비 파일 검색 경로:
  - `AppData/Videos`
  - `AppData/TrashCan`
- 장비 파일 다운로드 임시 폴더:
  - `/tmp/boxer-device-files`
  - 요청 종료 후 즉시 삭제
  - 오래된 임시 파일도 주기적으로 정리

# Communication

- 답변은 반말, 짧고 핵심만
