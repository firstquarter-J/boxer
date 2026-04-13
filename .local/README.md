# Local Notes

최근 업데이트: 2026-04-09

이 폴더의 바뀌는 메모는 이 파일 하나로 관리한다.

원칙:

- 공개 문서 설명은 `README.md`
- 오래 가는 규칙은 `AGENTS.md`
- 운영 메모, 현재 상태, 인프라, 배포 절차, 검증 케이스는 이 파일에만 적는다
- `.local`의 나머지 파일은 문서가 아니라 실행 스크립트만 둔다
- 공개 README에는 `boxer`, `boxer_adapter_slack` 중심의 오픈소스 설명만 둔다
- 회사 패키지명, 회사 설치 방식, 회사 운영 내용은 `AGENTS.md`와 이 파일에서만 관리한다

프로젝트 방향:

- 설치 단위 기준 핵심 패키지는 `boxer`, `boxer_adapter_slack`, `boxer_company`, `boxer_company_adapter_slack` 네 개다
- `boxer_company`는 `boxer`를 import해서 확장한다
- `boxer_company_adapter_slack`는 `boxer_adapter_slack`, `boxer_company`를 import해서 조립한다
- 회사 레이어는 공개 패키지를 복사하지 않고 import해서 쓴다
- `boxer_adapter_web`, `widget`은 아직 설치 단위가 아니라 자리만 있다

현재 남겨둔 파일:

- `README.md`: 로컬 메모 단일 기준 문서
- `deploy_ec2.sh`: SSM 기반 배포 헬퍼
- `verify_usage_examples.py`: 사용법 예시 회귀 검증 스크립트

현재 모듈 경계:

- `boxer/`: 채널 중립 RAG core
- `boxer/core`: 설정, LLM, 공통 utils
- `boxer/context`: entries, builder, windowing
  - `ContextEntry` 기본 축: `kind`, `source`, `author_id`, `text`, `created_at`
- `boxer/observability`: request log, audit, sqlite snapshot helper
- `boxer/retrieval/connectors`: 공통 DB/S3/Notion connector
- `boxer/retrieval/synthesis.py`: retrieval synthesis
- `boxer_adapter_slack/`: 공개용 Slack adapter 구현, Slack context loader 포함
- `boxer_company/`: 회사 도메인 패키지
- `boxer_company_adapter_slack/`: 회사 전용 Slack adapter 조립부
- `boxer_adapter_web/`: 웹 adapter 자리, 아직 설치 단위 아님
- `widget/`: 브라우저 위젯 자리, 아직 설치 단위 아님
- `pyproject.toml`: `boxer` open-core 패키지 metadata
- `boxer_adapter_slack/pyproject.toml`: 공개 Slack adapter 패키지 metadata
- `boxer_company/pyproject.toml`: 회사 도메인 패키지 metadata
- `boxer_company_adapter_slack/pyproject.toml`: 회사 Slack adapter 패키지 metadata

## 현재 상태

- 저장소 성격: 설치 단위 4개 기준으로 정리된 `open core + company adapter` 운영 질의응답 봇
- 실제 런타임 중심: 아직 `boxer_company_adapter_slack/company.py` 쪽
- 현재 회사용 Slack entrypoint 권장값: `boxer_company_adapter_slack.company:create_app`
- 기본 채널: Slack `app_mention`
- 핵심 기능:
  - 바코드 기준 `recordings` 조회
  - 바코드/날짜 기준 로그 분석
  - S3 raw 영상/로그 조회
  - 장비 파일 조회와 DM 다운로드 링크 전달
  - 매일 병원 1곳씩 장비 순회 점검 리포트
  - 요청 로그 조회
  - 구조화 DB 조회
  - Notion 기반 문서 질의응답
  - 매주 월요일 오전 09:00 KST 직전 주간 `recordings` 병원별 집계 리포트
  - app-user 조회 권한 가드
  - 팀 자유대화 보조
  - 프롬프트/내부 컨텍스트 조회 시도 차단

최근 검증 메모:

- 사용법 예시는 `.local/verify_usage_examples.py` 기준으로 검증
- 최근 기준: 사용법 예시 `30/30` 비에러 응답 확인
- S3 explicit query 경로 `NameError` 수정 완료
- `.venv/bin/python -m unittest tests.company.test_device_audio_probe tests.company.test_daily_device_round tests.company.test_daily_device_round_reporter tests.company.test_device_update` 기준 `30개 OK`
- 일일 장비 순회 테스트는 mock 기반만 수행했고 실제 MDA 업데이트 호출은 시도하지 않음

현재 주의점:

- 외부 의존성 상태에 따라 LLM, DB, S3, Notion live 검증 가능 여부가 달라진다
- 자유 질문형 예시는 운영 LLM 비가용 시 실패할 수 있다
- 파일 복구 라우터 코드는 남아 있지만 운영 활성 여부는 env로 제어한다

열려 있는 작업:

- README를 프로젝트 설명/사용법 중심으로 계속 슬림하게 유지
- 공통 코어와 채널 adapter의 경계 정리 지속
- 운영 검증 케이스를 이 파일에 누적

운영 자동화 backlog:

- 일일 장비 순회 결과에 장비별 후속 액션 분류 더 붙이기
- 박서 멘션으로 휴지통 비우기 같은 남은 운영 액션 실행하기
- 일일 순회 점검 결과를 MDA activity log나 별도 상태 파일로 누적해서 추세 보기

주간 recordings 리포트 설정:

- `WEEKLY_RECORDINGS_REPORT_ENABLED=true`면 백그라운드 리포터를 켠다
- `WEEKLY_RECORDINGS_REPORT_CHANNEL_ID`에 리포트 보낼 Slack 채널 ID를 넣는다
- 기본 발송 시각은 `매주 월요일 09:00 KST`고 필요하면 `WEEKLY_RECORDINGS_REPORT_HOUR_KST`, `WEEKLY_RECORDINGS_REPORT_MINUTE_KST`로 조정할 수 있다
- 기준 주간은 `월요일 ~ 일요일`이다
- 채널에는 전체 병원 목록 대신 `상위 10개 병원`과 `전주 대비 급증/급감 병원`만 요약해서 보낸다
- 중복 발송 방지 상태는 기본값으로 `data/weekly_recordings_report_state.json`에 저장한다

일일 장비 순회 점검 설정:

- `DAILY_DEVICE_ROUND_ENABLED=true`면 백그라운드 리포터를 켠다
- `DAILY_DEVICE_ROUND_CHANNEL_ID`에 리포트 보낼 Slack 채널 ID를 넣는다
- 기본 발송 시각은 `매일 22:30 KST`고 필요하면 `DAILY_DEVICE_ROUND_HOUR_KST`, `DAILY_DEVICE_ROUND_MINUTE_KST`로 조정할 수 있다
- 기본 poll 간격은 `DAILY_DEVICE_ROUND_POLL_INTERVAL_SEC=30`
- 중복 발송 방지와 다음 병원 순서는 기본값으로 `data/daily_device_round_state.json`에 저장한다
- 병원 선택은 `devices.hospitalSeq` 기준 오름차순 순회고, 대상 장비는 `deviceName`이 있고 `activeFlag=1`, `installFlag=1`인 장비만 본다
- 장비 점검은 기존 `장비 상태 점검`과 `업데이트 상태 확인` 로직을 재사용한다
- 자동 업데이트는 기본 `OFF`고 `DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT`, `DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX`를 각각 따로 켠다
- 권장 활성화 순서:
  먼저 `DAILY_DEVICE_ROUND_ENABLED=true`만 켜고 며칠 관찰
  그다음 `DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT=true`
  마지막에 `DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX=true`
- 자동 업데이트 테스트는 현재 mock 기반만 검증했고, 실제 운영 반영 전에는 채널 리포트만 먼저 확인하는 게 안전하다

최근 반영 기능:

- 장비 소리 출력 점검
  - 예: `MB2-C00419 장비 소리 출력 점검`
  - 장비 SSH 접속 후 재생 장치, 볼륨, 짧은 소리 출력 테스트를 점검해 장비 자체 오디오 출력 경로 정상 여부를 안내
  - 장비가 정상으로 보이면 연결된 스피커 전원, 케이블, 입력 소스 점검 안내까지 함께 응답
- 일일 장비 순회 점검
  - 매일 병원 1곳씩 순회하면서 장비 상태와 업데이트 필요 여부를 Slack에 요약한다
  - 에이전트/박스 자동 업데이트는 env로 별도 제어한다
  - 실제 운영 적용은 `리포트만 관찰 -> agent 자동 업데이트 -> box 자동 업데이트` 순으로 단계 적용한다

## 인프라

앱 서버:

- Instance ID: `i-0cb53572cdab066fa`
- Region: `ap-northeast-2`
- 서비스명: `boxer`
- 경로: `/home/ec2-user/rag-bot`

LLM 서버:

- Instance ID: `i-0ddb8ff589d69885f`
- Region: `ap-northeast-2`
- EC2 Name tag: `boxer-llm`
- Ollama endpoint: `http://10.40.24.47:11434`
- 운영 모델 참고값: `qwen3:30b`
- 자주 `stopped` 상태일 수 있다
- 앱 서버에서 `boxer-ec2` 호스트명은 현재 DNS 기준이 불안정해서 endpoint나 private DNS 기준으로 본다

현재 운영 LLM 기준 env:

- `LLM_PROVIDER=claude`
- `ANTHROPIC_MODEL=claude-sonnet-4-6`
- `ANTHROPIC_MAX_TOKENS=700`
- 로컬 `.env`와 앱 서버 `.env`가 위 값과 어긋나지 않게 유지

LLM 작업 전 확인:

- 상태 확인:
  ```bash
  aws ec2 describe-instances --instance-ids i-0ddb8ff589d69885f --region ap-northeast-2
  ```
- 필요시 시작:
  ```bash
  aws ec2 start-instances --instance-ids i-0ddb8ff589d69885f --region ap-northeast-2
  ```
- 실행 대기:
  ```bash
  aws ec2 wait instance-running --instance-ids i-0ddb8ff589d69885f --region ap-northeast-2
  ```
- Ollama health:
  ```bash
  curl http://10.40.24.47:11434/api/tags
  ```

Reference repo:

- snapshot:
  - `/home/ec2-user/reference-repos/mmb-mommybox-v2/v2.11.300`
  - `/home/ec2-user/reference-repos/mmb-mommybox-v2/legacy`

운영 메모:

- `recording_failure_analysis`는 운영 근거 우선으로 본다
- 코드 비교가 필요하면 위 snapshot을 참고한다
- 앱 서버가 원격 Ollama를 호출하려면 앱 서버 `.env`에 `LLM_PROVIDER=ollama`여야 한다
- 핑크 바코드는 바코드 유효성 검증으로만 차단/허용이 갈리고, 핑크 바코드만 따로 예외 허용하는 설정은 없다
- 바코드 유효성 검증을 해제하면 검증 없이 녹화가 진행된다

## 배포

기본 원칙:

- 순서: `커밋 -> 푸시 -> EC2 반영`
- 앱 서버 접속은 SSH보다 `SSM(Session Manager)` 기준
- `git pull`, `pip install`은 항상 `ec2-user`로 실행
- 로컬 `.env`가 바뀌었거나 EC2 `.env`에 누락된 키가 있으면 필요한 키만 동기화 후 재시작

앱 서버 접속:

```bash
aws ssm start-session --target i-0cb53572cdab066fa --region ap-northeast-2
```

원격 커밋 확인:

```bash
sudo -u ec2-user -H bash -lc 'cd /home/ec2-user/rag-bot && git rev-parse HEAD'
```

최신 반영:

```bash
sudo -u ec2-user -H bash -lc 'cd /home/ec2-user/rag-bot && git pull --ff-only origin main && /home/ec2-user/rag-bot/.venv/bin/pip install -e . && /home/ec2-user/rag-bot/.venv/bin/pip install -e ./boxer_adapter_slack && /home/ec2-user/rag-bot/.venv/bin/pip install -e ./boxer_company && /home/ec2-user/rag-bot/.venv/bin/pip install -e ./boxer_company_adapter_slack'
```

로컬 헬퍼:

```bash
.local/deploy_ec2.sh
```

헬퍼 메모:

- 기본값은 이 파일 기준
- `DEPLOY_SYNC_ENV=true`면 로컬 `.env`도 같이 올리되 `AWS_*` 키는 제외한다
- 로컬 헬퍼는 company runtime editable install 체인을 고정으로 실행한다
- 운영 EC2 `.env`에는 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`를 두지 않고 인스턴스 역할을 사용한다

`.env` 동기화가 필요한 경우 예시:

```bash
sudo -u ec2-user -H bash -lc 'cd /home/ec2-user/rag-bot && grep -v "^NOTION_" .env > .env.next && cat /tmp/notion.env >> .env.next && mv .env.next .env'
```

서비스 재시작/확인:

```bash
sudo systemctl restart boxer
sudo systemctl is-active boxer
sudo journalctl -u boxer -n 20 --no-pager -o short-iso
```

로그 명령:

- 실시간:
  ```bash
  sudo journalctl -u boxer -f -o short-iso
  ```
- 최근 100줄:
  ```bash
  sudo journalctl -u boxer -n 100 --no-pager -o short-iso
  ```

성공 기준:

- 원격 `git rev-parse HEAD`가 방금 푸시한 커밋 SHA와 같다
- `systemctl is-active boxer` 결과가 `active`
- 로그에 `Bolt app is running!`, `Starting to receive messages from a new connection`가 보인다

## 운영 검증 케이스

확인된 케이스:

### 2026-03-06

- 바코드: `16336636442`
- 요청: `16336636442 3월 5일 로그`
- 기대: 세션 도중 마미박스 재시작 감지, 정상 녹화 실패로 판단
- 확인 결과: 정상 응답 확인
- 메모: 세션 중 재시작 로그가 있으면 `마미박스 비정상 종료`로 표시되는 케이스

### 2026-03-06

- 바코드: `48194663047`
- 요청: `48194663047 3월6일 영상 로그`
- 기대: 같은 날짜 2개 세션을 분리해서 표시
- 확인 결과:
  - 세션 1: ffmpeg DTS/PTS 계열 오류로 영상 손상
  - 세션 2: 병원에서 재촬영, 최종 영상 정상
- 메모:
  - 같은 날짜 같은 바코드라도 세션 단위로 반드시 분리해서 봐야 한다
  - `C_STOPSESS`가 있어도 ffmpeg 오류가 있으면 세션별로 실제 영상 정상 여부를 추가 확인해야 한다
  - 현재 규칙상 세션 2도 `영상 손상 가능성 높음`으로 표시될 수 있어 후속 보정 후보다

조사 예정 케이스:

- 바코드: `16336636442`
  - 요청/주제: 세션 중 재부팅 케이스
  - 메모: 이미 확인된 재부팅 케이스. 이후 회귀 테스트 기준으로 유지
- 바코드: `54689555493`
  - 요청/주제: 다른 영상 있는지 문의 케이스
- 바코드: `48473494322`
  - 요청/주제: 다른 영상 있는지 문의 케이스
  - 기준 시각: `2026-02-28 09:08:19`
- 바코드: `39519832257`
  - 요청/주제: `1월 31일 영상 조회`
  - 메모: 종료 바코드 스캔 안 함, 영상 손상
- 바코드: `58141657202`
  - 요청/주제: `2월 24일 캡처보드 오류`
- 바코드: `81183103314`
  - 요청/주제: `2/22 다른 영상 확인 요청`
- 바코드: `33483603437`
  - 요청/주제: 세션 중 마미박스 재부팅, 영상 확인 요청
- 병원/병실: `제이여성병원(서대문) / 4진료실`
  - 요청/주제: 마미박스 반복 재부팅 증상
  - 메모: LED 연결 실패 로그 연관 케이스
- 바코드: `23786835336`
  - 요청/주제: `0219 영상 재생 중도 종료`
  - 메모:
    - 영상 길이 4분 45초로 표시되지만 실제 재생은 약 1분 12초에서 종료
    - 1분 5초 이후 데이터 없음
    - 실제 영상 데이터는 없고, 표시 길이는 단순 녹화 세션 길이 기준 업로드 구버전 케이스

운영 메모:

- Mommybox 프로젝트 경로: `/Users/firstquarter/humanscape/mmb-mommybox-v2`
- Endpoint/JWT/Uploader의 `getaddrinfo EAI_AGAIN`, JWT 갱신 실패, 상태 전송 실패, 업로드 재시도 오류는 그 자체로 녹화 실패 원인으로 보지 않는다

템플릿:

### YYYY-MM-DD

- 바코드 또는 병원/병실:
- 요청:
- 기대:
- 확인 결과:
- 메모:

## 경계 정리 메모

현재 판단:

- 공통 Notion helper는 저수준 API helper 형태로 정리된 상태다
- 공통 retrieval synthesis는 현재 `boxer/retrieval/synthesis.py`에서 generic input builder + provider 호출 구조다
- 회사 전용 규칙은 `boxer_company/*`, `boxer_company/routers/*`, `boxer_company_adapter_slack/*`에 두는 원칙이 맞다
- `.env.example`도 현재는 공통 key 위주라 예전보다 표면이 많이 정리됐다

남은 리스크:

- 같은 저장소에서 customer bot까지 같이 키우면 공통 모듈에 회사 규칙이 다시 스며들 수 있다
- `company` 어댑터가 워낙 크기 때문에 기능 추가 때 경계가 흐려지기 쉽다
- 새 도메인을 붙일 때 공통 모듈 수정이 필요해지는 순간 구조가 다시 무너질 수 있다

실무 기준:

- 공통 재사용 코드는 `boxer/core`, `boxer/context`, `boxer/observability`, `boxer/retrieval`에만 둔다
- request log/audit 계층은 `boxer/observability`에 둔다
- 채널별 history/thread fetch는 adapter 패키지에 둔다
- 회사 정책, 회사 질의 패턴, 회사 응답 포맷은 `company` 영역에만 둔다
- 새 고객용 CS bot은 같은 저장소 확장보다 별도 프로젝트로 시작하고 필요한 공통 패턴만 가져가는 쪽이 안전하다
