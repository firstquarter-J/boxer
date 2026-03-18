# Boxer: Retrieval-Grounded Assistant Bot

Open-core framework for retrieval-grounded bots with domain-specific adapters.

Boxer는 오픈소스로 재사용 가능한 `Retrieval-Grounded Assistant (RGA)` LLM bot을 목표로 하는 프로젝트다.
질문을 받으면 승인된 데이터 소스(DB/S3/API/Notion)를 먼저 조회하고, 그 근거를 바탕으로 답변한다.

중요한 점:

- open core는 DB/S3/API/Notion helper와 synthesis pipeline을 제공한다
- 어떤 질문을 어떤 source로 라우팅할지는 기본 내장 정책이 아니라 각 adapter가 결정한다

## Who This Is For

- Slack 기반 retrieval bot을 빠르게 만들고 싶은 사람
- LLM이 직접 판단만 하게 두지 않고, 서버가 근거 조회를 통제하길 원하는 사람
- 도메인별 정책 가드와 connector 조합을 직접 설계할 사람
- open core 위에 자기 조직용 adapter를 붙이려는 팀

## Not For

- 설정 없이 바로 쓰는 완성형 SaaS chatbot을 원하는 경우
- 모든 자연어 라우팅이 기본 내장된 제품을 기대하는 경우
- 특정 도메인 규칙과 운영 정책이 이미 완성된 패키지를 찾는 경우
- Slack 말고 다른 채널까지 즉시 turnkey로 붙어 있길 기대하는 경우

핵심 원칙:

- 추측보다 조회 결과
- LLM fallback보다 라우터 우선
- 서버가 실제 조회를 수행하고, LLM은 수집된 근거를 바탕으로만 문장화
- 근거가 없으면 아는 척하지 않고 `없음`, `확인 필요`로 답변

## 이 저장소가 담고 있는 것

- 재사용 가능한 open core
- Slack 기반 sample adapter
- DB/S3/Notion/request log 같은 retrieval 경로
- Ollama / Claude 기반 retrieval synthesis

지향점은 누구나 open core를 가져다 쓰고, 도메인별 adapter만 바꿔서 자기 조직용 bot을 만들 수 있게 하는 거다.
이 저장소 안에는 더 풍부한 reference adapter가 함께 있을 수 있지만, README는 open core 기준으로만 설명한다.

## Open Core 와 Domain-Specific Adapter

### Open Core

- `boxer/core`
- `boxer/adapters/common`
- `boxer/adapters/sample`
- `boxer/routers/common`

공통 설정, Slack 공통 래퍼, LLM 호출, request log, 저수준 DB/S3/Notion helper 같은 재사용 가능한 기반을 둔다.

### Domain-Specific Adapter

- `boxer/adapters/<your_domain>`
- `boxer/routers/<your_domain>`
- 필요하면 `boxer/<your_domain>`

도메인 규칙, 권한 정책, prompt, env, 구체적인 라우터와 connector 조합을 둔다.

이 저장소의 기본 경계 원칙은 단순하다.

- open core에는 조직 고유 규칙을 넣지 않는다
- domain-specific 정책과 도메인 로직은 adapter/domain 모듈에만 둔다

## Core vs Adapter Feature Matrix

| 영역 | Open Core | Domain-Specific Adapter |
| --- | --- | --- |
| Slack 이벤트 정규화 | 제공 | 사용 |
| reply wrapper | 제공 | 사용 |
| request log 저장 | 제공 | 어떤 요청을 어떻게 기록할지 결정 |
| DB/S3/Notion 저수준 helper | 제공 | 어떤 helper를 어떤 질문에 연결할지 결정 |
| 질문 라우팅 | 제공하지 않음 | 구현 |
| 정책 가드 | 제공하지 않음 | 구현 |
| 권한 규칙 | 제공하지 않음 | 구현 |
| prompt / 답변 스타일 | 기본값만 제공 | 필요시 재정의 |
| source 조합 전략 | 제공하지 않음 | 구현 |
| 도메인 명령어 / 자연어 패턴 | 제공하지 않음 | 구현 |

## 요청 처리 흐름

1. Slack 이벤트와 질문을 정규화한다
2. 라우터와 정책 가드가 직접 처리 가능한 질문을 먼저 잡는다
3. 승인된 DB/S3/API/Notion 조회만 서버가 실행한다
4. 수집된 근거를 합치고 필요하면 LLM이 근거 기반으로 최종 문장을 만든다
5. 응답과 요청 메타데이터를 남긴다

즉, `LLM이 임의로 조회를 상상하는 구조`가 아니라 `서버가 먼저 근거를 확보하고 LLM은 그 근거를 바탕으로만 답하는 구조`다.

이때 open core가 기본적으로 하는 일은 `도구를 제공하는 것`이지 `질문을 특정 source로 자동 라우팅하는 것`이 아니다.
DB/S3/API/Notion을 어떤 질문에 노출할지, 어떤 정책 가드로 감쌀지는 adapter가 정한다.

## 짧은 구조도

```text
Input
  -> Adapter Router
  -> Policy Guard
  -> Retrieval Helpers
  -> Evidence + Synthesis
  -> Reply
```

## Why RGA Instead Of Plain Chat Bot

- 서버가 먼저 근거를 조회하므로, LLM이 조회 사실을 상상하는 문제를 줄일 수 있다
- adapter가 정책 가드를 두기 쉬워서 민감 질문, 권한, 허용 source를 강제하기 쉽다
- 어떤 source를 언제 썼는지 request log와 evidence 단위로 추적하기 쉽다
- 단순 채팅보다 운영 질의응답, 내부 도구형 bot, audit 가능한 assistant에 더 잘 맞는다

## 환경 파일

- `.env.example`: open core / 공통 key만 기록
- `.env`: 실제 실행 값만 기록

실제 비밀값은 `.env`에만 두고 커밋하지 않는다.
필요하면 `BOXER_DOTENV_PATH`로 다른 env 파일을 지정하거나 `BOXER_SKIP_DOTENV=true`로 dotenv 로딩 자체를 끌 수 있다.
별도 설정이 없으면 retrieval synthesis 기본 응답 언어는 `질문 언어를 따라가고`, request log timezone 기본값은 `UTC`다.

## 빠른 시작

### Sample Adapter

`sample adapter`는 open core 동작 확인용 최소 구현이다.
도메인 전용 규칙 없이 Slack 이벤트 정규화, reply wrapper, request log 흐름만 확인할 수 있다.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
```

최소 환경 변수:

- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`
- `SLACK_SIGNING_SECRET`

선택:

- `ADAPTER_ENTRYPOINT=boxer.adapters.sample.slack:create_app`
- LLM 기능을 실험할 때만 `LLM_PROVIDER`와 provider별 env(`ANTHROPIC_API_KEY`, `OLLAMA_*`) 추가

실행:

```bash
scripts/smoke_sample_adapter.sh
python app.py
```

`smoke_sample_adapter.sh`는 `BOXER_SKIP_DOTENV=true`로 실행돼서, 로컬 `.env`에 회사용 설정이 있어도 샘플 팩토리 확인만 안정적으로 수행한다.

예시:

- `@Bot ping`

## 내 adapter를 붙이는 방법

새 도메인을 붙일 때는 open core를 수정하기보다 adapter를 추가하는 쪽을 권장한다.

1. `boxer.adapters.sample.slack`를 시작점으로 삼는다
2. 질문 파싱, 정책 가드, retrieval 라우터를 도메인 모듈에 둔다
3. `ADAPTER_ENTRYPOINT=<your_module>:create_app` 으로 연결한다
4. 공통으로 재사용 가능한 코드는 `boxer/core`, `boxer/adapters/common`, `boxer/routers/common`에만 올린다

최소 adapter contract는 단순하다.

- `create_app() -> slack_bolt.App`
- 공통 Slack wrapper는 `boxer.adapters.common.slack.create_slack_app()`로 붙인다
- 엔트리포인트 선택은 `ADAPTER_ENTRYPOINT`가 담당한다

### Custom Adapter Example

실제 예제는 [`examples/custom_adapter/`](examples/custom_adapter/) 에 추가돼 있다.
예를 들면 이런 구조로 시작할 수 있다.

```text
examples/custom_adapter/
  adapters/
    slack.py
  routers/
    faq.py
```

`adapters/slack.py`:

```python
from slack_bolt import App

from boxer.adapters.common.slack import create_slack_app


def create_app() -> App:
    def _handle_mention(payload, reply, _client, _logger) -> None:
        question = payload["question"].strip()
        if question == "ping":
            reply("pong")
            return
        reply("custom adapter is running")

    return create_slack_app(_handle_mention)
```

핵심은 여기서 `DB/S3/API/Notion`을 직접 노출하지 않는다는 점이다.
먼저 adapter가 질문을 분기하고, 필요한 경우에만 자기 라우터가 open core helper를 호출하게 만드는 편이 안전하다.

그 다음 `.env`에는 아래처럼 연결하면 된다.

```bash
ADAPTER_ENTRYPOINT=my_project.adapters.slack:create_app
```

이 저장소에 포함된 예제를 바로 써보려면:

```bash
ADAPTER_ENTRYPOINT=examples.custom_adapter.adapters.slack:create_app python app.py
```

## 범용 기능 예시

open core에서 바로 쓸 수 있는 범용 기반은 이런 것들이다.

- Slack 이벤트 정규화와 reply wrapper
- request log 저장
- read-only DB 실행 helper
- S3 client helper
- Notion page/block 로더
- retrieval evidence masking / serialization / synthesis
- Ollama / Claude provider 라우팅

이 위에 어떤 질문을 어떤 connector로 처리할지는 각 adapter가 정한다.

## Requirements Files

- 기본 설치: `pip install -e .`
- company/reference adapter 확장까지 필요할 때: `pip install -e ".[company]"`
- `requirements-open-core.txt`: requirements 기반 설치가 필요할 때의 open core 목록
- `requirements-company.txt`: company/reference adapter 확장까지 포함한 requirements 목록
- `requirements.txt`: 공개 기본 설치용 alias (`requirements-open-core.txt` 포함)

## 검증 스크립트

```bash
scripts/smoke_sample_adapter.sh
```

- sample adapter smoke test

```bash
scripts/verify_open_core_boundary.sh
```

- open core / domain-specific adapter 경계가 깨지지 않았는지 확인

## Contributing

새 adapter를 추가할 때는 아래 체크리스트를 권장한다.

- `boxer/core`, `boxer/adapters/common`, `boxer/routers/common`에 도메인 고유 규칙을 넣지 않는다
- 질문 라우팅과 정책 가드는 adapter 쪽에 둔다
- connector 호출은 adapter가 명시적으로 선택한다
- DB 조회는 read-only만 유지한다
- 민감 정보가 필요한 질문은 adapter에서 명시적으로 차단하거나 마스킹한다
- 가능하면 sample adapter 또는 `examples/` 예제로 먼저 구조를 검증한다
- open core에 올릴 코드는 다른 도메인에서도 재사용 가능한지 먼저 확인한다

## License

Apache License 2.0을 따른다. 자세한 내용은 [`LICENSE`](LICENSE) 참고.

## 보안 / 운영 원칙

- DB 조회는 read-only만 허용
- 민감 조회는 adapter의 정책 가드에서 차단한다
- request log는 SQLite 기반으로 저장하고, 필요하면 S3 snapshot backup을 붙일 수 있다
- 비밀값은 `.env`에만 두고 예제 파일에는 key만 남긴다
