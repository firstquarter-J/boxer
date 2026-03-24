# Boxer: Retrieval-Grounded Assistant Bot

Open-core framework for retrieval-grounded bots with domain-specific adapters.

Boxer는 오픈소스로 재사용 가능한 `Retrieval-Grounded Assistant (RGA)` LLM bot을 목표로 하는 프로젝트다.
질문을 받으면 승인된 데이터 소스(DB/S3/API/Notion)를 먼저 조회하고, 그 근거를 바탕으로 답변한다.

핵심 원칙:

- open core는 retrieval connector와 synthesis pipeline을 제공한다
- 추측보다 조회 결과
- LLM fallback보다 라우터 우선
- 서버가 실제 조회를 수행하고, LLM은 수집된 근거를 바탕으로만 문장화
- 근거가 없으면 아는 척하지 않고 `없음`, `확인 필요`로 답변
- 어떤 질문을 어떤 source로 라우팅할지는 각 adapter가 결정한다

## 이 저장소가 담고 있는 것

- `boxer/`: 채널 중립 RAG core
- `boxer_adapter_slack/`: 공개 Slack reference adapter
- `boxer_adapter_web/`: 웹 API / BFF 자리
- `widget/`: 브라우저 채팅 UI 자리

핵심 공개 설치 단위는 현재 2개다.

- `boxer`
- `boxer_adapter_slack`

`boxer_adapter_web`, `widget`은 공개 확장 자리만 잡혀 있고 아직 설치 단위는 아니다.

## Monorepo Layout

```text
boxer/
  pyproject.toml
  boxer/
    core/
    context/
    observability/
    retrieval/
      connectors/
  boxer_adapter_slack/
    pyproject.toml
  boxer_adapter_web/
  widget/
  examples/
  tests/
```

경계 원칙:

- open core에는 조직 고유 규칙을 넣지 않는다
- 질문 라우팅, 정책 가드, 권한 규칙은 adapter/domain 패키지에 둔다
- 공개 패키지는 다른 adapter가 `import`해서 확장할 수 있어야 한다

open core 내부 구조:

- `boxer/core`: 설정, LLM, 공통 utils
- `boxer/context`: entries / builder / windowing
- `boxer/observability`: request log / audit / sqlite snapshot helper
- `boxer/retrieval/connectors`: DB/S3/Notion connector
- `boxer/retrieval/synthesis.py`: retrieval evidence masking / serialization / synthesis
- `boxer_adapter_slack/context.py`: Slack thread/history loader

## 환경 파일

- `.env.example`: open core / 공통 key만 기록
- `.env`: 실제 실행 값만 기록

실제 비밀값은 `.env`에만 두고 커밋하지 않는다.
필요하면 `BOXER_DOTENV_PATH`로 다른 env 파일을 지정하거나 `BOXER_SKIP_DOTENV=true`로 dotenv 로딩 자체를 끌 수 있다.
별도 설정이 없으면 retrieval synthesis 기본 응답 언어는 `질문 언어를 따라가고`, request log timezone 기본값은 `UTC`다.

## 빠른 시작

### Sample Slack Adapter

`sample adapter`는 open core 동작 확인용 최소 구현이다.
도메인 전용 규칙 없이 Slack 이벤트 정규화, reply wrapper, request log 흐름만 확인할 수 있다.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
pip install -e ./boxer_adapter_slack
cp .env.example .env
```

최소 환경 변수:

- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`
- `SLACK_SIGNING_SECRET`

선택:

- `ADAPTER_ENTRYPOINT=boxer_adapter_slack.sample:create_app`
- LLM 기능을 실험할 때만 `LLM_PROVIDER`와 provider별 env(`ANTHROPIC_API_KEY`, `OLLAMA_*`) 추가

참고:

- 실제 Slack 런타임 진입점은 `boxer_adapter_slack.runtime:main`

실행:

```bash
scripts/smoke_sample_adapter.sh
boxer-slack
```

`smoke_sample_adapter.sh`는 `BOXER_SKIP_DOTENV=true`로 실행돼서, 로컬 `.env`에 다른 설정이 있어도 샘플 팩토리 확인만 안정적으로 수행한다.

예시:

- `@Bot ping`

## 내 Slack Adapter를 붙이는 방법

새 Slack 도메인을 붙일 때는 open core를 수정하기보다 Slack adapter를 추가하는 쪽을 권장한다.

1. `boxer_adapter_slack.sample`를 시작점으로 삼는다
2. 질문 파싱, 정책 가드, retrieval 라우터를 도메인 모듈에 둔다
3. `ADAPTER_ENTRYPOINT=<your_module>:create_app` 으로 연결한다
4. 공통으로 재사용 가능한 코드는 `boxer/core`, `boxer/context`, `boxer/observability`, `boxer/retrieval`에만 올린다
5. 채널별 대화 문맥 수집은 각 adapter 패키지에서 처리한다

최소 adapter contract는 단순하다.

- `create_app() -> slack_bolt.App`
- 공통 Slack wrapper는 `boxer_adapter_slack.common.create_slack_app()`로 붙인다
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

from boxer_adapter_slack.common import create_slack_app


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
ADAPTER_ENTRYPOINT=examples.custom_adapter.adapters.slack:create_app boxer-slack
```

## 범용 기능 예시

open core에서 바로 쓸 수 있는 범용 기반은 이런 것들이다.

- request log 저장
- context builder / windowing
- read-only DB 실행 helper
- S3 client helper
- Notion page/block 로더
- retrieval evidence masking / serialization / synthesis
- Ollama / Claude provider 라우팅

이 위에 어떤 질문을 어떤 connector로 처리할지는 각 adapter가 정한다.

## Web Modules

현재 저장소에는 `boxer_adapter_web/`, `widget/` 폴더가 함께 존재한다.

- `boxer_adapter_web/`: 추후 Node/TypeScript 기반 웹 adapter 구현 자리
- `widget/`: 추후 Node/TypeScript 기반 브라우저 위젯 구현 자리

지금 단계에서는 폴더와 문서 경계만 잡아두고, 실제 구현은 이후 단계에서 진행한다.

## Packaging And Install

공개 패키지 의존성 기준은 각 설치 단위의 `pyproject.toml`이다.

- 루트 `pyproject.toml`: `boxer`
- `boxer_adapter_slack/pyproject.toml`: `boxer-adapter-slack`

설치 예시:

- open core만 필요할 때:
  ```bash
  pip install -e .
  ```
- 공개 Slack reference adapter까지 필요할 때:
  ```bash
  pip install -e .
  pip install -e ./boxer_adapter_slack
  ```

도메인 전용 adapter나 private package는 이 공개 패키지들 위에 별도로 얹는 구조를 권장한다.

빌드 경계:

- 루트 `pyproject.toml`은 `boxer`만 포함한다
- 공개 Slack adapter는 `boxer_adapter_slack/pyproject.toml`에서 따로 빌드한다

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

- `boxer/core`, `boxer/context`, `boxer/observability`, `boxer/retrieval`에 도메인 고유 규칙을 넣지 않는다
- `boxer/observability`에도 도메인 고유 규칙을 넣지 않는다
- 질문 라우팅과 정책 가드는 adapter 쪽에 둔다
- 채널별 history/thread fetch는 adapter 쪽에 둔다
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
