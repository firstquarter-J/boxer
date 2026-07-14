# Boxer Adapter Web

Public FastAPI web adapter package for Boxer.

이 패키지는 widget API/WebSocket, admin UI/API, SQLite 대화 저장소, knowledge sync를 제공한다.
widget UI 자체는 실제 서비스가 호스팅하고 이 서버와 교차 origin으로 통신한다.

## Install

```bash
pip install -e .
pip install -e ./boxer_adapter_web
```

## Runtime

필수 env:

- `BOXER_WEB_SECRET_KEY`

주요 선택 env:

- `BOXER_WEB_HOST`
- `BOXER_WEB_PORT`
- `BOXER_WEB_DATA_PATH`
- `BOXER_WEB_KNOWLEDGE_SOURCE=markdown|notion`
- `BOXER_WEB_MARKDOWN_ROOT`
- `BOXER_WEB_CONFIG_PATH`
- `BOXER_WEB_ADMIN_DIST_PATH`
- `BOXER_WEB_ADMIN_COOKIE_SECURE`
- `BOXER_WEB_WIDGET_ALLOWED_ORIGINS`
- `BOXER_WEB_ADMIN_ALLOWED_ORIGINS`
- `BOXER_WEB_WS_RATE_LIMIT_PER_MINUTE`
- `BOXER_WEB_HANDOFF_ON_MISSING_EVIDENCE`
- `BOXER_WEB_HANDOFF_PROMPT_BEFORE_QUEUE`

Markdown FAQ로 시작할 때:

```bash
BOXER_WEB_KNOWLEDGE_SOURCE=markdown
BOXER_WEB_MARKDOWN_ROOT=examples/web_knowledge/markdown
BOXER_WEB_CONFIG_PATH=examples/web_config.sample.json
```

첫 관리자 생성:

```bash
boxer-web-bootstrap-admin --email admin@example.com --password admin1234 --name "Boxer Admin"
```

실행:

```bash
boxer-web
```

기본 경로:

- `/admin/`
- `/api/health`
- `/api/widget/config`
- `/ws/widget`
- `/ws/admin`

`/widget`, `/sdk`, `/demo`는 제공하지 않는다. widget SDK와 UI는 설치 대상 서비스가 호스팅한다.

운영 설정 예시:

```bash
BOXER_WEB_ADMIN_COOKIE_SECURE=true
BOXER_WEB_WIDGET_ALLOWED_ORIGINS=https://www.example.com,https://app.example.com
BOXER_WEB_ADMIN_DIST_PATH=/opt/boxer-web/admin
BOXER_WEB_WS_RATE_LIMIT_PER_MINUTE=120
```

admin POST API는 session cookie만으로 처리하지 않고 `X-Boxer-Csrf-Token` 헤더도 같이 검사한다.
기본 admin SPA는 로그인 응답으로 받은 CSRF cookie를 자동으로 헤더에 실어 보낸다.
admin WebSocket은 별도 허용 origin이 없으면 같은 host에서만 연결되고, widget CORS와 WebSocket은 widget origin 목록을 함께 사용한다.

`BOXER_WEB_CONFIG_PATH` JSON에는 welcome 문구, starter entry, workflow, handoff policy를 함께 둘 수 있다.
샘플 파일은 [`../examples/web_config.sample.json`](../examples/web_config.sample.json)에 있다.

```json
{
  "welcomeTitle": "Support desk",
  "welcomeMessage": "Welcome to the help center",
  "widgetAllowedOrigins": ["https://www.example.com"],
  "handoffPolicy": {
    "onMissingEvidence": true,
    "promptBeforeQueue": false
  },
  "starterEntries": [
    { "key": "account_access", "label": "Account access" }
  ],
  "workflows": {
    "account_access": {
      "label": "Account access",
      "steps": [
        {
          "field": "email",
          "prompt": "Which account email should support check?",
          "validationRegex": "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$",
          "retryPrompt": "Enter a valid email."
        }
      ],
      "completionMessage": "Captured. Support will continue."
    }
  }
}
```

## Public API

- `from boxer_adapter_web import create_web_app`
- console script: `boxer-web`
- console script: `boxer-web-bootstrap-admin`

## Scope

- widget WebSocket session/init/message flow
- active knowledge source sync/search
- retrieval-grounded answer synthesis
- `EN/KO` language preference persistence and localized fallback
- greeting / thanks / goodbye small talk shortcut responses
- workflow starter / validation / branching metadata / action hook metadata
- configurable handoff policy
- admin login / knowledge preview / conversation log / realtime conversation push

이번 alpha는 FAQ + workflow + handoff + conversation log 범위만 다룬다.
multi-tenant는 포함하지 않는다.
