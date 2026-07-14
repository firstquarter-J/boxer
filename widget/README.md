# Boxer Widget

Public browser widget SDK and service-hosted SPA package for Boxer.

## Deployment Boundary

- 실제 서비스는 npm 패키지의 `dist/sdk`와 `dist/widget`을 설치·호스팅한다.
- widget은 `boxerWebUrl`로 지정한 Boxer Web의 `/api/widget/*`, `/ws/widget`과 통신한다.
- Boxer Web은 widget이나 SDK를 호스팅하지 않는다.
- 같은 UI workspace에서 생성되는 `dist/admin`은 npm 패키지에 포함하지 않고 Boxer Web 배포물에만 넣는다.

## Install And Build

```bash
pnpm --prefix widget install
pnpm --prefix widget build
```

빌드 결과:

- `dist/sdk`: 서비스 코드에서 불러오는 iframe SDK
- `dist/widget`: 서비스가 정적 경로에 호스팅할 widget UI
- `dist/admin`: Boxer Web이 `/admin`에서 제공할 관리자 UI

widget 로컬 미리보기:

```bash
pnpm --prefix widget dev
```

`http://127.0.0.1:4173/?boxerWebUrl=http://127.0.0.1:8000`으로 접속하면 로컬 Boxer Web과 연결된다.

## Public SDK

서비스는 `dist/widget`을 `/boxer-widget/` 같은 정적 경로로 복사한 뒤 SDK를 실행한다.

```ts
import { boot } from "boxer-widget";

const controller = boot({
  widgetUrl: "/boxer-widget/",
  boxerWebUrl: "https://boxer-web.example.com",
  autoOpen: true,
  context: {
    language: "en",
  },
});

controller.identify({ id: "user-1", email: "user@example.com" });
controller.setContext({ language: "ko", tags: ["faq"], metadata: { locale: "ko-KR" } });
controller.open();
```

공개 메서드:

- `boot`
- `open`
- `close`
- `identify`
- `setContext`
- `destroy`

## Runtime Contract

- widget iframe과 정적 파일의 소유자는 widget을 설치한 서비스다.
- Boxer Web URL은 widget 파일의 URL과 별도로 `boxerWebUrl`에 명시한다.
- widget은 browser `localStorage`에 `sessionId`와 `language`를 저장한다.
- 첫 진입 언어는 브라우저 locale을 기준으로 `EN/KO` 중 하나를 선택한다.
- widget HTTP 요청은 관리자 cookie를 전송하지 않는다.
- SDK와 iframe 사이의 `postMessage`는 실제 widget origin으로 제한한다.
