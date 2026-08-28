# Boxer Company Package

Company-specific domain package for Boxer.

The base install exposes provider-free DTOs and routing contracts for adapters.
DB, S3, MDA/SSH, Redis, Google, and HPA execution dependencies are isolated in
the `runtime` extra and are installed by `boxer_company_api`, not by the Slack
gateway.

This package contains company-only:

- settings
- prompts
- domain routers
- retrieval rules

## 장비 스캐너 ABI 패치

회사 API의 `operations` 경로는 아래 exact Slack 명령만 단일 장비
`node-hid` ABI 패치로 분류한다.

```text
@Boxer MB2-A00037 스캐너 패치
```

- `DEVICE_SCANNER_ABI_PATCH_ENABLED=true`일 때만 실행한다.
- 장비명 두 개, 질문형·부정형·전체 장비 요청은 실행하지 않는다.
- MDA의 exact 장비명, agent/device 연결, `NOSESS`, 녹화·업로드 `false`를
  확인하고 MDA가 제공한 SSH endpoint만 사용한다.
- 패치 자산은 `boxer_company.assets`에 포함하고 고정 SHA-256을 검증한다.
- 빌드는 앱을 켠 채 수행한다. 실제 PM2 정지 직전 MDA ping으로 갱신된
  상태를 다시 확인해 일회성 gate를 연 경우에만 교체한다.
- 같은 request ID, 같은 장비 동시 요청, 장비 내부 중복 실행을 각각 API
  request guard, process-local lock, 원격 `flock`으로 차단한다.
- 실제 Slack 메시지 검증은 수행하지 않고 API/mock 계약으로 확인한다.
