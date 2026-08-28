# Boxer Company API

사내 adapter가 공통 회사 질의응답 런타임을 호출할 때 사용하는 내부 전용
FastAPI install unit이다.

이 install unit은 `boxer-company[runtime]`을 선택해 DB·S3·MDA/SSH·Redis·
Google·HPA provider 의존성과 실제 회사 도메인 실행을 함께 소유한다.

브라우저나 widget이 직접 호출하는 공개 API가 아니며, 서버에 등록된 Bearer
caller만 아래 내부 경계를 호출할 수 있다.

- assistant turn: `/internal/v1/assistant/turns`
- 자동화 전달: `/internal/v1/automation/deliveries/pull`,
  `/internal/v1/automation/deliveries/ack`
- HPA 요청·조회·전달: `/internal/v1/hpa-change/*`

자동 cycle 실행은 API companion이 소유한다. 외부
`/internal/v1/automation/cycles` endpoint는 제공하지 않는다.
