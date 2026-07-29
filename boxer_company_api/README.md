# Boxer Company API

사내 adapter가 공통 회사 질의응답 런타임을 호출할 때 사용하는 내부 전용
FastAPI install unit이다.

브라우저나 widget이 직접 호출하는 공개 API가 아니며, 서버에 등록된 Bearer
caller만 `/internal/v1/assistant/turns`를 호출할 수 있다.
