from __future__ import annotations

import os
from pathlib import Path
import sys
import time

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)
from boxer_company_api.app import create_company_api_app
from boxer_company_api.settings import (
    CompanyApiCallerSettings,
    CompanyApiSettings,
)


_TOKEN_ENV_KEY = "BOXER_TEST_COMPANY_API_TOKEN"
_PORT_ENV_KEY = "BOXER_TEST_COMPANY_API_PORT"
_PROGRESS_RELEASE_PATH_ENV_KEY = (
    "BOXER_TEST_COMPANY_API_PROGRESS_RELEASE_PATH"
)
_EXPECTED_TENANT_ID = "T-PROCESS"
_EXPECTED_ACTOR_ID = "U-PROCESS"
_EXPECTED_CONVERSATION_ID = "THREAD-PROCESS"
_COMMERCE_QUESTION = "회사 노션에서 커머스 운영 기준 찾아줘"
_SALES_QUESTION = "회사 노션에서 영업 운영 기준 찾아줘"
_PROGRESS_QUESTION = "12345678910 로그 분석해줘"


class _DeterministicCompanyAssistantRuntime:
    """네트워크 왕복만 검증하도록 외부 의존성 없이 고정 결과를 만든다."""

    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult:
        if not self._has_expected_transport_scope(request):
            return CompanyAssistantResult(
                route="process_fixture_scope",
                outcome="denied",
                messages=(
                    AssistantMessage(
                        body="**요청 범위를 확인하지 못했어**",
                    ),
                ),
                fallback_reason="fixture_scope_mismatch",
            )

        if request.question == _COMMERCE_QUESTION:
            return CompanyAssistantResult(
                route="company_notion_qa",
                outcome="answered",
                messages=(
                    AssistantMessage(
                        body=(
                            "**회사 Notion 문서 답변**\n"
                            "- 커머스 운영 기준을 확인했어"
                        ),
                    ),
                ),
                sources=(
                    SourceReference(
                        # API가 검증한 X-Request-ID가 실제 domain request까지
                        # 전달됐는지 부모 프로세스에서 확인할 수 있게 한다.
                        source_id=request.request_id,
                        title="커머스 운영 기준",
                        uri=(
                            "https://app.notion.com/p/"
                            "process-commerce"
                        ),
                    ),
                ),
            )
        if request.question == _SALES_QUESTION:
            return CompanyAssistantResult(
                route="company_notion_qa",
                outcome="answered",
                messages=(
                    AssistantMessage(
                        body=(
                            "**회사 Notion 문서 답변**\n"
                            "- 영업 운영 기준을 확인했어"
                        ),
                    ),
                ),
            )
        return CompanyAssistantResult(
            route="company_notion_search",
            outcome="no_evidence",
            messages=(
                AssistantMessage(
                    body="**회사 Notion 검색**\n- 관련 문서를 찾지 못했어",
                ),
            ),
            fallback_reason="no_search_results",
        )

    def answer_stage(
        self,
        request: CompanyAssistantRequest,
        stage: str,
        *,
        on_partial_result=None,
    ) -> CompanyAssistantResult:
        """실제 HTTP stream에서 partial과 final을 분리해 발행한다."""

        if request.question != _PROGRESS_QUESTION:
            return self.answer(request)
        if (
            stage != "log"
            or not self._has_expected_transport_scope(request)
            or not callable(on_partial_result)
        ):
            raise RuntimeError("fixture_progress_scope_invalid")

        on_partial_result(
            CompanyAssistantResult(
                route="barcode_log_analysis",
                outcome="answered",
                messages=(
                    AssistantMessage(
                        body="로그 근거 수집 완료",
                        mention_actor=False,
                    ),
                ),
            )
        )
        release_path = Path(
            str(
                os.environ.get(_PROGRESS_RELEASE_PATH_ENV_KEY)
                or ""
            ).strip()
        )
        deadline = time.monotonic() + 5
        while not release_path.is_file():
            if time.monotonic() >= deadline:
                raise RuntimeError("fixture_progress_release_timeout")
            time.sleep(0.005)
        return CompanyAssistantResult(
            route="barcode_log_analysis",
            outcome="answered",
            messages=(AssistantMessage(body="로그 분석 완료"),),
        )

    @staticmethod
    def _has_expected_transport_scope(
        request: CompanyAssistantRequest,
    ) -> bool:
        return bool(
            request.tenant_id == _EXPECTED_TENANT_ID
            and request.actor_id == _EXPECTED_ACTOR_ID
            and request.channel == "slack"
            and request.conversation_id == _EXPECTED_CONVERSATION_ID
        )


def create_app():
    """Uvicorn ``--factory``가 호출할 독립 FastAPI app을 만든다."""

    # fixture가 우연히 Slack 조립부를 끌어오면 독립 프로세스 검증 의미가
    # 사라지므로 서버가 열리기 전에 즉시 실패시킨다.
    forbidden_imports = (
        "boxer_adapter_slack",
        "boxer_company_adapter_slack",
        "slack_bolt",
    )
    if any(
        module_name == forbidden
        or module_name.startswith(f"{forbidden}.")
        for module_name in sys.modules
        for forbidden in forbidden_imports
    ):
        raise RuntimeError("fixture_server_import_boundary_failed")

    token = str(os.environ.get(_TOKEN_ENV_KEY) or "").strip()
    raw_port = str(os.environ.get(_PORT_ENV_KEY) or "8010").strip()
    if not token:
        raise RuntimeError("fixture_server_token_missing")
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise RuntimeError("fixture_server_port_invalid") from exc

    settings = CompanyApiSettings(
        host="127.0.0.1",
        port=port,
        callers=(
            CompanyApiCallerSettings(
                caller_id="process-integration",
                token=token,
                tenant_ids=frozenset({_EXPECTED_TENANT_ID}),
                channels=frozenset({"slack"}),
                actor_ids=frozenset({_EXPECTED_ACTOR_ID}),
                allow_anonymous_actor=False,
                capabilities=frozenset({"assistant.turn.read"}),
            ),
        ),
    )
    return create_company_api_app(
        settings=settings,
        assistant_runtime=_DeterministicCompanyAssistantRuntime(),
        readiness_probe=lambda: True,
    )


__all__ = ["create_app"]
