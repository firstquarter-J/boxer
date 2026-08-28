from __future__ import annotations

from types import SimpleNamespace

from boxer_company.transport_contracts import (
    HpaChangePollResult,
    HpaChangePollState,
)
from boxer_company_adapter_slack.hpa_change_renderer import (
    _format_hpa_change_poll_messages,
)


def _poll(
    state: HpaChangePollState,
    *,
    result: dict[str, object] | None = None,
    pr_urls: tuple[str, ...] = (),
) -> HpaChangePollResult:
    return HpaChangePollResult(
        task_id="hpa-20260828123456-12345678",
        state=state,
        job=SimpleNamespace(request_text="비밀 요청", attachments=()),
        result=result or {},
        pr_urls=pr_urls,
    )


def test_clarification_renderer_uses_public_codes_and_redacts_raw_values() -> None:
    result = {
        "review": {
            "requesterView": {
                "summaryCode": "adaptation_available",
                "wrongAssumptions": [
                    {
                        "assumption": "Vercel token=top-secret 값을 그대로 옮겨",
                        "explanationCode": "configuration_not_shared",
                    }
                ],
                "requestItems": [
                    {
                        "request": "src/private.ts와 Redis 구성을 복사",
                        "handling": "adapted",
                    }
                ],
            },
            "blocking_questions": [
                {"subject": "<@UOTHER> 발송 범위", "question": "secret=raw"}
            ],
        }
    }

    messages = _format_hpa_change_poll_messages(
        _poll(HpaChangePollState.NEEDS_CLARIFICATION, result=result)
    )
    rendered = "\n".join(messages)

    assert len(messages) == 2
    assert "HPA 제품 방식" in rendered
    assert "질문 1" in rendered
    for leaked in ("top-secret", "src/private.ts", "Redis", "UOTHER"):
        assert leaked not in rendered


def test_pr_renderer_only_exposes_allowlisted_pr_urls() -> None:
    result = {
        "implementation": {
            "appliedResults": [
                {"request": "요청 동작 반영", "status": "applied"}
            ]
        }
    }
    safe = "https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/123"

    message = _format_hpa_change_poll_messages(
        _poll(
            HpaChangePollState.PR_OPENED,
            result=result,
            pr_urls=(safe, "https://evil.example/pull/1"),
        )
    )[0]

    assert safe in message
    assert "evil.example" not in message
    assert "미머지 · 미배포" in message


def test_failed_renderer_never_echoes_raw_api_error() -> None:
    message = _format_hpa_change_poll_messages(
        _poll(
            HpaChangePollState.FAILED,
            result={"error": "github_pat_abcdefghijklmnopqrstuvwxyz"},
        )
    )[0]

    assert "github_pat_" not in message
    assert "자동 재실행하지 않았어" in message
