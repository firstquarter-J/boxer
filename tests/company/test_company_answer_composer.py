from __future__ import annotations

import logging
import unittest

from boxer.answering import AnswerRequest, AnswerResult
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerComposerDeps,
    CompanyEvidenceAnswerPolicy,
)
from boxer_company.assistant.contracts import (
    CompanyAssistantRequest,
    SourceReference,
)


class _FakeAnswerEngine:
    def __init__(
        self,
        result: AnswerResult | Exception,
        *,
        provider: str = "claude",
    ) -> None:
        self.provider = provider
        self.result = result
        self.requests: list[AnswerRequest] = []

    def answer(self, request: AnswerRequest) -> AnswerResult:
        self.requests.append(request)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _request() -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question="조회 결과를 설명해줘",
        locale="ko",
        context_entries=(
            {
                "kind": "message",
                "source": "slack",
                "author_id": "ACTOR-2",
                "text": "직전 질문 문맥",
            },
        ),
    )


def _policy(**overrides) -> CompanyEvidenceAnswerPolicy:
    values = {
        "route": "recording_lookup",
        "fallback_message": "조회 결과를 직접 확인해줘",
        "fallback_outcome": "no_evidence",
    }
    values.update(overrides)
    return CompanyEvidenceAnswerPolicy(**values)


def _composer(
    engine: _FakeAnswerEngine,
    *,
    synthesis_enabled: bool = True,
    provider_ready: bool = True,
    actor_allowed: bool = True,
) -> CompanyEvidenceAnswerComposer:
    logger = logging.Logger("test.company.answer_composer")
    logger.disabled = True
    return CompanyEvidenceAnswerComposer(
        CompanyEvidenceAnswerComposerDeps(
            answer_engine=engine,  # type: ignore[arg-type]
            synthesis_enabled=synthesis_enabled,
            provider_ready=lambda: provider_ready,
            actor_allowed_for_llm=lambda actor_id: actor_allowed,
        ),
        logger=logger,
    )


class CompanyEvidenceAnswerComposerTests(unittest.TestCase):
    def test_disabled_synthesis_uses_route_fallback_without_engine_call(self) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(text="사용 안 됨", provider="claude", used_llm=True)
        )

        result = _composer(engine, synthesis_enabled=False).compose(
            _request(),
            evidence={"count": 3},
            policy=_policy(),
        )

        self.assertEqual(result.outcome, "no_evidence")
        self.assertEqual(result.messages[0].body, "조회 결과를 직접 확인해줘")
        self.assertEqual(result.fallback_reason, "synthesis_disabled")
        self.assertFalse(result.used_llm)
        self.assertEqual(engine.requests, [])

    def test_unavailable_provider_uses_fallback_before_engine_call(self) -> None:
        for engine, provider_ready in (
            (
                _FakeAnswerEngine(
                    AnswerResult(text="", provider="", used_llm=False),
                    provider="unsupported",
                ),
                True,
            ),
            (
                _FakeAnswerEngine(
                    AnswerResult(text="", provider="claude", used_llm=False)
                ),
                False,
            ),
        ):
            with self.subTest(
                provider=engine.provider,
                provider_ready=provider_ready,
            ):
                result = _composer(
                    engine,
                    provider_ready=provider_ready,
                ).compose(
                    _request(),
                    evidence={},
                    policy=_policy(),
                )

                self.assertEqual(
                    result.fallback_reason,
                    "provider_unavailable",
                )
                self.assertEqual(engine.requests, [])

    def test_claude_actor_denial_uses_fallback_but_ollama_skips_allowlist(self) -> None:
        denied_engine = _FakeAnswerEngine(
            AnswerResult(text="사용 안 됨", provider="claude", used_llm=True)
        )
        denied = _composer(
            denied_engine,
            actor_allowed=False,
        ).compose(
            _request(),
            evidence={},
            policy=_policy(),
        )
        self.assertEqual(
            denied.fallback_reason,
            "actor_not_allowed_for_llm",
        )
        self.assertEqual(denied_engine.requests, [])

        ollama_engine = _FakeAnswerEngine(
            AnswerResult(
                text="로컬 합성 답변",
                provider="ollama",
                used_llm=True,
            ),
            provider="ollama",
        )
        allowed = _composer(
            ollama_engine,
            actor_allowed=False,
        ).compose(
            _request(),
            evidence={},
            policy=_policy(),
        )
        self.assertEqual(allowed.outcome, "answered")
        self.assertTrue(allowed.used_llm)

    def test_success_passes_context_and_answer_options_to_engine(self) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="  근거에 따른 답변  ",
                provider="claude",
                used_llm=True,
            )
        )
        def transform(evidence):
            return evidence
        sources = (
            SourceReference(
                source_id="recording:1",
                title="녹화 1",
                uri="boxer://recordings/1",
            ),
        )

        result = _composer(engine).compose(
            _request(),
            evidence={"recordingCount": 3},
            policy=_policy(
                system_prompt="근거만 사용해",
                extra_rules="숫자를 보존해",
                evidence_transform=transform,
                max_tokens=320,
                timeout_sec=15,
            ),
            sources=sources,
        )

        self.assertEqual(result.route, "recording_lookup")
        self.assertEqual(result.outcome, "answered")
        self.assertEqual(result.messages[0].body, "근거에 따른 답변")
        self.assertEqual(result.sources, sources)
        self.assertTrue(result.used_llm)
        self.assertIsNone(result.fallback_reason)
        self.assertEqual(len(engine.requests), 1)
        answer_request = engine.requests[0]
        self.assertEqual(
            answer_request.context_entries,
            _request().context_entries,
        )
        self.assertEqual(answer_request.evidence, {"recordingCount": 3})
        self.assertEqual(answer_request.system_prompt, "근거만 사용해")
        self.assertEqual(answer_request.extra_rules, "숫자를 보존해")
        self.assertIs(answer_request.evidence_transform, transform)
        self.assertEqual(answer_request.max_tokens, 320)
        self.assertEqual(answer_request.timeout_sec, 15)

    def test_success_normalizes_legacy_single_star_emphasis_to_commonmark(
        self,
    ) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="*조회 결과*\n• 개수: *3개*",
                provider="claude",
                used_llm=True,
            )
        )

        result = _composer(engine).compose(
            _request(),
            evidence={"recordingCount": 3},
            policy=_policy(),
        )

        self.assertEqual(
            result.messages[0].body,
            "**조회 결과**\n• 개수: **3개**",
        )

    def test_timeout_can_choose_route_fallback_or_explicit_message(self) -> None:
        for fallback_on_timeout, expected_message in (
            (True, "조회 결과를 직접 확인해줘"),
            (False, "AI API가 10초 내 응답하지 않았어"),
        ):
            with self.subTest(fallback_on_timeout=fallback_on_timeout):
                engine = _FakeAnswerEngine(
                    AnswerResult(
                        text="",
                        provider="claude",
                        used_llm=False,
                        failure_reason="timeout",
                    )
                )
                result = _composer(engine).compose(
                    _request(),
                    evidence={},
                    policy=_policy(
                        fallback_on_timeout=fallback_on_timeout,
                        timeout_message="AI API가 10초 내 응답하지 않았어",
                    ),
                )

                self.assertEqual(result.messages[0].body, expected_message)
                self.assertEqual(result.fallback_reason, "timeout")
                self.assertFalse(result.used_llm)

    def test_provider_error_and_empty_answer_use_fallback(self) -> None:
        cases = (
            (
                AnswerResult(
                    text="",
                    provider="claude",
                    used_llm=False,
                    failure_reason="provider_error",
                ),
                "provider_error",
            ),
            (
                AnswerResult(
                    text="   ",
                    provider="claude",
                    used_llm=True,
                ),
                "empty_response",
            ),
            (RuntimeError("provider failed"), "provider_error"),
        )
        for engine_result, expected_reason in cases:
            with self.subTest(expected_reason=expected_reason):
                engine = _FakeAnswerEngine(engine_result)
                result = _composer(engine).compose(
                    _request(),
                    evidence={},
                    policy=_policy(),
                )

                self.assertEqual(
                    result.messages[0].body,
                    "조회 결과를 직접 확인해줘",
                )
                self.assertEqual(result.fallback_reason, expected_reason)
                self.assertFalse(result.used_llm)

    def test_answer_validator_rejects_unsafe_answer(self) -> None:
        validated: list[str] = []
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="근거에 없는 다른 바코드를 안내해",
                provider="claude",
                used_llm=True,
            )
        )

        result = _composer(engine).compose(
            _request(),
            evidence={},
            policy=_policy(
                answer_validator=lambda answer: validated.append(answer)
                or "다른 바코드" not in answer
            ),
        )

        self.assertEqual(validated, ["근거에 없는 다른 바코드를 안내해"])
        self.assertEqual(result.fallback_reason, "answer_validation_failed")
        self.assertEqual(result.messages[0].body, "조회 결과를 직접 확인해줘")
        self.assertFalse(result.used_llm)

    def test_global_guard_rejects_new_other_barcode_advice(self) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="안내: 다른 바코드로 다시 해봐",
                provider="claude",
                used_llm=True,
            )
        )

        result = _composer(engine).compose(
            _request(),
            evidence={},
            policy=_policy(),
        )

        self.assertEqual(
            result.fallback_reason,
            "answer_validation_failed",
        )
        self.assertEqual(
            result.messages[0].body,
            "조회 결과를 직접 확인해줘",
        )
        self.assertFalse(result.used_llm)

    def test_context_can_be_excluded_for_evidence_only_route(self) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="문서 근거 답변",
                provider="claude",
                used_llm=True,
            )
        )

        _composer(engine).compose(
            _request(),
            evidence={"documents": ["근거"]},
            policy=_policy(include_context=False),
        )

        self.assertEqual(engine.requests[0].context_entries, ())


if __name__ == "__main__":
    unittest.main()
