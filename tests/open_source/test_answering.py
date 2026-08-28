import subprocess
import sys
import unittest
from unittest.mock import patch

import boxer
from boxer.answering import (
    AnswerEngine,
    AnswerRequest,
    AnswerResult,
    create_answer_engine_from_settings,
    synthesize_retrieval_answer,
)


class AnswerEngineTests(unittest.TestCase):
    def test_package_import_does_not_eagerly_load_llm_provider(self) -> None:
        # Adapter의 provider-free 계약 import가 Anthropic 초기화를 유발하지 않는다.
        script = """
import sys
import boxer

assert 'boxer.answering' not in sys.modules
assert 'boxer.core.llm' not in sys.modules
assert 'anthropic' not in sys.modules
assert boxer.ContextEntry.__name__ == 'ContextEntry'
assert 'boxer.answering' not in sys.modules
"""
        completed = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_top_level_package_exports_stable_answer_contract(self) -> None:
        self.assertIs(boxer.AnswerEngine, AnswerEngine)
        self.assertIs(boxer.AnswerRequest, AnswerRequest)
        self.assertIs(boxer.AnswerResult, AnswerResult)
        self.assertIs(
            boxer.create_answer_engine_from_settings,
            create_answer_engine_from_settings,
        )
        self.assertIs(
            boxer.synthesize_retrieval_answer,
            synthesize_retrieval_answer,
        )

    @patch("boxer.answering._build_claude_client")
    def test_settings_factory_closes_provider_when_synthesis_is_disabled(self, mocked_builder) -> None:
        # feature flag가 꺼지면 provider credential 유무와 관계없이 모든 adapter가 같은 no-LLM 엔진을 받는다.
        with (
            patch("boxer.answering.s.LLM_SYNTHESIS_ENABLED", False),
            patch("boxer.answering.s.LLM_PROVIDER", "claude"),
            patch("boxer.answering.s.ANTHROPIC_API_KEY", "configured"),
        ):
            engine = create_answer_engine_from_settings()

        self.assertEqual(engine.provider, "")
        mocked_builder.assert_not_called()
        result = engine.answer(AnswerRequest(question="질문", evidence={"answer": "근거"}))
        self.assertEqual(result.failure_reason, "provider_unconfigured")

    @patch("boxer.answering._build_claude_client")
    def test_settings_factory_does_not_build_client_from_placeholder_credentials(
        self,
        mocked_builder,
    ) -> None:
        # 샘플 placeholder는 API key로 쓰거나 OAuth command로 실행하지 않는다.
        with (
            patch("boxer.answering.s.LLM_SYNTHESIS_ENABLED", True),
            patch("boxer.answering.s.LLM_PROVIDER", "claude"),
            patch("boxer.answering.s.ANTHROPIC_API_KEY", "REPLACE_ME"),
            patch("boxer.answering.s.ANTHROPIC_AUTH_TOKEN", ""),
            patch(
                "boxer.answering.s.ANTHROPIC_AUTH_TOKEN_COMMAND",
                "REPLACE_ME",
            ),
        ):
            engine = create_answer_engine_from_settings()

        self.assertEqual(engine.provider, "claude")
        mocked_builder.assert_not_called()

    def test_answer_passes_normalized_context_and_evidence_to_provider(self) -> None:
        calls: list[dict] = []

        def synthesize(**kwargs):
            calls.append(kwargs)
            return "근거 기반 답변"

        engine = AnswerEngine(
            provider="claude",
            provider_client=object(),
            synthesize=synthesize,
        )
        result = engine.answer(
            AnswerRequest(
                question="질문",
                evidence={"route": "example"},
                context_entries=(
                    {
                        "kind": "message",
                        "source": "slack",
                        "author_id": "U1",
                        "text": "이전 질문",
                    },
                ),
                system_prompt="system",
                extra_rules="\n추가 규칙",
                max_tokens=123,
            )
        )

        self.assertEqual(
            result,
            AnswerResult(
                text="근거 기반 답변",
                provider="claude",
                used_llm=True,
            ),
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["question"], "질문")
        self.assertEqual(calls[0]["thread_context"], "U1: 이전 질문")
        self.assertEqual(calls[0]["evidence_payload"], {"route": "example"})
        self.assertIsNotNone(calls[0]["provider_client"])
        self.assertEqual(calls[0]["max_tokens"], 123)

    def test_timeout_is_returned_as_safe_structured_failure(self) -> None:
        def timeout(**kwargs):
            raise TimeoutError("secret provider detail")

        result = AnswerEngine(
            provider="ollama",
            synthesize=timeout,
        ).answer(
            AnswerRequest(
                question="질문",
                evidence={"private": "raw evidence"},
            )
        )

        self.assertEqual(result.text, "")
        self.assertFalse(result.used_llm)
        self.assertEqual(result.failure_reason, "timeout")
        self.assertNotIn("secret", repr(result))
        self.assertNotIn("raw evidence", repr(result))

    def test_provider_specific_timeout_is_normalized_without_error_detail(self) -> None:
        class ProviderTimeoutError(Exception):
            pass

        def timeout(**kwargs):
            raise ProviderTimeoutError("request timed out with secret detail")

        result = AnswerEngine(
            provider="claude",
            synthesize=timeout,
        ).answer(AnswerRequest(question="질문", evidence={"private": "raw"}))

        self.assertEqual(result.failure_reason, "timeout")
        self.assertNotIn("secret", repr(result))
        self.assertNotIn("raw", repr(result))

    def test_unsupported_provider_does_not_call_synthesizer(self) -> None:
        calls: list[dict] = []

        result = AnswerEngine(
            provider="unknown",
            synthesize=lambda **kwargs: calls.append(kwargs) or "unexpected",
        ).answer(AnswerRequest(question="질문", evidence={}))

        self.assertEqual(result.failure_reason, "unsupported_provider")
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
