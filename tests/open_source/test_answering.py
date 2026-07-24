import unittest

import boxer
from boxer.answering import (
    AnswerEngine,
    AnswerRequest,
    AnswerResult,
    synthesize_retrieval_answer,
)


class AnswerEngineTests(unittest.TestCase):
    def test_top_level_package_exports_stable_answer_contract(self) -> None:
        self.assertIs(boxer.AnswerEngine, AnswerEngine)
        self.assertIs(boxer.AnswerRequest, AnswerRequest)
        self.assertIs(boxer.AnswerResult, AnswerResult)
        self.assertIs(
            boxer.synthesize_retrieval_answer,
            synthesize_retrieval_answer,
        )

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
