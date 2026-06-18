import logging
import unittest
from unittest.mock import patch

from boxer_company.thread_playbook_learning import (
    ThreadPlaybookDraft,
    _build_rag_index_line,
    _generate_thread_playbook_draft,
    _is_thread_playbook_learning_request,
    _save_thread_playbook_to_notion,
)
from boxer_company_adapter_slack.thread_learning_routes import (
    ThreadLearningRoutesContext,
    _handle_thread_learning_routes,
)


class FakeSlackClient:
    def __init__(self) -> None:
        self.permalink_calls: list[dict[str, str]] = []

    def conversations_replies(self, **kwargs):
        return {
            "messages": [
                {
                    "ts": "1.0",
                    "user": "U_ROSA",
                    "text": "녹화시작을 누르지 않았으나 자동으로 녹화시작이 되어 확인 요청",
                },
                {
                    "ts": "1.1",
                    "user": "U_HYUN",
                    "text": "모션감지 사용안함 설정은 바코드 스캔 후 1시간이 지나면 자동 녹화를 시작합니다.",
                },
                {
                    "ts": "1.2",
                    "user": "U_HYUN",
                    "text": "v2.11.300 버전부터 이렇게 동작하고 있습니다.",
                },
                {
                    "ts": "1.3",
                    "user": "U_REQUESTER",
                    "text": "<@UBOT> 이 스레드 학습",
                },
            ],
            "has_more": False,
        }

    def chat_getPermalink(self, **kwargs):
        self.permalink_calls.append(kwargs)
        return {"permalink": "https://slack.example/thread"}


def _payload() -> dict[str, object]:
    return {
        "raw_text": "<@UBOT> 이 스레드 학습",
        "text": "이 스레드 학습",
        "question": "이 스레드 학습",
        "user_id": "U_REQUESTER",
        "workspace_id": "W123",
        "channel_id": "C123",
        "current_ts": "1.3",
        "thread_ts": "1.0",
        "request_log": {},
    }


class ThreadPlaybookLearningTests(unittest.TestCase):
    def test_detects_thread_learning_request(self) -> None:
        self.assertTrue(_is_thread_playbook_learning_request("스레드 학습"))
        self.assertTrue(_is_thread_playbook_learning_request("이 스레드 학습"))
        self.assertTrue(_is_thread_playbook_learning_request("thread 학습해줘"))
        self.assertFalse(_is_thread_playbook_learning_request("스레드 내용을 요약해줘"))

    def test_generate_thread_playbook_draft_from_claude_json(self) -> None:
        raw_json = """
        {
          "title": "모션감지 사용안함 상태 자동 녹화 시작",
          "symptom": "바코드 스캔 후 약 1시간 뒤 자동으로 녹화가 시작됨",
          "cause": "v2.11.300부터 모션감지 사용안함 설정에서는 1시간 뒤 자동 녹화를 시작함",
          "answerTemplate": "모션감지 사용안함이면 바코드 스캔 후 1시간 뒤 자동 녹화가 시작돼.",
          "checks": ["모션감지 사용안함 여부", "바코드 스캔 시각", "1시간 뒤 녹화 시작 여부"],
          "keywords": ["자동 녹화", "모션감지 사용안함", "v2.11.300"],
          "sourceNotes": ["Hyun의 확정 답변 기준"]
        }
        """

        with (
            patch("boxer_company.thread_playbook_learning.s.LLM_PROVIDER", "claude"),
            patch("boxer_company.thread_playbook_learning._ask_claude", return_value=raw_json),
        ):
            draft = _generate_thread_playbook_draft(
                "U1: 자동 녹화 문의\nU2: v2.11.300부터 1시간 뒤 자동 녹화",
                thread_permalink="https://slack.example/thread",
                claude_client=object(),
            )

        self.assertEqual(draft.title, "모션감지 사용안함 상태 자동 녹화 시작")
        self.assertIn("1시간", draft.cause)
        self.assertIn("자동 녹화", draft.keywords)

    def test_save_thread_playbook_creates_page_and_updates_rag_index(self) -> None:
        draft = ThreadPlaybookDraft(
            title="모션감지 사용안함 상태 자동 녹화 시작",
            symptom="바코드 스캔 후 약 1시간 뒤 자동 녹화",
            cause="v2.11.300부터 모션감지 사용안함이면 1시간 뒤 자동 녹화",
            answer_template="모션감지 사용안함이면 바코드 스캔 후 1시간 뒤 자동 녹화가 시작돼.",
            checks=["모션감지 사용안함 여부"],
            keywords=["자동 녹화", "모션감지 사용안함", "v2.11.300"],
            source_notes=["확정 답변 기준"],
        )
        root_page_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        created_page_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        calls: list[tuple[str, str, dict[str, object] | None]] = []

        def fake_notion_request(path: str, *, method: str = "GET", payload=None):
            calls.append((path, method, payload))
            if path == "/pages":
                return {"id": created_page_id, "url": "https://notion.example/playbook"}
            return {}

        with (
            patch("boxer_company.thread_playbook_learning._is_notion_configured", return_value=True),
            patch("boxer_company.thread_playbook_learning._notion_request", side_effect=fake_notion_request),
            patch(
                "boxer_company.thread_playbook_learning._fetch_all_notion_blocks",
                return_value=[
                    {
                        "id": "cccccccccccccccccccccccccccccccc",
                        "type": "heading_2",
                        "heading_2": {"rich_text": [{"plain_text": "RAG 인덱스"}]},
                    }
                ],
            ),
            patch("boxer_company.thread_playbook_learning._invalidate_notion_playbook_cache"),
        ):
            result = _save_thread_playbook_to_notion(
                draft,
                root_page_id=root_page_id,
                thread_permalink="https://slack.example/thread",
                learned_by_user_id="U_REQUESTER",
            )

        self.assertEqual(result.page_id, created_page_id)
        self.assertTrue(result.rag_index_updated)
        self.assertEqual(calls[0][0], "/pages")
        self.assertEqual(calls[1][0], f"/blocks/{root_page_id}/children")
        index_payload = calls[1][2] or {}
        self.assertIn("after", index_payload)

    def test_rag_index_line_uses_playbook_search_keywords(self) -> None:
        draft = ThreadPlaybookDraft(
            title="모션감지 사용안함 상태 자동 녹화 시작",
            symptom="바코드 스캔 후 자동 녹화",
            cause="v2.11.300 기준",
            answer_template="답변",
            checks=[],
            keywords=["자동 녹화", "모션감지 사용안함"],
            source_notes=[],
        )

        line = _build_rag_index_line(draft, page_id="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

        self.assertIn("title=모션감지 사용안함 상태 자동 녹화 시작", line)
        self.assertIn("keywords=자동 녹화, 모션감지 사용안함", line)

    def test_thread_learning_route_learns_before_freeform(self) -> None:
        replies: list[str] = []
        client = FakeSlackClient()

        with (
            patch("boxer_company_adapter_slack.thread_learning_routes.cs.THREAD_PLAYBOOK_LEARNING_ENABLED", True),
            patch("boxer_company_adapter_slack.thread_learning_routes.cs.THREAD_PLAYBOOK_LEARNING_ALLOWED_USER_IDS", set()),
            patch(
                "boxer_company_adapter_slack.thread_learning_routes._learn_slack_thread_playbook",
                return_value=type(
                    "Result",
                    (),
                    {
                        "title": "모션감지 사용안함 상태 자동 녹화 시작",
                        "url": "https://notion.example/playbook",
                        "keywords": ["자동 녹화", "모션감지 사용안함"],
                        "page_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    },
                )(),
            ) as learn_mock,
        ):
            handled = _handle_thread_learning_routes(
                ThreadLearningRoutesContext(
                    question="이 스레드 학습",
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U_REQUESTER",
                    channel_id="C123",
                    current_ts="1.3",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    logger=logging.getLogger(__name__),
                    client=client,
                    claude_client=object(),
                )
            )

        self.assertTrue(handled)
        self.assertEqual(len(replies), 1)
        self.assertIn("스레드 학습 완료", replies[0])
        learn_mock.assert_called_once()
        learned_thread_text = learn_mock.call_args.args[0]
        self.assertIn("1시간이 지나면 자동 녹화", learned_thread_text)
        self.assertNotIn("이 스레드 학습", learned_thread_text)


if __name__ == "__main__":
    unittest.main()
