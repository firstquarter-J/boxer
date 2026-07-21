from concurrent.futures import ThreadPoolExecutor
import json
import logging
import threading
import time
import unittest
from unittest.mock import patch

from boxer_company.thread_playbook_learning import (
    ThreadPlaybookDraft,
    ThreadPlaybookSaveResult,
    _build_slack_permalink_source_key,
    _build_slack_thread_source_key,
    _build_rag_index_line,
    _build_thread_source_pending_line,
    _build_thread_source_index_line,
    _delete_thread_source_reservation_best_effort,
    _ensure_legacy_thread_sources_migrated,
    _extract_page_source_keys,
    _generate_thread_playbook_draft,
    _inspect_thread_source_index,
    _inspect_thread_source_index_state,
    _is_thread_playbook_learning_request,
    _learn_slack_thread_playbook,
    _refresh_thread_source_reservation,
    _save_thread_playbook_to_notion,
    _thread_source_owner,
)
from boxer_company.notion_playbooks import _parse_notion_rag_index_line
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


class FailingPermalinkSlackClient(FakeSlackClient):
    def chat_getPermalink(self, **kwargs):
        raise RuntimeError("permalink unavailable")


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


def _notion_text_block(
    block_id: str,
    block_type: str,
    text: str,
    *,
    created_time: str | None = None,
) -> dict[str, object]:
    block: dict[str, object] = {
        "id": block_id,
        "type": block_type,
        block_type: {"rich_text": [{"plain_text": text}]},
    }
    if created_time:
        block["created_time"] = created_time
    return block


class ThreadPlaybookLearningTests(unittest.TestCase):
    def test_detects_thread_learning_request(self) -> None:
        self.assertTrue(_is_thread_playbook_learning_request("스레드 학습"))
        self.assertTrue(_is_thread_playbook_learning_request("이 스레드 학습"))
        self.assertTrue(_is_thread_playbook_learning_request("thread 학습해줘"))
        self.assertFalse(_is_thread_playbook_learning_request("스레드 내용을 요약해줘"))

    def test_builds_stable_slack_thread_source_key(self) -> None:
        source_key = _build_slack_thread_source_key("W123", "C123", "1712345678.001230")

        self.assertEqual(
            source_key,
            _build_slack_thread_source_key(" W123 ", " C123 ", " 1712345678.001230 "),
        )
        self.assertTrue(source_key.startswith("slack-thread:v1:"))
        self.assertNotEqual(source_key, _build_slack_thread_source_key("W124", "C123", "1712345678.001230"))
        self.assertNotEqual(source_key, _build_slack_thread_source_key("W123", "C124", "1712345678.001230"))
        self.assertNotEqual(source_key, _build_slack_thread_source_key("W123", "C123", "1712345678.001231"))
        with self.assertRaises(ValueError):
            _build_slack_thread_source_key("", "C123", "1712345678.001230")

    def test_source_index_matches_only_exact_source_key(self) -> None:
        source_key = _build_slack_thread_source_key("W123", "C123", "1.0")
        target_page_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        blocks = [
            _notion_text_block(
                "11111111111111111111111111111111",
                "paragraph",
                f"일반 본문에 source_key={source_key} 포함",
            ),
            _notion_text_block("22222222222222222222222222222222", "heading_2", "Slack 스레드 소스 인덱스"),
            _notion_text_block(
                "33333333333333333333333333333333",
                "bulleted_list_item",
                f"source_key={source_key}0 | page_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            _notion_text_block(
                "44444444444444444444444444444444",
                "bulleted_list_item",
                _build_thread_source_index_line(source_key, page_id=target_page_id),
            ),
        ]

        page_id, insert_after, found_heading = _inspect_thread_source_index(blocks, source_key=source_key)

        self.assertEqual(page_id, target_page_id)
        self.assertEqual(insert_after, "44444444444444444444444444444444")
        self.assertTrue(found_heading)

    def test_source_index_rejects_one_key_pointing_to_multiple_pages(self) -> None:
        source_key = _build_slack_thread_source_key("W123", "C123", "1.0")
        blocks = [
            _notion_text_block("1" * 32, "heading_2", "Slack 스레드 소스 인덱스"),
            _notion_text_block(
                "2" * 32,
                "bulleted_list_item",
                _build_thread_source_index_line(source_key, page_id="a" * 32),
            ),
            _notion_text_block(
                "3" * 32,
                "bulleted_list_item",
                _build_thread_source_index_line(source_key, page_id="b" * 32),
            ),
        ]

        with self.assertRaises(RuntimeError):
            _inspect_thread_source_index(blocks, source_key=source_key)

    def test_source_index_selects_same_winner_for_duplicate_pending_blocks(self) -> None:
        source_key = _build_slack_thread_source_key("W123", "C123", "1.0")
        older = _notion_text_block(
            "a" * 32,
            "bulleted_list_item",
            _build_thread_source_pending_line(source_key, owner="1" * 12, updated_at=100),
            created_time="2026-07-21T00:00:00.000Z",
        )
        newer = _notion_text_block(
            "b" * 32,
            "bulleted_list_item",
            _build_thread_source_pending_line(source_key, owner="2" * 12, updated_at=101),
            created_time="2026-07-21T00:00:01.000Z",
        )
        heading = _notion_text_block("c" * 32, "heading_2", "Slack 스레드 소스 인덱스")

        forward = _inspect_thread_source_index_state([heading, older, newer], source_key=source_key)
        reversed_state = _inspect_thread_source_index_state(
            [heading, newer, older],
            source_key=source_key,
        )

        self.assertEqual(forward.pending_block_id, "a" * 32)
        self.assertEqual(reversed_state.pending_block_id, "a" * 32)
        self.assertEqual(forward.pending_block_ids, ("a" * 32, "b" * 32))

    def test_loser_pending_cannot_refresh_reservation(self) -> None:
        root_page_id = "d" * 32
        source_key = _build_slack_thread_source_key("W123", "C123", "1.0")
        blocks = [
            _notion_text_block("c" * 32, "heading_2", "Slack 스레드 소스 인덱스"),
            _notion_text_block(
                "a" * 32,
                "bulleted_list_item",
                _build_thread_source_pending_line(source_key, owner="1" * 12, updated_at=100),
                created_time="2026-07-21T00:00:00.000Z",
            ),
            _notion_text_block(
                "b" * 32,
                "bulleted_list_item",
                _build_thread_source_pending_line(source_key, owner="2" * 12, updated_at=101),
                created_time="2026-07-21T00:00:01.000Z",
            ),
        ]

        with (
            patch(
                "boxer_company.thread_playbook_learning._load_company_root_blocks",
                return_value=blocks,
            ),
            patch("boxer_company.thread_playbook_learning._notion_request") as request_mock,
            self.assertRaisesRegex(RuntimeError, "우선권을 잃었어"),
        ):
            _refresh_thread_source_reservation(
                root_page_id=root_page_id,
                source_key=source_key,
                reservation_block_id="b" * 32,
                expected_owner="2" * 12,
            )

        request_mock.assert_not_called()

    def test_source_marker_is_read_only_from_source_section(self) -> None:
        malicious_key = _build_slack_thread_source_key("W999", "C999", "9.0")
        actual_key = _build_slack_thread_source_key("W123", "C123", "1.0")

        source_keys = _extract_page_source_keys(
            {
                "lines": [
                    "근거 요약",
                    f"- Slack source key: {malicious_key}",
                    "출처",
                    f"- Slack source key: {actual_key}",
                    "다음 섹션",
                    f"- Slack source key: {malicious_key}",
                ]
            }
        )

        self.assertEqual(source_keys, {actual_key})

    def test_process_owner_changes_with_pid(self) -> None:
        with patch("boxer_company.thread_playbook_learning.os.getpid", return_value=100):
            first_owner = _thread_source_owner()
        with patch("boxer_company.thread_playbook_learning.os.getpid", return_value=101):
            second_owner = _thread_source_owner()

        self.assertNotEqual(first_owner, second_owner)
        self.assertRegex(first_owner, r"^[0-9a-f]{12}$")

    def test_reservation_delete_requires_exact_source_owner_and_timestamp(self) -> None:
        root_page_id = "d" * 32
        block_id = "e" * 32
        source_key = _build_slack_thread_source_key("W123", "C123", "1.0")
        pending_block = _notion_text_block(
            block_id,
            "bulleted_list_item",
            _build_thread_source_pending_line(source_key, owner="1" * 12, updated_at=100),
        )

        calls: list[str] = []

        def fake_request(path: str, *, method="GET", payload=None, token=None):
            calls.append(method)
            return pending_block if method == "GET" else {}

        with (
            patch("boxer_company.thread_playbook_learning._company_notion_token", return_value="company-token"),
            patch("boxer_company.thread_playbook_learning._notion_request", side_effect=fake_request),
            patch("boxer_company.thread_playbook_learning._invalidate_notion_playbook_cache"),
        ):
            wrong_owner = _delete_thread_source_reservation_best_effort(
                root_page_id=root_page_id,
                source_key=source_key,
                reservation_block_id=block_id,
                expected_owner="2" * 12,
                expected_updated_at=100,
            )
            refreshed = _delete_thread_source_reservation_best_effort(
                root_page_id=root_page_id,
                source_key=source_key,
                reservation_block_id=block_id,
                expected_owner="1" * 12,
                expected_updated_at=99,
            )
            exact = _delete_thread_source_reservation_best_effort(
                root_page_id=root_page_id,
                source_key=source_key,
                reservation_block_id=block_id,
                expected_owner="1" * 12,
                expected_updated_at=100,
            )

        self.assertFalse(wrong_owner)
        self.assertFalse(refreshed)
        self.assertTrue(exact)
        self.assertEqual(calls, ["GET", "GET", "GET", "DELETE"])

    def test_legacy_permalink_pages_are_indexed_once(self) -> None:
        root_page_id = "a" * 32
        legacy_page_id = "b" * 32
        permalink = "https://lifexio.slack.com/archives/C123/p1712345678001230?thread_ts=1"
        permalink_key = _build_slack_permalink_source_key(permalink)
        root_blocks: list[dict[str, object]] = [
            {
                "id": legacy_page_id,
                "type": "child_page",
                "child_page": {"title": "기존 학습 페이지"},
            }
        ]

        def fake_fetch(*args, **kwargs):
            return list(root_blocks)

        def fake_request(path: str, *, method="GET", payload=None, token=None):
            created: list[dict[str, object]] = []
            for child in (payload or {}).get("children") or []:
                block_type = child["type"]
                text = "".join(
                    str(part.get("text", {}).get("content") or "")
                    for part in child[block_type]["rich_text"]
                )
                block = _notion_text_block(f"{len(root_blocks) + 1:032x}", block_type, text)
                root_blocks.append(block)
                created.append(block)
            return {"results": created}

        with (
            patch("boxer_company.thread_playbook_learning._company_notion_token", return_value="company-token"),
            patch("boxer_company.thread_playbook_learning._fetch_all_notion_blocks", side_effect=fake_fetch),
            patch("boxer_company.thread_playbook_learning._notion_request", side_effect=fake_request) as request_mock,
            patch(
                "boxer_company.thread_playbook_learning._load_notion_page_content_cached",
                return_value={"lines": ["출처", f"- Slack thread: {permalink}"]},
            ) as page_mock,
            patch("boxer_company.thread_playbook_learning._invalidate_notion_playbook_cache"),
        ):
            first_blocks = _ensure_legacy_thread_sources_migrated(root_page_id)
            second_blocks = _ensure_legacy_thread_sources_migrated(root_page_id)

        page_id, _, _ = _inspect_thread_source_index(first_blocks, source_key=permalink_key)
        self.assertEqual(page_id, legacy_page_id)
        self.assertEqual(first_blocks, second_blocks)
        self.assertEqual(page_mock.call_count, 1)
        self.assertEqual(request_mock.call_count, 1)

    def test_company_notion_learning_does_not_fallback_to_personal_settings(self) -> None:
        kwargs = {
            "workspace_id": "W123",
            "channel_id": "C123",
            "thread_ts": "1.0",
            "claude_client": object(),
        }
        with (
            patch("boxer_company.notion_playbooks.cs.NOTION_TOKEN_COMPANY", ""),
            patch("boxer_company.notion_playbooks.s.NOTION_TOKEN_PERSONAL", "personal-token"),
            patch("boxer_company.thread_playbook_learning._generate_thread_playbook_draft") as generate_mock,
            patch("boxer_company.thread_playbook_learning._notion_request") as notion_mock,
        ):
            with self.assertRaises(RuntimeError):
                _learn_slack_thread_playbook(
                    "스레드 본문",
                    root_page_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    **kwargs,
                )
        generate_mock.assert_not_called()
        notion_mock.assert_not_called()

        with (
            patch("boxer_company.notion_playbooks.cs.NOTION_TOKEN_COMPANY", "company-token"),
            patch("boxer_company.notion_playbooks.cs.THREAD_PLAYBOOK_NOTION_ROOT_PAGE_ID", ""),
            patch("boxer_company.notion_playbooks.s.NOTION_TEST_PAGE_ID", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            patch("boxer_company.thread_playbook_learning._generate_thread_playbook_draft") as generate_mock,
            patch("boxer_company.thread_playbook_learning._notion_request") as notion_mock,
        ):
            with self.assertRaises(RuntimeError):
                _learn_slack_thread_playbook("스레드 본문", **kwargs)
        generate_mock.assert_not_called()
        notion_mock.assert_not_called()

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

        def fake_notion_request(path: str, *, method: str = "GET", payload=None, token=None):
            calls.append((path, method, payload))
            if path == "/pages":
                return {"id": created_page_id, "url": "https://notion.example/playbook"}
            return {}

        with (
            patch("boxer_company.thread_playbook_learning._is_company_notion_configured", return_value=True),
            patch("boxer_company.thread_playbook_learning._company_notion_token", return_value="company-token"),
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

    def test_save_rejects_incomplete_reservation_before_page_creation(self) -> None:
        draft = ThreadPlaybookDraft("제목", "증상", "원인", "답변", [], [], [])

        with (
            patch("boxer_company.thread_playbook_learning._is_company_notion_configured", return_value=True),
            patch("boxer_company.thread_playbook_learning._create_notion_playbook_page") as create_mock,
            self.assertRaisesRegex(RuntimeError, "예약 key와 owner"),
        ):
            _save_thread_playbook_to_notion(
                draft,
                root_page_id="a" * 32,
                source_key=_build_slack_thread_source_key("W123", "C123", "1.0"),
                source_reservation_block_id="b" * 32,
                thread_permalink=None,
                learned_by_user_id=None,
            )

        create_mock.assert_not_called()

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

    def test_rag_index_line_sanitizes_protocol_delimiters(self) -> None:
        draft = ThreadPlaybookDraft(
            title="자동 녹화 | 장애",
            symptom="증상",
            cause="원인",
            answer_template="답변",
            checks=[],
            keywords=["자동 | 녹화"],
            source_notes=[],
        )

        parsed = _parse_notion_rag_index_line(
            _build_rag_index_line(draft, page_id="b" * 32)
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["title"], "자동 녹화 장애")
        self.assertNotIn("|", ", ".join(parsed["keywords"]))

    def test_learn_thread_reuses_existing_notion_page_for_same_source_key(self) -> None:
        root_page_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        existing_page_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        source_key = _build_slack_thread_source_key("W123", "C123", "1.0")
        root_blocks = [
            _notion_text_block("11111111111111111111111111111111", "heading_2", "Slack 스레드 소스 인덱스"),
            _notion_text_block(
                "22222222222222222222222222222222",
                "bulleted_list_item",
                _build_thread_source_index_line(source_key, page_id=existing_page_id),
            ),
            _notion_text_block(
                "33333333333333333333333333333333",
                "bulleted_list_item",
                "migration=slack-permalink:v1 | status=complete",
            ),
            _notion_text_block("44444444444444444444444444444444", "heading_2", "RAG 인덱스"),
            _notion_text_block(
                "55555555555555555555555555555555",
                "bulleted_list_item",
                "page_id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb | section=장애 대응 | "
                "kind=runbook | priority=high | title=기존 자동 녹화 플레이북 | keywords=자동 녹화",
            ),
        ]

        with (
            patch("boxer_company.thread_playbook_learning._is_company_notion_configured", return_value=True),
            patch("boxer_company.thread_playbook_learning._company_notion_token", return_value="company-token"),
            patch(
                "boxer_company.thread_playbook_learning._fetch_all_notion_blocks",
                return_value=root_blocks,
            ),
            patch(
                "boxer_company.thread_playbook_learning._load_notion_page_content_cached",
                return_value={
                    "title": "기존 자동 녹화 플레이북",
                    "url": "https://notion.example/existing",
                    "lines": [
                        "관련 키워드",
                        "자동 녹화, 모션감지 사용안함",
                    ],
                },
            ),
            patch("boxer_company.thread_playbook_learning._generate_thread_playbook_draft") as generate_mock,
            patch("boxer_company.thread_playbook_learning._notion_request") as notion_request_mock,
        ):
            result = _learn_slack_thread_playbook(
                "스레드 본문",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                thread_permalink="https://slack.example/thread?thread_ts=1.0&cid=C123",
                learned_by_user_id="U_REQUESTER",
                claude_client=object(),
                root_page_id=root_page_id,
            )

        self.assertFalse(result.created)
        self.assertFalse(result.rag_index_updated)
        self.assertEqual(result.page_id, existing_page_id)
        self.assertEqual(result.url, "https://notion.example/existing")
        self.assertIn("자동 녹화", result.keywords)
        generate_mock.assert_not_called()
        notion_request_mock.assert_not_called()

    def test_pending_source_recovers_created_page_without_creating_another(self) -> None:
        root_page_id = "a" * 32
        page_id = "b" * 32
        reservation_block_id = "c" * 32
        source_key = _build_slack_thread_source_key("W123", "C123", "1.0")
        root_blocks = [
            _notion_text_block("1" * 32, "heading_2", "Slack 스레드 소스 인덱스"),
            _notion_text_block(
                reservation_block_id,
                "bulleted_list_item",
                _build_thread_source_pending_line(source_key),
            ),
            _notion_text_block(
                "2" * 32,
                "bulleted_list_item",
                "migration=slack-permalink:v1 | status=complete",
            ),
            {
                "id": page_id,
                "type": "child_page",
                "child_page": {"title": "생성된 페이지"},
            },
        ]
        existing_result = ThreadPlaybookSaveResult(
            "생성된 페이지",
            page_id,
            "https://notion.example/page",
            ["자동 녹화"],
            True,
            created=False,
        )

        with (
            patch("boxer_company.thread_playbook_learning._is_company_notion_configured", return_value=True),
            patch("boxer_company.thread_playbook_learning._company_notion_token", return_value="company-token"),
            patch(
                "boxer_company.thread_playbook_learning._ensure_legacy_thread_sources_migrated",
                return_value=root_blocks,
            ),
            patch(
                "boxer_company.thread_playbook_learning._load_notion_page_content_cached",
                return_value={"lines": ["출처", f"- Slack source key: {source_key}"]},
            ),
            patch("boxer_company.thread_playbook_learning._finalize_thread_source_reservation") as finalize_mock,
            patch("boxer_company.thread_playbook_learning._delete_thread_source_pending_blocks_best_effort"),
            patch(
                "boxer_company.thread_playbook_learning._build_existing_thread_playbook_result",
                return_value=existing_result,
            ),
            patch("boxer_company.thread_playbook_learning._generate_thread_playbook_draft") as generate_mock,
            patch("boxer_company.thread_playbook_learning._save_thread_playbook_to_notion") as save_mock,
        ):
            result = _learn_slack_thread_playbook(
                "스레드 본문",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                root_page_id=root_page_id,
            )

        self.assertEqual(result, existing_result)
        finalize_mock.assert_called_once_with(
            root_page_id=root_page_id,
            source_key=source_key,
            page_id=page_id,
            reservation_block_id=reservation_block_id,
            expected_owner=_inspect_thread_source_index_state(
                root_blocks,
                source_key=source_key,
            ).pending_owner,
        )
        generate_mock.assert_not_called()
        save_mock.assert_not_called()

    def test_learning_reuses_legacy_permalink_mapping_and_backfills_stable_key(self) -> None:
        root_page_id = "a" * 32
        page_id = "b" * 32
        permalink = "https://lifexio.slack.com/archives/C123/p1712345678001230"
        permalink_key = _build_slack_permalink_source_key(f"{permalink}?thread_ts=1")
        stable_key = _build_slack_thread_source_key("W123", "C123", "1.0")
        root_blocks = [
            _notion_text_block("1" * 32, "heading_2", "Slack 스레드 소스 인덱스"),
            _notion_text_block(
                "2" * 32,
                "bulleted_list_item",
                _build_thread_source_index_line(permalink_key, page_id=page_id),
            ),
            _notion_text_block(
                "3" * 32,
                "bulleted_list_item",
                "migration=slack-permalink:v1 | status=complete",
            ),
        ]
        existing_result = ThreadPlaybookSaveResult("기존 페이지", page_id, "", [], False, created=False)

        with (
            patch("boxer_company.thread_playbook_learning._is_company_notion_configured", return_value=True),
            patch(
                "boxer_company.thread_playbook_learning._ensure_legacy_thread_sources_migrated",
                return_value=root_blocks,
            ),
            patch("boxer_company.thread_playbook_learning._append_thread_source_index_entry") as append_mock,
            patch(
                "boxer_company.thread_playbook_learning._build_existing_thread_playbook_result",
                return_value=existing_result,
            ),
            patch("boxer_company.thread_playbook_learning._generate_thread_playbook_draft") as generate_mock,
        ):
            result = _learn_slack_thread_playbook(
                "스레드 본문",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                thread_permalink=permalink,
                root_page_id=root_page_id,
            )

        self.assertEqual(result, existing_result)
        append_mock.assert_called_once_with(
            root_page_id=root_page_id,
            source_key=stable_key,
            page_id=page_id,
        )
        generate_mock.assert_not_called()

    def test_learning_failure_before_page_creation_releases_reservation(self) -> None:
        root_page_id = "a" * 32
        reservation_block_id = "b" * 32
        root_blocks = [
            _notion_text_block("1" * 32, "heading_2", "Slack 스레드 소스 인덱스"),
            _notion_text_block(
                "2" * 32,
                "bulleted_list_item",
                "migration=slack-permalink:v1 | status=complete",
            ),
        ]

        with (
            patch("boxer_company.thread_playbook_learning._is_company_notion_configured", return_value=True),
            patch(
                "boxer_company.thread_playbook_learning._ensure_legacy_thread_sources_migrated",
                return_value=root_blocks,
            ),
            patch(
                "boxer_company.thread_playbook_learning._reserve_thread_source_index_entry",
                return_value=reservation_block_id,
            ),
            patch("boxer_company.thread_playbook_learning._thread_source_owner", return_value="1" * 12),
            patch(
                "boxer_company.thread_playbook_learning._generate_thread_playbook_draft",
                side_effect=RuntimeError("LLM failed"),
            ),
            patch(
                "boxer_company.thread_playbook_learning._delete_thread_source_reservation_best_effort"
            ) as delete_mock,
            patch("boxer_company.thread_playbook_learning._save_thread_playbook_to_notion") as save_mock,
            self.assertRaisesRegex(RuntimeError, "LLM failed"),
        ):
            _learn_slack_thread_playbook(
                "스레드 본문",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                root_page_id=root_page_id,
            )

        delete_mock.assert_called_once_with(
            root_page_id=root_page_id,
            source_key=_build_slack_thread_source_key("W123", "C123", "1.0"),
            reservation_block_id=reservation_block_id,
            expected_owner="1" * 12,
        )
        save_mock.assert_not_called()

    def test_retry_repairs_rag_index_after_partial_failure(self) -> None:
        root_page_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        created_page_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        source_key = _build_slack_thread_source_key("W123", "C123", "1.0")
        root_blocks: list[dict[str, object]] = []
        calls = {"page": 0, "reserve": 0, "finalize": 0, "rag": 0, "block": 0}
        draft = ThreadPlaybookDraft(
            title="자동 녹화 플레이북",
            symptom="자동 녹화",
            cause="설정 기준",
            answer_template="설정 기준으로 동작해.",
            checks=["설정 확인"],
            keywords=["자동 녹화"],
            source_notes=["확정 답변"],
        )

        def append_payload_blocks(payload: dict[str, object]) -> list[dict[str, object]]:
            created_blocks: list[dict[str, object]] = []
            for child in payload.get("children", []):  # type: ignore[union-attr]
                block = dict(child)
                block_type = str(block["type"])
                rich_text = block[block_type]["rich_text"]  # type: ignore[index]
                text = "".join(str(part.get("text", {}).get("content") or "") for part in rich_text)
                calls["block"] += 1
                stored_block = _notion_text_block(f"{calls['block']:032x}", block_type, text)
                root_blocks.append(stored_block)
                created_blocks.append(stored_block)
            return created_blocks

        def fake_notion_request(path: str, *, method: str = "GET", payload=None, token=None):
            self.assertEqual(token, "company-token")
            if path == "/pages":
                calls["page"] += 1
                root_blocks.append(
                    {
                        "id": created_page_id,
                        "type": "child_page",
                        "child_page": {"title": draft.title},
                    }
                )
                return {"id": created_page_id, "url": "https://notion.example/playbook"}

            if path != f"/blocks/{root_page_id}/children":
                block_id = path.rsplit("/", 1)[-1]
                if method == "DELETE":
                    root_blocks[:] = [block for block in root_blocks if block.get("id") != block_id]
                    return {}
                if method == "GET":
                    return next(block for block in root_blocks if block.get("id") == block_id)
                rich_text = (payload or {}).get("bulleted_list_item", {}).get("rich_text", [])
                text = "".join(str(part.get("text", {}).get("content") or "") for part in rich_text)
                for index, block in enumerate(root_blocks):
                    if block.get("id") == block_id:
                        root_blocks[index] = _notion_text_block(block_id, "bulleted_list_item", text)
                        break
                if "source_key=" in text and "page_id=" in text:
                    calls["finalize"] += 1
                return {}

            children = list((payload or {}).get("children") or [])
            rendered = json.dumps(children, ensure_ascii=False)
            if "status=pending" in rendered:
                calls["reserve"] += 1
            elif "page_id=" in rendered:
                calls["rag"] += 1
                if calls["rag"] == 1:
                    raise RuntimeError("RAG index write failed")
            return {"results": append_payload_blocks(payload or {})}

        def fake_fetch(*args, **kwargs):
            self.assertEqual(kwargs.get("token"), "company-token")
            self.assertEqual(kwargs.get("max_blocks"), 0)
            return list(root_blocks)

        with (
            patch("boxer_company.thread_playbook_learning._is_company_notion_configured", return_value=True),
            patch("boxer_company.thread_playbook_learning._company_notion_token", return_value="company-token"),
            patch("boxer_company.thread_playbook_learning._fetch_all_notion_blocks", side_effect=fake_fetch),
            patch("boxer_company.thread_playbook_learning._notion_request", side_effect=fake_notion_request),
            patch("boxer_company.thread_playbook_learning._generate_thread_playbook_draft", return_value=draft) as generate_mock,
            patch(
                "boxer_company.thread_playbook_learning._load_notion_page_content_cached",
                return_value={
                    "title": draft.title,
                    "url": "https://notion.example/playbook",
                    "lines": [
                        "관련 키워드",
                        "자동 녹화",
                        "출처",
                        f"- Slack source key: {source_key}",
                    ],
                },
            ),
            patch("boxer_company.thread_playbook_learning._invalidate_notion_playbook_cache"),
        ):
            with self.assertRaisesRegex(RuntimeError, "RAG index write failed"):
                _learn_slack_thread_playbook(
                    "스레드 본문",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    root_page_id=root_page_id,
                    claude_client=object(),
                )

            result = _learn_slack_thread_playbook(
                "스레드 본문",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                root_page_id=root_page_id,
                claude_client=object(),
            )

        self.assertFalse(result.created)
        self.assertTrue(result.rag_index_updated)
        self.assertEqual(result.page_id, created_page_id)
        self.assertEqual(calls["page"], 1)
        self.assertEqual(calls["reserve"], 1)
        self.assertEqual(calls["finalize"], 1)
        self.assertEqual(calls["rag"], 2)
        generate_mock.assert_called_once()

    def test_concurrent_learning_creates_one_page_per_source_key(self) -> None:
        page_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        state: dict[str, str] = {}
        state_lock = threading.Lock()
        start_barrier = threading.Barrier(2)
        calls = {"generate": 0, "save": 0}
        draft = ThreadPlaybookDraft("제목", "증상", "원인", "답변", [], ["키워드"], [])

        def fake_root_blocks(*args, **kwargs):
            with state_lock:
                existing_page_id = state.get("page_id")
            blocks = [
                _notion_text_block("1" * 32, "heading_2", "Slack 스레드 소스 인덱스"),
                _notion_text_block(
                    "2" * 32,
                    "bulleted_list_item",
                    "migration=slack-permalink:v1 | status=complete",
                ),
            ]
            if existing_page_id:
                source_key = _build_slack_thread_source_key("W123", "C123", "1.0")
                blocks.insert(
                    1,
                    _notion_text_block(
                        "3" * 32,
                        "bulleted_list_item",
                        _build_thread_source_index_line(source_key, page_id=existing_page_id),
                    ),
                )
            return blocks

        def fake_generate(*args, **kwargs):
            calls["generate"] += 1
            time.sleep(0.05)
            return draft

        def fake_save(*args, **kwargs):
            calls["save"] += 1
            with state_lock:
                state["page_id"] = page_id
            return ThreadPlaybookSaveResult("제목", page_id, "https://notion.example/page", ["키워드"], True)

        def run_learning() -> ThreadPlaybookSaveResult:
            start_barrier.wait()
            return _learn_slack_thread_playbook(
                "스레드 본문",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                root_page_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                claude_client=object(),
            )

        with (
            patch("boxer_company.thread_playbook_learning._is_company_notion_configured", return_value=True),
            patch(
                "boxer_company.thread_playbook_learning._ensure_legacy_thread_sources_migrated",
                side_effect=fake_root_blocks,
            ),
            patch(
                "boxer_company.thread_playbook_learning._load_company_root_blocks",
                side_effect=fake_root_blocks,
            ),
            patch(
                "boxer_company.thread_playbook_learning._reserve_thread_source_index_entry",
                return_value="4" * 32,
            ),
            patch("boxer_company.thread_playbook_learning._refresh_thread_source_reservation"),
            patch("boxer_company.thread_playbook_learning._find_page_with_source_key") as find_page_mock,
            patch("boxer_company.thread_playbook_learning._generate_thread_playbook_draft", side_effect=fake_generate),
            patch("boxer_company.thread_playbook_learning._save_thread_playbook_to_notion", side_effect=fake_save),
            patch(
                "boxer_company.thread_playbook_learning._build_existing_thread_playbook_result",
                return_value=ThreadPlaybookSaveResult(
                    "제목",
                    page_id,
                    "https://notion.example/page",
                    ["키워드"],
                    False,
                    created=False,
                ),
            ),
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _: run_learning(), range(2)))

        self.assertEqual(calls, {"generate": 1, "save": 1})
        find_page_mock.assert_not_called()
        self.assertEqual({result.page_id for result in results}, {page_id})
        self.assertEqual(sorted(result.created for result in results), [False, True])

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
                    workspace_id="W123",
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
        self.assertEqual(learn_mock.call_args.kwargs["workspace_id"], "W123")
        self.assertEqual(learn_mock.call_args.kwargs["channel_id"], "C123")
        self.assertEqual(learn_mock.call_args.kwargs["thread_ts"], "1.0")

    def test_thread_learning_route_marks_existing_playbook(self) -> None:
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
                        "title": "기존 자동 녹화 플레이북",
                        "url": "https://notion.example/existing",
                        "keywords": ["자동 녹화"],
                        "page_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "created": False,
                    },
                )(),
            ),
        ):
            handled = _handle_thread_learning_routes(
                ThreadLearningRoutesContext(
                    question="이 스레드 학습",
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U_REQUESTER",
                    workspace_id="W123",
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
        self.assertIn("이미 학습된 스레드야", replies[0])
        self.assertIn("https://notion.example/existing", replies[0])

    def test_thread_learning_route_continues_when_permalink_lookup_fails(self) -> None:
        replies: list[str] = []
        client = FailingPermalinkSlackClient()

        with (
            patch("boxer_company_adapter_slack.thread_learning_routes.cs.THREAD_PLAYBOOK_LEARNING_ENABLED", True),
            patch("boxer_company_adapter_slack.thread_learning_routes.cs.THREAD_PLAYBOOK_LEARNING_ALLOWED_USER_IDS", set()),
            patch(
                "boxer_company_adapter_slack.thread_learning_routes._learn_slack_thread_playbook",
                return_value=ThreadPlaybookSaveResult(
                    "기존 플레이북",
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "https://notion.example/existing",
                    ["자동 녹화"],
                    False,
                    created=False,
                ),
            ) as learn_mock,
        ):
            handled = _handle_thread_learning_routes(
                ThreadLearningRoutesContext(
                    question="이 스레드 학습",
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U_REQUESTER",
                    workspace_id="W123",
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
        self.assertIn("이미 학습된 스레드야", replies[0])
        self.assertIsNone(learn_mock.call_args.kwargs["thread_permalink"])
        self.assertEqual(learn_mock.call_args.kwargs["workspace_id"], "W123")


if __name__ == "__main__":
    unittest.main()
