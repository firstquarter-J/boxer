import unittest
from unittest.mock import patch

from boxer_company.notion_playbooks import (
    _NOTION_INDEX_CACHE,
    _invalidate_notion_playbook_cache,
    _load_notion_rag_index,
    _select_notion_references,
)


def _notion_text_block(block_id: str, block_type: str, text: str) -> dict[str, object]:
    return {
        "id": block_id,
        "type": block_type,
        block_type: {"rich_text": [{"plain_text": text}]},
    }


class NotionPlaybooksTests(unittest.TestCase):
    def test_company_notion_lookup_does_not_fallback_to_personal_settings(self) -> None:
        with (
            patch("boxer_company.notion_playbooks.cs.NOTION_TOKEN_COMPANY", ""),
            patch("boxer_company.notion_playbooks.cs.THREAD_PLAYBOOK_NOTION_ROOT_PAGE_ID", "a" * 32),
            patch("boxer_company.notion_playbooks.s.NOTION_TOKEN_PERSONAL", "personal-token"),
            patch("boxer_company.notion_playbooks._fetch_all_notion_blocks") as fetch_mock,
        ):
            references = _select_notion_references("마미박스 녹화 프로세스 설명해줘")

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "마미박스 프로세스 순서")
        fetch_mock.assert_not_called()

    def test_company_rag_index_helper_does_not_fallback_to_personal_token(self) -> None:
        with (
            patch("boxer_company.notion_playbooks.cs.NOTION_TOKEN_COMPANY", ""),
            patch("boxer_company.notion_playbooks.s.NOTION_TOKEN_PERSONAL", "personal-token"),
            patch("boxer_company.notion_playbooks._fetch_all_notion_blocks") as fetch_mock,
            self.assertRaises(RuntimeError),
        ):
            _load_notion_rag_index("a" * 32)

        fetch_mock.assert_not_called()

    def test_remote_notion_failure_keeps_local_playbook(self) -> None:
        with (
            patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=True),
            patch("boxer_company.notion_playbooks._select_notion_playbooks", side_effect=RuntimeError("api failed")),
        ):
            references = _select_notion_references(
                "모션감지 사용안함 설정 상태에서 자동으로 녹화시작 되는 이유는?",
                root_page_id="a" * 32,
            )

        self.assertTrue(references)
        self.assertEqual(
            references[0]["title"],
            "모션감지 사용안함 상태에서 바코드 스캔 후 1시간 뒤 자동 녹화 시작",
        )

    def test_remote_notion_timeout_keeps_local_playbook(self) -> None:
        with (
            patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=True),
            patch("boxer_company.notion_playbooks._select_notion_playbooks", side_effect=TimeoutError("timeout")),
        ):
            references = _select_notion_references(
                "모션감지 사용안함 설정 상태에서 자동으로 녹화시작 되는 이유는?",
                root_page_id="a" * 32,
            )

        self.assertTrue(references)
        self.assertIn("자동 녹화 시작", references[0]["title"])

    def test_remote_overview_failure_keeps_local_playbook(self) -> None:
        with (
            patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=True),
            patch("boxer_company.notion_playbooks._build_notion_overview_reference", side_effect=RuntimeError("api failed")),
            patch("boxer_company.notion_playbooks._select_notion_playbooks", return_value=[]) as playbook_mock,
        ):
            references = _select_notion_references(
                "마미박스 녹화 프로세스 설명해줘",
                root_page_id="a" * 32,
            )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "마미박스 프로세스 순서")
        playbook_mock.assert_not_called()

    def test_remote_notion_failure_does_not_hide_programming_error(self) -> None:
        with (
            patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=True),
            patch("boxer_company.notion_playbooks._select_local_playbooks", return_value=[]),
            patch("boxer_company.notion_playbooks._select_notion_playbooks", side_effect=AssertionError("bug")),
            self.assertRaises(AssertionError),
        ):
            _select_notion_references("임의 질의", root_page_id="a" * 32)

    def test_rag_index_cache_is_separated_and_invalidated_by_token(self) -> None:
        root_page_id = "a" * 32
        fetch_tokens: list[str | None] = []
        _NOTION_INDEX_CACHE.clear()

        def fake_fetch(page_id: str, *, token=None, max_blocks=None):
            fetch_tokens.append(token)
            learned_page_id = "b" * 32 if token == "token-A" else "c" * 32
            title = "A 문서" if token == "token-A" else "B 문서"
            return [
                _notion_text_block("1" * 32, "heading_2", "RAG 인덱스"),
                _notion_text_block(
                    "2" * 32,
                    "bulleted_list_item",
                    f"page_id={learned_page_id} | section=가이드 | kind=guide | priority=high | "
                    f"title={title} | keywords=테스트",
                ),
            ]

        with (
            patch("boxer_company.notion_playbooks._fetch_all_notion_blocks", side_effect=fake_fetch),
            patch("boxer_company.notion_playbooks._invalidate_notion_page_cache"),
        ):
            token_a_first = _load_notion_rag_index(root_page_id, token="token-A")
            token_b_first = _load_notion_rag_index(root_page_id, token="token-B")
            token_a_second = _load_notion_rag_index(root_page_id, token="token-A")
            _invalidate_notion_playbook_cache(root_page_id, token="token-A")
            token_b_second = _load_notion_rag_index(root_page_id, token="token-B")
            token_a_after_invalidation = _load_notion_rag_index(root_page_id, token="token-A")

        self.assertEqual(token_a_first, token_a_second)
        self.assertEqual(token_b_first, token_b_second)
        self.assertNotEqual(token_a_first, token_b_first)
        self.assertEqual(token_a_first, token_a_after_invalidation)
        self.assertEqual(fetch_tokens, ["token-A", "token-B", "token-A"])
        self.assertFalse(any("token-A" in cache_key or "token-B" in cache_key for cache_key in _NOTION_INDEX_CACHE))
        _NOTION_INDEX_CACHE.clear()

    def test_root_cache_invalidation_without_token_clears_all_token_snapshots(self) -> None:
        with patch("boxer_company.notion_playbooks._invalidate_notion_page_cache") as page_cache_mock:
            _invalidate_notion_playbook_cache("a" * 32)

        page_cache_mock.assert_called_once_with("a" * 32, token=None, all_tokens=True)

    @patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=False)
    def test_local_mommybox_process_playbook_is_selected_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "마미박스 녹화 프로세스 설명해줘",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "마미박스 녹화 프로세스 설명해줘",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "마미박스 프로세스 순서")
        self.assertIn("준비 음성", references[0]["previewLines"][0])

    @patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=False)
    def test_local_mommybox_cancel_voice_question_selects_process_playbook(self, _: object) -> None:
        references = _select_notion_references(
            "마미박스 녹화 취소 음성 왜 나와?",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "마미박스 녹화 취소 음성 왜 나와?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "마미박스 프로세스 순서")
        self.assertTrue(any("녹화 취소 안내 음성" in line for line in references[0]["previewLines"]))

    @patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=False)
    def test_local_auto_recording_retry_gap_playbook_is_selected_without_notion(self, _: object) -> None:
        # 종료 직후 재스캔 실패 케이스는 Notion 없이도 자유답변에서 운영 안내가 나와야 한다.
        references = _select_notion_references(
            "파란 LED 본 뒤 자동으로 녹화가 시작됐고 바로 재녹화하니 두 번째 스캔 실패했어",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "파란 LED 본 뒤 자동으로 녹화가 시작됐고 바로 재녹화하니 두 번째 스캔 실패했어",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "자동 녹화 시작 후 즉시 재녹화 실패")
        self.assertTrue(any("10초 이상" in line for line in references[0]["previewLines"]))

    @patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=False)
    def test_local_motion_disabled_auto_recording_playbook_is_selected_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "모션감지 사용안함 설정 상태에서 자동으로 녹화시작 되는 이유는?",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "모션감지 사용안함 설정 상태에서 자동으로 녹화시작 되는 이유는?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(
            references[0]["title"],
            "모션감지 사용안함 상태에서 바코드 스캔 후 1시간 뒤 자동 녹화 시작",
        )
        self.assertTrue(any("v2.11.300" in line for line in references[0]["previewLines"]))
        self.assertFalse(any("파란 LED는 모션 감지 대기" in line for line in references[0]["previewLines"]))

    @patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=False)
    def test_local_pink_barcode_overview_playbook_is_selected_for_overview_query(self, _: object) -> None:
        references = _select_notion_references(
            "핑크 바코드 전체 정리해줘",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "핑크 바코드 전체 정리해줘",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "핑크 바코드: 운영 개요")
        self.assertIn("동기화, 앱 표시, 검증 정책 3가지", references[0]["previewLines"][0])

    @patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=False)
    def test_local_barcode_edge_case_playbook_is_selected_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "분만 병원에서 바코드 구매 후 첫 촬영이 비분만 병원이면 앱에 핑크 바코드로 떠?",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "분만 병원에서 바코드 구매 후 첫 촬영이 비분만 병원이면 앱에 핑크 바코드로 떠?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "바코드 표시: 구매 병원과 첫 촬영 병원이 다른 경우")
        self.assertIn("첫 녹화가 발생한 병원 기준", references[0]["previewLines"][0])

    @patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=False)
    def test_local_pink_barcode_validation_playbook_is_selected_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "핑크바코드는 분만 병원에서 녹화 차단 설정 가능해? 검증 풀면 녹화 진행돼?",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "핑크바코드는 분만 병원에서 녹화 차단 설정 가능해? 검증 풀면 녹화 진행돼?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "바코드 검증: 핑크 바코드만 예외 허용할 수 있는지")
        self.assertIn("핑크 바코드만 따로 허용하거나 막는 설정은 없어", references[0]["previewLines"][0])

    @patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=False)
    def test_local_validation_status_question_selects_validation_playbook_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "해당 박스에 유효성 검사가 정상적으로 작동 되고 있어?",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "해당 박스에 유효성 검사가 정상적으로 작동 되고 있어?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "바코드 검증: 핑크 바코드만 예외 허용할 수 있는지")
        self.assertIn("촬영 전에 이 바코드로 진행해도 되는지", references[0]["previewLines"][1])

    @patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=False)
    def test_local_led_playbook_is_selected_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "LED 초록불 길게 깜빡이다가 빨간불 잠시 들어옴 반복은 어떤 상태야?",
            evidence_payload={
                "route": "device_led_pattern_guide",
                "request": {
                    "question": "LED 초록불 길게 깜빡이다가 빨간불 잠시 들어옴 반복은 어떤 상태야?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "마미박스 장비 LED 상태표시등")
        self.assertIn("warning", references[0]["previewLines"][1])


if __name__ == "__main__":
    unittest.main()
