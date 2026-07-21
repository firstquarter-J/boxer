import http.client
import io
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

from boxer.retrieval.connectors.notion import (
    _NOTION_PAGE_CACHE,
    _build_notion_headers,
    _fetch_all_notion_blocks,
    _invalidate_notion_page_cache,
    _load_notion_page_content_cached,
    _notion_cache_key,
    _notion_request,
)


class NotionConnectorTests(unittest.TestCase):
    def test_explicit_token_is_used_without_personal_token(self) -> None:
        # 회사 integration은 개인 토큰 설정과 무관하게 호출별 토큰을 사용할 수 있어야 한다.
        with patch("boxer.retrieval.connectors.notion.s.NOTION_TOKEN_PERSONAL", ""):
            headers = _build_notion_headers("company-token")

        self.assertEqual(headers["Authorization"], "Bearer company-token")

    def test_page_cache_key_is_separated_by_token(self) -> None:
        page_id = "11111111111111111111111111111111"

        self.assertNotEqual(
            _notion_cache_key(page_id, "personal-token"),
            _notion_cache_key(page_id, "company-token"),
        )

    def test_page_cache_is_kept_separately_and_invalidated_by_token(self) -> None:
        page_id = "11111111111111111111111111111111"
        _NOTION_PAGE_CACHE.clear()

        def fake_load(target_page_id: str, *, token=None):
            return {"pageId": target_page_id, "title": f"title:{token}"}

        with patch("boxer.retrieval.connectors.notion._load_notion_page_content", side_effect=fake_load) as load_mock:
            token_a_first = _load_notion_page_content_cached(page_id, token="token-A")
            token_b_first = _load_notion_page_content_cached(page_id, token="token-B")
            token_a_second = _load_notion_page_content_cached(page_id, token="token-A")

            self.assertEqual(token_a_first, token_a_second)
            self.assertNotEqual(token_a_first, token_b_first)
            self.assertEqual(load_mock.call_count, 2)

            _invalidate_notion_page_cache(page_id, token="token-A")
            _load_notion_page_content_cached(page_id, token="token-B")
            _load_notion_page_content_cached(page_id, token="token-A")

        self.assertEqual(load_mock.call_count, 3)
        self.assertFalse(any("token-A" in cache_key or "token-B" in cache_key for cache_key in _NOTION_PAGE_CACHE))
        _NOTION_PAGE_CACHE.clear()

    def test_unbounded_block_fetch_reads_beyond_default_limit(self) -> None:
        calls: list[tuple[str | None, int, str | None]] = []

        def fake_fetch(block_id: str, *, start_cursor=None, page_size=100, token=None):
            calls.append((start_cursor, page_size, token))
            page_number = {None: 0, "C1": 1, "C2": 2}[start_cursor]
            results = [{"id": f"{page_number * 100 + index:032x}"} for index in range(100 if page_number < 2 else 1)]
            return {
                "results": results,
                "has_more": page_number < 2,
                "next_cursor": ("C1", "C2", None)[page_number],
            }

        with patch("boxer.retrieval.connectors.notion._fetch_notion_block_children", side_effect=fake_fetch):
            blocks = _fetch_all_notion_blocks(
                "11111111111111111111111111111111",
                token="company-token",
                max_blocks=0,
            )

        self.assertEqual(len(blocks), 201)
        self.assertEqual(calls, [(None, 100, "company-token"), ("C1", 100, "company-token"), ("C2", 100, "company-token")])

    def test_default_block_fetch_still_honors_configured_limit(self) -> None:
        def fake_fetch(block_id: str, *, start_cursor=None, page_size=100, token=None):
            return {
                "results": [{"id": f"{index:032x}"} for index in range(100)],
                "has_more": True,
                "next_cursor": "C1",
            }

        with (
            patch("boxer.retrieval.connectors.notion.s.NOTION_MAX_BLOCKS", 1),
            patch("boxer.retrieval.connectors.notion._fetch_notion_block_children", side_effect=fake_fetch) as fetch_mock,
        ):
            blocks = _fetch_all_notion_blocks("1" * 32, token="company-token")

        self.assertEqual(len(blocks), 1)
        self.assertEqual(fetch_mock.call_args.kwargs["page_size"], 1)

    def test_negative_block_limit_is_rejected(self) -> None:
        with (
            patch("boxer.retrieval.connectors.notion._fetch_notion_block_children") as fetch_mock,
            self.assertRaises(ValueError),
        ):
            _fetch_all_notion_blocks("1" * 32, token="company-token", max_blocks=-1)

        fetch_mock.assert_not_called()

    def test_block_fetch_rejects_repeated_pagination_cursor(self) -> None:
        responses = [
            {"results": [], "has_more": True, "next_cursor": "C1"},
            {"results": [], "has_more": True, "next_cursor": "C1"},
        ]
        with (
            patch("boxer.retrieval.connectors.notion._fetch_notion_block_children", side_effect=responses),
            self.assertRaises(RuntimeError),
        ):
            _fetch_all_notion_blocks(
                "11111111111111111111111111111111",
                token="company-token",
                max_blocks=0,
            )

    def test_block_fetch_rejects_missing_cursor_when_more_pages_exist(self) -> None:
        with (
            patch(
                "boxer.retrieval.connectors.notion._fetch_notion_block_children",
                return_value={"results": [], "has_more": True, "next_cursor": None},
            ),
            self.assertRaises(RuntimeError),
        ):
            _fetch_all_notion_blocks(
                "1" * 32,
                token="company-token",
                max_blocks=0,
            )

    def test_notion_request_retries_rate_limit_using_retry_after(self) -> None:
        rate_limit_error = urllib.error.HTTPError(
            "https://api.notion.test/pages",
            429,
            "rate limited",
            {"Retry-After": "0"},
            io.BytesIO(b'{"message":"slow down"}'),
        )
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"ok": true}'

        with (
            patch("boxer.retrieval.connectors.notion.urllib.request.urlopen", side_effect=[rate_limit_error, response]) as urlopen_mock,
            patch("boxer.retrieval.connectors.notion.time.sleep") as sleep_mock,
        ):
            payload = _notion_request("/pages", token="company-token")

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(urlopen_mock.call_count, 2)
        sleep_mock.assert_called_once_with(0.1)

    def test_notion_request_wraps_response_transport_failures(self) -> None:
        for error in (
            ConnectionResetError("connection reset"),
            http.client.IncompleteRead(b"partial"),
        ):
            with self.subTest(error_type=type(error).__name__):
                response = MagicMock()
                response.__enter__.return_value.read.side_effect = error
                with (
                    patch(
                        "boxer.retrieval.connectors.notion.urllib.request.urlopen",
                        return_value=response,
                    ),
                    self.assertRaisesRegex(RuntimeError, "Notion API 연결 실패"),
                ):
                    _notion_request("/pages", token="company-token")


if __name__ == "__main__":
    unittest.main()
