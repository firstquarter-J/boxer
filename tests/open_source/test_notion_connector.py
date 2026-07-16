import unittest
from unittest.mock import patch

from boxer.retrieval.connectors.notion import (
    _build_notion_headers,
    _notion_cache_key,
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


if __name__ == "__main__":
    unittest.main()
