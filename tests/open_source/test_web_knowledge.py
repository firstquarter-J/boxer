import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from boxer.retrieval import (
    KnowledgeDocument,
    KnowledgeSearchResult,
    MarkdownKnowledgeSource,
    NotionKnowledgeSource,
)


class MarkdownKnowledgeSourceTests(unittest.TestCase):
    def test_load_documents_extracts_title_and_content(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "refund-policy.md").write_text(
                "# Refund policy\n\nRefund requests are reviewed within 3 business days.\n",
                encoding="utf-8",
            )

            source = MarkdownKnowledgeSource(root)
            documents = source.load_documents()

        self.assertEqual(len(documents), 1)
        self.assertEqual(
            documents[0],
            KnowledgeDocument(
                id="markdown:refund-policy.md",
                title="Refund policy",
                content="Refund requests are reviewed within 3 business days.",
                source_type="markdown",
                source_uri="refund-policy.md",
                metadata={"path": "refund-policy.md"},
            ),
        )

    def test_search_returns_ranked_results(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            # 질의와 더 많이 겹치는 문서가 앞에 와야 widget 답변 source ref도 안정적으로 재현된다.
            (root / "refund.md").write_text(
                "# Refund policy\n\nRefund requests are reviewed within 3 business days.\n",
                encoding="utf-8",
            )
            (root / "password.md").write_text(
                "# Reset password\n\nUse the reset link from the login page.\n",
                encoding="utf-8",
            )

            source = MarkdownKnowledgeSource(root)
            results = source.search("refund policy", limit=5)

        self.assertGreaterEqual(len(results), 1)
        self.assertIsInstance(results[0], KnowledgeSearchResult)
        self.assertEqual(results[0].document.title, "Refund policy")
        self.assertGreater(results[0].score, 0)

    def test_search_supports_korean_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "refund-ko.md").write_text(
                "# 환불 정책\n\n환불은 영업일 3일 안에 처리됩니다.\n",
                encoding="utf-8",
            )

            source = MarkdownKnowledgeSource(root)
            results = source.search("환불은 얼마나 걸려?", limit=5)

        self.assertGreaterEqual(len(results), 1)
        self.assertEqual(results[0].document.title, "환불 정책")
        self.assertGreater(results[0].score, 0)


class NotionKnowledgeSourceTests(unittest.TestCase):
    @patch("boxer.retrieval.knowledge._load_notion_page_content_cached")
    def test_load_documents_maps_notion_payload(self, mocked_loader) -> None:
        mocked_loader.return_value = {
            "pageId": "1234567890abcdef1234567890abcdef",
            "title": "FAQ",
            "url": "https://notion.so/example",
            "plainText": "FAQ body",
        }

        source = NotionKnowledgeSource(["1234567890abcdef1234567890abcdef"])
        documents = source.load_documents()

        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0].id, "notion:1234567890abcdef1234567890abcdef")
        self.assertEqual(documents[0].title, "FAQ")
        self.assertEqual(documents[0].content, "FAQ body")
        self.assertEqual(documents[0].source_uri, "https://notion.so/example")

    @patch("boxer.retrieval.knowledge._load_notion_page_content_cached")
    def test_search_uses_loaded_documents(self, mocked_loader) -> None:
        mocked_loader.return_value = {
            "pageId": "page01page01page01page01page01aa",
            "title": "Shipping FAQ",
            "url": "https://notion.so/shipping",
            "plainText": "Shipping takes two business days for domestic orders.",
        }

        source = NotionKnowledgeSource(["page01page01page01page01page01aa"])
        results = source.search("shipping business days", limit=3)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].document.title, "Shipping FAQ")
        self.assertGreater(results[0].score, 0)


if __name__ == "__main__":
    unittest.main()
