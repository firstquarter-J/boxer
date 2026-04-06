import unittest

from boxer_company_adapter_slack.barcode_logs import (
    _needs_barcode_log_fallback,
    _split_barcode_log_reply,
)


class BarcodeLogHelperTests(unittest.TestCase):
    def test_split_barcode_log_reply_preserves_scanned_block_context(self) -> None:
        reply_text = "\n".join(
            [
                "*로그 분석 결과*",
                "",
                "• scanned 이벤트:",
                "```",
                "09:00:01 scanned foo",
                "09:00:02 scanned bar",
                "09:00:03 scanned baz",
                "```",
            ]
        )

        chunks = _split_barcode_log_reply(reply_text, max_chars=60)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(chunks[0].startswith("*로그 분석 결과*"))
        self.assertTrue(any("• scanned 이벤트 (계속)" in chunk for chunk in chunks[1:]))

    def test_needs_barcode_log_fallback_when_required_metadata_is_missing(self) -> None:
        fallback_text = "\n".join(
            [
                "*로그 분석 결과*",
                "• 바코드: `123`",
                "• 날짜: `2026-04-06`",
                "• 매핑 장비: `box-a`",
            ]
        )

        self.assertTrue(
            _needs_barcode_log_fallback(
                "요약만 있는 답변",
                fallback_text,
                "barcode log analysis",
            )
        )
        self.assertFalse(
            _needs_barcode_log_fallback(
                fallback_text,
                fallback_text,
                "barcode log analysis",
            )
        )


if __name__ == "__main__":
    unittest.main()
