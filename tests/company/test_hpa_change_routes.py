import logging
import unittest
from typing import Any
from unittest.mock import patch

from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeRequest,
    HpaChangeRoutesConfig,
    HpaChangeRoutesContext,
    HpaChangeRoutesDeps,
    HpaChangeSubmissionResult,
    HpaChangeSubmissionStatus,
    _handle_hpa_change_request,
    _looks_like_hpa_change_request,
    _download_slack_file,
    _validate_slack_file_url,
)


class _FakeSlackClient:
    def __init__(
        self,
        pages: dict[str, dict[str, Any]] | None = None,
        *,
        file_info: dict[str, dict[str, Any]] | None = None,
        fetch_error: Exception | None = None,
    ) -> None:
        self.pages = pages or {
            "": {
                "messages": [
                    {
                        "ts": "1.0",
                        "user": "UJUSTIN",
                        "text": "HPA CR 반영 요청",
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }
        self.file_info = file_info or {}
        self.fetch_error = fetch_error
        self.reply_calls: list[dict[str, Any]] = []
        self.files_info_calls: list[str] = []
        self.permalink_calls: list[dict[str, str]] = []
        self.token = "xoxb-test-token"

    def conversations_replies(self, **kwargs) -> dict[str, Any]:
        self.reply_calls.append(kwargs)
        if self.fetch_error is not None:
            raise self.fetch_error
        return self.pages[str(kwargs.get("cursor") or "")]

    def files_info(self, *, file: str) -> dict[str, Any]:
        self.files_info_calls.append(file)
        return {"file": self.file_info[file]}

    def chat_getPermalink(self, *, channel: str, message_ts: str) -> dict[str, str]:
        self.permalink_calls.append({"channel": channel, "message_ts": message_ts})
        return {"permalink": f"https://workspace.slack.com/archives/{channel}/p{message_ts}"}


class _FakeDownloadResponse:
    def __init__(
        self,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        chunks: list[bytes] | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {"Content-Length": "3"}
        self._chunks = chunks or [b"abc"]

    def __enter__(self) -> "_FakeDownloadResponse":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, *, chunk_size: int) -> list[bytes]:
        return self._chunks


def _payload() -> dict[str, Any]:
    return {
        "raw_text": "<@UBOXER> HPA CR 반영 요청",
        "text": "<@uboxer> hpa cr 반영 요청",
        "question": "HPA CR 반영 요청",
        "user_id": "UJUSTIN",
        "workspace_id": "TWORK",
        "channel_id": "CHPA",
        "current_ts": "2.0",
        "thread_ts": "1.0",
        "request_log": {},
    }


def _config(**overrides: Any) -> HpaChangeRoutesConfig:
    values: dict[str, Any] = {
        "enabled": True,
        "allowed_user_ids": frozenset({"UJUSTIN"}),
        "allowed_channel_ids": frozenset({"CHPA"}),
        "max_thread_chars": 60_000,
        "max_attachment_count": 3,
        "max_attachment_bytes": 100,
        "max_total_attachment_bytes": 180,
    }
    values.update(overrides)
    return HpaChangeRoutesConfig(**values)


def _context(
    client: _FakeSlackClient,
    replies: list[tuple[str, dict[str, Any]]],
    *,
    question: str = "HPA CR 반영 요청",
    payload: dict[str, Any] | None = None,
    user_id: str | None = "UJUSTIN",
    channel_id: str = "CHPA",
    current_ts: str = "2.0",
    thread_ts: str = "1.0",
    logger: logging.Logger | None = None,
) -> HpaChangeRoutesContext:
    actual_payload = payload or _payload()
    return HpaChangeRoutesContext(
        question=question,
        payload=actual_payload,  # type: ignore[arg-type]
        user_id=user_id,
        workspace_id="TWORK",
        channel_id=channel_id,
        current_ts=current_ts,
        thread_ts=thread_ts,
        reply=lambda text, **kwargs: replies.append((text, kwargs)),
        client=client,
        logger=logger or logging.getLogger(f"{__name__}.silent"),
    )


class HpaChangeRoutesTests(unittest.TestCase):
    def test_default_downloader_is_callable_with_route_arguments(self) -> None:
        # 운영 intake가 기본값을 사용할 때도 실제 파일 다운로더가 직접 호출돼야 한다.
        deps = HpaChangeRoutesDeps(
            submit_request=lambda request: HpaChangeSubmissionResult(
                HpaChangeSubmissionStatus.ACCEPTED,
            )
        )

        self.assertIs(deps.download_file, _download_slack_file)

    def test_trigger_requires_product_and_action_tokens(self) -> None:
        cases = {
            "HPA 반영 요청": True,
            "CR 코드 검토해줘": True,
            "내재화 구현 부탁": True,
            "hpa PR 만들어줘": True,
            "HPA 현재 상태 알려줘": False,
            "이 코드 반영해줘": False,
            "create 함수 구현": False,
            "critical 오류 검토": False,
        }

        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(_looks_like_hpa_change_request(question), expected)

    def test_unrelated_question_returns_false_without_side_effect(self) -> None:
        client = _FakeSlackClient()
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        handled = _handle_hpa_change_request(
            _context(client, replies, question="HPA 상태 알려줘"),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )

        self.assertFalse(handled)
        self.assertEqual(client.reply_calls, [])
        self.assertEqual(replies, [])
        self.assertEqual(submitted, [])

    def test_disabled_and_empty_allowlists_fail_closed(self) -> None:
        cases = (
            (_config(enabled=False), "꺼져"),
            (_config(allowed_user_ids=frozenset()), "설정이 없어"),
            (_config(allowed_channel_ids=frozenset()), "설정이 없어"),
        )

        for config, expected_text in cases:
            with self.subTest(expected_text=expected_text):
                client = _FakeSlackClient()
                replies: list[tuple[str, dict[str, Any]]] = []
                handled = _handle_hpa_change_request(
                    _context(client, replies),
                    config,
                    HpaChangeRoutesDeps(
                        submit_request=lambda request: HpaChangeSubmissionResult(
                            HpaChangeSubmissionStatus.ACCEPTED
                        )
                    ),
                )

                self.assertTrue(handled)
                self.assertIn(expected_text, replies[0][0])
                self.assertEqual(client.reply_calls, [])

    def test_rejects_unlisted_user_and_channel_before_fetch(self) -> None:
        cases = (
            ({"user_id": "UOTHER"}, "권한이 없어"),
            ({"channel_id": "COTHER"}, "이 채널에서는"),
        )

        for overrides, expected_text in cases:
            with self.subTest(overrides=overrides):
                client = _FakeSlackClient()
                replies: list[tuple[str, dict[str, Any]]] = []
                handled = _handle_hpa_change_request(
                    _context(client, replies, **overrides),
                    _config(),
                    HpaChangeRoutesDeps(
                        submit_request=lambda request: HpaChangeSubmissionResult(
                            HpaChangeSubmissionStatus.ACCEPTED
                        )
                    ),
                )

                self.assertTrue(handled)
                self.assertIn(expected_text, replies[0][0])
                self.assertEqual(client.reply_calls, [])

    def test_collects_all_pages_thread_and_allowed_attachments(self) -> None:
        first_content = b"export const classifier = 'gemini-2.5-flash';\n"
        second_content = b"export const HAND_RETRY_SUFFIX = 'retry';\n"
        pages = {
            "": {
                "messages": [
                    {
                        "ts": "1.0",
                        "user": "UJUSTIN",
                        "text": "HPA CR 반영 요청",
                        "files": [
                            {
                                "id": "F_TS",
                                "name": "scan-precrop.ts",
                                "mimetype": "text/plain",
                                "size": len(first_content),
                                "url_private_download": "https://files.slack.com/files-pri/F_TS/download",
                            },
                            {
                                "id": "F_IMAGE",
                                "name": "sample.png",
                                "mimetype": "image/png",
                                "size": 10,
                                "url_private_download": "https://files.slack.com/files-pri/F_IMAGE/download",
                            },
                        ],
                    }
                ],
                "response_metadata": {"next_cursor": "NEXT"},
            },
            "NEXT": {
                "messages": [
                    {
                        "ts": "1.1",
                        "user": "UHYUN",
                        "text": "현재 HPA는 Vercel이 아니야",
                        # 일부 Slack 이벤트는 files:read 조회 전에는 URL을 생략한다.
                        "files": [{"id": "F_TSX"}],
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            },
        }
        file_info = {
            "F_TSX": {
                "id": "F_TSX",
                "name": "hand-qa.tsx",
                "mimetype": "text/plain",
                "size": len(second_content),
                "url_private": "https://files-origin.slack.com/files-pri/F_TSX",
            }
        }
        client = _FakeSlackClient(pages, file_info=file_info)
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []
        downloads: list[str] = []
        content_by_name = {
            "scan-precrop.ts": first_content,
            "hand-qa.tsx": second_content,
        }

        def download_file(_client: Any, file_payload: dict[str, Any], _max_bytes: int) -> bytes:
            name = str(file_payload["name"])
            downloads.append(name)
            return content_by_name[name]

        def submit_request(request: HpaChangeRequest) -> HpaChangeSubmissionResult:
            submitted.append(request)
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionStatus.ACCEPTED,
                request_id="TASK-123",
            )

        payload = _payload()
        handled = _handle_hpa_change_request(
            _context(client, replies, payload=payload),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=submit_request,
                download_file=download_file,
            ),
        )

        self.assertTrue(handled)
        self.assertEqual(len(client.reply_calls), 2)
        self.assertNotIn("cursor", client.reply_calls[0])
        self.assertEqual(client.reply_calls[1]["cursor"], "NEXT")
        self.assertEqual(client.files_info_calls, ["F_TSX"])
        self.assertEqual(downloads, ["scan-precrop.ts", "hand-qa.tsx"])
        self.assertEqual(len(submitted), 1)
        request = submitted[0]
        self.assertEqual(request.request_key, "slack:TWORK:CHPA:2.0")
        self.assertEqual(request.event_ts, "2.0")
        self.assertEqual(
            request.thread_url,
            "https://workspace.slack.com/archives/CHPA/p1.0",
        )
        self.assertEqual(request.thread_message_count, 2)
        self.assertIn("HPA CR 반영 요청", request.thread_text)
        self.assertIn("현재 HPA는 Vercel이 아니야", request.thread_text)
        self.assertEqual([item.name for item in request.attachments], ["scan-precrop.ts", "hand-qa.tsx"])
        self.assertEqual(request.attachments[0].content, first_content.decode())
        self.assertIn("TASK-123", replies[0][0])
        self.assertIn("코드 첨부 2개", replies[0][0])
        self.assertTrue(payload["request_log"]["skip_persist"])
        self.assertEqual(payload["request_log"]["route_name"], "hpa_change_request")
        metadata = payload["request_log"]["metadata"]
        self.assertEqual(metadata["attachmentCount"], 2)
        self.assertNotIn("thread_text", metadata)
        self.assertNotIn("scan-precrop.ts", str(metadata))

    def test_rejects_thread_over_dispatch_character_limit(self) -> None:
        pages = {
            "": {
                "messages": [
                    {
                        "ts": "1.0",
                        "user": "UJUSTIN",
                        "text": "HPA 반영 요청 " + ("x" * 30),
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        handled = _handle_hpa_change_request(
            _context(_FakeSlackClient(pages), replies),
            _config(max_thread_chars=20),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )

        self.assertTrue(handled)
        self.assertIn("20자 제한을 초과", replies[0][0])
        self.assertEqual(submitted, [])

    def test_rejects_path_traversal_attachment(self) -> None:
        pages = {
            "": {
                "messages": [
                    {
                        "ts": "1.0",
                        "user": "UJUSTIN",
                        "text": "HPA 반영 요청",
                        "files": [
                            {
                                "id": "F_BAD",
                                "name": "../../secret.ts",
                                "size": 5,
                                "url_private": "https://files.slack.com/files-pri/F_BAD",
                            }
                        ],
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }
        client = _FakeSlackClient(pages)
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []
        downloaded: list[bool] = []

        handled = _handle_hpa_change_request(
            _context(client, replies),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request),  # type: ignore[func-returns-value]
                download_file=lambda *args: downloaded.append(True) or b"bad",
            ),
        )

        self.assertTrue(handled)
        self.assertIn("경로 문자가 포함", replies[0][0])
        self.assertEqual(downloaded, [])
        self.assertEqual(submitted, [])

    def test_enforces_attachment_count_and_declared_file_size_limits(self) -> None:
        files = [
            {
                "id": f"F{index}",
                "name": f"file-{index}.ts",
                "size": 3,
                "url_private": f"https://files.slack.com/files-pri/F{index}",
            }
            for index in range(2)
        ]
        pages = {
            "": {
                "messages": [{"ts": "1.0", "user": "UJUSTIN", "text": "CR 구현", "files": files}],
                "response_metadata": {"next_cursor": ""},
            }
        }

        for config, expected_text in (
            (_config(max_attachment_count=1), "최대 1개"),
            (_config(max_attachment_bytes=2), "최대 2바이트"),
        ):
            with self.subTest(expected_text=expected_text):
                replies: list[tuple[str, dict[str, Any]]] = []
                submitted: list[HpaChangeRequest] = []
                handled = _handle_hpa_change_request(
                    _context(_FakeSlackClient(pages), replies, question="CR 구현"),
                    config,
                    HpaChangeRoutesDeps(
                        submit_request=lambda request: submitted.append(request),  # type: ignore[func-returns-value]
                        download_file=lambda *args: b"abc",
                    ),
                )

                self.assertTrue(handled)
                self.assertIn(expected_text, replies[0][0])
                self.assertEqual(submitted, [])

    def test_enforces_actual_total_size_and_utf8_content(self) -> None:
        pages = {
            "": {
                "messages": [
                    {
                        "ts": "1.0",
                        "user": "UJUSTIN",
                        "text": "내재화 구현",
                        "files": [
                            {
                                "id": "F1",
                                "name": "one.ts",
                                "size": 0,
                                "url_private": "https://files.slack.com/files-pri/F1",
                            },
                            {
                                "id": "F2",
                                "name": "two.ts",
                                "size": 0,
                                "url_private": "https://files.slack.com/files-pri/F2",
                            },
                        ],
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }

        cases = (
            (
                {"one.ts": b"123", "two.ts": b"456"},
                _config(max_total_attachment_bytes=5),
                "전체는 최대 5바이트",
            ),
            ({"one.ts": b"\xff", "two.ts": b""}, _config(), "UTF-8"),
        )
        for contents, config, expected_text in cases:
            with self.subTest(expected_text=expected_text):
                replies: list[tuple[str, dict[str, Any]]] = []
                submitted: list[HpaChangeRequest] = []

                handled = _handle_hpa_change_request(
                    _context(_FakeSlackClient(pages), replies, question="내재화 구현"),
                    config,
                    HpaChangeRoutesDeps(
                        submit_request=lambda request: submitted.append(request),  # type: ignore[func-returns-value]
                        download_file=lambda _client, item, _limit: contents[str(item["name"])],
                    ),
                )

                self.assertTrue(handled)
                self.assertIn(expected_text, replies[0][0])
                self.assertEqual(submitted, [])

    def test_same_event_timestamp_uses_submit_result_for_duplicate(self) -> None:
        client = _FakeSlackClient()
        submitted_keys: set[str] = set()
        requests: list[HpaChangeRequest] = []

        def submit_request(request: HpaChangeRequest) -> HpaChangeSubmissionResult:
            requests.append(request)
            if request.request_key in submitted_keys:
                return HpaChangeSubmissionResult(
                    HpaChangeSubmissionStatus.DUPLICATE,
                    request_id="TASK-SAME",
                )
            submitted_keys.add(request.request_key)
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionStatus.ACCEPTED,
                request_id="TASK-SAME",
            )

        replies: list[tuple[str, dict[str, Any]]] = []
        for _ in range(2):
            handled = _handle_hpa_change_request(
                _context(client, replies, current_ts="9.9"),
                _config(),
                HpaChangeRoutesDeps(submit_request=submit_request),
            )
            self.assertTrue(handled)

        self.assertEqual([request.request_key for request in requests], [
            "slack:TWORK:CHPA:9.9",
            "slack:TWORK:CHPA:9.9",
        ])
        self.assertIn("요청 접수", replies[0][0])
        self.assertIn("이미 접수된", replies[1][0])

    def test_rejected_submission_uses_safe_callback_result(self) -> None:
        client = _FakeSlackClient()
        replies: list[tuple[str, dict[str, Any]]] = []

        handled = _handle_hpa_change_request(
            _context(client, replies),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=lambda request: HpaChangeSubmissionResult(
                    HpaChangeSubmissionStatus.REJECTED,
                    request_id="TASK-FAIL",
                    user_message="현재 작업 큐가 가득 찼어\x00",
                )
            ),
        )

        self.assertTrue(handled)
        self.assertIn("접수하지 못했어", replies[0][0])
        self.assertIn("현재 작업 큐가 가득 찼어", replies[0][0])
        self.assertNotIn("\x00", replies[0][0])

    def test_fetch_and_submit_errors_do_not_log_request_or_file_content(self) -> None:
        logger = logging.getLogger(f"{__name__}.no_raw")
        logger.setLevel(logging.WARNING)
        secret = "VERY_SECRET_SOURCE_TEXT"

        with self.assertLogs(logger, level="WARNING") as captured:
            replies: list[tuple[str, dict[str, Any]]] = []
            _handle_hpa_change_request(
                _context(
                    _FakeSlackClient(fetch_error=RuntimeError(secret)),
                    replies,
                    logger=logger,
                ),
                _config(),
                HpaChangeRoutesDeps(
                    submit_request=lambda request: HpaChangeSubmissionResult(
                        HpaChangeSubmissionStatus.ACCEPTED
                    )
                ),
            )
        self.assertNotIn(secret, "\n".join(captured.output))
        self.assertIn("history 권한", replies[0][0])

        with self.assertLogs(logger, level="WARNING") as captured:
            replies = []

            def fail_submit(_request: HpaChangeRequest) -> HpaChangeSubmissionResult:
                raise RuntimeError(secret)

            _handle_hpa_change_request(
                _context(_FakeSlackClient(), replies, logger=logger),
                _config(),
                HpaChangeRoutesDeps(submit_request=fail_submit),
            )
        self.assertNotIn(secret, "\n".join(captured.output))
        self.assertIn("작업 큐에 접수하지 못했어", replies[0][0])

    def test_requires_stable_slack_event_identity(self) -> None:
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        handled = _handle_hpa_change_request(
            _context(_FakeSlackClient(), replies, current_ts=""),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )

        self.assertTrue(handled)
        self.assertIn("식별 정보가 부족", replies[0][0])
        self.assertEqual(submitted, [])

    @patch("boxer_company_adapter_slack.hpa_change_routes.requests.get")
    def test_downloads_slack_file_without_forwarding_auth_on_redirect(
        self,
        get_request: Any,
    ) -> None:
        # Slack 파일 endpoint의 redirect도 허용 호스트 안에서만 따라가고 토큰은 첫 요청에만 보낸다.
        get_request.side_effect = [
            _FakeDownloadResponse(
                status_code=302,
                headers={"Location": "https://files-origin.slack.com/files-pri/F_TS/download"},
                chunks=[],
            ),
            _FakeDownloadResponse(),
        ]

        content = _download_slack_file(
            _FakeSlackClient(),
            {"url_private_download": "https://files.slack.com/files-pri/F_TS/download"},
            10,
        )

        self.assertEqual(content, b"abc")
        self.assertEqual(get_request.call_args_list[0].kwargs, {
            "headers": {"Authorization": "Bearer xoxb-test-token"},
            "stream": True,
            "timeout": 10,
            "allow_redirects": False,
        })
        self.assertEqual(get_request.call_args_list[1].kwargs, {
            "headers": {},
            "stream": True,
            "timeout": 10,
            "allow_redirects": False,
        })
        self.assertEqual(get_request.call_args_list[0].args, (
            "https://files.slack.com/files-pri/F_TS/download",
        ))
        self.assertEqual(get_request.call_args_list[1].args, (
            "https://files-origin.slack.com/files-pri/F_TS/download",
        ))

    def test_file_url_validation_allows_only_slack_file_hosts(self) -> None:
        for url in (
            "https://files.slack.com/files-pri/F1",
            "https://files-origin.slack.com/files-pri/F1",
            "https://slack-files.com/T1-F1",
            "https://cdn.slack-files.com/T1-F1",
        ):
            with self.subTest(url=url):
                _validate_slack_file_url(url)

        for url in (
            "http://files.slack.com/files-pri/F1",
            "https://example.com/file.ts",
            "https://files.slack.com.evil.example/file.ts",
            "https://files.slack.com:444/file.ts",
            "https://files.slack.com:bad/file.ts",
            "https://user@files.slack.com/file.ts",
            "file:///etc/passwd",
        ):
            with self.subTest(url=url):
                with self.assertRaises(RuntimeError):
                    _validate_slack_file_url(url)


if __name__ == "__main__":
    unittest.main()
