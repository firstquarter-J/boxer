import logging
import unittest
import uuid
from typing import Any
from unittest.mock import patch

from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeRequest,
    HpaChangeRoutesConfig,
    HpaChangeRoutesContext,
    HpaChangeRoutesDeps,
    HpaChangeSubmissionResult,
    HpaChangeSubmissionStatus,
    HpaChangeThreadLookupResult,
    HpaChangeThreadLookupState,
    _handle_hpa_change_request,
    _looks_like_hpa_clarification_followup,
    _looks_like_hpa_change_request,
    _download_slack_file,
    _select_clarification_followup_messages,
    _validate_slack_file_url,
)


class _FakeSlackClient:
    def __init__(
        self,
        pages: dict[str, dict[str, Any]] | None = None,
        *,
        file_info: dict[str, dict[str, Any]] | None = None,
        permalinks: dict[str, str] | None = None,
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
        self.permalinks = permalinks or {}
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
        if message_ts in self.permalinks:
            return {"permalink": self.permalinks[message_ts]}
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
            "요구사항 검토\nBasic과 Bonus 발송 버튼 분리": True,
            "요구 사항 검토: 이미지 다운로드 형식 선택 추가": True,
            "요구사항 검토 해줘\n- Bonus 생성 전 손 QA 추가": True,
            "HPA 현재 상태 알려줘": False,
            "이 코드 반영해줘": False,
            "create 함수 구현": False,
            "critical 오류 검토": False,
            "어제 요구사항 검토 결과 알려줘": False,
            "요구사항 검토 결과만 알려줘": False,
        }

        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(_looks_like_hpa_change_request(question), expected)

    def test_clarification_followup_trigger_is_narrow(self) -> None:
        cases = {
            "진행해": True,
            "HPA 진행해": True,
            "구현 진행해주세요": True,
            "질문1 답변: Basic과 Bonus를 분리": True,
            "질문 2 답변: 생성 실패 처리": True,
            "진행 상황 알려줘": False,
            "이 작업 진행해도 돼?": False,
            "회의 진행해": False,
            "답변을 확인해줘": False,
        }

        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(
                    _looks_like_hpa_clarification_followup(question),
                    expected,
                )

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

    def test_plain_continue_does_not_capture_thread_without_pending_clarification(self) -> None:
        client = _FakeSlackClient()
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        handled = _handle_hpa_change_request(
            _context(client, replies, question="진행해"),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request),  # type: ignore[func-returns-value]
                lookup_thread_job=lambda *_args: HpaChangeThreadLookupResult(
                    HpaChangeThreadLookupState.NONE
                ),
            ),
        )

        self.assertFalse(handled)
        self.assertEqual(client.reply_calls, [])
        self.assertEqual(replies, [])
        self.assertEqual(submitted, [])

    def test_active_thread_consumes_explicit_continue_without_new_submission(self) -> None:
        client = _FakeSlackClient()
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        handled = _handle_hpa_change_request(
            _context(client, replies, question="HPA 구현 진행해"),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request),  # type: ignore[func-returns-value]
                lookup_thread_job=lambda *_args: HpaChangeThreadLookupResult(
                    HpaChangeThreadLookupState.ACTIVE,
                    request_id="TASK-ACTIVE",
                    job_status="running",
                    event_ts="1.5",
                ),
            ),
        )

        self.assertTrue(handled)
        self.assertEqual(client.reply_calls, [])
        self.assertEqual(submitted, [])
        self.assertIn("TASK-ACTIVE", replies[0][0])
        self.assertIn("running", replies[0][0])
        self.assertIn("새 작업은 만들지 않았어", replies[0][0])

    def test_thread_lookup_error_fails_closed_without_fresh_hpa_request(self) -> None:
        client = _FakeSlackClient()
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        handled = _handle_hpa_change_request(
            _context(client, replies, question="HPA 구현 진행해"),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request),  # type: ignore[func-returns-value]
                lookup_thread_job=lambda *_args: (_ for _ in ()).throw(
                    RuntimeError("db unavailable")
                ),
            ),
        )

        self.assertTrue(handled)
        self.assertEqual(client.reply_calls, [])
        self.assertEqual(submitted, [])
        self.assertIn("조회 오류", replies[0][0])
        self.assertIn("새 작업으로 우회하지 않았어", replies[0][0])

    def test_same_event_retry_reports_pending_job_without_grandchild(self) -> None:
        client = _FakeSlackClient()
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        handled = _handle_hpa_change_request(
            _context(client, replies, question="진행해"),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request),  # type: ignore[func-returns-value]
                lookup_thread_job=lambda *_args: HpaChangeThreadLookupResult(
                    HpaChangeThreadLookupState.NEEDS_CLARIFICATION,
                    request_id="TASK-RETRY",
                    job_status="needs_clarification",
                    event_ts="2.0",
                    current_event=True,
                ),
            ),
        )

        self.assertTrue(handled)
        self.assertEqual(client.reply_calls, [])
        self.assertEqual(submitted, [])
        self.assertIn("TASK-RETRY", replies[0][0])
        self.assertIn("다시 만들지 않았어", replies[0][0])

    def test_pending_clarification_followup_collects_current_thread_for_new_task(self) -> None:
        pages = {
            "": {
                "messages": [
                    {
                        "ts": "1.0",
                        "user": "UJUSTIN",
                        "text": "HPA 요청사항 검토",
                    },
                    {
                        "ts": "1.1",
                        "bot_id": "BBOXER",
                        "text": "질문 1: 발송 범위를 결정해줘",
                    },
                    {
                        "ts": "1.2",
                        "user": "UJUSTIN",
                        "text": "질문1 답변: Basic과 Bonus 각각 독립 버튼",
                    },
                    {
                        "ts": "1.3",
                        "user": "UJUSTIN",
                        "text": "<@UBOXER> 진행해",
                    },
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }
        client = _FakeSlackClient(pages)
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []
        finder_calls: list[tuple[str, str, str, str]] = []

        def lookup_thread_job(*args: str) -> HpaChangeThreadLookupResult:
            finder_calls.append(args)
            return HpaChangeThreadLookupResult(
                HpaChangeThreadLookupState.NEEDS_CLARIFICATION,
                request_id="TASK-PARENT",
                job_status="needs_clarification",
                event_ts="1.0",
            )

        def submit_request(request: HpaChangeRequest) -> HpaChangeSubmissionResult:
            submitted.append(request)
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionStatus.ACCEPTED,
                request_id="TASK-CHILD",
            )

        handled = _handle_hpa_change_request(
            _context(
                client,
                replies,
                question="진행해",
                current_ts="1.3",
                thread_ts="1.0",
            ),
            _config(),
            HpaChangeRoutesDeps(
                submit_request=submit_request,
                lookup_thread_job=lookup_thread_job,
            ),
        )

        self.assertTrue(handled)
        self.assertEqual(finder_calls, [("TWORK", "CHPA", "1.0", "1.3")])
        self.assertEqual(len(submitted), 1)
        request = submitted[0]
        self.assertEqual(request.continuation_of_request_id, "TASK-PARENT")
        self.assertEqual(request.selection_mode, "clarification_followup")
        self.assertIn("Basic과 Bonus 각각 독립 버튼", request.thread_text)
        self.assertIn("<@UBOXER> 진행해", request.thread_text)
        self.assertNotIn("발송 범위를 결정해줘", request.thread_text)
        self.assertNotIn("HPA 요청사항 검토", request.thread_text)
        self.assertIn("TASK-PARENT", replies[0][0])
        self.assertIn("TASK-CHILD", replies[0][0])
        self.assertIn("추가 답변 재접수", replies[0][0])

    def test_followup_message_selection_keeps_only_delta_after_parent_event(self) -> None:
        selected = _select_clarification_followup_messages(
            [
                {"ts": "1.0", "user": "UJUSTIN", "text": "thread root"},
                {"ts": "1.2", "user": "UJUSTIN", "text": "이전 답변"},
                {"ts": "1.4", "bot_id": "BBOXER", "text": "추가 질문"},
                {"ts": "1.5", "user": "UJUSTIN", "text": "새 답변"},
                {"ts": "1.6", "user": "UOTHER", "text": "허용되지 않은 답변"},
            ],
            thread_ts="1.0",
            allowed_user_ids={"UJUSTIN"},
            after_event_ts="1.3",
        )

        self.assertEqual([item["text"] for item in selected], ["새 답변"])

    def test_direct_requirements_review_collects_current_message_without_link(self) -> None:
        requirement = "Basic과 Bonus 발송 버튼을 분리하고 Basic만 발송할 수 있게 해줘"
        pages = {
            "": {
                "messages": [
                    {
                        "ts": "2.0",
                        "user": "UJUSTIN",
                        "text": f"<@UBOXER> 요구사항 검토\n\n- {requirement}",
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }
        client = _FakeSlackClient(pages)
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        def submit_request(request: HpaChangeRequest) -> HpaChangeSubmissionResult:
            submitted.append(request)
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionStatus.ACCEPTED,
                request_id="TASK-DIRECT",
            )

        handled = _handle_hpa_change_request(
            _context(
                client,
                replies,
                question=f"요구사항 검토\n\n- {requirement}",
                current_ts="2.0",
                thread_ts="2.0",
            ),
            _config(),
            HpaChangeRoutesDeps(submit_request=submit_request),
        )

        self.assertTrue(handled)
        self.assertEqual(len(submitted), 1)
        request = submitted[0]
        self.assertEqual(request.selection_mode, "thread")
        self.assertEqual(request.requester_user_id, "UJUSTIN")
        self.assertEqual(request.source_channel_id, "CHPA")
        self.assertEqual(request.thread_message_count, 1)
        self.assertIn(requirement, request.thread_text)
        self.assertNotIn("slack.com", request.question)
        self.assertIn("TASK-DIRECT", replies[0][0])
        self.assertIn("스레드 1개", replies[0][0])

    def test_direct_requirements_review_keeps_user_and_channel_allowlists(self) -> None:
        question = "요구사항 검토\n- Basic과 Bonus 발송 버튼 분리"
        cases = (
            ({"user_id": "UOTHER"}, "권한이 없어"),
            ({"channel_id": "COTHER"}, "이 채널에서는"),
        )

        for overrides, expected_text in cases:
            with self.subTest(overrides=overrides):
                client = _FakeSlackClient()
                replies: list[tuple[str, dict[str, Any]]] = []
                submitted: list[HpaChangeRequest] = []

                handled = _handle_hpa_change_request(
                    _context(client, replies, question=question, **overrides),
                    _config(),
                    HpaChangeRoutesDeps(
                        submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
                    ),
                )

                self.assertTrue(handled)
                self.assertIn(expected_text, replies[0][0])
                self.assertEqual(client.reply_calls, [])
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

    def test_linked_reply_selects_only_that_message_and_keeps_command_thread(self) -> None:
        root_ts = "1720580000.000001"
        target_ts = "1720580000.000023"
        command_ts = "1800000000.000001"
        permalink = (
            "https://workspace.slack.com/archives/CSOURCE/p1720580000000023"
            "?thread_ts=1720580000.000001&cid=CSOURCE"
        )
        target_content = b"export const delivery = 'basic-only';\n"
        pages = {
            "": {
                "messages": [
                    {
                        "ts": root_ts,
                        "user": "UOTHER",
                        "text": "Bonus 프롬프트 변경 요청",
                        "files": [
                            {
                                "id": "F_ROOT",
                                "name": "root.ts",
                                "mimetype": "text/plain",
                                "size": 3,
                                "url_private": "https://files.slack.com/files-pri/F_ROOT",
                            }
                        ],
                    },
                    {
                        "ts": target_ts,
                        "thread_ts": root_ts,
                        "user": "UJUSTIN",
                        "text": "Basic만 발송할 수 있게 분리 요청",
                        "files": [
                            {
                                "id": "F_TARGET",
                                "name": "basic-only.ts",
                                "mimetype": "text/plain",
                                "size": len(target_content),
                                "url_private": "https://files.slack.com/files-pri/F_TARGET",
                            }
                        ],
                    },
                    {
                        "ts": "1720580000.000024",
                        "thread_ts": root_ts,
                        "user": "UOTHER",
                        "text": "Motion 변경 요청",
                    },
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }
        client = _FakeSlackClient(
            pages,
            permalinks={target_ts: permalink},
        )
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []
        downloaded: list[str] = []

        def download_file(_client: Any, item: dict[str, Any], _limit: int) -> bytes:
            downloaded.append(str(item["name"]))
            return target_content

        handled = _handle_hpa_change_request(
            _context(
                client,
                replies,
                question=f"HPA 변경 요청 검토 이 댓글만 <{permalink}|저스틴 댓글>",
                user_id="UHYUN",
                channel_id="CHPA",
                current_ts=command_ts,
                thread_ts=command_ts,
            ),
            _config(
                allowed_user_ids=frozenset({"UHYUN", "UJUSTIN"}),
                allowed_channel_ids=frozenset({"CHPA", "CSOURCE"}),
            ),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)
                or HpaChangeSubmissionResult(
                    HpaChangeSubmissionStatus.ACCEPTED,
                    request_id="TASK-LINKED",
                ),
                download_file=download_file,
            ),
        )

        self.assertTrue(handled)
        self.assertEqual(len(submitted), 1)
        request = submitted[0]
        # 링크는 입력만 고르고 응답 목적지는 새로 멘션한 글의 thread로 유지한다.
        self.assertEqual(request.channel_id, "CHPA")
        self.assertEqual(request.thread_ts, command_ts)
        self.assertEqual(request.requester_user_id, "UJUSTIN")
        self.assertEqual(request.initiator_user_id, "UHYUN")
        self.assertEqual(request.source_channel_id, "CSOURCE")
        self.assertEqual(request.source_message_ts, target_ts)
        self.assertEqual(request.selection_mode, "linked_message")
        self.assertEqual(request.thread_url, permalink)
        self.assertIn("/archives/CHPA/", request.response_thread_url)
        self.assertIn("Basic만 발송", request.thread_text)
        self.assertNotIn("Bonus 프롬프트", request.thread_text)
        self.assertNotIn("Motion 변경", request.thread_text)
        self.assertEqual([item.name for item in request.attachments], ["basic-only.ts"])
        self.assertEqual(downloaded, ["basic-only.ts"])
        self.assertEqual(client.reply_calls[0]["channel"], "CSOURCE")
        self.assertEqual(client.reply_calls[0]["ts"], root_ts)
        self.assertEqual(
            client.permalink_calls,
            [
                {"channel": "CSOURCE", "message_ts": target_ts},
                {"channel": "CHPA", "message_ts": command_ts},
            ],
        )
        self.assertIn("선택 댓글 1개", replies[0][0])

    def test_linked_reply_rejects_unallowed_channel_before_fetch(self) -> None:
        permalink = "https://workspace.slack.com/archives/COTHER/p1720580000000023"
        client = _FakeSlackClient()
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        handled = _handle_hpa_change_request(
            _context(
                client,
                replies,
                question=f"HPA 변경 요청 검토 {permalink}",
                user_id="UHYUN",
            ),
            _config(allowed_user_ids=frozenset({"UHYUN"})),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )

        self.assertTrue(handled)
        self.assertIn("허용 채널이 아니야", replies[0][0])
        self.assertEqual(client.reply_calls, [])
        self.assertEqual(submitted, [])

    def test_malformed_permalink_does_not_fall_back_to_full_thread(self) -> None:
        malformed = "https://workspace.slack.com/archives/CSOURCE/not-a-message"
        client = _FakeSlackClient()
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        _handle_hpa_change_request(
            _context(
                client,
                replies,
                question=f"HPA 변경 요청 검토 {malformed}",
                user_id="UHYUN",
            ),
            _config(
                allowed_user_ids=frozenset({"UHYUN"}),
                allowed_channel_ids=frozenset({"CHPA", "CSOURCE"}),
            ),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )

        self.assertIn("채널과 메시지를 확인하지 못했어", replies[-1][0])
        self.assertEqual(client.reply_calls, [])
        self.assertEqual(submitted, [])

        # URL parser 자체가 실패하는 링크도 일반 스레드 요청으로 처리하지 않는다.
        replies = []
        _handle_hpa_change_request(
            _context(
                client,
                replies,
                question=(
                    "HPA 변경 요청 검토 "
                    "https://[broken.slack.com/archives/CSOURCE/p1720580000000023"
                ),
                user_id="UHYUN",
            ),
            _config(
                allowed_user_ids=frozenset({"UHYUN"}),
                allowed_channel_ids=frozenset({"CHPA", "CSOURCE"}),
            ),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )

        self.assertIn("링크 형식이 올바르지 않아", replies[-1][0])
        self.assertEqual(client.reply_calls, [])
        self.assertEqual(submitted, [])

    def test_linked_reply_rejects_multiple_links_and_workspace_mismatch(self) -> None:
        root_ts = "1720580000.000001"
        target_ts = "1720580000.000023"
        first = (
            "https://workspace.slack.com/archives/CSOURCE/p1720580000000023"
            "?thread_ts=1720580000.000001&cid=CSOURCE"
        )
        second = "https://workspace.slack.com/archives/CSOURCE/p1720580000000024"

        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []
        multiple_client = _FakeSlackClient()
        _handle_hpa_change_request(
            _context(
                multiple_client,
                replies,
                question=f"HPA 변경 요청 검토 {first} {second}",
                user_id="UHYUN",
            ),
            _config(
                allowed_user_ids=frozenset({"UHYUN"}),
                allowed_channel_ids=frozenset({"CHPA", "CSOURCE"}),
            ),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )
        self.assertIn("하나만 지정", replies[-1][0])
        self.assertEqual(multiple_client.reply_calls, [])

        # workspace만 다른 같은 channel/message 링크도 하나로 합치지 않는다.
        cross_workspace_client = _FakeSlackClient()
        replies = []
        other_workspace_same_target = first.replace(
            "workspace.slack.com",
            "other.slack.com",
        )
        _handle_hpa_change_request(
            _context(
                cross_workspace_client,
                replies,
                question=(
                    f"HPA 변경 요청 검토 {first} {other_workspace_same_target}"
                ),
                user_id="UHYUN",
            ),
            _config(
                allowed_user_ids=frozenset({"UHYUN"}),
                allowed_channel_ids=frozenset({"CHPA", "CSOURCE"}),
            ),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )
        self.assertIn("하나만 지정", replies[-1][0])
        self.assertEqual(cross_workspace_client.reply_calls, [])

        mismatch_pages = {
            "": {
                "messages": [
                    {
                        "ts": root_ts,
                        "user": "UOTHER",
                        "text": "root",
                    },
                    {
                        "ts": target_ts,
                        "thread_ts": root_ts,
                        "user": "UJUSTIN",
                        "text": "Basic 분리 요청",
                    },
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }
        mismatch_permalink = first.replace("workspace.slack.com", "other.slack.com")
        mismatch_client = _FakeSlackClient(
            mismatch_pages,
            permalinks={target_ts: mismatch_permalink},
        )
        replies = []
        _handle_hpa_change_request(
            _context(
                mismatch_client,
                replies,
                question=f"HPA 변경 요청 검토 {first}",
                user_id="UHYUN",
            ),
            _config(
                allowed_user_ids=frozenset({"UHYUN"}),
                allowed_channel_ids=frozenset({"CHPA", "CSOURCE"}),
            ),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )
        self.assertIn("현재 워크스페이스 메시지와 일치하지 않아", replies[-1][0])
        self.assertEqual(submitted, [])

    def test_linked_reply_rejects_unallowed_human_author(self) -> None:
        target_ts = "1720580000.000023"
        permalink = "https://workspace.slack.com/archives/CSOURCE/p1720580000000023"
        pages = {
            "": {
                "messages": [
                    {
                        "ts": target_ts,
                        "user": "UOTHER",
                        "text": "HPA 변경 요청",
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }
        client = _FakeSlackClient(pages, permalinks={target_ts: permalink})
        replies: list[tuple[str, dict[str, Any]]] = []
        submitted: list[HpaChangeRequest] = []

        _handle_hpa_change_request(
            _context(
                client,
                replies,
                question=f"HPA 변경 요청 검토 {permalink}",
                user_id="UHYUN",
            ),
            _config(
                allowed_user_ids=frozenset({"UHYUN", "UJUSTIN"}),
                allowed_channel_ids=frozenset({"CHPA", "CSOURCE"}),
            ),
            HpaChangeRoutesDeps(
                submit_request=lambda request: submitted.append(request)  # type: ignore[func-returns-value]
            ),
        )

        self.assertIn("댓글 작성자는", replies[-1][0])
        self.assertIn("허용 사용자가 아니야", replies[-1][0])
        self.assertEqual(submitted, [])

    def test_linked_reply_rejects_bot_author(self) -> None:
        target_ts = "1720580000.000023"
        permalink = "https://workspace.slack.com/archives/CSOURCE/p1720580000000023"
        pages = {
            "": {
                "messages": [
                    {
                        "ts": target_ts,
                        "user": "UJUSTIN",
                        "bot_id": "BBOXER",
                        "subtype": "bot_message",
                        "text": "자동 생성 요청",
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }
        }
        client = _FakeSlackClient(pages, permalinks={target_ts: permalink})
        replies: list[tuple[str, dict[str, Any]]] = []

        _handle_hpa_change_request(
            _context(
                client,
                replies,
                question=f"HPA 변경 요청 검토 {permalink}",
                user_id="UHYUN",
            ),
            _config(
                allowed_user_ids=frozenset({"UHYUN"}),
                allowed_channel_ids=frozenset({"CHPA", "CSOURCE"}),
            ),
            HpaChangeRoutesDeps(
                submit_request=lambda request: HpaChangeSubmissionResult(
                    HpaChangeSubmissionStatus.ACCEPTED
                )
            ),
        )

        self.assertIn("댓글 작성자를 확인하지 못했어", replies[-1][0])

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
        first_client_msg_id = replies[0][1]["client_msg_id"]
        self.assertEqual(replies[1][1]["client_msg_id"], first_client_msg_id)
        self.assertEqual(str(uuid.UUID(first_client_msg_id)), first_client_msg_id)

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
