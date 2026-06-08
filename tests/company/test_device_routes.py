import logging
import unittest
from unittest.mock import patch

from boxer_company_adapter_slack.device_routes import (
    DeviceRoutesContext,
    DeviceRoutesDeps,
    _handle_device_routes,
)


def _payload() -> dict[str, object]:
    return {
        "text": "핑",
        "question": "핑",
        "user_id": "U123",
        "workspace_id": "W123",
        "channel_id": "C123",
        "current_ts": "1.1",
        "thread_ts": "1.0",
    }


class DeviceRouteHandlerTests(unittest.TestCase):
    def test_monthly_streaming_restore_request_bypasses_device_recovery(self) -> None:
        replies: list[str] = []

        deps = DeviceRoutesDeps(
            get_s3_client=lambda: self.fail("device file lookup should not handle monthly restore"),
            get_recordings_context=lambda: self.fail(
                "recordings lookup should not handle monthly restore"
            ),
            has_recordings_device_mapping=lambda context: False,
            send_dm_message=lambda user_id, text: False,
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
            reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
        )

        # 월 단위 MDA 복원은 "영상 복구" 문구가 있어도 장비 파일 복구 라우터가 선점하지 않는다.
        handled = _handle_device_routes(
            DeviceRoutesContext(
                question="35033165423 2024년 4월 영상 복구",
                barcode="35033165423",
                phase2_hospital_name=None,
                phase2_room_name=None,
                payload=_payload(),  # type: ignore[arg-type]
                user_id="U123",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                reply=lambda text, **kwargs: replies.append(text),
                client=None,
                logger=logging.getLogger(__name__),
            ),
            deps,
        )

        self.assertFalse(handled)
        self.assertEqual(replies, [])

    def test_download_without_barcode_asks_for_barcode_before_lookup(self) -> None:
        replies: list[str] = []

        deps = DeviceRoutesDeps(
            get_s3_client=lambda: self.fail("S3 lookup should not run without barcode"),
            get_recordings_context=lambda: self.fail(
                "recordings lookup should not run without barcode"
            ),
            has_recordings_device_mapping=lambda context: False,
            send_dm_message=lambda user_id, text: False,
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
            reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
        )

        # 다운로드는 병원/병실/날짜만으로 세션을 확정하지 않는다.
        handled = _handle_device_routes(
            DeviceRoutesContext(
                question="미래로병원(부산) 5진료실 2026-04-28 영상 다운로드",
                barcode=None,
                phase2_hospital_name="미래로병원(부산)",
                phase2_room_name="5진료실",
                payload=_payload(),  # type: ignore[arg-type]
                user_id="U123",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                reply=lambda text, **kwargs: replies.append(text),
                client=None,
                logger=logging.getLogger(__name__),
            ),
            deps,
        )

        self.assertTrue(handled)
        self.assertEqual(
            replies,
            [
                "영상 다운로드는 바코드 없이는 특정할 수 없어.\n"
                "11자리 바코드랑 날짜를 같이 보내줘. "
                "예: `12345678910 2026-04-28 영상 다운로드`"
            ],
        )

    def test_uses_recordings_scope_fallback_for_dated_device_file_probe(self) -> None:
        replies: list[str] = []

        deps = DeviceRoutesDeps(
            get_s3_client=lambda: None,
            get_recordings_context=lambda: {"summary": {"recordingCount": 1}, "rows": []},
            has_recordings_device_mapping=lambda context: False,
            send_dm_message=lambda user_id, text: False,
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
            reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
        )

        with (
            patch("boxer_company_adapter_slack.device_routes.s.S3_QUERY_ENABLED", True),
            patch("boxer_company_adapter_slack.device_routes.s.DB_HOST", "db-host"),
            patch("boxer_company_adapter_slack.device_routes.s.DB_USERNAME", "db-user"),
            patch("boxer_company_adapter_slack.device_routes.s.DB_PASSWORD", "db-pass"),
            patch("boxer_company_adapter_slack.device_routes.s.DB_DATABASE", "db-name"),
            patch(
                "boxer_company_adapter_slack.device_routes._extract_log_date_with_presence",
                return_value=("2026-04-18", True),
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._is_device_runtime_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._lookup_device_contexts_by_barcode_on_date",
                return_value=[{"deviceName": "MB2-C00419"}],
            ) as scope_lookup,
            patch(
                "boxer_company_adapter_slack.device_routes._locate_barcode_file_candidates",
                return_value=("*파일 확인 대상 세션 조회 결과*\n• 결과: 테스트", {"summary": {"recordCount": 1}}),
            ) as locate_candidates,
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="13194526492 2026-04-18 장비에 남은 영상",
                    barcode="13194526492",
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                deps,
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*파일 확인 대상 세션 조회 결과*\n• 결과: 테스트"])
        scope_lookup.assert_called_once_with("13194526492", "2026-04-18")
        self.assertEqual(
            locate_candidates.call_args.kwargs["device_contexts"],
            [{"deviceName": "MB2-C00419"}],
        )

    def test_direct_device_name_bypasses_manual_hospital_room_lookup_for_download(self) -> None:
        replies: list[str] = []

        deps = DeviceRoutesDeps(
            get_s3_client=lambda: None,
            get_recordings_context=lambda: {"summary": {"recordingCount": 0}, "rows": []},
            has_recordings_device_mapping=lambda context: False,
            send_dm_message=lambda user_id, text: False,
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
            reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
        )

        with (
            patch("boxer_company_adapter_slack.device_routes.s.S3_QUERY_ENABLED", True),
            patch("boxer_company_adapter_slack.device_routes.s.DB_HOST", "db-host"),
            patch("boxer_company_adapter_slack.device_routes.s.DB_USERNAME", "db-user"),
            patch("boxer_company_adapter_slack.device_routes.s.DB_PASSWORD", "db-pass"),
            patch("boxer_company_adapter_slack.device_routes.s.DB_DATABASE", "db-name"),
            patch("boxer_company_adapter_slack.device_routes.cs.DEVICE_FILE_DOWNLOAD_BUCKET", "bucket"),
            patch(
                "boxer_company_adapter_slack.device_routes._is_device_runtime_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._lookup_device_contexts_by_hospital_room",
                return_value=[],
            ) as hospital_room_lookup,
            patch(
                "boxer_company_adapter_slack.device_routes._locate_barcode_file_candidates",
                return_value=("*파일 확인 대상 세션 조회 결과*\n• 결과: 테스트", {"summary": {"recordCount": 0}}),
            ) as locate_candidates,
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question=(
                        "16971952215 나무정원여성병원(양주) 2층1-1진료실 "
                        "MB2-A00313 2026-04-22 영상 다운"
                    ),
                    barcode="16971952215",
                    phase2_hospital_name="나무정원여성병원(양주)",
                    phase2_room_name="2층1-1진료실",
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                deps,
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*파일 확인 대상 세션 조회 결과*\n• 결과: 테스트"])
        hospital_room_lookup.assert_not_called()
        self.assertEqual(
            locate_candidates.call_args.kwargs["device_contexts"],
            [
                {
                    "deviceName": "MB2-A00313",
                    "hospitalName": "나무정원여성병원(양주)",
                    "roomName": "2층1-1진료실",
                }
            ],
        )

    def test_device_download_sends_summary_and_each_link_as_separate_dm(self) -> None:
        replies: list[str] = []
        dm_messages: list[tuple[str | None, str]] = []
        long_url_1 = "https://example.invalid/temp/a.motion.mp4?" + "X-Amz-Security-Token=" + "a" * 3500
        long_url_2 = "https://example.invalid/temp/b.motion.mp4?" + "X-Amz-Security-Token=" + "b" * 3500

        deps = DeviceRoutesDeps(
            get_s3_client=lambda: None,
            get_recordings_context=lambda: {"summary": {"recordingCount": 1}, "rows": []},
            has_recordings_device_mapping=lambda context: True,
            send_dm_message=lambda user_id, text: dm_messages.append((user_id, text)) or True,
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
            reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
        )

        payload = {
            "summary": {"recordCount": 1},
            "request": {},
            "records": [
                {
                    "deviceName": "MB2-B00046",
                    "hospitalName": "좋은문화병원(부산)",
                    "roomName": "2층 15진료실",
                    "sessions": [
                        {
                            "probe": {
                                "ok": True,
                                "files": [
                                    "/home/mommytalk/AppData/Videos/a.motion.mp4",
                                    "/home/mommytalk/AppData/Videos/b.motion.mp4",
                                ],
                            },
                            "download": {
                                "downloads": [
                                    {"ok": True, "fileName": "a.motion.mp4", "url": long_url_1},
                                    {"ok": True, "fileName": "b.motion.mp4", "url": long_url_2},
                                ]
                            },
                        }
                    ],
                }
            ],
        }

        with (
            patch("boxer_company_adapter_slack.device_routes.s.S3_QUERY_ENABLED", True),
            patch("boxer_company_adapter_slack.device_routes.s.DB_HOST", "db-host"),
            patch("boxer_company_adapter_slack.device_routes.s.DB_USERNAME", "db-user"),
            patch("boxer_company_adapter_slack.device_routes.s.DB_PASSWORD", "db-pass"),
            patch("boxer_company_adapter_slack.device_routes.s.DB_DATABASE", "db-name"),
            patch("boxer_company_adapter_slack.device_routes.cs.DEVICE_FILE_DOWNLOAD_BUCKET", "bucket"),
            patch(
                "boxer_company_adapter_slack.device_routes._is_device_runtime_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._extract_log_date_with_presence",
                return_value=("2026-06-03", True),
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._locate_barcode_file_candidates",
                return_value=("*장비 영상 다운로드 결과*", payload),
            ),
            patch("boxer_company_adapter_slack.device_routes._load_slack_user_name", return_value="Rosa"),
            patch("boxer_company_adapter_slack.device_routes._log_device_download_activity", return_value=1),
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="23754508923 2026-06-03 영상 다운",
                    barcode="23754508923",
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                deps,
            )

        self.assertTrue(handled)
        self.assertEqual(len(dm_messages), 3)
        self.assertIn("파일별 별도 DM", dm_messages[0][1])
        self.assertNotIn(long_url_1, dm_messages[0][1])
        self.assertNotIn(long_url_2, dm_messages[0][1])
        self.assertIn(f"🎣 <{long_url_1}|a.motion.mp4>", dm_messages[1][1])
        self.assertIn(f"🎣 <{long_url_2}|b.motion.mp4>", dm_messages[2][1])
        self.assertEqual(replies[0].count("다운로드 링크: DM으로 보냈어"), 1)


if __name__ == "__main__":
    unittest.main()
