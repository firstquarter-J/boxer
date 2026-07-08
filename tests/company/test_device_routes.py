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


def _deps() -> DeviceRoutesDeps:
    return DeviceRoutesDeps(
        get_s3_client=lambda: None,
        get_recordings_context=lambda: {},
        has_recordings_device_mapping=lambda context: False,
        send_dm_message=lambda user_id, text: False,
        build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
        reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
    )


class DeviceRouteHandlerTests(unittest.TestCase):
    def test_device_health_alert_delivery_enable_command_updates_runtime_setting(self) -> None:
        replies: list[str] = []
        logger = logging.getLogger(__name__)

        with patch(
            "boxer_company_adapter_slack.device_routes._set_device_health_monitor_alert_delivery_enabled",
            return_value={
                "enabled": True,
                "envDefault": False,
                "source": "slack_override",
                "updatedAt": "2026-07-08T10:00:00+09:00",
                "updatedBy": "U123",
                "monitorEnabled": True,
            },
        ) as set_alert_delivery:
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="이상 알림 메시지 보내기 켜",
                    barcode=None,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logger,
                ),
                _deps(),
            )

        self.assertTrue(handled)
        self.assertTrue(set_alert_delivery.call_args.args[0])
        self.assertEqual(set_alert_delivery.call_args.kwargs["user_id"], "U123")
        self.assertIs(set_alert_delivery.call_args.kwargs["logger"], logger)
        self.assertIn("상태: *켜짐*", replies[0])
        self.assertIn("Slack 명령", replies[0])
        self.assertIn("다음 장비 상태 모니터 poll부터", replies[0])

    def test_device_health_alert_delivery_status_command_does_not_change_setting(self) -> None:
        replies: list[str] = []

        with (
            patch(
                "boxer_company_adapter_slack.device_routes._resolve_device_health_monitor_alert_delivery_status",
                return_value={
                    "enabled": False,
                    "envDefault": True,
                    "source": "slack_override",
                    "updatedAt": "2026-07-08T10:00:00+09:00",
                    "updatedBy": "U123",
                    "monitorEnabled": False,
                },
            ) as resolve_status,
            patch(
                "boxer_company_adapter_slack.device_routes._set_device_health_monitor_alert_delivery_enabled"
            ) as set_alert_delivery,
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="이상 알림 상태 확인",
                    barcode=None,
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
                _deps(),
            )

        self.assertTrue(handled)
        resolve_status.assert_called_once_with()
        set_alert_delivery.assert_not_called()
        self.assertIn("상태: *꺼짐*", replies[0])
        self.assertIn("DEVICE_HEALTH_MONITOR_ENABLED=true", replies[0])

    def test_device_led_log_question_uses_log_analysis_before_pattern_guide(self) -> None:
        replies: list[str] = []
        synth_calls: list[tuple[object, ...]] = []

        deps = DeviceRoutesDeps(
            get_s3_client=lambda: "s3-client",
            get_recordings_context=lambda: {},
            has_recordings_device_mapping=lambda context: False,
            send_dm_message=lambda user_id, text: False,
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
            reply_with_retrieval_synthesis=lambda *args, **kwargs: synth_calls.append(args),
        )

        with (
            patch("boxer_company_adapter_slack.device_routes.s.S3_QUERY_ENABLED", True),
            patch(
                "boxer_company_adapter_slack.device_routes._analyze_device_led_log",
                return_value=("*장비 LED 로그 확인*\n• 결론: 테스트", {"route": "device_led_log_analysis"}),
            ) as analyze_led_log,
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question=(
                        "MB2-C00570 2026-07-04 LED 이상 조사. "
                        "대기모드일때는 초록색만 나와야하는데 전원오프상태의 led가 표시됐다고 해"
                    ),
                    barcode=None,
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
        analyze_led_log.assert_called_once_with("s3-client", "MB2-C00570", "2026-07-04")
        self.assertEqual(replies, ["*장비 LED 로그 확인*\n• 결론: 테스트"])
        self.assertEqual(synth_calls, [])

    def test_daily_box_auto_update_enable_command_updates_runtime_setting(self) -> None:
        replies: list[str] = []

        with patch(
            "boxer_company_adapter_slack.device_routes._set_daily_device_round_auto_update",
            return_value={
                "box": {
                    "label": "마미박스",
                    "enabled": True,
                    "envDefault": False,
                    "source": "slack_override",
                    "updatedAt": "2026-06-17T10:00:00+09:00",
                    "updatedBy": "U123",
                },
                "agent": {
                    "label": "에이전트",
                    "enabled": True,
                    "envDefault": True,
                    "source": "env",
                    "updatedAt": "",
                    "updatedBy": "",
                },
            },
        ) as set_auto_update:
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="박스 자동 업데이트 켜",
                    barcode=None,
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
                _deps(),
            )

        self.assertTrue(handled)
        set_auto_update.assert_called_once_with("box", True, user_id="U123")
        self.assertIn("마미박스: *켜짐*", replies[0])
        self.assertIn("에이전트: *켜짐*", replies[0])
        self.assertIn("적용: 다음 데일리 순회부터", replies[0])

    def test_daily_agent_auto_update_disable_command_updates_runtime_setting(self) -> None:
        replies: list[str] = []

        with patch(
            "boxer_company_adapter_slack.device_routes._set_daily_device_round_auto_update",
            return_value={
                "box": {
                    "label": "마미박스",
                    "enabled": False,
                    "envDefault": False,
                    "source": "env",
                    "updatedAt": "",
                    "updatedBy": "",
                },
                "agent": {
                    "label": "에이전트",
                    "enabled": False,
                    "envDefault": True,
                    "source": "slack_override",
                    "updatedAt": "2026-06-17T10:00:00+09:00",
                    "updatedBy": "U123",
                },
            },
        ) as set_auto_update:
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="에이전트 자동 업데이트 꺼",
                    barcode=None,
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
                _deps(),
            )

        self.assertTrue(handled)
        set_auto_update.assert_called_once_with("agent", False, user_id="U123")
        self.assertIn("마미박스: *꺼짐*", replies[0])
        self.assertIn("에이전트: *꺼짐*", replies[0])

    def test_daily_box_auto_update_status_command_does_not_change_setting(self) -> None:
        replies: list[str] = []

        with (
            patch(
                "boxer_company_adapter_slack.device_routes._build_daily_device_round_auto_update_status",
                return_value={
                    "box": {
                        "label": "마미박스",
                        "enabled": False,
                        "envDefault": False,
                        "source": "env",
                        "updatedAt": "",
                        "updatedBy": "",
                    },
                    "agent": {
                        "label": "에이전트",
                        "enabled": True,
                        "envDefault": True,
                        "source": "env",
                        "updatedAt": "",
                        "updatedBy": "",
                    },
                },
            ) as build_status,
            patch(
                "boxer_company_adapter_slack.device_routes._set_daily_device_round_auto_update"
            ) as set_auto_update,
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="데일리 자동 업데이트 상태",
                    barcode=None,
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
                _deps(),
            )

        self.assertTrue(handled)
        build_status.assert_called_once_with()
        set_auto_update.assert_not_called()
        self.assertIn("마미박스: *꺼짐*", replies[0])
        self.assertIn("에이전트: *켜짐*", replies[0])

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

    def test_video_extract_keyword_without_date_asks_for_date_before_lookup(self) -> None:
        replies: list[str] = []

        deps = DeviceRoutesDeps(
            get_s3_client=lambda: self.fail("S3 lookup should not run without requested date"),
            get_recordings_context=lambda: self.fail(
                "recordings lookup should not run without requested date"
            ),
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
        ):
            # "영상 꺼내"는 다운로드 의도지만, 날짜가 없으면 장비 조회 전에 보강 입력을 요청한다.
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="장유산부인과의원(김해) 6진료실 45707511017 18:37:36 영상 꺼내와주세요",
                    barcode="45707511017",
                    phase2_hospital_name="장유산부인과의원(김해)",
                    phase2_room_name="6진료실",
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
                "영상 다운로드는 날짜 없이는 특정할 수 없어.\n"
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
