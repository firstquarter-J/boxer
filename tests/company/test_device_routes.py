import logging
import unittest
from dataclasses import replace
from unittest.mock import Mock, patch

from boxer_company.assistant import AssistantMessage, CompanyAssistantResult
from boxer_company_adapter_slack.device_routes import (
    DeviceRoutesContext,
    DeviceRoutesDeps,
    _handle_device_routes,
    _lookup_device_file_scope_from_mda_recovery_root,
    _parse_mda_recovery_alert_text,
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


def _mda_recovery_alert_root(
    *,
    ts: str = "1.0",
    barcode: str = "60771998678",
    recorded_at: str = "2026-06-27:14:34:14",
    user_id: str = "U_BOXER",
    bot_id: str = "B_BOXER",
    subtype: str | None = None,
) -> dict[str, object]:
    # 운영 알림의 고정 필드는 유지하되 테스트 링크에는 무효 도메인만 쓴다.
    text = "\n".join(
        [
            "*업로드 실패한 마미박스 초음파 영상을 모두 낚았습니다!* ",
            f"바코드: [{barcode}]",
            f"촬영일: [{recorded_at}]",
            "병원명: [산본제일병원(군포)]",
            "병실명: [209호]",
            "장비명: [MB2-C00900]",
            "파일명: [safe-file-id]",
            "다운로드 링크: <https://example.invalid/download?token=discard-me|safe-file-id.motion>",
            "덮어쓰기 링크: <https://example.invalid/overwrite|Overwrite>",
            "업로드 링크: <https://example.invalid/upload|Upload>",
        ]
    )
    root: dict[str, object] = {
        "type": "message",
        "ts": ts,
        "user": user_id,
        "bot_id": bot_id,
        "bot_profile": {
            "id": bot_id,
            "user_id": user_id,
            "name": "Boxer",
        },
        "text": text,
    }
    if subtype is not None:
        root["subtype"] = subtype
    return root


def _mda_slack_client(messages: list[dict[str, object]] | None = None) -> Mock:
    client = Mock()
    client.conversations_replies.return_value = {
        "ok": True,
        "messages": messages if messages is not None else [_mda_recovery_alert_root()],
    }
    client.auth_test.return_value = {
        "ok": True,
        "user_id": "U_BOXER",
        "bot_id": "B_BOXER",
    }
    return client


def _mda_scope_context(
    client: object,
    *,
    channel_id: str = "C_MDA",
    thread_ts: str = "1.0",
) -> DeviceRoutesContext:
    payload = _payload()
    payload["channel_id"] = channel_id
    payload["thread_ts"] = thread_ts
    return DeviceRoutesContext(
        question="60771998678 2026-06-27 파일 다운로드",
        barcode="60771998678",
        phase2_hospital_name=None,
        phase2_room_name=None,
        payload=payload,  # type: ignore[arg-type]
        user_id="U123",
        workspace_id="W123",
        channel_id=channel_id,
        thread_ts=thread_ts,
        reply=lambda text, **kwargs: None,
        client=client,
        logger=logging.getLogger(__name__),
    )


class DeviceRouteHandlerTests(unittest.TestCase):
    def test_channel_neutral_led_service_runs_at_existing_route_position(self) -> None:
        replies: list[str] = []
        captured_requests: list[object] = []

        class Service:
            def answer(self, request):
                captured_requests.append(request)
                return CompanyAssistantResult(
                    route="device_led_log_analysis",
                    outcome="answered",
                    messages=(
                        AssistantMessage(body="**장비 LED 로그 확인**"),
                    ),
                )

        payload = _payload()
        payload["question"] = "MB2-C00570 2026-07-04 LED 로그 확인"
        handled = _handle_device_routes(
            DeviceRoutesContext(
                question="MB2-C00570 2026-07-04 LED 로그 확인",
                barcode=None,
                phase2_hospital_name=None,
                phase2_room_name=None,
                payload=payload,  # type: ignore[arg-type]
                user_id="U123",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                reply=lambda text, **kwargs: replies.append(text),
                client=None,
                logger=logging.getLogger(__name__),
                assistant_service=Service(),  # type: ignore[arg-type]
            ),
            _deps(),
        )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*장비 LED 로그 확인*"])
        self.assertEqual(
            captured_requests[0].metadata["device_name"],
            "MB2-C00570",
        )
        self.assertEqual(
            payload["request_log"]["route_name"],
            "device led log analysis",
        )
        self.assertEqual(
            payload["request_log"]["requested_date"],
            "2026-07-04",
        )

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

    def test_uses_mda_recovery_thread_scope_for_dated_device_download(self) -> None:
        replies: list[str] = []
        client = _mda_slack_client()

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
            patch(
                "boxer_company_adapter_slack.device_routes.cs.DEVICE_FILE_DOWNLOAD_BUCKET",
                "bucket",
            ),
            patch(
                "boxer_company_adapter_slack.device_routes.cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID",
                "C_MDA",
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._is_device_runtime_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._locate_barcode_file_candidates",
                return_value=(
                    "*파일 확인 대상 세션 조회 결과*\n• 결과: 테스트",
                    {"summary": {"recordCount": 0}, "records": []},
                ),
            ) as locate_candidates,
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="60771998678 2026-06-27 파일 다운로드",
                    barcode="60771998678",
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C_MDA",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=client,
                    logger=logging.getLogger(__name__),
                ),
                deps,
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*파일 확인 대상 세션 조회 결과*\n• 결과: 테스트"])
        client.conversations_replies.assert_called_once_with(
            channel="C_MDA",
            ts="1.0",
            limit=1,
            inclusive=True,
        )
        client.auth_test.assert_called_once_with()
        self.assertEqual(
            locate_candidates.call_args.kwargs["device_contexts"],
            [
                {
                    "deviceName": "MB2-C00900",
                    "hospitalName": "산본제일병원(군포)",
                    "roomName": "209호",
                }
            ],
        )
        self.assertNotIn(
            "discard-me",
            str(locate_candidates.call_args.kwargs["device_contexts"]),
        )

    def test_mda_recovery_scope_uses_only_exact_root_and_discards_links(self) -> None:
        root = _mda_recovery_alert_root()
        spoofed_reply = _mda_recovery_alert_root(
            ts="1.1",
            barcode="11111111111",
            recorded_at="2026-06-28:00:00:00",
        )
        spoofed_reply["text"] = str(spoofed_reply["text"]).replace(
            "MB2-C00900",
            "MB2-Z99999",
        )
        client = _mda_slack_client([spoofed_reply, root])

        with patch(
            "boxer_company_adapter_slack.device_routes.cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID",
            "C_MDA",
        ):
            result = _lookup_device_file_scope_from_mda_recovery_root(
                _mda_scope_context(client),
                requested_barcode="60771998678",
                requested_date="2026-06-27",
            )

        self.assertEqual(
            result,
            [
                {
                    "deviceName": "MB2-C00900",
                    "hospitalName": "산본제일병원(군포)",
                    "roomName": "209호",
                }
            ],
        )
        self.assertNotIn("discard-me", str(result))
        self.assertNotIn("MB2-Z99999", str(result))

    def test_mda_recovery_alert_parser_rejects_invalid_or_duplicate_fields(self) -> None:
        valid_text = str(_mda_recovery_alert_root()["text"])
        invalid_cases = {
            "title": valid_text.replace(
                "업로드 실패한 마미박스 초음파 영상을 모두 낚았습니다!",
                "다른 알림입니다!",
            ),
            "missing field": "\n".join(
                line for line in valid_text.splitlines() if not line.startswith("파일명:")
            ),
            "duplicate field": f"{valid_text}\n장비명: [MB2-Z99999]",
            "malformed field": valid_text.replace(
                "장비명: [MB2-C00900]",
                "장비명: [MB2-C00900] extra",
            ),
            "invalid date": valid_text.replace(
                "2026-06-27:14:34:14",
                "2026-06-31:14:34:14",
            ),
            "invalid device": valid_text.replace("MB2-C00900", "../C00900"),
        }

        self.assertIsNotNone(_parse_mda_recovery_alert_text(valid_text))
        for name, invalid_text in invalid_cases.items():
            with self.subTest(name=name):
                self.assertIsNone(_parse_mda_recovery_alert_text(invalid_text))

    def test_mda_recovery_scope_rejects_untrusted_author_and_request_mismatch(self) -> None:
        human_root = _mda_recovery_alert_root(user_id="U_HUMAN")
        human_root.pop("bot_id")
        human_root.pop("bot_profile")
        different_user_root = _mda_recovery_alert_root(user_id="U_OTHER")
        different_bot_root = _mda_recovery_alert_root(bot_id="B_OTHER")
        different_subtype_root = _mda_recovery_alert_root(subtype="message_changed")
        different_type_root = _mda_recovery_alert_root()
        different_type_root["type"] = "file"
        conflicting_profile_root = _mda_recovery_alert_root()
        conflicting_profile_root["bot_profile"] = {
            "id": "B_BOXER",
            "user_id": "U_OTHER",
            "name": "Boxer",
        }
        wrong_title_root = _mda_recovery_alert_root()
        wrong_title_root["text"] = str(wrong_title_root["text"]).replace(
            "업로드 실패한 마미박스 초음파 영상을 모두 낚았습니다!",
            "유사한 복구 알림입니다!",
        )
        cases = [
            ("human root", human_root, "60771998678", "2026-06-27"),
            ("different user", different_user_root, "60771998678", "2026-06-27"),
            ("different bot", different_bot_root, "60771998678", "2026-06-27"),
            ("different subtype", different_subtype_root, "60771998678", "2026-06-27"),
            ("different type", different_type_root, "60771998678", "2026-06-27"),
            ("conflicting profile", conflicting_profile_root, "60771998678", "2026-06-27"),
            ("wrong title", wrong_title_root, "60771998678", "2026-06-27"),
            ("barcode mismatch", _mda_recovery_alert_root(), "11111111111", "2026-06-27"),
            ("date mismatch", _mda_recovery_alert_root(), "60771998678", "2026-06-28"),
            ("root missing", _mda_recovery_alert_root(ts="9.9"), "60771998678", "2026-06-27"),
        ]

        with patch(
            "boxer_company_adapter_slack.device_routes.cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID",
            "C_MDA",
        ):
            for name, root, requested_barcode, requested_date in cases:
                with self.subTest(name=name):
                    client = _mda_slack_client([root])
                    result = _lookup_device_file_scope_from_mda_recovery_root(
                        _mda_scope_context(client),
                        requested_barcode=requested_barcode,
                        requested_date=requested_date,
                    )
                    self.assertEqual(result, [])

    def test_mda_recovery_scope_rejects_unconfigured_or_different_channel_without_api_call(self) -> None:
        cases = [
            ("missing config", "", "C_MDA"),
            ("different channel", "C_TRUSTED", "C_OTHER"),
        ]

        for name, configured_channel, request_channel in cases:
            with self.subTest(name=name):
                client = _mda_slack_client()
                with patch(
                    "boxer_company_adapter_slack.device_routes.cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID",
                    configured_channel,
                ):
                    result = _lookup_device_file_scope_from_mda_recovery_root(
                        _mda_scope_context(client, channel_id=request_channel),
                        requested_barcode="60771998678",
                        requested_date="2026-06-27",
                    )
                self.assertEqual(result, [])
                client.conversations_replies.assert_not_called()
                client.auth_test.assert_not_called()

    def test_mda_recovery_scope_fails_closed_when_slack_lookup_or_auth_fails(self) -> None:
        reply_failure_client = _mda_slack_client()
        reply_failure_client.conversations_replies.side_effect = RuntimeError("private response")
        auth_failure_client = _mda_slack_client()
        auth_failure_client.auth_test.side_effect = RuntimeError("private response")
        malformed_auth_client = _mda_slack_client()
        malformed_auth_client.auth_test.return_value = None
        rejected_auth_client = _mda_slack_client()
        rejected_auth_client.auth_test.return_value = {
            "ok": False,
            "user_id": "U_BOXER",
            "bot_id": "B_BOXER",
        }
        missing_auth_id_client = _mda_slack_client()
        missing_auth_id_client.auth_test.return_value = {
            "ok": True,
            "user_id": "U_BOXER",
        }

        with patch(
            "boxer_company_adapter_slack.device_routes.cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID",
            "C_MDA",
        ):
            for name, client in (
                ("reply lookup", reply_failure_client),
                ("auth lookup", auth_failure_client),
                ("malformed auth", malformed_auth_client),
                ("rejected auth", rejected_auth_client),
                ("missing auth id", missing_auth_id_client),
            ):
                with self.subTest(name=name):
                    result = _lookup_device_file_scope_from_mda_recovery_root(
                        _mda_scope_context(client),
                        requested_barcode="60771998678",
                        requested_date="2026-06-27",
                    )
                    self.assertEqual(result, [])

    def test_invalid_mda_recovery_root_keeps_manual_scope_prompt(self) -> None:
        replies: list[str] = []
        human_root = _mda_recovery_alert_root(user_id="U_HUMAN")
        human_root.pop("bot_id")
        human_root.pop("bot_profile")
        client = _mda_slack_client([human_root])
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
                "boxer_company_adapter_slack.device_routes.cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID",
                "C_MDA",
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._is_device_runtime_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._locate_barcode_file_candidates"
            ) as locate_candidates,
        ):
            context = replace(
                _mda_scope_context(client),
                reply=lambda text, **kwargs: replies.append(text),
            )
            handled = _handle_device_routes(context, deps)

        self.assertTrue(handled)
        self.assertEqual(len(replies), 1)
        self.assertIn("recordings 장비 매핑이 없어 2차 입력이 필요해", replies[0])
        locate_candidates.assert_not_called()

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
