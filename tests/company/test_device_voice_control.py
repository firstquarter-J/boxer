import logging
import unittest
from unittest.mock import Mock, patch

from boxer_company.routers.device_voice_control import (
    _build_device_voice_choices_message,
    _change_device_voice,
    _extract_device_voice_label,
    _is_device_voice_change_request,
)
from boxer_company_adapter_slack.device_routes import (
    DeviceRoutesContext,
    DeviceRoutesDeps,
    _handle_device_routes,
)


def _deps() -> DeviceRoutesDeps:
    return DeviceRoutesDeps(
        get_s3_client=lambda: None,
        get_recordings_context=lambda: {},
        has_recordings_device_mapping=lambda context: False,
        send_dm_message=lambda user_id, text: False,
        build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
        reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
    )


def _context(question: str, replies: list[str], *, user_id: str = "UHYUN") -> DeviceRoutesContext:
    payload = {
        "text": question,
        "question": question,
        "user_id": user_id,
        "workspace_id": "W123",
        "channel_id": "C123",
        "current_ts": "1.1",
        "thread_ts": "1.0",
    }
    return DeviceRoutesContext(
        question=question,
        barcode=None,
        phase2_hospital_name=None,
        phase2_room_name=None,
        payload=payload,  # type: ignore[arg-type]
        user_id=user_id,
        workspace_id="W123",
        channel_id="C123",
        thread_ts="1.0",
        reply=lambda text, **kwargs: replies.append(text),
        client=None,
        logger=logging.getLogger(__name__),
    )


class DeviceVoiceControlTests(unittest.TestCase):
    def test_maps_all_allowed_voice_labels_to_fixed_commands(self) -> None:
        expected = {
            "귀여운 음성": "S_VOICE1",
            "진지한 음성": "S_VOICE2",
            "기존 귀여운 음성": "S_VOICE_LEGACY_1",
            "기존 진지한 음성": "S_VOICE_LEGACY_2",
        }

        for label, command in expected.items():
            with self.subTest(label=label):
                dispatcher = Mock(
                    return_value={"affected": 1, "status": True, "message": "sent"}
                )
                text, payload = _change_device_voice(
                    "MB2-C00419",
                    label,
                    command_dispatcher=dispatcher,
                )

                dispatcher.assert_called_once_with(
                    "MB2-C00419",
                    command="scansim",
                    acme=command,
                )
                self.assertEqual(payload["deviceCommand"], command)
                self.assertIn("명령 전송 완료", text)

    def test_prefers_legacy_label_over_shorter_nested_label(self) -> None:
        self.assertEqual(
            _extract_device_voice_label("기존 귀여운 음성으로 바꿔줘"),
            "기존 귀여운 음성",
        )

    def test_does_not_treat_voice_information_question_as_change(self) -> None:
        self.assertFalse(_is_device_voice_change_request("귀여운 음성이 뭐야?"))

    def test_choices_message_lists_exactly_four_supported_voices(self) -> None:
        message = _build_device_voice_choices_message()
        for label in (
            "귀여운 음성",
            "진지한 음성",
            "기존 귀여운 음성",
            "기존 진지한 음성",
        ):
            self.assertIn(f"`{label}`", message)

    def test_route_sends_voice_change_for_any_user(self) -> None:
        replies: list[str] = []
        dispatch = {"affected": 1, "status": True, "message": "sent"}

        with (
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_GRAPHQL_URL", "https://mda.example/graphql"),
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_ADMIN_USER_PASSWORD", "secret"),
            patch(
                "boxer_company_adapter_slack.device_routes._send_mda_device_command",
                return_value=dispatch,
            ) as send_command,
        ):
            context = _context("MB2-C00419 기존 진지한 음성으로 바꿔줘", replies)
            handled = _handle_device_routes(context, _deps())

        self.assertTrue(handled)
        send_command.assert_called_once_with(
            "MB2-C00419",
            command="scansim",
            acme="S_VOICE_LEGACY_2",
        )
        self.assertIn("기존 진지한 음성", replies[0])
        self.assertEqual(context.payload["request_log"]["route_name"], "device voice change")

    def test_route_does_not_apply_user_allowlist(self) -> None:
        replies: list[str] = []
        dispatch = {"affected": 1, "status": True, "message": "sent"}

        with (
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_GRAPHQL_URL", "https://mda.example/graphql"),
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_ADMIN_USER_PASSWORD", "secret"),
            patch(
                "boxer_company_adapter_slack.device_routes._send_mda_device_command",
                return_value=dispatch,
            ) as send_command,
        ):
            handled = _handle_device_routes(
                _context("MB2-C00419 귀여운 음성으로 바꿔줘", replies, user_id="UOTHER"),
                _deps(),
            )

        self.assertTrue(handled)
        send_command.assert_called_once_with(
            "MB2-C00419",
            command="scansim",
            acme="S_VOICE1",
        )
        self.assertIn("명령 전송 완료", replies[0])

    def test_route_requests_device_name_before_dispatch(self) -> None:
        replies: list[str] = []

        handled = _handle_device_routes(
            _context("진지한 음성으로 바꿔줘", replies),
            _deps(),
        )

        self.assertTrue(handled)
        self.assertIn("장비명이 필요해", replies[0])


if __name__ == "__main__":
    unittest.main()
