import unittest
from unittest.mock import Mock

from boxer_company.operation_routing import (
    _is_device_voice_catalog_request,
    _is_device_voice_change_request,
)
from boxer_company.routers.device_voice_control import (
    _build_device_voice_catalog_message,
    _build_device_voice_choices_message,
    _change_device_voice,
    _dispatch_device_voice_guide,
    _extract_device_voice_label,
)


class DeviceVoiceControlTests(unittest.TestCase):
    def test_dispatches_voice_guide_with_fixed_command_only(self) -> None:
        dispatcher = Mock(
            return_value={"affected": 1, "status": True, "message": "sent"}
        )

        result = _dispatch_device_voice_guide(
            "  MB2-C00419  ",
            command_dispatcher=dispatcher,
        )

        dispatcher.assert_called_once_with(
            "MB2-C00419",
            command="voice_guide",
        )
        self.assertTrue(result["status"])

    def test_rejects_arbitrary_command_or_acme_arguments(self) -> None:
        # Slack payload가 command/acme를 조작해도 helper 인터페이스에 들어올 수 없어야 한다.
        for field, value in (("command", "reboot"), ("acme", "INJECTED")):
            with self.subTest(field=field):
                dispatcher = Mock()

                with self.assertRaises(TypeError):
                    _dispatch_device_voice_guide(
                        "MB2-C00419",
                        command_dispatcher=dispatcher,
                        **{field: value},  # type: ignore[arg-type]
                    )

                dispatcher.assert_not_called()

    def test_rejects_invalid_device_names_before_dispatch(self) -> None:
        for device_name in (
            "",
            "  ",
            "MB",
            "MB2-C00419;reboot",
            "MB2/C00419",
            "MB2 C00419",
        ):
            with self.subTest(device_name=device_name):
                dispatcher = Mock()

                with self.assertRaisesRegex(ValueError, "장비명이 올바르지 않아"):
                    _dispatch_device_voice_guide(
                        device_name,
                        command_dispatcher=dispatcher,
                    )

                dispatcher.assert_not_called()

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

    def test_detects_voice_catalog_questions(self) -> None:
        for question in (
            "음성 세트 목록",
            "지원 음성 종류 알려줘",
            "음성 스캔 명령 알려줘",
        ):
            with self.subTest(question=question):
                self.assertTrue(_is_device_voice_catalog_request(question))

    def test_catalog_message_lists_config_and_scan_values(self) -> None:
        message = _build_device_voice_catalog_message()
        expected = {
            "귀여운 음성": ("n", "S_VOICE1"),
            "진지한 음성": ("s", "S_VOICE2"),
            "기존 귀여운 음성": ("ln", "S_VOICE_LEGACY_1"),
            "기존 진지한 음성": ("ls", "S_VOICE_LEGACY_2"),
        }

        self.assertIn("`command=scansim`, `acme=<스캔값>`", message)
        self.assertIn("`cmd=scansim`, `acme=<스캔값>`", message)
        for label, (voice_type, scan_value) in expected.items():
            with self.subTest(label=label):
                self.assertIn(f"*{label}*", message)
                self.assertIn(f"설정값 `{voice_type}`", message)
                self.assertIn(f"스캔값 `{scan_value}`", message)

    def test_choices_message_lists_exactly_four_supported_voices(self) -> None:
        message = _build_device_voice_choices_message()
        for label in (
            "귀여운 음성",
            "진지한 음성",
            "기존 귀여운 음성",
            "기존 진지한 음성",
        ):
            self.assertIn(f"*{label}*", message)

if __name__ == "__main__":
    unittest.main()
