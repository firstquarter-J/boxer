import unittest

from boxer_company.routers.device_audio_probe import (
    _extract_device_name_for_audio_probe,
    _is_device_audio_probe_request,
    _parse_mixer_control,
    _parse_playback_devices,
    _parse_playback_test,
    _render_device_audio_probe_result,
    _summarize_device_audio_probe,
)


_PLAYBACK_DEVICES_OUTPUT = """**** List of PLAYBACK Hardware Devices ****
card 0: Intel [HDA Intel], device 0: Generic Analog [Generic Analog]
  Subdevices: 1/1
  Subdevice #0: subdevice #0
card 0: Intel [HDA Intel], device 3: Generic Digital [Generic Digital]
  Subdevices: 1/1
  Subdevice #0: subdevice #0
"""

_MASTER_MIXER_OUTPUT = """Simple mixer control 'Master',0
  Capabilities: pvolume pswitch
  Playback channels: Front Left - Front Right
  Limits: Playback 0 - 87
  Mono:
  Front Left: Playback 76 [87%] [-8.25dB] [on]
  Front Right: Playback 76 [87%] [-8.25dB] [on]
"""

_PCM_MIXER_OUTPUT = """Simple mixer control 'PCM',0
  Capabilities: pvolume
  Playback channels: Front Left - Front Right
  Limits: Playback 0 - 255
  Mono:
  Front Left: Playback 255 [100%] [0.00dB]
  Front Right: Playback 255 [100%] [0.00dB]
"""

_SPEAKER_TEST_OUTPUT = """speaker-test 1.1.3

Playback device is default
Stream parameters are 48000Hz, S16_LE, 2 channels
Sine wave rate is 440.0000Hz
Rate set to 48000Hz (requested 48000Hz)
Buffer size range from 2048 to 8192
Period size range from 1024 to 1024
Using max buffer size 8192
Periods = 4
was set period_size = 1024
was set buffer_size = 8192
 0 - Front Left
 1 - Front Right
Time per period = 5.822676
"""


class DeviceAudioProbeRoutingTests(unittest.TestCase):
    def test_extracts_device_name_from_leading_audio_probe_question(self) -> None:
        self.assertEqual(
            _extract_device_name_for_audio_probe("MB2-C00419 장비 소리 출력 점검"),
            "MB2-C00419",
        )

    def test_extracts_device_name_from_explicit_scope_audio_probe_question(self) -> None:
        self.assertEqual(
            _extract_device_name_for_audio_probe("장비명 MB2-C00419 소리 재생 테스트"),
            "MB2-C00419",
        )

    def test_requires_device_name_for_audio_probe_request(self) -> None:
        self.assertFalse(_is_device_audio_probe_request("소리 출력 점검"))
        self.assertTrue(_is_device_audio_probe_request("MB2-C00419 장비 소리 출력 점검"))


class DeviceAudioProbeParsingTests(unittest.TestCase):
    def test_parses_playback_devices(self) -> None:
        parsed = _parse_playback_devices(_PLAYBACK_DEVICES_OUTPUT)

        self.assertTrue(parsed["available"])
        self.assertEqual(parsed["deviceCount"], 2)
        self.assertEqual(parsed["devices"][0]["deviceName"], "Generic Analog")

    def test_parses_master_and_pcm_mixer_controls(self) -> None:
        master = _parse_mixer_control(_MASTER_MIXER_OUTPUT, control_name="Master")
        pcm = _parse_mixer_control(_PCM_MIXER_OUTPUT, control_name="PCM")

        self.assertTrue(master["available"])
        self.assertEqual(master["percent"], 87)
        self.assertEqual(master["switch"], "on")
        self.assertTrue(pcm["available"])
        self.assertEqual(pcm["percent"], 100)

    def test_summarizes_probe_as_pass_when_playback_path_is_healthy(self) -> None:
        playback_devices = _parse_playback_devices(_PLAYBACK_DEVICES_OUTPUT)
        master = _parse_mixer_control(_MASTER_MIXER_OUTPUT, control_name="Master")
        pcm = _parse_mixer_control(_PCM_MIXER_OUTPUT, control_name="PCM")
        playback_test = _parse_playback_test(_SPEAKER_TEST_OUTPUT, 0)
        summary = _summarize_device_audio_probe(
            tool_paths={
                "aplay": "/usr/bin/aplay",
                "amixer": "/usr/bin/amixer",
                "speaker-test": "/usr/bin/speaker-test",
            },
            playback_devices=playback_devices,
            master_mixer=master,
            pcm_mixer=pcm,
            default_sink={"available": False, "reason": "pactl_missing", "defaultSink": ""},
            playback_test=playback_test,
        )

        self.assertEqual(summary["status"], "pass")
        self.assertFalse(summary["mixerMuted"])
        self.assertIn("정상", summary["summary"])
        self.assertIn("연결된 스피커", summary["recommendedAction"])

        rendered = _render_device_audio_probe_result(
            device_name="MB2-C00419",
            device_info={
                "hospitalName": "아이사랑산부인과의원(부산)",
                "roomName": "2진료실",
                "version": "2.11.300",
            },
            ssh_ready=True,
            ssh_reason="ready",
            checks=[],
            summary=summary,
            playback_devices=playback_devices,
            master_mixer=master,
            pcm_mixer=pcm,
            default_sink={"available": False, "reason": "pactl_missing", "defaultSink": ""},
            playback_test=playback_test,
        )

        self.assertIn("*장비 소리 출력 점검*", rendered)
        self.assertIn("• 판정: *정상*", rendered)
        self.assertIn("• 근거:", rendered)
        self.assertIn("• 안내:", rendered)


if __name__ == "__main__":
    unittest.main()
