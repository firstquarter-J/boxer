import unittest
from unittest.mock import patch

from boxer_company.routers.device_status_probe import (
    _build_trashcan_storage_usage,
    _build_led_pattern_help_evidence,
    _build_led_pattern_help_reply,
    _build_device_remote_access_probe_config_message,
    _patch_device_pm2_memory,
    _extract_device_name_for_remote_access_probe,
    _extract_device_name_for_status_probe,
    _is_device_captureboard_probe_request,
    _is_device_led_pattern_help_request,
    _is_device_led_probe_request,
    _is_device_memory_patch_request,
    _is_device_pm2_probe_request,
    _is_device_remote_access_probe_request,
    _is_device_status_probe_request,
    _parse_count_value,
    _parse_device_path_list,
    _parse_directory_usage,
    _parse_disk_usage,
    _parse_pm2_memory_restart_values,
    _parse_pm2_processes,
    _parse_usb_devices,
    _probe_device_remote_access,
    _probe_device_status_overview,
    _render_device_status_overview_result,
    _run_remote_ssh_command,
    _run_device_trashcan_cleanup,
    _summarize_captureboard_probe,
    _summarize_led_probe,
    _summarize_audio_path_probe,
    _summarize_pm2_probe,
    _summarize_storage_probe,
)


_PM2_JLIST_OUTPUT = """[
  {
    "name": "mommybox-v2",
    "pm2_env": {
      "status": "online",
      "restart_time": 1,
      "version": "2.11.300"
    },
    "monit": {
      "cpu": 0,
      "memory": 12345678
    }
  },
  {
    "name": "mommybox-v2-agent",
    "pm2_env": {
      "status": "online",
      "restart_time": 0,
      "versioning": {
        "version": "1.2.0"
      }
    },
    "monit": {
      "cpu": 1,
      "memory": 2345678
    }
  }
]"""

_LSUSB_OUTPUT = """Bus 001 Device 002: ID 1a86:7523 QinHeng Electronics CH340 serial converter
Bus 001 Device 003: ID 1164:f57a YUH01 HDMI capture
"""

_DF_ROOT_OUTPUT = """Filesystem     1024-blocks      Used Available Capacity Mounted on
/dev/nvme0n1p2    488061244 128735836 334481104      28% /
"""

_DU_TRASHCAN_OUTPUT = "2981888\t/home/mommytalk/AppData/TrashCan\n"


class DeviceStatusProbeRoutingTests(unittest.TestCase):
    def test_extracts_device_name_for_status_probe(self) -> None:
        self.assertEqual(
            _extract_device_name_for_status_probe("MB2-C00419 장비 상태 점검"),
            "MB2-C00419",
        )

    def test_extracts_device_name_for_remote_access_probe(self) -> None:
        self.assertEqual(
            _extract_device_name_for_remote_access_probe("MB2-C00419 ssh 연결 안 돼"),
            "MB2-C00419",
        )

    def test_routes_specific_pm2_captureboard_led_questions(self) -> None:
        self.assertTrue(_is_device_pm2_probe_request("MB2-C00419 pm2 상태"))
        self.assertTrue(_is_device_captureboard_probe_request("MB2-C00419 캡처보드 상태"))
        self.assertTrue(_is_device_led_probe_request("MB2-C00419 LED 상태"))

    def test_routes_led_pattern_help_questions_without_runtime_probe(self) -> None:
        self.assertTrue(_is_device_led_pattern_help_request("LED 증상은 어떨 때 나타나?"))
        self.assertTrue(_is_device_led_pattern_help_request("MB2-C00419 LED 패턴 의미가 뭐야?"))
        self.assertFalse(_is_device_led_pattern_help_request("MB2-C00419 LED 상태"))

    def test_routes_memory_patch_only_for_explicit_action_question(self) -> None:
        self.assertTrue(_is_device_memory_patch_request("MB2-C00419 메모리 패치"))
        self.assertTrue(_is_device_memory_patch_request("MB2-C00419 pm2 메모리 패치 해줘"))
        self.assertFalse(_is_device_memory_patch_request("MB2-C00419 메모리 패치 방법"))

    def test_routes_overview_status_but_not_specific_probe(self) -> None:
        self.assertTrue(_is_device_status_probe_request("MB2-C00419 장비 상태"))
        self.assertFalse(_is_device_status_probe_request("MB2-C00419 pm2 상태"))

    def test_routes_remote_access_probe_only_for_device_specific_question(self) -> None:
        self.assertTrue(_is_device_remote_access_probe_request("MB2-C00419 ssh 연결 안 돼"))
        self.assertTrue(_is_device_remote_access_probe_request("MB2-C00419 원격 접속 ping 확인"))
        self.assertFalse(_is_device_remote_access_probe_request("MB2-C00419 장비 상태"))

    def test_builds_remote_access_probe_config_message(self) -> None:
        self.assertIn("MDA_GRAPHQL_URL", _build_device_remote_access_probe_config_message())
        self.assertNotIn("DEVICE_SSH_PASSWORD", _build_device_remote_access_probe_config_message())


class DeviceStatusProbeParsingTests(unittest.TestCase):
    def test_parses_pm2_processes(self) -> None:
        parsed = _parse_pm2_processes(_PM2_JLIST_OUTPUT)

        self.assertTrue(parsed["available"])
        self.assertEqual(parsed["processes"][0]["name"], "mommybox-v2")
        self.assertEqual(parsed["processes"][0]["status"], "online")
        self.assertEqual(parsed["processes"][0]["version"], "2.11.300")
        self.assertEqual(parsed["processes"][1]["name"], "mommybox-v2-agent")
        self.assertEqual(parsed["processes"][1]["version"], "1.2.0")

    def test_summarizes_pm2_as_pass_when_main_and_agent_apps_are_online(self) -> None:
        summary = _summarize_pm2_probe(_parse_pm2_processes(_PM2_JLIST_OUTPUT))

        self.assertEqual(summary["status"], "pass")
        self.assertIn("정상 실행", summary["summary"])
        self.assertIn("2.11.300", summary["evidence"])
        self.assertIn("1.2.0", summary["evidence"])

    def test_summarizes_pm2_as_warning_when_agent_app_is_missing(self) -> None:
        summary = _summarize_pm2_probe(
            {
                "available": True,
                "reason": "ok",
                "processes": [
                    {
                        "name": "mommybox-v2",
                        "status": "online",
                        "version": "2.11.300",
                        "restartCount": 0,
                    }
                ],
            }
        )

        self.assertEqual(summary["status"], "warning")
        self.assertIn("agent", summary["summary"])

    def test_parses_pm2_memory_restart_values(self) -> None:
        parsed = _parse_pm2_memory_restart_values("max_memory_restart: 4294967296")

        self.assertTrue(parsed["available"])
        self.assertEqual(parsed["values"], [4294967296])
        self.assertTrue(parsed["hasExpectedLimit"])
        self.assertIn("4.0 GB", parsed["display"])

    def test_parses_root_disk_usage(self) -> None:
        parsed = _parse_disk_usage(_DF_ROOT_OUTPUT)

        self.assertTrue(parsed["available"])
        self.assertEqual(parsed["filesystem"], "/dev/nvme0n1p2")
        self.assertEqual(parsed["mount"], "/")
        self.assertEqual(parsed["usedPercent"], 28)
        self.assertGreater(parsed["availableBytes"], 0)

    def test_parses_trashcan_directory_usage(self) -> None:
        parsed = _parse_directory_usage(_DU_TRASHCAN_OUTPUT)

        self.assertTrue(parsed["available"])
        self.assertEqual(parsed["path"], "/home/mommytalk/AppData/TrashCan")
        self.assertGreater(parsed["sizeBytes"], 0)

    def test_summarizes_storage_as_fail_when_trashcan_share_exceeds_threshold(self) -> None:
        summary = _summarize_storage_probe(
            _build_trashcan_storage_usage(
                filesystem_usage=_parse_disk_usage(
                    "Filesystem 1024-blocks Used Available Capacity Mounted on\n"
                    "/dev/nvme0n1p2 102400000 35840000 66560000 35% /\n"
                ),
                directory_usage=_parse_directory_usage(
                    "64000000\t/home/mommytalk/AppData/TrashCan\n"
                ),
                file_count=_parse_count_value("210"),
                expired_file_count=_parse_count_value("45"),
                cleanup_threshold_percent=60,
                cleanup_age_days=30,
            )
        )

        self.assertEqual(summary["status"], "fail")
        self.assertEqual(summary["label"], "이상")
        self.assertIn("AppData/TrashCan", summary["evidence"])
        self.assertIn("30일 초과 `45개`", summary["evidence"])
        self.assertIn("자동 정리 기준 `60%`", summary["summary"])
        self.assertEqual(summary["diskLabel"], "정상")
        self.assertEqual(summary["trashcanLabel"], "이상")
        self.assertIn("경로 `/`", summary["diskOverviewDetail"])
        self.assertIn("경로 `AppData/TrashCan`", summary["trashcanOverviewDetail"])

    def test_marks_trashcan_cleanup_candidate_without_execute(self) -> None:
        cleanup = _run_device_trashcan_cleanup(
            {
                "ssh": {"ready": True, "host": "127.0.0.1", "port": 22},
                "overview": {
                    "storage": {
                        "directorySharePercent": 61.2,
                        "expiredFileCount": 12,
                        "fileCount": 100,
                        "directorySizeBytes": 12 * 1024 * 1024 * 1024,
                        "displayPath": "AppData/TrashCan",
                    }
                },
            },
            execute=False,
            cleanup_threshold_percent=60,
            cleanup_age_days=30,
        )

        self.assertEqual(cleanup["status"], "candidate")
        self.assertEqual(cleanup["label"], "대상")
        self.assertIn("기준 `60%` 초과", cleanup["detail"])
        self.assertIn("`30일` 초과 `12개`", cleanup["detail"])

    def test_summarizes_captureboard_as_pass_with_usb_and_video_device(self) -> None:
        summary = _summarize_captureboard_probe(
            device_info={"captureBoardType": "YUH01"},
            usb_devices=_parse_usb_devices(_LSUSB_OUTPUT),
            video_devices=_parse_device_path_list("/dev/video0\n/dev/video1", missing_token="no_video_device"),
            v4l2_devices="YUH01 HDMI capture\n\t/dev/video0\n\t/dev/video1",
        )

        self.assertEqual(summary["status"], "pass")
        self.assertIn("USB `YUH01`", summary["evidence"])

    def test_summarizes_led_as_pass_when_led_usb_is_found(self) -> None:
        summary = _summarize_led_probe(
            usb_devices=_parse_usb_devices(_LSUSB_OUTPUT),
            serial_devices=_parse_device_path_list("/dev/ttyUSB0", missing_token="no_serial_device"),
        )

        self.assertEqual(summary["status"], "pass")
        self.assertIn("LED USB", summary["evidence"])

    def test_builds_led_pattern_help_reply_with_warning_and_network_note(self) -> None:
        reply = _build_led_pattern_help_reply("LED 초록불 길게 깜빡이다가 빨간불 잠시 들어옴 반복은 뭐야?")

        self.assertIn("• 결론:", reply)
        self.assertIn("warning", reply)
        self.assertIn("네트워크 오프라인", reply)

    def test_builds_led_pattern_help_evidence_with_spec_and_interpretation(self) -> None:
        evidence = _build_led_pattern_help_evidence("LED 초록불 길게 깜빡이다가 빨간불 잠시 들어옴 반복은 뭐야?")

        self.assertEqual(evidence["route"], "device_led_pattern_guide")
        self.assertEqual(evidence["patternInterpretation"]["status"], "warning")
        self.assertFalse(evidence["notes"]["networkOfflineLedMapped"])
        self.assertGreaterEqual(len(evidence["ledSpec"]), 6)

    def test_summarizes_audio_path_as_passive_ok_when_devices_and_volume_exist(self) -> None:
        summary = _summarize_audio_path_probe(
            {
                "tools": {"output": "aplay=/usr/bin/aplay\namixer=/usr/bin/amixer\npactl=\nspeaker-test="},
                "playback_devices": {
                    "output": (
                        "**** List of PLAYBACK Hardware Devices ****\n"
                        "card 0: Intel [HDA Intel], device 0: Generic Analog [Generic Analog]\n"
                        "card 0: Intel [HDA Intel], device 3: Generic Digital [Generic Digital]\n"
                    )
                },
                "master_mixer": {
                    "output": (
                        "Simple mixer control 'Master',0\n"
                        "  Front Left: Playback 76 [87%] [-8.25dB] [on]\n"
                        "  Front Right: Playback 76 [87%] [-8.25dB] [on]\n"
                    )
                },
                "pcm_mixer": {
                    "output": (
                        "Simple mixer control 'PCM',0\n"
                        "  Front Left: Playback 255 [100%] [0.00dB]\n"
                        "  Front Right: Playback 255 [100%] [0.00dB]\n"
                    )
                },
                "pactl_info": {"output": "pactl_missing"},
            }
        )

        self.assertEqual(summary["status"], "pass")
        self.assertEqual(summary["label"], "정상")
        self.assertIn("Generic Analog", summary["evidence"])
        self.assertIn("87%", summary["evidence"])
        self.assertIn("오디오 장치", summary["evidence"])

    def test_overview_render_mentions_each_component(self) -> None:
        rendered = _render_device_status_overview_result(
            device_name="MB2-C00419",
            device_info={
                "version": "2.11.300",
                "hospitalName": "아이사랑산부인과의원(부산)",
                "roomName": "2진료실",
            },
            ping_result={
                "status": True,
                "message": "Command dispatched to device",
            },
            ssh_ready=True,
            ssh_reason="ready",
            audio_summary={
                "label": "정상",
                "status": "pass",
                "summary": "미니PC 오디오 장치와 음량 설정은 정상으로 보여",
                "overviewDetail": "오디오 장치 `Generic Analog`, `Generic Digital` / 음량 `Master 87% on, PCM 100%`",
                "deviceLabelsText": "`Generic Analog`, `Generic Digital`",
                "volumeText": "`Master 87% on, PCM 100%`",
            },
            pm2_summary={
                "label": "정상",
                "status": "pass",
                "summary": "PM2 기준 mommybox-v2 와 mommybox-agent 앱이 정상 실행 중이야",
                "overviewDetail": "mommybox-v2 v2.11.300 online / mommybox-agent v2.0.0 online",
            },
            storage_summary={
                "label": "정상",
                "status": "pass",
                "summary": "TrashCan 용량은 아직 안정 범위야",
                "overviewDetail": "경로 `AppData/TrashCan` / 폴더 `2.8 GB` (`0.6%`) / 파일 `167개` / 30일 초과 `0개`",
                "diskLabel": "정상",
                "diskOverviewDetail": "경로 `/` / 사용량 `28%` / 여유 `319.0 GB` / 전체 `465.5 GB` / 파일시스템 `/dev/nvme0n1p2`",
                "trashcanLabel": "정상",
                "trashcanOverviewDetail": "경로 `AppData/TrashCan` / 폴더 `2.8 GB` (`0.6%`) / 파일 `167개` / 30일 초과 `0개`",
            },
            captureboard_summary={
                "label": "정상",
                "status": "pass",
                "summary": "캡처보드 USB와 비디오 장치가 같이 보여",
                "overviewDetail": "MDA 타입 `YUH01` / USB `YUH01` / /dev/video `1개`",
            },
            led_summary={
                "label": "정상",
                "status": "pass",
                "summary": "LED 장치 USB 연결은 정상으로 보여",
                "overviewDetail": "LED USB 감지 / 시리얼 경로 `1개`",
            },
        )

        self.assertIn("*장비 상태 점검*", rendered)
        self.assertIn("*오디오*", rendered)
        self.assertIn("• 소리 출력: *정상* | 장치 `Generic Analog`, `Generic Digital` / 음량 `Master 87% on, PCM 100%`", rendered)
        self.assertIn("*런타임*", rendered)
        self.assertIn("• ping 전송 여부: 🔵 *성공* | 장비로 ping 전송 완료", rendered)
        self.assertIn("• SSH 연결 상태: 🔵 *연결 가능*", rendered)
        self.assertIn("• 초음파 영상 다운로드 가능 상태: 🔵 *가능*", rendered)
        self.assertIn("• pm2 앱: *정상* | mommybox-v2 v2.11.300 online / mommybox-agent v2.0.0 online", rendered)
        self.assertIn("• 디스크 용량: *정상* | 경로 `/` / 사용량 `28%` / 여유 `319.0 GB` / 전체 `465.5 GB` / 파일시스템 `/dev/nvme0n1p2`", rendered)
        self.assertIn("• TrashCan 용량: *정상* | 경로 `AppData/TrashCan` / 폴더 `2.8 GB` (`0.6%`) / 파일 `167개` / 30일 초과 `0개`", rendered)
        self.assertIn("*하드웨어*", rendered)
        self.assertIn("• 캡처보드: *정상* | MDA 타입 `YUH01` / USB `YUH01` / /dev/video `1개`", rendered)
        self.assertIn("• LED: *정상* | LED USB 감지 / 시리얼 경로 `1개`", rendered)
        self.assertIn("*종합*", rendered)
        self.assertIn("• 상태: *정상*", rendered)
        self.assertIn("• 안내: 실제 소리 출력 테스트는 `MB2-C00419 장비 소리 출력 점검`으로 다시 명령해", rendered)

    def test_overview_render_marks_ssh_and_download_unavailable_when_ssh_not_ready(self) -> None:
        rendered = _render_device_status_overview_result(
            device_name="MB2-C00419",
            device_info={
                "deviceIsConnected": False,
                "isConnected": False,
            },
            ping_result={
                "status": False,
                "message": "Device is offline",
            },
            ssh_ready=False,
            ssh_reason="agent_ssh_not_ready",
            audio_summary=None,
            pm2_summary=None,
            storage_summary=None,
            captureboard_summary=None,
            led_summary=None,
        )

        self.assertIn("• ping 전송 여부: 🔴 *실패* | 장비 offline", rendered)
        self.assertIn("• SSH 연결 상태: 🔴 *연결 불가*", rendered)
        self.assertIn("• 초음파 영상 다운로드 가능 상태: 🔴 *불가*", rendered)
        self.assertIn("• 디스크 용량: *점검 불가*", rendered)
        self.assertIn("• TrashCan 용량: *점검 불가*", rendered)
        self.assertIn("• 판단: 박서가 직접 ping도 못 보냈어. 장비 자체가 MDA 기준 offline이라 병원 네트워크나 장비 연결 문제를 먼저 봐야 해", rendered)
        self.assertIn("• 조치: 장비 전원, 병원 네트워크, 앱 연결 상태를 먼저 확인한 뒤 다시 점검해", rendered)

    def test_overview_render_explains_ping_success_but_ssh_unavailable_in_detail(self) -> None:
        rendered = _render_device_status_overview_result(
            device_name="MB2-C00419",
            device_info={
                "deviceIsConnected": True,
                "isConnected": True,
                "agentSsh": None,
            },
            ping_result={
                "status": True,
                "message": "Command dispatched to device",
            },
            ssh_ready=False,
            ssh_reason="agent_ssh_not_ready",
            audio_summary=None,
            pm2_summary=None,
            storage_summary=None,
            captureboard_summary=None,
            led_summary=None,
        )

        self.assertIn("• ping 전송 여부: 🔵 *성공* | 장비로 ping 전송 완료", rendered)
        self.assertIn("• SSH 연결 상태: 🔴 *연결 불가*", rendered)
        self.assertIn("• 디스크 용량: *점검 불가*", rendered)
        self.assertIn("• TrashCan 용량: *점검 불가*", rendered)
        self.assertIn("• 판단: 장비는 온라인인데 SSH 접속이 안 열려 있어 보여. 병원 네트워크 쪽 문제로 보는 게 맞고, 특히 SSH 방화벽이나 포트 제한 가능성이 커", rendered)
        self.assertIn("• 조치: 병원 쪽 방화벽, 포트 22 제한, 원격 접속 허용 정책을 확인해", rendered)


class DeviceRemoteAccessAndMemoryPatchExecutionTests(unittest.TestCase):
    def test_status_overview_probe_includes_ping_result_when_ssh_not_ready(self) -> None:
        with (
            patch(
                "boxer_company.routers.device_status_probe._send_mda_device_ping",
                return_value={
                    "status": False,
                    "message": "Device is offline",
                    "affected": 0,
                    "command": "ping",
                },
            ),
            patch(
                "boxer_company.routers.device_status_probe._collect_runtime_checks",
                return_value=(
                    {
                        "route": "device_status_probe",
                        "source": "mda_graphql+ssh",
                        "request": {
                            "deviceName": "MB2-C00419",
                            "component": "all",
                        },
                        "ssh": {
                            "ready": False,
                            "reason": "agent_ssh_not_ready",
                        },
                    },
                    {
                        "deviceName": "MB2-C00419",
                        "version": "2.11.300",
                        "deviceIsConnected": False,
                        "isConnected": False,
                    },
                    {},
                ),
            ),
            patch(
                "boxer_company.routers.device_status_probe._select_remote_access_notion_references",
                return_value=[
                    {
                        "title": "병원 방화벽으로 MDA/원격 접속이 안 될 때",
                        "url": "https://www.notion.so/MDA-322cf826870c812aaee6f9c62838b486",
                    }
                ],
            ),
        ):
            result_text, evidence = _probe_device_status_overview("MB2-C00419")

        self.assertIn("• ping 전송 여부: 🔴 *실패* | 장비 offline", result_text)
        self.assertIn("• 판단: 박서가 직접 ping도 못 보냈어. 장비 자체가 MDA 기준 offline이라 병원 네트워크나 장비 연결 문제를 먼저 봐야 해", result_text)
        self.assertIn("*함께 참고할 문서*", result_text)
        self.assertIn("병원 방화벽으로 MDA/원격 접속이 안 될 때", result_text)
        self.assertFalse(evidence["ping"]["status"])
        self.assertEqual(evidence["notionReferences"][0]["title"], "병원 방화벽으로 MDA/원격 접속이 안 될 때")
        self.assertEqual(evidence["overview"]["audio"], None)

    def test_remote_access_probe_handles_ping_success_without_detail_state(self) -> None:
        with (
            patch(
                "boxer_company.routers.device_status_probe._get_mda_device_detail",
                return_value=None,
            ),
            patch(
                "boxer_company.routers.device_status_probe._send_mda_device_ping",
                return_value={
                    "status": True,
                    "message": "Command dispatched to device",
                    "affected": 1,
                    "command": "ping",
                },
            ),
        ):
            result_text, _ = _probe_device_remote_access("MB2-C00419")

        self.assertIn("장비 ping 전송은 성공했지만 상세 상태를 아직 못 읽었어", result_text)
        self.assertIn("잠시 후 다시 점검하거나 MDA 장비 상세 상태를 확인해", result_text)

    def test_remote_access_probe_points_to_network_when_ping_fails(self) -> None:
        with (
            patch(
                "boxer_company.routers.device_status_probe._get_mda_device_detail",
                return_value={
                    "deviceName": "MB2-C00419",
                    "version": "2.11.300",
                    "hospitalName": "아이사랑산부인과의원(부산)",
                    "roomName": "2진료실",
                    "deviceIsConnected": False,
                    "isConnected": False,
                    "agentSsh": None,
                },
            ),
            patch(
                "boxer_company.routers.device_status_probe._send_mda_device_ping",
                return_value={
                    "status": False,
                    "message": "Device is offline",
                    "affected": 0,
                    "command": "ping",
                },
            ),
            patch(
                "boxer_company.routers.device_status_probe._select_remote_access_notion_references",
                return_value=[
                    {
                        "title": "병원 방화벽으로 MDA/원격 접속이 안 될 때",
                        "url": "https://www.notion.so/MDA-322cf826870c812aaee6f9c62838b486",
                    },
                    {
                        "title": "초음파 영상 업로드 안됨(네트워크 이슈)",
                        "url": "https://www.notion.so/390aa941853c4c279e545de06e49dce7?pvs=21",
                    },
                ],
            ),
        ):
            result_text, evidence = _probe_device_remote_access("MB2-C00419")

        self.assertIn("*장비 원격 접속 점검*", result_text)
        self.assertIn("• ping 전송 여부: 🔴 *실패* | 장비 offline", result_text)
        self.assertIn("병원 네트워크나 장비 연결 문제를 먼저 봐야 해", result_text)
        self.assertIn("장비 전원, 병원 네트워크, 앱 연결 상태를 먼저 확인", result_text)
        self.assertIn("*함께 참고할 문서*", result_text)
        self.assertIn("병원 방화벽으로 MDA/원격 접속이 안 될 때", result_text)
        self.assertFalse(evidence["ping"]["status"])

    def test_remote_access_probe_points_to_ssh_policy_when_ping_succeeds_but_ssh_missing(self) -> None:
        with (
            patch(
                "boxer_company.routers.device_status_probe._get_mda_device_detail",
                return_value={
                    "deviceName": "MB2-C00419",
                    "version": "2.11.300",
                    "hospitalName": "아이사랑산부인과의원(부산)",
                    "roomName": "2진료실",
                    "deviceIsConnected": True,
                    "isConnected": True,
                    "agentSsh": {
                        "host": "",
                        "port": None,
                    },
                },
            ),
            patch(
                "boxer_company.routers.device_status_probe._send_mda_device_ping",
                return_value={
                    "status": True,
                    "message": "Command dispatched to device",
                    "affected": 1,
                    "command": "ping",
                },
            ),
        ):
            result_text, evidence = _probe_device_remote_access("MB2-C00419")

        self.assertIn("• ping 전송 여부: 🔵 *성공* | 장비로 ping 전송 완료", result_text)
        self.assertIn("• SSH 준비 상태: 🔴 *미준비*", result_text)
        self.assertIn("병원 네트워크 쪽 문제로 보는 게 맞고, 특히 SSH 방화벽이나 포트 제한 가능성이 커", result_text)
        self.assertIn("방화벽, 포트 22 제한, 원격 접속 허용 정책", result_text)
        self.assertTrue(evidence["ping"]["status"])

    def test_remote_access_probe_points_to_agent_when_device_ping_succeeds_but_agent_offline(self) -> None:
        with (
            patch(
                "boxer_company.routers.device_status_probe._get_mda_device_detail",
                return_value={
                    "deviceName": "MB2-C00419",
                    "version": "2.11.300",
                    "hospitalName": "아이사랑산부인과의원(부산)",
                    "roomName": "2진료실",
                    "deviceIsConnected": True,
                    "isConnected": False,
                    "agentSsh": None,
                },
            ),
            patch(
                "boxer_company.routers.device_status_probe._send_mda_device_ping",
                return_value={
                    "status": True,
                    "message": "Command dispatched to device",
                    "affected": 1,
                    "command": "ping",
                },
            ),
        ):
            result_text, _ = _probe_device_remote_access("MB2-C00419")

        self.assertIn("장비 통신은 보이는데 원격 접속 준비가 안 된 상태야", result_text)
        self.assertIn("장비 쪽 원격 접속 서비스 상태를 먼저 확인해", result_text)

    def test_patches_device_memory_only_when_precheck_is_not_4gb(self) -> None:
        client = _FakeSshClient(
            [
                {
                    "exit_status": 0,
                    "stdout": "max_memory_restart: 209715200",
                    "stderr": "",
                },
                {
                    "exit_status": 0,
                    "stdout": "deleted\nstarted\nsaved",
                    "stderr": "",
                },
                {
                    "exit_status": 0,
                    "stdout": "max_memory_restart: 4294967296",
                    "stderr": "",
                },
            ]
        )

        with (
            patch(
                "boxer_company.routers.device_status_probe._wait_for_mda_device_agent_ssh",
                return_value={
                    "ready": True,
                    "pollCount": 0,
                    "reusedExisting": True,
                    "device": {
                        "deviceName": "MB2-C00419",
                        "version": "2.11.300",
                        "hospitalName": "아이사랑산부인과의원(부산)",
                        "roomName": "2진료실",
                        "agentSsh": {
                            "host": "127.0.0.1",
                            "port": 22,
                        },
                    },
                },
            ),
            patch(
                "boxer_company.routers.device_status_probe._connect_device_ssh_client",
                return_value={
                    "ok": True,
                    "client": client,
                },
            ),
        ):
            result_text, evidence = _patch_device_pm2_memory("MB2-C00419")

        self.assertIn("*장비 메모리 패치*", result_text)
        self.assertIn("• 판정: *완료*", result_text)
        self.assertIn("• 사전 확인: `max_memory_restart=209715200 (200.0 MB)`", result_text)
        self.assertIn("• 실행 후 확인: `max_memory_restart=4294967296 (4.0 GB)`", result_text)
        self.assertEqual(evidence["precheck"]["display"], "209715200 (200.0 MB)")
        self.assertEqual(evidence["execution"]["summary"], "메모리 패치 실행")
        self.assertTrue(evidence["verification"]["hasExpectedLimit"])
        self.assertEqual(len(client.commands), 3)
        self.assertIn("pm2 prettylist | grep max_memory_restart", client.commands[0])
        self.assertTrue(client.closed)

    def test_skips_memory_patch_when_precheck_is_already_4gb(self) -> None:
        client = _FakeSshClient(
            [
                {
                    "exit_status": 0,
                    "stdout": "max_memory_restart: 4294967296",
                    "stderr": "",
                },
            ]
        )

        with (
            patch(
                "boxer_company.routers.device_status_probe._wait_for_mda_device_agent_ssh",
                return_value={
                    "ready": True,
                    "pollCount": 0,
                    "reusedExisting": True,
                    "device": {
                        "deviceName": "MB2-C00419",
                        "version": "2.11.300",
                        "hospitalName": "아이사랑산부인과의원(부산)",
                        "roomName": "2진료실",
                        "agentSsh": {
                            "host": "127.0.0.1",
                            "port": 22,
                        },
                    },
                },
            ),
            patch(
                "boxer_company.routers.device_status_probe._connect_device_ssh_client",
                return_value={
                    "ok": True,
                    "client": client,
                },
            ),
        ):
            result_text, evidence = _patch_device_pm2_memory("MB2-C00419")

        self.assertIn("• 판정: *정상*", result_text)
        self.assertIn("• 실행: 이미 정상이라 생략", result_text)
        self.assertIn("이미 4GB 메모리 설정이라 메모리 패치를 생략했어", result_text)
        self.assertTrue(evidence["precheck"]["hasExpectedLimit"])
        self.assertIsNone(evidence["execution"])
        self.assertIsNone(evidence["verification"])
        self.assertEqual(len(client.commands), 1)
        self.assertTrue(client.closed)


class _FakeSshChannel:
    def __init__(self, exit_status: int) -> None:
        self._exit_status = exit_status
        self.closed = False

    def recv_exit_status(self) -> int:
        return self._exit_status

    def close(self) -> None:
        self.closed = True


class _FakeSshStream:
    def __init__(self, text: str, exit_status: int) -> None:
        self._text = text
        self.channel = _FakeSshChannel(exit_status)
        self.closed = False

    def read(self) -> bytes:
        return self._text.encode("utf-8")

    def close(self) -> None:
        self.closed = True


class _FakeSshClient:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self._responses = list(responses)
        self.commands: list[str] = []
        self.closed = False
        self.streams: list[_FakeSshStream] = []

    def exec_command(self, command: str, timeout: int | None = None):
        del timeout
        self.commands.append(command)
        response = self._responses.pop(0)
        exit_status = int(response.get("exit_status") or 0)
        stdout = _FakeSshStream(str(response.get("stdout") or ""), exit_status)
        stderr = _FakeSshStream(str(response.get("stderr") or ""), exit_status)
        self.streams.extend([stdout, stderr])
        return None, stdout, stderr

    def close(self) -> None:
        self.closed = True


class DeviceStatusProbeSshLifecycleTests(unittest.TestCase):
    def test_closes_ssh_streams_after_command(self) -> None:
        client = _FakeSshClient(
            [
                {
                    "exit_status": 0,
                    "stdout": "ok",
                    "stderr": "",
                }
            ]
        )

        result = _run_remote_ssh_command(
            client,
            command="echo ok",
            summary="테스트 명령",
            timeout_sec=5,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(len(client.streams), 2)
        self.assertTrue(all(stream.closed for stream in client.streams))
        self.assertTrue(all(stream.channel.closed for stream in client.streams))


if __name__ == "__main__":
    unittest.main()
