import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from boxer_company import daily_device_round as rounder


def _build_status_payload(*, ssh_ready: bool = True, overall: str = "정상") -> dict:
    status_map = {
        "정상": ("pass", "정상"),
        "확인 필요": ("warning", "확인 필요"),
        "이상": ("fail", "이상"),
    }
    component_status, component_label = status_map.get(overall, ("warning", "확인 필요"))
    return {
        "ssh": {"ready": ssh_ready, "reason": "ready" if ssh_ready else "agent_ssh_not_ready"},
        "overview": {
            "audio": {"status": component_status, "label": component_label},
            "pm2": {"status": component_status, "label": component_label},
            "storage": {
                "status": component_status,
                "label": component_label,
                "directorySharePercent": 10.0,
                "expiredFileCount": 0,
                "fileCount": 12,
                "directorySizeBytes": 2 * 1024 * 1024 * 1024,
                "displayPath": "AppData/TrashCan",
            },
            "captureboard": {"status": component_status, "label": component_label},
            "led": {"status": component_status, "label": component_label},
        },
    }


def _build_update_payload(
    *,
    box_version: str,
    latest_box_version: str,
    agent_version: str,
    agent_latest: bool,
    connected: bool = True,
    gate_ok: bool = True,
) -> dict:
    return {
        "device": {
            "deviceName": "MB2-C00419",
            "version": box_version,
            "hospitalName": "테스트병원",
            "roomName": "1진료실",
            "isConnected": connected,
        },
        "latestVersion": latest_box_version,
        "boxRuntime": {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-v2",
                "status": "online",
                "version": box_version,
            },
        },
        "agentRuntime": {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-agent",
                "status": "online",
                "version": agent_version,
            },
            "repo": {
                "available": True,
                "reason": "ok",
                "head": "abcdef0",
                "originMain": "abcdef0" if agent_latest else "1234567",
                "branch": "main",
                "packageVersion": agent_version,
                "latest": agent_latest,
            },
        },
        "agentGate": {
            "ok": gate_ok,
            "version": agent_version,
            "reason": "선행조건 충족" if gate_ok else "에이전트 2.0 이상이 필요해",
        },
    }


class DailyDeviceRoundSelectionTests(unittest.TestCase):
    def test_excludes_prefixed_hospitals_from_candidates(self) -> None:
        self.assertTrue(rounder._is_daily_device_round_excluded_hospital_name("4_입고"))
        self.assertTrue(rounder._is_daily_device_round_excluded_hospital_name("0_테스트"))
        self.assertTrue(rounder._is_daily_device_round_excluded_hospital_name("7_창고"))
        self.assertFalse(rounder._is_daily_device_round_excluded_hospital_name("8_운영"))
        self.assertFalse(rounder._is_daily_device_round_excluded_hospital_name("장비 창고"))
        self.assertFalse(rounder._is_daily_device_round_excluded_hospital_name("루이스산부인과의원(동작)"))

    def test_selects_next_hospital_after_last_seq_and_wraps(self) -> None:
        candidates = [
            {"hospitalSeq": 10, "hospitalName": "A", "deviceCount": 2},
            {"hospitalSeq": 20, "hospitalName": "B", "deviceCount": 3},
            {"hospitalSeq": 30, "hospitalName": "C", "deviceCount": 1},
        ]

        selected = rounder._select_daily_device_round_hospital(
            candidates,
            state={"lastHospitalSeq": 20},
        )
        wrapped = rounder._select_daily_device_round_hospital(
            candidates,
            state={"lastHospitalSeq": 30},
        )

        self.assertEqual(selected["hospitalSeq"], 30)
        self.assertEqual(wrapped["hospitalSeq"], 10)
        self.assertEqual(
            rounder._resolve_next_daily_device_round_hospital_seq(candidates, 30),
            10,
        )

    def test_skips_already_processed_hospitals_in_same_window(self) -> None:
        candidates = [
            {"hospitalSeq": 10, "hospitalName": "A", "deviceCount": 2},
            {"hospitalSeq": 20, "hospitalName": "B", "deviceCount": 3},
            {"hospitalSeq": 30, "hospitalName": "C", "deviceCount": 1},
        ]

        selected = rounder._select_daily_device_round_hospital(
            candidates,
            state={
                "lastHospitalSeq": 10,
                "nextHospitalSeq": 20,
                "processedHospitalSeqs": [10, 20],
            },
        )
        completed = rounder._select_daily_device_round_hospital(
            candidates,
            state={
                "lastHospitalSeq": 30,
                "nextHospitalSeq": 10,
                "processedHospitalSeqs": [10, 20, 30],
            },
        )

        self.assertEqual(selected["hospitalSeq"], 30)
        self.assertIsNone(completed)


class DailyDeviceRoundExecutionTests(unittest.TestCase):
    def test_defers_priority_when_network_is_unavailable(self) -> None:
        priority = rounder._build_daily_device_round_priority(
            _build_status_payload(ssh_ready=False)
        )

        self.assertFalse(priority["eligible"])
        self.assertEqual(priority["label"], "판단 보류")
        self.assertEqual(priority["reason"], "네트워크 연결 불가로 이상 징후 판단 보류")

    def test_marks_pm2_failure_as_high_priority_when_network_is_available(self) -> None:
        status_payload = _build_status_payload(overall="정상")
        status_payload["overview"]["pm2"] = {"status": "fail", "label": "이상"}

        priority = rounder._build_daily_device_round_priority(status_payload)

        self.assertTrue(priority["eligible"])
        self.assertEqual(priority["label"], "높음")
        self.assertEqual(priority["reason"], "pm2 이상")

    def test_marks_storage_warning_as_medium_priority_when_network_is_available(self) -> None:
        status_payload = _build_status_payload(overall="정상")
        status_payload["overview"]["storage"] = {"status": "warning", "label": "확인 필요"}

        priority = rounder._build_daily_device_round_priority(status_payload)

        self.assertTrue(priority["eligible"])
        self.assertEqual(priority["label"], "중간")
        self.assertEqual(priority["reason"], "용량 확인 필요")

    def test_builds_issue_line_with_component_summary_before_generic_reason(self) -> None:
        device_line = rounder._build_daily_device_round_device_line(
            {
                "deviceName": "MB2-D00268",
                "roomName": "4층 3진료실",
                "overallLabel": "확인 필요",
                "priorityReason": "용량 확인 필요",
                "componentLabels": {
                    "audio": "정상",
                    "pm2": "정상",
                    "storage": "확인 필요",
                    "captureboard": "정상",
                    "led": "정상",
                },
                "statusPayload": {
                    "ssh": {"ready": True, "reason": "ready"},
                    "overview": {
                        "storage": {
                            "status": "warning",
                            "label": "확인 필요",
                            "summary": "TrashCan 용량이 빠르게 커지고 있어",
                            "directorySharePercent": 43.8,
                            "directorySizeBytes": 95.6 * 1024**3,
                            "expiredFileCount": 4520,
                            "cleanupAgeDays": 30,
                            "overviewDetail": "경로 `AppData/TrashCan` / 폴더 `95.6 GB` (43.8%) / 파일 `5728개` / 30일 초과 `4520개`",
                        }
                    },
                },
                "trashcanCleanup": {
                    "status": "disabled",
                    "label": "꺼짐",
                    "detail": "기준 `60%` 미만 | 현재 `43.8%`",
                    "required": False,
                    "executed": False,
                },
                "finalPlan": {
                    "agent": {
                        "reason": "에이전트 정상",
                        "currentVersion": "2.0.0",
                        "isLatest": True,
                        "shouldUpdate": False,
                    },
                    "box": {
                        "reason": "박스 최신",
                        "currentVersion": "2.11.300",
                        "latestVersion": "2.11.300",
                        "alreadyLatest": True,
                        "shouldUpdate": False,
                    },
                },
                "agentAction": None,
                "boxAction": None,
            }
        )

        self.assertIn(
            "  *이슈*  TrashCan 용량이 빠르게 커지고 있어 | 현재 `43.8%` / 폴더 `95.6 GB` / `30일` 초과 `4,520개`",
            device_line,
        )
        self.assertNotIn("  *이슈*  용량 확인 필요", device_line)

    def test_build_update_plan_falls_back_to_agent_runtime_gate(self) -> None:
        plan = rounder._build_daily_device_round_update_plan(
            {
                "device": {
                    "deviceName": "MB2-C00419",
                    "version": "2.11.299",
                    "isConnected": True,
                },
                "latestVersion": "2.11.300",
                "boxRuntime": {
                    "process": {
                        "name": "mommybox-v2",
                        "status": "online",
                        "version": "2.11.299",
                    }
                },
                "agentRuntime": {
                    "process": {
                        "name": "mommybox-agent",
                        "status": "online",
                        "version": "2.0.0",
                    },
                    "repo": {
                        "available": True,
                        "latest": True,
                        "packageVersion": "2.0.0",
                    },
                },
            }
        )

        self.assertTrue(plan["agent"]["isLatest"])
        self.assertFalse(plan["agent"]["shouldUpdate"])
        self.assertEqual(plan["agent"]["reason"], "에이전트 정상")
        self.assertTrue(plan["box"]["gateOk"])
        self.assertTrue(plan["box"]["shouldUpdate"])
        self.assertEqual(plan["box"]["reason"], "박스 2.11.299 -> 2.11.300")

    def test_build_update_plan_does_not_repeat_agent_install_for_online_v2_runtime(self) -> None:
        plan = rounder._build_daily_device_round_update_plan(
            {
                "device": {
                    "deviceName": "MB2-C00419",
                    "version": "2.11.299",
                    "isConnected": True,
                },
                "latestVersion": "2.11.300",
                "boxRuntime": {
                    "process": {
                        "name": "mommybox-v2",
                        "status": "online",
                        "version": "2.11.299",
                    }
                },
                "agentRuntime": {
                    "process": {
                        "name": "mommybox-agent",
                        "status": "online",
                        "version": "2.0.0",
                    },
                    "repo": {
                        "available": True,
                        "latest": False,
                        "packageVersion": "2.0.0",
                    },
                },
                "agentGate": {
                    "ok": True,
                    "version": "2.0.0",
                    "source": "pm2",
                    "reason": "에이전트 2.0.0 확인돼서 박스 업데이트 진행 가능해",
                    "minimumVersion": "2.0.0",
                },
            }
        )

        self.assertTrue(plan["agent"]["isHealthy"])
        self.assertFalse(plan["agent"]["shouldUpdate"])
        self.assertEqual(plan["agent"]["reason"], "에이전트 정상")

    def test_build_update_plan_updates_agent_when_repo_missing_but_pm2_version_is_too_old(self) -> None:
        plan = rounder._build_daily_device_round_update_plan(
            {
                "device": {
                    "deviceName": "MB2-C00819",
                    "version": "",
                    "isConnected": True,
                },
                "latestVersion": "2.11.300",
                "boxRuntime": {
                    "process": {
                        "name": "mommybox-v2",
                        "status": "online",
                        "version": "2.9.276",
                    }
                },
                "agentRuntime": {
                    "process": {
                        "name": "mommybox-v2-agent",
                        "status": "online",
                        "version": "1.5.0",
                    },
                    "repo": {
                        "available": False,
                        "reason": "repo_missing",
                        "packageVersion": "",
                        "latest": False,
                    },
                },
                "agentGate": {
                    "ok": False,
                    "version": "1.5.0",
                    "source": "pm2",
                    "reason": "에이전트 1.5.0라서 박스 업데이트를 막았어. 먼저 에이전트 2.0 이상으로 올려줘",
                    "minimumVersion": "2.0.0",
                },
            }
        )

        self.assertTrue(plan["agent"]["shouldUpdate"])
        self.assertEqual(plan["agent"]["reason"], "에이전트 1.5.0 업데이트 필요")
        self.assertFalse(plan["box"]["shouldUpdate"])

    @patch("boxer_company.daily_device_round._request_device_box_update")
    @patch("boxer_company.daily_device_round._request_device_agent_update")
    @patch("boxer_company.daily_device_round._query_device_update_status")
    @patch("boxer_company.daily_device_round._probe_device_status_overview")
    def test_runs_agent_then_box_update_when_enabled(
        self,
        mock_probe_device_status_overview,
        mock_query_device_update_status,
        mock_request_device_agent_update,
        mock_request_device_box_update,
    ) -> None:
        mock_probe_device_status_overview.return_value = (
            "status text",
            _build_status_payload(overall="정상"),
        )
        mock_query_device_update_status.side_effect = [
            (
                "initial update status",
                _build_update_payload(
                    box_version="3.2.0",
                    latest_box_version="3.2.10",
                    agent_version="1.9.0",
                    agent_latest=False,
                    gate_ok=False,
                ),
            ),
            (
                "after agent update",
                _build_update_payload(
                    box_version="3.2.0",
                    latest_box_version="3.2.10",
                    agent_version="2.0.0",
                    agent_latest=True,
                    gate_ok=True,
                ),
            ),
            (
                "after box update",
                _build_update_payload(
                    box_version="3.2.10",
                    latest_box_version="3.2.10",
                    agent_version="2.0.0",
                    agent_latest=True,
                    gate_ok=True,
                ),
            ),
        ]
        mock_request_device_agent_update.return_value = (
            "agent update result",
            {
                "route": "device_agent_update",
                "payload": {
                    "precheck": {
                        "process": {
                            "version": "1.9.0",
                        },
                        "repo": {
                            "packageVersion": "1.9.0",
                        },
                    }
                },
                "dispatch": {"status": True},
                "wait": {"ok": True, "status": "completed"},
            },
        )
        mock_request_device_box_update.return_value = (
            "box update result",
            {
                "route": "device_box_update",
                "payload": {
                    "device": {
                        "version": "3.2.0",
                    },
                    "precheck": {
                        "process": {
                            "version": "3.2.0",
                        }
                    },
                },
                "dispatch": {"status": True},
                "wait": {"ok": True, "status": "completed"},
            },
        )

        result = rounder._run_daily_device_round_for_device(
            "MB2-C00419",
            auto_update_agent=True,
            auto_update_box=True,
        )

        mock_request_device_agent_update.assert_called_once_with(
            "MB2-C00419 에이전트 업데이트",
            device_name="MB2-C00419",
        )
        mock_request_device_box_update.assert_called_once_with(
            "MB2-C00419 장비 업데이트",
            device_name="MB2-C00419",
        )
        self.assertEqual(result["overallLabel"], "정상")
        self.assertTrue(result["initialPlan"]["agent"]["shouldUpdate"])
        self.assertTrue(result["agentAction"]["ok"])
        self.assertTrue(result["boxAction"]["ok"])
        self.assertTrue(result["finalPlan"]["box"]["alreadyLatest"])
        self.assertEqual(result["priorityLabel"], "정상")
        self.assertEqual(result["priorityReason"], "원격 점검상 이상 징후 없음")
        self.assertEqual(result["agentActionText"], "에이전트 업데이트 완료")
        self.assertEqual(result["boxActionText"], "박스 업데이트 완료")

    @patch("boxer_company.daily_device_round._request_device_agent_update")
    @patch("boxer_company.daily_device_round._query_device_update_status")
    @patch("boxer_company.daily_device_round._probe_device_status_overview")
    def test_runs_agent_update_when_repo_missing_but_pm2_version_is_too_old(
        self,
        mock_probe_device_status_overview,
        mock_query_device_update_status,
        mock_request_device_agent_update,
    ) -> None:
        mock_probe_device_status_overview.return_value = (
            "status text",
            _build_status_payload(overall="정상"),
        )
        mock_query_device_update_status.side_effect = [
            (
                "initial update status",
                {
                    "device": {
                        "deviceName": "MB2-C00819",
                        "version": "",
                        "hospitalName": "통일산부인과의원(서초)",
                        "roomName": "2진료실",
                        "isConnected": True,
                    },
                    "latestVersion": "2.11.300",
                    "boxRuntime": {
                        "process": {
                            "name": "mommybox-v2",
                            "status": "online",
                            "version": "2.9.276",
                        }
                    },
                    "agentRuntime": {
                        "process": {
                            "name": "mommybox-v2-agent",
                            "status": "online",
                            "version": "1.5.0",
                        },
                        "repo": {
                            "available": False,
                            "reason": "repo_missing",
                            "packageVersion": "",
                            "latest": False,
                        },
                    },
                    "agentGate": {
                        "ok": False,
                        "version": "1.5.0",
                        "source": "pm2",
                        "reason": "에이전트 1.5.0라서 박스 업데이트를 막았어. 먼저 에이전트 2.0 이상으로 올려줘",
                        "minimumVersion": "2.0.0",
                    },
                },
            ),
            (
                "final update status",
                {
                    "device": {
                        "deviceName": "MB2-C00819",
                        "version": "",
                        "hospitalName": "통일산부인과의원(서초)",
                        "roomName": "2진료실",
                        "isConnected": True,
                    },
                    "latestVersion": "2.11.300",
                    "boxRuntime": {
                        "process": {
                            "name": "mommybox-v2",
                            "status": "online",
                            "version": "2.9.276",
                        }
                    },
                    "agentRuntime": {
                        "process": {
                            "name": "mommybox-v2-agent",
                            "status": "online",
                            "version": "2.0.0",
                        },
                        "repo": {
                            "available": False,
                            "reason": "repo_missing",
                            "packageVersion": "",
                            "latest": False,
                        },
                    },
                    "agentGate": {
                        "ok": True,
                        "version": "2.0.0",
                        "source": "pm2",
                        "reason": "에이전트 2.0.0 확인돼서 박스 업데이트 진행 가능해",
                        "minimumVersion": "2.0.0",
                    },
                },
            ),
        ]
        mock_request_device_agent_update.return_value = (
            "agent update result",
            {
                "route": "device_agent_update",
                "payload": {
                    "precheck": {
                        "process": {
                            "version": "1.5.0",
                        }
                    },
                },
                "dispatch": {"status": True},
                "wait": {"ok": True, "status": "completed"},
            },
        )

        result = rounder._run_daily_device_round_for_device(
            "MB2-C00819",
            auto_update_agent=True,
            auto_update_box=False,
        )

        mock_request_device_agent_update.assert_called_once_with(
            "MB2-C00819 에이전트 업데이트",
            device_name="MB2-C00819",
        )
        self.assertTrue(result["initialPlan"]["agent"]["shouldUpdate"])
        self.assertEqual(result["initialPlan"]["agent"]["reason"], "에이전트 1.5.0 업데이트 필요")
        self.assertTrue(result["agentAction"]["ok"])


class DailyDeviceRoundSummaryTests(unittest.TestCase):
    @patch("boxer_company.daily_device_round._run_daily_device_round_for_device")
    @patch("boxer_company.daily_device_round._load_daily_device_round_devices")
    @patch("boxer_company.daily_device_round._load_daily_device_round_hospital_candidates")
    def test_builds_summary_and_next_hospital(
        self,
        mock_load_hospitals,
        mock_load_devices,
        mock_run_for_device,
    ) -> None:
        mock_load_hospitals.return_value = [
            {"hospitalSeq": 10, "hospitalName": "A병원", "deviceCount": 1},
            {"hospitalSeq": 20, "hospitalName": "B병원", "deviceCount": 2},
        ]
        mock_load_devices.return_value = [
            {
                "deviceName": "MB2-C00001",
                "hospitalName": "B병원",
                "roomName": "1진료실",
            },
            {
                "deviceName": "MB2-C00002",
                "hospitalName": "B병원",
                "roomName": "2진료실",
            },
        ]
        mock_run_for_device.side_effect = [
            {
                "deviceName": "MB2-C00001",
                "hospitalName": "B병원",
                "roomName": "1진료실",
                "overallLabel": "정상",
                "priorityEligible": True,
                "priorityScore": 0,
                "priorityLabel": "정상",
                "priorityReason": "원격 점검상 이상 징후 없음",
                "componentLabels": {
                    "audio": "정상",
                    "pm2": "정상",
                    "storage": "정상",
                    "captureboard": "정상",
                    "led": "정상",
                },
                "storageDetails": {
                    "diskLabel": "정상",
                    "diskDetail": "경로 `/` / 사용량 `12%` / 여유 `190.0 GB` / 전체 `218.0 GB` / 파일시스템 `/dev/sda2`",
                    "trashcanLabel": "정상",
                    "trashcanDetail": "경로 `AppData/TrashCan` / 폴더 `2.0 GB` (`0.9%`) / 파일 `20개` / 30일 초과 `0개`",
                },
                "trashcanCleanup": {
                    "status": "disabled",
                    "label": "꺼짐",
                    "detail": "기준 `60%` 미만 | 현재 `10%`",
                    "required": False,
                    "executed": False,
                },
                "initialPlan": {
                    "agent": {"shouldUpdate": False, "isLatest": True, "reason": "에이전트 정상"},
                    "box": {"shouldUpdate": False, "alreadyLatest": True, "reason": "박스 최신"},
                },
                "finalPlan": {
                    "agent": {"shouldUpdate": False, "isLatest": True, "reason": "에이전트 정상"},
                    "box": {"shouldUpdate": False, "alreadyLatest": True, "reason": "박스 최신"},
                },
                "agentAction": None,
                "boxAction": None,
                "agentActionText": "에이전트 정상",
                "boxActionText": "박스 최신",
            },
            {
                "deviceName": "MB2-C00002",
                "hospitalName": "B병원",
                "roomName": "2진료실",
                "overallLabel": "확인 필요",
                "priorityEligible": True,
                "priorityScore": 1,
                "priorityLabel": "낮음",
                "priorityReason": "오디오 확인 필요",
                "componentLabels": {
                    "audio": "확인 필요",
                    "pm2": "정상",
                    "storage": "정상",
                    "captureboard": "정상",
                    "led": "정상",
                },
                "storageDetails": {
                    "diskLabel": "정상",
                    "diskDetail": "경로 `/` / 사용량 `18%` / 여유 `178.0 GB` / 전체 `218.0 GB` / 파일시스템 `/dev/sda2`",
                    "trashcanLabel": "정상",
                    "trashcanDetail": "경로 `AppData/TrashCan` / 폴더 `9.0 GB` (`4.1%`) / 파일 `120개` / 30일 초과 `4개`",
                },
                "trashcanCleanup": {
                    "status": "completed",
                    "label": "성공",
                    "detail": "`30일` 초과 `4개` 삭제 / `9.0 GB` -> `6.0 GB` / 현재 `55%` / 남은 `30일` 초과 `0개`",
                    "required": True,
                    "executed": True,
                },
                "initialPlan": {
                    "agent": {"shouldUpdate": True, "isLatest": False, "reason": "에이전트 1.9.0 업데이트 필요"},
                    "box": {"shouldUpdate": False, "alreadyLatest": False, "reason": "선행조건 미충족"},
                },
                "finalPlan": {
                    "agent": {"shouldUpdate": False, "isLatest": True, "reason": "에이전트 정상"},
                    "box": {"shouldUpdate": True, "alreadyLatest": False, "reason": "박스 3.2.0 -> 3.2.10"},
                },
                "agentAction": {"ok": True, "status": "completed"},
                "boxAction": None,
                "agentActionText": "에이전트 업데이트 완료",
                "boxActionText": "박스 업데이트 후보",
            },
        ]

        summary = rounder._build_daily_device_round_summary(
            now=datetime(2026, 4, 8, 9, 30, tzinfo=ZoneInfo("Asia/Seoul")),
            state={"lastHospitalSeq": 10},
            auto_update_agent=True,
            auto_update_box=False,
        )

        self.assertEqual(summary["hospitalSeq"], 20)
        self.assertEqual(summary["hospitalName"], "B병원")
        self.assertEqual(summary["deviceCount"], 2)
        self.assertEqual(summary["statusCounts"]["정상"], 1)
        self.assertEqual(summary["statusCounts"]["확인 필요"], 1)
        self.assertEqual(summary["updateCounts"]["agentCandidates"], 1)
        self.assertEqual(summary["updateCounts"]["agentUpdated"], 1)
        self.assertEqual(summary["updateCounts"]["boxCandidates"], 1)
        self.assertEqual(summary["cleanupCounts"]["candidates"], 1)
        self.assertEqual(summary["cleanupCounts"]["executed"], 1)
        self.assertEqual(summary["nextHospitalSeq"], 10)

    def test_formats_report_with_hospital_label_and_multiline_device_lines(self) -> None:
        report_text = rounder._format_daily_device_round_report(
            {
                "hospitalSeq": 604,
                "hospitalName": "루이스산부인과의원(동작)",
                "deviceCount": 1,
                "scheduledDeviceCount": 1,
                "autoUpdateAgent": False,
                "autoUpdateBox": False,
                "autoCleanupTrashCan": False,
                "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
                "updateCounts": {
                    "agentCandidates": 0,
                    "agentUpdated": 0,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 1,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "candidates": 0,
                    "executed": 0,
                    "failed": 0,
                },
                "deviceResults": [
                    {
                        "deviceName": "MB2-C01431",
                        "roomName": "1진료실",
                        "overallLabel": "정상",
                        "priorityEligible": True,
                        "priorityScore": 0,
                        "priorityLabel": "정상",
                        "priorityReason": "원격 점검상 이상 징후 없음",
                        "componentLabels": {
                            "audio": "정상",
                            "pm2": "정상",
                            "storage": "정상",
                            "captureboard": "정상",
                            "led": "정상",
                        },
                        "storageDetails": {
                            "diskLabel": "정상",
                            "diskDetail": "경로 `/` / 사용량 `12%` / 여유 `190.0 GB` / 전체 `218.0 GB` / 파일시스템 `/dev/sda2`",
                            "trashcanLabel": "정상",
                            "trashcanDetail": "경로 `AppData/TrashCan` / 폴더 `2.0 GB` (`0.9%`) / 파일 `20개` / 30일 초과 `0개`",
                        },
                        "statusPayload": {
                            "overview": {
                                "storage": {
                                    "filesystemUsedPercent": 12,
                                    "filesystemAvailableBytes": 190 * 1024**3,
                                    "filesystemSizeBytes": 218 * 1024**3,
                                    "directorySizeBytes": 2 * 1024**3,
                                    "directorySharePercent": 0.9,
                                    "fileCount": 20,
                                    "expiredFileCount": 0,
                                    "cleanupAgeDays": 30,
                                }
                            }
                        },
                        "trashcanCleanup": {
                            "label": "꺼짐",
                            "detail": "기준 `60%` 미만 | 현재 `12%`",
                        },
                        "finalPlan": {
                            "agent": {
                                "reason": "에이전트 정상",
                                "currentVersion": "2.0.0",
                                "isLatest": True,
                            },
                            "box": {
                                "reason": "박스 2.11.299 -> 2.11.300",
                                "currentVersion": "2.11.299",
                                "latestVersion": "2.11.300",
                                "alreadyLatest": False,
                                "shouldUpdate": True,
                            },
                        },
                        "agentAction": None,
                        "boxAction": None,
                    }
                ],
            },
            now=datetime(2026, 4, 8, 22, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )

        self.assertIn("일일 장비 순회 점검 & 업데이트 | #604 루이스산부인과의원(동작)", report_text)
        self.assertIn("*#604 루이스산부인과의원(동작)*", report_text)
        self.assertNotIn("• 자동 동작:", report_text)
        self.assertIn("• 🟢 문제 장비 없음", report_text)
        self.assertIn("• 박스 업데이트 대상 `1`", report_text)
        self.assertNotIn("업데이트 성공", report_text)
        self.assertNotIn("디스크 정리 실행", report_text)
        self.assertIn("*문제/작업 장비*", report_text)
        self.assertIn("• *1진료실*  |  *MB2-C01431*  |  🟢 *정상*", report_text)
        self.assertNotIn("*이슈*", report_text)
        self.assertNotIn("*점검*", report_text)
        self.assertNotIn("*디스크 용량*", report_text)
        self.assertNotIn("*TrashCan 용량*", report_text)
        self.assertNotIn("*디스크 정리*", report_text)
        self.assertNotIn("*에이전트 업데이트*", report_text)
        self.assertIn("  *박스 업데이트*  🟠 *업데이트 필요* | 박스 2.11.299 -> 2.11.300", report_text)

    def test_formats_box_success_with_previous_and_final_version(self) -> None:
        report_text = rounder._format_daily_device_round_report(
            {
                "hospitalSeq": 604,
                "hospitalName": "루이스산부인과의원(동작)",
                "deviceCount": 1,
                "scheduledDeviceCount": 1,
                "autoUpdateAgent": True,
                "autoUpdateBox": True,
                "autoCleanupTrashCan": True,
                "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
                "updateCounts": {
                    "agentCandidates": 0,
                    "agentUpdated": 0,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 1,
                    "boxUpdated": 1,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "candidates": 1,
                    "executed": 1,
                    "failed": 0,
                },
                "deviceResults": [
                    {
                        "deviceName": "MB2-C01431",
                        "roomName": "1진료실",
                        "overallLabel": "정상",
                        "priorityEligible": True,
                        "priorityScore": 0,
                        "priorityLabel": "정상",
                        "priorityReason": "원격 점검상 이상 징후 없음",
                        "componentLabels": {
                            "audio": "정상",
                            "pm2": "정상",
                            "storage": "정상",
                            "captureboard": "정상",
                            "led": "정상",
                        },
                        "storageDetails": {
                            "diskLabel": "정상",
                            "diskDetail": "경로 `/` / 사용량 `12%` / 여유 `190.0 GB` / 전체 `218.0 GB` / 파일시스템 `/dev/sda2`",
                            "trashcanLabel": "정상",
                            "trashcanDetail": "경로 `AppData/TrashCan` / 폴더 `9.0 GB` (`4.1%`) / 파일 `120개` / 30일 초과 `4개`",
                        },
                        "statusPayload": {
                            "overview": {
                                "storage": {
                                    "filesystemUsedPercent": 12,
                                    "filesystemAvailableBytes": 190 * 1024**3,
                                    "filesystemSizeBytes": 218 * 1024**3,
                                    "directorySizeBytes": 9 * 1024**3,
                                    "directorySharePercent": 4.1,
                                    "fileCount": 120,
                                    "expiredFileCount": 4,
                                    "cleanupAgeDays": 30,
                                }
                            }
                        },
                        "trashcanCleanup": {
                            "label": "성공",
                            "detail": "`30일` 초과 `4개` 삭제 / `9.0 GB` -> `6.0 GB` / 현재 `55%` / 남은 `30일` 초과 `0개`",
                        },
                        "finalPlan": {
                            "agent": {
                                "reason": "에이전트 정상",
                                "currentVersion": "2.0.0",
                                "isLatest": True,
                            },
                            "box": {
                                "reason": "박스 최신",
                                "currentVersion": "2.11.300",
                                "latestVersion": "2.11.300",
                                "alreadyLatest": True,
                                "shouldUpdate": False,
                            },
                        },
                        "agentAction": None,
                        "boxAction": {
                            "ok": True,
                            "status": "completed",
                            "payload": {
                                "device": {"version": "2.11.299"},
                                "precheck": {
                                    "process": {"version": "2.11.299"},
                                },
                            },
                        },
                    }
                ],
            },
            now=datetime(2026, 4, 8, 22, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )

        self.assertIn("• 🟢 문제 장비 없음", report_text)
        self.assertIn("• 박스 업데이트 성공 `1`", report_text)
        self.assertIn("• 🧹 디스크 정리 실행 `1`", report_text)
        self.assertIn("*문제/작업 장비*", report_text)
        self.assertIn("  *디스크 정리*  *성공* | `30일` 초과 `4개` 삭제 / `9.0 GB` -> `6.0 GB` / 현재 `55%` / 남은 `30일` 초과 `0개`", report_text)
        self.assertNotIn("*에이전트 업데이트*", report_text)
        self.assertIn("  *박스 업데이트*  🟢 *업데이트 완료* | `2.11.299` -> `2.11.300`", report_text)

    def test_formats_ssh_unavailable_device_as_single_guidance_line(self) -> None:
        report_text = rounder._format_daily_device_round_report(
            {
                "hospitalSeq": 24,
                "hospitalName": "푸른산부인과의원(전주)",
                "deviceCount": 1,
                "scheduledDeviceCount": 1,
                "autoUpdateAgent": False,
                "autoUpdateBox": False,
                "autoCleanupTrashCan": True,
                "statusCounts": {"정상": 0, "확인 필요": 0, "이상": 0, "점검 불가": 1},
                "updateCounts": {
                    "agentCandidates": 0,
                    "agentUpdated": 0,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 0,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "candidates": 0,
                    "executed": 0,
                    "failed": 0,
                },
                "deviceResults": [
                    {
                        "deviceName": "MB1-B00461",
                        "roomName": "1진료실",
                        "overallLabel": "점검 불가",
                        "priorityLabel": "판단 보류",
                        "priorityReason": "네트워크 연결 불가로 이상 징후 판단 보류",
                        "componentLabels": {
                            "audio": "확인 필요",
                            "pm2": "확인 필요",
                            "storage": "확인 필요",
                            "captureboard": "확인 필요",
                            "led": "확인 필요",
                        },
                        "storageDetails": {
                            "diskLabel": "확인 필요",
                            "diskDetail": "",
                            "trashcanLabel": "확인 필요",
                            "trashcanDetail": "",
                        },
                        "trashcanCleanup": {
                            "label": "실행 불가",
                            "detail": "SSH 연결 불가라 정리 판단을 못 했어",
                        },
                        "statusPayload": {
                            "ssh": {"ready": False, "reason": "agent_ssh_not_ready"},
                        },
                        "finalPlan": {
                            "agent": {"reason": "장비 agent 연결 끊김", "shouldUpdate": False},
                            "box": {"reason": "장비 agent 연결 끊김", "shouldUpdate": False},
                        },
                        "agentAction": None,
                        "boxAction": None,
                    }
                ],
            },
            now=datetime(2026, 4, 11, 0, 46, 43, tzinfo=ZoneInfo("Asia/Seoul")),
        )

        self.assertIn("• ⚫ 점검 불가 `1`", report_text)
        self.assertNotIn("업데이트 성공", report_text)
        self.assertNotIn("디스크 정리 실행", report_text)
        self.assertIn("*문제/작업 장비*", report_text)
        self.assertIn("• *1진료실*  |  *MB1-B00461*  |  ⚫ *점검 불가*", report_text)
        self.assertIn("  *안내*  ⚫ *장비 종료 또는 네트워크 연결 불가로 점검 불가*", report_text)
        self.assertNotIn("*디스크 용량*", report_text)
        self.assertNotIn("*디스크 정리*", report_text)
        self.assertNotIn("*에이전트 업데이트*", report_text)
        self.assertNotIn("*박스 업데이트*", report_text)

    def test_builds_blocks_with_separate_device_sections(self) -> None:
        blocks = rounder._build_daily_device_round_blocks(
            {
                "hospitalSeq": 604,
                "hospitalName": "루이스산부인과의원(동작)",
                "deviceCount": 2,
                "scheduledDeviceCount": 2,
                "autoUpdateAgent": False,
                "autoUpdateBox": False,
                "autoCleanupTrashCan": True,
                "statusCounts": {"정상": 2, "확인 필요": 0, "이상": 0, "점검 불가": 0},
                "updateCounts": {
                    "agentCandidates": 0,
                    "agentUpdated": 0,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 2,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "candidates": 1,
                    "executed": 1,
                    "failed": 0,
                },
                "deviceResults": [
                    {
                        "deviceName": "MB2-C01431",
                        "roomName": "1진료실",
                        "overallLabel": "정상",
                        "priorityEligible": True,
                        "priorityScore": 0,
                        "priorityLabel": "정상",
                        "priorityReason": "원격 점검상 이상 징후 없음",
                        "componentLabels": {
                            "audio": "정상",
                            "pm2": "정상",
                            "storage": "정상",
                            "captureboard": "정상",
                            "led": "정상",
                        },
                        "storageDetails": {
                            "diskLabel": "정상",
                            "diskDetail": "경로 `/` / 사용량 `12%` / 여유 `190.0 GB` / 전체 `218.0 GB` / 파일시스템 `/dev/sda2`",
                            "trashcanLabel": "정상",
                            "trashcanDetail": "경로 `AppData/TrashCan` / 폴더 `2.0 GB` (`0.9%`) / 파일 `20개` / 30일 초과 `0개`",
                        },
                        "trashcanCleanup": {
                            "label": "꺼짐",
                            "detail": "기준 `60%` 미만 | 현재 `12%`",
                        },
                        "finalPlan": {
                            "agent": {"reason": "에이전트 정상", "currentVersion": "2.0.0", "isLatest": True},
                            "box": {
                                "reason": "박스 2.11.299 -> 2.11.300",
                                "currentVersion": "2.11.299",
                                "latestVersion": "2.11.300",
                                "alreadyLatest": False,
                                "shouldUpdate": True,
                            },
                        },
                        "agentAction": None,
                        "boxAction": None,
                    },
                    {
                        "deviceName": "MB2-C01432",
                        "roomName": "2진료실",
                        "overallLabel": "정상",
                        "priorityEligible": True,
                        "priorityScore": 0,
                        "priorityLabel": "정상",
                        "priorityReason": "원격 점검상 이상 징후 없음",
                        "componentLabels": {
                            "audio": "정상",
                            "pm2": "정상",
                            "storage": "정상",
                            "captureboard": "정상",
                            "led": "정상",
                        },
                        "storageDetails": {
                            "diskLabel": "정상",
                            "diskDetail": "경로 `/` / 사용량 `18%` / 여유 `178.0 GB` / 전체 `218.0 GB` / 파일시스템 `/dev/sda2`",
                            "trashcanLabel": "정상",
                            "trashcanDetail": "경로 `AppData/TrashCan` / 폴더 `9.0 GB` (`4.1%`) / 파일 `120개` / 30일 초과 `4개`",
                        },
                        "trashcanCleanup": {
                            "label": "성공",
                            "detail": "`30일` 초과 `4개` 삭제 / `9.0 GB` -> `6.0 GB` / 현재 `55%` / 남은 `30일` 초과 `0개`",
                        },
                        "finalPlan": {
                            "agent": {"reason": "에이전트 정상", "currentVersion": "2.0.0", "isLatest": True},
                            "box": {
                                "reason": "박스 2.11.299 -> 2.11.300",
                                "currentVersion": "2.11.299",
                                "latestVersion": "2.11.300",
                                "alreadyLatest": False,
                                "shouldUpdate": True,
                            },
                        },
                        "agentAction": None,
                        "boxAction": None,
                    },
                ],
            },
            now=datetime(2026, 4, 8, 22, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )

        self.assertEqual(blocks[0]["type"], "header")
        self.assertIn("#604 루이스산부인과의원(동작)", blocks[0]["text"]["text"])
        self.assertEqual(blocks[1]["type"], "header")
        self.assertEqual(blocks[1]["text"]["text"], "#604 루이스산부인과의원(동작)")
        self.assertEqual(blocks[2]["type"], "context")
        self.assertIn("발송 `2026-04-08 22:00:00 KST` | 장비 `2대`", blocks[2]["elements"][0]["text"])
        self.assertEqual(blocks[3]["type"], "rich_text")
        self.assertEqual(blocks[3]["elements"][0]["type"], "rich_text_list")
        self.assertEqual("🟢 문제 장비 없음", blocks[3]["elements"][0]["elements"][0]["elements"][0]["text"])
        self.assertEqual("박스 업데이트 대상 `2`", blocks[3]["elements"][0]["elements"][1]["elements"][0]["text"])
        self.assertEqual("🧹 디스크 정리 실행 `1`", blocks[3]["elements"][0]["elements"][2]["elements"][0]["text"])
        self.assertEqual(blocks[4]["type"], "divider")
        self.assertEqual(blocks[5]["type"], "section")
        self.assertIn("*1진료실*  |  *MB2-C01431*  |  🟢 *정상*", blocks[5]["text"]["text"])
        self.assertEqual(blocks[6]["type"], "section")
        self.assertIn("*2진료실*  |  *MB2-C01432*  |  🟢 *정상*", blocks[6]["text"]["text"])


if __name__ == "__main__":
    unittest.main()
