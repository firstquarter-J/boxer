import json
import logging
import threading
import unittest
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import ANY, patch
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack import daily_device_round_reporter as reporter


class _FakeSlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.permalink_requests: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs) -> dict[str, str]:
        self.messages.append(kwargs)
        return {"ts": f"2000.{len(self.messages):03d}"}

    def chat_getPermalink(self, **kwargs) -> dict[str, str]:
        self.permalink_requests.append(kwargs)
        message_ts = str(kwargs.get("message_ts") or "").replace(".", "")
        return {"permalink": f"https://slack.example/{kwargs.get('channel')}/p{message_ts}"}


class DailyDeviceRoundReporterPreviewTests(unittest.TestCase):
    def setUp(self) -> None:
        with reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
            reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE.clear()

    def test_builds_minimal_preview_text_with_only_cleanup(self) -> None:
        text = reporter._build_daily_device_round_report_text(
            {
                "hospitalSeq": 24,
                "hospitalName": "푸른산부인과의원(전주)",
                "statusCounts": {"정상": 0, "확인 필요": 1, "이상": 0, "점검 불가": 1},
                "updateCounts": {
                    "agentUpdated": 0,
                    "agentUpdateFailed": 0,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "executed": 1,
                    "failed": 0,
                },
            }
        )

        self.assertEqual(
            text,
            "#24 푸른산부인과의원(전주) | 정리 실행 1",
        )

    def test_builds_minimal_preview_text_with_update_and_cleanup_failures(self) -> None:
        text = reporter._build_daily_device_round_report_text(
            {
                "hospitalSeq": 24,
                "hospitalName": "푸른산부인과의원(전주)",
                "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 1, "점검 불가": 0},
                "updateCounts": {
                    "agentUpdated": 1,
                    "agentUpdateFailed": 0,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 1,
                },
                "cleanupCounts": {
                    "executed": 0,
                    "failed": 1,
                },
            }
        )

        self.assertEqual(
            text,
            "#24 푸른산부인과의원(전주) | 업데이트 에이전트 1 / 박스 0 실패 1 | 정리 실행 0 / 실패 1",
        )

    def test_splits_long_text_fallback_by_line_and_character_limit(self) -> None:
        with patch.object(reporter, "_DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE", 10):
            chunks = reporter._split_daily_device_round_text("12345678901\nabc")

        self.assertEqual(chunks, ["1234567890", "1\nabc"])

    def test_remote_renderer_accepts_only_presentation_dto(self) -> None:
        payload = {
            "runDate": "2026-08-10",
            "hospitalSeq": 24,
            "hospitalName": "테스트병원",
            "deviceCount": 1,
            "scheduledDeviceCount": 1,
            "statusCounts": {
                "정상": 0,
                "확인 필요": 1,
                "이상": 0,
                "점검 불가": 0,
            },
            "updateCounts": {
                "agentCandidates": 1,
                "agentUpdated": 1,
                "agentUpdateFailed": 0,
                "boxCandidates": 0,
                "boxUpdated": 0,
                "boxUpdateFailed": 0,
            },
            "cleanupCounts": {"candidates": 0, "executed": 0, "failed": 0},
            "powerCounts": {
                "requested": 0,
                "poweredOff": 0,
                "alreadyOffline": 0,
                "powerOffFailed": 0,
            },
            "summaryLine": "확인 필요 1",
            "messageBlocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "MB2-TEST 기존 block"},
                }
            ],
            "fallbackText": "MB2-TEST 기존 fallback",
            "deviceResults": [
                {
                    "deviceName": "MB2-TEST",
                    "roomName": "1진료실",
                    "overallLabel": "확인 필요",
                    "networkUnavailable": False,
                    "issueSummary": "스토리지 확인 필요",
                    "storage": {
                        "label": "확인 필요",
                        "filesystemUsedPercent": 81,
                    },
                    "cleanup": {
                        "visible": False,
                        "statusKind": "latest",
                        "label": "불필요",
                        "summary": "정리 대상 아님",
                    },
                    "agentUpdate": {
                        "actionable": True,
                        "statusKind": "success",
                        "label": "업데이트 완료",
                        "summary": "버전 1.0.0 -> 1.1.0",
                    },
                    "boxUpdate": {
                        "actionable": False,
                        "statusKind": "latest",
                        "label": "업데이트 불필요",
                        "summary": "버전 2.1.0",
                    },
                    "power": {
                        "visible": False,
                        "statusKind": "latest",
                        "label": "미실행",
                        "summary": "종료 요청 없음",
                    },
                }
            ],
        }

        validated = reporter._validate_remote_daily_device_round_presentation(
            payload
        )
        blocks = reporter._build_remote_daily_device_round_blocks(
            validated,
            now=datetime(2026, 8, 10, 23, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )
        rendered = json.dumps(blocks, ensure_ascii=False)

        self.assertIn("MB2-TEST", rendered)
        self.assertIn("기존 block", rendered)
        poisoned = json.loads(json.dumps(payload, ensure_ascii=False))
        poisoned["deviceResults"][0]["statusPayload"] = {
            "ssh": {"host": "synthetic-secret-host"}
        }
        with self.assertRaises(RuntimeError):
            reporter._validate_remote_daily_device_round_presentation(poisoned)

    def test_remote_receipt_is_flushed_after_daily_window_closes(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(
            2026,
            8,
            27,
            6,
            0,
            tzinfo=ZoneInfo("Asia/Seoul"),
        )
        class _AutomationClient:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def run(self, **kwargs: object) -> object:
                self.calls.append(kwargs)
                return SimpleNamespace(deliveries=())

        automation_client = _AutomationClient()

        with TemporaryDirectory() as temporary_directory:
            delivery_state_path = (
                Path(temporary_directory) / "automation_delivery.json"
            )
            with (
                patch.object(
                    reporter.cs,
                    "AUTOMATION_DELIVERY_STATE_PATH",
                    str(delivery_state_path),
                ),
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 6),
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            ):
                # 05:59 Slack 발송 성공 receipt가 06:00 poll에서 과거
                # cycle key 그대로 ACK되는 운영 경계 사례를 재현한다.
                reporter.remember_automation_delivery(
                    cycle="daily_device_round",
                    cycle_key="daily:2026-08-26",
                    delivery=reporter.AutomationSlackDelivery(
                        delivery_id="daily_device_round:hospital:85",
                        external_message_id="2000.001",
                        permalink="",
                        delivered_at=local_now.replace(
                            hour=5,
                            minute=59,
                        ),
                    ),
                )
                sent = reporter._run_daily_device_round_remote(
                    client,
                    logger,
                    automation_client=automation_client,  # type: ignore[arg-type]
                    local_now=local_now,
                    state={"windowKey": "2026-08-26"},
                    channel_id="C_DAILY",
                )
                journal = json.loads(
                    delivery_state_path.read_text(encoding="utf-8")
                )

        self.assertFalse(sent)
        self.assertEqual(client.messages, [])
        self.assertEqual(len(automation_client.calls), 1)
        self.assertEqual(
            automation_client.calls[0]["cycle_key"],
            "daily:2026-08-26",
        )
        self.assertIs(automation_client.calls[0]["ack_only"], True)
        self.assertEqual(
            journal["cycles"]["daily_device_round"]["receipts"],
            [],
        )


class DailyDeviceRoundReporterDueTests(unittest.TestCase):
    def setUp(self) -> None:
        with reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
            reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE.clear()

    def test_box_auto_update_override_beats_env_default(self) -> None:
        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT", True),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", True),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", True),
        ):
            status = reporter._build_daily_device_round_auto_update_status(
                {
                    "autoUpdateAgentOverride": False,
                    "autoUpdateBoxFreeOverride": False,
                    "autoUpdateBoxPaidOverride": False,
                }
            )

        self.assertFalse(status["agent"]["enabled"])
        self.assertTrue(status["agent"]["envDefault"])
        self.assertEqual(status["agent"]["source"], "slack_override")
        self.assertFalse(status["boxFree"]["enabled"])
        self.assertTrue(status["boxFree"]["envDefault"])
        self.assertEqual(status["boxFree"]["source"], "slack_override")
        self.assertFalse(status["boxPaid"]["enabled"])
        self.assertTrue(status["boxPaid"]["envDefault"])
        self.assertEqual(status["boxPaid"]["source"], "slack_override")

    def test_legacy_box_override_carries_over_to_free_target_only(self) -> None:
        # 분리 전 단일 마미박스 override는 무료병원 순회 시절 값이라 무료병원에만 승계된다.
        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", True),
        ):
            status = reporter._build_daily_device_round_auto_update_status(
                {
                    "autoUpdateBoxOverride": True,
                    "autoUpdateBoxUpdatedAt": "2026-08-19T14:36:36+09:00",
                    "autoUpdateBoxUpdatedBy": "U0629HDSJHG",
                }
            )

        self.assertTrue(status["boxFree"]["enabled"])
        self.assertEqual(status["boxFree"]["source"], "slack_override")
        self.assertEqual(status["boxFree"]["updatedBy"], "U0629HDSJHG")
        # 유료병원 값은 legacy override의 영향을 받지 않고 env 기본값을 쓴다.
        self.assertTrue(status["boxPaid"]["enabled"])
        self.assertEqual(status["boxPaid"]["source"], "env")

    def test_auto_update_status_shows_single_effective_source(self) -> None:
        rendered = reporter._format_daily_device_round_auto_update_status(
            {
                "hospitalScope": "all",
                "boxFree": {
                    "label": "마미박스(무료병원)",
                    "enabled": False,
                    "envDefault": True,
                    "source": "slack_override",
                    "updatedAt": "2026-06-30T15:18:44+09:00",
                    "updatedBy": "U123",
                },
                "boxPaid": {
                    "label": "마미박스(유료병원)",
                    "enabled": True,
                    "envDefault": False,
                    "source": "slack_override",
                    "updatedAt": "2026-06-30T15:18:44+09:00",
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
            }
        )

        self.assertIn("순회 범위: *전체 병원* (`all`)", rendered)
        self.assertIn("마미박스(무료병원): *꺼짐* | 기준 `저장 설정`", rendered)
        self.assertIn("마미박스(유료병원): *켜짐* | 기준 `저장 설정`", rendered)
        self.assertIn("에이전트: *켜짐* | 기준 `초기 기본값`", rendered)
        self.assertNotIn(".env `true`", rendered)
        self.assertNotIn(".env `false`", rendered)

    def test_sets_auto_update_override_in_state_file(self) -> None:
        local_now = datetime(2026, 4, 8, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        with TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "daily_device_round_state.json"
            with (
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_STATE_PATH", str(state_path)),
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT", True),
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False),
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False),
            ):
                status = reporter._set_daily_device_round_auto_update(
                    "box_free",
                    True,
                    user_id="U123",
                    now=local_now,
                )
                status = reporter._set_daily_device_round_auto_update(
                    "box_paid",
                    True,
                    user_id="U123",
                    now=local_now,
                )
                status = reporter._set_daily_device_round_auto_update(
                    "agent",
                    False,
                    user_id="U123",
                    now=local_now,
                )

            saved = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertFalse(status["agent"]["enabled"])
        self.assertTrue(status["agent"]["envDefault"])
        self.assertEqual(status["agent"]["updatedBy"], "U123")
        self.assertTrue(status["boxFree"]["enabled"])
        self.assertFalse(status["boxFree"]["envDefault"])
        self.assertEqual(status["boxFree"]["updatedBy"], "U123")
        self.assertTrue(status["boxPaid"]["enabled"])
        self.assertFalse(status["boxPaid"]["envDefault"])
        self.assertEqual(status["boxPaid"]["updatedBy"], "U123")
        self.assertFalse(saved["autoUpdateAgentOverride"])
        self.assertEqual(saved["autoUpdateAgentUpdatedAt"], local_now.isoformat())
        self.assertEqual(saved["autoUpdateAgentUpdatedBy"], "U123")
        self.assertTrue(saved["autoUpdateBoxFreeOverride"])
        self.assertEqual(saved["autoUpdateBoxFreeUpdatedAt"], local_now.isoformat())
        self.assertEqual(saved["autoUpdateBoxFreeUpdatedBy"], "U123")
        self.assertTrue(saved["autoUpdateBoxPaidOverride"])
        self.assertEqual(saved["autoUpdateBoxPaidUpdatedAt"], local_now.isoformat())
        self.assertEqual(saved["autoUpdateBoxPaidUpdatedBy"], "U123")

    def test_auto_update_save_failure_keeps_file_and_runtime_unchanged(self) -> None:
        local_now = datetime(2026, 4, 8, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        with TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "daily_device_round_state.json"
            with patch.object(
                reporter.cs,
                "DAILY_DEVICE_ROUND_STATE_PATH",
                str(state_path),
            ):
                reporter._set_daily_device_round_auto_update(
                    "box_paid",
                    False,
                    user_id="U_OLD",
                    now=local_now,
                )
                file_before = state_path.read_text(encoding="utf-8")
                runtime_before = reporter._load_daily_device_round_runtime_state()

                # 저장 실패를 성공처럼 runtime에 먼저 게시하면 다음 라운드가
                # 오류 응답과 반대로 유료병원 박스를 업데이트할 수 있다.
                with patch.object(
                    reporter,
                    "_save_daily_device_round_state",
                    side_effect=PermissionError("read only"),
                ):
                    with self.assertRaises(PermissionError):
                        reporter._set_daily_device_round_auto_update(
                            "box_paid",
                            True,
                            user_id="U_NEW",
                            now=local_now,
                        )

                self.assertEqual(
                    state_path.read_text(encoding="utf-8"),
                    file_before,
                )
                self.assertEqual(
                    reporter._load_daily_device_round_runtime_state(),
                    runtime_before,
                )

    def test_round_persistence_preserves_latest_auto_update_controls(self) -> None:
        local_now = datetime(2026, 4, 8, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        with TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "daily_device_round_state.json"
            with patch.object(
                reporter.cs,
                "DAILY_DEVICE_ROUND_STATE_PATH",
                str(state_path),
            ):
                reporter._set_daily_device_round_auto_update(
                    "box_paid",
                    False,
                    user_id="U_OLD",
                    now=local_now,
                )
                stale_round_state = reporter._load_daily_device_round_state()
                reporter._set_daily_device_round_auto_update(
                    "box_paid",
                    True,
                    user_id="U_NEW",
                    now=local_now,
                )

                # 장시간 실행된 라운드의 과거 snapshot을 저장해도 그 사이 들어온
                # Slack 설정과 변경 메타데이터는 최신 값으로 다시 합쳐야 한다.
                reporter._persist_daily_device_round_state(
                    {**stale_round_state, "lastHospitalSeq": 99},
                    now=local_now,
                    preserve_latest_auto_update_controls=True,
                )
                saved = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertTrue(saved["autoUpdateBoxPaidOverride"])
        self.assertEqual(saved["autoUpdateBoxPaidUpdatedBy"], "U_NEW")
        self.assertEqual(saved["lastHospitalSeq"], 99)

    def test_concurrent_auto_update_commands_do_not_lose_each_other(self) -> None:
        local_now = datetime(2026, 4, 8, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        first_save_started = threading.Event()
        release_first_save = threading.Event()
        second_finished = threading.Event()
        failures: list[BaseException] = []

        with TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "daily_device_round_state.json"
            original_save = reporter._save_daily_device_round_state
            save_count = 0
            save_count_lock = threading.Lock()

            def _blocking_first_save(state, state_path_override=None):
                nonlocal save_count
                with save_count_lock:
                    save_count += 1
                    current_save_count = save_count
                if current_save_count == 1:
                    first_save_started.set()
                    if not release_first_save.wait(timeout=2):
                        raise TimeoutError("first state save was not released")
                return original_save(state, state_path_override)

            def _set_target(target: str, user_id: str) -> None:
                try:
                    reporter._set_daily_device_round_auto_update(
                        target,
                        True,
                        user_id=user_id,
                        now=local_now,
                    )
                except Exception as exc:  # pragma: no cover - assertion below reports it
                    failures.append(exc)
                finally:
                    if target == "box_paid":
                        second_finished.set()

            with (
                patch.object(
                    reporter.cs,
                    "DAILY_DEVICE_ROUND_STATE_PATH",
                    str(state_path),
                ),
                patch.object(
                    reporter,
                    "_save_daily_device_round_state",
                    side_effect=_blocking_first_save,
                ),
            ):
                free_thread = threading.Thread(
                    target=_set_target,
                    args=("box_free", "U_FREE"),
                )
                paid_thread = threading.Thread(
                    target=_set_target,
                    args=("box_paid", "U_PAID"),
                )
                free_thread.start()
                self.assertTrue(first_save_started.wait(timeout=2))
                paid_thread.start()
                # 첫 transaction이 끝나기 전에는 두 번째 명령이 저장까지 갈 수 없다.
                self.assertFalse(second_finished.wait(timeout=0.1))
                release_first_save.set()
                free_thread.join(timeout=2)
                paid_thread.join(timeout=2)

            saved = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertFalse(failures)
        self.assertFalse(free_thread.is_alive())
        self.assertFalse(paid_thread.is_alive())
        self.assertTrue(saved["autoUpdateBoxFreeOverride"])
        self.assertEqual(saved["autoUpdateBoxFreeUpdatedBy"], "U_FREE")
        self.assertTrue(saved["autoUpdateBoxPaidOverride"])
        self.assertEqual(saved["autoUpdateBoxPaidUpdatedBy"], "U_PAID")

    def test_atomic_state_replace_failure_keeps_previous_json(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "daily_device_round_state.json"
            reporter._save_daily_device_round_state(
                {"autoUpdateBoxPaidOverride": False},
                state_path,
            )
            file_before = state_path.read_text(encoding="utf-8")

            # replace 직전 실패해도 기존 JSON은 온전히 남고 임시 파일은 정리돼야 한다.
            with patch.object(
                reporter.os,
                "replace",
                side_effect=OSError("replace failed"),
            ):
                with self.assertRaises(OSError):
                    reporter._save_daily_device_round_state(
                        {"autoUpdateBoxPaidOverride": True},
                        state_path,
                    )

            self.assertEqual(state_path.read_text(encoding="utf-8"), file_before)
            self.assertEqual(list(state_path.parent.glob(f".{state_path.name}.*.tmp")), [])

    def test_clears_legacy_fixed_target_self_loop_on_new_window(self) -> None:
        local_tz = ZoneInfo("Asia/Seoul")

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
        ):
            normalized = reporter._normalize_daily_device_round_state(
                {
                    "lastHospitalSeq": 604,
                    "nextHospitalSeq": 604,
                    "lastRunDate": "2026-04-08",
                    "hospitalScope": "free_barcode",
                    "hospitalOrder": "recordings_month_asc",
                    "activeHospitalSeq": 601,
                    "activeHospitalName": "A병원",
                    "activeDeviceIndex": 3,
                },
                now=datetime(2026, 4, 9, 22, 0, tzinfo=local_tz),
            )

        self.assertEqual(normalized["windowKey"], "2026-04-09")
        self.assertEqual(normalized["lastHospitalSeq"], 604)
        self.assertIsNone(normalized["nextHospitalSeq"])
        self.assertEqual(normalized["processedHospitalSeqs"], [])
        self.assertEqual(normalized["windowThreadTs"], "")
        self.assertEqual(normalized["windowThreadChannelId"], "")
        self.assertNotIn("activeHospitalSeq", normalized)
        self.assertNotIn("activeDeviceIndex", normalized)

    def test_is_due_only_inside_overnight_window_until_completed(self) -> None:
        local_tz = ZoneInfo("Asia/Seoul")

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
        ):
            self.assertFalse(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 8, 21, 59, tzinfo=local_tz),
                    {},
                )
            )
            self.assertTrue(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 8, 22, 0, tzinfo=local_tz),
                    {},
                )
            )
            self.assertTrue(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 9, 4, 59, tzinfo=local_tz),
                    {
                        "windowKey": "2026-04-08",
                        "hospitalScope": "free_barcode",
                        "hospitalOrder": "recordings_month_asc",
                        "processedHospitalSeqs": [10],
                    },
                )
            )
            self.assertFalse(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 9, 5, 0, tzinfo=local_tz),
                    {
                        "windowKey": "2026-04-08",
                        "hospitalScope": "free_barcode",
                        "hospitalOrder": "recordings_month_asc",
                        "processedHospitalSeqs": [10],
                    },
                )
            )
            self.assertFalse(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 9, 1, 0, tzinfo=local_tz),
                    {
                        "windowKey": "2026-04-08",
                        "hospitalScope": "free_barcode",
                        "hospitalOrder": "recordings_month_asc",
                        "windowCompletedAt": "2026-04-09T00:30:00+09:00",
                    },
                )
            )

    def test_resets_window_progress_when_hospital_scope_changes(self) -> None:
        local_tz = ZoneInfo("Asia/Seoul")

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOSPITAL_SCOPE", "non_free_barcode"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
        ):
            normalized = reporter._normalize_daily_device_round_state(
                {
                    "windowKey": "2026-04-08",
                    "hospitalScope": "free_barcode",
                    "hospitalOrder": "recordings_month_asc",
                    "processedHospitalSeqs": [10, 20],
                    "lastHospitalSeq": 20,
                    "nextHospitalSeq": 30,
                    "windowCompletedAt": "2026-04-09T00:30:00+09:00",
                    "windowThreadTs": "2000.001",
                    "windowThreadChannelId": "C_DAILY",
                },
                now=datetime(2026, 4, 9, 1, 0, tzinfo=local_tz),
            )

        self.assertEqual(normalized["windowKey"], "2026-04-08")
        self.assertEqual(normalized["hospitalScope"], "non_free_barcode")
        self.assertEqual(normalized["hospitalOrder"], "recordings_month_asc")
        self.assertIsNone(normalized["lastHospitalSeq"])
        self.assertIsNone(normalized["nextHospitalSeq"])
        self.assertEqual(normalized["processedHospitalSeqs"], [])
        self.assertNotIn("windowCompletedAt", normalized)
        self.assertEqual(normalized["windowThreadTs"], "")
        self.assertEqual(normalized["windowThreadChannelId"], "")

    def test_resets_window_progress_when_hospital_order_changes(self) -> None:
        local_tz = ZoneInfo("Asia/Seoul")

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOSPITAL_SCOPE", "free_barcode"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOSPITAL_ORDER", "recordings_month_asc"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
        ):
            normalized = reporter._normalize_daily_device_round_state(
                {
                    "windowKey": "2026-04-08",
                    "hospitalScope": "free_barcode",
                    "hospitalOrder": "recordings_month_desc",
                    "processedHospitalSeqs": [10, 20],
                    "lastHospitalSeq": 20,
                    "nextHospitalSeq": 30,
                    "windowThreadTs": "2000.001",
                    "windowThreadChannelId": "C_DAILY",
                },
                now=datetime(2026, 4, 9, 1, 0, tzinfo=local_tz),
            )

        self.assertEqual(normalized["hospitalScope"], "free_barcode")
        self.assertEqual(normalized["hospitalOrder"], "recordings_month_asc")
        self.assertIsNone(normalized["lastHospitalSeq"])
        self.assertIsNone(normalized["nextHospitalSeq"])
        self.assertEqual(normalized["processedHospitalSeqs"], [])
        self.assertEqual(normalized["windowThreadTs"], "")
        self.assertEqual(normalized["windowThreadChannelId"], "")


class DailyDeviceRoundReporterRunTests(unittest.TestCase):
    def setUp(self) -> None:
        with reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
            reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE.clear()

    def test_posts_report_and_saves_window_state_when_due(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 2,
            "nextHospitalSeq": 10,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 1, "확인 필요": 1, "이상": 0, "점검 불가": 0},
            "updateCounts": {
                "agentCandidates": 1,
                "agentUpdated": 1,
                "agentUpdateFailed": 0,
                "boxCandidates": 1,
                "boxUpdated": 0,
                "boxUpdateFailed": 0,
            },
            "cleanupCounts": {
                "candidates": 1,
                "executed": 1,
                "failed": 0,
            },
            "deviceResults": [],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": True,
            "autoPowerOff": False,
            "powerCounts": {
                "requested": 0,
                "poweredOff": 0,
                "alreadyOffline": 0,
                "powerOffFailed": 0,
            },
        }

        # Python 3.11은 단일 with의 정적 중첩 블록 수를 제한하므로
        # ExitStack으로 같은 patch 범위와 검증 대상을 유지한다.
        with ExitStack() as stack:
            stack.enter_context(patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True))
            stack.enter_context(patch.object(reporter.s, "DB_QUERY_ENABLED", True))
            stack.enter_context(
                patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql")
            )
            stack.enter_context(
                patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret")
            )
            stack.enter_context(patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"))
            stack.enter_context(
                patch.object(
                    reporter.cs,
                    "MDA_GRAPHQL_ORIGIN",
                    "https://mda.kr.mmtalkbox.com",
                )
            )
            stack.enter_context(
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY")
            )
            stack.enter_context(patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22))
            stack.enter_context(patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0))
            stack.enter_context(
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5)
            )
            stack.enter_context(
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0)
            )
            stack.enter_context(
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT", True)
            )
            stack.enter_context(
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False)
            )
            stack.enter_context(
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False)
            )
            stack.enter_context(
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_POWER_OFF", False)
            )
            stack.enter_context(
                patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", True)
            )
            stack.enter_context(
                patch(
                    "boxer_company_adapter_slack.daily_device_round_reporter._load_daily_device_round_state",
                    return_value={
                        "windowKey": "2026-04-08",
                        "hospitalScope": "free_barcode",
                        "hospitalOrder": "recordings_month_asc",
                        "processedHospitalSeqs": [10],
                        "lastHospitalSeq": 10,
                        "nextHospitalSeq": 20,
                    },
                )
            )
            build_summary_mock = stack.enter_context(
                patch(
                    "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                    return_value=summary,
                )
            )
            format_mock = stack.enter_context(
                patch(
                    "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                    return_value="daily round body",
                )
            )
            blocks_mock = stack.enter_context(
                patch(
                    "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                    return_value=[
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": "daily round block"},
                        }
                    ],
                )
            )
            save_state_mock = stack.enter_context(
                patch(
                    "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
                )
            )
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        build_summary_mock.assert_called_once_with(
            now=local_now,
            state={
                "windowKey": "2026-04-08",
                "hospitalScope": "free_barcode",
                "hospitalOrder": "recordings_month_asc",
                "processedHospitalSeqs": [10],
                "lastHospitalSeq": 10,
                "nextHospitalSeq": 20,
                "windowThreadTs": "",
                "windowThreadChannelId": "",
            },
            auto_update_agent=True,
            auto_update_box_free=False,
            auto_update_box_paid=False,
            auto_cleanup_trashcan=True,
            auto_power_off=False,
            progress_callback=ANY,
        )
        format_mock.assert_called_once_with(summary, now=local_now)
        blocks_mock.assert_called_once_with(
            summary,
            now=local_now,
            include_header=False,
        )
        self.assertEqual(len(client.messages), 2)
        self.assertEqual(client.messages[0]["channel"], "C_DAILY")
        self.assertEqual(client.messages[0]["text"], "마미박스 일일 순회 업데이트 | 2026-04-08")
        self.assertEqual(client.messages[1]["channel"], "C_DAILY")
        self.assertEqual(client.messages[1]["text"], "daily round body")
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        self.assertEqual(save_state_mock.call_count, 2)
        self.assertEqual(
            save_state_mock.call_args_list[0].args[0],
            {
                "windowKey": "2026-04-08",
                "hospitalScope": "free_barcode",
                "hospitalOrder": "recordings_month_asc",
                "processedHospitalSeqs": [10],
                "lastHospitalSeq": 10,
                "nextHospitalSeq": 20,
                "windowThreadTs": "2000.001",
                "windowThreadChannelId": "C_DAILY",
                "channelId": "C_DAILY",
            },
        )
        self.assertEqual(
            save_state_mock.call_args_list[1].args[0],
            {
                "lastRunDate": "2026-04-08",
                "lastHospitalSeq": 20,
                "lastHospitalName": "B병원",
                "nextHospitalSeq": 10,
                "lastSentAt": local_now.isoformat(),
                "channelId": "C_DAILY",
                "windowKey": "2026-04-08",
                "hospitalScope": "free_barcode",
                "hospitalOrder": "recordings_month_asc",
                "windowThreadTs": "2000.001",
                "windowThreadChannelId": "C_DAILY",
                "processedHospitalSeqs": [10, 20],
                "windowCompletedAt": "",
                "statusCounts": {"정상": 1, "확인 필요": 1, "이상": 0, "점검 불가": 0},
                "updateCounts": {
                    "agentCandidates": 1,
                    "agentUpdated": 1,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 1,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "candidates": 1,
                    "executed": 1,
                    "failed": 0,
                },
                "powerCounts": {
                    "requested": 0,
                    "poweredOff": 0,
                    "alreadyOffline": 0,
                    "powerOffFailed": 0,
                },
            }
        )

    def test_posts_only_report_when_abnormal_found(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "nextHospitalSeq": 30,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 0, "확인 필요": 0, "이상": 1, "점검 불가": 0},
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
                    "roomName": "1진료실",
                    "deviceName": "MB2-C00043",
                    "overallLabel": "이상",
                    "priorityReason": "LED USB 장치를 찾지 못했어",
                }
            ],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
            "autoPowerOff": False,
            "powerCounts": {
                "requested": 0,
                "poweredOff": 0,
                "alreadyOffline": 0,
                "powerOffFailed": 0,
            },
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_POWER_OFF", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ENABLED", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                return_value=summary,
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                return_value="daily round body",
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "daily round block"}}],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ),
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 2)
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        self.assertEqual(client.permalink_requests, [])

    def test_skips_root_alert_when_health_monitor_owns_alerting(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "nextHospitalSeq": 30,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 0, "확인 필요": 0, "이상": 1, "점검 불가": 0},
            "updateCounts": {},
            "cleanupCounts": {},
            "powerCounts": {},
            "deviceResults": [{"deviceName": "MB2-C00043", "overallLabel": "이상"}],
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_POWER_OFF", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ENABLED", True),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                return_value=summary,
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                return_value="daily round body",
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "daily round block"}}],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ),
        ):
            sent = reporter._run_daily_device_round_if_due(client, logger, now=local_now)

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 2)
        self.assertEqual(client.permalink_requests, [])

    def test_reuses_existing_window_thread_for_next_hospital(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 30, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 30,
            "hospitalName": "C병원",
            "deviceCount": 1,
            "nextHospitalSeq": 40,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
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
            "deviceResults": [],
            "autoUpdateAgent": False,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
            "autoPowerOff": False,
            "powerCounts": {
                "requested": 0,
                "poweredOff": 0,
                "alreadyOffline": 0,
                "powerOffFailed": 0,
            },
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 30),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._load_daily_device_round_state",
                return_value={
                    "windowKey": "2026-04-08",
                    "hospitalScope": "free_barcode",
                    "hospitalOrder": "recordings_month_asc",
                    "processedHospitalSeqs": [10, 20],
                    "lastHospitalSeq": 20,
                    "nextHospitalSeq": 30,
                    "windowThreadTs": "2000.777",
                    "windowThreadChannelId": "C_DAILY",
                },
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                return_value=summary,
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                return_value="daily round body",
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "daily round block"}}],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ) as save_state_mock,
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 1)
        self.assertEqual(client.messages[0]["channel"], "C_DAILY")
        self.assertEqual(client.messages[0]["thread_ts"], "2000.777")
        self.assertEqual(save_state_mock.call_count, 1)
        self.assertEqual(
            save_state_mock.call_args_list[0].args[0],
            {
                "windowKey": "2026-04-08",
                "hospitalScope": "free_barcode",
                "hospitalOrder": "recordings_month_asc",
                "processedHospitalSeqs": [10, 20, 30],
                "lastHospitalSeq": 30,
                "lastHospitalName": "C병원",
                "nextHospitalSeq": 40,
                "windowThreadTs": "2000.777",
                "windowThreadChannelId": "C_DAILY",
                "windowCompletedAt": local_now.isoformat(),
                "lastRunDate": "2026-04-08",
                "lastSentAt": local_now.isoformat(),
                "channelId": "C_DAILY",
                "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
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
                "powerCounts": {
                    "requested": 0,
                    "poweredOff": 0,
                    "alreadyOffline": 0,
                    "powerOffFailed": 0,
                },
            }
        )

    def test_falls_back_to_plain_text_when_block_post_fails(self) -> None:
        class _BlockFailingClient(_FakeSlackClient):
            def chat_postMessage(self, **kwargs) -> dict[str, str]:
                self.messages.append(kwargs)
                if kwargs.get("blocks"):
                    raise RuntimeError("invalid_blocks")
                return {"ts": f"2000.{len(self.messages):03d}"}

        client = _BlockFailingClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "scheduledDeviceCount": 1,
            "nextHospitalSeq": 30,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
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
            "deviceResults": [],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                return_value=summary,
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                return_value="daily round body",
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[{"type": "rich_text", "elements": []}],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ) as save_state_mock,
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 3)
        self.assertEqual(client.messages[0]["text"], "마미박스 일일 순회 업데이트 | 2026-04-08")
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        self.assertIn("blocks", client.messages[1])
        self.assertEqual(client.messages[2]["thread_ts"], "2000.001")
        self.assertNotIn("blocks", client.messages[2])
        self.assertIn("B병원", client.messages[2]["text"])
        save_state_mock.assert_called()

    def test_splits_block_messages_when_chunk_limit_is_hit(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 2,
            "scheduledDeviceCount": 2,
            "nextHospitalSeq": 30,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 1, "확인 필요": 1, "이상": 0, "점검 불가": 0},
            "updateCounts": {
                "agentCandidates": 1,
                "agentUpdated": 0,
                "agentUpdateFailed": 0,
                "boxCandidates": 1,
                "boxUpdated": 0,
                "boxUpdateFailed": 0,
            },
            "cleanupCounts": {
                "candidates": 1,
                "executed": 0,
                "failed": 0,
            },
            "deviceResults": [],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": True,
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", True),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                return_value=summary,
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                return_value="daily round body",
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[
                    {"type": "section", "text": {"type": "mrkdwn", "text": "block-1"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": "block-2"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": "block-3"}},
                ],
            ),
            patch.object(reporter, "_DAILY_DEVICE_ROUND_MAX_BLOCKS_PER_MESSAGE", 1),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ),
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 4)
        self.assertEqual(client.messages[0]["text"], "마미박스 일일 순회 업데이트 | 2026-04-08")
        self.assertEqual(client.messages[1]["text"], "daily round body | 계속 1/3")
        self.assertEqual(client.messages[2]["text"], "daily round body | 계속 2/3")
        self.assertEqual(client.messages[3]["text"], "daily round body | 계속 3/3")
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        self.assertEqual(client.messages[2]["thread_ts"], "2000.001")
        self.assertEqual(client.messages[3]["thread_ts"], "2000.001")

    def test_reuses_runtime_thread_state_when_final_state_save_fails(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        first_summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "nextHospitalSeq": 30,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
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
            "deviceResults": [],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
        }
        second_summary = {
            **first_summary,
            "hospitalSeq": 30,
            "hospitalName": "C병원",
            "nextHospitalSeq": 40,
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                side_effect=[first_summary, second_summary],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                side_effect=["first body", "second body"],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "daily round block"}}],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state",
                side_effect=[
                    RuntimeError("disk write failed"),
                    RuntimeError("disk write failed"),
                    None,
                    None,
                ],
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "disk write failed"):
                reporter._run_daily_device_round_if_due(
                    client,
                    logger,
                    now=local_now,
                )

            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 3)
        self.assertEqual(client.messages[0]["text"], "마미박스 일일 순회 업데이트 | 2026-04-08")
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        self.assertEqual(client.messages[2]["thread_ts"], "2000.001")

    def test_posts_title_and_saves_active_progress_before_hospital_finishes(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "nextHospitalSeq": 30,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
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
            "deviceResults": [],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
        }

        def _build_summary_side_effect(**kwargs):
            progress_callback = kwargs["progress_callback"]
            progress_callback(
                "hospital_started",
                {
                    "hospitalSeq": 20,
                    "hospitalName": "B병원",
                    "deviceCount": 1,
                    "startedAt": local_now.isoformat(),
                },
            )
            self.assertEqual(len(client.messages), 1)
            self.assertEqual(client.messages[0]["text"], "마미박스 일일 순회 업데이트 | 2026-04-08")
            progress_callback(
                "device_started",
                {
                    "hospitalSeq": 20,
                    "hospitalName": "B병원",
                    "deviceCount": 1,
                    "deviceIndex": 1,
                    "deviceName": "MB2-C00001",
                    "updatedAt": local_now.isoformat(),
                },
            )
            return summary

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                side_effect=_build_summary_side_effect,
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                return_value="daily round body",
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "daily round block"}}],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ) as save_state_mock,
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertGreaterEqual(save_state_mock.call_count, 4)
        self.assertEqual(
            save_state_mock.call_args_list[1].args[0]["activeHospitalSeq"],
            20,
        )
        self.assertEqual(
            save_state_mock.call_args_list[2].args[0]["activeDeviceIndex"],
            1,
        )

    def test_marks_window_completed_without_post_when_no_hospital_left(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 9, 4, 30, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-09",
            "hospitalSeq": None,
            "hospitalName": "미선정",
            "deviceCount": 0,
            "nextHospitalSeq": None,
            "candidateHospitalCount": 2,
            "statusCounts": {"정상": 0, "확인 필요": 0, "이상": 0, "점검 불가": 0},
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
            "deviceResults": [],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
            "autoPowerOff": False,
            "powerCounts": {
                "requested": 0,
                "poweredOff": 0,
                "alreadyOffline": 0,
                "powerOffFailed": 0,
            },
            "summaryLine": "이번 야간 업데이트 창에서 처리할 병원을 모두 끝냈어",
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT", True),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", True),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_POWER_OFF", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._load_daily_device_round_state",
                return_value={
                    "windowKey": "2026-04-08",
                    "hospitalScope": "free_barcode",
                    "hospitalOrder": "recordings_month_asc",
                    "processedHospitalSeqs": [10, 20],
                    "lastHospitalSeq": 20,
                    "nextHospitalSeq": 30,
                    "autoUpdateBoxOverride": False,
                },
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                return_value=summary,
            ) as build_summary_mock,
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ) as save_state_mock,
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertFalse(sent)
        self.assertEqual(client.messages, [])
        build_summary_mock.assert_called_once_with(
            now=local_now,
            state={
                "windowKey": "2026-04-08",
                "hospitalScope": "free_barcode",
                "hospitalOrder": "recordings_month_asc",
                "processedHospitalSeqs": [10, 20],
                "lastHospitalSeq": 20,
                "nextHospitalSeq": 30,
                "autoUpdateBoxOverride": False,
                "windowThreadTs": "",
                "windowThreadChannelId": "",
            },
            auto_update_agent=True,
            auto_update_box_free=False,
            auto_update_box_paid=False,
            auto_cleanup_trashcan=False,
            auto_power_off=False,
            progress_callback=ANY,
        )
        save_state_mock.assert_called_once_with(
            {
                "windowKey": "2026-04-08",
                "hospitalScope": "free_barcode",
                "hospitalOrder": "recordings_month_asc",
                "processedHospitalSeqs": [10, 20],
                "autoUpdateBoxOverride": False,
                "windowCompletedAt": local_now.isoformat(),
                "lastRunDate": "2026-04-09",
                "lastHospitalSeq": None,
                "lastHospitalName": "미선정",
                "nextHospitalSeq": None,
                "lastSentAt": local_now.isoformat(),
                "channelId": "C_DAILY",
                "windowThreadTs": "",
                "windowThreadChannelId": "",
                "statusCounts": {"정상": 0, "확인 필요": 0, "이상": 0, "점검 불가": 0},
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
                "powerCounts": {
                    "requested": 0,
                    "poweredOff": 0,
                    "alreadyOffline": 0,
                    "powerOffFailed": 0,
                },
            }
        )


if __name__ == "__main__":
    unittest.main()
