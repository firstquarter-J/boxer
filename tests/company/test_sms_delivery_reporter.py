import json
import logging
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from boxer_company_adapter_slack import sms_delivery_reporter


class SmsDeliveryReporterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.outbox_path = Path(self.temp_dir.name) / "sms-delivery-outbox.json"
        self.outbox_path_patcher = patch.object(
            sms_delivery_reporter.cs,
            "SMS_DELIVERY_OUTBOX_PATH",
            str(self.outbox_path),
        )
        self.repair_grace_patcher = patch.object(
            sms_delivery_reporter.cs,
            "SMS_DELIVERY_OUTBOX_REPAIR_GRACE_SEC",
            0,
        )
        self.outbox_path_patcher.start()
        self.repair_grace_patcher.start()

    def tearDown(self) -> None:
        self.repair_grace_patcher.stop()
        self.outbox_path_patcher.stop()
        self.temp_dir.cleanup()

    def _remember_accepted(
        self,
        group_id: str = "G-ACCEPTED",
        *,
        detected_at: datetime | None = None,
        permalink: str = "",
    ) -> None:
        actual_detected_at = detected_at or datetime(
            2026,
            7,
            23,
            9,
            0,
            tzinfo=timezone.utc,
        )
        remembered = sms_delivery_reporter.remember_sms_delivery_sheet_record(
            {
                "device": "MB2-C00043",
                "hospitalName": "테스트 병원",
                "room": "1진료실",
                "problemComponents": ["캡처보드", "LED"],
                "issue": "캡처보드와 LED를 찾지 못했어",
                "smsDeliveryStatus": "accepted",
                "smsGroupId": group_id,
                "smsPhoneNumber": "01012345678",
                "smsMessage": "외부로 저장되면 안 되는 문자 본문",
            },
            detected_at=actual_detected_at,
            sms_accepted_at=actual_detected_at,
            permalink=permalink,
        )
        self.assertTrue(remembered)

    def test_remembers_only_allowlisted_fields_and_merges_permalink(self) -> None:
        self._remember_accepted()
        first_item = sms_delivery_reporter._load_sms_delivery_outbox_items()[0]
        self._remember_accepted(
            detected_at=datetime(2026, 7, 23, 9, 1, tzinfo=timezone.utc),
            permalink="https://lifexio.slack.com/archives/C123/p123",
        )

        payload = json.loads(self.outbox_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["version"], 1)
        self.assertEqual(len(payload["items"]), 1)
        item = payload["items"][0]
        self.assertEqual(
            set(item),
            sms_delivery_reporter._SMS_DELIVERY_OUTBOX_ALLOWED_KEYS,
        )
        self.assertEqual(item["smsGroupId"], "G-ACCEPTED")
        self.assertEqual(item["components"], ["캡처보드", "LED"])
        self.assertEqual(
            item["permalink"],
            "https://lifexio.slack.com/archives/C123/p123",
        )
        self.assertEqual(
            item["smsAcceptedAt"],
            first_item["smsAcceptedAt"],
        )
        self.assertGreaterEqual(
            datetime.fromisoformat(item["storedAt"].replace("Z", "+00:00")),
            datetime.fromisoformat(
                first_item["storedAt"].replace("Z", "+00:00")
            ),
        )
        serialized = self.outbox_path.read_text(encoding="utf-8")
        self.assertNotIn("01012345678", serialized)
        self.assertNotIn("외부로 저장되면 안 되는 문자 본문", serialized)
        self.assertEqual(
            list(Path(self.temp_dir.name).glob(".sms-delivery-outbox.json.*.tmp")),
            [],
        )

    def test_legacy_outbox_uses_detected_at_for_new_timestamps(self) -> None:
        detected_at = "2026-07-22T01:02:03Z"
        self.outbox_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "items": [
                        {
                            "device": "MB2-LEGACY",
                            "hospital": "기존 병원",
                            "room": "진료실",
                            "components": ["캡처보드"],
                            "issue": "기존 장애",
                            "smsDeliveryStatus": "accepted",
                            "smsGroupId": "G-LEGACY",
                            "detectedAt": detected_at,
                            "permalink": "",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        item = sms_delivery_reporter._load_sms_delivery_outbox_items()[0]

        self.assertEqual(item["smsAcceptedAt"], detected_at)
        self.assertEqual(item["storedAt"], detected_at)

    def test_remembers_immediate_final_result_for_sheet_retry(self) -> None:
        remembered = sms_delivery_reporter.remember_sms_delivery_sheet_record(
            {
                "device": "MB2-C00043",
                "hospitalName": "테스트 병원",
                "room": "1진료실",
                "issue": "캡처보드 이상",
                "smsDeliveryStatus": "delivered",
                "smsGroupId": "G-DELIVERED",
            },
            detected_at=datetime(2026, 7, 23, 9, 0, tzinfo=timezone.utc),
            permalink="https://example.com/delivered",
        )

        self.assertTrue(remembered)
        self.assertEqual(
            sms_delivery_reporter._load_sms_delivery_outbox_items()[0][
                "smsDeliveryStatus"
            ],
            "delivered",
        )

    def test_remember_preserves_alert_item_sms_accepted_at(self) -> None:
        accepted_at = "2026-07-22T01:02:03Z"

        remembered = sms_delivery_reporter.remember_sms_delivery_sheet_record(
            {
                "device": "MB2-C00043",
                "hospitalName": "테스트 병원",
                "room": "1진료실",
                "issue": "캡처보드 이상",
                "smsDeliveryStatus": "accepted",
                "smsGroupId": "G-PRESERVED-ACCEPTED-AT",
                "smsAcceptedAt": accepted_at,
            },
            detected_at=datetime(2026, 7, 22, 1, 0, tzinfo=timezone.utc),
        )

        self.assertTrue(remembered)
        self.assertEqual(
            sms_delivery_reporter._load_sms_delivery_outbox_items()[0][
                "smsAcceptedAt"
            ],
            accepted_at,
        )

    def test_concurrent_remember_keeps_every_group_without_lost_update(self) -> None:
        def remember(index: int) -> bool:
            return sms_delivery_reporter.remember_sms_delivery_sheet_record(
                {
                    "device": f"MB2-{index:05d}",
                    "hospital": "동시성 테스트 병원",
                    "room": "검사실",
                    "components": ["캡처보드"],
                    "issue": "연결 실패",
                    "smsDeliveryStatus": "accepted",
                    "smsGroupId": f"G-{index:03d}",
                },
                detected_at=datetime(2026, 7, 23, 9, 0, tzinfo=timezone.utc),
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(remember, range(20)))

        self.assertTrue(all(results))
        items = sms_delivery_reporter._load_sms_delivery_outbox_items()
        self.assertEqual(
            {item["smsGroupId"] for item in items},
            {f"G-{index:03d}" for index in range(20)},
        )

    def test_repair_waits_for_grace_then_appends_pre_slack_record(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        self._remember_accepted(permalink="")
        stored_at = datetime.fromisoformat(
            sms_delivery_reporter._load_sms_delivery_outbox_items()[0][
                "storedAt"
            ].replace("Z", "+00:00")
        )

        with (
            patch.object(
                sms_delivery_reporter.cs,
                "SMS_DELIVERY_OUTBOX_REPAIR_GRACE_SEC",
                60,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                return_value={},
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                return_value={
                    "status": "SENDING",
                    "count": {"sentPending": 1},
                },
            ),
            patch.object(
                sms_delivery_reporter,
                "_has_device_health_sheet_sms_tracking_group_id",
            ) as has_group_mock,
            patch.object(
                sms_delivery_reporter,
                "_append_sms_delivery_outbox_item_to_sheet",
            ) as append_mock,
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=stored_at + timedelta(seconds=59),
            )

        self.assertEqual(changed, 0)
        has_group_mock.assert_not_called()
        append_mock.assert_not_called()

        accepted_row = {
            "G-ACCEPTED": {
                "rowNumber": 6,
                "groupId": "G-ACCEPTED",
                "smsStatus": "접수됨",
            }
        }
        with (
            patch.object(
                sms_delivery_reporter.cs,
                "SMS_DELIVERY_OUTBOX_REPAIR_GRACE_SEC",
                60,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                side_effect=[{}, accepted_row],
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                return_value={
                    "status": "SENDING",
                    "count": {"sentPending": 1},
                },
            ),
            patch.object(
                sms_delivery_reporter,
                "_has_device_health_sheet_sms_tracking_group_id",
                return_value=False,
            ) as has_group_mock,
            patch.object(
                sms_delivery_reporter,
                "_append_sms_delivery_outbox_item_to_sheet",
                return_value=True,
            ) as append_mock,
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=stored_at + timedelta(seconds=60),
            )

        self.assertEqual(changed, 1)
        has_group_mock.assert_called_once_with("G-ACCEPTED")
        append_mock.assert_called_once()
        self.assertEqual(append_mock.call_args.args[0]["permalink"], "")

    def test_ambiguous_identity_with_existing_group_never_reappends(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        self._remember_accepted()
        stored_at = datetime.fromisoformat(
            sms_delivery_reporter._load_sms_delivery_outbox_items()[0][
                "storedAt"
            ].replace("Z", "+00:00")
        )

        with (
            patch.object(
                sms_delivery_reporter.cs,
                "SMS_DELIVERY_OUTBOX_REPAIR_GRACE_SEC",
                60,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                return_value={},
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                return_value={
                    "status": "SENDING",
                    "count": {"sentPending": 1},
                },
            ),
            patch.object(
                sms_delivery_reporter,
                "_has_device_health_sheet_sms_tracking_group_id",
                return_value=True,
            ),
            patch.object(
                sms_delivery_reporter,
                "_append_sms_delivery_outbox_item_to_sheet",
            ) as append_mock,
            self.assertLogs(
                "test.sms_delivery_reporter",
                level="WARNING",
            ) as captured_logs,
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=stored_at + timedelta(seconds=61),
            )

        self.assertEqual(changed, 0)
        append_mock.assert_not_called()
        self.assertEqual(
            len(sms_delivery_reporter._load_sms_delivery_outbox_items()),
            1,
        )
        self.assertIn("identity match가 모호", "\n".join(captured_logs.output))

    def test_reconcile_lock_setup_failure_releases_thread_lock(self) -> None:
        for method_name in ("mkdir", "open"):
            with (
                self.subTest(method_name=method_name),
                patch.object(
                    Path,
                    method_name,
                    side_effect=OSError("permission denied"),
                ),
                self.assertRaises(OSError),
            ):
                with sms_delivery_reporter._try_sms_delivery_reconcile_lock(
                    outbox_path=self.outbox_path
                ):
                    self.fail("잠금 파일 준비 실패인데 context가 열리면 안 돼")

            acquired = (
                sms_delivery_reporter._SMS_DELIVERY_RECONCILE_THREAD_LOCK.acquire(
                    blocking=False
                )
            )
            self.assertTrue(acquired)
            if acquired:
                sms_delivery_reporter._SMS_DELIVERY_RECONCILE_THREAD_LOCK.release()

    def test_reconciles_after_temporary_append_failure_without_duplicate(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        detected_at = datetime(2026, 7, 23, 9, 0, tzinfo=timezone.utc)
        self._remember_accepted(detected_at=detected_at)
        # outbox 저장 시각은 실제 현재시각이므로 그 값 이후를 reconcile 시계로 사용한다.
        stored_at = datetime.fromisoformat(
            sms_delivery_reporter._load_sms_delivery_outbox_items()[0][
                "storedAt"
            ].replace("Z", "+00:00")
        )
        now = stored_at + timedelta(seconds=1)

        with (
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                return_value={},
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                return_value={
                    "status": "SENDING",
                    "count": {"sentPending": 1},
                },
            ),
            patch.object(
                sms_delivery_reporter,
                "_has_device_health_sheet_sms_tracking_group_id",
                return_value=False,
            ),
            patch.object(
                sms_delivery_reporter,
                "_append_sms_delivery_outbox_item_to_sheet",
                side_effect=RuntimeError("temporary sheet failure"),
            ) as append_mock,
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=now,
            )

        self.assertEqual(changed, 0)
        append_mock.assert_called_once()
        self.assertEqual(
            sms_delivery_reporter._load_sms_delivery_outbox_items()[0][
                "smsDeliveryStatus"
            ],
            "accepted",
        )

        accepted_row = {
            "G-ACCEPTED": {
                "rowNumber": 8,
                "groupId": "G-ACCEPTED",
                "smsStatus": "접수됨",
            }
        }
        with (
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                side_effect=[{}, accepted_row],
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                return_value={
                    "status": "SENDING",
                    "count": {"sentPending": 1},
                },
            ),
            patch.object(
                sms_delivery_reporter,
                "_has_device_health_sheet_sms_tracking_group_id",
                return_value=False,
            ),
            patch.object(
                sms_delivery_reporter,
                "_append_sms_delivery_outbox_item_to_sheet",
                return_value=True,
            ) as append_mock,
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=now,
            )

        self.assertEqual(changed, 1)
        append_mock.assert_called_once()
        self.assertEqual(
            len(sms_delivery_reporter._load_sms_delivery_outbox_items()),
            1,
        )

        with (
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                return_value=accepted_row,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                return_value={
                    "status": "COMPLETE",
                    "count": {
                        "sentSuccess": 1,
                        "sentFailed": 0,
                        "sentPending": 0,
                    },
                },
            ),
            patch.object(
                sms_delivery_reporter,
                "_append_sms_delivery_outbox_item_to_sheet",
            ) as append_mock,
            patch.object(
                sms_delivery_reporter,
                "_update_device_health_sheet_sms_status_by_group_id",
                return_value=True,
            ) as update_mock,
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=now,
            )

        self.assertEqual(changed, 1)
        append_mock.assert_not_called()
        update_mock.assert_called_once_with(
            row_number=8,
            group_id="G-ACCEPTED",
            sms_status="수신 완료",
        )
        self.assertEqual(
            sms_delivery_reporter._load_sms_delivery_outbox_items(),
            [],
        )

    def test_outbox_max_age_marks_confirmation_without_provider_lookup(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        detected_at = datetime(2026, 7, 21, 8, 0, tzinfo=timezone.utc)
        now = detected_at + timedelta(hours=49)
        self._remember_accepted(detected_at=detected_at)

        with (
            patch.object(
                sms_delivery_reporter.cs,
                "SOLAPI_DELIVERY_REPORT_MAX_AGE_HOURS",
                48,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                return_value={
                    "G-ACCEPTED": {
                        "rowNumber": 9,
                        "groupId": "G-ACCEPTED",
                        "smsStatus": "접수됨",
                    }
                },
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
            ) as load_group_mock,
            patch.object(
                sms_delivery_reporter,
                "_update_device_health_sheet_sms_status_by_group_id",
                return_value=True,
            ) as update_mock,
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=now,
            )

        self.assertEqual(changed, 1)
        load_group_mock.assert_not_called()
        update_mock.assert_called_once_with(
            row_number=9,
            group_id="G-ACCEPTED",
            sms_status="확인 필요",
        )
        self.assertEqual(
            sms_delivery_reporter._load_sms_delivery_outbox_items(),
            [],
        )

    def test_outbox_max_age_uses_sms_acceptance_not_detection_time(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        now = datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc)
        remembered = sms_delivery_reporter.remember_sms_delivery_sheet_record(
            {
                "device": "MB2-C00043",
                "hospital": "테스트 병원",
                "room": "진료실",
                "components": ["캡처보드"],
                "issue": "연결 실패",
                "smsDeliveryStatus": "accepted",
                "smsGroupId": "G-ACCEPTANCE-CLOCK",
            },
            detected_at=now - timedelta(hours=100),
            sms_accepted_at=now - timedelta(hours=1),
        )
        self.assertTrue(remembered)

        with (
            patch.object(
                sms_delivery_reporter.cs,
                "SOLAPI_DELIVERY_REPORT_MAX_AGE_HOURS",
                48,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                return_value={
                    "G-ACCEPTANCE-CLOCK": {
                        "rowNumber": 10,
                        "groupId": "G-ACCEPTANCE-CLOCK",
                        "smsStatus": "접수됨",
                    }
                },
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                return_value={
                    "status": "SENDING",
                    "count": {"sentPending": 1},
                },
            ) as load_group_mock,
            patch.object(
                sms_delivery_reporter,
                "_update_device_health_sheet_sms_status_by_group_id",
            ) as update_mock,
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=now,
            )

        self.assertEqual(changed, 0)
        load_group_mock.assert_called_once_with("G-ACCEPTANCE-CLOCK")
        update_mock.assert_not_called()

    def test_persists_final_provider_result_before_sheet_update_retry(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        now = datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc)
        self._remember_accepted(detected_at=now - timedelta(hours=1))
        accepted_row = {
            "G-ACCEPTED": {
                "rowNumber": 11,
                "groupId": "G-ACCEPTED",
                "smsStatus": "접수됨",
            }
        }

        with (
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                return_value=accepted_row,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                return_value={
                    "status": "COMPLETE",
                    "count": {
                        "sentSuccess": 1,
                        "sentFailed": 0,
                        "sentPending": 0,
                    },
                },
            ) as load_group_mock,
            patch.object(
                sms_delivery_reporter,
                "_update_device_health_sheet_sms_status_by_group_id",
                side_effect=RuntimeError("temporary sheet failure"),
            ),
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=now,
            )

        self.assertEqual(changed, 0)
        load_group_mock.assert_called_once_with("G-ACCEPTED")
        self.assertEqual(
            sms_delivery_reporter._load_sms_delivery_outbox_items()[0][
                "smsDeliveryStatus"
            ],
            "delivered",
        )

        with (
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_sms_delivery_rows",
                return_value=accepted_row,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
            ) as load_group_mock,
            patch.object(
                sms_delivery_reporter,
                "_update_device_health_sheet_sms_status_by_group_id",
                return_value=True,
            ),
        ):
            changed = sms_delivery_reporter._reconcile_sms_delivery_outbox_once(
                logger,
                now=now,
            )

        self.assertEqual(changed, 1)
        load_group_mock.assert_not_called()
        self.assertEqual(
            sms_delivery_reporter._load_sms_delivery_outbox_items(),
            [],
        )

    def test_sheet_only_pending_uses_accepted_at_max_age(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        accepted_at = datetime(2026, 7, 21, 8, 0, tzinfo=timezone.utc)
        now = accepted_at + timedelta(hours=49)

        with (
            patch.object(
                sms_delivery_reporter.cs,
                "SOLAPI_DELIVERY_REPORT_MAX_AGE_HOURS",
                48,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_pending_sms_deliveries",
                return_value=[
                    {
                        "rowNumber": 12,
                        "groupId": "G-SHEET-ONLY",
                        "acceptedAt": accepted_at.isoformat(),
                    }
                ],
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
            ) as load_group_mock,
            patch.object(
                sms_delivery_reporter,
                "_update_device_health_sheet_sms_status_by_group_id",
                return_value=True,
            ) as update_mock,
        ):
            changed = sms_delivery_reporter._run_sms_delivery_reporter_once(
                logger,
                now=now,
            )

        self.assertEqual(changed, 1)
        load_group_mock.assert_not_called()
        update_mock.assert_called_once_with(
            row_number=12,
            group_id="G-SHEET-ONLY",
            sms_status="확인 필요",
        )

    def test_sheet_only_missing_provider_record_marks_confirmation(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        response = Mock(status_code=404)
        missing_error = RuntimeError("missing")
        missing_error.response = response

        with (
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_pending_sms_deliveries",
                return_value=[
                    {
                        "rowNumber": 14,
                        "groupId": "G-MISSING",
                        "acceptedAt": "2026-07-23T09:00:00Z",
                    }
                ],
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                side_effect=missing_error,
            ),
            patch.object(
                sms_delivery_reporter,
                "_update_device_health_sheet_sms_status_by_group_id",
                return_value=True,
            ) as update_mock,
        ):
            changed = sms_delivery_reporter._run_sms_delivery_reporter_once(
                logger,
                now=datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc),
            )

        self.assertEqual(changed, 1)
        update_mock.assert_called_once_with(
            row_number=14,
            group_id="G-MISSING",
            sms_status="확인 필요",
        )

    def test_updates_only_final_delivery_results_and_reuses_group_lookup(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        pending = [
            {"rowNumber": 2, "groupId": "G-SUCCESS"},
            {"rowNumber": 3, "groupId": "G-SUCCESS"},
            {"rowNumber": 4, "groupId": "G-PENDING"},
            {"rowNumber": 5, "groupId": "G-FAILED"},
        ]

        def load_group(group_id: str):
            if group_id == "G-SUCCESS":
                return {
                    "status": "COMPLETE",
                    "count": {"sentSuccess": 1, "sentPending": 0},
                }
            if group_id == "G-FAILED":
                return {
                    "status": "COMPLETE",
                    "count": {"sentFailed": 1},
                }
            return {
                "status": "SENDING",
                "count": {"sentPending": 1},
            }

        with (
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_pending_sms_deliveries",
                return_value=pending,
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                side_effect=load_group,
            ) as load_group_mock,
            patch.object(
                sms_delivery_reporter,
                "_update_device_health_sheet_sms_status_by_group_id",
                return_value=True,
            ) as update_mock,
        ):
            updated_count = sms_delivery_reporter._run_sms_delivery_reporter_once(
                logger
            )

        self.assertEqual(updated_count, 3)
        self.assertEqual(load_group_mock.call_count, 3)
        self.assertEqual(
            [call.kwargs for call in update_mock.call_args_list],
            [
                {
                    "row_number": 2,
                    "group_id": "G-SUCCESS",
                    "sms_status": "수신 완료",
                },
                {
                    "row_number": 3,
                    "group_id": "G-SUCCESS",
                    "sms_status": "수신 완료",
                },
                {
                    "row_number": 5,
                    "group_id": "G-FAILED",
                    "sms_status": "수신 실패",
                },
            ],
        )

    def test_provider_lookup_error_keeps_sheet_row_pending(self) -> None:
        logger = logging.getLogger("test.sms_delivery_reporter")
        with (
            patch.object(
                sms_delivery_reporter,
                "_load_device_health_sheet_pending_sms_deliveries",
                return_value=[{"rowNumber": 2, "groupId": "G123"}],
            ),
            patch.object(
                sms_delivery_reporter,
                "_load_solapi_group_info",
                side_effect=RuntimeError("temporary"),
            ),
            patch.object(
                sms_delivery_reporter,
                "_update_device_health_sheet_sms_status_by_group_id",
            ) as update_mock,
        ):
            updated_count = sms_delivery_reporter._run_sms_delivery_reporter_once(
                logger
            )

        self.assertEqual(updated_count, 0)
        update_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
