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


if __name__ == "__main__":
    unittest.main()
