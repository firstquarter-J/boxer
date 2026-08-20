from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
from unittest.mock import patch

from boxer_company.routers.mda_graphql import (
    _get_mda_latest_device_version,
    _open_mda_device_ssh,
    _get_mda_stopped_recording_restore_candidates,
    _normalize_mda_device_detail,
    _restore_mda_stopped_recordings,
    _send_mda_device_command,
    _wait_for_mda_device_agent_ssh,
)


class MdaSshOpenTests(unittest.TestCase):
    @patch("boxer_company.routers.mda_graphql._execute_mda_graphql_request")
    @patch(
        "boxer_company.routers.mda_graphql._get_mda_access_token",
        return_value="cached-token",
    )
    def test_ssh_open_does_not_retry_unauthorized_mutation(
        self,
        get_access_token,
        execute_request,
    ) -> None:
        execute_request.side_effect = RuntimeError("Unauthorized")

        with self.assertRaisesRegex(RuntimeError, "Unauthorized"):
            _open_mda_device_ssh(
                "MB2-C00419",
                host="tunnel.internal",
            )

        get_access_token.assert_called_once_with()
        execute_request.assert_called_once()


class MdaLatestDeviceVersionTests(unittest.TestCase):
    @patch("boxer_company.routers.mda_graphql._execute_mda_graphql")
    def test_picks_highest_visible_semver_even_when_auto_update_is_false(
        self,
        mock_execute_mda_graphql,
    ) -> None:
        mock_execute_mda_graphql.return_value = {
            "deviceVersions": [
                {"versionName": "legacy", "autoUpdate": True, "visibleFlag": True},
                {"versionName": "2.11.299", "autoUpdate": False, "visibleFlag": True},
                {"versionName": "2.11.300", "autoUpdate": False, "visibleFlag": True},
                {"versionName": "3.0.0-beta", "autoUpdate": True, "visibleFlag": True},
                {"versionName": "3.2.10", "autoUpdate": True, "visibleFlag": False},
            ]
        }

        result = _get_mda_latest_device_version()

        self.assertEqual(result["versionName"], "2.11.300")
        self.assertTrue(result["visibleFlag"])

    @patch("boxer_company.routers.mda_graphql._execute_mda_graphql")
    def test_raises_when_no_semver_version_exists(
        self,
        mock_execute_mda_graphql,
    ) -> None:
        mock_execute_mda_graphql.return_value = {
            "deviceVersions": [
                {"versionName": "legacy", "autoUpdate": True, "visibleFlag": True},
                {"versionName": "", "autoUpdate": False, "visibleFlag": True},
            ]
        }

        with self.assertRaisesRegex(RuntimeError, "최신 박스 버전"):
            _get_mda_latest_device_version()


class MdaDeviceDetailNormalizationTests(unittest.TestCase):
    def test_normalizes_optional_device_config_booleans(self) -> None:
        result = _normalize_mda_device_detail(
            {
                "deviceName": "MB2-C00419",
                "version": "2.11.300",
                "cfg1_use_diary_capture": 1,
                "cfg1_check_invalid_barcode": 0,
                "cfg1_check_expired_barcode": "1",
                "cfg1_check_pink_barcode": -1,
                "deviceState": {},
                "hospital": {},
                "hospitalRoom": {},
                "agentState": {},
            },
            device_name="MB2-C00419",
        )

        self.assertTrue(result["useDiaryCapture"])
        self.assertFalse(result["checkInvalidBarcode"])
        self.assertEqual(result["checkExpiredBarcode"], 1)
        self.assertEqual(result["checkPinkBarcode"], -1)


class MdaDeviceCommandTests(unittest.TestCase):
    @patch("boxer_company.routers.mda_graphql._execute_mda_graphql_once")
    def test_sends_optional_acme_payload_for_scan_simulation(
        self,
        mock_execute_mda_graphql,
    ) -> None:
        # 음성 변경은 command와 payload를 분리해 MDA의 기존 mutation 계약으로 보낸다.
        mock_execute_mda_graphql.return_value = {
            "sendCommand": {"affected": 1, "status": True, "message": "sent"}
        }

        result = _send_mda_device_command(
            "MB2-C00419",
            command="scansim",
            acme="S_VOICE1",
        )

        variables = mock_execute_mda_graphql.call_args.args[1]
        self.assertEqual(
            variables,
            {
                "deviceName": "MB2-C00419",
                "command": "scansim",
                "acme": "S_VOICE1",
            },
        )
        self.assertEqual(result["acme"], "S_VOICE1")
        self.assertTrue(result["status"])

    @patch("boxer_company.routers.mda_graphql._execute_mda_graphql_request")
    @patch(
        "boxer_company.routers.mda_graphql._get_mda_access_token",
        return_value="cached-token",
    )
    def test_device_command_does_not_retry_unauthorized_mutation(
        self,
        get_access_token,
        execute_request,
    ) -> None:
        execute_request.side_effect = RuntimeError("Unauthorized")

        with self.assertRaisesRegex(RuntimeError, "Unauthorized"):
            _send_mda_device_command(
                "MB2-C00419",
                command="scansim",
                acme="S_VOICE1",
            )

        get_access_token.assert_called_once_with()
        execute_request.assert_called_once()

    @patch("boxer_company.routers.mda_graphql._open_mda_device_ssh")
    @patch("boxer_company.routers.mda_graphql._get_mda_device_agent_ssh")
    def test_existing_endpoint_reuses_without_open(
        self,
        get_device_ssh,
        open_device_ssh,
    ) -> None:
        # Redis/MDA에 endpoint가 이미 있으면 기존 Slack과 동일하게 즉시 쓴다.
        get_device_ssh.return_value = {
            "deviceName": "MB2-C00419",
            "agentSsh": {"host": "remotes.example", "port": 61001},
        }

        result = _wait_for_mda_device_agent_ssh(
            "MB2-C00419",
            resend_enabled=False,
        )

        self.assertTrue(result["ready"])
        self.assertTrue(result["reusedExisting"])
        open_device_ssh.assert_not_called()

    def test_closed_stale_endpoint_opens_configured_host_instead_of_reuse(
        self,
    ) -> None:
        closed = {
            "deviceName": "MB2-C00419",
            "agentSsh": {
                "host": "13.209.169.234",
                "port": 61001,
                "status": "close",
            },
        }
        opened = {
            "deviceName": "MB2-C00419",
            "agentSsh": {
                "host": "remotes.example",
                "port": 61002,
                "status": "open",
            },
        }

        with (
            patch(
                "boxer_company.routers.mda_graphql.time.monotonic",
                side_effect=[0, 0, 1],
            ),
            patch(
                "boxer_company.routers.mda_graphql.time.sleep",
                return_value=None,
            ),
            patch(
                "boxer_company.routers.mda_graphql._get_mda_device_agent_ssh",
                side_effect=[closed, opened],
            ),
            patch(
                "boxer_company.routers.mda_graphql._open_mda_device_ssh",
                return_value={"status": True},
            ) as open_device_ssh,
        ):
            result = _wait_for_mda_device_agent_ssh(
                "MB2-C00419",
                host="remotes.example",
                poll_timeout_sec=2,
                poll_interval_sec=1,
                resend_enabled=False,
            )

        open_device_ssh.assert_called_once_with(
            "MB2-C00419",
            host="remotes.example",
        )
        self.assertTrue(result["ready"])
        self.assertFalse(result["reusedExisting"])

    def test_api_path_opens_once_and_only_polls_afterward(self) -> None:
        missing = {
            "deviceName": "MB2-C00419",
            "agentSsh": None,
        }
        ready = {
            "deviceName": "MB2-C00419",
            "agentSsh": {"host": "remotes.example", "port": 61001},
        }

        with (
            patch(
                "boxer_company.routers.mda_graphql.time.monotonic",
                side_effect=[0, 0, 1, 2],
            ),
            patch(
                "boxer_company.routers.mda_graphql.time.sleep",
                return_value=None,
            ),
            patch(
                "boxer_company.routers.mda_graphql._open_mda_device_ssh",
                return_value={"status": True},
            ) as open_device_ssh,
            patch(
                "boxer_company.routers.mda_graphql._get_mda_device_agent_ssh",
                side_effect=[missing, missing, missing, ready],
            ) as get_device_ssh,
        ):
            result = _wait_for_mda_device_agent_ssh(
                "MB2-C00419",
                host="remotes.example",
                poll_timeout_sec=5,
                poll_interval_sec=1,
                # resend 주기가 1이어도 API 경로는 최초 open만 허용한다.
                resend_every=1,
                resend_enabled=False,
            )

        open_device_ssh.assert_called_once_with(
            "MB2-C00419",
            host="remotes.example",
        )
        self.assertTrue(result["ready"])
        self.assertFalse(result["reusedExisting"])
        self.assertEqual(get_device_ssh.call_count, 4)

    def test_same_device_concurrent_waits_send_open_once(self) -> None:
        # 두 요청이 동시에 endpoint 부재를 봐도 첫 요청의 open/poll 전체가
        # 끝난 뒤 두 번째 요청이 endpoint를 재조회해 기존 tunnel을 재사용한다.
        state_lock = threading.Lock()
        start = threading.Barrier(2)
        state = {"ready": False, "open_count": 0}

        def get_device_ssh(_device_name: str):
            with state_lock:
                ready = state["ready"]
            return {
                "deviceName": "MB2-C00419",
                "agentSsh": (
                    {"host": "remotes.example", "port": 61001}
                    if ready
                    else None
                ),
            }

        def open_device_ssh(_device_name: str, *, host: str):
            self.assertEqual(host, "remotes.example")
            with state_lock:
                state["open_count"] += 1
                state["ready"] = True
            return {"status": True}

        def wait_for_endpoint():
            start.wait(timeout=2)
            return _wait_for_mda_device_agent_ssh(
                "MB2-C00419",
                host="remotes.example",
                poll_timeout_sec=2,
                poll_interval_sec=1,
                resend_enabled=False,
            )

        with (
            patch(
                "boxer_company.routers.mda_graphql.time.sleep",
                return_value=None,
            ),
            patch(
                "boxer_company.routers.mda_graphql._get_mda_device_agent_ssh",
                side_effect=get_device_ssh,
            ),
            patch(
                "boxer_company.routers.mda_graphql._open_mda_device_ssh",
                side_effect=open_device_ssh,
            ),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            results = list(executor.map(lambda _index: wait_for_endpoint(), range(2)))

        self.assertEqual(state["open_count"], 1)
        self.assertTrue(all(result["ready"] for result in results))
        self.assertEqual(
            sorted(result["reusedExisting"] for result in results),
            [False, True],
        )

    @patch("boxer_company.routers.mda_graphql.time.sleep", return_value=None)
    @patch("boxer_company.routers.mda_graphql._open_mda_device_ssh")
    @patch("boxer_company.routers.mda_graphql._get_mda_device_agent_ssh")
    def test_force_reopen_does_not_reuse_cached_ssh_endpoint(
        self,
        get_device_ssh,
        open_device_ssh,
        _sleep,
    ) -> None:
        cached = {
            "deviceName": "MB2-C00419",
            "agentUpdatedAt": "2026-08-04T06:40:00Z",
            "agentSsh": {"host": "remotes.example", "port": 61001},
        }
        refreshed = {
            "deviceName": "MB2-C00419",
            "agentUpdatedAt": "2026-08-04T06:40:02Z",
            # 같은 포트가 재할당돼도 agent 갱신 시각으로 새 터널을 구분한다.
            "agentSsh": {"host": "remotes.example", "port": 61001},
        }
        # 첫 poll의 기존 endpoint는 무시하고 실제 agent 갱신까지 기다린다.
        get_device_ssh.side_effect = [cached, cached, refreshed]
        open_device_ssh.return_value = {"status": True}

        result = _wait_for_mda_device_agent_ssh(
            "MB2-C00419",
            poll_timeout_sec=2,
            poll_interval_sec=1,
            force_reopen=True,
        )

        open_device_ssh.assert_called_once_with(
            "MB2-C00419",
            host="remotes.example",
        )
        self.assertTrue(result["ready"])
        self.assertFalse(result["reusedExisting"])
        self.assertEqual(result["device"]["agentSsh"]["port"], 61001)
        self.assertEqual(
            result["device"]["agentUpdatedAt"],
            "2026-08-04T06:40:02Z",
        )
        self.assertEqual(get_device_ssh.call_count, 3)

    def test_force_reopen_sends_open_once_across_multiple_polls(self) -> None:
        cached = {
            "deviceName": "MB2-C00419",
            "agentUpdatedAt": "2026-08-04T06:40:00Z",
            "agentSsh": {"host": "remotes.example", "port": 61001},
        }

        with (
            patch(
                "boxer_company.routers.mda_graphql.time.monotonic",
                side_effect=[0, 0, 1, 2, 3, 4, 5],
            ),
            patch(
                "boxer_company.routers.mda_graphql.time.sleep",
                return_value=None,
            ),
            patch(
                "boxer_company.routers.mda_graphql._open_mda_device_ssh",
                return_value={"status": True},
            ) as open_device_ssh,
            patch(
                "boxer_company.routers.mda_graphql._get_mda_device_agent_ssh",
                return_value=cached,
            ) as get_device_ssh,
        ):
            result = _wait_for_mda_device_agent_ssh(
                "MB2-C00419",
                poll_timeout_sec=5,
                poll_interval_sec=1,
                force_reopen=True,
            )

        open_device_ssh.assert_called_once_with(
            "MB2-C00419",
            host="remotes.example",
        )
        self.assertFalse(result["ready"])
        self.assertEqual(result["pollCount"], 5)
        self.assertEqual(get_device_ssh.call_count, 6)


class MdaStoppedRecordingRestoreTests(unittest.TestCase):
    @patch("boxer_company.routers.mda_graphql._execute_mda_graphql")
    def test_normalizes_stopped_recording_restore_candidates(
        self,
        mock_execute_mda_graphql,
    ) -> None:
        mock_execute_mda_graphql.return_value = {
            "stoppedRecordingRestoreCandidates": [
                {
                    "seq": 101,
                    "fullBarcode": "35033165423",
                    "fileId": "abc",
                    "recordedAt": "2024-04-12T00:00:00.000Z",
                    "currentS3FileKey": "0000/abc.mp4",
                    "expectedS3FileKey": "35033165423/abc.mp4",
                    "restorable": True,
                    "failureReason": None,
                }
            ]
        }

        result = _get_mda_stopped_recording_restore_candidates("35033165423", 53)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["seq"], 101)
        self.assertEqual(result[0]["fullBarcode"], "35033165423")
        self.assertTrue(result[0]["restorable"])
        self.assertEqual(result[0]["expectedS3FileKey"], "35033165423/abc.mp4")

    @patch("boxer_company.routers.mda_graphql._execute_mda_graphql_once")
    def test_restore_stopped_recordings_uses_mda_mutation_input(
        self,
        mock_execute_mda_graphql,
    ) -> None:
        mock_execute_mda_graphql.return_value = {
            "restoreStoppedRecordings": {
                "status": True,
                "message": "복원 1건, 실패 0건",
                "requestedCount": 1,
                "restoredCount": 1,
                "failedCount": 0,
                "failedItems": [],
            }
        }

        result = _restore_mda_stopped_recordings(
            barcode="35033165423",
            hospital_seq=53,
            recording_seqs=[101],
            reason="Boxer 테스트",
        )

        self.assertTrue(result["status"])
        self.assertEqual(result["restoredCount"], 1)
        variables = mock_execute_mda_graphql.call_args.args[1]
        self.assertEqual(
            variables["input"],
            {
                "barcode": "35033165423",
                "hospitalSeq": 53,
                "recordingSeqs": [101],
                "reason": "Boxer 테스트",
            },
        )


if __name__ == "__main__":
    unittest.main()
