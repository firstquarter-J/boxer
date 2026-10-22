"""OS 링크 근거와 앱 오류의 구분, 제한된 수집과 비밀값 제외를 검증한다."""

import ast
import datetime
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from boxer_company.routers import device_network_probe as network

_BOOT = "a" * 32
_STAMP = "2026-10-22T09:15:42+09:00"


def _check(value):
    return {"ok": True, "output": json.dumps(value)}


def _event(stamp, state, *, interface="enp1s0", source="kernel", boot=_BOOT, elapsed=1000, transition=None):
    return {
        "timestamp": stamp, "state": state, "source": source, "interface": interface,
        "boot": boot, "bootElapsedSeconds": elapsed, "transition": transition or "link_" + state,
    }


def _checks(events=None, app_events=None, **state):
    return {
        "network_state": _check({
            "status": "ok", "interface": "enp1s0", "connectionType": "wired", "carrier": True,
            "operstate": "up", "hasDefaultRoute": True, "dnsServers": ["192.0.2.53"], **state,
        }),
        "network_link_history": _check({"status": "ok", "events": events or []}),
        "network_app_history": _check({"status": "ok", "events": app_events or []}),
    }


def _remote_namespace(script):
    namespace = {}
    # 사용자 입력 없이 제품의 고정 원격 스크립트만 격리된 namespace에서 검증한다.
    exec(network._REMOTE_COMMON + script, namespace)  # noqa: S102
    results = []
    namespace["emit"] = results.append
    return namespace, results


class NetworkSummaryTests(unittest.TestCase):
    def test_c00330_link_loss_precedes_app_errors_and_current_recovery(self):
        # 실제 장애의 09:15 단절, 09:16 소켓 timeout, 09:32 재부팅 복구를 재현한다.
        events = [
            _event("2026-10-22T08:21:54+09:00", "up", elapsed=5),
            _event(_STAMP, "down"),
            _event("2026-10-22T09:15:42+09:00", "down", source="systemd-networkd.service", transition="lost_carrier"),
            _event("2026-10-22T09:32:39+09:00", "down", boot="b" * 32, elapsed=6),
            _event("2026-10-22T09:32:39+09:00", "up", boot="b" * 32, elapsed=6),
        ]
        app = [{"timestamp": "2026-10-22T09:16:11+09:00", "kind": "socket_error"}]
        result = network.build_network_summary(_checks(events, app))
        self.assertEqual(result["classification"], "network_link_failure")
        self.assertEqual(result["status"], "check_needed")
        self.assertIn("현재 링크는 연결", result["summary"])
        self.assertIn("1건", result["summary"])
        self.assertTrue(result["correlation"]["linkBeforeAppError"])
        self.assertIn("09:15:42", result["evidence"][0]["timestamp"])
        self.assertIn("랜포트", result["action"])

    def test_initial_kernel_down_is_not_incident_even_after_networkd_gain(self):
        events = [
            _event("2026-10-22T09:32:37+09:00", "up", source="systemd-networkd.service", elapsed=4),
            _event("2026-10-22T09:32:39+09:00", "down", elapsed=6),
            _event("2026-10-22T09:32:39+09:00", "up", elapsed=6),
        ]
        result = network.build_network_summary(_checks(events))
        self.assertEqual(result["classification"], "no_network_evidence")
        self.assertEqual(result["evidence"], [])

    def test_correlated_error_is_retained_when_later_retries_fill_evidence(self):
        # 원인 대조에 사용한 최초 오류가 후속 DNS 재시도에 밀려 사라지지 않는다.
        app = [{"timestamp": "2026-10-22T09:16:11+09:00", "kind": "socket_error"}]
        app += [{"timestamp": f"2026-10-22T10:{minute:02}:00+09:00", "kind": "dns_retry"} for minute in range(10)]
        result = network.build_network_summary(_checks([_event(_STAMP, "down")], app))
        self.assertTrue(result["correlation"]["linkBeforeAppError"])
        self.assertTrue(any(item["timestamp"] == result["correlation"]["appTimestamp"] for item in result["evidence"]))

    def test_kernel_down_after_long_uptime_is_retained_without_up_in_window(self):
        result = network.build_network_summary(_checks([_event(_STAMP, "down", elapsed=900000)]))
        self.assertEqual(result["classification"], "network_link_failure")

    def test_shutdown_related_link_down_does_not_prove_network_cause(self):
        events = [
            _event(_STAMP, "up"),
            _event("2026-10-22T09:27:33+09:00", "shutdown", source="system_lifecycle", interface="", transition="shutdown"),
            _event("2026-10-22T09:27:34+09:00", "down"),
        ]
        result = network.build_network_summary(_checks(events))
        self.assertEqual(result["classification"], "no_network_evidence")

    def test_shutdown_minutes_after_real_loss_does_not_hide_incident(self):
        events = [
            _event(_STAMP, "down"),
            _event("2026-10-22T09:27:33+09:00", "shutdown", source="system_lifecycle", interface="", transition="shutdown"),
        ]
        self.assertEqual(network.build_network_summary(_checks(events))["classification"], "network_link_failure")

    def test_shutdown_thirty_seconds_after_loss_preserves_cause_candidate(self):
        events = [
            _event(_STAMP, "down"),
            _event("2026-10-22T09:16:12+09:00", "shutdown", source="system_lifecycle", interface="", transition="shutdown"),
        ]
        self.assertEqual(network.build_network_summary(_checks(events))["classification"], "network_link_failure")

    def test_inactive_wifi_loopback_and_virtual_interfaces_do_not_affect_wired_route(self):
        events = [_event(_STAMP, "down", interface=name) for name in ("wlan0", "lo", "docker0", "veth123", "tun0")]
        result = network.build_network_summary(_checks(events))
        self.assertEqual(result["classification"], "no_network_evidence")
        self.assertEqual(result["connectionType"], "wired")

    def test_active_wireless_loss_has_wireless_action(self):
        result = network.build_network_summary(_checks(
            [_event(_STAMP, "down", interface="wlan0")], interface="wlan0", connectionType="wireless",
        ))
        self.assertEqual(result["classification"], "network_link_failure")
        self.assertIn("AP", result["action"])
        self.assertNotIn("케이블", result["action"])

    def test_dns_error_alone_never_proves_physical_link_failure(self):
        result = network.build_network_summary(_checks(app_events=[{"timestamp": _STAMP, "kind": "dns_retry"}]))
        self.assertEqual(result["classification"], "network_error_only")
        self.assertIn("확정할 수 없어", result["summary"])
        self.assertFalse(result["correlation"]["available"])

    def test_normal_client_disconnect_is_not_network_error(self):
        result = network.build_network_summary(_checks(app_events=[{"timestamp": _STAMP, "kind": "socket_disconnected"}]))
        self.assertEqual(result["classification"], "no_network_evidence")

    def test_manager_logical_disconnect_does_not_prove_physical_loss(self):
        event = _event(_STAMP, "disconnected", source="NetworkManager.service", transition="manager_state")
        result = network.build_network_summary(_checks([event]))
        self.assertEqual(result["classification"], "network_error_only")
        self.assertEqual(result["evidence"][0]["source"], "NetworkManager.service")

    def test_manager_event_is_not_app_correlation_evidence(self):
        events = [
            _event(_STAMP, "down"),
            _event(_STAMP, "disconnected", source="NetworkManager.service", transition="manager_state"),
        ]
        result = network.build_network_summary(_checks(events))
        self.assertFalse(result["correlation"]["available"])
        self.assertNotIn("앱 통신 오류도 기록", result["summary"])

    def test_mixed_timestamp_zones_do_not_crash_summary(self):
        events = [_event(_STAMP, "down"), _event("2026-10-22T09:16:00", "down", source="systemd-networkd.service", transition="lost_carrier")]
        result = network.build_network_summary(_checks(events))
        self.assertEqual(result["classification"], "network_link_failure")

    def test_current_link_down_and_missing_default_route_are_failures(self):
        self.assertEqual(network.build_network_summary(_checks(carrier=False, operstate="down"))["status"], "fail")
        result = network.build_network_summary(_checks(hasDefaultRoute=False))
        self.assertEqual(result["classification"], "network_route_failure")
        self.assertEqual(result["status"], "fail")

    def test_missing_permission_empty_timeout_and_partial_are_not_pass(self):
        # 수집 실패와 빈 조회를 각각 보존해 "정상"으로 오판하지 않는다.
        for status in ("permission_denied", "tool_missing", "empty", "timeout", "partial", "parse_failed"):
            with self.subTest(status=status):
                checks = _checks()
                checks["network_link_history"] = _check({"status": status, "events": []})
                result = network.build_network_summary(checks)
                self.assertEqual(result["status"], "check_needed")
                self.assertNotIn("network_link_history", result["summary"])
        result = network.build_network_summary({})
        self.assertFalse(result["available"])
        self.assertEqual(result["classification"], "check_unavailable")
        self.assertIsInstance(result["evidence"], list)

    def test_ssh_timeout_and_truncated_json_are_not_pass(self):
        for value in ({"ok": False, "reason": "timeout"}, {"ok": True, "output": '{"events":['}):
            checks = _checks()
            checks["network_link_history"] = value
            self.assertNotEqual(network.build_network_summary(checks)["status"], "pass")

    def test_untrusted_raw_lines_are_not_copied_to_summary(self):
        secret = "patient=Jane barcode=12345678901 token=supersecret"
        app = [{"timestamp": _STAMP, "kind": "dns_retry", "line": secret}, {"timestamp": secret, "kind": "dns_retry"}]
        result = json.dumps(network.build_network_summary(_checks(app_events=app)), ensure_ascii=False)
        self.assertNotIn("supersecret", result)
        self.assertNotIn("12345678901", result)
        self.assertNotIn("Jane", result)

    def test_error_before_link_event_does_not_claim_link_precedes_error(self):
        result = network.build_network_summary(_checks([_event(_STAMP, "down")], [{"timestamp": "2026-10-22T09:15:12+09:00", "kind": "dns_retry"}]))
        self.assertTrue(result["correlation"]["available"])
        self.assertFalse(result["correlation"]["linkBeforeAppError"])


class NetworkRemoteCollectorTests(unittest.TestCase):
    def test_remote_scripts_parse_with_python36_grammar_and_shell_syntax(self):
        # 원격 장비의 오래된 Python과 고정 명령 quoting을 실제 파서로 확인한다.
        for script in (network._REMOTE_STATE, network._REMOTE_LINK_HISTORY, network._REMOTE_APP_HISTORY):
            ast.parse(network._REMOTE_COMMON + script, feature_version=(3, 6))
        for spec in network.NETWORK_COMMAND_SPECS.values():
            result = subprocess.run(["sh", "-n", "-c", spec["command"]], capture_output=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_state_uses_default_route_interface_and_only_dns_ip_addresses(self):
        namespace, output = _remote_namespace(network._REMOTE_STATE)
        calls = []

        def fake_run(args, seconds=4):
            calls.append(args)
            if "route" in args:
                return "ok", json.dumps([{"dev": "wlan0", "metric": 600}, {"dev": "enp1s0", "metric": 100, "gateway": "192.0.2.1"}])
            if "address" in args:
                return "ok", json.dumps([{"addr_info": [{"scope": "global", "local": "192.0.2.10"}]}])
            return "ok", "Link 2 (enp1s0): 192.0.2.53 secret.invalid"

        namespace["run"] = fake_run
        namespace["read_text"] = lambda path: "1" if path.endswith("/carrier") else "up"
        with patch.object(namespace["shutil"], "which", return_value="/usr/bin/tool"):
            namespace["main"]()
        self.assertEqual(output[0]["interface"], "enp1s0")
        self.assertEqual(output[0]["connectionType"], "wired")
        self.assertEqual(output[0]["dnsServers"], ["192.0.2.53"])
        self.assertNotIn("secret.invalid", json.dumps(output))
        self.assertTrue(all(args[0] in {"ip", "resolvectl"} for args in calls))

    def test_history_collects_previous_boot_kernel_and_network_manager(self):
        namespace, output = _remote_namespace(network._REMOTE_LINK_HISTORY)
        rows = [
            {"_TRANSPORT": "kernel", "MESSAGE": "r8169 0000:01:00.0 enp1s0: Link is Down SECRET=hidden", "__MONOTONIC_TIMESTAMP": "900000000"},
            {"_SYSTEMD_UNIT": "NetworkManager.service", "MESSAGE": "device (enp1s0): state change: activated -> disconnected (reason 'carrier-changed')"},
            {"_PID": "1", "MESSAGE": "Stopping Network Configuration..."},
        ]
        for index, row in enumerate(rows):
            row.update({"_BOOT_ID": _BOOT, "__REALTIME_TIMESTAMP": str(1792631742000000 + index * 1000000)})
        calls = []

        def fake_run(args, seconds):
            calls.append(args)
            return "ok", "\n".join(json.dumps(row) for row in rows)

        namespace["run"] = fake_run
        namespace["main"]()
        self.assertEqual(len(output[0]["events"]), 3)
        self.assertEqual(output[0]["events"][0]["bootElapsedSeconds"], 900)
        self.assertEqual(output[0]["events"][1]["state"], "disconnected")
        self.assertEqual(output[0]["events"][2]["transition"], "shutdown")
        self.assertNotIn("hidden", json.dumps(output))
        self.assertNotIn("-k", calls[0])
        self.assertIn("72 hours ago", calls[0])

    def test_journal_permission_failure_is_explicit(self):
        namespace, output = _remote_namespace(network._REMOTE_LINK_HISTORY)
        namespace["run"] = lambda *args: ("permission_denied", "")
        namespace["main"]()
        self.assertEqual(output[0]["status"], "permission_denied")
        self.assertEqual(output[0]["events"], [])

    def test_duplicate_kernel_networkd_records_preserve_early_loss_under_cap(self):
        namespace, output = _remote_namespace(network._REMOTE_LINK_HISTORY)
        rows = []
        for index in range(10):
            for offset, state in ((0, "Up"), (30, "Down")):
                timestamp = 1792631742000000 + (index * 120 + offset) * 1000000
                for source, text in (("kernel", "enp1s0: Link is " + state), ("systemd-networkd.service", "enp1s0: " + ("Gained carrier" if state == "Up" else "Lost carrier"))):
                    row = {"MESSAGE": text, "_BOOT_ID": _BOOT, "__REALTIME_TIMESTAMP": str(timestamp), "__MONOTONIC_TIMESTAMP": "900000000"}
                    row["_TRANSPORT" if source == "kernel" else "_SYSTEMD_UNIT"] = source
                    rows.append(row)
        namespace["run"] = lambda *args: ("ok", "\n".join(json.dumps(row) for row in rows))
        namespace["main"]()
        payload = output[0]
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(len(payload["events"]), 20)
        self.assertEqual(payload["events"][1]["transition"], "lost_carrier")
        self.assertLess(len(json.dumps(payload, separators=(",", ":"))), 8000)

    def test_app_history_reads_four_dates_cuts_at_72_hours_and_discards_pii(self):
        # 시각은 실제 현재를 기준으로 만들고 원문은 임시 파일에만 둔다.
        now = datetime.datetime.now().astimezone()
        with tempfile.TemporaryDirectory() as directory:
            logs = Path(directory) / "AppData" / "logs"
            logs.mkdir(parents=True)
            lines = [
                (now - datetime.timedelta(hours=1), "Socket reconnection error: websocket error patient=Jane token=SECRET barcode=12345678901"),
                (now - datetime.timedelta(hours=2), "No Internet Connection"),
                (now - datetime.timedelta(hours=70), "FTP connect ENETUNREACH"),
                (now - datetime.timedelta(hours=73), "getaddrinfo EAI_AGAIN OLDSECRET"),
                (now - datetime.timedelta(minutes=1), "Mommybox Starting"),
            ]
            for stamp, message in lines:
                path = logs / ("log-" + stamp.date().isoformat() + ".log")
                with path.open("a") as stream:
                    stream.write(stamp.strftime("%Y-%m-%d_%H:%M:%S") + " [App] " + message + "\n")
            namespace, output = _remote_namespace(network._REMOTE_APP_HISTORY)
            with patch.dict(os.environ, {"HOME": directory}):
                namespace["main"]()
        payload = output[0]
        self.assertEqual(payload["counts"]["route_unreachable"], 1)
        self.assertEqual(payload["counts"]["socket_error"], 1)
        self.assertEqual(payload["counts"]["app_offline"], 1)
        self.assertEqual(payload["counts"]["app_lifecycle"], 1)
        self.assertNotIn("dns_retry", payload["counts"])
        for forbidden in ("Jane", "SECRET", "12345678901", "patient", "barcode"):
            self.assertNotIn(forbidden, json.dumps(payload))

    def test_app_permission_failure_is_not_empty_history(self):
        namespace, output = _remote_namespace(network._REMOTE_APP_HISTORY)
        with patch.object(namespace["os"], "stat", side_effect=PermissionError):
            namespace["main"]()
        self.assertEqual(output[0]["status"], "permission_denied")

    def test_repeated_app_errors_preserve_episode_start_and_total_count(self):
        now = datetime.datetime.now().astimezone()
        with tempfile.TemporaryDirectory() as directory:
            logs = Path(directory) / "AppData" / "logs"
            logs.mkdir(parents=True)
            path = logs / ("log-" + now.date().isoformat() + ".log")
            stamp = now.strftime("%Y-%m-%d_%H:%M:%S")
            path.write_text((stamp + " EAI_AGAIN\n") * 100)
            namespace, output = _remote_namespace(network._REMOTE_APP_HISTORY)
            with patch.dict(os.environ, {"HOME": directory}):
                namespace["main"]()
        self.assertEqual(output[0]["status"], "ok")
        self.assertEqual(len(output[0]["events"]), 1)
        self.assertEqual(output[0]["counts"]["dns_retry"], 100)
        self.assertLess(len(json.dumps(output[0])), 8000)

    def test_normal_reconnections_do_not_displace_first_failure(self):
        # 복구 이후 정상 연결 로그가 많아도 같은 날 최초 통신 오류를 남긴다.
        now = datetime.datetime.now().astimezone()
        entries = [(now - datetime.timedelta(hours=4), "FTP ENETUNREACH")]
        entries += [
            (now - datetime.timedelta(minutes=6 * index), "Socket Connected")
            for index in range(1, 21)
        ]
        with tempfile.TemporaryDirectory() as directory:
            logs = Path(directory) / "AppData" / "logs"
            logs.mkdir(parents=True)
            for stamp, message in entries:
                path = logs / ("log-" + stamp.date().isoformat() + ".log")
                with path.open("a") as stream:
                    stream.write(stamp.strftime("%Y-%m-%d_%H:%M:%S") + " " + message + "\n")
            namespace, output = _remote_namespace(network._REMOTE_APP_HISTORY)
            with patch.dict(os.environ, {"HOME": directory}):
                namespace["main"]()
        self.assertTrue(any(event["kind"] == "route_unreachable" for event in output[0]["events"]))
        self.assertLessEqual(len(output[0]["events"]), 24)
        self.assertTrue(output[0]["eventsTruncated"])

    def test_app_retry_episodes_keep_first_and_last_before_output_limit(self):
        now = datetime.datetime.now().astimezone()
        starts = [now - datetime.timedelta(hours=2), now - datetime.timedelta(hours=1)]
        with tempfile.TemporaryDirectory() as directory:
            logs = Path(directory) / "AppData" / "logs"
            logs.mkdir(parents=True)
            for start in starts:
                for index in range(40):
                    stamp = start + datetime.timedelta(seconds=20 * index)
                    path = logs / ("log-" + stamp.date().isoformat() + ".log")
                    with path.open("a") as stream:
                        stream.write(stamp.strftime("%Y-%m-%d_%H:%M:%S") + " Socket reconnection error: websocket error\n")
            namespace, output = _remote_namespace(network._REMOTE_APP_HISTORY)
            with patch.dict(os.environ, {"HOME": directory}):
                namespace["main"]()
        self.assertEqual(output[0]["counts"]["socket_error"], 80)
        self.assertEqual(len(output[0]["events"]), 4)
        self.assertEqual(output[0]["events"][0]["timestamp"], starts[0].replace(microsecond=0).isoformat())
        self.assertEqual(output[0]["events"][2]["timestamp"], starts[1].replace(microsecond=0).isoformat())

    def test_many_separate_app_incidents_report_output_truncation(self):
        now = datetime.datetime.now().astimezone()
        with tempfile.TemporaryDirectory() as directory:
            logs = Path(directory) / "AppData" / "logs"
            logs.mkdir(parents=True)
            for index in range(30):
                stamp = now - datetime.timedelta(minutes=6 * index)
                path = logs / ("log-" + stamp.date().isoformat() + ".log")
                with path.open("a") as stream:
                    stream.write(stamp.strftime("%Y-%m-%d_%H:%M:%S") + " EAI_AGAIN\n")
            namespace, output = _remote_namespace(network._REMOTE_APP_HISTORY)
            with patch.dict(os.environ, {"HOME": directory}):
                namespace["main"]()
        self.assertEqual(output[0]["status"], "partial")
        self.assertGreater(len(output[0]["events"]), 0)
        self.assertLessEqual(len(output[0]["events"]), 24)

    def test_remote_run_does_not_emit_raw_stderr_or_timeout_details(self):
        namespace, _ = _remote_namespace(network._REMOTE_STATE)
        result = subprocess.CompletedProcess(["ip"], 1, stdout="", stderr="Permission denied SECRET=hidden")
        with patch.object(namespace["shutil"], "which", return_value="/usr/bin/ip"), patch.object(namespace["subprocess"], "run", return_value=result):
            self.assertEqual(namespace["run"](["ip"]), ("permission_denied", ""))
        with patch.object(namespace["shutil"], "which", return_value="/usr/bin/ip"), patch.object(namespace["subprocess"], "run", side_effect=subprocess.TimeoutExpired(["ip"], 4, output="SECRET")):
            self.assertEqual(namespace["run"](["ip"]), ("timeout", ""))
