"""날짜 범위와 원격 수집 경계를 실제 고정 스크립트로 검증한다."""

import ast
import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from boxer_company.routers import device_network_probe as network

KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 10, 22, 12, 30, 0, tzinfo=KST)


def _remote(script, window):
    namespace = {"_FIXED_START": window["startEpoch"], "_FIXED_END": window["endEpoch"]}
    exec(network._REMOTE_COMMON + script, namespace)  # noqa: S102
    return namespace


def _run(namespace):
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        namespace["main"]()
    return json.loads(output.getvalue())


def _check(payload):
    return {"ok": True, "output": json.dumps(payload)}


class NetworkDateProbeTests(unittest.TestCase):
    def test_today_relative_and_explicit_today_end_at_request_time(self):
        with patch.dict(os.environ, {"TZ": "UTC"}):
            for question in ("", "오늘", "금일", "2026-10-22", "10월 22일"):
                with self.subTest(question=question):
                    window = network.resolve_network_window(question, NOW.astimezone(UTC))
                    self.assertEqual(window["date"], "2026-10-22")
                    self.assertEqual(window["startEpoch"], NOW.replace(hour=0, minute=0).timestamp())
                    self.assertEqual(window["endEpoch"], NOW.timestamp())
                    self.assertTrue(window["isCurrentDay"])

    def test_past_day_uses_full_kst_day(self):
        for question, expected in (("어제", "2026-10-21"), ("10월 21일", "2026-10-21"), ("2026-10-20", "2026-10-20"), ("그제", "2026-10-20"), ("day before yesterday", "2026-10-20")):
            with self.subTest(question=question):
                window = network.resolve_network_window(question, NOW)
                self.assertEqual(window["date"], expected)
                self.assertEqual(window["endEpoch"] - window["startEpoch"], 86400)
                self.assertFalse(window["isCurrentDay"])

    def test_version_tokens_do_not_select_an_unrequested_date(self):
        # 장비/앱 버전의 앞부분이 월일로 매칭되어 엉뚱한 하루를 조회하지 않는다.
        for version in ("2.11.300", "v2.11.300", "버전 12.22.1", "version: 12.22.1", "Node.js v12.22.1"):
            with self.subTest(version=version):
                window = network.resolve_network_window(f"MB2-C00330 마미박스 {version} 네트워크 오류 확인해줘", NOW)
                self.assertEqual(window["date"], "2026-10-22")
                self.assertEqual(window["endEpoch"], NOW.timestamp())
        for date in ("2026.10.21", "26.10.21"):
            with self.subTest(date=date):
                window = network.resolve_network_window(f"MB2-C00330 2.11.300 {date} 네트워크 오류 확인해줘", NOW)
                self.assertEqual(window["date"], "2026-10-21")
        with self.assertRaises(network.NetworkDateWindowError):
            network.resolve_network_window("MB2-C00330 2.11.300 2026.13.40 네트워크 확인해줘", NOW)

    def test_bad_ambiguous_future_or_unsupported_dates_fail_closed(self):
        for question in ("2026-02-30", "2026-02-30 2026-10-20", "2026-10-20 2026-10-21", "오늘 어제", "내일", "2026-10-23", "화요일부터", "지난주", "이번주", "최근 3일", "10월 20일부터 21일까지", "10월", "21일", "last week"):
            with self.subTest(question=question), self.assertRaises(network.NetworkDateWindowError):
                network.resolve_network_window(question, NOW)

    def test_window_command_builder_accepts_only_validated_epochs(self):
        window = network.resolve_network_window("어제", NOW)
        specs = network.build_network_command_specs(dict(window, label="UNTRUSTED_LABEL"))
        self.assertEqual(set(specs), set(network.NETWORK_COMMAND_KEYS))
        for spec in specs.values():
            self.assertNotIn("UNTRUSTED_LABEL", spec["command"])
            self.assertIn("_FIXED_START = " + repr(window["startEpoch"]), spec["command"])
            self.assertNotIn("72 hours ago", spec["command"])
        for bad in (dict(window, startEpoch="0; injected"), dict(window, endEpoch=float("nan")), dict(window, endEpoch=window["startEpoch"] - 1)):
            with self.assertRaises(network.NetworkDateWindowError):
                network.build_network_command_specs(bad)

    def test_all_remote_fragments_keep_python36_grammar(self):
        for script in (network._REMOTE_STATE, network._REMOTE_LINK_HISTORY, network._REMOTE_APP_HISTORY):
            ast.parse(network._REMOTE_COMMON + script, feature_version=(3, 6))
        self.assertIn("datetime.datetime.now(KST)", network._REMOTE_COMMON)

    def test_journal_filters_half_open_range_even_when_journal_returns_outside_rows(self):
        window = network.resolve_network_window("어제", NOW)
        namespace = _remote(network._REMOTE_LINK_HISTORY, window)
        rows = []
        for seconds, state in ((-1, "Down"), (0, "Up"), (60, "Down"), (86400, "Down")):
            rows.append(json.dumps({"_TRANSPORT": "kernel", "MESSAGE": "enp1s0: Link is " + state, "_BOOT_ID": "a" * 32, "__REALTIME_TIMESTAMP": str(int((window["startEpoch"] + seconds) * 1000000)), "__MONOTONIC_TIMESTAMP": "900000000"}))
        calls = []
        namespace["run"] = lambda args, seconds: (calls.append(args) or "ok", "\n".join(rows))
        result = _run(namespace)
        self.assertIn("--until", calls[0])
        self.assertEqual(result["eventCount"], 2)
        self.assertEqual(result["events"][0]["timestamp"], "2026-10-21T00:00:00+09:00")
        self.assertEqual(result["networkWindow"]["date"], "2026-10-21")

    def test_app_reads_selected_kst_file_and_excludes_neighbors(self):
        window = network.resolve_network_window("어제", NOW)
        namespace = _remote(network._REMOTE_APP_HISTORY, window)
        with tempfile.TemporaryDirectory(prefix="network-day-") as temporary:
            logs = Path(temporary) / "AppData" / "logs"
            logs.mkdir(parents=True)
            (logs / "log-2026-10-21.log").write_text("2026-10-20_23:59:59 Socket reconnection error: websocket error\n2026-10-21_00:00:00 Socket reconnection error: websocket error patient=SECRET\n2026-10-21_23:59:59 No Internet Connection\n2026-10-22_00:00:00 Socket reconnection error: websocket error\n")
            (logs / "log-2026-10-22.log").write_text("2026-10-21_12:00:00 Socket reconnection error: websocket error\n")
            with patch("os.path.expanduser", side_effect=lambda path: path.replace("~", temporary, 1)):
                result = _run(namespace)
        self.assertEqual(result["filesRead"], 1)
        self.assertEqual(len(result["events"]), 2)
        self.assertNotIn("SECRET", json.dumps(result))
        self.assertTrue(all(row["timestamp"].startswith("2026-10-21") for row in result["events"]))

    def test_missing_files_and_query_failure_still_report_window(self):
        window = network.resolve_network_window("어제", NOW)
        namespace = _remote(network._REMOTE_APP_HISTORY, window)
        with patch("os.stat", side_effect=FileNotFoundError):
            result = _run(namespace)
        self.assertEqual(result["filesRead"], 0)
        self.assertEqual(result["networkWindow"]["date"], "2026-10-21")
        namespace = _remote(network._REMOTE_LINK_HISTORY, window)
        namespace["run"] = lambda *_: ("permission_denied", "")
        self.assertEqual(_run(namespace)["networkWindow"]["date"], "2026-10-21")

    def test_historical_cause_ignores_current_carrier_and_outside_events(self):
        window = network.resolve_network_window("어제", NOW)
        checks = {
            "network_state": _check({"status": "ok", "interface": "enp1s0", "connectionType": "wired", "carrier": False, "operstate": "down", "hasDefaultRoute": False}),
            "network_link_history": _check({"status": "ok", "events": [{"timestamp": "2026-10-22T09:15:42+09:00", "source": "systemd-networkd.service", "interface": "enp1s0", "boot": "a" * 32, "state": "down", "transition": "lost_carrier"}]}),
            "network_app_history": _check({"status": "ok", "events": []}),
        }
        summary = network.build_network_summary(checks, window)
        self.assertEqual(summary["classification"], "no_network_evidence")
        self.assertEqual(summary["evidence"], [])
        self.assertIn("2026-10-21 00:00:00", summary["summary"])


if __name__ == "__main__":
    unittest.main()
