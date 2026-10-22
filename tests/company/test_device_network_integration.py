"""실제 조회 근거가 상태 답변과 진단 LLM 경계까지 전달되는지 검증한다."""

import json
import unittest
from unittest.mock import patch

from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company.routers import device_diagnostics as diagnostics
from boxer_company.routers import device_status_probe as status_probe


def _network_summary():
    return {
        "available": True,
        "status": "check_needed",
        "label": "과거 링크 단절",
        "connectionType": "wired",
        "interface": "enp1s0",
        "classification": "network_link_failure",
        "summary": "현재 유선 연결은 복구됐지만 최근 72시간에 실제 링크 단절이 확인됐어",
        "action": "랜 케이블, 연결 단자, 스위치 포트와 장비 랜 포트를 점검해",
        "evidence": [{
            "source": "network_link_history",
            "line": "2026-10-22T09:15:42+09:00 enp1s0: Lost carrier",
        }],
        "historyWindowHours": 72,
    }


class DeviceNetworkIntegrationTests(unittest.TestCase):
    def test_status_overview_reports_recovered_historical_failure(self):
        # 지금 SSH가 살아 있어도 이전 링크 단절을 종합 정상으로 숨기지 않는다.
        with (
            patch.object(status_probe, "_send_mda_device_ping", return_value={"status": True}),
            patch.object(status_probe, "_collect_runtime_checks", return_value=(
                {"ssh": {"ready": True}}, {}, {"network_state": {"ok": True}},
            )) as collect,
            patch.object(status_probe, "build_network_summary", return_value=_network_summary()) as network,
        ):
            text, evidence = status_probe._probe_device_status_overview(
                "MB2-C00330", resend_ssh_open=False, allow_force_reopen=False,
            )

        collect.assert_called_once_with(
            "MB2-C00330", "all", resend_ssh_open=False, allow_force_reopen=False,
            network_window=collect.call_args.kwargs["network_window"],
        )
        window = collect.call_args.kwargs["network_window"]
        self.assertEqual(window["timezone"], "Asia/Seoul")
        self.assertTrue(window["isCurrentDay"])
        network.assert_called_once_with({"network_state": {"ok": True}}, window=window)
        self.assertIn("Lost carrier", text)
        self.assertIn("랜 케이블", text)
        self.assertIn("확인 필요", text)
        self.assertEqual(evidence["overview"]["network"]["classification"], "network_link_failure")

    def test_ssh_failure_does_not_fabricate_network_diagnosis(self):
        with (
            patch.object(status_probe, "_send_mda_device_ping", return_value={"status": False}),
            patch.object(status_probe, "_collect_runtime_checks", return_value=(
                {"ssh": {"ready": False, "reason": "agent_ssh_not_ready"}}, {}, {},
            )),
            patch.object(status_probe, "_select_remote_access_notion_references", return_value=[]),
            patch.object(status_probe, "build_network_summary") as network,
        ):
            text, evidence = status_probe._probe_device_status_overview("MB2-C00330")

        network.assert_not_called()
        self.assertIsNone(evidence["overview"]["network"])
        self.assertIn("네트워크: *점검 불가*", text)
        self.assertNotIn("네트워크 연결 문제로 판단", text)

    def test_diagnostic_summary_keeps_network_evidence_separate_from_log_limit(self):
        checks = {"network_state": {"ok": True}}
        with patch.object(diagnostics, "build_network_summary", return_value=_network_summary()) as build:
            summary = diagnostics._build_device_diagnostic_summary({"checks": checks})
        build.assert_called_once_with(checks, window=None)
        self.assertEqual(summary["network"]["classification"], "network_link_failure")

    def test_repeated_app_errors_do_not_hide_latest_link_failure(self):
        network = _network_summary()
        network["evidence"] = [
            {"timestamp": f"2026-10-22T09:{minute}:00+09:00", "source": "kernel", "line": "링크 단절"}
            for minute in ("01", "02", "03", "15")
        ] + [
            {"timestamp": "2026-10-22T09:31:00+09:00", "source": "app_daily_log", "line": "DNS EAI_AGAIN"}
            for _ in range(6)
        ]
        displayed = status_probe._network_display_evidence(network)
        self.assertEqual(len(displayed), 3)
        self.assertTrue(any("09:15:00" in row["timestamp"] for row in displayed))
        self.assertEqual(displayed[-1]["source"], "app_daily_log")

    def test_manager_logical_changes_do_not_displace_physical_link_evidence(self):
        network = _network_summary()
        network["evidence"] = [
            {"timestamp": "2026-10-22T09:15:42+09:00", "source": "kernel", "kind": "link_down", "line": "실제 링크 단절"},
            {"timestamp": "2026-10-22T09:16:00+09:00", "source": "NetworkManager.service", "line": "연결 종료 상태 전이"},
            {"timestamp": "2026-10-22T09:17:00+09:00", "source": "NetworkManager.service", "line": "연결 종료 상태 전이"},
        ]
        displayed = status_probe._network_display_evidence(network)
        self.assertEqual(displayed[0]["kind"], "link_down")

    def test_diagnostic_fallback_uses_latest_network_summary(self):
        snapshot = {
            "request": {"deviceName": "MB2-C00330"},
            "ssh": {"ready": True},
            "summary": {},
            "followupLiveCheck": {
                "performed": True, "commandKeys": ["network_state"],
                "ssh": {"ready": True}, "summary": {"network": _network_summary()},
            },
        }
        text = diagnostics._build_device_diagnostic_followup_fallback("네트워크 원인", snapshot)
        self.assertIn("Lost carrier", text)
        self.assertIn("최근 72시간", text)
        self.assertIn("스위치 포트", text)

    def test_both_diagnostic_modes_keep_safe_network_summary_and_latest_evidence(self):
        for route in ("device_diagnostic_snapshot", "device_diagnostic_freeform"):
            with self.subTest(route=route):
                payload = {
                    "route": route,
                    "ssh": {"ready": True, "host": "private-host", "port": 12345},
                    "summary": {"network": _network_summary()},
                    "checks": {"network_state": {"command": "private-command", "output": "raw-state"}},
                    "followupLiveCheck": {
                        "performed": True, "readOnly": True, "capturedAt": "now",
                        "ssh": {"ready": True, "host": "private-host"},
                        "summary": {"network": _network_summary()},
                        "checks": {
                            "memory": {"ok": True, "output": "memory evidence"},
                            "network_app_history": {"output": "raw-history"},
                        },
                    },
                }
                transformed = _transform_company_retrieval_payload(payload)
                encoded = json.dumps(transformed, ensure_ascii=False)
                self.assertIn("Lost carrier", encoded)
                for excluded in ("private-host", "private-command", "raw-state", "raw-history"):
                    self.assertNotIn(excluded, encoded)
                self.assertEqual(transformed["followupLiveCheck"]["checks"]["memory"]["output"], "memory evidence")
                rules = _build_company_retrieval_rules(payload)
                for required in ("network_link_failure", "network_error_only", "check_unavailable", "followupLiveCheck"):
                    self.assertIn(required, rules)


if __name__ == "__main__":
    unittest.main()
