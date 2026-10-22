from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Any

from boxer_company._operation_routing_device import (
    _extract_device_name_for_diagnostic_freeform,
    _select_device_diagnostic_followup_command_keys,
    match_device_operation_route,
)
from boxer_company.operation_routing import (
    company_operation_legacy_stage,
    match_company_operation_route,
)


@dataclass(frozen=True)
class _Request:
    question: str
    metadata: dict[str, Any] = field(default_factory=lambda: {"route_group": "operations"})
    context_entries: tuple[dict[str, str], ...] = ()
    actor_id: str | None = "test-actor"
    channel: str = "test"


class DeviceNetworkRoutingTests(unittest.TestCase):
    def test_explicit_network_questions_analyze_link_and_app_evidence(self) -> None:
        # 매체 이름이나 연결 상태 표현이 달라도 앱 오류와 실제 회선 이력을 함께 본다.
        required_keys = {"network_state", "network_link_history", "network_app_history", "app_recent_logs"}
        for symptom in (
            "네트워크 확인해줘",
            "네트워크 오류 원인 확인해줘",
            "유선 연결 상태 확인해줘",
            "무선 단절 원인 확인해줘",
            "랜 연결 확인해줘",
            "랜선 끊김 확인해줘",
            "와이파이 연결 확인해줘",
            "와이 파이 단절 확인해줘",
            "통신오류 원인 확인해줘",
            "Wi-Fi 연결 확인해줘",
            "LAN 연결 상태 확인해줘",
            "ethernet 확인해줘",
        ):
            question = f"MB2-C00330 {symptom}"
            with self.subTest(question=question):
                self.assertEqual(match_device_operation_route(_Request(question)), "device_diagnostic_analysis")
                keys = _select_device_diagnostic_followup_command_keys(question)
                self.assertTrue(required_keys.issubset(keys))
                self.assertEqual(len(keys), len(set(keys)))

    def test_company_matcher_keeps_network_analysis_in_knowledge_stage(self) -> None:
        # Slack/API의 전체 분류에서도 DB 장비 조회나 일반 상태 probe가 선점하지 않는다.
        for symptom in ("네트워크 확인해줘", "유선 연결 상태 확인해줘", "네트워크 오류 원인 확인해줘"):
            request = _Request(f"MB2-C00330 {symptom}", metadata={})
            with self.subTest(symptom=symptom):
                self.assertEqual(match_company_operation_route(request), "device_diagnostic_analysis")
                self.assertEqual(company_operation_legacy_stage(request), "knowledge")

    def test_general_failure_analysis_also_checks_network(self) -> None:
        # 증상만 주어진 경우에도 회선 단절을 배제하지 않고 같은 시각의 근거를 수집한다.
        for symptom in ("왜 캡처가 안돼?", "앱오류 확인해줘", "재부팅 원인 확인해줘", "최근 종료 원인 알려줘"):
            question = f"MB2-C00330 {symptom}"
            with self.subTest(question=question):
                self.assertEqual(match_device_operation_route(_Request(question)), "device_diagnostic_analysis")
                self.assertTrue(
                    {"network_state", "network_link_history", "network_app_history"}.issubset(
                        _select_device_diagnostic_followup_command_keys(question)
                    )
                )

    def test_network_questions_keep_existing_target_scope(self) -> None:
        # 네트워크 의도 추가로 병실 탐색이나 metadata의 장비명 보강을 열지 않는다.
        for question in ("네트워크 확인해줘", "테스트병원 1진료실 네트워크 확인해줘"):
            with self.subTest(question=question):
                self.assertIsNone(match_device_operation_route(_Request(question)))
                self.assertIsNone(_extract_device_name_for_diagnostic_freeform(question))
        self.assertIsNone(
            match_device_operation_route(
                _Request("네트워크 확인해줘", {"route_group": "operations", "device_name": "MB2-C00330"})
            )
        )
        self.assertEqual(
            _extract_device_name_for_diagnostic_freeform("MB2-C00330 MB2-C00999 네트워크 확인해줘"),
            "MB2-C00330",
        )

    def test_network_followup_reuses_existing_diagnostic_context(self) -> None:
        request = _Request(
            "유선 연결 상태 확인해줘",
            context_entries=({"text": "MB2-C00330 진단 시작"},),
        )
        self.assertEqual(match_device_operation_route(request), "device_diagnostic_followup")

    def test_existing_component_and_mutation_priority_is_preserved(self) -> None:
        for symptom, route in (
            ("장비 상태 확인해줘", "device_status_probe"),
            ("PM2 상태 네트워크 확인해줘", "device_pm2_probe"),
            ("캡처보드 상태 네트워크 확인해줘", "device_captureboard_probe"),
            ("LED 상태 네트워크 확인해줘", "device_led_probe"),
            ("메모리 패치", "device_memory_patch"),
            ("진단 시작", "device_diagnostic_snapshot"),
        ):
            with self.subTest(symptom=symptom):
                self.assertEqual(match_device_operation_route(_Request(f"MB2-C00330 {symptom}")), route)

    def test_narrow_memory_disk_and_metadata_questions_keep_existing_commands(self) -> None:
        # 특정 자원만 확인할 때는 불필요한 네트워크 이력 명령을 추가하지 않는다.
        self.assertEqual(
            _select_device_diagnostic_followup_command_keys("메모리 확인해줘"),
            ["pm2_jlist", "memory", "kernel_oom"],
        )
        self.assertEqual(_select_device_diagnostic_followup_command_keys("디스크 확인해줘"), ["pm2_jlist", "disk"])
        for question in ("MB2-C00330 장비 상태 확인해줘", "이 장비 어느 병원이야?", "MB2-C00330 랜덤 확인해줘"):
            with self.subTest(question=question):
                self.assertEqual(_select_device_diagnostic_followup_command_keys(question), [])


if __name__ == "__main__":
    unittest.main()
