from __future__ import annotations

import unittest
from unittest.mock import Mock

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.device_db_detail_route import (
    DeviceDbDetailAssistantRoute,
    match_device_db_detail_route,
)


def _request(question: str) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-DEVICE-DB-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question=question,
        locale="ko",
    )


class DeviceDbDetailRouteTests(unittest.TestCase):
    def test_matcher_accepts_only_device_detail_and_list_queries(self) -> None:
        expected = (
            "MB2-C00419 장비 정보",
            "장비명=MB2-C00419 장비 상세",
            "deviceSeq=42 devices",
            "status=ACTIVE 장비 목록",
            "activeFlag=1 장비 목록",
        )

        for question in expected:
            with self.subTest(question=question):
                self.assertEqual(
                    match_device_db_detail_route(_request(question)),
                    "device_db_detail",
                )

    def test_matcher_leaves_count_and_existence_on_devices_filter(self) -> None:
        existing_rollout_queries = (
            "MB2-C00419 장비 몇 개야",
            "MB2-C00419 장비 있나",
            "MB2-C00419 장비 있는지",
            "activeFlag=1 장비 개수",
            "status=ACTIVE 장비 몇 개",
        )

        for question in existing_rollout_queries:
            with self.subTest(question=question):
                self.assertIsNone(
                    match_device_db_detail_route(_request(question))
                )

    def test_matcher_rejects_live_probe_and_enrichment_intents(self) -> None:
        live_queries = (
            "MB2-C00419 온라인이야?",
            "MB2-C00419 오프라인인지 확인해줘",
            "MB2-C00419 연결 상태 확인",
            "MB2-C00419 SSH 접속 돼?",
            "MB2-C00419 MDA 정보",
            "MB2-C00419 버전 알려줘",
            "MB2-C00419 캡처보드 종류 알려줘",
            "MB2-C00419 캡쳐 카드 종류 알려줘",
            "MB2-C00419 엠디에이 정보 알려줘",
            "MB2-C00419 원격 접속 확인해줘",
            "MB2-C00419 장비 정보 삭제해줘",
            "MB2-C00419 status 변경해줘",
            "MB2-C00419 PM2 프로세스 상태 확인",
            "MB2-C00419 장비 상태",
            "MB2-C00419 장비 상태 체크",
            "MB2-C00419 지금 상태 어때?",
            "MB2-C00419 ping 보내봐",
        )

        for question in live_queries:
            with self.subTest(question=question):
                self.assertIsNone(
                    match_device_db_detail_route(_request(question))
                )

    def test_route_calls_only_db_query_without_live_enrichment(self) -> None:
        query = Mock(
            return_value=(
                "*장비 조회 결과*\n"
                "• 장비 번호: `42`\n"
                "• 장비명: `MB2-C00419`\n"
                "• 버전: `9.9.9`\n"
                "• version: `9.9.9`\n"
                "• 병원: `테스트병원`\n"
                "• 병실: `1진료실`\n"
                "• SSH 연결 상태: 연결 가능\n"
                "• 초음파 영상 다운로드 가능 상태: 가능\n"
                "• 캡처보드 종류: `USB`\n"
                "• captureBoardType: `USB`\n"
                "• MDA 상태: `ONLINE`\n"
                "• PM2 상태: `online`\n"
                "• status: `ACTIVE`\n"
                "• 활성 유무: `활성`\n"
                "• 설치 유무: `설치`"
            )
        )
        route = DeviceDbDetailAssistantRoute(query_devices=query)

        result = route.handle(_request("MB2-C00419 장비 정보"))

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, "device_db_detail")
        self.assertEqual(result.outcome, "answered")
        self.assertFalse(result.used_llm)
        self.assertEqual(result.sources, ())
        self.assertIsNone(result.suggested_action)
        query.assert_called_once_with(
            device_name="MB2-C00419",
            device_seq=None,
            hospital_name=None,
            room_name=None,
            hospital_seq=None,
            hospital_room_seq=None,
            status=None,
            active_flag=None,
            install_flag=None,
            count_only=False,
            include_live_enrichment=False,
        )

        body = result.messages[0].body
        self.assertIn("**장비 조회 결과**", body)
        self.assertIn("`devices.status` DB 저장값", body)
        self.assertIn("DB 저장 status: `ACTIVE`", body)
        self.assertNotIn("버전", body)
        self.assertNotIn("version", body.lower())
        self.assertNotIn("캡처보드", body)
        self.assertNotIn("captureboard", body.lower())
        self.assertNotIn("SSH", body)
        self.assertNotIn("MDA", body)
        self.assertNotIn("PM2", body)
        self.assertNotIn("다운로드 가능 상태", body)

    def test_route_does_not_call_db_for_count_or_live_query(self) -> None:
        query = Mock(return_value="호출되면 안 됨")
        route = DeviceDbDetailAssistantRoute(query_devices=query)

        self.assertIsNone(route.handle(_request("MB2-C00419 장비 몇 개야")))
        self.assertIsNone(route.handle(_request("MB2-C00419 장비 상태 확인")))
        query.assert_not_called()

    def test_route_maps_db_dependency_failure_without_raw_error(self) -> None:
        query = Mock(side_effect=RuntimeError("secret-db-host"))
        route = DeviceDbDetailAssistantRoute(query_devices=query)

        result = route.handle(_request("MB2-C00419 장비 정보"))

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "dependency_error")
        self.assertNotIn("secret-db-host", result.messages[0].body)


if __name__ == "__main__":
    unittest.main()
