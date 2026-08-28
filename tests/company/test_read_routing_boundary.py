from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import textwrap
import unittest


class ReadRoutingBoundaryTests(unittest.TestCase):
    def test_all_transport_matchers_stay_provider_free(self) -> None:
        """Slack이 모든 matcher를 호출해도 회사 실행 graph를 열지 않는다."""

        project_root = Path(__file__).resolve().parents[2]
        script = textwrap.dedent(
            """
            import sys

            from boxer_company.assistant.contracts import CompanyAssistantRequest
            from boxer_company import read_routing as routing

            def request(question, *, route_group=None, context_entries=()):
                metadata = {}
                if route_group:
                    metadata["route_group"] = route_group
                return CompanyAssistantRequest(
                    request_id="read-boundary",
                    tenant_id="company",
                    actor_id="U123",
                    channel="slack",
                    conversation_id="C123",
                    question=question,
                    locale="ko-KR",
                    context_entries=context_entries,
                    metadata=metadata,
                )

            calls = (
                lambda: routing.match_barcode_log_route(
                    request("12345678901 2026-08-28 로그 분석")
                ),
                lambda: routing.match_barcode_query_route(
                    request("12345678901 영상 개수")
                ),
                lambda: routing.match_common_api_barcode_query_route(
                    request("12345678901 영상 목록")
                ),
                lambda: routing.match_barcode_timeline_route(
                    request("12345678901 2026-08-28 녹화가 있었어?")
                ),
                lambda: routing.match_device_detail_route(
                    request("MB2-A00068 현재 상태")
                ),
                lambda: routing.match_device_read_route(
                    request("MB2-A00068 LED 패턴 알려줘")
                ),
                lambda: routing.match_company_freeform_route(
                    request("안녕", route_group="freeform")
                ),
                lambda: routing.match_usage_help_route(
                    request("사용법", route_group="freeform")
                ),
                lambda: routing.match_usage_help_rollout_route(
                    request("도움말 알려줘")
                ),
                lambda: routing.match_barcode_evidence_freeform_route(
                    request("12345678901 녹화 기록 근거로 성공 여부를 분석해줘")
                ),
                lambda: routing.match_notion_playbook_route(
                    request("마미박스 녹화 취소 음성 원인 알려줘")
                ),
                lambda: routing.match_weekly_recordings_summary_route(
                    request("이번 주 초음파 영상 현황")
                ),
                lambda: routing.match_recording_failure_route(
                    request("12345678901 녹화 실패 원인 분석")
                ),
                lambda: routing.match_structured_device_count_route(
                    request("장비명 MB2-A00068 장비 개수")
                ),
                lambda: routing.match_structured_read_route(
                    request("2026년 생성된 병원 목록")
                ),
                lambda: routing._looks_like_company_notion_search(
                    "회사 노션에서 온보딩 문서 찾아줘"
                ),
                lambda: routing.is_safe_baby_magic_source_uri(
                    "https://cdn-kr.mmtalkbox.com/result/example.jpg"
                ),
            )
            for call in calls:
                call()

            forbidden = (
                "boto3",
                "botocore",
                "pymysql",
                "paramiko",
                "redis",
                "google.auth",
                "cryptography",
                "anthropic",
                "requests",
            )
            loaded = sorted(
                name
                for name in sys.modules
                if any(
                    name == prefix or name.startswith(prefix + ".")
                    for prefix in forbidden
                )
            )
            heavy = sorted(
                name
                for name in sys.modules
                if name.startswith("boxer_company.routers")
                or name in {
                    "boxer_company.assistant.barcode_log_route",
                    "boxer_company.assistant.barcode_query_route",
                    "boxer_company.assistant.device_db_detail_route",
                    "boxer_company.assistant.device_led_routes",
                    "boxer_company.assistant.freeform_route",
                    "boxer_company.assistant.knowledge_routes",
                    "boxer_company.assistant.operational_read_routes",
                    "boxer_company.assistant.recording_failure_route",
                    "boxer_company.assistant.structured_route",
                    "boxer_company.notion_workspace_search",
                    "boxer_company.weekly_recordings_report",
                }
            )
            if loaded or heavy:
                raise SystemExit(
                    f"provider={loaded!r} heavy={heavy!r}"
                )
            """
        )
        env = dict(os.environ)
        env["BOXER_SKIP_DOTENV"] = "true"
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=project_root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            msg=completed.stderr or completed.stdout,
        )


if __name__ == "__main__":
    unittest.main()
