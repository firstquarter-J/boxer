import unittest
from datetime import date
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from boxer.answering import AnswerResult
from boxer_company.assistant.factory import create_company_assistant_runtime
from boxer_company_api.app import create_company_api_app
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings


class NaturalLanguageReadApiTests(unittest.TestCase):
    TOKEN = "q" * 48
    BARCODE = "12345678910"
    RAW_ROUTE = "natural_language_read"

    def setUp(self):
        self.recordings = {"summary": {}, "rows": [], "limit": 30, "has_more": False}
        self.loader = Mock(return_value=self.recordings)
        # 실제 API→회사 런타임→공통 조회를 연결하고 외부 provider와 DB만 주입한다.
        with (
            patch(
                "boxer_company.assistant.factory.core_settings.LLM_PROVIDER", "ollama"
            ),
            patch(
                "boxer_company.assistant.factory.core_settings.LLM_SYNTHESIS_ENABLED",
                False,
            ),
            patch(
                "boxer_company.assistant.factory._load_recordings_context_by_barcode",
                self.loader,
            ),
        ):
            self.runtime = create_company_assistant_runtime(
                natural_language_read_enabled=True
            )

    def make_client(self, *, enabled=True):
        app = create_company_api_app(
            settings=CompanyApiSettings(
                host="127.0.0.1",
                port=8010,
                callers=(
                    CompanyApiCallerSettings(
                        caller_id="read-integration",
                        token=self.TOKEN,
                        tenant_ids=frozenset({"TENANT-1"}),
                        channels=frozenset({"slack"}),
                        actor_ids=frozenset({"ACTOR-1"}),
                        capabilities=frozenset({"assistant.turn.read"}),
                    ),
                ),
                natural_language_read_enabled=enabled,
            ),
            assistant_runtime=self.runtime,
            readiness_probe=lambda: True,
        )
        return TestClient(app)

    def post(self, client, question, **options):
        return client.post(
            "/internal/v1/assistant/turns",
            headers={
                "Authorization": f"Bearer {self.TOKEN}",
                "X-Request-ID": "req-natural-language-read",
            },
            json={
                "tenantId": "TENANT-1",
                "actorId": "ACTOR-1",
                "channel": "slack",
                "conversationId": "THREAD-1",
                "question": question,
                "locale": "ko",
                "routeGroup": self.RAW_ROUTE,
                **options,
            },
        )

    @staticmethod
    def model_response(intent):
        return AnswerResult(
            text=f'{{"intent":"{intent}"}}',
            provider="ollama",
            used_llm=True,
        )

    @staticmethod
    def context(text, author="ACTOR-1"):
        return {"kind": "message", "source": "slack", "authorId": author, "text": text}

    def test_rules_and_natural_language_share_the_same_zero_count(self):
        with (
            patch(
                "boxer.answering.AnswerEngine.answer",
                return_value=self.model_response("video_count"),
            ) as model,
            patch(
                "boxer_company.assistant.barcode_query_route._query_recordings_count_by_barcode",
                return_value="영상 개수 0건",
            ) as query,
            self.make_client() as client,
        ):
            standard = self.post(
                client, f"{self.BARCODE} 영상 개수", routeGroup="barcode"
            )
            model.assert_not_called()
            interpreted = self.post(
                client,
                "이걸로 찍힌 게 몇 건이야?",
                contextEntries=[self.context(f"확인할 바코드는 {self.BARCODE}야")],
            )
        self.assertEqual((standard.status_code, interpreted.status_code), (200, 200))
        self.assertEqual(interpreted.json()["route"], "barcode_video_count")
        self.assertEqual(standard.json()["messages"], interpreted.json()["messages"])
        self.assertTrue(interpreted.json()["usedLlm"])
        self.assertEqual(query.call_count, 2)
        self.assertEqual(query.call_args.args, (self.BARCODE,))
        model.assert_called_once()

    def test_date_followup_changes_only_the_date(self):
        with (
            patch(
                "boxer.answering.AnswerEngine.answer",
                return_value=self.model_response("recorded_on_date"),
            ),
            patch(
                "boxer_company.assistant.natural_language_read._current_local_date",
                return_value=date(2026, 11, 21),
            ),
            patch(
                "boxer_company.assistant.barcode_query_route._query_recordings_on_date_by_barcode",
                return_value="오늘 녹화 내역이 있어",
            ) as query,
            self.make_client() as client,
        ):
            response = self.post(
                client,
                "오늘은?",
                contextEntries=[self.context(f"{self.BARCODE} 어제 녹화됐어?")],
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["route"], "barcode recordedAt-on-date")
        self.assertEqual(response.json()["outcome"], "answered")
        query.assert_called_once_with(
            self.BARCODE, "2026-11-21", recordings_context=self.recordings
        )

    def test_count_does_not_drop_a_date_or_pick_another_actors_barcode(self):
        with (
            patch(
                "boxer.answering.AnswerEngine.answer",
                return_value=self.model_response("video_count"),
            ),
            self.make_client() as client,
        ):
            dated = self.post(client, f"{self.BARCODE} 어제 찍힌 게 몇 건이야?")
            ambiguous = self.post(
                client,
                "몇 건 나왔어?",
                contextEntries=[
                    self.context(f"내 바코드는 {self.BARCODE}", author="ANOTHER-ACTOR"),
                ],
            )
            scoped = self.post(
                client,
                f"{self.BARCODE} 기준으로 몇 건 나왔어?",
                scope={"hospitalName": "합성병원", "roomName": "합성병실"},
            )
        for response in (dated, ambiguous, scoped):
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["outcome"], "needs_input")
        self.loader.assert_not_called()

    def test_playbook_preserves_sources_and_no_evidence(self):
        source = {
            "pageId": "abcd1234",
            "title": "영상 업로드 점검",
            "url": "https://www.notion.so/upload-guide",
            "previewLines": ["네트워크 연결을 확인해"],
            "plainText": "전체 내장 문서 원문",
        }
        with (
            patch(
                "boxer.answering.AnswerEngine.answer",
                return_value=self.model_response("playbook"),
            ),
            patch(
                "boxer_company.assistant.knowledge_routes._select_notion_references",
                side_effect=[[source], []],
            ) as selector,
            self.make_client() as client,
        ):
            found = self.post(client, "서버로 파일이 안 올라가면 어디부터 봐?")
            missing = self.post(client, "서버로 파일이 안 올라가면 어디부터 봐?")
        self.assertEqual(found.json()["route"], "notion_playbook_qa")
        self.assertEqual(found.json()["outcome"], "answered")
        self.assertEqual(found.json()["sources"][0]["uri"], source["url"])
        self.assertNotIn(source["plainText"], found.text)
        self.assertEqual(missing.json()["route"], "notion_playbook_qa")
        self.assertEqual(missing.json()["outcome"], "no_evidence")
        self.assertEqual(selector.call_count, 2)
        self.loader.assert_not_called()

    def test_blocked_scopes_and_disabled_feature_do_not_call_the_model(self):
        with (
            patch("boxer.answering.AnswerEngine.answer") as model,
            self.make_client(enabled=False) as disabled_client,
            self.make_client() as client,
        ):
            disabled = self.post(disabled_client, "이걸로 찍힌 게 몇 건이야?")
            sensitive = self.post(client, "MB2-C00419 장비 정보")
            invalid_scope = self.post(
                client,
                f"{self.BARCODE} 기준 결과물이 몇 건이야?",
                scope={"barcode": "22345678910"},
            )
        self.assertEqual(disabled.json()["route"], "unhandled")
        self.assertEqual(sensitive.status_code, 403)
        self.assertEqual(invalid_scope.json()["route"], self.RAW_ROUTE)
        self.assertEqual(invalid_scope.json()["outcome"], "denied")
        model.assert_not_called()
        self.loader.assert_not_called()

    def test_true_unmatched_and_bad_provider_output_have_distinct_outcomes(self):
        with (
            patch(
                "boxer.answering.AnswerEngine.answer",
                side_effect=[
                    self.model_response("unhandled"),
                    self.model_response("unregistered_operation"),
                ],
            ),
            self.make_client() as client,
        ):
            unmatched = self.post(client, "안녕")
            malformed = self.post(client, f"{self.BARCODE} 기준 결과물이 몇 건이야?")
        self.assertEqual(unmatched.json()["fallbackReason"], "no_matching_route")
        self.assertEqual(malformed.json()["route"], self.RAW_ROUTE)
        self.assertEqual(malformed.json()["outcome"], "failed")
        self.assertNotIn("unregistered_operation", malformed.text)
        self.loader.assert_not_called()
