from __future__ import annotations

import hashlib
import logging
import unittest
import uuid
from datetime import timedelta
from typing import Any

from boxer_company.hpa_change_workflow import (
    GitHubArtifactArchive,
    GitHubWorkflowRun,
    HpaChangeJobStore,
    HpaChangePollResult,
    HpaChangePollState,
    HpaChangeStatus,
)
from boxer_company_adapter_slack.hpa_change_reporter import run_hpa_change_reporter_once
from boxer_company_adapter_slack.hpa_change_routes import HpaChangeRoutesConfig
from boxer_company_adapter_slack.hpa_change_runtime import HpaChangeRuntime


class _FakeSlackClient:
    def __init__(self, *, fail_count: int = 0, event_log: list[str] | None = None) -> None:
        self.fail_count = fail_count
        self.calls: list[dict[str, Any]] = []
        self.event_log = event_log

    def chat_postMessage(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(kwargs)
        if self.event_log is not None:
            self.event_log.append("slack_post")
        if self.fail_count > 0:
            self.fail_count -= 1
            raise RuntimeError("xoxb-secret-response")
        return {"ok": "true", "ts": "1720580999.000001"}


class _FakeWorkflow:
    def __init__(self, store: HpaChangeJobStore) -> None:
        self.store = store
        self.poll_calls: list[str] = []
        self.implementation_dispatches: list[str] = []
        self.event_log: list[str] | None = None

    def dispatch_implementation(self, task_id: str):
        self.implementation_dispatches.append(task_id)
        if self.event_log is not None:
            self.event_log.append("implementation_dispatch")
        self.store.claim_implementation_dispatch(task_id)
        return self.store.mark_dispatched(task_id)

    def poll_job(self, task_id: str) -> HpaChangePollResult:
        self.poll_calls.append(task_id)
        job = self.store.get_job(task_id)
        if (
            job.status is HpaChangeStatus.RESULT_READY
            and str(job.result.get("status") or "") == "no_change_needed"
        ):
            # 실제 workflow의 저장 artifact 재적용 경로를 reporter 순서 테스트에서도 재현한다.
            job = self.store.mark_no_change_needed(task_id, result=job.result)
        state = {
            HpaChangeStatus.NEEDS_CLARIFICATION: HpaChangePollState.NEEDS_CLARIFICATION,
            HpaChangeStatus.PR_CREATED: HpaChangePollState.PR_OPENED,
            HpaChangeStatus.NO_CHANGE_NEEDED: HpaChangePollState.NO_CHANGE_NEEDED,
            HpaChangeStatus.FAILED: HpaChangePollState.FAILED,
            HpaChangeStatus.CANCELED: HpaChangePollState.FAILED,
            HpaChangeStatus.RUNNING: HpaChangePollState.RUNNING,
            HpaChangeStatus.WORKFLOW_SUCCEEDED: HpaChangePollState.RUNNING,
            HpaChangeStatus.RESULT_READY: HpaChangePollState.RUNNING,
            HpaChangeStatus.REVIEW_READY: HpaChangePollState.REVIEW_READY,
            HpaChangeStatus.REVIEW_POSTED: HpaChangePollState.QUEUED,
        }.get(job.status, HpaChangePollState.QUEUED)
        return HpaChangePollResult(
            task_id=job.task_id,
            state=state,
            job=job,
            run_url=job.workflow_run_url,
            result=job.result,
            message=job.error_message or job.status_message,
            pr_urls=job.pr_urls,
        )


def _workflow_run(task_id: str) -> GitHubWorkflowRun:
    return GitHubWorkflowRun(
        run_id=501,
        status="in_progress",
        conclusion="",
        html_url=f"https://github.com/mmtalk-app/coordinator/actions/runs/501?task={task_id}",
        display_title=f"Boxer HPA - {task_id}",
        run_attempt=1,
        created_at=None,
        updated_at=None,
    )


def _artifact(task_id: str) -> GitHubArtifactArchive:
    content = b"{}"
    return GitHubArtifactArchive(
        artifact_id=701,
        workflow_run_id=501,
        name=f"boxer-hpa-result-{task_id}",
        size_in_bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        content=content,
    )


class HpaChangeReporterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = HpaChangeJobStore(":memory:")
        registration = self.store.register_job(
            workspace_id="TWORK",
            event_ts="1720580400.000100",
            channel_id="CHPA",
            thread_ts="1720580000.000001",
            requested_by="UJUSTIN",
            request_text=(
                "ORIGINAL REQUEST github_pat_abcdefghijklmnopqrstuvwxyz123456\n"
                "scan-precrop.ts와 hand-qa.ts를 그대로 추가하고 generateAdvanced()를 재사용\n"
                "사전 크롭과 손 품질 검사를 추가\n"
                "Basic/Bonus 발송 버튼 분리\n"
                "Bonus 프롬프트와 생성 설정 변경\n"
                "기존 API 키 재사용\n"
                "첨부 파일을 그대로 복사하면 된다\n"
                "손 QA 재시도 실패\n"
                "여러 줄 요청\n제목\n"
                "CR Web의 Vercel 설정을 그대로 옮기면 돼"
            ),
            thread_url="https://lifexio.slack.com/archives/CHPA/p1720580000000001",
            attachments=(
                {
                    "name": "attached.ts",
                    "content": "ExternalConfigService와 internal_prompt를 사용",
                },
            ),
        )
        self.task_id = registration.job.task_id
        self.workflow = _FakeWorkflow(self.store)
        self.runtime = HpaChangeRuntime(
            enabled=True,
            routes_config=HpaChangeRoutesConfig(enabled=True),
            poll_interval_sec=1,
            run_timeout_sec=5_400,
            store=self.store,
            workflow=self.workflow,  # type: ignore[arg-type]
        )

    def tearDown(self) -> None:
        self.store.close()

    @staticmethod
    def _minimal_review_result() -> dict[str, Any]:
        return {
            "review": {
                "requesterView": {
                    "summaryCode": "adaptation_available",
                    "wrongAssumptions": [],
                    "whyNotDirectCodes": ["different_product_structure"],
                    "requestItems": [
                        {
                            "itemId": "REQ-01",
                            "request": "사전 크롭과 손 품질 검사를 추가",
                            "handling": "adapted",
                            "reasonCode": "web_specific_code",
                            "applicationCode": "implement_hpa_equivalent",
                        }
                    ],
                }
            },
            "qualityGates": {"initialRequestCoveragePassed": True},
        }

    def _mark_running(self) -> None:
        self.store.begin_dispatch(self.task_id)
        self.store.mark_dispatched(self.task_id)
        self.store.mark_running(self.task_id, _workflow_run(self.task_id))

    def _mark_result_ready(self, result: dict[str, Any]) -> None:
        self._mark_running()
        run = _workflow_run(self.task_id)
        self.store.mark_workflow_succeeded(self.task_id, run)
        self.store.mark_result_ready(self.task_id, _artifact(self.task_id), result=result)

    def _mark_review_ready(self, result: dict[str, Any]) -> None:
        self._mark_running()
        run = _workflow_run(self.task_id)
        self.store.mark_workflow_succeeded(self.task_id, run)
        self.store.mark_review_ready(self.task_id, _artifact(self.task_id), result=result)

    def test_running_status_is_posted_to_same_thread_once(self) -> None:
        self._mark_running()
        client = _FakeSlackClient()

        first_count = run_hpa_change_reporter_once(self.runtime, client)
        second_count = run_hpa_change_reporter_once(self.runtime, client)

        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["channel"], "CHPA")
        self.assertEqual(client.calls[0]["thread_ts"], "1720580000.000001")
        # 진행 상태도 원 요청자에게 직접 알림이 가야 한다.
        self.assertTrue(client.calls[0]["text"].startswith("<@UJUSTIN> "))
        self.assertEqual(
            self.store.get_job(self.task_id).notified_status,
            HpaChangePollState.RUNNING.value,
        )

    def test_failed_slack_post_is_retried_without_marking_notified(self) -> None:
        self._mark_running()
        client = _FakeSlackClient(fail_count=1)
        silent_logger = logging.getLogger(f"{__name__}.retry")
        silent_logger.setLevel(logging.CRITICAL)

        self.assertEqual(
            run_hpa_change_reporter_once(
                self.runtime,
                client,
                logger=silent_logger,
            ),
            0,
        )
        self.assertEqual(self.store.get_job(self.task_id).notified_status, "")
        self.assertEqual(
            run_hpa_change_reporter_once(
                self.runtime,
                client,
                logger=silent_logger,
            ),
            1,
        )
        self.assertEqual(len(client.calls), 2)
        first_id = client.calls[0]["client_msg_id"]
        self.assertEqual(client.calls[1]["client_msg_id"], first_id)
        self.assertEqual(str(uuid.UUID(first_id)), first_id)

    def test_new_execution_uses_new_slack_idempotency_key(self) -> None:
        self._mark_running()
        client = _FakeSlackClient()
        run_hpa_change_reporter_once(self.runtime, client)
        first_running_id = client.calls[-1]["client_msg_id"]

        self.store.mark_failed(self.task_id, "첫 실행 실패")
        run_hpa_change_reporter_once(self.runtime, client)
        self.store.begin_dispatch(self.task_id)
        self.store.mark_dispatched(self.task_id)
        self.store.mark_running(self.task_id, _workflow_run(self.task_id))
        run_hpa_change_reporter_once(self.runtime, client)
        second_running_id = client.calls[-1]["client_msg_id"]

        self.assertNotEqual(second_running_id, first_running_id)
        self.assertEqual(str(uuid.UUID(second_running_id)), second_running_id)

    def test_review_is_posted_before_implementation_dispatch(self) -> None:
        result = {
            "review": {
                "requesterView": {
                    "summaryCode": "mixed",
                    "wrongAssumptions": [
                        {
                            "assumption": "첨부 파일을 그대로 복사하면 된다",
                            "explanationCode": "copy_not_portable",
                        }
                    ],
                    "whyNotDirectCodes": ["different_product_structure"],
                    "requestItems": [
                        {
                            "itemId": "REQ-01",
                            "request": "사전 크롭과 손 품질 검사를 추가",
                            "handling": "adapted",
                            "reasonCode": "web_specific_code",
                            "applicationCode": "implement_hpa_equivalent",
                        },
                        {
                            "itemId": "REQ-02",
                            "request": "Basic/Bonus 발송 버튼 분리",
                            "handling": "direct",
                            "reasonCode": "directly_compatible",
                            "applicationCode": "add_end_to_end_capability",
                        },
                    ],
                },
                "corrections": [
                    {
                        "claim": "첨부 파일을 그대로 복사하면 된다",
                        "correction": "HPA는 NestJS 서비스와 주입형 설정을 사용한다",
                        "evidence": "src/app/crystal-reveal/crystal-reveal.service.ts:1",
                    }
                ],
                "requesterGuidance": "CR Web과 HPA의 스택과 배포 구조가 달라 그대로 사용할 수 없어",
                "hpaDecision": "HPA 생성 서비스 경계에 맞춘 유틸로 변환한다",
                "hpaAdaptations": ["ExternalConfigService의 API 키 주입을 재사용한다"],
            },
            "qualityGates": {"initialRequestCoveragePassed": True},
        }
        self._mark_review_ready(result)
        event_log: list[str] = []
        self.workflow.event_log = event_log
        client = _FakeSlackClient(event_log=event_log)

        sent = run_hpa_change_reporter_once(self.runtime, client)

        self.assertEqual(sent, 1)
        self.assertEqual(len(client.calls), 1)
        message = client.calls[0]["text"]
        self.assertIn("*잘못된 전제*", message)
        self.assertIn("설명: 첨부 코드는 CR Web 환경에 맞춰 작성된 구현 예시야", message)
        self.assertNotIn("*HPA 실제 구조*", message)
        self.assertIn("*CR Web 코드를 그대로 못 쓰는 이유*", message)
        self.assertIn("*HPA에서 사용할 변환 구현안*", message)
        self.assertIn("*1. 사전 크롭과 손 품질 검사를 추가*", message)
        self.assertIn("*2. Basic/Bonus 발송 버튼 분리*", message)
        self.assertNotIn("src/app/crystal-reveal", message)
        self.assertEqual(self.workflow.implementation_dispatches, [self.task_id])
        self.assertEqual(event_log, ["slack_post", "implementation_dispatch"])
        job = self.store.get_job(self.task_id)
        self.assertEqual(job.status, HpaChangeStatus.DISPATCHED)
        self.assertEqual(job.notified_status, HpaChangePollState.REVIEW_READY.value)

    def test_review_post_failure_does_not_start_implementation(self) -> None:
        self._mark_review_ready(self._minimal_review_result())
        client = _FakeSlackClient(fail_count=1)
        silent_logger = logging.getLogger(f"{__name__}.review-retry")
        silent_logger.setLevel(logging.CRITICAL)

        sent = run_hpa_change_reporter_once(
            self.runtime,
            client,
            logger=silent_logger,
        )

        self.assertEqual(sent, 0)
        self.assertEqual(self.workflow.implementation_dispatches, [])
        self.assertEqual(self.store.get_job(self.task_id).status, HpaChangeStatus.REVIEW_READY)

    def test_invalid_review_contract_fails_without_starting_implementation(self) -> None:
        result = self._minimal_review_result()
        result["review"]["requesterView"]["requestItems"] = []
        self._mark_review_ready(result)
        client = _FakeSlackClient()

        sent = run_hpa_change_reporter_once(self.runtime, client)

        self.assertEqual(sent, 1)
        self.assertEqual(self.workflow.implementation_dispatches, [])
        self.assertEqual(self.store.get_job(self.task_id).status, HpaChangeStatus.FAILED)
        self.assertIn("요청 범위를 안전하게 확인하지 못해", client.calls[0]["text"])

    def test_review_contract_failure_retry_reuses_same_slack_id(self) -> None:
        result = self._minimal_review_result()
        result["review"]["requesterView"]["requestItems"] = []
        self._mark_review_ready(result)
        client = _FakeSlackClient(fail_count=1)
        silent_logger = logging.getLogger(f"{__name__}.contract-retry")
        silent_logger.setLevel(logging.CRITICAL)

        first = run_hpa_change_reporter_once(
            self.runtime,
            client,
            logger=silent_logger,
        )
        second = run_hpa_change_reporter_once(
            self.runtime,
            client,
            logger=silent_logger,
        )

        self.assertEqual(first, 0)
        self.assertEqual(second, 1)
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(
            client.calls[0]["client_msg_id"],
            client.calls[1]["client_msg_id"],
        )

    def test_completed_review_is_posted_even_when_original_phase_age_exceeds_timeout(self) -> None:
        self._mark_review_ready(self._minimal_review_result())
        created_at = self.store.get_job(self.task_id).created_at
        self.runtime.run_timeout_sec = 1
        client = _FakeSlackClient()

        sent = run_hpa_change_reporter_once(
            self.runtime,
            client,
            now=created_at + timedelta(seconds=120),
        )

        self.assertEqual(sent, 1)
        self.assertEqual(self.workflow.implementation_dispatches, [self.task_id])

    def test_review_corrections_and_blocking_questions_are_short_and_redacted(self) -> None:
        result = {
            "review": {
                "requesterView": {
                    "summaryCode": "adaptation_available",
                    "summary": "HPA는 Fargate에서 실행되고 Aurora 값을 읽어",
                    "wrongAssumptions": [
                        {
                            "assumption": "CR Web의 Vercel 설정을 그대로 옮기면 돼",
                            "explanationCode": "configuration_not_shared",
                            "explanation": "HPA는 데이터베이스의 활성 프롬프트를 우선해서 읽어.",
                        }
                    ],
                    "whyNotDirectCodes": ["different_operating_environment"],
                    "whyNotDirect": [
                        "src/app/crystal-reveal/private.service.ts:99를 그대로 사용하면 돼",
                        "https://files.slack.com/private/HPA-code를 참고하면 돼",
                        "apps/private/module과 Redis·MySQL 연결을 재사용하면 돼",
                        "crystal_reveal_seq와 internal_prompt를 그대로 사용하면 돼",
                        "crystal-reveal/prompts 경로를 복사하면 돼",
                    ],
                    "requestItems": [
                        {
                            "itemId": "REQ-01",
                            "request": "scan-precrop.ts와 hand-qa.ts를 그대로 추가",
                            "handling": "adapted",
                            "reasonCode": "web_specific_code",
                            "applicationCode": "implement_hpa_equivalent",
                            "reason": "ExternalConfigService와 GOOGLE_API_KEY 방식이 달라",
                            "appliedAs": "generateAdvanced()를 src/app 내부에서 다시 호출해",
                        },
                        {
                            "itemId": "REQ-02",
                            "request": "ExternalConfigService 변경",
                            "handling": "adapted",
                            "reasonCode": "cross_product_difference",
                            "applicationCode": "update_existing_behavior",
                        }
                    ],
                },
                "corrections": [
                    {
                        "claim": "CR Web의 Vercel 설정을 그대로 옮기면 돼",
                        "correction": (
                            "HPA Server는 Vercel이 아닌 NestJS/PM2 구조라 실제 생성 메서드와 "
                            "서버 의존성에 맞춰 변환해야 해. api_key=github_pat_abcdefghijklmnopqrstuvwxyz123456"
                        ),
                        "evidence": "server/package.json:1",
                    }
                ],
                "hpaDecision": "HPA에서는 CR Web 파일을 그대로 복사하지 않고 HPA 생성 경로에 맞춰 유틸과 의존성을 재구성해.",
                "requesterGuidance": "첨부 코드는 CR Web 기준이라 그대로 사용할 수 없어. HPA의 NestJS와 ECS 구조에 맞춰 변환 적용해.",
                "hpaAdaptations": [
                    "sharp를 HPA Server package.json과 lockfile에 추가하고 서버 빌드에서 확인해",
                ],
                "blocking_questions": [
                    {
                        "questionId": "Q-01",
                        "questionCode": "delivery_scope",
                        "subject": "Basic/Bonus 발송 버튼 분리",
                        "relatedItemId": "REQ-02",
                        "question": "<@UOTHER> 내부 API를 사용할까?",
                    },
                    {
                        "questionId": "Q-02",
                        "questionCode": "failure_behavior",
                        "subject": "손 QA 재시도 실패",
                        "relatedItemId": "REQ-01",
                    },
                ],
            },
            "request_echo": "ORIGINAL REQUEST",
        }
        self._mark_result_ready(result)
        self.store.mark_needs_clarification(
            self.task_id,
            "원문을 그대로 노출하면 안 돼",
            result=result,
        )
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(self.runtime, client)

        review_message = client.calls[0]["text"]
        question_message = client.calls[1]["text"]
        self.assertIn("*CR Web 코드를 그대로 못 쓰는 이유*", review_message)
        self.assertIn("\n\n*HPA에서 사용할 변환 구현안*", review_message)
        self.assertIn("\n\n*질문 1*", question_message)
        self.assertIn("• 전제: CR Web의 Vercel 설정을 그대로 옮기면 돼", review_message)
        self.assertIn("같은 값을 복사하는 것만으로는 동일한 동작을 보장할 수 없어", review_message)
        self.assertNotIn("*HPA 실제 구조*", review_message)
        self.assertNotIn("server/package.json", review_message)
        self.assertNotIn("src/app", review_message)
        self.assertNotIn("ExternalConfigService", review_message)
        self.assertNotIn("GOOGLE_API_KEY", review_message)
        self.assertNotIn("generateAdvanced", review_message)
        self.assertNotIn("files.slack.com", review_message)
        self.assertNotIn("https://", review_message)
        self.assertNotIn("apps/private", review_message)
        self.assertNotIn("Redis", review_message)
        self.assertNotIn("MySQL", review_message)
        self.assertNotIn("crystal_reveal_seq", review_message)
        self.assertNotIn("internal_prompt", review_message)
        self.assertNotIn("crystal-reveal/prompts", review_message)
        self.assertNotIn("질문 1:", review_message)
        self.assertIn("*질문 1*", question_message)
        self.assertIn("*질문 2*", question_message)
        self.assertNotIn("*질문 3*", question_message)
        self.assertNotIn("UOTHER", question_message)
        self.assertIn("결정 대상: Basic/Bonus 발송 버튼 분리", question_message)
        self.assertNotIn("github_pat_", review_message + question_message)
        self.assertNotIn("ORIGINAL REQUEST", review_message + question_message)
        self.assertNotIn("원문을 그대로", review_message + question_message)

    def test_pr_notification_only_contains_valid_pr_urls_and_requires_hyun_review(self) -> None:
        result = {
            "review": {
                "requesterView": {
                    "requestItems": [
                        {
                            "itemId": "REQ-01",
                            "request": "Bonus 프롬프트와 생성 설정 변경",
                            "handling": "adapted",
                            "reasonCode": "web_specific_code",
                            "applicationCode": "implement_hpa_equivalent",
                        },
                        {
                            "itemId": "REQ-02",
                            "request": "기존 API 키 재사용",
                            "handling": "not_needed",
                            "reasonCode": "existing_hpa_capability",
                            "applicationCode": "reuse_existing_capability",
                        },
                    ]
                },
            },
            "implementation": {
                "appliedResults": [
                    {
                        "itemId": "REQ-01",
                        "request": "Bonus 프롬프트와 생성 설정 변경",
                        "status": "applied",
                        "reasonCode": "web_specific_code",
                        "resultCode": "implemented_hpa_equivalent",
                    },
                    {
                        "itemId": "REQ-02",
                        "request": "기존 API 키 재사용",
                        "status": "already_satisfied",
                        "reasonCode": "existing_hpa_capability",
                        "resultCode": "existing_capability_reused",
                    },
                ]
            },
            "qualityGates": {
                "verificationPassed": True,
                "independentReviewPassed": True,
                "requestCoveragePassed": True,
                "initialRequestCoveragePassed": True,
            },
        }
        self._mark_result_ready(result)
        self.store.mark_pr_created(
            self.task_id,
            ("https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/123",),
            result=result,
        )
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(self.runtime, client)

        message = client.calls[0]["text"]
        self.assertIn("*요청별 반영 결과*", message)
        self.assertIn("*1. Bonus 프롬프트와 생성 설정 변경*", message)
        self.assertIn("• 처리: 반영 완료", message)
        self.assertIn("• 처리: 기존 기능으로 충족", message)
        self.assertIn("mmb-hospital-admin-server/pull/123", message)
        self.assertNotIn("src/internal", message)
        self.assertNotIn("HPA 기준 정정", message)
        self.assertIn("자동 빌드·테스트와 독립 리뷰를 통과했어", message)
        self.assertIn("현 승인 후 머지·배포", message)

    def test_pr_notification_rejects_incomplete_applied_results(self) -> None:
        result = {
            "review": {
                "requesterView": {
                    "requestItems": [
                        {"itemId": "REQ-01", "request": "Bonus 프롬프트와 생성 설정 변경"},
                        {"itemId": "REQ-02", "request": "기존 API 키 재사용"},
                    ]
                }
            },
            "implementation": {
                "appliedResults": [
                    {
                        "itemId": "REQ-01",
                        "request": "Bonus 프롬프트와 생성 설정 변경",
                        "status": "applied",
                        "reasonCode": "web_specific_code",
                        "resultCode": "implemented_hpa_equivalent",
                    }
                ]
            },
            "qualityGates": {
                "verificationPassed": True,
                "independentReviewPassed": True,
                "requestCoveragePassed": True,
                "initialRequestCoveragePassed": True,
            },
        }
        self._mark_result_ready(result)
        self.store.mark_pr_created(
            self.task_id,
            ("https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/123",),
            result=result,
        )
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(self.runtime, client)

        message = client.calls[0]["text"]
        self.assertIn("공개 가능한 항목별 요약이 없어 PR에서 확인이 필요해", message)
        self.assertNotIn("*1. Bonus 프롬프트와 생성 설정 변경*", message)
        self.assertNotIn("자동 빌드·테스트와 독립 리뷰를 통과했어", message)

    def test_no_change_notification_is_itemized_once_without_pr_review_step(self) -> None:
        result = {
            "review": {
                "requesterView": {
                    "requestItems": [
                        {
                            "itemId": "REQ-01",
                            "request": "Bonus 프롬프트와 생성 설정 변경",
                            "handling": "not_needed",
                            "reasonCode": "existing_hpa_capability",
                            "applicationCode": "no_change_needed",
                        },
                        {
                            "itemId": "REQ-02",
                            "request": "기존 API 키 재사용",
                            "handling": "not_needed",
                            "reasonCode": "not_applicable",
                            "applicationCode": "no_change_needed",
                        },
                    ]
                }
            },
            "implementation": {
                "appliedResults": [
                    {
                        "itemId": "REQ-01",
                        "request": "Bonus 프롬프트와 생성 설정 변경",
                        "status": "already_satisfied",
                        "reasonCode": "existing_hpa_capability",
                        "resultCode": "existing_capability_reused",
                    },
                    {
                        "itemId": "REQ-02",
                        "request": "기존 API 키 재사용",
                        "status": "not_applicable",
                        "reasonCode": "not_applicable",
                        "resultCode": "not_in_scope",
                    },
                ]
            },
            "qualityGates": {
                "verificationPassed": True,
                "independentReviewPassed": True,
                "requestCoveragePassed": True,
                "initialRequestCoveragePassed": True,
            },
        }
        self._mark_result_ready(result)
        self.store.mark_no_change_needed(self.task_id, result=result)
        client = _FakeSlackClient()

        first_count = run_hpa_change_reporter_once(self.runtime, client)
        second_count = run_hpa_change_reporter_once(self.runtime, client)

        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        self.assertEqual(len(client.calls), 1)
        message = client.calls[0]["text"]
        self.assertIn("*요청별 확인 결과*", message)
        self.assertIn("• 처리: 기존 기능으로 충족", message)
        self.assertIn("• 처리: 이번 HPA 변경 대상 아님", message)
        self.assertIn("코드 변경 불필요 · PR 없음", message)
        self.assertIn("코드 변경과 PR을 만들지 않았어", message)
        self.assertNotIn("*PR*", message)
        self.assertNotIn("현 승인 후", message)
        self.assertEqual(
            self.store.get_job(self.task_id).notified_status,
            HpaChangePollState.NO_CHANGE_NEEDED.value,
        )

    def test_multiline_request_label_keeps_readable_title(self) -> None:
        multiline_request = "여러 줄 요청\n제목"
        result = {
            "review": {
                "requesterView": {
                    "requestItems": [
                        {
                            "itemId": "REQ-01",
                            "request": multiline_request,
                            "handling": "not_needed",
                            "reasonCode": "existing_hpa_capability",
                            "applicationCode": "no_change_needed",
                        }
                    ]
                }
            },
            "implementation": {
                "appliedResults": [
                    {
                        "itemId": "REQ-01",
                        "request": multiline_request,
                        "status": "already_satisfied",
                        "reasonCode": "existing_hpa_capability",
                        "resultCode": "no_change_needed",
                    }
                ]
            },
            "qualityGates": {
                "verificationPassed": True,
                "independentReviewPassed": True,
                "requestCoveragePassed": True,
                "initialRequestCoveragePassed": True,
            },
        }
        self._mark_result_ready(result)
        self.store.mark_no_change_needed(self.task_id, result=result)
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(self.runtime, client)

        message = client.calls[0]["text"]
        self.assertIn("*1. 여러 줄 요청 제목*", message)
        self.assertNotIn("*1. 요청한 변경 항목*", message)

    def test_pr_notification_rejects_contradictory_result_codes(self) -> None:
        result = {
            "review": {
                "requesterView": {
                    "requestItems": [
                        {"itemId": "REQ-01", "request": "Bonus 프롬프트와 생성 설정 변경"}
                    ]
                }
            },
            "implementation": {
                "appliedResults": [
                    {
                        "itemId": "REQ-01",
                        "request": "Bonus 프롬프트와 생성 설정 변경",
                        "status": "not_applicable",
                        "reasonCode": "web_specific_code",
                        "resultCode": "implemented_hpa_equivalent",
                    }
                ]
            },
            "qualityGates": {
                "verificationPassed": True,
                "independentReviewPassed": True,
                "requestCoveragePassed": True,
                "initialRequestCoveragePassed": True,
            },
        }
        self._mark_result_ready(result)
        self.store.mark_pr_created(
            self.task_id,
            ("https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/123",),
            result=result,
        )
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(self.runtime, client)

        message = client.calls[0]["text"]
        self.assertIn("공개 가능한 항목별 요약이 없어 PR에서 확인이 필요해", message)
        self.assertNotIn("HPA 제품용 구현으로 반영했어", message)
        self.assertNotIn("자동 빌드·테스트와 독립 리뷰를 통과했어", message)

    def test_legacy_clarification_uses_neutral_summary(self) -> None:
        result = {"review": {"blocking_questions": ["발송 정책을 선택해줘"]}}
        self._mark_result_ready(result)
        self.store.mark_needs_clarification(
            self.task_id,
            "제품 결정 필요",
            result=result,
        )
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(self.runtime, client)

        review_message = client.calls[0]["text"]
        self.assertIn("요청 담당자의 결정이 필요한 항목이 있어", review_message)
        self.assertNotIn("필요한 항목은 HPA 방식으로 바꿔 적용할 수 있어", review_message)

    def test_pr_notification_does_not_guess_results_when_summary_is_missing(self) -> None:
        result = {
            "review": {
                "hpaAdaptations": [
                    "src/app/private.ts의 generateAdvanced()를 수정했어",
                ]
            }
        }
        self._mark_result_ready(result)
        self.store.mark_pr_created(
            self.task_id,
            ("https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/123",),
            result=result,
        )
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(self.runtime, client)

        message = client.calls[0]["text"]
        self.assertIn("공개 가능한 항목별 요약이 없어 PR에서 확인이 필요해", message)
        self.assertIn("자동 검증 통과 정보가 없어 PR에서 확인이 필요해", message)
        self.assertNotIn("자동 빌드·테스트와 독립 리뷰를 통과했어", message)
        self.assertNotIn("src/app/private", message)
        self.assertNotIn("generateAdvanced", message)

    def test_timeout_becomes_generic_failure_without_exposing_error_or_request(self) -> None:
        created_at = self.store.get_job(self.task_id).created_at
        self.runtime.run_timeout_sec = 1
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(
            self.runtime,
            client,
            now=created_at + timedelta(seconds=2),
        )

        job = self.store.get_job(self.task_id)
        message = client.calls[0]["text"]
        self.assertEqual(job.status, HpaChangeStatus.FAILED)
        self.assertEqual(job.notified_status, HpaChangePollState.FAILED.value)
        self.assertNotIn(job.error_message, message)
        self.assertNotIn("ORIGINAL REQUEST", message)

    def test_saved_terminal_result_recovers_before_timeout_failure(self) -> None:
        result = {
            "status": "no_change_needed",
            "review": {
                "requesterView": {
                    "requestItems": [
                        {
                            "itemId": "REQ-01",
                            "request": "기존 API 키 재사용",
                            "handling": "not_needed",
                            "reasonCode": "existing_hpa_capability",
                            "applicationCode": "no_change_needed",
                        }
                    ]
                }
            },
            "implementation": {
                "appliedResults": [
                    {
                        "itemId": "REQ-01",
                        "request": "기존 API 키 재사용",
                        "status": "already_satisfied",
                        "reasonCode": "existing_hpa_capability",
                        "resultCode": "existing_capability_reused",
                    }
                ]
            },
            "qualityGates": {
                "verificationPassed": True,
                "independentReviewPassed": True,
                "requestCoveragePassed": True,
                "initialRequestCoveragePassed": True,
            },
        }
        self._mark_result_ready(result)
        created_at = self.store.get_job(self.task_id).created_at
        self.runtime.run_timeout_sec = 1
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(
            self.runtime,
            client,
            now=created_at + timedelta(seconds=2),
        )

        job = self.store.get_job(self.task_id)
        self.assertEqual(job.status, HpaChangeStatus.NO_CHANGE_NEEDED)
        self.assertEqual(job.notified_status, HpaChangePollState.NO_CHANGE_NEEDED.value)
        self.assertIn("코드 변경 불필요 · PR 없음", client.calls[0]["text"])


if __name__ == "__main__":
    unittest.main()
