from __future__ import annotations

import hashlib
import logging
import unittest
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
    def __init__(self, *, fail_count: int = 0) -> None:
        self.fail_count = fail_count
        self.calls: list[dict[str, Any]] = []

    def chat_postMessage(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(kwargs)
        if self.fail_count > 0:
            self.fail_count -= 1
            raise RuntimeError("xoxb-secret-response")
        return {"ok": "true", "ts": "1720580999.000001"}


class _FakeWorkflow:
    def __init__(self, store: HpaChangeJobStore) -> None:
        self.store = store
        self.poll_calls: list[str] = []

    def poll_job(self, task_id: str) -> HpaChangePollResult:
        self.poll_calls.append(task_id)
        job = self.store.get_job(task_id)
        state = {
            HpaChangeStatus.NEEDS_CLARIFICATION: HpaChangePollState.NEEDS_CLARIFICATION,
            HpaChangeStatus.PR_CREATED: HpaChangePollState.PR_OPENED,
            HpaChangeStatus.FAILED: HpaChangePollState.FAILED,
            HpaChangeStatus.CANCELED: HpaChangePollState.FAILED,
            HpaChangeStatus.RUNNING: HpaChangePollState.RUNNING,
            HpaChangeStatus.WORKFLOW_SUCCEEDED: HpaChangePollState.RUNNING,
            HpaChangeStatus.RESULT_READY: HpaChangePollState.RUNNING,
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
            request_text="ORIGINAL REQUEST github_pat_abcdefghijklmnopqrstuvwxyz123456",
            thread_url="https://lifexio.slack.com/archives/CHPA/p1720580000000001",
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

    def _mark_running(self) -> None:
        self.store.begin_dispatch(self.task_id)
        self.store.mark_dispatched(self.task_id)
        self.store.mark_running(self.task_id, _workflow_run(self.task_id))

    def _mark_result_ready(self, result: dict[str, Any]) -> None:
        self._mark_running()
        run = _workflow_run(self.task_id)
        self.store.mark_workflow_succeeded(self.task_id, run)
        self.store.mark_result_ready(self.task_id, _artifact(self.task_id), result=result)

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

    def test_review_corrections_and_blocking_questions_are_short_and_redacted(self) -> None:
        result = {
            "review": {
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
                    {"question": "<@UOTHER> Basic과 Bonus를 병원별로 나눌까?"},
                    {"question": "재시도 QA 실패 시 Bonus를 FAILED 처리할까?"},
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
        self.assertIn("요청자 안내: 첨부 코드는 CR Web 기준이라 그대로 사용할 수 없어", review_message)
        self.assertIn("HPA 기준 정정: 요청 전제:", review_message)
        self.assertIn("HPA 최종 적용안:", review_message)
        self.assertIn("HPA 적용:", review_message)
        self.assertIn("근거: server/package.json:1", review_message)
        self.assertIn("HPA 적용 방식:", review_message)
        self.assertNotIn("질문 1:", review_message)
        self.assertIn("질문 1:", question_message)
        self.assertIn("질문 2:", question_message)
        self.assertIn("‹@UOTHER›", question_message)
        self.assertNotIn("github_pat_", review_message + question_message)
        self.assertNotIn("ORIGINAL REQUEST", review_message + question_message)
        self.assertNotIn("원문을 그대로", review_message + question_message)

    def test_pr_notification_only_contains_valid_pr_urls_and_requires_hyun_review(self) -> None:
        result = {"review": {"corrections": ["서버 timeout은 기존 설정을 재사용했어"]}}
        self._mark_result_ready(result)
        self.store.mark_pr_created(
            self.task_id,
            (
                "https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/123",
                "https://evil.example/pull/999",
            ),
            result=result,
        )
        client = _FakeSlackClient()

        run_hpa_change_reporter_once(self.runtime, client)

        message = client.calls[0]["text"]
        self.assertIn("mmb-hospital-admin-server/pull/123", message)
        self.assertNotIn("evil.example", message)
        self.assertIn("현 승인 후 머지·배포", message)

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


if __name__ == "__main__":
    unittest.main()
