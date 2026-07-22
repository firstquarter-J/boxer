from __future__ import annotations

import hashlib
import logging
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeAttachment,
    HpaChangeRequest,
    HpaChangeRoutesConfig,
    HpaChangeSubmissionStatus,
    HpaChangeThreadLookupState,
)
from boxer_company_adapter_slack.hpa_change_runtime import (
    HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS,
    HPA_CHANGE_POLICY_ALLOWED_USER_IDS,
    create_hpa_change_runtime,
)
from boxer_company.hpa_change_workflow import (
    GitHubArtifactArchive,
    GitHubWorkflowRun,
    HpaChangePollState,
)
from boxer_company_adapter_slack import company


class _FakeResponse:
    status_code = 204
    text = ""


class _FakeSession:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return _FakeResponse()


def _settings(db_path: str, **overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "HPA_CHANGE_REQUEST_ENABLED": True,
        "HPA_CHANGE_REQUEST_ALLOWED_USER_IDS": set(
            HPA_CHANGE_POLICY_ALLOWED_USER_IDS
        ),
        "HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS": set(
            HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS
        ),
        "HPA_CHANGE_GITHUB_COORDINATOR_REPOSITORY": "mmtalk-app/mmb-hospital-admin-server",
        "HPA_CHANGE_GITHUB_WORKFLOW_FILE": "boxer-hpa-change.yml",
        "HPA_CHANGE_GITHUB_API_URL": "https://api.github.com",
        "HPA_CHANGE_GITHUB_TOKEN": "github_pat_static_test_token_1234567890",
        "HPA_CHANGE_GITHUB_APP_ID": "",
        "HPA_CHANGE_GITHUB_APP_INSTALLATION_ID": "",
        "HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH": "",
        "HPA_CHANGE_JOB_DB_PATH": db_path,
        "HPA_CHANGE_POLL_INTERVAL_SEC": 20,
        "HPA_CHANGE_RUN_TIMEOUT_SEC": 5_400,
        "HPA_CHANGE_MAX_THREAD_CHARS": 30_000,
        "HPA_CHANGE_MAX_FILES": 5,
        "HPA_CHANGE_MAX_FILE_BYTES": 131_072,
        "HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES": 524_288,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _route_request(
    *, requester_user_id: str = "U07A5FM5XPD"
) -> HpaChangeRequest:
    return HpaChangeRequest(
        request_key="slack:TWORK:C02C08K7YEN:1720580400.000100",
        workspace_id="TWORK",
        channel_id="C02C08K7YEN",
        thread_ts="1720580000.000001",
        thread_url=(
            "https://lifexio.slack.com/archives/C068FVD5V7Y/"
            "p1720580000000001"
        ),
        event_ts="1720580400.000100",
        requester_user_id=requester_user_id,
        question="HPA CR 반영 요청",
        thread_text=(
            f"[1720580000.000001] {requester_user_id}\n"
            "Bonus 프롬프트 변경을 검토해줘"
        ),
        thread_message_count=1,
        attachments=(
            HpaChangeAttachment(
                file_id="FPRIVATE",
                name="handoff.txt",
                mimetype="text/plain",
                size_bytes=12,
                content="prompt body",
                message_ts="1720580000.000001",
            ),
        ),
        initiator_user_id="U0629HDSJHG",
        source_channel_id="C068FVD5V7Y",
        source_message_ts="1720580000.000001",
        selection_mode="linked_message",
        response_thread_url=(
            "https://lifexio.slack.com/archives/C02C08K7YEN/"
            "p1720580000000001"
        ),
    )


def _mark_needs_clarification(runtime: Any, task_id: str) -> None:
    run = GitHubWorkflowRun(
        run_id=501,
        status="completed",
        conclusion="success",
        html_url="https://github.com/mmtalk-app/mmb-hospital-admin-server/actions/runs/501",
        display_title=f"Boxer HPA Review - {task_id}",
        run_attempt=1,
        created_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
        updated_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )
    runtime.store.mark_running(task_id, run)
    runtime.store.mark_workflow_succeeded(task_id, run)
    runtime.store.mark_result_ready(
        task_id,
        GitHubArtifactArchive(
            artifact_id=700,
            workflow_run_id=run.run_id,
            name=f"boxer-hpa-result-{task_id}",
            size_in_bytes=2,
            sha256=hashlib.sha256(b"{}").hexdigest(),
            content=b"{}",
        ),
    )
    runtime.store.mark_needs_clarification(
        task_id,
        "발송 범위를 결정해줘",
        result={"status": "needs_clarification"},
    )
    runtime.store.mark_notified(task_id, HpaChangePollState.NEEDS_CLARIFICATION)


class HpaChangeRuntimeTests(unittest.TestCase):
    def test_disabled_runtime_does_not_require_github_credentials(self) -> None:
        settings = SimpleNamespace(HPA_CHANGE_REQUEST_ENABLED=False)

        runtime = create_hpa_change_runtime(settings=settings)

        self.assertFalse(runtime.enabled)
        self.assertIsNone(runtime.store)
        self.assertEqual(runtime.auth_mode, "disabled")

    def test_enabled_runtime_fails_closed_without_allowlists_or_auth(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(
                str(Path(temp_dir) / "jobs.sqlite3"),
                HPA_CHANGE_REQUEST_ALLOWED_USER_IDS=set(),
                HPA_CHANGE_GITHUB_TOKEN="",
            )

            with self.assertRaisesRegex(ValueError, "allowlist"):
                create_hpa_change_runtime(settings=settings)

            settings.HPA_CHANGE_REQUEST_ALLOWED_USER_IDS = set(
                HPA_CHANGE_POLICY_ALLOWED_USER_IDS
            )
            with self.assertRaisesRegex(ValueError, "GitHub App 또는 static token"):
                create_hpa_change_runtime(settings=settings)

    def test_enabled_runtime_requires_exact_company_policy_allowlists(self) -> None:
        cases = (
            {
                "HPA_CHANGE_REQUEST_ALLOWED_USER_IDS": (
                    set(HPA_CHANGE_POLICY_ALLOWED_USER_IDS) | {"UOTHER"}
                )
            },
            {
                "HPA_CHANGE_REQUEST_ALLOWED_USER_IDS": {"U0629HDSJHG"}
            },
            {
                # 저스틴의 보조 Slack 계정이 누락된 기존 운영 설정도 거부한다.
                "HPA_CHANGE_REQUEST_ALLOWED_USER_IDS": {
                    "U0629HDSJHG",
                    "U07A5FM5XPD",
                }
            },
            {
                "HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS": (
                    set(HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS) | {"COTHER"}
                )
            },
            {
                "HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS": {"C02C08K7YEN"}
            },
        )

        for index, overrides in enumerate(cases):
            with (
                self.subTest(overrides=overrides),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                settings = _settings(
                    str(Path(temp_dir) / f"jobs-{index}.sqlite3"),
                    **overrides,
                )

                with self.assertRaisesRegex(ValueError, "회사 고정"):
                    create_hpa_change_runtime(settings=settings)

    def test_partial_github_app_config_does_not_fallback_to_static_token(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(
                str(Path(temp_dir) / "jobs.sqlite3"),
                HPA_CHANGE_GITHUB_APP_ID="1234",
            )

            with self.assertRaisesRegex(ValueError, "세 항목"):
                create_hpa_change_runtime(settings=settings)

    def test_github_app_is_preferred_and_private_key_is_validated(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            private_key_path = Path(temp_dir) / "github-app.pem"
            private_key_path.write_bytes(
                private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
            settings = _settings(
                str(Path(temp_dir) / "jobs.sqlite3"),
                HPA_CHANGE_GITHUB_APP_ID="1234",
                HPA_CHANGE_GITHUB_APP_INSTALLATION_ID="5678",
                HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH=str(private_key_path),
            )

            runtime = create_hpa_change_runtime(settings=settings)
            self.addCleanup(runtime.close)

            self.assertEqual(runtime.auth_mode, "github_app")

    def test_submit_dispatches_exact_worker_payload_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = _FakeSession()
            runtime = create_hpa_change_runtime(
                settings=_settings(str(Path(temp_dir) / "jobs.sqlite3")),
                session=session,
            )
            self.addCleanup(runtime.close)

            first = runtime.submit_request(_route_request())
            duplicate = runtime.submit_request(_route_request())

            self.assertEqual(first.status, HpaChangeSubmissionStatus.ACCEPTED)
            self.assertEqual(duplicate.status, HpaChangeSubmissionStatus.DUPLICATE)
            self.assertEqual(first.request_id, duplicate.request_id)
            self.assertEqual(len(session.calls), 1)
            call = session.calls[0]
            self.assertEqual(call["method"], "POST")
            self.assertEqual(
                call["url"],
                "https://api.github.com/repos/mmtalk-app/mmb-hospital-admin-server/dispatches",
            )
            body = call["json"]
            self.assertEqual(body["event_type"], "boxer-hpa-change")
            self.assertEqual(
                set(body["client_payload"]),
                {"task_id", "request"},
            )
            worker_request = body["client_payload"]["request"]
            self.assertEqual(
                set(worker_request),
                {"text", "requester_slack_user_id", "thread_url", "attachments"},
            )
            self.assertEqual(worker_request["text"], _route_request().thread_text)
            self.assertEqual(
                worker_request["requester_slack_user_id"],
                "U07A5FM5XPD",
            )
            self.assertEqual(
                worker_request["attachments"],
                [
                    {
                        "name": "handoff.txt",
                        "content": "prompt body",
                        "sha256": hashlib.sha256(b"prompt body").hexdigest(),
                    }
                ],
            )
            job = runtime.store.get_job(first.request_id)
            self.assertEqual(job.channel_id, "C02C08K7YEN")
            self.assertEqual(job.thread_ts, "1720580000.000001")
            self.assertEqual(job.requested_by, "U07A5FM5XPD")
            self.assertEqual(job.metadata["initiator_user_id"], "U0629HDSJHG")
            self.assertEqual(job.metadata["source_channel_id"], "C068FVD5V7Y")
            self.assertEqual(job.metadata["selection_mode"], "linked_message")
            self.assertIn(
                "/archives/C02C08K7YEN/",
                job.metadata["response_thread_url"],
            )
        self.assertEqual(
            runtime.workflow.github.config.workflow_run_name_prefix,
            "Boxer HPA Review",
        )
        self.assertEqual(
            runtime.workflow.github.config.implementation_workflow_run_name_prefix,
            "Boxer HPA Implementation",
        )

    def test_clarification_followup_creates_new_task_with_parent_context(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = _FakeSession()
            runtime = create_hpa_change_runtime(
                settings=_settings(str(Path(temp_dir) / "jobs.sqlite3")),
                session=session,
            )
            self.addCleanup(runtime.close)

            parent_result = runtime.submit_request(_route_request())
            _mark_needs_clarification(runtime, parent_result.request_id)
            pending = runtime.lookup_thread_job(
                "TWORK",
                "C02C08K7YEN",
                "1720580000.000001",
                "1720580500.000200",
            )
            self.assertEqual(
                pending.state,
                HpaChangeThreadLookupState.NEEDS_CLARIFICATION,
            )
            self.assertEqual(pending.request_id, parent_result.request_id)
            self.assertEqual(pending.event_ts, "1720580400.000100")
            self.assertEqual(
                runtime.lookup_thread_job(
                    "TOTHER",
                    "C02C08K7YEN",
                    "1720580000.000001",
                    "1720580500.000200",
                ).state,
                HpaChangeThreadLookupState.NONE,
            )
            self.assertEqual(
                runtime.lookup_thread_job(
                    "TWORK",
                    "C068FVD5V7Y",
                    "1720580000.000001",
                    "1720580500.000200",
                ).state,
                HpaChangeThreadLookupState.NONE,
            )

            followup = replace(
                _route_request(requester_user_id="U0629HDSJHG"),
                request_key="slack:TWORK:C02C08K7YEN:1720580500.000200",
                event_ts="1720580500.000200",
                question="진행해",
                thread_text=(
                    "[1720580000.000001] U07A5FM5XPD\nHPA 요청사항 검토\n\n"
                    "[1720580490.000100] U0629HDSJHG\n"
                    "질문1 답변: Basic과 Bonus 각각 독립 버튼\n"
                    "질문2 답변: 최종 검증 실패 시 해당 결과 생성 실패 처리"
                ),
                attachments=(
                    HpaChangeAttachment(
                        file_id="FANSWER",
                        name="decision.md",
                        mimetype="text/markdown",
                        size_bytes=16,
                        content="confirmed decisions",
                        message_ts="1720580490.000100",
                    ),
                ),
                initiator_user_id="U0629HDSJHG",
                source_channel_id="C02C08K7YEN",
                source_message_ts="1720580000.000001",
                selection_mode="clarification_followup",
                continuation_of_request_id=parent_result.request_id,
            )

            child_result = runtime.submit_request(followup)
            duplicate = runtime.submit_request(followup)

            self.assertEqual(child_result.status, HpaChangeSubmissionStatus.ACCEPTED)
            self.assertEqual(duplicate.status, HpaChangeSubmissionStatus.DUPLICATE)
            self.assertEqual(duplicate.request_id, child_result.request_id)
            self.assertNotEqual(child_result.request_id, parent_result.request_id)
            # 최초 요청과 후속 요청은 서로 다른 task_id로 각각 한 번만 review dispatch한다.
            self.assertEqual(len(session.calls), 2)

            child = runtime.store.get_job(child_result.request_id)
            self.assertEqual(child.requested_by, "U07A5FM5XPD")
            self.assertEqual(child.channel_id, "C02C08K7YEN")
            self.assertEqual(child.thread_ts, "1720580000.000001")
            self.assertEqual(child.thread_url, _route_request().thread_url)
            self.assertIn("Bonus 프롬프트 변경", child.request_text)
            self.assertIn("Basic과 Bonus 각각 독립 버튼", child.request_text)
            self.assertEqual(
                {item.name for item in child.attachments},
                {"handoff.txt", "decision.md"},
            )
            self.assertEqual(
                child.metadata["continuation_of_request_id"],
                parent_result.request_id,
            )
            self.assertEqual(child.metadata["source_channel_id"], "C068FVD5V7Y")
            self.assertEqual(child.metadata["selection_mode"], "linked_message")
            self.assertEqual(child.metadata["initiator_user_id"], "U0629HDSJHG")

            worker_request = session.calls[1]["json"]["client_payload"]["request"]
            self.assertEqual(worker_request["requester_slack_user_id"], "U07A5FM5XPD")
            self.assertIn("최종 검증 실패", worker_request["text"])
            self.assertEqual(
                {item["name"] for item in worker_request["attachments"]},
                {"handoff.txt", "decision.md"},
            )
            # Slack retry는 이미 등록한 자식의 실제 상태를 반환하고 grandchild를 만들지 않는다.
            retry_lookup = runtime.lookup_thread_job(
                "TWORK",
                "C02C08K7YEN",
                "1720580000.000001",
                "1720580500.000200",
            )
            self.assertEqual(retry_lookup.state, HpaChangeThreadLookupState.ACTIVE)
            self.assertEqual(retry_lookup.request_id, child_result.request_id)
            self.assertTrue(retry_lookup.current_event)
            # 다른 event도 최신 자식의 실제 active 상태를 사용한다.
            latest_lookup = runtime.lookup_thread_job(
                "TWORK",
                "C02C08K7YEN",
                "1720580000.000001",
                "1720580600.000300",
            )
            self.assertEqual(latest_lookup.state, HpaChangeThreadLookupState.ACTIVE)
            self.assertEqual(latest_lookup.request_id, child_result.request_id)

            runtime.store.mark_failed(child_result.request_id, "worker failed")
            terminal_lookup = runtime.lookup_thread_job(
                "TWORK",
                "C02C08K7YEN",
                "1720580000.000001",
                "1720580700.000400",
            )
            self.assertEqual(
                terminal_lookup.state,
                HpaChangeThreadLookupState.TERMINAL,
            )
            self.assertEqual(terminal_lookup.job_status, "failed")

            with patch.object(
                runtime.store,
                "get_job_by_event_ts",
                side_effect=RuntimeError("db unavailable"),
            ):
                error_lookup = runtime.lookup_thread_job(
                    "TWORK",
                    "C02C08K7YEN",
                    "1720580000.000001",
                    "1720580800.000500",
                )
            self.assertEqual(error_lookup.state, HpaChangeThreadLookupState.ERROR)

    def test_submit_rejects_request_outside_company_policy_before_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            session = _FakeSession()
            runtime = create_hpa_change_runtime(
                settings=_settings(str(Path(temp_dir) / "jobs.sqlite3")),
                session=session,
            )
            self.addCleanup(runtime.close)
            base_request = _route_request()
            cases = (
                replace(base_request, requester_user_id="UOTHER"),
                replace(base_request, initiator_user_id="UOTHER"),
                replace(base_request, initiator_user_id=""),
                replace(base_request, channel_id="COTHER"),
                replace(base_request, source_channel_id="COTHER"),
                replace(base_request, source_channel_id=""),
            )

            for request in cases:
                with self.subTest(request=request):
                    result = runtime.submit_request(request)
                    self.assertEqual(
                        result.status,
                        HpaChangeSubmissionStatus.REJECTED,
                    )
                    self.assertIn("정책을 충족하지 않아", result.user_message)

            self.assertEqual(session.calls, [])

    def test_submit_accepts_both_justin_slack_accounts(self) -> None:
        # 동일한 요청자가 사용하는 두 Slack 계정 모두 HPA 작업 큐에 들어가야 한다.
        for index, requester_user_id in enumerate(
            ("U07A5FM5XPD", "U096JA81T6X")
        ):
            with (
                self.subTest(requester_user_id=requester_user_id),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                session = _FakeSession()
                runtime = create_hpa_change_runtime(
                    settings=_settings(
                        str(Path(temp_dir) / f"jobs-{index}.sqlite3")
                    ),
                    session=session,
                )
                self.addCleanup(runtime.close)
                self.assertIn(
                    requester_user_id,
                    runtime.routes_config.allowed_user_ids,
                )

                result = runtime.submit_request(
                    _route_request(requester_user_id=requester_user_id)
                )

                self.assertEqual(
                    result.status,
                    HpaChangeSubmissionStatus.ACCEPTED,
                )
                self.assertEqual(len(session.calls), 1)
                worker_request = session.calls[0]["json"]["client_payload"][
                    "request"
                ]
                self.assertEqual(
                    worker_request["requester_slack_user_id"],
                    requester_user_id,
                )

    def test_invalid_private_key_path_fails_before_runtime_starts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(
                str(Path(temp_dir) / "jobs.sqlite3"),
                HPA_CHANGE_GITHUB_APP_ID="1234",
                HPA_CHANGE_GITHUB_APP_INSTALLATION_ID="5678",
                HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH=str(Path(temp_dir) / "missing.pem"),
            )

            with self.assertRaisesRegex(ValueError, "경로"):
                create_hpa_change_runtime(settings=settings)

    def test_company_app_routes_hpa_before_ping_and_attaches_reporter(self) -> None:
        captured_handlers: dict[str, Any] = {}
        fake_app = SimpleNamespace(client=object())
        fake_runtime = SimpleNamespace(
            routes_config=HpaChangeRoutesConfig(enabled=True),
            submit_request=Mock(),
            lookup_thread_job=Mock(),
        )

        def fake_create_slack_app(mention_handler: Any, message_handler: Any) -> Any:
            captured_handlers["mention"] = mention_handler
            captured_handlers["message"] = message_handler
            return fake_app

        with (
            patch.object(company, "_validate_ec2_runtime_aws_env"),
            patch.object(company, "_validate_tokens"),
            patch.object(company.s, "LLM_PROVIDER", ""),
            patch.object(
                company,
                "create_hpa_change_runtime",
                return_value=fake_runtime,
            ),
            patch.object(
                company,
                "create_slack_app",
                side_effect=fake_create_slack_app,
            ),
            patch.object(company, "attach_hpa_change_reporter") as attach_reporter,
            patch.object(company, "attach_weekly_recordings_reporter"),
            patch.object(company, "attach_device_health_monitor_reporter"),
            patch.object(
                company,
                "attach_device_notification_alert_reporter",
            ) as attach_notification_reporter,
            patch.object(company, "attach_daily_device_round_reporter"),
            patch.object(
                company,
                "_handle_hpa_change_request",
                return_value=True,
            ) as handle_hpa,
            patch.object(company, "_check_ollama_health") as check_ping,
        ):
            app = company.create_app()
            captured_handlers["mention"](
                {
                    "text": "hpa 반영 ping",
                    "question": "HPA 반영 요청 ping",
                    "user_id": "UJUSTIN",
                    "workspace_id": "TWORK",
                    "channel_id": "CHPA",
                    "current_ts": "1720580400.000100",
                    "thread_ts": "1720580000.000001",
                },
                Mock(),
                Mock(),
                logging.getLogger(f"{__name__}.company"),
            )

        self.assertIs(app, fake_app)
        handle_hpa.assert_called_once()
        self.assertIs(
            handle_hpa.call_args.args[2].lookup_thread_job,
            fake_runtime.lookup_thread_job,
        )
        check_ping.assert_not_called()
        attach_reporter.assert_called_once()
        self.assertIs(attach_reporter.call_args.args[1], fake_runtime)
        attach_notification_reporter.assert_called_once()
        self.assertIs(
            attach_notification_reporter.call_args.kwargs["auto_sms_sender"],
            company._send_device_health_monitor_auto_sms_for_item,
        )


if __name__ == "__main__":
    unittest.main()
