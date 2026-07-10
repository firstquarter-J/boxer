from __future__ import annotations

import hashlib
import logging
import tempfile
import unittest
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
)
from boxer_company_adapter_slack.hpa_change_runtime import create_hpa_change_runtime
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
        "HPA_CHANGE_REQUEST_ALLOWED_USER_IDS": {"UJUSTIN"},
        "HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS": {"CHPA"},
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


def _route_request() -> HpaChangeRequest:
    return HpaChangeRequest(
        request_key="slack:TWORK:CHPA:1720580400.000100",
        workspace_id="TWORK",
        channel_id="CHPA",
        thread_ts="1720580000.000001",
        thread_url="https://lifexio.slack.com/archives/CHPA/p1720580000000001",
        event_ts="1720580400.000100",
        requester_user_id="UJUSTIN",
        question="HPA CR 반영 요청",
        thread_text="[1720580000.000001] UJUSTIN\nBonus 프롬프트 변경을 검토해줘",
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
        initiator_user_id="UHYUN",
        source_channel_id="CSOURCE",
        source_message_ts="1720580000.000001",
        selection_mode="linked_message",
        response_thread_url=(
            "https://lifexio.slack.com/archives/CHPA/p1720580000000001"
        ),
    )


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

            settings.HPA_CHANGE_REQUEST_ALLOWED_USER_IDS = {"UJUSTIN"}
            with self.assertRaisesRegex(ValueError, "GitHub App 또는 static token"):
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
            self.assertEqual(worker_request["requester_slack_user_id"], "UJUSTIN")
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
            self.assertEqual(job.channel_id, "CHPA")
            self.assertEqual(job.thread_ts, "1720580000.000001")
            self.assertEqual(job.requested_by, "UJUSTIN")
            self.assertEqual(job.metadata["initiator_user_id"], "UHYUN")
            self.assertEqual(job.metadata["source_channel_id"], "CSOURCE")
            self.assertEqual(job.metadata["selection_mode"], "linked_message")
            self.assertIn("/archives/CHPA/", job.metadata["response_thread_url"])
        self.assertEqual(
            runtime.workflow.github.config.workflow_run_name_prefix,
            "Boxer HPA Review",
        )
        self.assertEqual(
            runtime.workflow.github.config.implementation_workflow_run_name_prefix,
            "Boxer HPA Implementation",
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
        check_ping.assert_not_called()
        attach_reporter.assert_called_once()
        self.assertIs(attach_reporter.call_args.args[1], fake_runtime)


if __name__ == "__main__":
    unittest.main()
