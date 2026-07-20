from __future__ import annotations

import base64
import hashlib
import io
import json
import tempfile
import threading
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from boxer_company.hpa_change_workflow import (
    GitHubApiError,
    GitHubAppPermissions,
    GitHubAppTokenProvider,
    GitHubArtifactArchive,
    GitHubArtifactError,
    GitHubArtifactNotReady,
    GitHubCoordinatorClient,
    GitHubCoordinatorConfig,
    GitHubWorkflowRun,
    HpaChangeAttachment,
    HpaChangeJobStore,
    HpaChangePollState,
    HpaChangeRequest,
    HpaChangeStatus,
    HpaChangeWorkflowService,
    InvalidHpaChangeContinuation,
    InvalidHpaChangeTransition,
    StaticGitHubTokenProvider,
    generate_hpa_change_task_id,
    redact_sensitive_data,
    redact_sensitive_text,
)


_NOW = datetime(2026, 7, 10, 3, 0, 0, tzinfo=timezone.utc)


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        json_data: Any = None,
        text: str = "",
        content: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self._content = content
        self.headers = headers or {}

    def json(self) -> Any:
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data

    def iter_content(self, chunk_size: int):
        for index in range(0, len(self._content), max(1, chunk_size)):
            yield self._content[index : index + chunk_size]


class _FakeSession:
    def __init__(self, *responses: _FakeResponse) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"예상하지 않은 HTTP 호출이야: {method} {url}")
        return self.responses.pop(0)


class _TaskIdFactory:
    def __init__(self) -> None:
        self.counter = 0

    def __call__(self, event_ts: str, now: datetime) -> str:
        self.counter += 1
        return generate_hpa_change_task_id(
            event_ts,
            now=now,
            entropy=f"{self.counter:08x}",
        )


def _attachment(name: str = "request.ts", content: str = "export const ok = true;") -> HpaChangeAttachment:
    return HpaChangeAttachment(
        name=name,
        content=content,
        sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
    )


def _register(store: HpaChangeJobStore, **overrides: Any):
    values: dict[str, Any] = {
        "workspace_id": "T_WORKSPACE",
        "event_ts": "1720580400.000100",
        "channel_id": "C_REQUESTS",
        "thread_ts": "1720580000.000001",
        "requested_by": "U_JUSTIN",
        "request_text": "CR Bonus 프롬프트를 반영해줘",
        "thread_url": "https://lifexio.slack.com/archives/C_REQUESTS/p1720580000000001",
        "attachments": (_attachment(),),
        "metadata": {"source": "slack"},
    }
    values.update(overrides)
    return store.register_job(**values)


def _workflow_run(
    *,
    run_id: int = 501,
    status: str = "in_progress",
    conclusion: str = "",
) -> GitHubWorkflowRun:
    return GitHubWorkflowRun(
        run_id=run_id,
        status=status,
        conclusion=conclusion,
        html_url=f"https://github.com/mmtalk-app/boxer-coordinator/actions/runs/{run_id}",
        display_title="",
        run_attempt=1,
        created_at=_NOW,
        updated_at=_NOW,
    )


def _mark_store_needs_clarification(
    store: HpaChangeJobStore,
    task_id: str,
) -> None:
    """continuation 저장소 테스트가 실제 허용 전이를 따라 부모 상태를 만든다."""

    run = _workflow_run()
    store.begin_dispatch(task_id)
    store.mark_dispatched(task_id)
    store.mark_running(task_id, run)
    store.mark_workflow_succeeded(
        task_id,
        _workflow_run(status="completed", conclusion="success"),
    )
    store.mark_result_ready(
        task_id,
        GitHubArtifactArchive(
            artifact_id=10,
            workflow_run_id=run.run_id,
            name=f"boxer-hpa-result-{task_id}",
            size_in_bytes=20,
            sha256="a" * 64,
            content=b"zip",
        ),
    )
    store.mark_needs_clarification(task_id, "결정이 필요해")


def _zip_result(payload: dict[str, Any], member_name: str = "result.json") -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(member_name, json.dumps(payload, ensure_ascii=False))
    return output.getvalue()


class HpaChangeJobStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = lambda: _NOW
        self.store = HpaChangeJobStore(
            ":memory:",
            clock=self.clock,
            task_id_factory=_TaskIdFactory(),
        )

    def tearDown(self) -> None:
        self.store.close()

    def test_task_id_is_safe_and_contains_event_digest(self) -> None:
        task_id = generate_hpa_change_task_id(
            "1720580400.000100",
            now=_NOW,
            entropy="deadbeef",
        )

        self.assertRegex(task_id, r"^hpa-20260710030000-[a-f0-9]{8}-deadbeef$")
        self.assertNotIn("1720580400", task_id)

    def test_redacts_tokens_assignments_and_nested_sensitive_keys(self) -> None:
        raw = (
            "Authorization: Bearer abcdefghijklmnop "
            "github_token=ghp_abcdefghijklmnopqrstuvwxyz123456 "
            "xoxb-1234567890-abcdefghijkl password=hunter2"
        )

        redacted = redact_sensitive_text(raw)
        nested = redact_sensitive_data(
            {"safe": raw, "api_key": "plain-secret", "child": {"password": "pw"}}
        )

        self.assertNotIn("abcdefghijklmnop", redacted)
        self.assertNotIn("hunter2", redacted)
        self.assertNotIn("xoxb-", redacted)
        self.assertEqual(nested["api_key"], "[REDACTED]")
        self.assertEqual(nested["child"]["password"], "[REDACTED]")

    def test_register_is_idempotent_per_workspace_and_event_ts(self) -> None:
        first = _register(self.store)
        duplicate = _register(
            self.store,
            request_text="재전송된 다른 본문이어도 첫 작업을 사용해",
        )
        another_workspace = _register(
            self.store,
            workspace_id="T_ANOTHER",
        )

        self.assertTrue(first.created)
        self.assertFalse(duplicate.created)
        self.assertEqual(duplicate.job.task_id, first.job.task_id)
        self.assertTrue(another_workspace.created)
        self.assertNotEqual(another_workspace.job.task_id, first.job.task_id)
        self.assertEqual(len(self.store.list_jobs()), 2)

    def test_latest_job_by_thread_does_not_return_stale_clarification(self) -> None:
        first = _register(self.store).job
        second = _register(
            self.store,
            event_ts="1720580400.000101",
            request_text="추가 답변을 반영한 새 요청",
        ).job

        latest = self.store.get_latest_job_by_thread(
            "T_WORKSPACE",
            "C_REQUESTS",
            "1720580000.000001",
        )
        other_thread = self.store.get_latest_job_by_thread(
            "T_WORKSPACE",
            "C_REQUESTS",
            "1720580000.999999",
        )

        # 같은 clock 시각이어도 Slack event 순서로 최신 등록을 선택한다.
        self.assertEqual(latest.task_id, second.task_id)
        self.assertNotEqual(latest.task_id, first.task_id)
        self.assertIsNone(other_thread)

    def test_continuation_parent_is_consumed_atomically_across_stores(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "continuation-race.db"
            first_store = HpaChangeJobStore(
                db_path,
                clock=self.clock,
                task_id_factory=_TaskIdFactory(),
            )
            second_store = HpaChangeJobStore(
                db_path,
                clock=self.clock,
                task_id_factory=_TaskIdFactory(),
            )
            try:
                parent = _register(first_store).job
                _mark_store_needs_clarification(first_store, parent.task_id)
                barrier = threading.Barrier(2)
                created: list[str] = []
                rejected: list[Exception] = []

                def register_child(store: HpaChangeJobStore, event_ts: str) -> None:
                    barrier.wait()
                    try:
                        registration = store.register_continuation_job(
                            parent_task_id=parent.task_id,
                            workspace_id=parent.workspace_id,
                            event_ts=event_ts,
                            channel_id=parent.channel_id,
                            thread_ts=parent.thread_ts,
                            requested_by=parent.requested_by,
                            request_text="추가 답변",
                            thread_url=parent.thread_url,
                            metadata={
                                "source": "slack",
                                "continuation_of_request_id": parent.task_id,
                            },
                        )
                        if registration.created:
                            created.append(registration.job.task_id)
                    except Exception as exc:
                        rejected.append(exc)

                threads = [
                    threading.Thread(
                        target=register_child,
                        args=(first_store, "1720580500.000200"),
                    ),
                    threading.Thread(
                        target=register_child,
                        args=(second_store, "1720580501.000201"),
                    ),
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=5)

                self.assertTrue(all(not thread.is_alive() for thread in threads))
                self.assertEqual(len(created), 1)
                self.assertEqual(len(rejected), 1)
                self.assertIsInstance(rejected[0], InvalidHpaChangeContinuation)
                self.assertEqual(len(first_store.list_jobs()), 2)
            finally:
                second_store.close()
                first_store.close()

    def test_persists_job_and_notified_status_across_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "hpa-change.db"
            first_store = HpaChangeJobStore(
                db_path,
                clock=self.clock,
                task_id_factory=_TaskIdFactory(),
            )
            registration = _register(first_store)
            notified = first_store.mark_notified(
                registration.job.task_id,
                HpaChangePollState.QUEUED,
            )
            first_store.close()

            second_store = HpaChangeJobStore(db_path, clock=self.clock)
            loaded = second_store.get_job(registration.job.task_id)
            second_store.close()

        self.assertEqual(notified.notified_status, "queued")
        self.assertEqual(loaded.notified_status, "queued")
        self.assertEqual(loaded.workspace_id, "T_WORKSPACE")
        self.assertEqual(loaded.attachments[0].name, "request.ts")

    def test_rejects_invalid_transition_and_allows_clarification_redispatch(self) -> None:
        job = _register(self.store).job

        with self.assertRaises(InvalidHpaChangeTransition):
            self.store.mark_pr_created(
                job.task_id,
                ["https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/1"],
            )

        self.store.begin_dispatch(job.task_id)
        self.store.mark_dispatched(job.task_id)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        archive = GitHubArtifactArchive(
            artifact_id=10,
            workflow_run_id=501,
            name=f"boxer-hpa-result-{job.task_id}",
            size_in_bytes=20,
            sha256="a" * 64,
            content=b"zip",
        )
        self.store.mark_result_ready(job.task_id, archive)
        self.store.mark_needs_clarification(job.task_id, "원하는 버튼 정책을 알려줘")
        redispatching = self.store.begin_dispatch(job.task_id)

        self.assertEqual(redispatching.status, HpaChangeStatus.DISPATCHING)
        self.assertEqual(redispatching.dispatch_count, 2)
        self.assertIsNone(redispatching.workflow_run_id)

    def test_implementation_dispatch_claim_is_single_owner_and_recovers_when_stale(self) -> None:
        now = [_NOW]
        store = HpaChangeJobStore(
            ":memory:",
            clock=lambda: now[0],
            task_id_factory=_TaskIdFactory(),
        )
        try:
            job = _register(store, event_ts="1720580400.000109").job
            store.begin_dispatch(job.task_id)
            store.mark_dispatched(job.task_id)
            store.mark_running(job.task_id, _workflow_run(run_id=501))
            store.mark_workflow_succeeded(
                job.task_id,
                _workflow_run(run_id=501, status="completed", conclusion="success"),
            )
            archive = GitHubArtifactArchive(
                artifact_id=10,
                workflow_run_id=501,
                name=f"boxer-hpa-result-{job.task_id}",
                size_in_bytes=20,
                sha256="a" * 64,
                content=b"zip",
            )
            store.mark_review_ready(job.task_id, archive, result={"status": "review_ready"})
            store.mark_review_posted(job.task_id)

            first = store.claim_implementation_dispatch(job.task_id)
            duplicate = store.claim_implementation_dispatch(
                job.task_id,
                allow_recovery=True,
                retry_after_sec=60,
            )
            now[0] = _NOW + timedelta(seconds=61)
            recovered = store.claim_implementation_dispatch(
                job.task_id,
                allow_recovery=True,
                retry_after_sec=60,
            )

            self.assertIsNotNone(first)
            self.assertIsNone(duplicate)
            self.assertIsNotNone(recovered)
            self.assertEqual(first.workflow_phase, "implementation")
            self.assertEqual(first.phase_started_at, _NOW)
            self.assertEqual(recovered.dispatch_count, 3)
            self.assertEqual(recovered.phase_started_at, _NOW)
        finally:
            store.close()

    def test_attachment_rejects_path_traversal_and_wrong_hash(self) -> None:
        with self.assertRaisesRegex(ValueError, "경로 없는 이름"):
            _register(self.store, attachments=(_attachment("../request.ts"),))
        with self.assertRaisesRegex(ValueError, "sha256"):
            _register(
                self.store,
                event_ts="1720580400.000101",
                attachments=(
                    HpaChangeAttachment(name="request.ts", content="hello", sha256="0" * 64),
                ),
            )

    def test_attachment_secret_is_redacted_and_hash_is_recomputed(self) -> None:
        content = "const token = 'ghp_abcdefghijklmnopqrstuvwxyz123456';"
        job = _register(
            self.store,
            attachments=(_attachment(content=content),),
        ).job

        stored = job.attachments[0]
        self.assertNotIn("ghp_", stored.content)
        self.assertEqual(stored.sha256, hashlib.sha256(stored.content.encode()).hexdigest())
        self.assertNotIn("REDACTED GITHUB TOKEN", repr(stored))

    def test_allows_empty_thread_url_when_slack_permalink_lookup_failed(self) -> None:
        job = _register(
            self.store,
            event_ts="1720580400.000102",
            thread_url="",
        ).job

        self.assertEqual(job.thread_url, "")

    def test_list_reportable_jobs_excludes_terminal_jobs_already_notified(self) -> None:
        active = _register(
            self.store,
            event_ts="1720580400.000301",
        ).job
        clarification = _register(
            self.store,
            event_ts="1720580400.000302",
        ).job
        pr_created = _register(
            self.store,
            event_ts="1720580400.000303",
        ).job
        no_change = _register(
            self.store,
            event_ts="1720580400.000305",
        ).job
        failed = _register(
            self.store,
            event_ts="1720580400.000304",
        ).job

        archive = GitHubArtifactArchive(
            artifact_id=10,
            workflow_run_id=501,
            name=f"boxer-hpa-result-{clarification.task_id}",
            size_in_bytes=20,
            sha256="a" * 64,
            content=b"zip",
        )
        for job in (clarification, pr_created, no_change):
            self.store.begin_dispatch(job.task_id)
            self.store.mark_dispatched(job.task_id)
            self.store.mark_running(job.task_id, _workflow_run())
            self.store.mark_workflow_succeeded(
                job.task_id,
                _workflow_run(status="completed", conclusion="success"),
            )
            self.store.mark_result_ready(job.task_id, archive)
        self.store.mark_needs_clarification(clarification.task_id, "질문")
        self.store.mark_pr_created(
            pr_created.task_id,
            ["https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/1"],
        )
        self.store.mark_no_change_needed(no_change.task_id)

        before_notification = {job.task_id for job in self.store.list_reportable_jobs()}
        self.assertIn(no_change.task_id, before_notification)

        self.store.mark_notified(pr_created.task_id, HpaChangePollState.PR_OPENED)
        self.store.mark_notified(no_change.task_id, HpaChangePollState.NO_CHANGE_NEEDED)
        self.store.mark_failed(failed.task_id, "실패")
        self.store.mark_notified(failed.task_id, HpaChangePollState.FAILED)

        reportable_ids = {job.task_id for job in self.store.list_reportable_jobs()}

        self.assertEqual(reportable_ids, {active.task_id, clarification.task_id})


class GitHubAuthenticationTests(unittest.TestCase):
    def test_static_provider_hides_token_from_repr(self) -> None:
        provider = StaticGitHubTokenProvider("ghp_abcdefghijklmnopqrstuvwxyz123456")

        self.assertEqual(provider.get_token(), "ghp_abcdefghijklmnopqrstuvwxyz123456")
        self.assertNotIn("ghp_", repr(provider))

    def test_app_provider_signs_rs256_jwt_and_caches_installation_token(self) -> None:
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()
        expires_at = (_NOW + timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
        session = _FakeSession(
            _FakeResponse(
                201,
                json_data={"token": "ghs_installation_secret_token", "expires_at": expires_at},
            )
        )
        provider = GitHubAppTokenProvider(
            app_id=1234,
            installation_id=5678,
            private_key_pem=private_pem,
            session=session,
            clock=lambda: _NOW,
            restrictions=GitHubAppPermissions(
                repositories=("boxer-coordinator",),
                permissions={"actions": "read", "contents": "write"},
            ),
        )

        first_token = provider.get_token()
        second_token = provider.get_token()

        self.assertEqual(first_token, "ghs_installation_secret_token")
        self.assertEqual(second_token, first_token)
        self.assertEqual(len(session.calls), 1)
        call = session.calls[0]
        app_jwt = call["headers"]["Authorization"].removeprefix("Bearer ")
        header_part, payload_part, signature_part = app_jwt.split(".")
        padded_payload = payload_part + "=" * (-len(payload_part) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded_payload))
        padded_signature = signature_part + "=" * (-len(signature_part) % 4)
        private_key.public_key().verify(
            base64.urlsafe_b64decode(padded_signature),
            f"{header_part}.{payload_part}".encode(),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        self.assertEqual(payload["iss"], "1234")
        self.assertLessEqual(payload["exp"] - payload["iat"], 540)
        self.assertEqual(call["json"]["repositories"], ["boxer-coordinator"])
        self.assertNotIn("PRIVATE KEY", repr(provider))

    def test_app_provider_redacts_failed_response(self) -> None:
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()
        session = _FakeSession(
            _FakeResponse(
                401,
                text="token=ghp_abcdefghijklmnopqrstuvwxyz123456",
            )
        )
        provider = GitHubAppTokenProvider(
            app_id=1,
            installation_id=2,
            private_key_pem=private_pem,
            session=session,
            clock=lambda: _NOW,
        )

        with self.assertRaises(GitHubApiError) as raised:
            provider.get_token()

        self.assertNotIn("ghp_", str(raised.exception))


class GitHubCoordinatorClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = HpaChangeJobStore(
            ":memory:",
            clock=lambda: _NOW,
            task_id_factory=_TaskIdFactory(),
        )
        self.job = _register(
            self.store,
            request_text="token=ghp_abcdefghijklmnopqrstuvwxyz123456 CR 변경",
        ).job
        self.config = GitHubCoordinatorConfig(
            owner="mmtalk-app",
            repository="boxer-coordinator",
            workflow_id="hpa-change.yml",
        )

    def tearDown(self) -> None:
        self.store.close()

    def _client(self, session: _FakeSession) -> GitHubCoordinatorClient:
        return GitHubCoordinatorClient(
            self.config,
            StaticGitHubTokenProvider("ghp_abcdefghijklmnopqrstuvwxyz123456"),
            session=session,
            clock=lambda: _NOW,
        )

    def test_dispatch_uses_exact_worker_contract_and_omits_slack_routing_metadata(self) -> None:
        session = _FakeSession(_FakeResponse(204))
        client = self._client(session)

        receipt = client.dispatch_job(self.job)

        call = session.calls[0]
        body = call["json"]
        self.assertEqual(receipt.event_type, "boxer-hpa-change")
        self.assertEqual(
            call["url"],
            "https://api.github.com/repos/mmtalk-app/boxer-coordinator/dispatches",
        )
        self.assertEqual(body["event_type"], "boxer-hpa-change")
        self.assertEqual(set(body["client_payload"]), {"task_id", "request"})
        request_payload = body["client_payload"]["request"]
        self.assertEqual(
            set(request_payload),
            {"text", "requester_slack_user_id", "thread_url", "attachments"},
        )
        self.assertNotIn("ghp_", request_payload["text"])
        self.assertEqual(request_payload["requester_slack_user_id"], "U_JUSTIN")
        self.assertNotIn("workspace_id", json.dumps(body))
        self.assertNotIn("channel_id", json.dumps(body))
        self.assertNotIn("event_ts", json.dumps(body))

    def test_implementation_dispatch_links_review_run_and_uses_separate_event(self) -> None:
        session = _FakeSession(_FakeResponse(204))
        client = self._client(session)

        receipt = client.dispatch_implementation(self.job, review_run_id=501)

        body = session.calls[0]["json"]
        self.assertEqual(receipt.event_type, "boxer-hpa-implement")
        self.assertEqual(body["event_type"], "boxer-hpa-implement")
        self.assertEqual(body["client_payload"]["task_id"], self.job.task_id)
        self.assertEqual(body["client_payload"]["review_run_id"], 501)
        self.assertEqual(set(body["client_payload"]), {"task_id", "review_run_id"})

    def test_finds_only_exact_task_run_title_in_fixed_workflow(self) -> None:
        expected_title = self.config.expected_run_title(self.job.task_id)
        session = _FakeSession(
            _FakeResponse(
                200,
                json_data={
                    "workflow_runs": [
                        {
                            "id": 100,
                            "display_title": f"prefix {expected_title}",
                            "status": "completed",
                            "conclusion": "success",
                            "created_at": "2026-07-10T03:00:00Z",
                        },
                        {
                            "id": 101,
                            "display_title": expected_title,
                            "status": "in_progress",
                            "conclusion": None,
                            "html_url": "https://github.com/run/101",
                            "created_at": "2026-07-10T03:01:00Z",
                            "updated_at": "2026-07-10T03:02:00Z",
                            "run_attempt": 1,
                        },
                    ]
                },
            )
        )
        client = self._client(session)

        run = client.find_workflow_run(self.job.task_id, created_after=_NOW)

        self.assertIsNotNone(run)
        self.assertEqual(run.run_id, 101)
        self.assertIn(
            "/actions/workflows/hpa-change.yml/runs",
            session.calls[0]["url"],
        )
        self.assertEqual(session.calls[0]["params"]["event"], "repository_dispatch")

    def test_finds_earliest_implementation_run_by_phase_specific_title(self) -> None:
        expected_title = self.config.expected_run_title(
            self.job.task_id,
            phase="implementation",
        )
        session = _FakeSession(
            _FakeResponse(
                200,
                json_data={
                    "workflow_runs": [
                        {
                            "id": 202,
                            "display_title": expected_title,
                            "status": "in_progress",
                            "created_at": "2026-07-10T03:02:00Z",
                        },
                        {
                            "id": 201,
                            "display_title": expected_title,
                            "status": "in_progress",
                            "created_at": "2026-07-10T03:01:00Z",
                        },
                    ]
                },
            )
        )
        client = self._client(session)

        run = client.find_workflow_run(
            self.job.task_id,
            phase="implementation",
            created_after=_NOW,
        )

        self.assertIsNotNone(run)
        self.assertEqual(run.run_id, 201)

    def test_downloads_dynamic_result_zip_from_fixed_artifact_url(self) -> None:
        content = _zip_result(
            {
                "task_id": self.job.task_id,
                "status": "pr_opened",
                "prs": [
                    {
                        "repository": "mmb-hospital-admin-server",
                        "url": "https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/700",
                        "number": 700,
                        "branch": "fix/hpa-cr-700",
                        "base": "develop",
                    }
                ],
            }
        )
        artifact_name = self.config.result_artifact_name(self.job.task_id)
        session = _FakeSession(
            _FakeResponse(
                200,
                json_data={
                    "id": 501,
                    "display_title": self.config.expected_run_title(self.job.task_id),
                    "path": ".github/workflows/hpa-change.yml@main",
                    "status": "completed",
                    "conclusion": "success",
                    "html_url": "https://github.com/run/501",
                    "created_at": "2026-07-10T03:00:00Z",
                    "updated_at": "2026-07-10T03:10:00Z",
                },
            ),
            _FakeResponse(
                200,
                json_data={
                    "artifacts": [
                        {
                            "id": 900,
                            "name": artifact_name,
                            "expired": False,
                            "size_in_bytes": len(content),
                            "archive_download_url": "https://attacker.example/archive.zip",
                        }
                    ]
                },
            ),
            _FakeResponse(
                200,
                content=content,
                headers={"Content-Length": str(len(content))},
            ),
        )
        client = self._client(session)

        archive = client.download_result_artifact_zip(501, self.job.task_id)
        result = client.read_result_artifact_json(archive)

        self.assertEqual(archive.name, artifact_name)
        self.assertEqual(archive.sha256, hashlib.sha256(content).hexdigest())
        self.assertEqual(result["status"], "pr_opened")
        self.assertEqual(
            session.calls[2]["url"],
            "https://api.github.com/repos/mmtalk-app/boxer-coordinator/actions/artifacts/900/zip",
        )
        self.assertNotIn("attacker.example", session.calls[2]["url"])

    def test_missing_dynamic_artifact_is_retryable(self) -> None:
        session = _FakeSession(
            _FakeResponse(
                200,
                json_data={
                    "id": 501,
                    "display_title": self.config.expected_run_title(self.job.task_id),
                    "path": ".github/workflows/hpa-change.yml",
                    "status": "completed",
                    "conclusion": "success",
                },
            ),
            _FakeResponse(200, json_data={"artifacts": []}),
        )
        client = self._client(session)

        with self.assertRaises(GitHubArtifactNotReady):
            client.download_result_artifact_zip(501, self.job.task_id)

    def test_rejects_invalid_zip_and_duplicate_result_members(self) -> None:
        archive = GitHubArtifactArchive(
            artifact_id=1,
            workflow_run_id=2,
            name=self.config.result_artifact_name(self.job.task_id),
            size_in_bytes=3,
            sha256=hashlib.sha256(b"bad").hexdigest(),
            content=b"bad",
        )
        client = self._client(_FakeSession())

        with self.assertRaises(GitHubArtifactError):
            client.read_result_artifact_json(archive)


class _FakeCoordinator:
    def __init__(self) -> None:
        self.dispatches: list[str] = []
        self.implementation_dispatches: list[tuple[str, int]] = []
        self.find_phases: list[str] = []
        self.runs: list[GitHubWorkflowRun | None] = []
        self.result: dict[str, Any] = {}
        self.artifact_not_ready = False
        self.dispatch_error: Exception | None = None
        self.implementation_dispatch_error: Exception | None = None

    def dispatch_job(self, job) -> None:
        if self.dispatch_error is not None:
            raise self.dispatch_error
        self.dispatches.append(job.task_id)

    def dispatch_implementation(self, job, *, review_run_id: int) -> None:
        if self.implementation_dispatch_error is not None:
            raise self.implementation_dispatch_error
        self.implementation_dispatches.append((job.task_id, review_run_id))

    def find_workflow_run(
        self,
        _task_id: str,
        *,
        phase: str = "review",
        created_after: datetime,
    ):
        del created_after
        self.find_phases.append(phase)
        return self.runs.pop(0) if self.runs else None

    def download_result_artifact_zip(self, run_id: int, task_id: str) -> GitHubArtifactArchive:
        if self.artifact_not_ready:
            raise GitHubArtifactNotReady("아직 없어")
        content = _zip_result(self.result)
        return GitHubArtifactArchive(
            artifact_id=900,
            workflow_run_id=run_id,
            name=f"boxer-hpa-result-{task_id}",
            size_in_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            content=content,
        )

    def read_result_artifact_json(self, _archive: GitHubArtifactArchive) -> dict[str, Any]:
        return self.result


class HpaChangeWorkflowServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = HpaChangeJobStore(
            ":memory:",
            clock=lambda: _NOW,
            task_id_factory=_TaskIdFactory(),
        )
        self.coordinator = _FakeCoordinator()
        self.service = HpaChangeWorkflowService(self.store, self.coordinator)  # type: ignore[arg-type]
        self.request = HpaChangeRequest(
            workspace_id="T_WORKSPACE",
            event_ts="1720580400.000100",
            channel_id="C_REQUESTS",
            thread_ts="1720580000.000001",
            requested_by="U_JUSTIN",
            request_text="HPA CR 변경 요청",
            thread_url="https://lifexio.slack.com/archives/C_REQUESTS/p1720580000000001",
            attachments=(_attachment(),),
        )

    def tearDown(self) -> None:
        self.store.close()

    @staticmethod
    def _no_change_result(task_id: str) -> dict[str, Any]:
        request = "Bonus 프롬프트와 생성 설정 변경"
        return {
            "task_id": task_id,
            "status": "no_change_needed",
            "review": {
                "requesterView": {
                    "requestItems": [
                        {
                            "itemId": "REQ-01",
                            "request": request,
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
                        "request": request,
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
            "prs": [],
        }

    def test_submit_dispatches_once_and_returns_existing_job_for_duplicate_event(self) -> None:
        first_job, first_created = self.service.submit(self.request)
        duplicate_job, duplicate_created = self.service.submit(self.request)

        self.assertTrue(first_created)
        self.assertFalse(duplicate_created)
        self.assertEqual(first_job.status, HpaChangeStatus.DISPATCHED)
        self.assertEqual(duplicate_job.task_id, first_job.task_id)
        self.assertEqual(self.coordinator.dispatches, [first_job.task_id])

    def test_review_transport_error_stays_dispatching_for_reconciliation(self) -> None:
        self.coordinator.dispatch_error = GitHubApiError("timeout", status_code=None)

        job, created = self.service.submit(self.request)

        self.assertTrue(created)
        self.assertEqual(job.status, HpaChangeStatus.DISPATCHING)
        self.assertEqual(job.workflow_phase, "review")

    def test_review_http_error_is_terminal(self) -> None:
        self.coordinator.dispatch_error = GitHubApiError("forbidden", status_code=403)

        with self.assertRaises(GitHubApiError):
            self.service.submit(self.request)

        job = self.store.get_job_by_event_ts(
            self.request.workspace_id,
            self.request.event_ts,
        )
        self.assertIsNotNone(job)
        self.assertEqual(job.status, HpaChangeStatus.FAILED)

    def test_poll_dispatches_job_left_received_by_process_restart(self) -> None:
        registration = _register(
            self.store,
            event_ts="1720580400.000200",
        )

        result = self.service.poll_job(registration.job.task_id)

        self.assertEqual(result.state, HpaChangePollState.QUEUED)
        self.assertEqual(result.job.status, HpaChangeStatus.DISPATCHED)
        self.assertEqual(self.coordinator.dispatches, [registration.job.task_id])

    def test_stale_review_claim_is_redispatched_after_process_restart(self) -> None:
        now = [_NOW]
        self.store._clock = lambda: now[0]
        registration = _register(
            self.store,
            event_ts="1720580400.000201",
        )
        # 외부 GitHub 호출 직전에 프로세스가 종료된 상태를 만든다.
        self.store.claim_review_dispatch(registration.job.task_id)
        now[0] = _NOW + timedelta(seconds=61)

        recovered = self.service.poll_job(registration.job.task_id)

        self.assertEqual(recovered.job.status, HpaChangeStatus.DISPATCHED)
        self.assertEqual(self.coordinator.dispatches, [registration.job.task_id])
        self.assertEqual(recovered.job.dispatch_count, 2)

    def test_dispatching_job_accepts_already_completed_workflow_after_response_loss(self) -> None:
        registration = _register(
            self.store,
            event_ts="1720580400.000202",
        )
        self.store.claim_review_dispatch(registration.job.task_id)
        self.coordinator.runs.append(
            _workflow_run(run_id=501, status="completed", conclusion="success")
        )
        self.coordinator.result = {
            "task_id": registration.job.task_id,
            "status": "review_ready",
            "review": {"summary": "응답 유실 뒤에도 검토 결과 복구"},
        }

        recovered = self.service.poll_job(registration.job.task_id)

        self.assertEqual(recovered.state, HpaChangePollState.REVIEW_READY)
        self.assertEqual(recovered.job.status, HpaChangeStatus.REVIEW_READY)
        self.assertEqual(recovered.job.workflow_run_id, 501)

    def test_poll_moves_queued_running_and_pr_opened_with_prs_contract(self) -> None:
        job, _ = self.service.submit(self.request)
        self.coordinator.runs.extend(
            [
                None,
                _workflow_run(status="in_progress"),
                _workflow_run(status="completed", conclusion="success"),
            ]
        )

        queued = self.service.poll_job(job)
        running = self.service.poll_job(job.task_id)
        self.coordinator.result = {
            "task_id": job.task_id,
            "status": "pr_opened",
            "summary": "두 PR을 열었어",
            "prs": [
                {
                    "repository": "mmb-hospital-admin-server",
                    "url": "https://github.com/mmtalk-app/mmb-hospital-admin-server/pull/700",
                    "number": 700,
                    "branch": "fix/hpa-cr-700",
                    "base": "develop",
                },
                {
                    "repository": "mmb-hospital-admin-client",
                    "url": "https://github.com/mmtalk-app/mmb-hospital-admin-client/pull/765",
                    "number": 765,
                    "branch": "feat/hpa-cr-ui",
                    "base": "develop",
                },
            ],
        }
        completed = self.service.poll_job(job.task_id)

        self.assertEqual(queued.state, HpaChangePollState.QUEUED)
        self.assertEqual(running.state, HpaChangePollState.RUNNING)
        self.assertEqual(completed.state, HpaChangePollState.PR_OPENED)
        self.assertEqual(len(completed.pr_urls), 2)
        self.assertEqual(completed.result["prs"][0]["base"], "develop")
        notified = self.store.mark_notified(job.task_id, completed.state)
        self.assertEqual(notified.notified_status, "pr_opened")

    def test_poll_rejects_pr_from_non_hpa_repository(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        self.coordinator.result = {
            "task_id": job.task_id,
            "status": "pr_opened",
            "prs": [{"url": "https://github.com/another-org/private-repo/pull/777"}],
        }

        with self.assertRaises(GitHubArtifactError):
            self.service.poll_job(job.task_id)

        self.assertEqual(
            self.store.get_job(job.task_id).status,
            HpaChangeStatus.FAILED,
        )

    def test_poll_maps_clarification_result(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        self.coordinator.result = {
            "task_id": job.task_id,
            "status": "needs_clarification",
            "questions": ["Basic만 보낼지 병원 설정으로 둘지 알려줘"],
        }

        result = self.service.poll_job(job.task_id)

        self.assertEqual(result.state, HpaChangePollState.NEEDS_CLARIFICATION)
        self.assertIn("Basic", result.message)

    def test_poll_maps_verified_no_change_result_without_pr(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        self.coordinator.result = self._no_change_result(job.task_id)

        result = self.service.poll_job(job.task_id)

        self.assertEqual(result.state, HpaChangePollState.NO_CHANGE_NEEDED)
        self.assertEqual(result.job.status, HpaChangeStatus.NO_CHANGE_NEEDED)
        self.assertEqual(result.pr_urls, ())

    def test_poll_recovers_saved_no_change_result_after_process_restart(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        archive = GitHubArtifactArchive(
            artifact_id=701,
            workflow_run_id=501,
            name=f"boxer-hpa-result-{job.task_id}",
            size_in_bytes=2,
            sha256=hashlib.sha256(b"{}").hexdigest(),
            content=b"{}",
        )
        self.store.mark_result_ready(
            job.task_id,
            archive,
            result=self._no_change_result(job.task_id),
        )

        recovered = self.service.poll_job(job.task_id)

        self.assertEqual(recovered.state, HpaChangePollState.NO_CHANGE_NEEDED)
        self.assertEqual(recovered.job.status, HpaChangeStatus.NO_CHANGE_NEEDED)

    def test_poll_recovers_saved_legacy_success_with_hpa_pr(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        archive = GitHubArtifactArchive(
            artifact_id=702,
            workflow_run_id=501,
            name=f"boxer-hpa-result-{job.task_id}",
            size_in_bytes=2,
            sha256=hashlib.sha256(b"{}").hexdigest(),
            content=b"{}",
        )
        self.store.mark_result_ready(
            job.task_id,
            archive,
            result={
                "task_id": job.task_id,
                "status": "success",
                "prs": [
                    {
                        "url": (
                            "https://github.com/mmtalk-app/"
                            "mmb-hospital-admin-server/pull/801"
                        )
                    }
                ],
            },
        )

        recovered = self.service.poll_job(job.task_id)

        self.assertEqual(recovered.state, HpaChangePollState.PR_OPENED)
        self.assertEqual(recovered.job.status, HpaChangeStatus.PR_CREATED)

    def test_poll_rejects_malformed_saved_legacy_pr_payload(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        archive = GitHubArtifactArchive(
            artifact_id=703,
            workflow_run_id=501,
            name=f"boxer-hpa-result-{job.task_id}",
            size_in_bytes=2,
            sha256=hashlib.sha256(b"{}").hexdigest(),
            content=b"{}",
        )
        self.store.mark_result_ready(
            job.task_id,
            archive,
            result={
                "task_id": job.task_id,
                "status": "success",
                "prs": {
                    "url": (
                        "https://github.com/mmtalk-app/"
                        "mmb-hospital-admin-server/pull/802"
                    )
                },
            },
        )

        with self.assertRaises(GitHubArtifactError):
            self.service.poll_job(job.task_id)

        self.assertEqual(
            self.store.get_job(job.task_id).status,
            HpaChangeStatus.FAILED,
        )

    def test_poll_rejects_null_saved_legacy_pr_payload(self) -> None:
        request = HpaChangeRequest(
            **{
                **self.request.__dict__,
                "event_ts": "1720580400.000299",
            }
        )
        job, _ = self.service.submit(request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        archive = GitHubArtifactArchive(
            artifact_id=704,
            workflow_run_id=501,
            name=f"boxer-hpa-result-{job.task_id}",
            size_in_bytes=2,
            sha256=hashlib.sha256(b"{}").hexdigest(),
            content=b"{}",
        )
        self.store.mark_result_ready(
            job.task_id,
            archive,
            result={"task_id": job.task_id, "status": "success", "prs": None},
        )

        with self.assertRaises(GitHubArtifactError):
            self.service.poll_job(job.task_id)

        self.assertEqual(
            self.store.get_job(job.task_id).status,
            HpaChangeStatus.FAILED,
        )

    def test_no_change_result_fails_closed_when_public_contract_is_invalid(self) -> None:
        invalid_results: list[tuple[str, Any]] = [
            ("qualityGates.verificationPassed", False),
            ("implementation.appliedResults.0.status", "applied"),
            ("implementation.appliedResults.0.request", "다른 요청"),
            ("review.requesterView.requestItems.0.handling", "adapted"),
            ("prs", [{"url": "https://invalid.example/pull/1"}]),
        ]
        for index, (field, value) in enumerate(invalid_results, 1):
            with self.subTest(field=field):
                request = HpaChangeRequest(
                    **{
                        **self.request.__dict__,
                        "event_ts": f"1720580400.{index + 100:06d}",
                    }
                )
                job, _ = self.service.submit(request)
                self.store.mark_running(job.task_id, _workflow_run())
                self.store.mark_workflow_succeeded(
                    job.task_id,
                    _workflow_run(status="completed", conclusion="success"),
                )
                result = self._no_change_result(job.task_id)
                target: Any = result
                parts = field.split(".")
                for part in parts[:-1]:
                    target = target[int(part)] if part.isdigit() else target[part]
                target[parts[-1]] = value
                self.coordinator.result = result

                with self.assertRaises(GitHubArtifactError):
                    self.service.poll_job(job.task_id)
                self.assertEqual(
                    self.store.get_job(job.task_id).status,
                    HpaChangeStatus.FAILED,
                )

    def test_review_ready_waits_for_slack_post_before_implementation_dispatch(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        self.coordinator.result = {
            "task_id": job.task_id,
            "status": "review_ready",
            "review": {
                "summary": "HPA 구조 기준 변환안을 확정했어",
                "blockingQuestions": [],
            },
        }

        reviewed = self.service.poll_job(job.task_id)

        self.assertEqual(reviewed.state, HpaChangePollState.REVIEW_READY)
        self.assertEqual(reviewed.job.status, HpaChangeStatus.REVIEW_READY)
        self.assertEqual(self.coordinator.implementation_dispatches, [])
        with self.assertRaises(InvalidHpaChangeTransition):
            self.service.dispatch_implementation(job.task_id)

        self.store.mark_review_posted(job.task_id)
        dispatched = self.service.dispatch_implementation(job.task_id)

        self.assertEqual(dispatched.status, HpaChangeStatus.DISPATCHED)
        self.assertEqual(
            self.coordinator.implementation_dispatches,
            [(job.task_id, 501)],
        )
        self.assertEqual(dispatched.result["review"]["summary"], "HPA 구조 기준 변환안을 확정했어")

    def test_implementation_transport_error_stays_dispatching_for_reconciliation(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        self.coordinator.result = {
            "task_id": job.task_id,
            "status": "review_ready",
            "review": {"summary": "검토 완료"},
        }
        self.service.poll_job(job.task_id)
        self.store.mark_review_posted(job.task_id)
        self.coordinator.implementation_dispatch_error = GitHubApiError(
            "timeout",
            status_code=None,
        )

        dispatching = self.service.dispatch_implementation(job.task_id)

        self.assertEqual(dispatching.status, HpaChangeStatus.DISPATCHING)
        self.assertEqual(dispatching.workflow_phase, "implementation")

    def test_poll_recovers_implementation_dispatch_after_review_post(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        self.coordinator.result = {
            "task_id": job.task_id,
            "status": "review_ready",
            "review": {"summary": "검토 완료"},
        }
        self.service.poll_job(job.task_id)
        self.store.mark_review_posted(job.task_id)

        recovered = self.service.poll_job(job.task_id)

        self.assertEqual(recovered.job.status, HpaChangeStatus.DISPATCHED)
        self.assertEqual(self.coordinator.implementation_dispatches, [(job.task_id, 501)])

    def test_implementation_poll_uses_phase_specific_run_lookup(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run(run_id=501))
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(run_id=501, status="completed", conclusion="success"),
        )
        self.coordinator.result = {
            "task_id": job.task_id,
            "status": "review_ready",
            "review": {"summary": "검토 완료"},
        }
        self.service.poll_job(job.task_id)
        self.store.mark_review_posted(job.task_id)
        self.service.dispatch_implementation(job.task_id)
        self.coordinator.runs.append(_workflow_run(run_id=502, status="in_progress"))

        running = self.service.poll_job(job.task_id)

        self.assertEqual(running.job.status, HpaChangeStatus.RUNNING)
        self.assertEqual(running.job.workflow_run_id, 502)
        self.assertEqual(self.coordinator.find_phases[-1], "implementation")

    def test_stale_implementation_claim_is_redispatched_after_process_restart(self) -> None:
        now = [_NOW]
        self.store._clock = lambda: now[0]
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run(run_id=501))
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(run_id=501, status="completed", conclusion="success"),
        )
        self.coordinator.result = {
            "task_id": job.task_id,
            "status": "review_ready",
            "review": {"summary": "검토 완료"},
        }
        self.service.poll_job(job.task_id)
        self.store.mark_review_posted(job.task_id)
        # 외부 GitHub 호출 직전에 프로세스가 종료된 상태를 만든다.
        self.store.claim_implementation_dispatch(job.task_id)
        now[0] = _NOW + timedelta(seconds=61)

        recovered = self.service.poll_job(job.task_id)

        self.assertEqual(recovered.job.status, HpaChangeStatus.DISPATCHED)
        self.assertEqual(self.coordinator.implementation_dispatches, [(job.task_id, 501)])
        self.assertEqual(recovered.job.dispatch_count, 3)

    def test_poll_keeps_successful_workflow_running_while_artifact_is_not_ready(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        self.coordinator.artifact_not_ready = True

        result = self.service.poll_job(job.task_id)

        self.assertEqual(result.state, HpaChangePollState.RUNNING)
        self.assertEqual(result.job.status, HpaChangeStatus.WORKFLOW_SUCCEEDED)

    def test_failed_result_redacts_error(self) -> None:
        job, _ = self.service.submit(self.request)
        self.store.mark_running(job.task_id, _workflow_run())
        self.store.mark_workflow_succeeded(
            job.task_id,
            _workflow_run(status="completed", conclusion="success"),
        )
        self.coordinator.result = {
            "task_id": job.task_id,
            "status": "failed",
            "error": "github_token=ghp_abcdefghijklmnopqrstuvwxyz123456",
        }

        result = self.service.poll_job(job.task_id)

        self.assertEqual(result.state, HpaChangePollState.FAILED)
        self.assertNotIn("ghp_", result.message)


if __name__ == "__main__":
    unittest.main()
