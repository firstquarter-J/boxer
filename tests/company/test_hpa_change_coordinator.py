from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from boxer_company.hpa_change_coordinator import (
    HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS,
    HpaChangeCoordinator,
    HpaChangeCoordinatorConfig,
    HpaChangeDeliveryAck,
    HpaChangeSubmissionAttachment,
    HpaChangeSubmissionRequest,
    HpaChangeSubmissionState,
    create_hpa_change_coordinator,
)
from boxer_company.hpa_change_workflow import (
    GitHubArtifactArchive,
    GitHubWorkflowRun,
    HpaChangeJobStore,
    HpaChangePollState,
    HpaChangeStatus,
    HpaChangeWorkflowService,
)
import pytest


class _GitHub:
    def __init__(self) -> None:
        self.review_dispatches: list[str] = []
        self.implementation_dispatches: list[tuple[str, int]] = []

    def dispatch_job(self, job):
        self.review_dispatches.append(job.task_id)

    def dispatch_implementation(self, job, *, review_run_id: int):
        self.implementation_dispatches.append((job.task_id, review_run_id))

    def find_workflow_run(self, *_args, **_kwargs):
        return None


def _config() -> HpaChangeCoordinatorConfig:
    return HpaChangeCoordinatorConfig(
        enabled=True,
        allowed_channel_ids=HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS,
        poll_interval_sec=20,
        run_timeout_sec=10_800,
        max_thread_chars=30_000,
        max_attachment_count=5,
        max_attachment_bytes=131_072,
        max_total_attachment_bytes=524_288,
    )


def _request(event_ts: str = "1720580400.000100") -> HpaChangeSubmissionRequest:
    content = "prompt body"
    return HpaChangeSubmissionRequest(
        request_key=f"slack:TWORK:C02C08K7YEN:{event_ts}",
        workspace_id="TWORK",
        channel_id="C02C08K7YEN",
        thread_ts="1720580000.000001",
        thread_url=(
            "https://lifexio.slack.com/archives/C068FVD5V7Y/"
            "p1720580000000001"
        ),
        event_ts=event_ts,
        requester_user_id="U07A5FM5XPD",
        initiator_user_id="U07A5FM5XPD",
        thread_text="Bonus 프롬프트 변경을 검토해줘",
        attachments=(
            HpaChangeSubmissionAttachment(
                name="handoff.txt",
                content=content,
                sha256=hashlib.sha256(content.encode()).hexdigest(),
            ),
        ),
        source_channel_id="C068FVD5V7Y",
        source_message_ts="1720580000.000001",
        selection_mode="linked_message",
        response_thread_url=(
            "https://lifexio.slack.com/archives/C02C08K7YEN/"
            "p1720580000000001"
        ),
    )


def _runtime(tmp_path: Path) -> tuple[HpaChangeCoordinator, _GitHub]:
    store = HpaChangeJobStore(tmp_path / "hpa.sqlite3")
    github = _GitHub()
    workflow = HpaChangeWorkflowService(store, github)  # type: ignore[arg-type]
    return (
        HpaChangeCoordinator(
            _config(),
            store=store,
            workflow=workflow,
            auth_mode="test",
        ),
        github,
    )


def _review_result() -> dict:
    return {
        "status": "review_ready",
        "qualityGates": {"initialRequestCoveragePassed": True},
        "review": {
            "requesterView": {
                "summaryCode": "adaptation_available",
                "wrongAssumptions": [],
                "whyNotDirectCodes": ["different_product_structure"],
                "requestItems": [
                    {
                        "itemId": "REQ-01",
                        "request": "Bonus 프롬프트 변경",
                        "handling": "adapted",
                        "reasonCode": "cross_product_difference",
                        "applicationCode": "implement_hpa_equivalent",
                    }
                ],
            }
        },
    }


def _review_run() -> GitHubWorkflowRun:
    now = datetime(2026, 8, 27, 5, 0, tzinfo=timezone.utc)
    return GitHubWorkflowRun(
        run_id=501,
        status="completed",
        conclusion="success",
        html_url=(
            "https://github.com/mmtalk-app/"
            "mmb-hospital-admin-server/actions/runs/501"
        ),
        created_at=now,
    )


def _review_archive(task_id: str) -> GitHubArtifactArchive:
    content = b"{}"
    return GitHubArtifactArchive(
        artifact_id=700,
        name=f"boxer-hpa-result-{task_id}",
        content=content,
    )


def test_submit_owns_sqlite_and_github_dispatch_outside_slack(tmp_path: Path) -> None:
    coordinator, github = _runtime(tmp_path)

    first = coordinator.submit(_request())
    duplicate = coordinator.submit(_request())

    assert first.state is HpaChangeSubmissionState.ACCEPTED
    assert duplicate.state is HpaChangeSubmissionState.DUPLICATE
    assert duplicate.request_id == first.request_id
    assert github.review_dispatches == [first.request_id]
    assert coordinator.store is not None
    assert coordinator.store.get_job(first.request_id).status is HpaChangeStatus.DISPATCHED


def test_tick_pauses_state_progress_until_exact_delivery_ack(tmp_path: Path) -> None:
    coordinator, _github = _runtime(tmp_path)
    submitted = coordinator.submit(_request())
    assert coordinator.store is not None
    coordinator.store.mark_running(submitted.request_id, _review_run())

    first = coordinator.tick()
    second = coordinator.tick()

    # pending transport가 있으면 GitHub를 더 poll하지 않고 같은 snapshot을 유지한다.
    assert len(first) == 1
    assert second[0].delivery_id == first[0].delivery_id
    assert first[0].state is HpaChangePollState.RUNNING
    ack = coordinator.acknowledge_delivery(
        HpaChangeDeliveryAck(
            delivery_id=first[0].delivery_id,
            task_id=first[0].task_id,
            workspace_id=first[0].workspace_id,
            state=first[0].state,
        )
    )
    assert ack.acknowledged is True
    assert coordinator.pull_pending(workspace_id="TWORK") == ()


def test_review_ack_starts_implementation_in_api_and_is_idempotent(tmp_path: Path) -> None:
    coordinator, github = _runtime(tmp_path)
    submitted = coordinator.submit(_request())
    assert coordinator.store is not None
    run = _review_run()
    coordinator.store.mark_running(submitted.request_id, run)
    coordinator.store.mark_workflow_succeeded(submitted.request_id, run)
    coordinator.store.mark_review_ready(
        submitted.request_id,
        _review_archive(submitted.request_id),
        result=_review_result(),
    )

    delivery = coordinator.tick()[0]
    ack_input = HpaChangeDeliveryAck(
        delivery_id=delivery.delivery_id,
        task_id=delivery.task_id,
        workspace_id=delivery.workspace_id,
        state=delivery.state,
    )
    first = coordinator.acknowledge_delivery(ack_input)
    second = coordinator.acknowledge_delivery(ack_input)

    assert delivery.state is HpaChangePollState.REVIEW_READY
    assert first.implementation_dispatch_started is True
    assert second.acknowledged is True
    assert github.implementation_dispatches == [(submitted.request_id, 501)]
    job = coordinator.store.get_job(submitted.request_id)
    assert job.workflow_phase == "implementation"
    assert job.notified_status == HpaChangePollState.REVIEW_READY.value

    # 같은 task/state라도 다른 delivery id는 ACK timeout retry로 가장할 수 없다.
    with pytest.raises(ValueError, match="exact receipt"):
        coordinator.acknowledge_delivery(
            HpaChangeDeliveryAck(
                delivery_id="hpa-delivery:" + "f" * 64,
                task_id=delivery.task_id,
                workspace_id=delivery.workspace_id,
                state=delivery.state,
            )
        )


def test_review_ack_transaction_recovers_dispatch_after_process_exit(
    tmp_path: Path,
) -> None:
    coordinator, github = _runtime(tmp_path)
    submitted = coordinator.submit(_request())
    assert coordinator.store is not None
    run = _review_run()
    coordinator.store.mark_running(submitted.request_id, run)
    coordinator.store.mark_workflow_succeeded(submitted.request_id, run)
    coordinator.store.mark_review_ready(
        submitted.request_id,
        _review_archive(submitted.request_id),
        result=_review_result(),
    )
    delivery = coordinator.tick()[0]

    # ACK transaction 직후 process가 종료돼 외부 dispatch 호출을 못 한 상태다.
    coordinator.store.mark_delivery_notified(
        delivery.task_id,
        delivery.state,
        delivery.delivery_id,
        advance_review_posted=True,
    )
    persisted = coordinator.store.get_job(delivery.task_id)
    assert persisted.status is HpaChangeStatus.REVIEW_POSTED
    assert persisted.notified_status == HpaChangePollState.REVIEW_READY.value
    assert github.implementation_dispatches == []

    # 다음 companion tick은 중간 running 알림 없이 구현 dispatch를 이어받는다.
    assert coordinator.tick() == ()
    assert github.implementation_dispatches == [(delivery.task_id, 501)]


def test_invalid_review_contract_fails_before_delivery_or_implementation(tmp_path: Path) -> None:
    coordinator, github = _runtime(tmp_path)
    submitted = coordinator.submit(_request())
    assert coordinator.store is not None
    run = _review_run()
    coordinator.store.mark_running(submitted.request_id, run)
    coordinator.store.mark_workflow_succeeded(submitted.request_id, run)
    coordinator.store.mark_review_ready(
        submitted.request_id,
        _review_archive(submitted.request_id),
        result={"status": "review_ready", "review": {"requesterView": {}}},
    )

    delivery = coordinator.tick()[0]

    assert delivery.state is HpaChangePollState.FAILED
    assert github.implementation_dispatches == []
    assert coordinator.store.get_job(submitted.request_id).status is HpaChangeStatus.FAILED


def test_disabled_api_coordinator_needs_no_github_or_sqlite_credentials() -> None:
    coordinator = create_hpa_change_coordinator(
        settings=SimpleNamespace(HPA_CHANGE_REQUEST_ENABLED=False)
    )

    assert coordinator.enabled is False
    assert coordinator.store is None
    assert coordinator.auth_mode == "disabled"


def test_factory_places_job_database_at_api_configured_path() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "api-hpa.sqlite3"
        settings = SimpleNamespace(
            HPA_CHANGE_REQUEST_ENABLED=True,
            HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS=(
                HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS
            ),
            HPA_CHANGE_POLL_INTERVAL_SEC=20,
            HPA_CHANGE_RUN_TIMEOUT_SEC=10_800,
            HPA_CHANGE_MAX_THREAD_CHARS=30_000,
            HPA_CHANGE_MAX_FILES=5,
            HPA_CHANGE_MAX_FILE_BYTES=131_072,
            HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES=524_288,
            HPA_CHANGE_GITHUB_COORDINATOR_REPOSITORY=(
                "mmtalk-app/mmb-hospital-admin-server"
            ),
            HPA_CHANGE_GITHUB_WORKFLOW_FILE="boxer-hpa-change.yml",
            HPA_CHANGE_GITHUB_API_URL="https://api.github.com",
            HPA_CHANGE_GITHUB_TOKEN="test-token",
            HPA_CHANGE_GITHUB_APP_ID="",
            HPA_CHANGE_GITHUB_APP_INSTALLATION_ID="",
            HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH="",
            HPA_CHANGE_JOB_DB_PATH=str(db_path),
        )

        coordinator = create_hpa_change_coordinator(settings=settings)
        try:
            assert coordinator.enabled is True
            assert coordinator.store is not None
            assert db_path.exists()
            assert stat.S_IMODE(db_path.stat().st_mode) == 0o600
        finally:
            coordinator.close()


def test_factory_rejects_relative_or_unprotected_existing_job_database() -> None:
    base = SimpleNamespace(
        HPA_CHANGE_REQUEST_ENABLED=True,
        HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS=(
            HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS
        ),
        HPA_CHANGE_POLL_INTERVAL_SEC=20,
        HPA_CHANGE_RUN_TIMEOUT_SEC=10_800,
        HPA_CHANGE_MAX_THREAD_CHARS=30_000,
        HPA_CHANGE_MAX_FILES=5,
        HPA_CHANGE_MAX_FILE_BYTES=131_072,
        HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES=524_288,
        HPA_CHANGE_GITHUB_COORDINATOR_REPOSITORY=(
            "mmtalk-app/mmb-hospital-admin-server"
        ),
        HPA_CHANGE_GITHUB_WORKFLOW_FILE="boxer-hpa-change.yml",
        HPA_CHANGE_GITHUB_API_URL="https://api.github.com",
        HPA_CHANGE_GITHUB_TOKEN="test-token",
        HPA_CHANGE_GITHUB_APP_ID="",
        HPA_CHANGE_GITHUB_APP_INSTALLATION_ID="",
        HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH="",
        HPA_CHANGE_JOB_DB_PATH="relative.sqlite3",
    )
    with pytest.raises(ValueError, match="절대 파일 경로"):
        create_hpa_change_coordinator(settings=base)

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "hpa.sqlite3"
        db_path.touch(mode=0o600)
        os.chmod(db_path, 0o644)
        base.HPA_CHANGE_JOB_DB_PATH = str(db_path)
        with pytest.raises(ValueError, match="파일이 안전하지 않아"):
            create_hpa_change_coordinator(settings=base)
