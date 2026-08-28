from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from boxer_company import settings as company_settings
from boxer_company.hpa_change_workflow import (
    GitHubAppPermissions,
    GitHubAppTokenProvider,
    GitHubCoordinatorClient,
    GitHubCoordinatorConfig,
    HpaChangeAttachment,
    HpaChangeJob,
    HpaChangeJobStore,
    HpaChangeRequest,
    HpaChangeStatus,
    HpaChangeWorkflowService,
    InvalidHpaChangeContinuation,
    StaticGitHubTokenProvider,
)
from boxer_company.transport_contracts import (
    HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS,
    HpaChangePollState,
)


_GITHUB_EVENT_TYPE = "boxer-hpa-change"
_GITHUB_RUN_NAME_PREFIX = "Boxer HPA Review"
_GITHUB_IMPLEMENTATION_RUN_NAME_PREFIX = "Boxer HPA Implementation"
_ACTIVE_STATUSES = frozenset(
    {
        HpaChangeStatus.RECEIVED,
        HpaChangeStatus.DISPATCHING,
        HpaChangeStatus.DISPATCHED,
        HpaChangeStatus.RUNNING,
        HpaChangeStatus.WORKFLOW_SUCCEEDED,
        HpaChangeStatus.RESULT_READY,
    }
)
_RECOVERABLE_RESULT_STATUSES = frozenset(
    {
        "needs_clarification",
        "clarification_required",
        "blocked",
        "pr_opened",
        "pr_created",
        "failed",
        "error",
        "canceled",
        "cancelled",
        "no_change_needed",
    }
)
_REVIEW_SUMMARY_CODES = frozenset(
    {
        "adaptation_available",
        "already_supported",
        "product_decision_required",
        "mixed",
    }
)
_WRONG_ASSUMPTION_CODES = frozenset(
    {
        "web_only_term",
        "copy_not_portable",
        "referenced_call_unavailable",
        "configuration_not_shared",
        "timeout_baseline_differs",
        "already_satisfied",
        "product_decision_needed",
    }
)
_INCOMPATIBILITY_CODES = frozenset(
    {
        "different_product_structure",
        "different_operating_environment",
        "different_state_and_data_flow",
        "different_release_validation",
        "web_specific_sample",
    }
)
_REVIEW_ITEM_COMBINATIONS = {
    "direct": frozenset(
        {
            ("directly_compatible", "update_existing_behavior"),
            ("directly_compatible", "add_end_to_end_capability"),
        }
    ),
    "adapted": frozenset(
        {
            ("web_specific_code", "implement_hpa_equivalent"),
            ("web_specific_code", "update_existing_behavior"),
            ("cross_product_difference", "implement_hpa_equivalent"),
            ("cross_product_difference", "update_existing_behavior"),
            ("cross_product_difference", "add_end_to_end_capability"),
        }
    ),
    "not_needed": frozenset(
        {
            ("existing_hpa_capability", "reuse_existing_capability"),
            ("existing_hpa_capability", "no_change_needed"),
            ("not_applicable", "no_change_needed"),
        }
    ),
}


class HpaChangeSubmissionState(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


class HpaChangeThreadState(str, Enum):
    NONE = "none"
    NEEDS_CLARIFICATION = "needs_clarification"
    ACTIVE = "active"
    TERMINAL = "terminal"
    ERROR = "error"


@dataclass(frozen=True, slots=True, repr=False)
class HpaChangeSubmissionAttachment:
    name: str
    content: str
    sha256: str = ""


@dataclass(frozen=True, slots=True, repr=False)
class HpaChangeSubmissionRequest:
    """Slack이 수집만 끝낸 뒤 API coordinator에 넘기는 고정 입력 계약이다."""

    request_key: str
    workspace_id: str
    channel_id: str
    thread_ts: str
    thread_url: str
    event_ts: str
    requester_user_id: str
    initiator_user_id: str
    thread_text: str
    attachments: tuple[HpaChangeSubmissionAttachment, ...] = ()
    source_channel_id: str = ""
    source_message_ts: str = ""
    selection_mode: str = "thread"
    response_thread_url: str = ""
    continuation_of_request_id: str = ""


@dataclass(frozen=True, slots=True)
class HpaChangeSubmissionResult:
    state: HpaChangeSubmissionState
    request_id: str = ""
    user_message: str = ""


@dataclass(frozen=True, slots=True)
class HpaChangeThreadLookup:
    state: HpaChangeThreadState
    request_id: str = ""
    job_status: str = ""
    event_ts: str = ""
    current_event: bool = False


@dataclass(frozen=True, slots=True, repr=False)
class HpaChangeDelivery:
    """API 상태를 전진시키지 않고 Slack gateway가 전달할 exact snapshot이다."""

    delivery_id: str
    task_id: str
    workspace_id: str
    channel_id: str
    thread_ts: str
    state: HpaChangePollState
    workflow_phase: str
    result: Mapping[str, Any] = field(default_factory=dict)
    pr_urls: tuple[str, ...] = ()
    request_text: str = ""
    attachment_names: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class HpaChangeDeliveryAck:
    delivery_id: str
    task_id: str
    workspace_id: str
    state: HpaChangePollState


@dataclass(frozen=True, slots=True)
class HpaChangeDeliveryAckResult:
    acknowledged: bool
    task_id: str
    job_status: str
    implementation_dispatch_started: bool = False


@dataclass(frozen=True, slots=True)
class HpaChangeCoordinatorConfig:
    enabled: bool
    allowed_channel_ids: frozenset[str]
    poll_interval_sec: int
    run_timeout_sec: int
    max_thread_chars: int
    max_attachment_count: int
    max_attachment_bytes: int
    max_total_attachment_bytes: int


class HpaChangeCoordinator:
    """GitHub credential, SQLite와 상태 전이를 API 쪽에서만 소유한다."""

    def __init__(
        self,
        config: HpaChangeCoordinatorConfig,
        *,
        store: HpaChangeJobStore | None = None,
        workflow: HpaChangeWorkflowService | None = None,
        auth_mode: str = "disabled",
        logger: logging.Logger | None = None,
    ) -> None:
        if config.enabled and (store is None or workflow is None):
            raise ValueError("활성 HPA coordinator에는 store와 workflow가 필요해")
        self.config = config
        self.store = store
        self.workflow = workflow
        self.auth_mode = str(auth_mode or "disabled")
        self.logger = logger or logging.getLogger(__name__)

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def close(self) -> None:
        if self.store is not None:
            self.store.close()

    def lookup_thread_job(
        self,
        workspace_id: str,
        channel_id: str,
        thread_ts: str,
        event_ts: str,
    ) -> HpaChangeThreadLookup:
        if not self.enabled or self.store is None:
            return HpaChangeThreadLookup(HpaChangeThreadState.NONE)
        try:
            existing = self.store.get_job_by_event_ts(workspace_id, event_ts)
            if existing is not None and (
                existing.channel_id != channel_id or existing.thread_ts != thread_ts
            ):
                return HpaChangeThreadLookup(HpaChangeThreadState.ERROR)
            job = existing or self.store.get_latest_job_by_thread(
                workspace_id,
                channel_id,
                thread_ts,
            )
        except Exception as exc:
            self.logger.warning(
                "Failed to look up HPA coordinator job error_type=%s",
                type(exc).__name__,
            )
            return HpaChangeThreadLookup(HpaChangeThreadState.ERROR)
        if job is None:
            return HpaChangeThreadLookup(HpaChangeThreadState.NONE)
        if job.status is HpaChangeStatus.NEEDS_CLARIFICATION:
            state = HpaChangeThreadState.NEEDS_CLARIFICATION
        elif job.status in {
            HpaChangeStatus.RECEIVED,
            HpaChangeStatus.DISPATCHING,
            HpaChangeStatus.DISPATCHED,
            HpaChangeStatus.RUNNING,
            HpaChangeStatus.WORKFLOW_SUCCEEDED,
            HpaChangeStatus.RESULT_READY,
            HpaChangeStatus.REVIEW_READY,
            HpaChangeStatus.REVIEW_POSTED,
        }:
            state = HpaChangeThreadState.ACTIVE
        else:
            state = HpaChangeThreadState.TERMINAL
        return HpaChangeThreadLookup(
            state=state,
            request_id=job.task_id,
            job_status=job.status.value,
            event_ts=job.event_ts,
            current_event=existing is not None,
        )

    def submit(self, request: HpaChangeSubmissionRequest) -> HpaChangeSubmissionResult:
        if not self.enabled or self.store is None or self.workflow is None:
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.REJECTED,
                user_message="HPA 코드 변경 작업 큐가 활성화되지 않았어",
            )
        policy_error = self._validate_submission_policy(request)
        if policy_error:
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.REJECTED,
                user_message=policy_error,
            )
        workflow_request = self._build_workflow_request(request)
        if isinstance(workflow_request, HpaChangeSubmissionResult):
            return workflow_request
        try:
            if request.continuation_of_request_id:
                job, created = self.workflow.submit_continuation(
                    workflow_request,
                    parent_task_id=request.continuation_of_request_id,
                )
            else:
                job, created = self.workflow.submit(workflow_request)
        except InvalidHpaChangeContinuation:
            latest = self.store.get_latest_job_by_thread(
                request.workspace_id,
                request.channel_id,
                request.thread_ts,
            )
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.REJECTED,
                request_id=latest.task_id if latest is not None else "",
                user_message="이 스레드의 추가 답변은 다른 HPA 작업이 먼저 이어받았어",
            )
        except Exception as exc:
            # GitHub 응답과 credential은 기록하지 않고 안전한 타입만 남긴다.
            self.logger.warning(
                "Failed to submit HPA coordinator job error_type=%s",
                type(exc).__name__,
            )
            failed = self.store.get_job_by_event_ts(
                request.workspace_id,
                request.event_ts,
            )
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.REJECTED,
                request_id=failed.task_id if failed is not None else "",
                user_message="격리 worker에 작업을 전달하지 못했어. 운영 설정을 확인해줘",
            )
        if created:
            prefix = (
                "추가 답변을 기존 요청과 합쳐 새 격리 worker에 전달했어. "
                if request.continuation_of_request_id
                else "격리 worker에 전달했어. "
            )
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.ACCEPTED,
                request_id=job.task_id,
                user_message=prefix + "진행 상황과 PR은 이 스레드에 알릴게",
            )
        return HpaChangeSubmissionResult(
            HpaChangeSubmissionState.DUPLICATE,
            request_id=job.task_id,
            user_message="기존 작업의 진행 상황을 이 스레드에서 계속 확인할게",
        )

    def tick(
        self,
        *,
        now: datetime | None = None,
        limit: int = 500,
    ) -> tuple[HpaChangeDelivery, ...]:
        """API coordinator loop가 GitHub 상태를 전진시키고 pending 전달만 반환한다."""

        if not self.enabled or self.store is None or self.workflow is None:
            return ()
        actual_now = _coerce_utc(now or datetime.now(timezone.utc))
        deliveries: list[HpaChangeDelivery] = []
        for listed_job in self.store.list_reportable_jobs(limit=limit):
            try:
                current_state = _poll_state(listed_job)
                # ACK 전에는 같은 job을 더 전진시키지 않아 pull snapshot을 안정적으로 유지한다.
                if (
                    current_state is not HpaChangePollState.QUEUED
                    and listed_job.notified_status != current_state.value
                ):
                    if (
                        current_state is HpaChangePollState.REVIEW_READY
                        and not validate_hpa_change_review_result(listed_job.result)
                    ):
                        listed_job = self.store.mark_failed(
                            listed_job.task_id,
                            "HPA 공개 검토 계약 또는 초기 요청 커버리지 검증 실패",
                        )
                        current_state = HpaChangePollState.FAILED
                    deliveries.append(self._delivery_for_job(listed_job, current_state))
                    continue
                if _is_timed_out(listed_job, self.config.run_timeout_sec, actual_now):
                    self.store.mark_failed(
                        listed_job.task_id,
                        "HPA 변경 worker 실행 제한 시간 초과",
                    )
                poll = self.workflow.poll_job(listed_job.task_id)
                if poll.state is HpaChangePollState.QUEUED:
                    continue
                if poll.job.notified_status == poll.state.value:
                    continue
                if (
                    poll.state is HpaChangePollState.REVIEW_READY
                    and not validate_hpa_change_review_result(poll.result)
                ):
                    failed = self.store.mark_failed(
                        poll.task_id,
                        "HPA 공개 검토 계약 또는 초기 요청 커버리지 검증 실패",
                    )
                    deliveries.append(
                        self._delivery_for_job(failed, HpaChangePollState.FAILED)
                    )
                    continue
                deliveries.append(self._delivery_for_job(poll.job, poll.state))
            except Exception as exc:
                self.logger.warning(
                    "Failed to advance HPA coordinator task_id=%s error_type=%s",
                    listed_job.task_id,
                    type(exc).__name__,
                )
        return tuple(deliveries)

    def pull_pending(
        self,
        *,
        workspace_id: str,
        limit: int = 100,
    ) -> tuple[HpaChangeDelivery, ...]:
        """Slack pull은 외부 조회나 상태 전이를 일으키지 않는다."""

        if not self.enabled or self.store is None:
            return ()
        normalized_workspace_id = str(workspace_id or "").strip()
        deliveries: list[HpaChangeDelivery] = []
        for job in self.store.list_reportable_jobs(limit=max(1, min(500, limit))):
            if job.workspace_id != normalized_workspace_id:
                continue
            state = _poll_state(job)
            if state is HpaChangePollState.QUEUED or job.notified_status == state.value:
                continue
            deliveries.append(self._delivery_for_job(job, state))
        return tuple(deliveries)

    def acknowledge_delivery(
        self,
        ack: HpaChangeDeliveryAck,
    ) -> HpaChangeDeliveryAckResult:
        """Slack 발송 성공 뒤 exact snapshot만 ACK하고 구현 dispatch는 API에서 시작한다."""

        if not self.enabled or self.store is None or self.workflow is None:
            raise RuntimeError("HPA coordinator가 활성화되지 않았어")
        job = self.store.get_job(ack.task_id)
        if job.workspace_id != str(ack.workspace_id or "").strip():
            raise ValueError("HPA delivery workspace가 작업과 달라")
        existing_receipt = self.store.get_delivery_receipt(job.task_id, ack.state)
        if existing_receipt is not None:
            if existing_receipt != ack.delivery_id:
                raise ValueError("HPA delivery ACK가 기존 exact receipt와 달라")
            # ACK 응답 유실 뒤 같은 exact request가 오면 외부 dispatch를 반복하지 않는다.
            return HpaChangeDeliveryAckResult(
                acknowledged=True,
                task_id=job.task_id,
                job_status=job.status.value,
                implementation_dispatch_started=(
                    ack.state is HpaChangePollState.REVIEW_READY
                    and job.workflow_phase == "implementation"
                ),
            )
        expected = self._delivery_for_job(job, ack.state)
        if expected.delivery_id != ack.delivery_id or _poll_state(job) is not ack.state:
            raise ValueError("HPA delivery ACK가 현재 pending snapshot과 달라")
        self.store.mark_delivery_notified(
            job.task_id,
            ack.state,
            ack.delivery_id,
            # 검토 게시 ACK와 구현 가능 상태를 SQLite transaction 하나로
            # 묶어 ACK 직후 재시작도 companion poll이 이어받게 한다.
            advance_review_posted=(
                ack.state is HpaChangePollState.REVIEW_READY
            ),
        )
        if ack.state is not HpaChangePollState.REVIEW_READY:
            current = self.store.get_job(job.task_id)
            return HpaChangeDeliveryAckResult(
                acknowledged=True,
                task_id=current.task_id,
                job_status=current.status.value,
            )

        # 위 transaction이 Slack 게시와 REVIEW_POSTED를 함께 확정한 뒤에만
        # implementation workflow를 dispatch한다.
        dispatch_started = False
        try:
            current = self.workflow.dispatch_implementation(job.task_id)
            dispatch_started = current.workflow_phase == "implementation"
        except Exception as exc:
            # review 전달 ACK는 이미 확정됐고 workflow 실패는 별도 failure delivery로 알린다.
            self.logger.warning(
                "Failed to dispatch HPA implementation task_id=%s error_type=%s",
                job.task_id,
                type(exc).__name__,
            )
            current = self.store.get_job(job.task_id)
        return HpaChangeDeliveryAckResult(
            acknowledged=True,
            task_id=current.task_id,
            job_status=current.status.value,
            implementation_dispatch_started=dispatch_started,
        )

    def _validate_submission_policy(
        self,
        request: HpaChangeSubmissionRequest,
    ) -> str:
        requester = str(request.requester_user_id or "").strip()
        initiator = str(request.initiator_user_id or "").strip()
        source_channel = str(request.source_channel_id or "").strip()
        if (
            not requester
            or not initiator
            or requester != initiator
            or not source_channel
            or {request.channel_id, source_channel} - self.config.allowed_channel_ids
        ):
            return "HPA 코드 변경 요청의 사용자·채널 정책을 충족하지 않아"
        if len(request.thread_text) > self.config.max_thread_chars:
            return "검토할 Slack 스레드 내용이 허용 길이를 초과했어"
        if len(request.attachments) > self.config.max_attachment_count:
            return "코드 첨부 파일 개수가 제한을 초과했어"
        sizes = [len(item.content.encode("utf-8")) for item in request.attachments]
        if any(size > self.config.max_attachment_bytes for size in sizes):
            return "코드 첨부 파일 하나의 허용 크기를 초과했어"
        if sum(sizes) > self.config.max_total_attachment_bytes:
            return "코드 첨부 파일 전체 허용 크기를 초과했어"
        return ""

    def _build_workflow_request(
        self,
        request: HpaChangeSubmissionRequest,
    ) -> HpaChangeRequest | HpaChangeSubmissionResult:
        assert self.store is not None
        current_attachments = tuple(_workflow_attachment(item) for item in request.attachments)
        parent_id = str(request.continuation_of_request_id or "").strip()
        actor_id = str(request.requester_user_id or "").strip()
        if not parent_id:
            return HpaChangeRequest(
                workspace_id=request.workspace_id,
                event_ts=request.event_ts,
                channel_id=request.channel_id,
                thread_ts=request.thread_ts,
                requested_by=actor_id,
                request_text=request.thread_text,
                thread_url=request.thread_url,
                attachments=current_attachments,
                metadata=_submission_metadata(request),
            )
        existing = self.store.get_job_by_event_ts(request.workspace_id, request.event_ts)
        if existing is not None:
            if (
                str(existing.metadata.get("continuation_of_request_id") or "") == parent_id
                and existing.channel_id == request.channel_id
                and existing.thread_ts == request.thread_ts
                and existing.requested_by == actor_id
            ):
                return HpaChangeSubmissionResult(
                    HpaChangeSubmissionState.DUPLICATE,
                    request_id=existing.task_id,
                    user_message="기존 추가 답변 작업의 진행 상황을 계속 확인할게",
                )
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.REJECTED,
                request_id=existing.task_id,
                user_message="같은 Slack 이벤트가 다른 HPA 작업에 이미 사용됐어",
            )
        try:
            parent = self.store.get_job(parent_id)
        except KeyError:
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.REJECTED,
                user_message="이어갈 HPA 추가 확인 작업을 찾지 못했어",
            )
        if (
            parent.status is not HpaChangeStatus.NEEDS_CLARIFICATION
            or parent.workspace_id != request.workspace_id
            or parent.channel_id != request.channel_id
            or parent.thread_ts != request.thread_ts
            or parent.requested_by != actor_id
            or str(parent.metadata.get("initiator_user_id") or "") != actor_id
        ):
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.REJECTED,
                user_message="이 스레드의 최신 HPA 작업은 추가 답변을 기다리는 상태가 아니야",
            )
        combined_text = "\n\n".join(
            (
                "[기존 HPA 변경 요청]\n" + parent.request_text,
                "[이전 작업 접수 이후 추가 답변과 진행 명령]\n" + request.thread_text,
            )
        )
        if len(combined_text) > self.config.max_thread_chars:
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.REJECTED,
                user_message="기존 요청과 추가 답변을 합친 내용이 허용 길이를 초과했어",
            )
        merged = list(parent.attachments)
        by_name = {item.name: item for item in parent.attachments}
        for attachment in current_attachments:
            previous = by_name.get(attachment.name)
            if previous is not None:
                if previous.sha256 == attachment.sha256:
                    continue
                return HpaChangeSubmissionResult(
                    HpaChangeSubmissionState.REJECTED,
                    user_message=f"같은 이름의 첨부 내용이 달라 확인이 필요해: {attachment.name}",
                )
            by_name[attachment.name] = attachment
            merged.append(attachment)
        total_bytes = sum(len(item.content.encode("utf-8")) for item in merged)
        if (
            len(merged) > self.config.max_attachment_count
            or total_bytes > self.config.max_total_attachment_bytes
        ):
            return HpaChangeSubmissionResult(
                HpaChangeSubmissionState.REJECTED,
                user_message="기존 요청과 추가 답변의 첨부 제한을 초과했어",
            )
        metadata = dict(parent.metadata)
        metadata.update(_submission_metadata(request))
        metadata["continuation_of_request_id"] = parent.task_id
        metadata["continuation_event_ts"] = request.event_ts
        return HpaChangeRequest(
            workspace_id=request.workspace_id,
            event_ts=request.event_ts,
            channel_id=request.channel_id,
            thread_ts=request.thread_ts,
            requested_by=actor_id,
            request_text=combined_text,
            thread_url=parent.thread_url,
            attachments=tuple(merged),
            metadata=metadata,
        )

    @staticmethod
    def _delivery_for_job(
        job: HpaChangeJob,
        state: HpaChangePollState,
    ) -> HpaChangeDelivery:
        fingerprint_payload = {
            "taskId": job.task_id,
            "state": state.value,
            "phase": job.workflow_phase,
            "runId": job.workflow_run_id,
            "artifactId": job.artifact_id,
            "result": job.result,
            "prUrls": list(job.pr_urls),
        }
        digest = hashlib.sha256(
            json.dumps(
                fingerprint_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return HpaChangeDelivery(
            delivery_id=f"hpa-delivery:{digest}",
            task_id=job.task_id,
            workspace_id=job.workspace_id,
            channel_id=job.channel_id,
            thread_ts=job.thread_ts,
            state=state,
            workflow_phase=job.workflow_phase,
            result=dict(job.result),
            pr_urls=job.pr_urls,
            request_text=job.request_text,
            attachment_names=tuple(item.name for item in job.attachments),
        )


def validate_hpa_change_review_result(result: Mapping[str, Any]) -> bool:
    """구현 dispatch 전에 worker 공개 검토 계약을 API 경계에서 재검사한다."""

    quality_gates = result.get("qualityGates") or result.get("quality_gates")
    if (
        not isinstance(quality_gates, Mapping)
        or quality_gates.get("initialRequestCoveragePassed") is not True
    ):
        return False
    review = result.get("review")
    containers = (result, review) if isinstance(review, Mapping) else (result,)
    report: Mapping[str, Any] = {}
    for container in containers:
        candidate = (
            container.get("requesterView")
            or container.get("requester_view")
            or container.get("publicReport")
            or container.get("public_report")
        )
        if isinstance(candidate, Mapping):
            report = candidate
            break
    if str(report.get("summaryCode") or "") not in _REVIEW_SUMMARY_CODES:
        return False
    wrong = report.get("wrongAssumptions") or []
    if not isinstance(wrong, (list, tuple)) or len(wrong) > 5:
        return False
    if any(
        not isinstance(item, Mapping)
        or str(item.get("explanationCode") or "") not in _WRONG_ASSUMPTION_CODES
        for item in wrong
    ):
        return False
    reason_codes = report.get("whyNotDirectCodes") or []
    if (
        not isinstance(reason_codes, (list, tuple))
        or not reason_codes
        or len(reason_codes) > 5
        or any(str(code or "") not in _INCOMPATIBILITY_CODES for code in reason_codes)
    ):
        return False
    items = report.get("requestItems") or []
    if not isinstance(items, (list, tuple)) or not 1 <= len(items) <= 10:
        return False
    for index, item in enumerate(items, 1):
        if not isinstance(item, Mapping):
            return False
        handling = str(item.get("handling") or "")
        combination = (
            str(item.get("reasonCode") or ""),
            str(item.get("applicationCode") or ""),
        )
        if (
            str(item.get("itemId") or "") != f"REQ-{index:02d}"
            or combination not in _REVIEW_ITEM_COMBINATIONS.get(handling, frozenset())
        ):
            return False
    return True


def create_hpa_change_coordinator(
    *,
    settings: Any = company_settings,
    session: Any | None = None,
    logger: logging.Logger | None = None,
) -> HpaChangeCoordinator:
    """API process가 고정 env를 검증한 뒤 credential 포함 coordinator를 조립한다."""

    enabled = bool(_setting(settings, "HPA_CHANGE_REQUEST_ENABLED", False))
    allowed_channels = _normalized_ids(
        _setting(settings, "HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS", ())
    )
    config = HpaChangeCoordinatorConfig(
        enabled=enabled,
        allowed_channel_ids=allowed_channels,
        poll_interval_sec=max(1, int(_setting(settings, "HPA_CHANGE_POLL_INTERVAL_SEC", 20))),
        run_timeout_sec=max(1, int(_setting(settings, "HPA_CHANGE_RUN_TIMEOUT_SEC", 10_800))),
        max_thread_chars=max(0, int(_setting(settings, "HPA_CHANGE_MAX_THREAD_CHARS", 30_000))),
        max_attachment_count=max(0, int(_setting(settings, "HPA_CHANGE_MAX_FILES", 5))),
        max_attachment_bytes=max(0, int(_setting(settings, "HPA_CHANGE_MAX_FILE_BYTES", 131_072))),
        max_total_attachment_bytes=max(
            0,
            int(_setting(settings, "HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES", 524_288)),
        ),
    )
    if not enabled:
        return HpaChangeCoordinator(config, logger=logger)
    if allowed_channels != HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS:
        raise ValueError("HPA 코드 변경 요청 allowlist가 회사 고정 채널 정책과 달라")
    for value_name, value in (
        ("HPA_CHANGE_POLL_INTERVAL_SEC", config.poll_interval_sec),
        ("HPA_CHANGE_RUN_TIMEOUT_SEC", config.run_timeout_sec),
        ("HPA_CHANGE_MAX_THREAD_CHARS", config.max_thread_chars),
        ("HPA_CHANGE_MAX_FILES", config.max_attachment_count),
        ("HPA_CHANGE_MAX_FILE_BYTES", config.max_attachment_bytes),
        ("HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES", config.max_total_attachment_bytes),
    ):
        if value <= 0:
            raise ValueError(f"{value_name} 설정은 양의 정수여야 해")
    if config.max_total_attachment_bytes < config.max_attachment_bytes:
        raise ValueError("전체 첨부 한도는 파일별 한도 이상이어야 해")
    owner, repository = _parse_repository(
        _setting(settings, "HPA_CHANGE_GITHUB_COORDINATOR_REPOSITORY", "")
    )
    workflow_id = str(_setting(settings, "HPA_CHANGE_GITHUB_WORKFLOW_FILE", "") or "").strip()
    api_url = _validate_api_url(
        _setting(settings, "HPA_CHANGE_GITHUB_API_URL", "https://api.github.com")
    )
    token_provider, auth_mode = _build_token_provider(
        settings,
        # GitHub installation-token API의 repositories 필드는 owner/repo가
        # 아니라 installation 안의 repository name만 받는다.
        repository=repository,
        api_url=api_url,
        session=session,
    )
    db_path = _validate_hpa_job_db_path(
        _setting(settings, "HPA_CHANGE_JOB_DB_PATH", "")
    )
    store = HpaChangeJobStore(db_path)
    # SQLite/WAL은 systemd UMask=0077과 함께 쓰고, main DB도 생성 직후
    # owner-only로 고정해 Slack thread·첨부 원문을 다른 계정에 노출하지 않는다.
    os.chmod(db_path, 0o600)
    github = GitHubCoordinatorClient(
        GitHubCoordinatorConfig(
            owner=owner,
            repository=repository,
            workflow_id=workflow_id,
            event_type=_GITHUB_EVENT_TYPE,
            workflow_run_name_prefix=_GITHUB_RUN_NAME_PREFIX,
            implementation_workflow_run_name_prefix=(
                _GITHUB_IMPLEMENTATION_RUN_NAME_PREFIX
            ),
            api_base_url=api_url,
        ),
        token_provider,
        session=session,
    )
    return HpaChangeCoordinator(
        config,
        store=store,
        workflow=HpaChangeWorkflowService(store, github),
        auth_mode=auth_mode,
        logger=logger,
    )


def _workflow_attachment(item: HpaChangeSubmissionAttachment) -> HpaChangeAttachment:
    content = str(item.content or "")
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    declared = str(item.sha256 or "").strip().lower()
    if declared and declared != digest:
        raise ValueError("HPA 첨부 sha256이 실제 내용과 달라")
    return HpaChangeAttachment(name=str(item.name or "").strip(), content=content, sha256=digest)


def _submission_metadata(request: HpaChangeSubmissionRequest) -> dict[str, str]:
    return {
        "source": "slack",
        "request_key": str(request.request_key or "").strip(),
        "initiator_user_id": str(request.initiator_user_id or "").strip(),
        "source_channel_id": str(request.source_channel_id or request.channel_id).strip(),
        "source_message_ts": str(request.source_message_ts or request.thread_ts).strip(),
        "selection_mode": str(request.selection_mode or "thread").strip(),
        "response_thread_url": str(request.response_thread_url or "").strip(),
    }


def _poll_state(job: HpaChangeJob) -> HpaChangePollState:
    if job.status is HpaChangeStatus.REVIEW_READY:
        return HpaChangePollState.REVIEW_READY
    if job.status is HpaChangeStatus.NEEDS_CLARIFICATION:
        return HpaChangePollState.NEEDS_CLARIFICATION
    if job.status is HpaChangeStatus.PR_CREATED:
        return HpaChangePollState.PR_OPENED
    if job.status is HpaChangeStatus.NO_CHANGE_NEEDED:
        return HpaChangePollState.NO_CHANGE_NEEDED
    if job.status in {HpaChangeStatus.FAILED, HpaChangeStatus.CANCELED}:
        return HpaChangePollState.FAILED
    if job.status in {
        HpaChangeStatus.RUNNING,
        HpaChangeStatus.WORKFLOW_SUCCEEDED,
        HpaChangeStatus.RESULT_READY,
    }:
        return HpaChangePollState.RUNNING
    if job.status is HpaChangeStatus.REVIEW_POSTED:
        # review ACK는 이미 전달된 review_ready 상태다. 이 값을 유지해야
        # companion이 새 running delivery를 만들지 않고 구현 dispatch를 복구한다.
        return HpaChangePollState.REVIEW_READY
    return HpaChangePollState.QUEUED


def _is_timed_out(job: HpaChangeJob, timeout_sec: int, now: datetime) -> bool:
    if job.status not in _ACTIVE_STATUSES:
        return False
    if job.status is HpaChangeStatus.RESULT_READY and isinstance(job.result, Mapping):
        raw_status = str(job.result.get("status") or "").strip().lower().replace("-", "_")
        recoverable = raw_status in _RECOVERABLE_RESULT_STATUSES or (
            raw_status in {"completed", "success"}
            and any(bool(job.result.get(key)) for key in ("prs", "pr_urls", "pull_requests"))
        )
        if recoverable:
            return False
    return (now - _coerce_utc(job.phase_started_at)).total_seconds() > max(1, timeout_sec)


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _setting(settings: Any, name: str, default: Any = None) -> Any:
    return getattr(settings, name, default)


def _normalized_ids(value: Any) -> frozenset[str]:
    candidates = value.split(",") if isinstance(value, str) else (value or ())
    return frozenset(str(item or "").strip() for item in candidates if str(item or "").strip())


def _parse_repository(value: Any) -> tuple[str, str]:
    parts = str(value or "").strip().split("/")
    if len(parts) != 2 or not all(parts):
        raise ValueError("HPA coordinator repository는 owner/repository 형식이어야 해")
    return parts[0], parts[1]


def _validate_api_url(value: Any) -> str:
    api_url = str(value or "").strip().rstrip("/")
    try:
        parsed = urlsplit(api_url)
        port = parsed.port
    except ValueError:
        raise ValueError("HPA GitHub API URL 형식이 올바르지 않아") from None
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("HPA GitHub API URL은 credential 없는 HTTPS URL이어야 해")
    return api_url


def _read_private_key(path_value: Any) -> str:
    raw_path = str(path_value or "").strip()
    if not raw_path:
        raise ValueError("HPA GitHub App private key 경로가 없어")
    try:
        path = Path(raw_path).expanduser().resolve(strict=True)
        if not path.is_file() or path.stat().st_size > 64 * 1024:
            raise ValueError("HPA GitHub App private key 파일이 올바르지 않아")
        pem = path.read_text(encoding="utf-8")
        key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
    except (OSError, RuntimeError, UnicodeError, TypeError, ValueError):
        raise ValueError("HPA GitHub App private key를 안전하게 읽지 못했어") from None
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("HPA GitHub App private key는 RSA key여야 해")
    return pem


def _validate_hpa_job_db_path(path_value: Any) -> str:
    """API StateDirectory 아래의 명시적 owner-only SQLite만 허용한다."""

    raw_path = str(path_value or "").strip()
    path = Path(raw_path).expanduser()
    if not raw_path or not path.is_absolute() or path == Path("/"):
        raise ValueError("HPA_CHANGE_JOB_DB_PATH는 절대 파일 경로여야 해")
    parent = path.parent
    try:
        parent_stat = parent.lstat()
    except OSError:
        raise ValueError("HPA_CHANGE_JOB_DB_PATH 상위 경로가 없어") from None
    if (
        stat.S_ISLNK(parent_stat.st_mode)
        or not stat.S_ISDIR(parent_stat.st_mode)
        or parent_stat.st_uid not in {0, os.geteuid()}
        or parent_stat.st_mode & 0o022
    ):
        raise ValueError("HPA_CHANGE_JOB_DB_PATH 상위 경로가 안전하지 않아")
    try:
        file_stat = path.lstat()
    except FileNotFoundError:
        return str(path)
    except OSError:
        raise ValueError("HPA_CHANGE_JOB_DB_PATH를 확인하지 못했어") from None
    if (
        stat.S_ISLNK(file_stat.st_mode)
        or not stat.S_ISREG(file_stat.st_mode)
        or file_stat.st_uid not in {0, os.geteuid()}
        or stat.S_IMODE(file_stat.st_mode) != 0o600
    ):
        raise ValueError("HPA_CHANGE_JOB_DB_PATH 파일이 안전하지 않아")
    return str(path)


def _build_token_provider(
    settings: Any,
    *,
    repository: str,
    api_url: str,
    session: Any | None,
) -> tuple[Any, str]:
    app_id = str(_setting(settings, "HPA_CHANGE_GITHUB_APP_ID", "") or "").strip()
    installation_id = str(
        _setting(settings, "HPA_CHANGE_GITHUB_APP_INSTALLATION_ID", "") or ""
    ).strip()
    key_path = str(
        _setting(settings, "HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH", "") or ""
    ).strip()
    app_values = (app_id, installation_id, key_path)
    if any(app_values) and not all(app_values):
        raise ValueError("HPA GitHub App 인증 설정 세 항목을 모두 입력해야 해")
    if all(app_values):
        return (
            GitHubAppTokenProvider(
                app_id=app_id,
                installation_id=installation_id,
                private_key_pem=_read_private_key(key_path),
                session=session,
                api_base_url=api_url,
                restrictions=GitHubAppPermissions(
                    repositories=(repository,),
                    permissions={"actions": "read", "contents": "write"},
                ),
            ),
            "github_app",
        )
    token = str(_setting(settings, "HPA_CHANGE_GITHUB_TOKEN", "") or "").strip()
    if not token:
        raise ValueError("HPA 코드 변경 자동화용 GitHub App 또는 static token 설정이 없어")
    return StaticGitHubTokenProvider(token=token), "static_token"


__all__ = [
    "HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS",
    "HpaChangeCoordinator",
    "HpaChangeCoordinatorConfig",
    "HpaChangeDelivery",
    "HpaChangeDeliveryAck",
    "HpaChangeDeliveryAckResult",
    "HpaChangeSubmissionAttachment",
    "HpaChangeSubmissionRequest",
    "HpaChangeSubmissionResult",
    "HpaChangeSubmissionState",
    "HpaChangeThreadLookup",
    "HpaChangeThreadState",
    "create_hpa_change_coordinator",
    "validate_hpa_change_review_result",
]
