from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from boxer_company import settings as cs
from boxer_company.hpa_change_workflow import (
    GitHubAppPermissions,
    GitHubAppTokenProvider,
    GitHubCoordinatorClient,
    GitHubCoordinatorConfig,
    HpaChangeAttachment,
    HpaChangeJobStore,
    HpaChangeRequest as WorkflowHpaChangeRequest,
    HpaChangeStatus,
    HpaChangeWorkflowService,
    InvalidHpaChangeContinuation,
    StaticGitHubTokenProvider,
)
from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeRequest as RouteHpaChangeRequest,
    HpaChangeRoutesConfig,
    HpaChangeSubmissionResult,
    HpaChangeSubmissionStatus,
    HpaChangeThreadLookupResult,
    HpaChangeThreadLookupState,
)


_GITHUB_EVENT_TYPE = "boxer-hpa-change"
_GITHUB_RUN_NAME_PREFIX = "Boxer HPA Review"
_GITHUB_IMPLEMENTATION_RUN_NAME_PREFIX = "Boxer HPA Implementation"

# HPA CR 자동 PR은 회사가 승인한 두 사람과 두 채널에서만 동작한다.
# 저스틴은 Slack 계정이 두 개라 두 user ID를 동일한 허용 대상으로 관리한다.
# 환경변수는 기능 on/off가 아니라 이 고정 정책과의 일치 여부를 검증하는 용도다.
HPA_CHANGE_POLICY_ALLOWED_USER_IDS = frozenset(
    {"U0629HDSJHG", "U07A5FM5XPD", "U096JA81T6X"}
)
HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS = frozenset(
    {"C02C08K7YEN", "C068FVD5V7Y"}
)


@dataclass
class HpaChangeRuntime:
    """Slack intake와 영속 workflow를 묶는 runtime이야.

    운영 process 안에서 코드 실행 권한은 갖지 않는다.
    """

    enabled: bool
    routes_config: HpaChangeRoutesConfig
    poll_interval_sec: int
    run_timeout_sec: int
    store: HpaChangeJobStore | None = None
    workflow: HpaChangeWorkflowService | None = None
    auth_mode: str = "disabled"
    logger: logging.Logger | None = None

    def lookup_thread_job(
        self,
        workspace_id: str,
        channel_id: str,
        thread_ts: str,
        event_ts: str,
    ) -> HpaChangeThreadLookupResult:
        """후속 명령을 실제 thread 작업 상태에 연결하고 조회 오류는 닫힌 상태로 돌려준다."""

        if not self.enabled or self.store is None:
            return HpaChangeThreadLookupResult(HpaChangeThreadLookupState.NONE)
        normalized_workspace_id = str(workspace_id or "").strip()
        normalized_channel_id = str(channel_id or "").strip()
        normalized_thread_ts = str(thread_ts or "").strip()
        normalized_event_ts = str(event_ts or "").strip()

        try:
            # 동일 event가 이미 등록됐다면 최신 thread 작업보다 먼저 찾아 Slack retry를 복원한다.
            existing = self.store.get_job_by_event_ts(
                normalized_workspace_id,
                normalized_event_ts,
            )
            if existing is not None and (
                existing.channel_id != normalized_channel_id
                or existing.thread_ts != normalized_thread_ts
            ):
                return HpaChangeThreadLookupResult(HpaChangeThreadLookupState.ERROR)
            job = existing or self.store.get_latest_job_by_thread(
                normalized_workspace_id,
                normalized_channel_id,
                normalized_thread_ts,
            )
        except Exception as exc:
            actual_logger = self.logger or logging.getLogger(__name__)
            actual_logger.warning(
                "Failed to look up HPA thread job error_type=%s",
                type(exc).__name__,
            )
            return HpaChangeThreadLookupResult(HpaChangeThreadLookupState.ERROR)

        if job is None:
            return HpaChangeThreadLookupResult(HpaChangeThreadLookupState.NONE)
        if job.status is HpaChangeStatus.NEEDS_CLARIFICATION:
            state = HpaChangeThreadLookupState.NEEDS_CLARIFICATION
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
            state = HpaChangeThreadLookupState.ACTIVE
        else:
            state = HpaChangeThreadLookupState.TERMINAL
        return HpaChangeThreadLookupResult(
            state=state,
            request_id=job.task_id,
            job_status=job.status.value,
            event_ts=job.event_ts,
            current_event=existing is not None,
        )

    @staticmethod
    def _route_attachments(
        request: RouteHpaChangeRequest,
    ) -> tuple[HpaChangeAttachment, ...]:
        return tuple(
            HpaChangeAttachment(
                name=item.name,
                content=item.content,
                sha256=hashlib.sha256(item.content.encode("utf-8")).hexdigest(),
            )
            for item in request.attachments
        )

    def _build_workflow_request(
        self,
        request: RouteHpaChangeRequest,
    ) -> WorkflowHpaChangeRequest | HpaChangeSubmissionResult:
        if self.store is None:
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message="HPA 코드 변경 작업 큐가 활성화되지 않았어",
            )

        continuation_of = str(request.continuation_of_request_id or "").strip()
        current_attachments = self._route_attachments(request)
        if not continuation_of:
            return WorkflowHpaChangeRequest(
                workspace_id=request.workspace_id,
                event_ts=request.event_ts,
                channel_id=request.channel_id,
                thread_ts=request.thread_ts,
                requested_by=request.requester_user_id,
                request_text=request.thread_text,
                thread_url=request.thread_url,
                attachments=current_attachments,
                metadata={
                    "source": "slack",
                    "request_key": request.request_key,
                    "initiator_user_id": request.initiator_user_id
                    or request.requester_user_id,
                    "source_channel_id": request.source_channel_id or request.channel_id,
                    "source_message_ts": request.source_message_ts or request.thread_ts,
                    "selection_mode": request.selection_mode,
                    "response_thread_url": request.response_thread_url,
                },
            )

        existing = self.store.get_job_by_event_ts(
            request.workspace_id,
            request.event_ts,
        )
        if existing is not None:
            existing_parent = str(
                existing.metadata.get("continuation_of_request_id") or ""
            ).strip()
            if (
                existing_parent == continuation_of
                and existing.channel_id == request.channel_id
                and existing.thread_ts == request.thread_ts
            ):
                return HpaChangeSubmissionResult(
                    status=HpaChangeSubmissionStatus.DUPLICATE,
                    request_id=existing.task_id,
                    user_message="기존 추가 답변 작업의 진행 상황을 계속 확인할게",
                )
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                request_id=existing.task_id,
                user_message="같은 Slack 이벤트가 다른 HPA 작업에 이미 사용됐어",
            )

        try:
            parent = self.store.get_job(continuation_of)
        except KeyError:
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message="이어갈 HPA 추가 확인 작업을 찾지 못했어",
            )
        if (
            parent.status is not HpaChangeStatus.NEEDS_CLARIFICATION
            or parent.workspace_id != request.workspace_id
            or parent.channel_id != request.channel_id
            or parent.thread_ts != request.thread_ts
        ):
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message="이 스레드의 최신 HPA 작업은 추가 답변을 기다리는 상태가 아니야",
            )

        parent_source_channel_id = str(
            parent.metadata.get("source_channel_id") or parent.channel_id
        ).strip()
        if (
            parent.requested_by not in HPA_CHANGE_POLICY_ALLOWED_USER_IDS
            or parent.channel_id not in HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS
            or parent_source_channel_id not in HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS
        ):
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message="이전 HPA 요청의 사용자·채널 정책을 확인하지 못했어",
            )

        combined_text = "\n\n".join(
            (
                "[기존 HPA 변경 요청]\n" + parent.request_text,
                "[이전 작업 접수 이후 추가 답변과 진행 명령]\n" + request.thread_text,
            )
        )
        if len(combined_text) > max(0, self.routes_config.max_thread_chars):
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message="기존 요청과 추가 답변을 합친 내용이 허용 길이를 초과했어",
            )

        # 부모 첨부를 먼저 보존하고, 같은 이름·같은 내용은 한 번만 전달한다.
        merged_attachments: list[HpaChangeAttachment] = list(parent.attachments)
        attachment_by_name = {item.name: item for item in parent.attachments}
        for attachment in current_attachments:
            previous = attachment_by_name.get(attachment.name)
            if previous is not None:
                if previous.sha256 == attachment.sha256:
                    continue
                return HpaChangeSubmissionResult(
                    status=HpaChangeSubmissionStatus.REJECTED,
                    user_message=f"같은 이름의 첨부 내용이 달라 확인이 필요해: {attachment.name}",
                )
            attachment_by_name[attachment.name] = attachment
            merged_attachments.append(attachment)
        if len(merged_attachments) > max(0, self.routes_config.max_attachment_count):
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message="기존 요청과 추가 답변의 첨부 개수 합계가 제한을 초과했어",
            )
        total_attachment_bytes = sum(
            len(item.content.encode("utf-8")) for item in merged_attachments
        )
        if total_attachment_bytes > max(
            0,
            self.routes_config.max_total_attachment_bytes,
        ):
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message="기존 요청과 추가 답변의 첨부 크기 합계가 제한을 초과했어",
            )

        # 원 요청자와 선택한 원문 소스는 그대로 두고, 실행자와 응답 thread만 이번 event로 갱신한다.
        metadata = dict(parent.metadata)
        metadata.update(
            {
                "source": "slack",
                "request_key": request.request_key,
                "initiator_user_id": request.initiator_user_id
                or request.requester_user_id,
                "response_thread_url": request.response_thread_url
                or parent.metadata.get("response_thread_url")
                or "",
                "continuation_of_request_id": parent.task_id,
                "continuation_event_ts": request.event_ts,
                "continuation_selection_mode": request.selection_mode,
            }
        )
        return WorkflowHpaChangeRequest(
            workspace_id=request.workspace_id,
            event_ts=request.event_ts,
            channel_id=request.channel_id,
            thread_ts=request.thread_ts,
            requested_by=parent.requested_by,
            request_text=combined_text,
            thread_url=parent.thread_url,
            attachments=tuple(merged_attachments),
            metadata=metadata,
        )

    def submit_request(self, request: RouteHpaChangeRequest) -> HpaChangeSubmissionResult:
        if not self.enabled or self.store is None or self.workflow is None:
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message="HPA 코드 변경 작업 큐가 활성화되지 않았어",
            )

        # Slack route를 우회해 runtime이 직접 호출돼도 요청자·실행자와
        # 응답·원문 채널이 모두 회사 고정 정책 안에 있어야 dispatch한다.
        initiator_user_id = str(request.initiator_user_id or "").strip()
        source_channel_id = str(request.source_channel_id or "").strip()
        if not initiator_user_id or not source_channel_id or {
            request.requester_user_id,
            initiator_user_id,
        } - HPA_CHANGE_POLICY_ALLOWED_USER_IDS or {
            request.channel_id,
            source_channel_id,
        } - HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS:
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message=(
                    "HPA 코드 변경 요청의 사용자·채널 정책을 충족하지 않아"
                ),
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
            # 다른 process가 같은 부모를 먼저 이어받은 경우 새 작업을 중복 생성하지 않는다.
            latest = self.store.get_latest_job_by_thread(
                request.workspace_id,
                request.channel_id,
                request.thread_ts,
            )
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                request_id=latest.task_id if latest is not None else "",
                user_message="이 스레드의 추가 답변은 다른 HPA 작업이 먼저 이어받았어",
            )
        except Exception as exc:
            # GitHub 오류 원문에는 응답이나 credential이 포함될 수 있어 type만 기록한다.
            actual_logger = self.logger or logging.getLogger(__name__)
            actual_logger.warning(
                "Failed to submit HPA change workflow error_type=%s",
                type(exc).__name__,
            )
            failed_job = self.store.get_job_by_event_ts(
                request.workspace_id,
                request.event_ts,
            )
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                request_id=failed_job.task_id if failed_job is not None else "",
                user_message=(
                    "격리 worker에 작업을 전달하지 못했어. "
                    "운영 설정을 확인해줘"
                ),
            )

        if created:
            continuation_message = (
                "추가 답변을 기존 요청과 합쳐 새 격리 worker에 전달했어. "
                if request.continuation_of_request_id
                else "격리 worker에 전달했어. "
            )
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.ACCEPTED,
                request_id=job.task_id,
                user_message=(
                    continuation_message
                    + "진행 상황과 PR은 이 스레드에 알릴게"
                ),
            )
        return HpaChangeSubmissionResult(
            status=HpaChangeSubmissionStatus.DUPLICATE,
            request_id=job.task_id,
            user_message=(
                "기존 작업의 진행 상황을 이 스레드에서 계속 확인할게"
            ),
        )

    def close(self) -> None:
        if self.store is not None:
            self.store.close()


def _setting(settings: Any, name: str, default: Any = None) -> Any:
    return getattr(settings, name, default)


def _positive_int(settings: Any, name: str) -> int:
    try:
        value = int(_setting(settings, name, 0))
    except (TypeError, ValueError):
        raise ValueError(f"{name} 설정은 양의 정수여야 해") from None
    if value <= 0:
        raise ValueError(f"{name} 설정은 양의 정수여야 해")
    return value


def _normalized_ids(value: Any) -> frozenset[str]:
    if isinstance(value, str):
        candidates = value.split(",")
    else:
        candidates = value or ()
    return frozenset(
        str(item or "").strip()
        for item in candidates
        if str(item or "").strip()
    )


def _parse_repository(value: Any) -> tuple[str, str]:
    repository = str(value or "").strip()
    parts = repository.split("/")
    if len(parts) != 2 or not all(parts):
        raise ValueError(
            "HPA_CHANGE_GITHUB_COORDINATOR_REPOSITORY는 "
            "owner/repository 형식이어야 해"
        )
    return parts[0], parts[1]


def _validate_api_url(value: Any) -> str:
    api_url = str(value or "").strip().rstrip("/")
    try:
        parsed = urlsplit(api_url)
        port = parsed.port
    except ValueError:
        raise ValueError("HPA_CHANGE_GITHUB_API_URL 형식이 올바르지 않아") from None
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("HPA_CHANGE_GITHUB_API_URL은 credential 없는 HTTPS URL이어야 해")
    return api_url


def _read_github_app_private_key(path_value: Any) -> str:
    raw_path = str(path_value or "").strip()
    if not raw_path:
        raise ValueError("HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH가 없어")
    try:
        path = Path(raw_path).expanduser().resolve(strict=True)
    except (OSError, RuntimeError):
        raise ValueError("GitHub App private key 경로를 찾지 못했어") from None
    if not path.is_file():
        raise ValueError("GitHub App private key 경로가 파일이 아니야")
    try:
        key_size = path.stat().st_size
        private_key_pem = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise ValueError("GitHub App private key 파일을 읽지 못했어") from None
    if key_size > 64 * 1024:
        raise ValueError("GitHub App private key 파일이 허용 크기를 넘었어")
    try:
        private_key = serialization.load_pem_private_key(
            private_key_pem.encode("utf-8"),
            password=None,
        )
    except (TypeError, ValueError):
        raise ValueError("GitHub App private key 형식이 올바르지 않아") from None
    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise ValueError("GitHub App private key는 RSA key여야 해")
    return private_key_pem


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
    private_key_path = str(
        _setting(settings, "HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH", "") or ""
    ).strip()
    app_values = (app_id, installation_id, private_key_path)

    # App 설정 일부만 누락된 상태에서
    # static token으로 조용히 우회하지 않는다.
    # 세 값이 모두 있을 때 App을 우선하고,
    # 모두 없을 때만 token fallback을 쓴다.
    if any(app_values) and not all(app_values):
        raise ValueError("GitHub App 인증 설정 세 항목을 모두 입력해야 해")
    if all(app_values):
        private_key_pem = _read_github_app_private_key(private_key_path)
        return (
            GitHubAppTokenProvider(
                app_id=app_id,
                installation_id=installation_id,
                private_key_pem=private_key_pem,
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
        raise ValueError(
            "HPA 코드 변경 자동화용 GitHub App 또는 static token 설정이 없어"
        )
    return StaticGitHubTokenProvider(token=token), "static_token"


def create_hpa_change_runtime(
    *,
    settings: Any = cs,
    session: Any | None = None,
    logger: logging.Logger | None = None,
) -> HpaChangeRuntime:
    """환경 설정을 한 번 검증하고 고정 coordinator용 runtime을 만든다."""

    enabled = bool(_setting(settings, "HPA_CHANGE_REQUEST_ENABLED", False))
    allowed_user_ids = _normalized_ids(
        _setting(settings, "HPA_CHANGE_REQUEST_ALLOWED_USER_IDS", ())
    )
    allowed_channel_ids = _normalized_ids(
        _setting(settings, "HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS", ())
    )
    routes_config = HpaChangeRoutesConfig(
        enabled=enabled,
        allowed_user_ids=allowed_user_ids,
        allowed_channel_ids=allowed_channel_ids,
        max_thread_chars=max(
            0,
            int(_setting(settings, "HPA_CHANGE_MAX_THREAD_CHARS", 30_000) or 0),
        ),
        max_attachment_count=max(
            0,
            int(_setting(settings, "HPA_CHANGE_MAX_FILES", 5) or 0),
        ),
        max_attachment_bytes=max(
            0,
            int(_setting(settings, "HPA_CHANGE_MAX_FILE_BYTES", 131_072) or 0),
        ),
        max_total_attachment_bytes=max(
            0,
            int(_setting(settings, "HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES", 524_288) or 0),
        ),
    )
    if not enabled:
        return HpaChangeRuntime(
            enabled=False,
            routes_config=routes_config,
            poll_interval_sec=max(
                1,
                int(_setting(settings, "HPA_CHANGE_POLL_INTERVAL_SEC", 20) or 20),
            ),
            run_timeout_sec=max(
                1,
                int(_setting(settings, "HPA_CHANGE_RUN_TIMEOUT_SEC", 10_800) or 10_800),
            ),
            logger=logger,
        )

    # 활성화 시 비어 있는 allowlist와 비정상 한도는
    # 전체 허용이나 무제한으로 해석하지 않는다.
    if not allowed_user_ids or not allowed_channel_ids:
        raise ValueError(
            "HPA 코드 변경 요청의 사용자·채널 allowlist가 모두 필요해"
        )
    if (
        allowed_user_ids != HPA_CHANGE_POLICY_ALLOWED_USER_IDS
        or allowed_channel_ids != HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS
    ):
        raise ValueError(
            "HPA 코드 변경 요청 allowlist가 회사 고정 사용자·채널 정책과 달라"
        )
    poll_interval_sec = _positive_int(settings, "HPA_CHANGE_POLL_INTERVAL_SEC")
    run_timeout_sec = _positive_int(settings, "HPA_CHANGE_RUN_TIMEOUT_SEC")
    for setting_name in (
        "HPA_CHANGE_MAX_THREAD_CHARS",
        "HPA_CHANGE_MAX_FILES",
        "HPA_CHANGE_MAX_FILE_BYTES",
        "HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES",
    ):
        _positive_int(settings, setting_name)
    if routes_config.max_total_attachment_bytes < routes_config.max_attachment_bytes:
        raise ValueError(
            "HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES는 파일별 한도 이상이어야 해"
        )

    owner, repository = _parse_repository(
        _setting(settings, "HPA_CHANGE_GITHUB_COORDINATOR_REPOSITORY", "")
    )
    workflow_id = str(
        _setting(settings, "HPA_CHANGE_GITHUB_WORKFLOW_FILE", "") or ""
    ).strip()
    api_url = _validate_api_url(
        _setting(settings, "HPA_CHANGE_GITHUB_API_URL", "https://api.github.com")
    )
    coordinator_config = GitHubCoordinatorConfig(
        owner=owner,
        repository=repository,
        workflow_id=workflow_id,
        event_type=_GITHUB_EVENT_TYPE,
        workflow_run_name_prefix=_GITHUB_RUN_NAME_PREFIX,
        implementation_workflow_run_name_prefix=_GITHUB_IMPLEMENTATION_RUN_NAME_PREFIX,
        api_base_url=api_url,
    )
    token_provider, auth_mode = _build_token_provider(
        settings,
        repository=repository,
        api_url=api_url,
        session=session,
    )
    db_path = str(_setting(settings, "HPA_CHANGE_JOB_DB_PATH", "") or "").strip()
    if not db_path:
        raise ValueError("HPA_CHANGE_JOB_DB_PATH 설정이 없어")

    # coordinator repository와 workflow는 process 시작 시 고정하고
    # Slack/LLM 입력으로 바꾸지 않는다.
    store = HpaChangeJobStore(db_path)
    github = GitHubCoordinatorClient(
        coordinator_config,
        token_provider,
        session=session,
    )
    workflow = HpaChangeWorkflowService(store, github)
    return HpaChangeRuntime(
        enabled=True,
        routes_config=routes_config,
        poll_interval_sec=poll_interval_sec,
        run_timeout_sec=run_timeout_sec,
        store=store,
        workflow=workflow,
        auth_mode=auth_mode,
        logger=logger,
    )


__all__ = ["HpaChangeRuntime", "create_hpa_change_runtime"]
