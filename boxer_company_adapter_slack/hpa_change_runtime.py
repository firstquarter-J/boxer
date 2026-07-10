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
    HpaChangeWorkflowService,
    StaticGitHubTokenProvider,
)
from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeRequest as RouteHpaChangeRequest,
    HpaChangeRoutesConfig,
    HpaChangeSubmissionResult,
    HpaChangeSubmissionStatus,
)


_GITHUB_EVENT_TYPE = "boxer-hpa-change"
_GITHUB_RUN_NAME_PREFIX = "Boxer HPA Review"
_GITHUB_IMPLEMENTATION_RUN_NAME_PREFIX = "Boxer HPA Implementation"


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

    def submit_request(self, request: RouteHpaChangeRequest) -> HpaChangeSubmissionResult:
        if not self.enabled or self.store is None or self.workflow is None:
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.REJECTED,
                user_message="HPA 코드 변경 작업 큐가 활성화되지 않았어",
            )

        # worker에는 정규화한 스레드 요구사항과 텍스트 첨부만 전달한다.
        # Slack file id, channel id 같은 내부 intake 정보는
        # dispatch payload에 넣지 않는다.
        attachments = tuple(
            HpaChangeAttachment(
                name=item.name,
                content=item.content,
                sha256=hashlib.sha256(item.content.encode("utf-8")).hexdigest(),
            )
            for item in request.attachments
        )
        workflow_request = WorkflowHpaChangeRequest(
            workspace_id=request.workspace_id,
            event_ts=request.event_ts,
            channel_id=request.channel_id,
            thread_ts=request.thread_ts,
            requested_by=request.requester_user_id,
            request_text=request.thread_text,
            thread_url=request.thread_url,
            attachments=attachments,
            metadata={"source": "slack", "request_key": request.request_key},
        )
        try:
            job, created = self.workflow.submit(workflow_request)
        except Exception as exc:
            # GitHub 오류 원문에는 응답이나 credential이 포함될 수 있어
            # type만 기록한다.
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
            return HpaChangeSubmissionResult(
                status=HpaChangeSubmissionStatus.ACCEPTED,
                request_id=job.task_id,
                user_message=(
                    "격리 worker에 전달했어. "
                    "진행 상황과 PR은 이 스레드에 알릴게"
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
