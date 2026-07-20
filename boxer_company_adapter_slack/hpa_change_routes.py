import logging
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Protocol
from urllib.parse import parse_qs, urljoin, urlsplit

import requests

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _load_slack_permalink,
    _merge_request_log_metadata,
    _set_request_log_route,
    _set_request_log_skip_persist,
)


_TARGET_TOKEN_RE = re.compile(r"(?<![0-9A-Za-z])(hpa|cr)(?![0-9A-Za-z])", re.IGNORECASE)
_ACTION_TOKEN_RE = re.compile(r"(?<![0-9A-Za-z])pr(?![0-9A-Za-z])", re.IGNORECASE)
_TARGET_KOREAN_TOKENS = ("내재화",)
_ACTION_KOREAN_TOKENS = ("검토", "반영", "구현")
# 허용된 HPA 채널에서 새 글에 요구사항을 직접 적을 때 쓰는 명시적 명령이다.
# 일반 대화의 "요구사항 검토 결과"까지 가로채지 않도록 메시지 시작과 구분자를 고정한다.
_DIRECT_REQUIREMENTS_REVIEW_RE = re.compile(
    r"^\s*요구\s*사항\s*검토"
    r"(?:\s*(?:해\s*줘|해\s*주세요|해주세요|부탁(?:해|드립니다)?))?"
    r"\s*(?::|：|-|\r?\n|$)"
)
# 추가 확인 질문에 답한 뒤 쓰는 짧은 명령은 HPA/CR 토큰이 없을 수 있다.
# 메시지 자체는 좁게 잡고, 실제 처리는 같은 thread의 최신 job 상태까지 일치할 때만 한다.
_CLARIFICATION_CONTINUE_RE = re.compile(
    r"^\s*(?:(?:이대로|계속)\s+)?(?:HPA\s+)?(?:(?:구현|작업)\s+)?"
    r"진행(?:해|해\s*줘|해주세요|해\s*주세요)\s*[.!]?\s*$",
    re.IGNORECASE,
)
_CLARIFICATION_ANSWER_RE = re.compile(
    r"(?:^|\r?\n)\s*(?:질문\s*\d+\s*)?답변\s*[:：-]",
    re.IGNORECASE,
)
_URL_TOKEN_RE = re.compile(r"https?://[^\s<>|]+", re.IGNORECASE)
_SLACK_ARCHIVE_PATH_RE = re.compile(
    r"^/archives/(?P<channel>[A-Z0-9]{6,30})/p(?P<timestamp>[0-9]{7,26})/?$"
)
_SLACK_TIMESTAMP_RE = re.compile(r"^[0-9]{1,20}\.[0-9]{6}$")
_SLACK_USER_ID_RE = re.compile(r"^[UW][A-Z0-9]{5,20}$")
_ALLOWED_ATTACHMENT_SUFFIXES = frozenset(
    {".ts", ".tsx", ".js", ".jsx", ".json", ".txt", ".md"}
)


class HpaChangeSubmissionStatus(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


class HpaChangeThreadLookupState(str, Enum):
    NONE = "none"
    NEEDS_CLARIFICATION = "needs_clarification"
    ACTIVE = "active"
    TERMINAL = "terminal"
    ERROR = "error"


@dataclass(frozen=True)
class HpaChangeThreadLookupResult:
    """후속 명령을 새 요청과 구분할 수 있는 현재 thread 작업 상태다."""

    state: HpaChangeThreadLookupState
    request_id: str = ""
    job_status: str = ""
    event_ts: str = ""
    current_event: bool = False


@dataclass(frozen=True)
class HpaChangeAttachment:
    file_id: str
    name: str
    mimetype: str
    size_bytes: int
    content: str
    message_ts: str


@dataclass(frozen=True)
class HpaChangeRequest:
    request_key: str
    workspace_id: str
    channel_id: str
    thread_ts: str
    # worker가 검토 근거로 사용할 현재 thread 또는 선택 댓글 permalink야.
    thread_url: str
    event_ts: str
    requester_user_id: str
    question: str
    thread_text: str
    thread_message_count: int
    attachments: tuple[HpaChangeAttachment, ...]
    # 링크 선택 모드에서는 질문 대상과 실행 요청자를 분리해 보존한다.
    initiator_user_id: str = ""
    source_channel_id: str = ""
    source_message_ts: str = ""
    selection_mode: str = "thread"
    response_thread_url: str = ""
    # 추가 확인 뒤에는 기존 task를 재실행하지 않고 새 task가 부모 요청을 이어받는다.
    continuation_of_request_id: str = ""


@dataclass(frozen=True)
class HpaChangeSubmissionResult:
    status: HpaChangeSubmissionStatus
    request_id: str = ""
    user_message: str = ""


class HpaChangeSubmitRequest(Protocol):
    def __call__(self, request: HpaChangeRequest) -> HpaChangeSubmissionResult: ...


class HpaChangeThreadJobLookup(Protocol):
    def __call__(
        self,
        workspace_id: str,
        channel_id: str,
        thread_ts: str,
        event_ts: str,
    ) -> HpaChangeThreadLookupResult: ...


class HpaChangeFileDownloader(Protocol):
    def __call__(
        self,
        client: Any,
        file_payload: Mapping[str, Any],
        max_bytes: int,
    ) -> bytes: ...


@dataclass(frozen=True)
class HpaChangeRoutesConfig:
    enabled: bool = False
    # 비어 있는 allowlist는 전체 허용이 아니라 기능 차단으로 해석한다.
    allowed_user_ids: frozenset[str] = field(default_factory=frozenset)
    allowed_channel_ids: frozenset[str] = field(default_factory=frozenset)
    max_thread_chars: int = 60_000
    max_attachment_count: int = 10
    max_attachment_bytes: int = 1 * 1024 * 1024
    max_total_attachment_bytes: int = 4 * 1024 * 1024


@dataclass(frozen=True)
class HpaChangeRoutesDeps:
    # 이 콜백은 분석이나 구현을 직접 하지 않고
    # 영속 작업 큐에 넣은 뒤 바로 반환해야 한다.
    submit_request: HpaChangeSubmitRequest
    # 함수가 아래에서 정의되므로 default_factory로 인스턴스 생성 시점에
    # 실제 다운로더를 참조한다. 반환 함수를 만드는 일반 default 람다는
    # intake 호출 시 인자 3개를 받지 못해 모든 Slack 첨부를 TypeError로 만든다.
    download_file: HpaChangeFileDownloader = field(
        default_factory=lambda: _download_slack_file
    )
    lookup_thread_job: HpaChangeThreadJobLookup | None = None


@dataclass(frozen=True)
class HpaChangeRoutesContext:
    question: str
    payload: MentionPayload
    user_id: str | None
    workspace_id: str
    channel_id: str
    current_ts: str
    thread_ts: str
    reply: SlackReplyFn
    client: Any
    logger: logging.Logger


class HpaChangeIntakeError(RuntimeError):
    """Slack intake에서 사용자에게 안전한 문구만 전달하기 위한 오류야."""


@dataclass(frozen=True)
class _SlackMessagePermalinkTarget:
    workspace_hostname: str
    channel_id: str
    message_ts: str
    thread_ts: str
    permalink: str


def _looks_like_hpa_change_request(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    if _DIRECT_REQUIREMENTS_REVIEW_RE.search(text):
        return True
    has_target = bool(_TARGET_TOKEN_RE.search(text)) or any(
        token in text for token in _TARGET_KOREAN_TOKENS
    )
    has_action = bool(_ACTION_TOKEN_RE.search(text)) or any(
        token in text for token in _ACTION_KOREAN_TOKENS
    )
    return has_target and has_action


def _looks_like_hpa_clarification_followup(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    return bool(
        _CLARIFICATION_CONTINUE_RE.fullmatch(text)
        or _CLARIFICATION_ANSWER_RE.search(text)
    )


def _normalize_allowlist(values: frozenset[str]) -> set[str]:
    return {str(value or "").strip() for value in values if str(value or "").strip()}


def _build_request_key(
    workspace_id: str,
    channel_id: str,
    event_ts: str,
) -> str:
    # Slack retry는 같은 event timestamp를 유지하므로 이 키를 작업 큐의 idempotency key로 쓴다.
    return f"slack:{workspace_id}:{channel_id}:{event_ts}"


def _build_hpa_reply_client_msg_id(
    workspace_id: str,
    channel_id: str,
    thread_ts: str,
    event_ts: str,
) -> str | None:
    """동일 Slack event의 접수·상태 응답을 하나의 logical 댓글로 고정한다."""

    identity = tuple(
        str(value or "").strip()
        for value in (workspace_id, channel_id, thread_ts, event_ts)
    )
    if not all(identity):
        return None
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "boxer:hpa-change:intake:" + ":".join(identity),
        )
    )


def _timestamp_from_permalink_token(value: str) -> str:
    digits = str(value or "").strip()
    if not digits.isdigit() or len(digits) < 7:
        raise HpaChangeIntakeError("Slack 댓글 링크의 메시지 시간을 확인하지 못했어")
    timestamp = f"{digits[:-6]}.{digits[-6:]}"
    if not _SLACK_TIMESTAMP_RE.fullmatch(timestamp):
        raise HpaChangeIntakeError("Slack 댓글 링크의 메시지 시간을 확인하지 못했어")
    return timestamp


def _parse_slack_message_permalink(value: str) -> _SlackMessagePermalinkTarget:
    permalink = str(value or "").strip().rstrip(".,);]")
    try:
        parsed = urlsplit(permalink)
        port = parsed.port
    except ValueError:
        raise HpaChangeIntakeError("Slack 댓글 링크 형식이 올바르지 않아") from None
    hostname = str(parsed.hostname or "").lower()
    if (
        parsed.scheme.lower() != "https"
        or not hostname.endswith(".slack.com")
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or bool(parsed.fragment)
    ):
        raise HpaChangeIntakeError("Slack 댓글 링크는 HTTPS Slack permalink만 사용할 수 있어")
    path_match = _SLACK_ARCHIVE_PATH_RE.fullmatch(parsed.path)
    if path_match is None:
        raise HpaChangeIntakeError("Slack 댓글 링크에서 채널과 메시지를 확인하지 못했어")

    channel_id = path_match.group("channel")
    message_ts = _timestamp_from_permalink_token(path_match.group("timestamp"))
    query = parse_qs(parsed.query, keep_blank_values=True)
    cid_values = [str(item or "").strip() for item in query.get("cid", [])]
    if cid_values and (len(cid_values) != 1 or cid_values[0] != channel_id):
        raise HpaChangeIntakeError("Slack 댓글 링크의 채널 정보가 서로 달라")
    thread_values = [str(item or "").strip() for item in query.get("thread_ts", [])]
    if len(thread_values) > 1:
        raise HpaChangeIntakeError("Slack 댓글 링크의 스레드 정보가 중복돼 있어")
    thread_ts = thread_values[0] if thread_values else message_ts
    if not _SLACK_TIMESTAMP_RE.fullmatch(thread_ts):
        raise HpaChangeIntakeError("Slack 댓글 링크의 스레드 시간을 확인하지 못했어")
    return _SlackMessagePermalinkTarget(
        workspace_hostname=hostname,
        channel_id=channel_id,
        message_ts=message_ts,
        thread_ts=thread_ts,
        permalink=permalink,
    )


def _extract_linked_message_target(question: str) -> _SlackMessagePermalinkTarget | None:
    targets: dict[tuple[str, str, str, str], _SlackMessagePermalinkTarget] = {}
    for match in _URL_TOKEN_RE.finditer(str(question or "")):
        raw_url = match.group(0)
        try:
            parsed = urlsplit(raw_url.rstrip(".,);]"))
        except ValueError:
            # Slack 댓글 선택자로 보이는 링크가 깨졌다면 전체 스레드 모드로
            # 조용히 되돌아가지 않고 명시적으로 거절한다.
            if ".slack.com" in raw_url.lower() or "/archives/" in raw_url:
                raise HpaChangeIntakeError(
                    "Slack 댓글 링크 형식이 올바르지 않아"
                ) from None
            continue
        hostname = str(parsed.hostname or "").lower()
        looks_like_selector = hostname.endswith(".slack.com") or "/archives/" in parsed.path
        if not looks_like_selector:
            continue
        target = _parse_slack_message_permalink(raw_url)
        targets[
            (
                target.workspace_hostname,
                target.channel_id,
                target.message_ts,
                target.thread_ts,
            )
        ] = target
    if len(targets) > 1:
        raise HpaChangeIntakeError("한 번에 검토할 Slack 댓글 링크는 하나만 지정해줘")
    return next(iter(targets.values()), None)


def _select_linked_message(
    messages: list[dict[str, Any]],
    target: _SlackMessagePermalinkTarget,
) -> dict[str, Any]:
    for message in messages:
        if str(message.get("ts") or "").strip() != target.message_ts:
            continue
        actual_thread_ts = str(
            message.get("thread_ts") or message.get("ts") or ""
        ).strip()
        if actual_thread_ts != target.thread_ts:
            raise HpaChangeIntakeError("Slack 댓글 링크와 실제 스레드 정보가 일치하지 않아")
        author_id = str(message.get("user") or "").strip()
        if (
            not _SLACK_USER_ID_RE.fullmatch(author_id)
            or bool(str(message.get("bot_id") or "").strip())
            or str(message.get("subtype") or "").strip() == "bot_message"
        ):
            raise HpaChangeIntakeError("링크된 Slack 댓글 작성자를 확인하지 못했어")
        return message
    raise HpaChangeIntakeError("링크된 Slack 댓글을 해당 스레드에서 찾지 못했어")


def _fetch_all_thread_messages(
    client: Any,
    *,
    channel_id: str,
    thread_ts: str,
    logger: logging.Logger,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    cursor = ""
    seen_cursors: set[str] = set()

    while True:
        kwargs: dict[str, Any] = {
            "channel": channel_id,
            "ts": thread_ts,
            "limit": 200,
            "inclusive": True,
        }
        if cursor:
            kwargs["cursor"] = cursor
        try:
            response = client.conversations_replies(**kwargs)
        except Exception as exc:
            # Slack 예외에는 요청 URL이나 응답 원문이 들어갈 수 있어 type만 남긴다.
            logger.warning(
                "Failed to fetch HPA change Slack thread error_type=%s",
                type(exc).__name__,
            )
            raise HpaChangeIntakeError(
                "Slack 스레드를 읽지 못했어. 채널 history 권한을 확인해줘"
            ) from None

        page_messages = (response or {}).get("messages") or []
        if not isinstance(page_messages, list):
            raise HpaChangeIntakeError("Slack 스레드 응답 형식을 확인하지 못했어")
        messages.extend(item for item in page_messages if isinstance(item, dict))

        metadata = (response or {}).get("response_metadata") or {}
        next_cursor = str((metadata or {}).get("next_cursor") or "").strip()
        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            logger.warning("Stopped repeated cursor while fetching HPA change Slack thread")
            raise HpaChangeIntakeError("Slack 스레드 페이지를 끝까지 읽지 못했어")
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    # 페이지 경계에서 같은 메시지가 겹쳐도 분석 입력에는 한 번만 들어가게 한다.
    deduplicated: list[dict[str, Any]] = []
    seen_message_keys: set[tuple[str, str]] = set()
    for message in messages:
        ts = str(message.get("ts") or "").strip()
        client_msg_id = str(message.get("client_msg_id") or "").strip()
        if ts:
            key = (ts, client_msg_id)
            if key in seen_message_keys:
                continue
            seen_message_keys.add(key)
        deduplicated.append(message)
    return deduplicated


def _render_thread_text(messages: list[dict[str, Any]]) -> str:
    rendered: list[str] = []
    for message in messages:
        text = str(message.get("text") or "").strip()
        if not text:
            continue
        author = str(
            message.get("user")
            or message.get("bot_id")
            or message.get("username")
            or "unknown"
        ).strip()
        ts = str(message.get("ts") or "").strip()
        rendered.append(f"[{ts}] {author}\n{text}")
    return "\n\n".join(rendered)


def _select_clarification_followup_messages(
    messages: list[dict[str, Any]],
    *,
    thread_ts: str,
    allowed_user_ids: set[str],
    after_event_ts: str,
) -> list[dict[str, Any]]:
    """부모 요청 뒤 사람이 직접 남긴 답변만 후속 worker 입력으로 사용한다."""

    def timestamp_key(value: str) -> tuple[int, str]:
        seconds, separator, fraction = str(value or "").strip().partition(".")
        if not separator or not seconds.isdigit() or not fraction.isdigit():
            return (-1, "")
        # Slack fraction은 오른쪽을 0으로 채우면 문자열 비교로도 시간 순서가 유지된다.
        return (int(seconds), fraction.ljust(20, "0")[:20])

    after_key = timestamp_key(after_event_ts)
    selected: list[dict[str, Any]] = []
    for message in messages:
        message_ts = str(message.get("ts") or "").strip()
        author_id = str(message.get("user") or "").strip()
        is_bot = bool(str(message.get("bot_id") or "").strip()) or (
            str(message.get("subtype") or "").strip() == "bot_message"
        )
        # 부모 request_text가 원 요청을 이미 보존한다. thread root와 Boxer의
        # 검토·질문·안내를 다시 넣으면 coverage 검사가 이를 새 요구사항으로 오해할 수 있다.
        if (
            message_ts
            and message_ts != thread_ts
            and timestamp_key(message_ts) > after_key
            and author_id in allowed_user_ids
            and not is_bot
        ):
            selected.append(message)
    return selected


def _is_safe_attachment_name(name: str) -> bool:
    if not name or "\x00" in name or "/" in name or "\\" in name:
        return False
    if name in {".", ".."} or ".." in PurePosixPath(name).parts:
        return False
    return PurePosixPath(name).name == name and PureWindowsPath(name).name == name


def _parse_file_size(value: Any) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    if parsed < 0:
        raise HpaChangeIntakeError("Slack 첨부 파일 크기 정보가 올바르지 않아")
    return parsed


def _resolve_file_payload(
    client: Any,
    file_payload: dict[str, Any],
    *,
    logger: logging.Logger,
) -> dict[str, Any]:
    required_values = (
        file_payload.get("name") or file_payload.get("title"),
        file_payload.get("url_private_download") or file_payload.get("url_private"),
    )
    if all(required_values):
        return file_payload

    file_id = str(file_payload.get("id") or "").strip()
    if not file_id:
        return file_payload
    try:
        response = client.files_info(file=file_id)
    except Exception as exc:
        logger.warning(
            "Failed to resolve HPA change Slack file metadata error_type=%s",
            type(exc).__name__,
        )
        raise HpaChangeIntakeError(
            "Slack 첨부 파일 정보를 읽지 못했어. files:read 권한을 확인해줘"
        ) from None
    resolved = (response or {}).get("file") or {}
    return dict(resolved) if isinstance(resolved, Mapping) else file_payload


def _collect_candidate_files(
    client: Any,
    messages: list[dict[str, Any]],
    *,
    config: HpaChangeRoutesConfig,
    logger: logging.Logger,
) -> list[tuple[dict[str, Any], str]]:
    candidates: list[tuple[dict[str, Any], str]] = []
    seen_files: set[str] = set()

    for message in messages:
        raw_files = message.get("files") or []
        if not isinstance(raw_files, list):
            continue
        for raw_file in raw_files:
            if not isinstance(raw_file, dict):
                continue
            file_payload = _resolve_file_payload(client, raw_file, logger=logger)
            name = str(file_payload.get("name") or file_payload.get("title") or "").strip()
            if not _is_safe_attachment_name(name):
                raise HpaChangeIntakeError(
                    "경로 문자가 포함된 Slack 첨부 파일은 안전상 처리하지 않아"
                )
            if PurePosixPath(name).suffix.lower() not in _ALLOWED_ATTACHMENT_SUFFIXES:
                continue

            file_id = str(file_payload.get("id") or "").strip()
            fallback_key = "|".join(
                (
                    name,
                    str(file_payload.get("url_private_download") or file_payload.get("url_private") or ""),
                )
            )
            dedup_key = file_id or fallback_key
            if dedup_key in seen_files:
                continue
            seen_files.add(dedup_key)
            candidates.append((file_payload, str(message.get("ts") or "").strip()))

    if len(candidates) > max(0, config.max_attachment_count):
        raise HpaChangeIntakeError(
            f"코드 첨부는 최대 {max(0, config.max_attachment_count)}개까지 받을 수 있어"
        )
    return candidates


def _validate_slack_file_url(url: str) -> None:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        raise HpaChangeIntakeError("Slack이 아닌 첨부 파일 URL은 다운로드하지 않아") from None
    hostname = str(parsed.hostname or "").lower()
    allowed_hosts = ("files.slack.com", "files-origin.slack.com", "slack-files.com")
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or not any(
            hostname == allowed or hostname.endswith(f".{allowed}")
            for allowed in allowed_hosts
        )
    ):
        raise HpaChangeIntakeError("Slack이 아닌 첨부 파일 URL은 다운로드하지 않아")


def _download_slack_file(
    client: Any,
    file_payload: Mapping[str, Any],
    max_bytes: int,
) -> bytes:
    url = str(
        file_payload.get("url_private_download")
        or file_payload.get("url_private")
        or ""
    ).strip()
    _validate_slack_file_url(url)
    token = str(getattr(client, "token", "") or "").strip()
    if not token:
        raise HpaChangeIntakeError("Slack 첨부 파일을 읽을 bot token이 없어")

    # Slack 파일 URL은 files.slack.com에서 files-origin.slack.com으로 redirect될 수 있다.
    # requests로 redirect를 직접 검증하고, 첫 요청 이후에는 Authorization을 제거해 토큰 유출을 막는다.
    current_url = url
    for redirect_count in range(3):
        _validate_slack_file_url(current_url)
        headers = {"Authorization": f"Bearer {token}"} if redirect_count == 0 else {}
        with requests.get(
            current_url,
            headers=headers,
            stream=True,
            timeout=10,
            allow_redirects=False,
        ) as response:
            if 300 <= response.status_code < 400:
                location = str(response.headers.get("Location") or "").strip()
                if not location or redirect_count >= 2:
                    raise HpaChangeIntakeError("Slack 첨부 파일 redirect를 확인하지 못했어")
                current_url = urljoin(current_url, location)
                continue

            if response.status_code in {401, 403}:
                raise HpaChangeIntakeError(
                    "Slack 첨부 파일 권한이 없어. files:read 권한과 앱 재설치를 확인해줘"
                )
            response.raise_for_status()

            content_length = _parse_file_size(response.headers.get("Content-Length"))
            if content_length > max_bytes:
                raise HpaChangeIntakeError("Slack 첨부 파일 하나의 허용 크기를 초과했어")

            chunks: list[bytes] = []
            downloaded = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                downloaded += len(chunk)
                if downloaded > max_bytes:
                    raise HpaChangeIntakeError("Slack 첨부 파일 하나의 허용 크기를 초과했어")
                chunks.append(chunk)
            return b"".join(chunks)

    raise HpaChangeIntakeError("Slack 첨부 파일 redirect가 너무 많아")


def _collect_attachments(
    client: Any,
    messages: list[dict[str, Any]],
    *,
    config: HpaChangeRoutesConfig,
    download_file: HpaChangeFileDownloader,
    logger: logging.Logger,
) -> tuple[HpaChangeAttachment, ...]:
    candidates = _collect_candidate_files(
        client,
        messages,
        config=config,
        logger=logger,
    )
    attachments: list[HpaChangeAttachment] = []
    total_bytes = 0
    max_file_bytes = max(0, config.max_attachment_bytes)
    max_total_bytes = max(0, config.max_total_attachment_bytes)

    for file_payload, message_ts in candidates:
        file_url = str(
            file_payload.get("url_private_download")
            or file_payload.get("url_private")
            or ""
        ).strip()
        # 주입 downloader를 쓰더라도 Slack 파일 도메인 검증은 route 경계에서 강제한다.
        _validate_slack_file_url(file_url)
        declared_size = _parse_file_size(file_payload.get("size"))
        if declared_size > max_file_bytes:
            raise HpaChangeIntakeError(
                f"코드 첨부 파일 하나는 최대 {max_file_bytes}바이트까지 받을 수 있어"
            )
        if total_bytes + declared_size > max_total_bytes:
            raise HpaChangeIntakeError(
                f"코드 첨부 전체는 최대 {max_total_bytes}바이트까지 받을 수 있어"
            )

        try:
            raw_content = download_file(client, file_payload, max_file_bytes)
        except HpaChangeIntakeError:
            raise
        except Exception as exc:
            # 파일 URL, 토큰, 응답 본문이 로그에 남지 않게 예외 원문은 기록하지 않는다.
            logger.warning(
                "Failed to download HPA change Slack file error_type=%s",
                type(exc).__name__,
            )
            raise HpaChangeIntakeError(
                "Slack 첨부 파일을 읽지 못했어. files:read 권한을 확인해줘"
            ) from None

        if len(raw_content) > max_file_bytes:
            raise HpaChangeIntakeError(
                f"코드 첨부 파일 하나는 최대 {max_file_bytes}바이트까지 받을 수 있어"
            )
        total_bytes += len(raw_content)
        if total_bytes > max_total_bytes:
            raise HpaChangeIntakeError(
                f"코드 첨부 전체는 최대 {max_total_bytes}바이트까지 받을 수 있어"
            )
        try:
            content = raw_content.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise HpaChangeIntakeError("코드 첨부 파일은 UTF-8 텍스트여야 해") from None
        if "\x00" in content:
            raise HpaChangeIntakeError("코드 첨부 파일에서 텍스트가 아닌 내용을 발견했어")

        attachments.append(
            HpaChangeAttachment(
                file_id=str(file_payload.get("id") or "").strip(),
                name=str(file_payload.get("name") or file_payload.get("title") or "").strip(),
                mimetype=str(file_payload.get("mimetype") or "").strip(),
                size_bytes=len(raw_content),
                content=content,
                message_ts=message_ts,
            )
        )
    return tuple(attachments)


def _safe_submission_message(message: str) -> str:
    # queue 구현이 돌려준 문구도 Slack 제어문자나 긴 내부 오류를 노출하지 않게 제한한다.
    normalized = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", str(message or ""))
    return normalized.strip()[:300]


def _format_thread_job_status_reply(result: HpaChangeThreadLookupResult) -> str:
    """후속 명령이 새 작업을 만들지 않았다는 사실과 실제 상태를 함께 알린다."""

    if result.state is HpaChangeThreadLookupState.ERROR:
        return (
            "*HPA 코드 변경 작업 상태를 확인하지 못했어*\n"
            "• 상태: 조회 오류\n"
            "• 안내: 새 작업으로 우회하지 않았어. 잠시 후 다시 시도해줘"
        )

    request_id = _safe_submission_message(result.request_id)
    raw_status = _safe_submission_message(result.job_status) or "unknown"
    status_labels = {
        "received": "접수됨",
        "dispatching": "worker 전달 준비 중",
        "dispatched": "worker 전달 완료",
        "running": "실행 중",
        "workflow_succeeded": "worker 결과 확인 중",
        "result_ready": "결과 검증 중",
        "review_ready": "검토 결과 게시 준비 중",
        "review_posted": "구현 시작 준비 중",
        "needs_clarification": "추가 답변 대기",
        "pr_created": "PR 생성 완료",
        "no_change_needed": "변경 불필요로 완료",
        "failed": "실패",
        "canceled": "취소",
    }
    status_label = status_labels.get(raw_status, "상태 확인 필요")
    request_id_line = f"\n• 요청 ID: `{request_id}`" if request_id else ""

    if result.state is HpaChangeThreadLookupState.ACTIVE:
        return (
            "*HPA 코드 변경 작업 진행 중*\n"
            f"• 상태: {status_label} (`{raw_status}`)"
            f"{request_id_line}\n"
            "• 안내: 기존 작업을 유지하고 새 작업은 만들지 않았어"
        )
    if result.state is HpaChangeThreadLookupState.NEEDS_CLARIFICATION:
        return (
            "*HPA 코드 변경 작업 추가 답변 대기 중*\n"
            f"• 상태: {status_label} (`{raw_status}`)"
            f"{request_id_line}\n"
            "• 안내: 같은 Slack 이벤트를 새 작업으로 다시 만들지 않았어"
        )
    return (
        "*HPA 코드 변경 작업이 이미 종료됐어*\n"
        f"• 상태: {status_label} (`{raw_status}`)"
        f"{request_id_line}\n"
        "• 안내: 이 후속 명령으로 새 작업은 만들지 않았어"
    )


def _format_submission_reply(
    result: HpaChangeSubmissionResult,
    *,
    message_count: int,
    attachment_count: int,
    selected_message: bool = False,
    continuation_of_request_id: str = "",
) -> str:
    request_id = str(result.request_id or "").strip()
    request_id_line = f"\n• 요청 ID: `{request_id}`" if request_id else ""
    parent_request_id = str(continuation_of_request_id or "").strip()
    parent_request_id_line = (
        f"\n• 이전 요청 ID: `{parent_request_id}`" if parent_request_id else ""
    )
    detail = _safe_submission_message(result.user_message)
    detail_line = f"\n• 안내: {detail}" if detail else ""

    if result.status is HpaChangeSubmissionStatus.ACCEPTED:
        if parent_request_id:
            return (
                "*HPA 추가 답변 재접수*\n"
                "• 상태: 같은 스레드의 답변과 기존 요청을 합쳐 새 작업 큐에 등록했어"
                f"\n• 수집: 스레드 {message_count}개, 코드 첨부 {attachment_count}개"
                f"{parent_request_id_line}{request_id_line}{detail_line}"
            )
        source_label = "선택 댓글" if selected_message else "스레드"
        return (
            "*HPA 코드 변경 요청 접수*\n"
            "• 상태: 요구사항과 HPA 코드를 검토할 작업 큐에 등록했어"
            f"\n• 수집: {source_label} {message_count}개, 코드 첨부 {attachment_count}개"
            f"{request_id_line}{detail_line}"
        )
    if result.status is HpaChangeSubmissionStatus.DUPLICATE:
        return (
            "*이미 접수된 HPA 코드 변경 요청이야*\n"
            "• 같은 Slack 이벤트를 다시 실행하지 않고 기존 작업을 유지할게"
            f"{parent_request_id_line}{request_id_line}{detail_line}"
        )
    return (
        "*HPA 코드 변경 요청을 접수하지 못했어*\n"
        "• 작업 큐 상태를 확인한 뒤 다시 요청해줘"
        f"{request_id_line}{detail_line}"
    )


def _handle_hpa_change_request(
    context: HpaChangeRoutesContext,
    config: HpaChangeRoutesConfig,
    deps: HpaChangeRoutesDeps,
) -> bool:
    is_explicit_request = _looks_like_hpa_change_request(context.question)
    is_clarification_followup = _looks_like_hpa_clarification_followup(
        context.question
    )
    if not is_explicit_request and not is_clarification_followup:
        return False

    event_ts = str(context.current_ts or "").strip()
    thread_ts = str(context.thread_ts or "").strip()
    workspace_id = str(context.workspace_id or "").strip()
    channel_id = str(context.channel_id or "").strip()
    reply_client_msg_id = _build_hpa_reply_client_msg_id(
        workspace_id,
        channel_id,
        thread_ts,
        event_ts,
    )

    def reply_once(text: str) -> None:
        context.reply(
            text,
            mention_user=False,
            client_msg_id=reply_client_msg_id,
        )

    thread_lookup = HpaChangeThreadLookupResult(HpaChangeThreadLookupState.NONE)
    continuation_of_request_id = ""
    continuation_after_event_ts = ""
    if (
        is_clarification_followup
        and event_ts
        and thread_ts
        and workspace_id
        and channel_id
        and event_ts != thread_ts
        and deps.lookup_thread_job is not None
    ):
        try:
            resolved = deps.lookup_thread_job(
                workspace_id,
                channel_id,
                thread_ts,
                event_ts,
            )
            if isinstance(resolved, HpaChangeThreadLookupResult):
                thread_lookup = resolved
                if (
                    resolved.state
                    not in {
                        HpaChangeThreadLookupState.NONE,
                        HpaChangeThreadLookupState.ERROR,
                    }
                    and not str(resolved.request_id or "").strip()
                ):
                    context.logger.warning("HPA thread lookup result omitted request id")
                    thread_lookup = HpaChangeThreadLookupResult(
                        HpaChangeThreadLookupState.ERROR
                    )
            else:
                context.logger.warning("Invalid HPA thread lookup result type")
                thread_lookup = HpaChangeThreadLookupResult(
                    HpaChangeThreadLookupState.ERROR
                )
        except Exception as exc:
            # 상태 조회 실패를 새 HPA 요청으로 우회하면 원 요청 소스와 권한이 바뀔 수 있다.
            context.logger.warning(
                "Failed to resolve HPA thread job error_type=%s",
                type(exc).__name__,
            )
            thread_lookup = HpaChangeThreadLookupResult(
                HpaChangeThreadLookupState.ERROR
            )

    if (
        thread_lookup.state is HpaChangeThreadLookupState.NEEDS_CLARIFICATION
        and not thread_lookup.current_event
    ):
        continuation_of_request_id = str(thread_lookup.request_id or "").strip()
        continuation_after_event_ts = str(thread_lookup.event_ts or "").strip()

    # 기존 HPA 작업이 없는 thread의 짧은 진행 문구는 일반 대화가 처리한다.
    if (
        not is_explicit_request
        and thread_lookup.state is HpaChangeThreadLookupState.NONE
    ):
        return False

    # 코드·프롬프트 원문은 request log에 저장하지 않고
    # 작업 큐의 별도 보안 경계로만 넘긴다.
    _set_request_log_skip_persist(context.payload)
    _set_request_log_route(
        context.payload,
        "hpa_change_request",
        route_mode="intake",
        handler_type="router",
        subject_type="slack_thread",
        subject_key=context.thread_ts,
    )

    if not config.enabled:
        reply_once("HPA 코드 변경 요청 접수 기능이 꺼져 있어")
        return True

    allowed_user_ids = _normalize_allowlist(config.allowed_user_ids)
    allowed_channel_ids = _normalize_allowlist(config.allowed_channel_ids)
    if not allowed_user_ids or not allowed_channel_ids:
        reply_once("HPA 코드 변경 요청의 허용 사용자 또는 채널 설정이 없어")
        return True
    if not context.user_id or context.user_id not in allowed_user_ids:
        reply_once("HPA 코드 변경 요청을 접수할 권한이 없어")
        return True
    if context.channel_id not in allowed_channel_ids:
        reply_once("이 채널에서는 HPA 코드 변경 요청을 접수하지 않아")
        return True

    if not event_ts or not thread_ts or not workspace_id or not channel_id:
        reply_once("Slack 요청 식별 정보가 부족해서 접수하지 못했어")
        return True

    if is_clarification_followup and (
        thread_lookup.state
        in {
            HpaChangeThreadLookupState.ERROR,
            HpaChangeThreadLookupState.ACTIVE,
            HpaChangeThreadLookupState.TERMINAL,
        }
        or (
            thread_lookup.state
            is HpaChangeThreadLookupState.NEEDS_CLARIFICATION
            and thread_lookup.current_event
        )
    ):
        _merge_request_log_metadata(
            context.payload,
            hpaChangeRequestId=str(thread_lookup.request_id or "").strip() or None,
            hpaChangeThreadLookupState=thread_lookup.state.value,
            hpaChangeJobStatus=str(thread_lookup.job_status or "").strip() or None,
            hpaChangeCurrentEvent=thread_lookup.current_event,
        )
        reply_once(_format_thread_job_status_reply(thread_lookup))
        return True

    request_key = _build_request_key(workspace_id, channel_id, event_ts)
    try:
        linked_target = (
            None
            if continuation_of_request_id
            else _extract_linked_message_target(context.question)
        )
        source_channel_id = channel_id
        source_thread_ts = thread_ts
        source_message_ts = thread_ts
        source_permalink = ""
        requester_user_id = str(context.user_id or "").strip()
        selection_mode = (
            "clarification_followup"
            if continuation_of_request_id
            else "thread"
        )
        if linked_target is not None:
            if linked_target.channel_id not in allowed_channel_ids:
                raise HpaChangeIntakeError(
                    "링크된 Slack 댓글 채널은 HPA 변경 요청 허용 채널이 아니야"
                )
            source_channel_id = linked_target.channel_id
            source_thread_ts = linked_target.thread_ts
            source_message_ts = linked_target.message_ts
            selection_mode = "linked_message"
            canonical_permalink = _load_slack_permalink(
                context.client,
                source_channel_id,
                source_message_ts,
                context.logger,
            )
            if not canonical_permalink:
                raise HpaChangeIntakeError(
                    "링크된 Slack 댓글의 canonical permalink를 확인하지 못했어"
                )
            canonical_target = _parse_slack_message_permalink(canonical_permalink)
            if (
                canonical_target.workspace_hostname != linked_target.workspace_hostname
                or canonical_target.channel_id != linked_target.channel_id
                or canonical_target.message_ts != linked_target.message_ts
                or canonical_target.thread_ts != linked_target.thread_ts
            ):
                raise HpaChangeIntakeError(
                    "링크된 Slack 댓글이 현재 워크스페이스 메시지와 일치하지 않아"
                )
            source_permalink = canonical_target.permalink

        messages = _fetch_all_thread_messages(
            context.client,
            channel_id=source_channel_id,
            thread_ts=source_thread_ts,
            logger=context.logger,
        )
        if not messages:
            raise HpaChangeIntakeError("검토할 Slack 스레드 내용을 찾지 못했어")
        if linked_target is not None:
            selected_message = _select_linked_message(messages, linked_target)
            messages = [selected_message]
            requester_user_id = str(selected_message.get("user") or "").strip()
            if requester_user_id not in allowed_user_ids:
                raise HpaChangeIntakeError(
                    "링크된 Slack 댓글 작성자는 HPA 변경 요청 허용 사용자가 아니야"
                )
        elif continuation_of_request_id:
            messages = _select_clarification_followup_messages(
                messages,
                thread_ts=thread_ts,
                allowed_user_ids=allowed_user_ids,
                after_event_ts=continuation_after_event_ts,
            )
            if not messages:
                raise HpaChangeIntakeError(
                    "추가 확인 질문에 대한 허용 사용자의 답변을 찾지 못했어"
                )
        thread_text = _render_thread_text(messages)
        if not thread_text:
            raise HpaChangeIntakeError("검토할 Slack 스레드 텍스트를 찾지 못했어")
        max_thread_chars = max(0, config.max_thread_chars)
        if len(thread_text) > max_thread_chars:
            # 임의 절단은 요구사항을 바꿀 수 있으므로
            # dispatch 한도를 넘으면 명확히 다시 요청받는다.
            raise HpaChangeIntakeError(
                f"Slack 검토 입력이 {max_thread_chars}자 제한을 초과했어. "
                "핵심 요구사항을 새 스레드로 정리해서 다시 요청해줘"
            )
        attachments = _collect_attachments(
            context.client,
            messages,
            config=config,
            download_file=deps.download_file,
            logger=context.logger,
        )
        if not source_permalink:
            source_permalink = _load_slack_permalink(
                context.client,
                source_channel_id,
                source_message_ts,
                context.logger,
            ) or ""
        if source_channel_id == channel_id and source_message_ts == thread_ts:
            response_thread_url = source_permalink
        else:
            response_thread_url = _load_slack_permalink(
                context.client,
                channel_id,
                thread_ts,
                context.logger,
            ) or ""
    except HpaChangeIntakeError as exc:
        reply_once(str(exc))
        return True

    request = HpaChangeRequest(
        request_key=request_key,
        workspace_id=workspace_id,
        # 응답 목적지는 링크가 아니라 박서를 멘션한 현재 메시지의 스레드로 고정한다.
        channel_id=channel_id,
        thread_ts=thread_ts,
        thread_url=source_permalink,
        event_ts=event_ts,
        requester_user_id=requester_user_id,
        question=str(context.question or "").strip(),
        thread_text=thread_text,
        thread_message_count=len(messages),
        attachments=attachments,
        initiator_user_id=str(context.user_id or "").strip(),
        source_channel_id=source_channel_id,
        source_message_ts=source_message_ts,
        selection_mode=selection_mode,
        response_thread_url=response_thread_url,
        continuation_of_request_id=continuation_of_request_id,
    )
    try:
        # 동일 event_ts의 중복 판정은 영속 큐가 원자적으로 처리하고 결과로 알려준다.
        result = deps.submit_request(request)
    except Exception as exc:
        context.logger.warning(
            "Failed to submit HPA change request error_type=%s",
            type(exc).__name__,
        )
        reply_once("HPA 코드 변경 작업 큐에 접수하지 못했어")
        return True
    if not isinstance(result, HpaChangeSubmissionResult):
        context.logger.warning("Invalid HPA change submission result type")
        reply_once("HPA 코드 변경 작업 큐 응답을 확인하지 못했어")
        return True

    _merge_request_log_metadata(
        context.payload,
        hpaChangeRequestKey=request_key,
        hpaChangeRequestId=str(result.request_id or "").strip() or None,
        hpaChangeSubmissionStatus=result.status.value,
        threadMessageCount=len(messages),
        sourceSelectionMode=selection_mode,
        continuationOfRequestId=continuation_of_request_id or None,
        attachmentCount=len(attachments),
        attachmentBytes=sum(item.size_bytes for item in attachments),
    )
    reply_once(
        _format_submission_reply(
            result,
            message_count=len(messages),
            attachment_count=len(attachments),
            selected_message=selection_mode == "linked_message",
            continuation_of_request_id=continuation_of_request_id,
        )
    )
    return True


__all__ = [
    "HpaChangeAttachment",
    "HpaChangeRequest",
    "HpaChangeRoutesConfig",
    "HpaChangeRoutesContext",
    "HpaChangeRoutesDeps",
    "HpaChangeSubmissionResult",
    "HpaChangeSubmissionStatus",
    "HpaChangeThreadLookupResult",
    "HpaChangeThreadLookupState",
    "_handle_hpa_change_request",
    "_looks_like_hpa_clarification_followup",
    "_looks_like_hpa_change_request",
]
