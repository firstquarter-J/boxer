from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import logging
import re
from typing import Any
from urllib.parse import urlsplit

from boxer.core import settings as s
from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer_company import settings as cs
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import (
    AssistantLink,
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.scope_guard import (
    AssistantRequestScopeMismatch,
    resolve_assistant_request_scope,
)
from boxer_company.assistant.operation_intent import (
    is_explicit_operation_execution,
)
from boxer_company.routers.barcode_log import _extract_log_date_with_presence
from boxer_company.routers.box_db import (
    _load_recordings_context_by_barcode,
    _lookup_device_contexts_by_barcode,
    _lookup_device_contexts_by_barcode_on_date,
    _lookup_device_contexts_by_hospital_room,
)
from boxer_company.routers.device_file_probe import (
    _build_device_file_download_config_message,
    _build_device_file_probe_config_message,
    _build_device_file_recovery_config_message,
    _build_device_file_scope_request_message,
    _is_barcode_device_file_probe_request,
    _locate_barcode_file_candidates,
    _should_download_device_files,
    _should_probe_device_files,
    _should_recover_device_files,
    _should_render_compact_device_download_result,
    _should_render_compact_device_file_list,
    _should_render_compact_device_recovery_result,
    _should_render_compact_file_id_result,
)
from boxer_company.routers.device_log_upload import (
    _check_and_request_device_log_upload,
    _extract_hospital_room_scope_for_log_upload,
    _is_device_log_upload_check_request,
)
from boxer_company.routers.mda_graphql import _send_mda_device_command
from boxer_company.routers.recording_streaming_restore import (
    _extract_recording_streaming_restore_month,
    _is_recording_streaming_restore_request,
)


DEVICE_LOG_UPLOAD_ROUTE = "device_log_upload"
DEVICE_FILE_LOOKUP_ROUTE = "device_file_lookup"
DEVICE_FILE_DOWNLOAD_ROUTE = "device_file_download"
DEVICE_FILE_RECOVERY_ROUTE = "device_file_recovery"

_OPERATIONS_ROUTE_GROUP = "operations"
_BARCODE_PATTERN = re.compile(r"(?<!\d)(\d{11})(?!\d)")
_DEVICE_NAME_LABEL_PATTERN = re.compile(
    r"(?:장비명|devicename)\s*[:=]?\s*([A-Za-z0-9._-]+)",
    re.IGNORECASE,
)
_DEVICE_NAME_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"([A-Za-z][A-Za-z0-9]*-[A-Za-z0-9-]*\d[A-Za-z0-9-]*)"
    r"(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_SLACK_LINK_PATTERN = re.compile(r"<(https?://[^|>\s]+)\|([^>\n]+)>")
_MAX_PRIVATE_LINK_URI_CHARS = 16_384


FileLookup = Callable[..., tuple[str, dict[str, Any]]]
LogUpload = Callable[..., tuple[str, dict[str, Any]]]
DeviceContextsLoader = Callable[
    [str, str, dict[str, Any]],
    list[dict[str, Any]],
]
HospitalRoomDeviceLoader = Callable[
    [str, str],
    list[dict[str, Any]],
]


def _s3_query_enabled() -> bool:
    return bool(s.S3_QUERY_ENABLED)


def _db_configured() -> bool:
    return bool(
        s.DB_HOST
        and s.DB_USERNAME
        and s.DB_PASSWORD
        and s.DB_DATABASE
    )


def _device_runtime_configured() -> bool:
    return bool(
        cs.MDA_GRAPHQL_URL
        and cs.MDA_ADMIN_USER_PASSWORD
        and cs.DEVICE_SSH_PASSWORD
    )


def _mda_configured() -> bool:
    return bool(cs.MDA_GRAPHQL_URL and cs.MDA_ADMIN_USER_PASSWORD)


def _download_configured() -> bool:
    return bool(cs.DEVICE_FILE_DOWNLOAD_BUCKET)


def _recovery_enabled() -> bool:
    return bool(cs.DEVICE_FILE_RECOVERY_ENABLED)


def _recovery_configured() -> bool:
    return bool(cs.BOX_UPLOADER_BASE_URL and cs.UPLOADER_JWT_SECRET)


def _load_device_contexts_for_file_operation(
    barcode: str,
    log_date: str,
    recordings_context: dict[str, Any],
) -> list[dict[str, Any]]:
    # 날짜 exact 매핑을 먼저 쓰고, 없을 때만 기존 최근 recordings 문맥으로
    # 보정한다. route가 이후 한 장비만 허용하므로 여기서는 mutation하지
    # 않는다.
    dated = _lookup_device_contexts_by_barcode_on_date(barcode, log_date)
    if dated:
        return dated
    return _lookup_device_contexts_by_barcode(
        barcode,
        recordings_context=recordings_context,
    )


@dataclass(frozen=True, slots=True)
class DeviceFileOperationsRouteDeps:
    """기존 파일 도메인 실행을 API route에서 한 번만 호출하도록 주입한다."""

    s3_client_factory: Callable[[], Any] = _build_s3_client
    recordings_loader: Callable[[str], dict[str, Any]] = (
        _load_recordings_context_by_barcode
    )
    device_contexts_loader: DeviceContextsLoader = (
        _load_device_contexts_for_file_operation
    )
    hospital_room_device_loader: HospitalRoomDeviceLoader = (
        _lookup_device_contexts_by_hospital_room
    )
    locate_files: FileLookup = _locate_barcode_file_candidates
    check_log_upload: LogUpload = _check_and_request_device_log_upload
    send_device_command: Callable[..., dict[str, Any]] = (
        _send_mda_device_command
    )
    s3_query_enabled: Callable[[], bool] = _s3_query_enabled
    db_configured: Callable[[], bool] = _db_configured
    mda_configured: Callable[[], bool] = _mda_configured
    device_runtime_configured: Callable[[], bool] = (
        _device_runtime_configured
    )
    download_configured: Callable[[], bool] = _download_configured
    recovery_enabled: Callable[[], bool] = _recovery_enabled
    recovery_configured: Callable[[], bool] = _recovery_configured


def match_device_file_operation_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """외부 호출 없이 explicit 단일 장비 또는 바코드 작업만 분류한다."""

    if (
        str(request.metadata.get("route_group") or "").strip()
        != _OPERATIONS_ROUTE_GROUP
    ):
        return None
    try:
        # metadata scope가 질문과 다르면 공통 runtime guard가 값 노출 없이
        # 응답하게 matcher에서는 실행 대상으로 고르지 않는다.
        scope = resolve_assistant_request_scope(request)
    except AssistantRequestScopeMismatch:
        return None

    question = request.question
    explicit_devices = _explicit_device_names(question)
    if _is_device_log_upload_check_request(question):
        if len(explicit_devices) > 1:
            return None
        hospital_name, room_name = _request_log_hospital_room(request)
        if len(explicit_devices) == 1 or (hospital_name and room_name):
            return DEVICE_LOG_UPLOAD_ROUTE
        return None

    explicit_barcodes = _explicit_barcodes(question)
    if len(explicit_barcodes) > 1 or len(explicit_devices) > 1:
        return None
    barcode = _single_request_barcode(request)
    if barcode is None:
        return None
    if _is_recording_streaming_restore_request(question, barcode):
        try:
            _extract_recording_streaming_restore_month(question)
        except ValueError:
            # 일자가 지정된 장비 파일 복구는 아래 파일 route가 소유한다.
            pass
        else:
            # 월 단위 MDA recordings 복원은 기존 전용 operation이 소유한다.
            return None
    if not _is_barcode_device_file_probe_request(question, barcode):
        return None
    if _should_recover_device_files(question):
        return DEVICE_FILE_RECOVERY_ROUTE
    if _should_download_device_files(question):
        return DEVICE_FILE_DOWNLOAD_ROUTE
    return DEVICE_FILE_LOOKUP_ROUTE


class DeviceFileOperationsAssistantRoute:
    """로그·장비 파일 작업을 채널 중립 최종 결과로 실행한다."""

    name = "device_file_operations"

    def __init__(
        self,
        deps: DeviceFileOperationsRouteDeps | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps or DeviceFileOperationsRouteDeps()
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        route = match_device_file_operation_route(request)
        if route is None:
            return None
        if route in {
            DEVICE_LOG_UPLOAD_ROUTE,
            DEVICE_FILE_LOOKUP_ROUTE,
            DEVICE_FILE_DOWNLOAD_ROUTE,
            DEVICE_FILE_RECOVERY_ROUTE,
        } and not is_explicit_operation_execution(request.question):
            # 조회 가능 여부나 방법 질문은 S3 확인 뒤 장비 명령으로 이어질 수
            # 있으므로 실행하지 않고 명시적인 재요청을 받는다.
            return _result(
                route=route,
                outcome="needs_input",
                body="실제 실행 요청이면 질문형 없이 작업을 명시해서 다시 요청해줘",
                fallback_reason="explicit_execution_required",
            )
        try:
            if route == DEVICE_LOG_UPLOAD_ROUTE:
                return self._handle_log_upload(request)
            return self._handle_file_operation(request, route=route)
        except ValueError as exc:
            self._log_failure(request, route, exc)
            return _result(
                route=route,
                outcome="needs_input",
                body=(
                    "장비 파일 요청 형식이 올바르지 않아. "
                    "대상과 날짜를 다시 확인해줘"
                ),
                fallback_reason="invalid_request",
            )
        except Exception as exc:
            # 장비 경로, presigned URL, credential이 예외 원문에 섞일 수 있어
            # request id와 오류 타입만 남긴다.
            self._log_failure(request, route, exc)
            return _result(
                route=route,
                outcome="failed",
                body=(
                    "장비 파일 요청 처리 중 오류가 발생했어. "
                    "잠시 후 다시 시도해줘"
                ),
                fallback_reason="operation_error",
            )

    def _handle_log_upload(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult:
        if not self._deps.s3_query_enabled():
            return _result(
                route=DEVICE_LOG_UPLOAD_ROUTE,
                outcome="failed",
                body="장비 로그 업로드 확인을 위해 S3_QUERY_ENABLED=true가 필요해",
                fallback_reason="s3_not_configured",
            )
        device_name = _single_explicit_device_name(request.question)
        if device_name is None:
            hospital_name, room_name = _request_log_hospital_room(request)
            if hospital_name and room_name:
                contexts = _dedupe_device_contexts(
                    self._deps.hospital_room_device_loader(
                        hospital_name,
                        room_name,
                    )
                )
                if len(contexts) == 1:
                    device_name = str(
                        contexts[0].get("deviceName") or ""
                    ).strip()
                elif len(contexts) > 1:
                    return _result(
                        route=DEVICE_LOG_UPLOAD_ROUTE,
                        outcome="needs_input",
                        body="병원과 병실에 연결된 장비가 여러 대라 장비명이 필요해",
                        fallback_reason="device_scope_ambiguous",
                    )
        if device_name is None:
            return _result(
                route=DEVICE_LOG_UPLOAD_ROUTE,
                outcome="needs_input",
                body="로그를 확인할 장비명을 하나만 입력해줘",
                fallback_reason="device_scope_required",
            )
        log_date, has_requested_date = _extract_log_upload_date(request)

        def dispatch(device: str, command: str) -> dict[str, Any]:
            # sendCommand는 인증 오류에도 재전송하지 않는 MDA helper를
            # 한 번만 호출한다.
            return self._deps.send_device_command(device, command=command)

        result_text, _ = self._deps.check_log_upload(
            self._deps.s3_client_factory(),
            device_name,
            log_date,
            has_requested_date=has_requested_date,
            dispatch_device_command=(
                dispatch if self._deps.mda_configured() else None
            ),
        )
        return _result(
            route=DEVICE_LOG_UPLOAD_ROUTE,
            outcome="answered",
            body=slack_mrkdwn_to_commonmark(result_text),
        )

    def _handle_file_operation(
        self,
        request: CompanyAssistantRequest,
        *,
        route: str,
    ) -> CompanyAssistantResult:
        config_result = self._file_config_result(route, request.question)
        if config_result is not None:
            return config_result

        log_date, has_requested_date = _extract_log_date_with_presence(
            request.question
        )
        if not has_requested_date:
            return _result(
                route=route,
                outcome="needs_input",
                body=(
                    "파일 확인 날짜를 같이 입력해줘. "
                    "예: `48194663047 2026-03-06 파일 있나`"
                ),
                fallback_reason="date_required",
            )
        barcode = _single_request_barcode(request)
        if barcode is None:
            return _result(
                route=route,
                outcome="needs_input",
                body="파일을 확인할 11자리 바코드를 하나만 입력해줘",
                fallback_reason="barcode_scope_required",
            )

        recordings_context = self._deps.recordings_loader(barcode)
        explicit_device = _single_explicit_device_name(request.question)
        scope = resolve_assistant_request_scope(request)
        if explicit_device is not None:
            device_contexts = [{"deviceName": explicit_device}]
        elif scope.hospital_name and scope.room_name:
            device_contexts = self._deps.hospital_room_device_loader(
                scope.hospital_name,
                scope.room_name,
            )
        else:
            device_contexts = self._deps.device_contexts_loader(
                barcode,
                log_date,
                recordings_context,
            )
        device_contexts = _dedupe_device_contexts(device_contexts)
        if len(device_contexts) != 1:
            reason = (
                "바코드와 날짜에 연결된 장비를 찾지 못했어"
                if not device_contexts
                else (
                    "바코드와 날짜에 연결된 장비가 여러 대라 "
                    "장비명이 필요해"
                )
            )
            return _result(
                route=route,
                outcome="needs_input",
                body=slack_mrkdwn_to_commonmark(
                    _build_device_file_scope_request_message(barcode, reason)
                ),
                fallback_reason="device_scope_required",
            )

        question = request.question
        download = route == DEVICE_FILE_DOWNLOAD_ROUTE
        recovery = route == DEVICE_FILE_RECOVERY_ROUTE
        result_text, payload = self._deps.locate_files(
            self._deps.s3_client_factory(),
            barcode,
            log_date,
            # 이미 exact 한 장비를 정했으므로 recordings 병원 범위로 다시
            # 확장하지 않는다.
            recordings_context=None,
            device_contexts=device_contexts,
            probe_remote_files=_should_probe_device_files(question),
            download_remote_files=download,
            recover_remote_files=recovery,
            compact_file_list=_should_render_compact_device_file_list(
                question
            ),
            compact_file_id=_should_render_compact_file_id_result(question),
            compact_download=_should_render_compact_device_download_result(
                question
            ),
            compact_recovery=_should_render_compact_device_recovery_result(
                question
            ),
            # API mutation은 endpoint open과 SSH probe를 같은 요청 안에서
            # 재전송하지 않는다. 첫 조회/open/poll 한 흐름만 허용한다.
            retry_remote_probe=False,
            resend_ssh_open=False,
        )
        if not download:
            return _result(
                route=route,
                outcome="answered",
                body=slack_mrkdwn_to_commonmark(result_text),
            )
        return _download_result(
            route=route,
            barcode=barcode,
            log_date=log_date,
            result_text=result_text,
            payload=payload,
        )

    def _file_config_result(
        self,
        route: str,
        question: str,
    ) -> CompanyAssistantResult | None:
        if not self._deps.s3_query_enabled():
            return _result(
                route=route,
                outcome="failed",
                body=(
                    "파일 확인 대상 세션 조회를 위해 "
                    "S3_QUERY_ENABLED=true가 필요해"
                ),
                fallback_reason="s3_not_configured",
            )
        if not self._deps.db_configured():
            return _result(
                route=route,
                outcome="failed",
                body=(
                    "파일 확인 대상 세션 조회를 위해 "
                    "DB 접속 정보(DB_*)가 필요해"
                ),
                fallback_reason="db_not_configured",
            )
        if (
            _should_probe_device_files(question)
            and not self._deps.device_runtime_configured()
        ):
            return _result(
                route=route,
                outcome="failed",
                body=slack_mrkdwn_to_commonmark(
                    _build_device_file_probe_config_message()
                ),
                fallback_reason="device_runtime_not_configured",
            )
        if (
            route == DEVICE_FILE_DOWNLOAD_ROUTE
            and not self._deps.download_configured()
        ):
            return _result(
                route=route,
                outcome="failed",
                body=slack_mrkdwn_to_commonmark(
                    _build_device_file_download_config_message()
                ),
                fallback_reason="download_not_configured",
            )
        if route == DEVICE_FILE_RECOVERY_ROUTE:
            if not self._deps.recovery_enabled():
                return _result(
                    route=route,
                    outcome="denied",
                    body="장비 영상 복구 기능은 현재 비활성화돼 있어",
                    fallback_reason="recovery_disabled",
                )
            if not self._deps.recovery_configured():
                return _result(
                    route=route,
                    outcome="failed",
                    body=slack_mrkdwn_to_commonmark(
                        _build_device_file_recovery_config_message()
                    ),
                    fallback_reason="recovery_not_configured",
                )
        return None

    def _log_failure(
        self,
        request: CompanyAssistantRequest,
        route: str,
        error: Exception,
    ) -> None:
        self._logger.warning(
            "Device file operation failed request_id=%s route=%s error_type=%s",
            request.request_id,
            route,
            type(error).__name__,
        )


def _download_result(
    *,
    route: str,
    barcode: str,
    log_date: str,
    result_text: str,
    payload: dict[str, Any],
) -> CompanyAssistantResult:
    links = _collect_private_download_links(payload)
    private_body = slack_mrkdwn_to_commonmark(
        _remove_slack_links(result_text)
    )
    if not links:
        return _result(
            route=route,
            outcome="answered",
            body=private_body,
        )

    used_expanded_scope = bool(
        ((payload.get("request") or {}) if isinstance(payload, dict) else {}).get(
            "usedExpandedScope"
        )
    )
    conversation_lines = [
        "**장비 영상 다운로드 준비 완료**",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
        f"• 다운로드 링크: `{len(links)}개`",
        "• 전달: 요청자 DM",
    ]
    if used_expanded_scope:
        conversation_lines.append(
            "• 참고: 매핑 장비 외 같은 병원 장비도 함께 검색했어"
        )
    return CompanyAssistantResult(
        route=route,
        outcome="answered",
        messages=(
            AssistantMessage(
                body="\n".join(conversation_lines),
                mention_actor=True,
            ),
            AssistantMessage(
                body=private_body,
                delivery_scope="requester",
                mention_actor=False,
                private_links=links,
            ),
        ),
    )


def _collect_private_download_links(
    payload: dict[str, Any],
) -> tuple[AssistantLink, ...]:
    links: list[AssistantLink] = []
    seen_uris: set[str] = set()
    for record in payload.get("records") or []:
        if not isinstance(record, dict):
            continue
        for session in record.get("sessions") or []:
            if not isinstance(session, dict):
                continue
            download = session.get("download")
            if not isinstance(download, dict):
                continue
            for item in download.get("downloads") or []:
                if not isinstance(item, dict) or not item.get("ok"):
                    continue
                uri = str(item.get("url") or "").strip()
                label = str(item.get("fileName") or "파일").strip()
                if (
                    not _is_safe_private_link_uri(uri)
                    or uri in seen_uris
                    or not label
                ):
                    continue
                seen_uris.add(uri)
                links.append(AssistantLink(label=label[:255], uri=uri))
    return tuple(links)


def _remove_slack_links(text: str) -> str:
    # presigned URL은 CommonMark 본문이나 source로 승격하지 않고, label만
    # 남겨 privateLinks transport 외 경로에서 재노출되지 않게 한다.
    return _SLACK_LINK_PATTERN.sub(
        lambda matched: f"`{matched.group(2).replace('`', '') or '파일'}`",
        str(text or ""),
    )


def _is_safe_private_link_uri(uri: str) -> bool:
    if (
        not uri
        or len(uri) > _MAX_PRIVATE_LINK_URI_CHARS
        or any(character.isspace() or ord(character) < 32 for character in uri)
        or any(character in uri for character in "<>|")
    ):
        return False
    try:
        parsed = urlsplit(uri)
    except ValueError:
        return False
    return bool(
        parsed.scheme.lower() == "https"
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
    )


def _explicit_barcodes(question: str) -> dict[str, str]:
    return {
        matched.group(1): matched.group(1)
        for matched in _BARCODE_PATTERN.finditer(str(question or ""))
    }


def _single_explicit_barcode(question: str) -> str | None:
    values = _explicit_barcodes(question)
    return next(iter(values.values())) if len(values) == 1 else None


def _single_request_barcode(
    request: CompanyAssistantRequest,
) -> str | None:
    explicit = _single_explicit_barcode(request.question)
    if explicit is not None:
        return explicit
    metadata_barcode = str(request.metadata.get("barcode") or "").strip()
    if _BARCODE_PATTERN.fullmatch(metadata_barcode):
        return metadata_barcode
    context_barcodes: dict[str, str] = {}
    for entry in _actor_context_entries(request):
        for matched in _BARCODE_PATTERN.finditer(
            str(entry.get("text") or "")
        ):
            context_barcodes.setdefault(matched.group(1), matched.group(1))
    if len(context_barcodes) != 1:
        return None
    return next(iter(context_barcodes.values()))


def _request_log_hospital_room(
    request: CompanyAssistantRequest,
) -> tuple[str | None, str | None]:
    hospital_name, room_name = _extract_hospital_room_scope_for_log_upload(
        request.question
    )
    if hospital_name and room_name:
        return hospital_name, room_name
    for entry in reversed(_actor_context_entries(request)):
        hospital_name, room_name = (
            _extract_hospital_room_scope_for_log_upload(
                str(entry.get("text") or "")
            )
        )
        if hospital_name and room_name:
            return hospital_name, room_name
    return None, None


def _extract_log_upload_date(
    request: CompanyAssistantRequest,
) -> tuple[str, bool]:
    # `2층1-1진료실` 같은 room 식별자가 단축 날짜로 오인되지 않도록
    # 이미 확정한 병원/병실 scope만 제거한 뒤 기존 날짜 parser를 쓴다.
    date_question = request.question
    hospital_name, room_name = _request_log_hospital_room(request)
    for scope_value in (hospital_name, room_name):
        if scope_value:
            date_question = date_question.replace(scope_value, " ")
    return _extract_log_date_with_presence(date_question)


def _actor_context_entries(
    request: CompanyAssistantRequest,
) -> tuple[dict[str, Any], ...]:
    actor_id = str(request.actor_id or "").strip()
    if not actor_id:
        return ()
    return tuple(
        entry
        for entry in request.context_entries
        if isinstance(entry, dict)
        and str(entry.get("author_id") or "").strip() == actor_id
    )


def needs_device_file_operation_context(question: str) -> bool:
    """현재 질문에 exact target이 없을 때만 Slack thread 문맥을 요청한다."""

    normalized = str(question or "")
    if _is_device_log_upload_check_request(normalized):
        devices = _explicit_device_names(normalized)
        hospital_name, room_name = _extract_hospital_room_scope_for_log_upload(
            normalized
        )
        return not devices and not (hospital_name and room_name)
    if _single_explicit_barcode(normalized) is not None:
        return False
    return _is_barcode_device_file_probe_request(normalized, "context")


def has_ambiguous_device_file_operation_scope(
    request: CompanyAssistantRequest,
) -> bool:
    """복수 target과 actor-safe thread scope 부재를 local 진입 전에 막는다."""

    if str(request.metadata.get("route_group") or "").strip() != (
        _OPERATIONS_ROUTE_GROUP
    ):
        return False
    question = request.question
    devices = _explicit_device_names(question)
    barcodes = _explicit_barcodes(question)
    first_barcode = next(iter(barcodes.values()), None)
    is_log_upload = _is_device_log_upload_check_request(question)
    is_file_operation = bool(
        first_barcode
        and _is_barcode_device_file_probe_request(
            question,
            first_barcode,
        )
    )
    is_streaming_restore = bool(
        first_barcode
        and _is_recording_streaming_restore_request(
            question,
            first_barcode,
        )
    )
    if len(devices) > 1 and (
        is_log_upload or is_file_operation or is_streaming_restore
    ):
        return True
    if len(barcodes) > 1 and (
        is_file_operation or is_streaming_restore
    ):
        return True
    if not needs_device_file_operation_context(question):
        return False

    actor_entries = _actor_context_entries(request)
    if is_log_upload:
        hospital_rooms = {
            (hospital_name, room_name)
            for entry in actor_entries
            for hospital_name, room_name in (
                _extract_hospital_room_scope_for_log_upload(
                    str(entry.get("text") or "")
                ),
            )
            if hospital_name and room_name
        }
        return len(hospital_rooms) != 1

    context_barcodes = {
        matched.group(1)
        for entry in actor_entries
        for matched in _BARCODE_PATTERN.finditer(
            str(entry.get("text") or "")
        )
    }
    return len(context_barcodes) != 1


def _explicit_device_names(question: str) -> dict[str, str]:
    normalized = re.sub(r"<@[^>]+>", " ", str(question or ""))
    candidates = [
        *(match.group(1) for match in _DEVICE_NAME_LABEL_PATTERN.finditer(normalized)),
        *(match.group(1) for match in _DEVICE_NAME_TOKEN_PATTERN.finditer(normalized)),
    ]
    exact: dict[str, str] = {}
    for raw_candidate in candidates:
        candidate = str(raw_candidate or "").strip().strip("`'\"")
        if candidate and cs.S3_DEVICE_NAME_PATTERN.fullmatch(candidate):
            exact.setdefault(candidate.casefold(), candidate)
    return exact


def _single_explicit_device_name(question: str) -> str | None:
    values = _explicit_device_names(question)
    return next(iter(values.values())) if len(values) == 1 else None


def _dedupe_device_contexts(
    contexts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in contexts or []:
        if not isinstance(item, dict):
            continue
        device_name = str(item.get("deviceName") or "").strip()
        key = device_name.casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _result(
    *,
    route: str,
    outcome: AssistantOutcome,
    body: str,
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,
        messages=(AssistantMessage(body=body),),
        fallback_reason=fallback_reason,
    )


__all__ = [
    "DEVICE_FILE_DOWNLOAD_ROUTE",
    "DEVICE_FILE_LOOKUP_ROUTE",
    "DEVICE_FILE_RECOVERY_ROUTE",
    "DEVICE_LOG_UPLOAD_ROUTE",
    "DeviceFileOperationsAssistantRoute",
    "DeviceFileOperationsRouteDeps",
    "has_ambiguous_device_file_operation_scope",
    "match_device_file_operation_route",
    "needs_device_file_operation_context",
]
