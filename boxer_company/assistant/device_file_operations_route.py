from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
import json
import logging
import re
import threading
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
    SourceReference,
)
from boxer_company.routers.barcode_log import (
    _extract_hospital_room_scope,
    _extract_log_date_with_presence,
)
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
from boxer_company.routers.mda_graphql import (
    _create_mda_activity_log,
    _send_mda_device_command,
)
from boxer_company.routers.recording_streaming_restore import (
    _extract_recording_streaming_restore_month,
    _is_recording_streaming_restore_request,
)


DEVICE_LOG_UPLOAD_ROUTE = "device_log_upload"
DEVICE_FILE_LOOKUP_ROUTE = "device_file_lookup"
DEVICE_FILE_DOWNLOAD_ROUTE = "device_file_download"
DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE = (
    "device_file_download_barcode_required"
)
DEVICE_FILE_RECOVERY_ROUTE = "device_file_recovery"
DEVICE_FILE_DOWNLOAD_DELIVERY_ACTION = (
    "device_file_download_delivery"
)
TRUSTED_MDA_RECOVERY_SCOPE_METADATA_KEY = (
    "trusted_mda_recovery_scope"
)

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
_MAX_DOWNLOAD_DELIVERY_STATES = 1_024


FileLookup = Callable[..., tuple[str, dict[str, Any]]]
LogUpload = Callable[..., tuple[str, dict[str, Any]]]
ActivityLog = Callable[[dict[str, Any]], dict[str, Any]]
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
    # 보정한다. 다중 장비도 legacy 파일 검색 범위로 그대로 반환한다.
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
    create_activity_log: ActivityLog = _create_mda_activity_log
    s3_query_enabled: Callable[[], bool] = _s3_query_enabled
    db_configured: Callable[[], bool] = _db_configured
    mda_configured: Callable[[], bool] = _mda_configured
    device_runtime_configured: Callable[[], bool] = (
        _device_runtime_configured
    )
    download_configured: Callable[[], bool] = _download_configured
    recovery_enabled: Callable[[], bool] = _recovery_enabled
    recovery_configured: Callable[[], bool] = _recovery_configured


@dataclass(frozen=True, slots=True)
class _DeviceFileDownloadDelivery:
    """URL 없이 receipt가 되돌려주는 다운로드 완료 근거다."""

    barcode: str
    log_date: str
    records: tuple[dict[str, Any], ...]
    used_expanded_scope: bool


def match_device_file_operation_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """외부 호출 없이 기존 Slack parser의 첫 작업 범위를 분류한다."""

    if (
        str(request.metadata.get("route_group") or "").strip()
        != _OPERATIONS_ROUTE_GROUP
    ):
        return None
    if is_device_file_download_delivery_receipt(request):
        # DM 성공 뒤 같은 request ID로 들어오는 typed receipt는 자연어를
        # 다시 실행하지 않고 URL 없는 delivery manifest만 확정한다.
        return DEVICE_FILE_DOWNLOAD_ROUTE
    question = request.question
    if (
        _single_request_barcode(request) is None
        and _is_missing_barcode_device_download_request(question)
    ):
        # 기존 Slack의 바코드 선행 안내를 장비 음성/상태 mutation보다 먼저
        # 확정해 복합 문장이 다른 작업으로 내려가지 않게 한다.
        return DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE
    explicit_devices = _explicit_device_names(question)
    if _is_device_log_upload_check_request(question):
        hospital_name, room_name = _request_log_hospital_room(request)
        if explicit_devices or (hospital_name and room_name):
            return DEVICE_LOG_UPLOAD_ROUTE
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


def is_device_file_download_delivery_receipt(
    request: CompanyAssistantRequest,
) -> bool:
    """고정된 delivered action만 다운로드 전달 receipt로 인정한다."""

    if (
        str(request.metadata.get("route_group") or "").strip()
        != _OPERATIONS_ROUTE_GROUP
    ):
        return False
    action = request.metadata.get("operation_action")
    return bool(
        isinstance(action, Mapping)
        and frozenset(action) == {"name", "phase", "delivery"}
        and str(action.get("name") or "").strip()
        == DEVICE_FILE_DOWNLOAD_DELIVERY_ACTION
        and str(action.get("phase") or "").strip() == "delivered"
    )


def _is_missing_barcode_device_download_request(question: str) -> bool:
    if _single_explicit_barcode(question) is not None:
        return False
    if not _should_download_device_files(question):
        return False
    normalized = str(question or "").strip()
    availability_hints = (
        "다운로드 가능 상태",
        "다운로드 가능 여부",
        "다운로드 가능한지",
        "다운 가능 상태",
        "다운 가능 여부",
        "다운 가능한지",
    )
    return not any(hint in normalized for hint in availability_hints)


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
        # presigned URL은 초기 응답에만 담고 receipt는 URL 없는 manifest를
        # 돌려준다. 같은 request ID의 중복 ack가 activity를 다시 쓰지
        # 않도록 완료 결과만 작은 process-local cache로 유지한다.
        self._delivery_lock = threading.Lock()
        self._completed_deliveries: OrderedDict[
            str,
            CompanyAssistantResult,
        ] = OrderedDict()
        self._delivery_in_flight: set[str] = set()

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        route = match_device_file_operation_route(request)
        if route is None:
            return None
        try:
            if is_device_file_download_delivery_receipt(request):
                return self._handle_download_delivery_receipt(request)
            if route == DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE:
                return _result(
                    route=route,
                    outcome="needs_input",
                    body=(
                        "영상 다운로드는 바코드 없이는 특정할 수 없어.\n"
                        "11자리 바코드랑 날짜를 같이 보내줘. "
                        "예: `12345678910 2026-04-28 영상 다운로드`"
                    ),
                    fallback_reason="barcode_scope_required",
                )
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
            # Slack 로컬과 같은 MDA helper로 장비 업로드 명령을 보낸다.
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
        hospital_name, room_name = _request_file_hospital_room(request)
        device_contexts: list[dict[str, Any]] | None
        if explicit_device is not None:
            # Slack 로컬의 2차 입력처럼 명시 장비를 가장 먼저 쓴다.
            device_contexts = [
                {
                    "deviceName": explicit_device,
                    "hospitalName": hospital_name,
                    "roomName": room_name,
                }
            ]
        elif hospital_name and room_name:
            device_contexts = self._deps.hospital_room_device_loader(
                hospital_name,
                room_name,
            )
            if not device_contexts:
                return _result(
                    route=route,
                    outcome="needs_input",
                    body=slack_mrkdwn_to_commonmark(
                        _build_device_file_scope_request_message(
                            barcode,
                            (
                                "입력한 병원명/병실명으로 장비를 "
                                "찾지 못했어. MDA 표시 이름과 정확히 "
                                "일치하게 입력해줘"
                            ),
                        )
                    ),
                    fallback_reason="device_scope_required",
                )
        else:
            summary = recordings_context.get("summary") or {}
            recording_count = int(summary.get("recordingCount") or 0)
            has_device_mapping = any(
                isinstance(row, dict) and row.get("deviceSeq") is not None
                for row in recordings_context.get("rows") or []
            )
            if recording_count > 0 and not has_device_mapping:
                # 기존 Slack처럼 최근 row에 장비 매핑이 없을 때만
                # 요청 날짜 recordings로 병원/병실 장비를 보정한다.
                device_contexts = self._deps.device_contexts_loader(
                    barcode,
                    log_date,
                    recordings_context,
                )
            elif recording_count <= 0:
                device_contexts = []
            else:
                # 일반 경로는 domain helper가 recordings 문맥에서 다중
                # 장비와 같은 병원 확장 범위를 기존과 같이 결정한다.
                device_contexts = None

        if device_contexts is not None and not device_contexts:
            # Slack adapter가 현재 bot의 MDA 복구 알림 root를 검증한 경우에만
            # typed metadata로 넘긴 exact 장비 문맥을 기존 fallback으로 쓴다.
            device_contexts = _trusted_mda_recovery_device_contexts(
                request,
                barcode=barcode,
                log_date=log_date,
            )
            if not device_contexts:
                return _result(
                    route=route,
                    outcome="needs_input",
                    body=slack_mrkdwn_to_commonmark(
                        _build_device_file_scope_request_message(
                            barcode,
                            "recordings 장비 매핑이 없어 2차 입력이 필요해",
                        )
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
            # Slack 로컬과 같이 recordings 병원 범위 확장과
            # 다중 장비 검색을 domain helper에 그대로 넘긴다.
            recordings_context=recordings_context,
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
            # API는 실패한 SSH probe를 이유로 tunnel을 다시 열지 않고,
            # 최초 open 뒤 poll도 조회만 수행한다.
            retry_remote_probe=False,
            resend_ssh_open=False,
        )
        if not download:
            rendered_body = slack_mrkdwn_to_commonmark(result_text)
            mda_search_uri = (
                f"https://mda.kr.mmtalkbox.com/cs?search={barcode}"
            )
            # 복구 성공 결과의 legacy `열기` 링크는 source allowlist에도
            # 등록해야 Slack renderer가 클릭 가능한 링크로 보존한다.
            sources = (
                (
                    SourceReference(
                        source_id=f"mda-recovery:{barcode}",
                        title="MDA 복구 결과 열기",
                        uri=mda_search_uri,
                    ),
                )
                if recovery and mda_search_uri in result_text
                else ()
            )
            return CompanyAssistantResult(
                route=route,
                outcome="answered",
                messages=(AssistantMessage(body=rendered_body),),
                sources=sources,
            )
        download_records = _collect_device_download_records(payload)
        links, link_contexts = _collect_private_download_links(
            download_records
        )
        if not links:
            return _result(
                route=route,
                outcome="answered",
                body=slack_mrkdwn_to_commonmark(
                    _remove_slack_links(result_text)
                ),
            )
        used_expanded_scope = bool(
            (
                (payload.get("request") or {})
                if isinstance(payload, dict)
                else {}
            ).get("usedExpandedScope")
        )
        return _download_pending_result(
            route=route,
            barcode=barcode,
            log_date=log_date,
            records=download_records,
            links=links,
            link_contexts=link_contexts,
            used_expanded_scope=used_expanded_scope,
        )

    def _handle_download_delivery_receipt(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult:
        request_id = str(request.request_id or "").strip()
        with self._delivery_lock:
            completed = self._completed_deliveries.get(request_id)
            if completed is not None:
                # Slack/API transport가 수동으로 같은 ack를 다시 보내도 MDA
                # activity는 다시 만들지 않고 이미 확정한 결과만 돌려준다.
                self._completed_deliveries.move_to_end(request_id)
                return completed
            if request_id in self._delivery_in_flight:
                return _result(
                    route=DEVICE_FILE_DOWNLOAD_ROUTE,
                    outcome="failed",
                    body="다운로드 전달 내역을 확정하고 있어",
                    fallback_reason="download_delivery_receipt_in_progress",
                )
            self._delivery_in_flight.add(request_id)

        delivery = _download_delivery_from_receipt(request)
        if delivery is None:
            with self._delivery_lock:
                self._delivery_in_flight.discard(request_id)
            return _result(
                route=DEVICE_FILE_DOWNLOAD_ROUTE,
                outcome="denied",
                body="다운로드 전달 내역 형식이 올바르지 않아",
                fallback_reason="download_delivery_receipt_invalid",
            )
        try:
            activity_count = self._log_download_activities(
                request,
                barcode=delivery.barcode,
                log_date=delivery.log_date,
                records=list(delivery.records),
            )
            result = _download_delivered_result(
                barcode=delivery.barcode,
                log_date=delivery.log_date,
                records=list(delivery.records),
                activity_logged=activity_count > 0,
                used_expanded_scope=delivery.used_expanded_scope,
            )
        except Exception:
            with self._delivery_lock:
                self._delivery_in_flight.discard(request_id)
            raise

        with self._delivery_lock:
            # completed 저장과 in-flight 해제를 한 critical section으로
            # 묶어 동시 ack가 사이에 들어와 activity를 중복 생성하지
            # 않게 한다.
            self._completed_deliveries[request_id] = result
            self._completed_deliveries.move_to_end(request_id)
            self._delivery_in_flight.discard(request_id)
            while (
                len(self._completed_deliveries)
                > _MAX_DOWNLOAD_DELIVERY_STATES
            ):
                self._completed_deliveries.popitem(last=False)
        return result

    def _log_download_activities(
        self,
        request: CompanyAssistantRequest,
        *,
        barcode: str,
        log_date: str,
        records: list[dict[str, Any]],
    ) -> int:
        success_count = 0
        for record in records:
            try:
                # 기존 Slack activity와 같은 record 단위 payload를 만들되
                # user/channel/thread는 HTTP request metadata에서 가져온다.
                self._deps.create_activity_log(
                    _build_device_download_activity_input(
                        record=record,
                        barcode=barcode,
                        log_date=log_date,
                        question=request.question,
                        user_id=str(request.actor_id or "").strip(),
                        user_name=_request_actor_name(request),
                        channel_id=_request_channel_id(request),
                        thread_ts=request.conversation_id,
                    )
                )
                success_count += 1
            except Exception as exc:
                # 기록 실패가 이미 준비된 요청자 전용 링크 전달을 막지 않는다.
                self._logger.warning(
                    "Device download activity log failed request_id=%s "
                    "error_type=%s",
                    request.request_id,
                    type(exc).__name__,
                )
        return success_count

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


def _download_pending_result(
    *,
    route: str,
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
    links: tuple[AssistantLink, ...],
    link_contexts: tuple[dict[str, str], ...],
    used_expanded_scope: bool,
) -> CompanyAssistantResult:
    """링크를 공개하지 않고 Slack의 DM receipt를 기다리는 결과를 만든다."""

    private_body = _render_device_download_dm_text(
        barcode,
        log_date,
        records,
    )
    failure_notice = _render_device_download_dm_failure_notice(
        barcode,
        log_date,
        records,
        used_expanded_scope=used_expanded_scope,
    )
    return CompanyAssistantResult(
        route=route,
        outcome="answered",
        messages=(
            AssistantMessage(
                body=private_body,
                delivery_scope="requester",
                mention_actor=False,
                private_links=links,
            ),
        ),
        operation_result={
            "kind": DEVICE_FILE_DOWNLOAD_DELIVERY_ACTION,
            "status": "pending",
            "failureNotice": failure_notice,
            "linkCount": len(links),
            "links": list(link_contexts),
            "delivery": _build_download_delivery_manifest(
                barcode=barcode,
                log_date=log_date,
                records=records,
                used_expanded_scope=used_expanded_scope,
            ),
        },
    )


def _download_delivered_result(
    *,
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
    activity_logged: bool,
    used_expanded_scope: bool,
) -> CompanyAssistantResult:
    """Slack DM 성공 receipt 뒤에만 공개 성공 안내를 만든다."""

    conversation_body = _render_device_download_thread_notice(
        barcode,
        log_date,
        records,
        activity_logged=activity_logged,
        used_expanded_scope=used_expanded_scope,
    )
    return CompanyAssistantResult(
        route=DEVICE_FILE_DOWNLOAD_ROUTE,
        outcome="answered",
        messages=(AssistantMessage(body=conversation_body),),
        sources=(
            (
                SourceReference(
                    source_id="mda-cs-activity",
                    title="CS 처리내역 엿보기",
                    uri="https://mda.kr.mmtalkbox.com/cs",
                ),
            )
            if activity_logged
            else ()
        ),
    )


def _collect_device_download_records(
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    """기존 Slack activity/notice가 쓰던 성공 download record를 만든다."""

    records: list[dict[str, Any]] = []
    for raw_record in payload.get("records") or []:
        if not isinstance(raw_record, dict):
            continue
        file_names: list[str] = []
        seen_files: set[str] = set()
        download_links: list[dict[str, str]] = []
        seen_links: set[str] = set()
        for session in raw_record.get("sessions") or []:
            if not isinstance(session, dict):
                continue
            probe = (
                session.get("probe")
                if isinstance(session.get("probe"), dict)
                else None
            )
            if probe and probe.get("ok"):
                for found_file in probe.get("files") or []:
                    file_name = str(found_file or "").strip().split("/")[-1]
                    if (
                        _is_safe_delivery_file_name(file_name)
                        and file_name not in seen_files
                    ):
                        seen_files.add(file_name)
                        file_names.append(file_name)
            download = (
                session.get("download")
                if isinstance(session.get("download"), dict)
                else None
            )
            if not download:
                continue
            for item in download.get("downloads") or []:
                if not isinstance(item, dict) or not item.get("ok"):
                    continue
                file_name = str(item.get("fileName") or "").strip()
                uri = str(item.get("url") or "").strip()
                if (
                    not _is_safe_delivery_file_name(file_name)
                    or not _is_safe_private_link_uri(uri)
                    or file_name in seen_links
                ):
                    continue
                seen_links.add(file_name)
                download_links.append(
                    {"fileName": file_name, "url": uri}
                )
        if not download_links:
            continue
        records.append(
            {
                "deviceName": str(
                    raw_record.get("deviceName") or ""
                ).strip()
                or "미확인",
                "deviceSeq": raw_record.get("deviceSeq"),
                "hospitalSeq": raw_record.get("hospitalSeq"),
                "hospitalRoomSeq": raw_record.get("hospitalRoomSeq"),
                "hospitalName": str(
                    raw_record.get("hospitalName") or ""
                ).strip()
                or "미확인",
                "roomName": str(
                    raw_record.get("roomName") or ""
                ).strip()
                or "미확인",
                "fileNames": file_names,
                "downloadLinks": download_links,
            }
        )
    return records


def _build_device_download_activity_input(
    *,
    record: dict[str, Any],
    barcode: str,
    log_date: str,
    question: str,
    user_id: str,
    user_name: str | None,
    channel_id: str,
    thread_ts: str,
) -> dict[str, Any]:
    """기존 MDA `recording.download` activity 계약을 그대로 보존한다."""

    device_name = str(record.get("deviceName") or "").strip() or "미확인"
    hospital_name = (
        str(record.get("hospitalName") or "").strip() or "미확인"
    )
    room_name = str(record.get("roomName") or "").strip() or "미확인"
    requester_name = str(user_name or "").strip()
    requester_label = requester_name or str(user_id or "").strip()
    file_names = [
        str(item).strip()
        for item in record.get("fileNames") or []
        if str(item).strip()
    ]
    download_file_names = _record_download_file_names(record)
    detail_log = {
        "source": "boxer_slack_device_download",
        "barcode": barcode,
        "logDate": log_date,
        "question": question,
        "slackUserId": user_id,
        "slackUserName": requester_name,
        "slackChannelId": channel_id,
        "slackThreadTs": thread_ts,
        "requestedBySlackUserId": user_id,
        "requestedBySlackUserName": requester_name,
        "deviceName": device_name,
        "deviceSeq": record.get("deviceSeq"),
        "hospitalSeq": record.get("hospitalSeq"),
        "hospitalRoomSeq": record.get("hospitalRoomSeq"),
        "hospitalName": hospital_name,
        "roomName": room_name,
        "fileNames": file_names,
        "downloadFileNames": download_file_names,
        "downloadLinkCount": len(download_file_names),
    }
    return {
        "activityType": "recording.download",
        "barcode": barcode or None,
        "hospitalSeq": record.get("hospitalSeq"),
        "hospitalRoomSeq": record.get("hospitalRoomSeq"),
        "deviceSeq": record.get("deviceSeq"),
        "targetEntityType": (
            "Device" if record.get("deviceSeq") is not None else None
        ),
        "targetEntitySeq": record.get("deviceSeq"),
        "reason": "Boxer Slack 다운로드 링크 전송 성공",
        "description": (
            "Boxer Slack 다운로드 링크 전송 완료: "
            f"병원명 [{hospital_name}], 병실명 [{room_name}], "
            f"장비명 [{device_name}]"
            f"{f', 요청자 [{requester_label}]' if requester_label else ''}, "
            f"파일 {len(download_file_names)}개"
        ),
        "detailLog": json.dumps(
            detail_log,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    }


def _render_device_download_dm_text(
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
) -> str:
    """기존 Slack 다운로드 요약 DM 문구를 그대로 만든다."""

    lines = [
        "**장비 영상 다운로드 결과**",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
    ]
    for record in records:
        lines.extend(
            (
                "",
                f"• 장비: `{record['deviceName']}`",
                f"• 병원: `{record['hospitalName']}`",
                f"• 병실: `{record['roomName']}`",
            )
        )
        file_names = record.get("fileNames") or []
        lines.append(
            f"• 장비에 존재하는 영상 목록: `{len(file_names)}개`"
        )
        lines.extend(f"  - `{file_name}`" for file_name in file_names)
        lines.append(
            "• 다운로드 링크: "
            f"`{len(_record_download_file_names(record))}개` "
            "(1시간, 파일별 별도 DM)"
        )
    return "\n".join(lines)


def _render_device_download_thread_notice(
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
    *,
    activity_logged: bool,
    used_expanded_scope: bool,
) -> str:
    lines = [
        "**장비 영상 다운로드 결과**",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
    ]
    if used_expanded_scope:
        lines.append("• 참고: 매핑 장비 외 같은 병원 장비도 함께 검색했어")
    for record in records:
        lines.extend(
            (
                "",
                f"• 장비: `{record['deviceName']}`",
                f"• 병원: `{record['hospitalName']}`",
                f"• 병실: `{record['roomName']}`",
            )
        )
        file_names = record.get("fileNames") or []
        lines.append(f"• 장비에 존재하는 영상 목록: `{len(file_names)}개`")
        lines.extend(f"  - `{file_name}`" for file_name in file_names)
        lines.append(
            "• 다운로드 링크: DM으로 보냈어 "
            f"(`{len(_record_download_file_names(record))}개`)"
        )
    if activity_logged:
        lines.extend(
            (
                "",
                "• 다운로드 내역 기록되었습니다. 🎣 "
                "[CS 처리내역 엿보기](https://mda.kr.mmtalkbox.com/cs)",
            )
        )
    return "\n".join(lines)


def _render_device_download_dm_failure_notice(
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
    *,
    used_expanded_scope: bool,
) -> str:
    """요약 또는 링크 DM 하나라도 실패했을 때의 기존 공개 안내다."""

    lines = [
        "**장비 영상 다운로드 결과**",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
    ]
    if used_expanded_scope:
        lines.append("• 참고: 매핑 장비 외 같은 병원 장비도 함께 검색했어")
    for record in records:
        lines.extend(
            (
                "",
                f"• 장비: `{record['deviceName']}`",
                f"• 병원: `{record['hospitalName']}`",
                f"• 병실: `{record['roomName']}`",
            )
        )
        file_names = record.get("fileNames") or []
        lines.append(
            f"• 장비에 존재하는 영상 목록: `{len(file_names)}개`"
        )
        lines.extend(f"  - `{file_name}`" for file_name in file_names)
    lines.append("• 다운로드 링크: DM 전송 실패. 봇 DM 권한을 확인해줘")
    return "\n".join(lines)


def _collect_private_download_links(
    records: list[dict[str, Any]],
) -> tuple[tuple[AssistantLink, ...], tuple[dict[str, str], ...]]:
    """legacy record 순서 그대로 모든 링크와 표시 문맥을 만든다."""

    links: list[AssistantLink] = []
    contexts: list[dict[str, str]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        device_name = str(record.get("deviceName") or "").strip() or "미확인"
        for item in record.get("downloadLinks") or []:
            if not isinstance(item, dict):
                continue
            uri = str(item.get("url") or "").strip()
            label = str(item.get("fileName") or "파일").strip()
            if not _is_safe_private_link_uri(uri) or not label:
                continue
            safe_label = label[:255]
            links.append(AssistantLink(label=safe_label, uri=uri))
            contexts.append(
                {
                    "deviceName": device_name[:160],
                    "fileName": safe_label,
                }
            )
    return tuple(links), tuple(contexts)


def _build_download_delivery_manifest(
    *,
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
    used_expanded_scope: bool,
) -> dict[str, Any]:
    """presigned URL을 제외한 activity/공개 안내 근거만 receipt에 싣는다."""

    return {
        "barcode": barcode,
        "logDate": log_date,
        "usedExpandedScope": used_expanded_scope,
        "records": [
            {
                "deviceName": str(record.get("deviceName") or "미확인"),
                "deviceSeq": record.get("deviceSeq"),
                "hospitalSeq": record.get("hospitalSeq"),
                "hospitalRoomSeq": record.get("hospitalRoomSeq"),
                "hospitalName": str(
                    record.get("hospitalName") or "미확인"
                ),
                "roomName": str(record.get("roomName") or "미확인"),
                "fileNames": [
                    str(item)
                    for item in record.get("fileNames") or []
                ],
                "downloadFileNames": _record_download_file_names(record),
            }
            for record in records
        ],
    }


def _download_delivery_from_receipt(
    request: CompanyAssistantRequest,
) -> _DeviceFileDownloadDelivery | None:
    action = request.metadata.get("operation_action")
    if not isinstance(action, Mapping):
        return None
    raw_delivery = action.get("delivery")
    if not isinstance(raw_delivery, Mapping):
        return None
    barcode = str(raw_delivery.get("barcode") or "").strip()
    log_date = str(raw_delivery.get("log_date") or "").strip()
    used_expanded_scope = raw_delivery.get("used_expanded_scope")
    raw_records = raw_delivery.get("records")
    if (
        _BARCODE_PATTERN.fullmatch(barcode) is None
        or not _is_valid_delivery_date(log_date)
        or type(used_expanded_scope) is not bool
        or not isinstance(raw_records, (list, tuple))
        or not raw_records
    ):
        return None
    records: list[dict[str, Any]] = []
    for raw_record in raw_records:
        normalized = _normalize_delivery_record(raw_record)
        if normalized is None:
            return None
        records.append(normalized)
    return _DeviceFileDownloadDelivery(
        barcode=barcode,
        log_date=log_date,
        records=tuple(records),
        used_expanded_scope=used_expanded_scope,
    )


def _normalize_delivery_record(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    expected_keys = {
        "device_name",
        "device_seq",
        "hospital_seq",
        "hospital_room_seq",
        "hospital_name",
        "room_name",
        "file_names",
        "download_file_names",
    }
    if frozenset(value) != expected_keys:
        return None
    sequence_values = (
        value.get("device_seq"),
        value.get("hospital_seq"),
        value.get("hospital_room_seq"),
    )
    if any(
        item is not None
        and (type(item) is not int or item < 1)
        for item in sequence_values
    ):
        return None
    device_name = _safe_delivery_text(value.get("device_name"), 160)
    hospital_name = _safe_delivery_text(value.get("hospital_name"), 200)
    room_name = _safe_delivery_text(value.get("room_name"), 200)
    file_names = _safe_delivery_file_names(value.get("file_names"))
    download_file_names = _safe_delivery_file_names(
        value.get("download_file_names")
    )
    if (
        device_name is None
        or hospital_name is None
        or room_name is None
        or file_names is None
        or not download_file_names
    ):
        return None
    return {
        "deviceName": device_name,
        "deviceSeq": value.get("device_seq"),
        "hospitalSeq": value.get("hospital_seq"),
        "hospitalRoomSeq": value.get("hospital_room_seq"),
        "hospitalName": hospital_name,
        "roomName": room_name,
        "fileNames": file_names,
        "downloadFileNames": download_file_names,
    }


def _safe_delivery_text(value: Any, maximum: int) -> str | None:
    normalized = " ".join(str(value or "").split())
    if (
        not normalized
        or len(normalized) > maximum
        or not normalized.isprintable()
    ):
        return None
    return normalized


def _safe_delivery_file_names(value: Any) -> list[str] | None:
    if not isinstance(value, (list, tuple)):
        return None
    normalized = [
        str(item or "").strip()
        for item in value
    ]
    if any(not _is_safe_delivery_file_name(item) for item in normalized):
        return None
    return normalized


def _is_safe_delivery_file_name(value: Any) -> bool:
    normalized = str(value or "").strip()
    return bool(
        normalized
        and len(normalized) <= 255
        and normalized.isprintable()
        and "/" not in normalized
        and "\\" not in normalized
    )


def _is_valid_delivery_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _record_download_file_names(record: Mapping[str, Any]) -> list[str]:
    explicit = record.get("downloadFileNames")
    if isinstance(explicit, (list, tuple)):
        return [str(item).strip() for item in explicit if str(item).strip()]
    return [
        str(item.get("fileName") or "").strip()
        for item in record.get("downloadLinks") or []
        if isinstance(item, Mapping)
        and str(item.get("fileName") or "").strip()
    ]


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


def build_trusted_mda_recovery_scope_metadata(
    *,
    barcode: str,
    log_date: str,
    device_context: Mapping[str, Any],
) -> dict[str, Any]:
    """검증된 Slack MDA 복구 root를 API에 넘기는 고정 metadata 계약이다."""

    scope = _normalize_trusted_mda_recovery_scope(
        {
            "barcode": barcode,
            "logDate": log_date,
            "deviceName": device_context.get("deviceName"),
            "hospitalName": device_context.get("hospitalName"),
            "roomName": device_context.get("roomName"),
        }
    )
    if scope is None:
        raise ValueError("trusted MDA recovery scope is invalid")
    return {TRUSTED_MDA_RECOVERY_SCOPE_METADATA_KEY: scope}


def resolve_device_file_operation_scope(
    request: CompanyAssistantRequest,
) -> tuple[str | None, str | None]:
    """Slack root 검증 전에 route와 같은 바코드·날짜 scope를 해석한다."""

    barcode = _single_request_barcode(request)
    log_date, has_requested_date = _extract_log_date_with_presence(
        request.question
    )
    return barcode, log_date if has_requested_date else None


def _trusted_mda_recovery_device_contexts(
    request: CompanyAssistantRequest,
    *,
    barcode: str,
    log_date: str,
) -> list[dict[str, Any]]:
    raw_scope = request.metadata.get(
        TRUSTED_MDA_RECOVERY_SCOPE_METADATA_KEY
    )
    if not isinstance(raw_scope, Mapping):
        return []
    scope = _normalize_trusted_mda_recovery_scope(raw_scope)
    if scope is None:
        return []
    # adapter가 검증한 root라도 현재 turn 대상과 exact 일치할 때만 쓴다.
    if scope["barcode"] != barcode or scope["logDate"] != log_date:
        return []
    return [
        {
            "deviceName": scope["deviceName"],
            "hospitalName": scope["hospitalName"],
            "roomName": scope["roomName"],
        }
    ]


def _normalize_trusted_mda_recovery_scope(
    raw_scope: Mapping[str, Any],
) -> dict[str, str] | None:
    barcode = str(raw_scope.get("barcode") or "").strip()
    log_date = str(raw_scope.get("logDate") or "").strip()
    device_name = str(raw_scope.get("deviceName") or "").strip()
    hospital_name = " ".join(
        str(raw_scope.get("hospitalName") or "").split()
    ).strip()
    room_name = " ".join(
        str(raw_scope.get("roomName") or "").split()
    ).strip()
    if not _BARCODE_PATTERN.fullmatch(barcode):
        return None
    try:
        datetime.strptime(log_date, "%Y-%m-%d")
    except ValueError:
        return None
    if not (
        len(device_name) <= 64
        and cs.S3_DEVICE_NAME_PATTERN.fullmatch(device_name)
    ):
        return None
    if not all(
        1 <= len(value) <= 200 and value.isprintable()
        for value in (hospital_name, room_name)
    ):
        return None
    return {
        "barcode": barcode,
        "logDate": log_date,
        "deviceName": device_name,
        "hospitalName": hospital_name,
        "roomName": room_name,
    }


def _single_explicit_barcode(question: str) -> str | None:
    matched = _BARCODE_PATTERN.search(str(question or ""))
    return matched.group(1) if matched is not None else None


def _single_request_barcode(
    request: CompanyAssistantRequest,
) -> str | None:
    explicit = _single_explicit_barcode(request.question)
    if explicit is not None:
        return explicit
    metadata_barcode = str(request.metadata.get("barcode") or "").strip()
    if _BARCODE_PATTERN.fullmatch(metadata_barcode):
        return metadata_barcode
    # 기존 turn scope처럼 같은 요청자의 최신 thread 메시지부터 보고,
    # 한 thread에 과거 바코드가 여러 개 있어도 가장 최근 대상을 쓴다.
    for entry in reversed(_request_context_entries(request)):
        matched = _BARCODE_PATTERN.search(str(entry.get("text") or ""))
        if matched is not None:
            return matched.group(1)
    return None


def _request_file_hospital_room(
    request: CompanyAssistantRequest,
) -> tuple[str | None, str | None]:
    """현재 질문을 우선하고 adapter의 phase2 scope는 fallback으로 쓴다."""

    question_hospital, question_room = _extract_hospital_room_scope(
        request.question
    )
    metadata_hospital = _first_metadata_text(
        request,
        "hospital_name",
        "hospitalName",
        "phase2_hospital_name",
        "phase2HospitalName",
    )
    metadata_room = _first_metadata_text(
        request,
        "room_name",
        "roomName",
        "phase2_room_name",
        "phase2RoomName",
    )
    return (
        question_hospital or metadata_hospital,
        question_room or metadata_room,
    )


def _request_log_hospital_room(
    request: CompanyAssistantRequest,
) -> tuple[str | None, str | None]:
    hospital_name, room_name = _extract_hospital_room_scope_for_log_upload(
        request.question
    )
    if hospital_name and room_name:
        return hospital_name, room_name
    metadata_hospital, metadata_room = _request_file_hospital_room(request)
    if metadata_hospital and metadata_room:
        return metadata_hospital, metadata_room
    for entry in reversed(_request_context_entries(request)):
        hospital_name, room_name = (
            _extract_hospital_room_scope_for_log_upload(
                str(entry.get("text") or "")
            )
        )
        if hospital_name and room_name:
            return hospital_name, room_name
    return None, None


def _first_metadata_text(
    request: CompanyAssistantRequest,
    *keys: str,
) -> str | None:
    for key in keys:
        value = request.metadata.get(key)
        if isinstance(value, str):
            normalized = " ".join(value.split()).strip()
            if normalized:
                return normalized
    return None


def _request_actor_name(request: CompanyAssistantRequest) -> str | None:
    return _first_metadata_text(
        request,
        "actor_name",
        "actorName",
        "user_name",
        "userName",
    )


def _request_channel_id(request: CompanyAssistantRequest) -> str:
    return str(
        request.metadata.get("channel_id") or request.channel
    ).strip()


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


def _request_context_entries(
    request: CompanyAssistantRequest,
) -> tuple[dict[str, Any], ...]:
    """Slack thread 전체를 원래 순서대로 보존한다."""

    return tuple(
        entry
        for entry in request.context_entries
        if isinstance(entry, dict)
    )


def needs_device_file_operation_context(question: str) -> bool:
    """현재 질문에 target이 없을 때만 Slack thread 문맥을 요청한다."""

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
    """기존 Slack은 현재 질문이나 최신 thread 문맥의 첫 범위를 썼다."""

    del request
    return False


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
    return next(iter(values.values()), None)


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
    "DEVICE_FILE_DOWNLOAD_DELIVERY_ACTION",
    "DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE",
    "DEVICE_FILE_DOWNLOAD_ROUTE",
    "DEVICE_FILE_LOOKUP_ROUTE",
    "DEVICE_FILE_RECOVERY_ROUTE",
    "DEVICE_LOG_UPLOAD_ROUTE",
    "TRUSTED_MDA_RECOVERY_SCOPE_METADATA_KEY",
    "DeviceFileOperationsAssistantRoute",
    "DeviceFileOperationsRouteDeps",
    "build_trusted_mda_recovery_scope_metadata",
    "has_ambiguous_device_file_operation_scope",
    "is_device_file_download_delivery_receipt",
    "match_device_file_operation_route",
    "needs_device_file_operation_context",
    "resolve_device_file_operation_scope",
]
