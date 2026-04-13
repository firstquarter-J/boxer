import logging
from dataclasses import dataclass
from typing import Any, Callable

import pymysql
from botocore.exceptions import BotoCoreError, ClientError

from boxer_adapter_slack.common import MentionPayload, SlackReplyFn, _load_slack_user_name, _set_request_log_route
from boxer_adapter_slack.context import _load_slack_thread_context
from boxer.core import settings as s
from boxer_company import settings as cs
from boxer_company.notion_playbooks import _select_notion_references
from boxer_company.routers.barcode_log import _extract_device_name_scope, _extract_log_date_with_presence
from boxer_company.routers.box_db import _lookup_device_contexts_by_hospital_room
from boxer_company.routers.device_audio_probe import (
    _build_device_audio_probe_config_message,
    _extract_device_name_for_audio_probe,
    _is_device_audio_probe_request,
    _probe_device_audio_output,
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
    _should_render_compact_file_id_result,
    _should_render_compact_device_download_result,
    _should_render_compact_device_file_list,
    _should_render_compact_device_recovery_result,
)
from boxer_company.routers.device_log_upload import (
    _build_device_log_upload_scope_ambiguous_reply,
    _build_device_log_upload_scope_not_found_reply,
    _check_and_request_device_log_upload,
    _extract_device_name_for_log_upload,
    _extract_hospital_room_scope_for_log_upload,
    _extract_latest_hospital_room_scope_from_thread_context,
    _is_device_log_upload_check_request,
)
from boxer_company.routers.device_status_probe import (
    _build_led_pattern_help_evidence,
    _build_led_pattern_help_reply,
    _build_device_memory_patch_config_message,
    _build_device_remote_access_probe_config_message,
    _build_device_status_probe_config_message,
    _extract_device_name_for_remote_access_probe,
    _extract_device_name_for_status_probe,
    _is_device_captureboard_probe_request,
    _is_device_led_pattern_help_request,
    _is_device_led_probe_request,
    _is_device_memory_patch_request,
    _is_device_pm2_probe_request,
    _is_device_remote_access_probe_request,
    _is_device_status_probe_request,
    _patch_device_pm2_memory,
    _probe_device_remote_access,
    _probe_device_runtime_component,
    _probe_device_status_overview,
)
from boxer_company.routers.device_update import (
    _build_device_update_config_message,
    _extract_device_name_for_update,
    _is_device_agent_update_request,
    _is_device_box_update_request,
    _is_device_update_status_request,
    _query_device_update_status,
    _request_device_agent_update,
    _request_device_box_update,
)
from boxer_company.routers.mda_graphql import _send_mda_device_command
from boxer_company_adapter_slack.device_activity import (
    _collect_device_download_records,
    _log_device_download_activity,
    _log_device_update_activity,
    _render_device_download_dm_failure_notice,
    _render_device_download_dm_text,
    _render_device_download_thread_notice,
)


@dataclass(frozen=True)
class DeviceRoutesContext:
    question: str
    barcode: str | None
    phase2_hospital_name: str | None
    phase2_room_name: str | None
    payload: MentionPayload
    user_id: str | None
    workspace_id: str
    channel_id: str
    thread_ts: str
    reply: SlackReplyFn
    client: Any
    logger: logging.Logger


@dataclass(frozen=True)
class DeviceRoutesDeps:
    get_s3_client: Callable[[], Any]
    get_recordings_context: Callable[[], dict[str, Any]]
    has_recordings_device_mapping: Callable[[dict[str, Any]], bool]
    send_dm_message: Callable[[str | None, str], bool]
    build_dependency_failure_reply: Callable[[str, Exception], str]
    reply_with_retrieval_synthesis: Callable[..., None]


def _is_device_runtime_configured() -> bool:
    return bool(
        cs.MDA_GRAPHQL_URL
        and cs.MDA_ADMIN_USER_PASSWORD
        and cs.DEVICE_SSH_PASSWORD
    )


def _handle_device_routes(
    context: DeviceRoutesContext,
    deps: DeviceRoutesDeps,
) -> bool:
    question = context.question
    barcode = context.barcode
    structured_device_name = _extract_device_name_scope(question)
    log_upload_device_name = _extract_device_name_for_log_upload(question) or structured_device_name
    log_upload_hospital_name, log_upload_room_name = _extract_hospital_room_scope_for_log_upload(question)
    if not (log_upload_hospital_name and log_upload_room_name):
        log_upload_hospital_name = context.phase2_hospital_name
        log_upload_room_name = context.phase2_room_name
    update_device_name = _extract_device_name_for_update(question) or structured_device_name
    audio_probe_device_name = _extract_device_name_for_audio_probe(question) or structured_device_name
    remote_access_device_name = _extract_device_name_for_remote_access_probe(question) or structured_device_name
    status_probe_device_name = _extract_device_name_for_status_probe(question) or structured_device_name

    if _is_device_led_pattern_help_request(question):
        fallback_text = _build_led_pattern_help_reply(question)
        evidence_payload = _build_led_pattern_help_evidence(question)
        notion_references = _select_notion_references(
            question,
            evidence_payload=evidence_payload,
            max_results=2,
        )
        if notion_references:
            evidence_payload["notionPlaybooks"] = notion_references
            evidence_payload["notionReferences"] = notion_references
        deps.reply_with_retrieval_synthesis(
            fallback_text,
            evidence_payload,
            route_name="device led pattern guide",
            max_tokens=280,
        )
        context.logger.info(
            "Responded with device led pattern guide in thread_ts=%s refs=%s",
            context.thread_ts,
            len(notion_references),
        )
        return True

    if _is_device_log_upload_check_request(question, device_name=log_upload_device_name):
        if not s.S3_QUERY_ENABLED:
            context.reply("장비 로그 업로드 확인을 위해 S3_QUERY_ENABLED=true가 필요해")
            return True
        try:
            log_date, has_requested_date = _extract_log_date_with_presence(question)
            resolved_device_name = log_upload_device_name
            resolved_hospital_name = log_upload_hospital_name
            resolved_room_name = log_upload_room_name

            if not resolved_device_name and not (resolved_hospital_name and resolved_room_name):
                thread_context = _load_slack_thread_context(
                    context.client,
                    context.logger,
                    context.channel_id,
                    context.thread_ts,
                    context.payload.get("current_ts"),
                )
                if thread_context:
                    resolved_hospital_name, resolved_room_name = _extract_latest_hospital_room_scope_from_thread_context(
                        thread_context
                    )

            if not resolved_device_name and resolved_hospital_name and resolved_room_name:
                device_contexts = _lookup_device_contexts_by_hospital_room(
                    resolved_hospital_name,
                    resolved_room_name,
                )
                if not device_contexts:
                    context.reply(
                        _build_device_log_upload_scope_not_found_reply(
                            resolved_hospital_name,
                            resolved_room_name,
                        )
                    )
                    return True
                if len(device_contexts) > 1:
                    context.reply(
                        _build_device_log_upload_scope_ambiguous_reply(
                            resolved_hospital_name,
                            resolved_room_name,
                            device_contexts,
                        )
                    )
                    return True
                resolved_device_name = str(device_contexts[0].get("deviceName") or "").strip()

            if not resolved_device_name:
                context.reply("장비 로그 업로드 확인은 장비명이나 병원명/병실명이 필요해")
                return True

            _set_request_log_route(
                context.payload,
                "device log upload check",
                handler_type="router",
                subject_type="device",
                subject_key=resolved_device_name,
                requested_date=log_date,
            )

            def _dispatch_device_command(device_name: str, command: str) -> dict[str, Any]:
                return _send_mda_device_command(device_name, command=command)

            command_dispatcher = (
                _dispatch_device_command
                if cs.MDA_GRAPHQL_URL and cs.MDA_ADMIN_USER_PASSWORD
                else None
            )
            result_text, _ = _check_and_request_device_log_upload(
                deps.get_s3_client(),
                resolved_device_name,
                log_date,
                has_requested_date=has_requested_date,
                dispatch_device_command=command_dispatcher,
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with device log upload check in thread_ts=%s deviceName=%s date=%s",
                context.thread_ts,
                resolved_device_name,
                log_date,
            )
        except ValueError as exc:
            context.reply(f"장비 로그 업로드 확인 요청 형식 오류: {exc}")
        except (BotoCoreError, ClientError, RuntimeError) as exc:
            context.logger.exception("Device log upload check failed")
            context.reply(deps.build_dependency_failure_reply("장비 로그 업로드 확인", exc))
        except Exception:
            context.logger.exception("Device log upload check failed")
            context.reply("장비 로그 업로드 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_barcode_device_file_probe_request(question, barcode):
        if not s.S3_QUERY_ENABLED:
            context.reply("파일 확인 대상 세션 조회를 위해 S3_QUERY_ENABLED=true가 필요해")
            return True
        if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
            context.reply("파일 확인 대상 세션 조회를 위해 DB 접속 정보(DB_*)가 필요해")
            return True

        probe_remote_files = _should_probe_device_files(question)
        download_remote_files = _should_download_device_files(question)
        recover_remote_files = _should_recover_device_files(question)
        compact_file_id = _should_render_compact_file_id_result(question)
        compact_file_list = _should_render_compact_device_file_list(question)
        compact_download = _should_render_compact_device_download_result(question)
        compact_recovery = _should_render_compact_device_recovery_result(question)

        if recover_remote_files and not cs.DEVICE_FILE_RECOVERY_ENABLED:
            context.reply("장비 영상 복구 기능은 현재 비활성화돼 있어")
            return True
        if probe_remote_files and not _is_device_runtime_configured():
            context.reply(_build_device_file_probe_config_message())
            return True
        if download_remote_files and (
            not cs.DEVICE_FILE_DOWNLOAD_BUCKET or not _is_device_runtime_configured()
        ):
            context.reply(_build_device_file_download_config_message())
            return True
        if recover_remote_files and (
            not cs.BOX_UPLOADER_BASE_URL
            or not cs.UPLOADER_JWT_SECRET
            or not _is_device_runtime_configured()
        ):
            context.reply(_build_device_file_recovery_config_message())
            return True

        try:
            log_date, has_requested_date = _extract_log_date_with_presence(question)
            if not has_requested_date:
                context.reply("파일 확인 대상 세션 조회는 날짜가 필요해. 예: `48194663047 2026-03-06 파일 있나`")
                return True

            recordings_context = deps.get_recordings_context()
            summary = recordings_context.get("summary") or {}
            recording_count = int(summary.get("recordingCount") or 0)
            has_device_mapping = deps.has_recordings_device_mapping(recordings_context)
            manual_device_contexts = None

            if context.phase2_hospital_name and context.phase2_room_name:
                manual_device_contexts = _lookup_device_contexts_by_hospital_room(
                    context.phase2_hospital_name,
                    context.phase2_room_name,
                )
                if not manual_device_contexts:
                    context.reply(
                        _build_device_file_scope_request_message(
                            barcode or "",
                            "입력한 병원명/병실명으로 장비를 찾지 못했어. MDA 표시 이름과 정확히 일치하게 입력해줘",
                        )
                    )
                    return True
            elif recording_count <= 0 or not has_device_mapping:
                context.reply(
                    _build_device_file_scope_request_message(
                        barcode or "",
                        "recordings 장비 매핑이 없어 2차 입력이 필요해",
                    )
                )
                return True

            result_text, probe_payload = _locate_barcode_file_candidates(
                deps.get_s3_client(),
                barcode or "",
                log_date,
                recordings_context=recordings_context,
                device_contexts=manual_device_contexts,
                probe_remote_files=probe_remote_files,
                download_remote_files=download_remote_files,
                recover_remote_files=recover_remote_files,
                compact_file_list=compact_file_list,
                compact_file_id=compact_file_id,
                compact_download=compact_download,
                compact_recovery=compact_recovery,
            )
            if download_remote_files:
                download_records = _collect_device_download_records(probe_payload)
                if download_records:
                    dm_text = _render_device_download_dm_text(
                        barcode or "",
                        log_date,
                        download_records,
                    )
                    if deps.send_dm_message(context.user_id, dm_text):
                        requester_name = _load_slack_user_name(
                            context.client,
                            context.workspace_id,
                            context.user_id,
                            context.logger,
                        )
                        logged_count = _log_device_download_activity(
                            records=download_records,
                            barcode=barcode or "",
                            log_date=log_date,
                            question=question,
                            user_id=context.user_id,
                            user_name=requester_name,
                            channel_id=context.channel_id,
                            thread_ts=context.thread_ts,
                            logger=context.logger,
                        )
                        thread_notice = _render_device_download_thread_notice(
                            barcode or "",
                            log_date,
                            download_records,
                            activity_logged=logged_count > 0,
                            used_expanded_scope=bool(
                                ((probe_payload.get("request") or {}).get("usedExpandedScope"))
                            ),
                        )
                        context.reply(thread_notice)
                    else:
                        failure_notice = _render_device_download_dm_failure_notice(
                            barcode or "",
                            log_date,
                            download_records,
                            used_expanded_scope=bool(
                                ((probe_payload.get("request") or {}).get("usedExpandedScope"))
                            ),
                        )
                        context.reply(failure_notice)
                else:
                    context.reply(result_text)
            else:
                context.reply(result_text)
            context.logger.info(
                "Responded with device file candidate lookup in thread_ts=%s barcode=%s records=%s",
                context.thread_ts,
                barcode,
                int(((probe_payload.get("summary") or {}).get("recordCount") or 0)),
            )
        except ValueError as exc:
            context.reply(f"파일 확인 대상 세션 조회 요청 형식 오류: {exc}")
        except (BotoCoreError, ClientError, pymysql.MySQLError, RuntimeError):
            context.logger.exception("Device file candidate lookup failed")
            context.reply("파일 확인 대상 세션 조회 중 오류가 발생했어. S3/DB 설정을 확인해줘")
        except Exception:
            context.logger.exception("Device file candidate lookup failed")
            context.reply("파일 확인 대상 세션 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_update_status_request(question, device_name=update_device_name):
        if not _is_device_runtime_configured():
            context.reply(_build_device_update_config_message())
            return True
        try:
            _set_request_log_route(context.payload, "device update status", handler_type="router")
            result_text, _ = _query_device_update_status(update_device_name or "")
            context.reply(result_text)
            context.logger.info(
                "Responded with device update status in thread_ts=%s deviceName=%s",
                context.thread_ts,
                update_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 업데이트 상태 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device update status failed")
            context.reply(deps.build_dependency_failure_reply("장비 업데이트 상태 확인", exc))
        except Exception:
            context.logger.exception("Device update status failed")
            context.reply("장비 업데이트 상태 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_box_update_request(question, device_name=update_device_name):
        if not _is_device_runtime_configured():
            context.reply(_build_device_update_config_message())
            return True
        try:
            _set_request_log_route(context.payload, "device box update", handler_type="router")

            def _reply_device_update_notice(notice_text: str) -> None:
                try:
                    context.reply(notice_text, mention_user=False)
                except Exception:
                    context.logger.exception(
                        "Failed to send device box update progress notice in thread_ts=%s deviceName=%s",
                        context.thread_ts,
                        update_device_name,
                    )

            result_text, result_payload = _request_device_box_update(
                question,
                device_name=update_device_name,
                on_dispatched=_reply_device_update_notice,
            )
            context.reply(result_text, mention_user=False)
            if bool(((result_payload.get("dispatch") or {}) if isinstance(result_payload, dict) else {}).get("status")):
                _log_device_update_activity(
                    question=question,
                    user_id=context.user_id,
                    channel_id=context.channel_id,
                    thread_ts=context.thread_ts,
                    result_payload=result_payload,
                    client=context.client,
                    logger=context.logger,
                )
            context.logger.info(
                "Responded with device box update in thread_ts=%s deviceName=%s",
                context.thread_ts,
                update_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 박스 업데이트 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device box update failed")
            context.reply(deps.build_dependency_failure_reply("장비 박스 업데이트", exc))
        except Exception:
            context.logger.exception("Device box update failed")
            context.reply("장비 박스 업데이트 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_agent_update_request(question, device_name=update_device_name):
        if not _is_device_runtime_configured():
            context.reply(_build_device_update_config_message())
            return True
        try:
            _set_request_log_route(context.payload, "device agent update", handler_type="router")

            def _reply_device_update_notice(notice_text: str) -> None:
                try:
                    context.reply(notice_text, mention_user=False)
                except Exception:
                    context.logger.exception(
                        "Failed to send device agent update progress notice in thread_ts=%s deviceName=%s",
                        context.thread_ts,
                        update_device_name,
                    )

            result_text, result_payload = _request_device_agent_update(
                question,
                device_name=update_device_name,
                on_dispatched=_reply_device_update_notice,
            )
            context.reply(result_text, mention_user=False)
            if bool(((result_payload.get("dispatch") or {}) if isinstance(result_payload, dict) else {}).get("status")):
                _log_device_update_activity(
                    question=question,
                    user_id=context.user_id,
                    channel_id=context.channel_id,
                    thread_ts=context.thread_ts,
                    result_payload=result_payload,
                    client=context.client,
                    logger=context.logger,
                )
            context.logger.info(
                "Responded with device agent update in thread_ts=%s deviceName=%s",
                context.thread_ts,
                update_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 에이전트 업데이트 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device agent update failed")
            context.reply(deps.build_dependency_failure_reply("장비 에이전트 업데이트", exc))
        except Exception:
            context.logger.exception("Device agent update failed")
            context.reply("장비 에이전트 업데이트 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_audio_probe_request(question, device_name=audio_probe_device_name):
        if not _is_device_runtime_configured():
            context.reply(_build_device_audio_probe_config_message())
            return True
        try:
            result_text, evidence_payload = _probe_device_audio_output(audio_probe_device_name or "")
            deps.reply_with_retrieval_synthesis(
                result_text,
                evidence_payload,
                route_name="device audio probe",
                max_tokens=280,
            )
            context.logger.info(
                "Responded with device audio probe in thread_ts=%s deviceName=%s",
                context.thread_ts,
                audio_probe_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 소리 출력 점검 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device audio probe failed")
            context.reply(deps.build_dependency_failure_reply("장비 소리 출력 점검", exc))
        except Exception:
            context.logger.exception("Device audio probe failed")
            context.reply("장비 소리 출력 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_remote_access_probe_request(question, device_name=remote_access_device_name):
        if not cs.MDA_GRAPHQL_URL or not cs.MDA_ADMIN_USER_PASSWORD:
            context.reply(_build_device_remote_access_probe_config_message())
            return True
        try:
            _set_request_log_route(context.payload, "device remote access probe", handler_type="router")
            result_text, _ = _probe_device_remote_access(remote_access_device_name or "")
            context.reply(result_text)
            context.logger.info(
                "Responded with device remote access probe in thread_ts=%s deviceName=%s",
                context.thread_ts,
                remote_access_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 원격 접속 점검 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device remote access probe failed")
            context.reply(deps.build_dependency_failure_reply("장비 원격 접속 점검", exc))
        except Exception:
            context.logger.exception("Device remote access probe failed")
            context.reply("장비 원격 접속 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_memory_patch_request(question, device_name=status_probe_device_name):
        if not _is_device_runtime_configured():
            context.reply(_build_device_memory_patch_config_message())
            return True
        try:
            _set_request_log_route(context.payload, "device memory patch", handler_type="router")
            result_text, _ = _patch_device_pm2_memory(status_probe_device_name or "")
            context.reply(result_text)
            context.logger.info(
                "Responded with device memory patch in thread_ts=%s deviceName=%s",
                context.thread_ts,
                status_probe_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 메모리 패치 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device memory patch failed")
            context.reply(deps.build_dependency_failure_reply("장비 메모리 패치", exc))
        except Exception:
            context.logger.exception("Device memory patch failed")
            context.reply("장비 메모리 패치 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_pm2_probe_request(question, device_name=status_probe_device_name):
        if not _is_device_runtime_configured():
            context.reply(_build_device_status_probe_config_message())
            return True
        try:
            _set_request_log_route(context.payload, "device pm2 probe", handler_type="router")
            result_text, _ = _probe_device_runtime_component(
                status_probe_device_name or "",
                component="pm2",
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with device pm2 probe in thread_ts=%s deviceName=%s",
                context.thread_ts,
                status_probe_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 PM2 상태 점검 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device pm2 probe failed")
            context.reply(deps.build_dependency_failure_reply("장비 PM2 상태 점검", exc))
        except Exception:
            context.logger.exception("Device pm2 probe failed")
            context.reply("장비 PM2 상태 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_captureboard_probe_request(question, device_name=status_probe_device_name):
        if not _is_device_runtime_configured():
            context.reply(_build_device_status_probe_config_message())
            return True
        try:
            _set_request_log_route(context.payload, "device captureboard probe", handler_type="router")
            result_text, _ = _probe_device_runtime_component(
                status_probe_device_name or "",
                component="captureboard",
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with device captureboard probe in thread_ts=%s deviceName=%s",
                context.thread_ts,
                status_probe_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 캡처보드 점검 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device captureboard probe failed")
            context.reply(deps.build_dependency_failure_reply("장비 캡처보드 점검", exc))
        except Exception:
            context.logger.exception("Device captureboard probe failed")
            context.reply("장비 캡처보드 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_led_probe_request(question, device_name=status_probe_device_name):
        if not _is_device_runtime_configured():
            context.reply(_build_device_status_probe_config_message())
            return True
        try:
            _set_request_log_route(context.payload, "device led probe", handler_type="router")
            result_text, _ = _probe_device_runtime_component(
                status_probe_device_name or "",
                component="led",
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with device led probe in thread_ts=%s deviceName=%s",
                context.thread_ts,
                status_probe_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 LED 점검 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device led probe failed")
            context.reply(deps.build_dependency_failure_reply("장비 LED 점검", exc))
        except Exception:
            context.logger.exception("Device led probe failed")
            context.reply("장비 LED 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_device_status_probe_request(question, device_name=status_probe_device_name):
        if not _is_device_runtime_configured():
            context.reply(_build_device_status_probe_config_message())
            return True
        try:
            _set_request_log_route(context.payload, "device status probe", handler_type="router")
            result_text, _ = _probe_device_status_overview(status_probe_device_name or "")
            context.reply(result_text)
            context.logger.info(
                "Responded with device status probe in thread_ts=%s deviceName=%s",
                context.thread_ts,
                status_probe_device_name,
            )
        except ValueError as exc:
            context.reply(f"장비 상태 점검 요청 형식 오류: {exc}")
        except RuntimeError as exc:
            context.logger.exception("Device status probe failed")
            context.reply(deps.build_dependency_failure_reply("장비 상태 점검", exc))
        except Exception:
            context.logger.exception("Device status probe failed")
            context.reply("장비 상태 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    return False
