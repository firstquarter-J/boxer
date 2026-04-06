import json
import logging
from typing import Any

from boxer_adapter_slack.common import _load_slack_user_name
from boxer_company.routers.device_update import _build_device_update_activity_input
from boxer_company.routers.mda_graphql import _create_mda_activity_log
from boxer_company.utils import _extract_barcode


def _extract_user_only_thread_text(thread_context: str, target_user_id: str) -> str:
    prefix = f"{(target_user_id or '').strip()}: "
    if not prefix.strip():
        return ""
    lines: list[str] = []
    for raw_line in (thread_context or "").splitlines():
        line = raw_line.strip()
        if not line.startswith(prefix):
            continue
        lines.append(line[len(prefix) :].strip())
    return "\n".join(part for part in lines if part)


def _extract_latest_barcode_from_thread_context(thread_context: str) -> str | None:
    lines = [line.strip() for line in (thread_context or "").splitlines() if line.strip()]
    for line in reversed(lines):
        barcode = _extract_barcode(line)
        if barcode:
            return barcode
    return None


def _collect_device_download_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for record in payload.get("records") or []:
        if not isinstance(record, dict):
            continue

        file_names: list[str] = []
        seen_files: set[str] = set()
        download_links: list[dict[str, str]] = []
        seen_links: set[str] = set()

        for session in record.get("sessions") or []:
            if not isinstance(session, dict):
                continue

            probe = session.get("probe") if isinstance(session.get("probe"), dict) else None
            if probe and probe.get("ok"):
                for found_file in probe.get("files") or []:
                    file_name = str(found_file or "").strip().split("/")[-1]
                    if file_name and file_name not in seen_files:
                        seen_files.add(file_name)
                        file_names.append(file_name)

            download = session.get("download") if isinstance(session.get("download"), dict) else None
            if not download:
                continue
            for item in download.get("downloads") or []:
                if not isinstance(item, dict) or not item.get("ok"):
                    continue
                file_name = str(item.get("fileName") or "").strip()
                url = str(item.get("url") or "").strip()
                if not file_name or not url:
                    continue
                dedupe_key = file_name
                if dedupe_key in seen_links:
                    continue
                seen_links.add(dedupe_key)
                download_links.append({"fileName": file_name, "url": url})

        if not download_links:
            continue

        records.append(
            {
                "deviceName": str(record.get("deviceName") or "").strip() or "미확인",
                "deviceSeq": record.get("deviceSeq"),
                "hospitalSeq": record.get("hospitalSeq"),
                "hospitalRoomSeq": record.get("hospitalRoomSeq"),
                "hospitalName": str(record.get("hospitalName") or "").strip() or "미확인",
                "roomName": str(record.get("roomName") or "").strip() or "미확인",
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
    device_name = str(record.get("deviceName") or "").strip() or "미확인"
    hospital_name = str(record.get("hospitalName") or "").strip() or "미확인"
    room_name = str(record.get("roomName") or "").strip() or "미확인"
    requester_name = str(user_name or "").strip()
    requester_label = requester_name or str(user_id or "").strip()
    file_names = [str(item).strip() for item in (record.get("fileNames") or []) if str(item).strip()]
    download_links = [
        item
        for item in (record.get("downloadLinks") or [])
        if isinstance(item, dict) and str(item.get("fileName") or "").strip() and str(item.get("url") or "").strip()
    ]

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
        "downloadFileNames": [
            str(item.get("fileName") or "").strip()
            for item in download_links
        ],
        "downloadLinkCount": len(download_links),
    }

    return {
        "activityType": "recording.download",
        "barcode": barcode or None,
        "hospitalSeq": record.get("hospitalSeq"),
        "hospitalRoomSeq": record.get("hospitalRoomSeq"),
        "deviceSeq": record.get("deviceSeq"),
        "targetEntityType": "Device" if record.get("deviceSeq") is not None else None,
        "targetEntitySeq": record.get("deviceSeq"),
        "reason": "Boxer Slack 다운로드 링크 전송 성공",
        "description": (
            f"Boxer Slack 다운로드 링크 전송 완료: 병원명 [{hospital_name}], "
            f"병실명 [{room_name}], 장비명 [{device_name}]"
            f"{f', 요청자 [{requester_label}]' if requester_label else ''}, 파일 {len(download_links)}개"
        ),
        "detailLog": json.dumps(detail_log, ensure_ascii=False, separators=(",", ":")),
    }


def _log_device_download_activity(
    *,
    records: list[dict[str, Any]],
    barcode: str,
    log_date: str,
    question: str,
    user_id: str,
    user_name: str | None,
    channel_id: str,
    thread_ts: str,
    logger: logging.Logger,
) -> int:
    if not records:
        return 0

    success_count = 0

    for record in records:
        try:
            _create_mda_activity_log(
                _build_device_download_activity_input(
                    record=record,
                    barcode=barcode,
                    log_date=log_date,
                    question=question,
                    user_id=user_id,
                    user_name=user_name,
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                )
            )
            success_count += 1
        except Exception:
            logger.warning(
                "Failed to create activity log for device download barcode=%s device=%s",
                barcode,
                record.get("deviceName"),
                exc_info=True,
            )
    return success_count


def _log_device_update_activity(
    *,
    question: str,
    user_id: str,
    channel_id: str,
    thread_ts: str,
    result_payload: dict[str, Any],
    client: Any,
    logger: logging.Logger,
) -> bool:
    try:
        user_name = _load_slack_user_name(
            client=client,
            user_id=user_id,
            logger=logger,
        )
        _create_mda_activity_log(
            _build_device_update_activity_input(
                question=question,
                user_id=user_id,
                user_name=user_name,
                channel_id=channel_id,
                thread_ts=thread_ts,
                result_payload=result_payload,
            )
        )
        return True
    except Exception:
        logger.warning(
            "Failed to create activity log for device update route=%s device=%s",
            result_payload.get("route"),
            ((result_payload.get("device") or {}) if isinstance(result_payload.get("device"), dict) else {}).get(
                "deviceName"
            ),
            exc_info=True,
        )
        return False


def _render_device_download_dm_text(
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
) -> str:
    lines = [
        "*장비 영상 다운로드 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
    ]
    for record in records:
        lines.append("")
        lines.append(f"• 장비: `{record['deviceName']}`")
        lines.append(f"• 병원: `{record['hospitalName']}`")
        lines.append(f"• 병실: `{record['roomName']}`")
        file_names = record.get("fileNames") or []
        lines.append(f"• 장비에 존재하는 영상 목록: `{len(file_names)}개`")
        for file_name in file_names:
            lines.append(f"  - `{file_name}`")
        download_links = record.get("downloadLinks") or []
        lines.append(f"• 다운로드 링크: `{len(download_links)}개` (1시간)")
        for item in download_links:
            lines.append(f"  - 🎣 <{item['url']}|{item['fileName']}>")
    return "\n".join(lines)


def _render_device_download_thread_notice(
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
    *,
    activity_logged: bool = False,
    used_expanded_scope: bool = False,
) -> str:
    lines = [
        "*장비 영상 다운로드 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
    ]
    if used_expanded_scope:
        lines.append("• 참고: 매핑 장비 외 같은 병원 장비도 함께 검색했어")
    for record in records:
        lines.append("")
        lines.append(f"• 장비: `{record['deviceName']}`")
        lines.append(f"• 병원: `{record['hospitalName']}`")
        lines.append(f"• 병실: `{record['roomName']}`")
        file_names = record.get("fileNames") or []
        lines.append(f"• 장비에 존재하는 영상 목록: `{len(file_names)}개`")
        for file_name in file_names:
            lines.append(f"  - `{file_name}`")
        lines.append(f"• 다운로드 링크: DM으로 보냈어 (`{len(record.get('downloadLinks') or [])}개`)")
    if activity_logged:
        lines.append("")
        lines.append("• 다운로드 내역 기록되었습니다. 🎣 <https://mda.kr.mmtalkbox.com/cs|CS 처리내역 엿보기>")
    return "\n".join(lines)


def _render_device_download_dm_failure_notice(
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
    *,
    used_expanded_scope: bool = False,
) -> str:
    lines = [
        "*장비 영상 다운로드 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
    ]
    if used_expanded_scope:
        lines.append("• 참고: 매핑 장비 외 같은 병원 장비도 함께 검색했어")
    for record in records:
        lines.append("")
        lines.append(f"• 장비: `{record['deviceName']}`")
        lines.append(f"• 병원: `{record['hospitalName']}`")
        lines.append(f"• 병실: `{record['roomName']}`")
        file_names = record.get("fileNames") or []
        lines.append(f"• 장비에 존재하는 영상 목록: `{len(file_names)}개`")
        for file_name in file_names:
            lines.append(f"  - `{file_name}`")
    lines.append("• 다운로드 링크: DM 전송 실패. 봇 DM 권한을 확인해줘")
    return "\n".join(lines)
