from __future__ import annotations


# 채널 중립 route가 생기기 전 Slack handler가 실제 저장하던 이름을
# 중앙 request-log에서도 그대로 사용한다. 매핑하지 않은 기존 snake
# route는 원문 이름 자체가 legacy 계약이므로 변경하지 않는다.
_LEGACY_COMPANY_REQUEST_LOG_ROUTE_NAMES = {
    "admin_readonly_sql": "db query result",
    "admin_request_log": "request log query",
    "admin_s3_device_log": "s3 log result",
    "admin_s3_ultrasound": "s3 ultrasound result",
    "barcode_evidence_freeform": "llm_freeform",
    "barcode_log_analysis": "barcode log analysis",
    "company_freeform": "llm_freeform",
    "device_agent_update": "device agent update",
    "device_audio_probe": "device audio probe",
    "device_box_update": "device box update",
    "device_captureboard_probe": "device captureboard probe",
    "device_db_detail": "devices_filter",
    "device_detail": "devices_filter",
    "device_diagnostic_analysis": "device diagnostic freeform",
    "device_diagnostic_followup": "device diagnostic followup",
    "device_diagnostic_snapshot": "device diagnostic snapshot",
    # 기존 파일 handler는 별도 route를 설정하지 않아 app_mention 초기값이
    # 조회·다운로드·복구와 다운로드 바코드 누락 응답의 저장 이름이었다.
    "device_file_download": "app_mention",
    "device_file_download_barcode_required": "app_mention",
    "device_file_lookup": "app_mention",
    "device_file_recovery": "app_mention",
    "device_led_log_analysis": "device led log analysis",
    "device_led_pattern_guide": "device led pattern guide",
    "device_led_probe": "device led probe",
    "device_log_upload": "device log upload check",
    "device_memory_patch": "device memory patch",
    "device_pm2_probe": "device pm2 probe",
    "device_power_off": "device power off",
    "device_remote_access_probe": "device remote access probe",
    "device_status_probe": "device status probe",
    "device_update_status": "device update status",
    "device_voice_catalog": "device voice catalog",
    "device_voice_change": "device voice change",
    "notion_playbook_qa": "notion playbook qa",
    "recording_failure_analysis": "recording failure analysis",
    "security_review": "bot security review",
    "weekly_recordings_summary": "weekly recordings report",
}


def legacy_company_request_log_route_name(route: str) -> str:
    """회사 route를 이전 Slack request-log의 실제 저장 이름으로 바꾼다."""

    normalized = str(route or "").strip()
    return _LEGACY_COMPANY_REQUEST_LOG_ROUTE_NAMES.get(
        normalized,
        normalized,
    )


__all__ = ["legacy_company_request_log_route_name"]
