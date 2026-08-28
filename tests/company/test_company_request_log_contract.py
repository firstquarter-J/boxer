from __future__ import annotations

import pytest

from boxer_company.assistant.request_log_contract import (
    legacy_company_request_log_route_name,
)


@pytest.mark.parametrize(
    ("route", "legacy_name"),
    (
        ("device_voice_catalog", "device voice catalog"),
        ("device_voice_change", "device voice change"),
        ("device_diagnostic_snapshot", "device diagnostic snapshot"),
        ("device_diagnostic_followup", "device diagnostic followup"),
        ("device_diagnostic_analysis", "device diagnostic freeform"),
        ("device_update_status", "device update status"),
        ("device_box_update", "device box update"),
        ("device_agent_update", "device agent update"),
        ("device_power_off", "device power off"),
        ("device_scanner_abi_patch", "device scanner ABI patch"),
        ("device_audio_probe", "device audio probe"),
        ("device_remote_access_probe", "device remote access probe"),
        ("device_memory_patch", "device memory patch"),
        ("device_pm2_probe", "device pm2 probe"),
        ("device_captureboard_probe", "device captureboard probe"),
        ("device_led_probe", "device led probe"),
        ("device_status_probe", "device status probe"),
        ("device_log_upload", "device log upload check"),
        ("admin_s3_ultrasound", "s3 ultrasound result"),
        ("admin_s3_device_log", "s3 log result"),
        ("admin_readonly_sql", "db query result"),
        ("admin_request_log", "request log query"),
        ("security_review", "bot security review"),
    ),
)
def test_maps_channel_neutral_routes_to_exact_legacy_names(
    route: str,
    legacy_name: str,
) -> None:
    assert legacy_company_request_log_route_name(route) == legacy_name


@pytest.mark.parametrize(
    "route",
    (
        "device_file_lookup",
        "device_file_download",
        "device_file_recovery",
        "device_file_download_barcode_required",
    ),
)
def test_file_operations_keep_the_original_app_mention_name(
    route: str,
) -> None:
    # legacy handler는 이 네 분기에서 route setter를 호출하지 않았으므로
    # app_mention event가 초기화한 이름이 최종 저장값이었다.
    assert legacy_company_request_log_route_name(route) == "app_mention"


@pytest.mark.parametrize(
    "route",
    (
        "app_user_lookup",
        "app_user_baby_selection_analysis",
        "barcode_pink_classification_reason",
        "barcode_validation_status",
        "recording_streaming_restore",
        "thread_playbook_learning",
    ),
)
def test_legacy_snake_case_names_stay_unchanged(route: str) -> None:
    assert legacy_company_request_log_route_name(route) == route


@pytest.mark.parametrize(
    ("route", "legacy_name"),
    (
        ("device_led_log_analysis", "device led log analysis"),
        ("device_led_pattern_guide", "device led pattern guide"),
        ("barcode_log_analysis", "barcode log analysis"),
        ("recording_failure_analysis", "recording failure analysis"),
        ("notion_playbook_qa", "notion playbook qa"),
        ("barcode_evidence_freeform", "llm_freeform"),
        ("company_freeform", "llm_freeform"),
        ("device_db_detail", "devices_filter"),
        ("device_detail", "devices_filter"),
        ("weekly_recordings_summary", "weekly recordings report"),
    ),
)
def test_preserves_existing_slack_bridge_mappings(
    route: str,
    legacy_name: str,
) -> None:
    assert legacy_company_request_log_route_name(route) == legacy_name


def test_normalizes_outer_whitespace_without_rewriting_unknown_routes() -> None:
    assert (
        legacy_company_request_log_route_name("  custom_company_route  ")
        == "custom_company_route"
    )
