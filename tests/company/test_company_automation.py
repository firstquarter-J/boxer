from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import logging
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from boxer_company import automation
from boxer_company.automation import (
    AutomationCycleContractError,
    AutomationCycleRequest,
    AutomationCycleResult,
    AutomationCycleService,
    AutomationDelivery,
    DailyDeviceRoundCycleHandler,
    SmsDeliveryCycleHandler,
    WeeklyRecordingsCycleHandler,
    build_default_automation_cycle_service,
)


_KST = ZoneInfo("Asia/Seoul")


def _request(
    cycle: str,
    *,
    cursor: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
) -> AutomationCycleRequest:
    return AutomationCycleRequest(
        request_id="cycle:test:1",
        tenant_id="lifex",
        cycle=cycle,  # type: ignore[arg-type]
        scheduled_at=datetime(2026, 8, 10, 9, 0, tzinfo=_KST),
        cursor=cursor or {},
        options=options or {},
    )


def test_cycle_contract_hides_state_and_payload_from_repr() -> None:
    request = _request(
        "sms_delivery",
        cursor={"marker": "private-person-value"},
    )
    delivery = AutomationDelivery(
        delivery_id="sms_delivery:1",
        kind="sms_delivery_result",
        payload={"marker": "private-person-value"},
    )
    result = AutomationCycleResult(
        cycle="sms_delivery",
        outcome="completed",
        cursor={"marker": "private-person-value"},
        deliveries=(delivery,),
        metrics={"updatedCount": 1},
    )

    # 디버그 repr가 상태나 delivery 원문을 우연히 로그에 남기지 않아야 한다.
    assert "private-person-value" not in repr(request)
    assert "private-person-value" not in repr(delivery)
    assert "private-person-value" not in repr(result)


@pytest.mark.parametrize(
    "payload",
    (
        {"apiToken": "secret-value"},
        {"nested": {"error": "password=secret-value"}},
        {"message": "Bearer secret-value"},
    ),
)
def test_delivery_rejects_secret_and_raw_error_fields(
    payload: dict[str, Any],
) -> None:
    with pytest.raises(AutomationCycleContractError):
        AutomationDelivery(
            delivery_id="health:1",
            kind="device_health_alert",
            payload=payload,
        )


def test_service_calls_mutation_capable_handler_once_without_logging_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret_marker = "phone-number-or-secret-marker"

    @dataclass
    class FailingHandler:
        name: str = "daily_device_round"
        calls: int = 0

        def run(
            self,
            request: AutomationCycleRequest,
        ) -> AutomationCycleResult:
            self.calls += 1
            raise RuntimeError(secret_marker)

    handler = FailingHandler()
    service = AutomationCycleService(
        (handler,),  # type: ignore[arg-type]
        logger=logging.getLogger("test_company_automation"),
    )

    with caplog.at_level(logging.WARNING):
        with pytest.raises(RuntimeError):
            service.run(_request("daily_device_round"))

    assert handler.calls == 1
    assert secret_marker not in caplog.text
    assert "error_type=RuntimeError" in caplog.text


def test_weekly_cycle_returns_channel_neutral_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def _fake_summary(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {
            "weekStartDate": "2026-08-03",
            "weekEndDate": "2026-08-09",
            "hospitalCount": 2,
            "totalCount": 31,
            "topRows": [],
        }

    monkeypatch.setattr(
        automation,
        "_build_weekly_recordings_report_summary",
        _fake_summary,
    )

    result = WeeklyRecordingsCycleHandler().run(
        _request("weekly_recordings")
    )

    assert len(calls) == 1
    assert result.outcome == "completed"
    assert result.auto_retry_allowed is False
    assert result.cursor["lastReportedWeekStartDate"] == "2026-08-03"
    assert result.cursor["cycleCompleted"] is True
    assert result.deliveries[0].kind == "weekly_recordings_report"
    assert result.deliveries[0].payload["totalCount"] == 31
    assert "blocks" not in result.deliveries[0].payload
    assert "channel" not in result.deliveries[0].payload


def test_daily_cycle_reuses_sync_domain_once_and_redacts_raw_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def _fake_summary(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {
            "hospitalSeq": 22,
            "hospitalName": "테스트병원",
            "deviceCount": 1,
            "candidateHospitalCount": 2,
            "nextHospitalSeq": 23,
            "hospitalScope": "all",
            "hospitalOrder": "hospital_seq_asc",
            "statusCounts": {"이상": 1},
            "updateCounts": {"agentUpdated": 1},
            "cleanupCounts": {"executed": 1},
            "powerCounts": {"poweredOff": 0},
            "deviceResults": [
                {
                    "deviceName": "MB2-TEST",
                    "roomName": "진료실",
                    "overallLabel": "점검 불가",
                    "componentLabels": {
                        "audio": "정상",
                        "pm2": "정상",
                        "storage": "확인 필요",
                        "captureboard": "정상",
                        "led": "정상",
                    },
                    "statusPayload": {
                        "ssh": {
                            "ready": False,
                            "host": "synthetic-secret-host",
                            "port": 22022,
                            "command": "echo synthetic-secret-command",
                            "env": {"PRIVATE": "synthetic-secret-env"},
                        },
                        "overview": {
                            "storage": {
                                "label": "확인 필요",
                                "filesystemUsedPercent": 81,
                                "detail": "synthetic-secret-detail",
                            }
                        },
                    },
                    "initialPlan": {
                        "agent": {"currentVersion": "1.0.0"},
                        "box": {"currentVersion": "2.0.0"},
                    },
                    "finalPlan": {
                        "agent": {
                            "currentVersion": "1.1.0",
                            "isHealthy": True,
                        },
                        "box": {
                            "currentVersion": "2.1.0",
                            "alreadyLatest": True,
                        },
                    },
                    "agentAction": {
                        "status": "completed",
                        "ok": True,
                        "payload": {
                            "command": "synthetic-secret-agent-command"
                        },
                    },
                    "boxAction": {
                        "status": "completed",
                        "ok": True,
                        "payload": {
                            "response": "{\"secret\":\"synthetic-secret-json\"}"
                        },
                    },
                    "powerAction": {
                        "status": "completed",
                        "ok": True,
                        "payload": {"host": "synthetic-secret-power-host"},
                    },
                    "trashcanCleanup": {
                        "status": "failed",
                        "detail": "synthetic-secret-cleanup-detail",
                    },
                    "error": "password=synthetic-secret-error",
                }
            ],
        }

    monkeypatch.setattr(
        automation,
        "_build_daily_device_round_summary",
        _fake_summary,
    )
    result = DailyDeviceRoundCycleHandler().run(
        _request(
            "daily_device_round",
            cursor={"processedHospitalSeqs": [21]},
            options={
                "autoUpdateAgent": True,
                "autoUpdateBoxFree": False,
                "autoUpdateBoxPaid": False,
                "autoCleanupTrashCan": True,
                "autoPowerOff": False,
            },
        )
    )

    assert len(calls) == 1
    assert calls[0]["auto_update_agent"] is True
    assert calls[0]["auto_update_box_free"] is False
    assert calls[0]["auto_update_box_paid"] is False
    assert calls[0]["auto_cleanup_trashcan"] is True
    assert result.cursor["processedHospitalSeqs"] == [21, 22]
    assert result.cursor["windowCompletedAt"] == (
        "2026-08-10T09:00:00+09:00"
    )
    assert result.cursor["cycleCompleted"] is True
    assert result.cursor["hospitalScope"] == "all"
    assert result.deliveries[0].kind == "daily_device_round_report"
    payload = result.deliveries[0].payload
    device = payload["deviceResults"][0]
    assert set(device) == {
        "deviceName",
        "roomName",
        "overallLabel",
        "networkUnavailable",
        "issueSummary",
        "storage",
        "cleanup",
        "agentUpdate",
        "boxUpdate",
        "power",
    }
    assert device["networkUnavailable"] is True
    assert device["storage"]["filesystemUsedPercent"] == 81
    assert set(device["agentUpdate"]) == {
        "actionable",
        "statusKind",
        "label",
        "summary",
    }
    assert set(device["boxUpdate"]) == set(device["agentUpdate"])
    assert set(device["power"]) == {
        "visible",
        "statusKind",
        "label",
        "summary",
    }
    from boxer_company_api.automation import serialize_automation_cycle_result

    serialized = json.dumps(
        serialize_automation_cycle_result(result, "cycle:test:1"),
        ensure_ascii=False,
        sort_keys=True,
    )
    # 실행용 구조와 synthetic secret은 API delivery 직렬화 전에 사라져야 한다.
    for forbidden in (
        "statusPayload",
        '"command"',
        '"host"',
        '"port"',
        '"env"',
        '"error"',
        '"detail"',
        "synthetic-secret",
    ):
        assert forbidden not in serialized


def test_daily_cycle_rejects_non_boolean_mutation_option(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def _fake_summary(**kwargs: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(
        automation,
        "_build_daily_device_round_summary",
        _fake_summary,
    )

    with pytest.raises(AutomationCycleContractError):
        DailyDeviceRoundCycleHandler().run(
            _request(
                "daily_device_round",
                options={"autoUpdateAgent": "true"},
            )
        )

    # 잘못된 mutation 설정은 어떤 장비 호출보다 먼저 차단한다.
    assert called is False


def test_default_service_registers_channel_neutral_company_cycles() -> None:
    service = build_default_automation_cycle_service()

    assert service.cycle_names == (
        "weekly_recordings",
        "daily_device_round",
        "device_health_monitor",
        "device_notification_alert",
        "sms_delivery",
    )


def test_sms_delivery_cycle_runs_once_without_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[datetime] = []

    def _fake_cycle(logger: logging.Logger, *, now: datetime) -> int:
        del logger
        calls.append(now)
        return 3

    monkeypatch.setattr(
        automation,
        "run_sms_delivery_cycle_once",
        _fake_cycle,
    )
    request = _request("sms_delivery")
    result = SmsDeliveryCycleHandler().run(request)

    assert calls == [request.scheduled_at]
    assert result.outcome == "completed"
    assert result.deliveries == ()
    assert result.metrics == {"updatedCount": 3, "deliveryCount": 0}
    assert result.cursor["cycleCompleted"] is False
