from __future__ import annotations

from boxer_company_api.request_guard import MutationRequestGuard
from boxer_company_api.schemas import AssistantTurnInput


def _turn(*, question: str, route_group: str = "operations") -> AssistantTurnInput:
    return AssistantTurnInput.model_validate(
        {
            "tenantId": "TENANT-1",
            "actorId": "ACTOR-1",
            "channel": "slack",
            "conversationId": "THREAD-1",
            "question": question,
            "locale": "ko",
            "contextEntries": [],
            "routeGroup": route_group,
        }
    )


def test_different_request_ids_keep_legacy_concurrent_execution() -> None:
    guard = MutationRequestGuard()
    first = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-1",
        turn=_turn(question="MB2-C00419 장비 종료해줘"),
        mutation_capable=True,
    )
    second = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-2",
        turn=_turn(
            question="MB2-C00419 장비 정보",
            route_group="device_detail",
        ),
        mutation_capable=True,
    )

    assert first.status == "reserved"
    assert second.status == "reserved"


def test_completed_target_is_released_but_request_id_is_replayed() -> None:
    guard = MutationRequestGuard()
    first = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-1",
        turn=_turn(question="MB2-C00419 장비 종료해줘"),
        mutation_capable=True,
    )
    assert first.reservation is not None
    guard.complete(first.reservation, {"route": "device_power_off"})

    replay = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-1",
        turn=_turn(question="MB2-C00419 장비 종료해줘"),
        mutation_capable=True,
    )
    new_request = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-2",
        turn=_turn(question="MB2-C00419 장비 종료해줘"),
        mutation_capable=True,
    )

    assert replay.status == "replay"
    assert replay.payload == {"route": "device_power_off"}
    assert new_request.status == "reserved"


def test_read_only_route_bypasses_mutation_registry() -> None:
    decision = MutationRequestGuard().reserve(
        caller_id="slack-prod",
        request_id="REQ-1",
        turn=_turn(
            question="12345678910 영상 몇 개야",
            route_group="structured",
        ),
        mutation_capable=False,
    )

    assert decision.status == "bypass"


def test_released_precheck_reservation_allows_same_request_and_target() -> None:
    guard = MutationRequestGuard()
    first = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-1",
        turn=_turn(question="MB2-C00419 장비 종료해줘"),
        mutation_capable=True,
    )
    assert first.reservation is not None

    # 실제 외부 전송 전에 실패한 동일 request ID reservation을 푼다.
    guard.release(first.reservation)
    same_request = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-1",
        turn=_turn(question="MB2-C00419 장비 종료해줘"),
        mutation_capable=True,
    )

    assert same_request.status == "reserved"


def test_uncertain_mutation_never_expires_during_process_lifetime() -> None:
    now = [0.0]
    guard = MutationRequestGuard(
        clock=lambda: now[0],
        uncertain_ttl_sec=60.0,
    )
    first = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-1",
        turn=_turn(question="MB2-C00419 장비 종료해줘"),
        mutation_capable=True,
    )
    assert first.reservation is not None
    guard.mark_uncertain(first.reservation)

    now[0] = 1_000_000.0
    same_request = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-1",
        turn=_turn(question="MB2-C00419 장비 종료해줘"),
        mutation_capable=True,
    )
    new_request = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-2",
        turn=_turn(question="MB2-C00570 장비 종료해줘"),
        mutation_capable=True,
    )
    read_only_request = guard.reserve(
        caller_id="slack-prod",
        request_id="REQ-3",
        turn=_turn(question="12345678910 유저 조회"),
        mutation_capable=False,
    )

    assert same_request.status == "busy"
    assert new_request.status == "reserved"
    assert read_only_request.status == "bypass"
