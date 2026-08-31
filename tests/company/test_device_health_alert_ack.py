from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import threading

import pytest

from boxer_company.device_health_alert_ack import (
    claim_device_health_alert_acknowledgement,
    ensure_device_health_alert_ack_schema,
)


def _target() -> dict[str, object]:
    return {
        "hospitalSeq": 84,
        "hospitalName": "한국의료재단(영등포)",
        "hospital": "#84 한국의료재단(영등포)",
        "room": "4층 초음파실9",
        "device": "MB2-A00140",
        "issue": "녹화 파일 증가 정지가 120초 동안 지속됐어",
    }


def _claim(
    db_path: Path,
    *,
    actor_user_id: str,
    acknowledged_at: datetime,
    schema_prepared: bool = False,
    workspace_id: str = "T-LIFEX",
    channel_id: str = "C-DEVICE-ALERT",
    message_ts: str = "1788134832.709819",
    target: dict[str, object] | None = None,
):
    return claim_device_health_alert_acknowledgement(
        workspace_id=workspace_id,
        channel_id=channel_id,
        message_ts=message_ts,
        target=target or _target(),
        actor_user_id=actor_user_id,
        acknowledged_at=acknowledged_at,
        db_path=db_path,
        schema_prepared=schema_prepared,
    )


def test_first_claim_persists_canonical_actor_and_utc_time(tmp_path: Path) -> None:
    db_path = tmp_path / "request_log.db"
    acknowledged_at = datetime(
        2026,
        8,
        31,
        9,
        12,
        34,
        987654,
        tzinfo=timezone(timedelta(hours=9)),
    )

    result = _claim(
        db_path,
        actor_user_id="U-FIRST",
        acknowledged_at=acknowledged_at,
    )

    assert result.created is True
    assert result.actor_user_id == "U-FIRST"
    assert result.acknowledged_at == datetime(
        2026,
        8,
        31,
        0,
        12,
        34,
        tzinfo=timezone.utc,
    )


def test_duplicate_claim_keeps_first_actor_and_time_after_reopen(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "request_log.db"
    first_at = datetime(2026, 8, 31, 0, 12, 34, tzinfo=timezone.utc)
    duplicate_at = datetime(2026, 8, 31, 0, 20, 0, tzinfo=timezone.utc)
    first = _claim(
        db_path,
        actor_user_id="U-FIRST",
        acknowledged_at=first_at,
    )

    # claim마다 SQLite 연결을 다시 열어 프로세스 내 cache가 아니라 영속 row가
    # 최초 담당자와 시간을 보존하는 계약을 확인한다.
    duplicate = _claim(
        db_path,
        actor_user_id="U-SECOND",
        acknowledged_at=duplicate_at,
    )

    assert first.created is True
    assert duplicate.created is False
    assert duplicate.actor_user_id == "U-FIRST"
    assert duplicate.acknowledged_at == first_at


def test_concurrent_claims_create_exactly_one_acknowledgement(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "request_log.db"
    ensure_device_health_alert_ack_schema(db_path)
    worker_count = 8
    barrier = threading.Barrier(worker_count)

    def claim(index: int):
        # 모든 worker가 같은 카드의 write transaction에 함께 진입하도록 맞춰
        # unique claim의 first-writer-wins 보장을 실제 SQLite에서 검증한다.
        barrier.wait(timeout=5)
        return _claim(
            db_path,
            actor_user_id=f"U-{index}",
            acknowledged_at=datetime(
                2026,
                8,
                31,
                0,
                12,
                index,
                tzinfo=timezone.utc,
            ),
        )

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        results = list(executor.map(claim, range(worker_count)))

    created = [result for result in results if result.created]
    assert len(created) == 1
    winner = created[0]
    assert all(result.actor_user_id == winner.actor_user_id for result in results)
    assert all(
        result.acknowledged_at == winner.acknowledged_at
        for result in results
    )


def test_prepared_claim_does_not_recreate_deleted_central_database(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "request_log.db"
    ensure_device_health_alert_ack_schema(db_path)
    db_path.unlink()

    # 운영 runtime은 startup 뒤 중앙 DB가 사라지면 새 ACK 정본을 만들지
    # 않고 실패해 기존 중복 방지 이력이 초기화된 것처럼 동작하지 않는다.
    with pytest.raises(sqlite3.OperationalError):
        _claim(
            db_path,
            actor_user_id="U-FIRST",
            acknowledged_at=datetime(
                2026,
                8,
                31,
                0,
                12,
                34,
                tzinfo=timezone.utc,
            ),
            schema_prepared=True,
        )

    assert not db_path.exists()


@pytest.mark.parametrize(
    "distinct_identity",
    (
        {"workspace_id": "T-OTHER"},
        {"channel_id": "C-OTHER"},
        {"message_ts": "1788134832.709820"},
        {"target": {**_target(), "device": "MB2-OTHER"}},
    ),
)
def test_each_card_identity_axis_creates_an_independent_claim(
    tmp_path: Path,
    distinct_identity: dict[str, object],
) -> None:
    db_path = tmp_path / "request_log.db"
    acknowledged_at = datetime(2026, 8, 31, 0, 12, 34, tzinfo=timezone.utc)
    first = _claim(
        db_path,
        actor_user_id="U-FIRST",
        acknowledged_at=acknowledged_at,
    )
    distinct = _claim(
        db_path,
        actor_user_id="U-SECOND",
        acknowledged_at=acknowledged_at,
        **distinct_identity,
    )

    # workspace/channel/message/target 어느 축이든 달라지면 다른 카드다.
    assert first.created is True
    assert distinct.created is True
    assert distinct.actor_user_id == "U-SECOND"
