from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import stat

import pytest

from boxer_company import sms_delivery_cycle
from boxer_company.sms_delivery_cycle import (
    initialize_automatic_sms_recovery_state,
    inspect_automatic_sms_recovery_state,
)
from boxer_company import sms_recovery_state_initializer


_NOW = datetime(2026, 8, 20, 1, 0, tzinfo=timezone.utc)


def _claim_path(outbox: Path) -> Path:
    return outbox.with_name(f"{outbox.name}.automatic-claims.json")


def _write_protected(path: Path, payload: object) -> bytes:
    raw = (
        json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n"
    ).encode("utf-8")
    path.write_bytes(raw)
    os.chmod(path, 0o600)
    return raw


def test_initializer_creates_exact_owner_only_state_and_is_idempotent(
    tmp_path: Path,
) -> None:
    outbox = tmp_path / "sms-outbox.json"

    created = initialize_automatic_sms_recovery_state(
        outbox_path=outbox,
        expected_outbox_path=outbox,
        now=_NOW,
    )

    claims = _claim_path(outbox)
    assert created == {
        "kind": "sms_recovery_state_initializer",
        "initialized": True,
        "created": True,
        "stateDigest": created["stateDigest"],
        "outboxItemCount": 0,
        "unresolvedClaimCount": 0,
        "settledClaimCount": 0,
        "activeSettledClaimCount": 0,
    }
    assert json.loads(outbox.read_text(encoding="utf-8")) == {
        "version": 1,
        "items": [],
    }
    assert json.loads(claims.read_text(encoding="utf-8")) == {
        "version": 2,
        "claims": {},
    }
    assert stat.S_IMODE(outbox.stat().st_mode) == 0o600
    assert stat.S_IMODE(claims.stat().st_mode) == 0o600
    revisions = {
        path: (path.stat().st_ino, path.read_bytes())
        for path in (outbox, claims)
    }

    existing = initialize_automatic_sms_recovery_state(
        outbox_path=outbox,
        expected_outbox_path=outbox,
        now=_NOW,
    )

    assert existing["created"] is False
    assert existing["stateDigest"] == created["stateDigest"]
    assert {
        path: (path.stat().st_ino, path.read_bytes())
        for path in (outbox, claims)
    } == revisions


def test_initializer_never_overwrites_existing_valid_claim_data(
    tmp_path: Path,
) -> None:
    outbox = tmp_path / "sms-outbox.json"
    initialize_automatic_sms_recovery_state(
        outbox_path=outbox,
        expected_outbox_path=outbox,
        now=_NOW,
    )
    claims = _claim_path(outbox)
    _write_protected(
        claims,
        {
            "version": 2,
            "claims": {
                "b" * 64: {
                    "claimedAt": _NOW.isoformat(),
                    "state": "settled",
                    "groupHash": "",
                }
            },
        },
    )
    original_revision = (claims.stat().st_ino, claims.read_bytes())

    result = initialize_automatic_sms_recovery_state(
        outbox_path=outbox,
        expected_outbox_path=outbox,
        now=_NOW,
    )

    assert result["created"] is False
    assert result["settledClaimCount"] == 1
    assert result["activeSettledClaimCount"] == 1
    assert (claims.stat().st_ino, claims.read_bytes()) == original_revision


@pytest.mark.parametrize("existing_leaf", ("outbox", "claims"))
def test_initializer_fails_closed_on_partial_state_without_overwrite(
    tmp_path: Path,
    existing_leaf: str,
) -> None:
    outbox = tmp_path / "sms-outbox.json"
    claims = _claim_path(outbox)
    existing_path = outbox if existing_leaf == "outbox" else claims
    existing_payload = (
        {"version": 1, "items": []}
        if existing_leaf == "outbox"
        else {"version": 2, "claims": {}}
    )
    existing_raw = _write_protected(existing_path, existing_payload)

    with pytest.raises(ValueError, match="부분 초기화"):
        initialize_automatic_sms_recovery_state(
            outbox_path=outbox,
            expected_outbox_path=outbox,
            now=_NOW,
        )

    assert existing_path.read_bytes() == existing_raw
    assert not (claims if existing_leaf == "outbox" else outbox).exists()


def test_initializer_rolls_back_only_its_first_leaf_on_second_create_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outbox = tmp_path / "sms-outbox.json"
    claims = _claim_path(outbox)
    real_create = sms_delivery_cycle._create_sms_recovery_json_file

    def _fail_claim_create(
        path: Path,
        payload: dict[str, object],
    ) -> tuple[str, int, int]:
        if path == claims:
            raise OSError("simulated claim create failure")
        return real_create(path, payload)

    monkeypatch.setattr(
        sms_delivery_cycle,
        "_create_sms_recovery_json_file",
        _fail_claim_create,
    )

    with pytest.raises(ValueError, match="초기화하지 못했어"):
        initialize_automatic_sms_recovery_state(
            outbox_path=outbox,
            expected_outbox_path=outbox,
            now=_NOW,
        )

    assert not outbox.exists()
    assert not claims.exists()


def test_initializer_rejects_noncanonical_or_unprotected_target(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.json"
    linked = tmp_path / "linked.json"
    linked.symlink_to(target)
    with pytest.raises(ValueError, match="canonical"):
        initialize_automatic_sms_recovery_state(
            outbox_path=linked,
            expected_outbox_path=linked,
        )

    loose_parent = tmp_path / "loose"
    loose_parent.mkdir(mode=0o700)
    os.chmod(loose_parent, 0o777)
    loose_outbox = loose_parent / "sms-outbox.json"
    with pytest.raises(ValueError, match="보호되지"):
        initialize_automatic_sms_recovery_state(
            outbox_path=loose_outbox,
            expected_outbox_path=loose_outbox,
        )
    assert not loose_outbox.exists()


def test_initializer_cli_requires_stopped_owner_and_exact_configured_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configured = tmp_path / "configured.json"
    alternate = tmp_path / "alternate.json"
    monkeypatch.setattr(
        sms_recovery_state_initializer.company_settings,
        "SMS_DELIVERY_OUTBOX_PATH",
        str(configured),
    )

    with pytest.raises(SystemExit) as exc_info:
        sms_recovery_state_initializer.main(
            ["--sms-outbox-path", str(configured)]
        )
    assert str(exc_info.value) == (
        "sms_recovery_state_initializer_requires_stopped_service"
    )
    assert not configured.exists()

    with pytest.raises(SystemExit, match="설정된 경로와 달라"):
        sms_recovery_state_initializer.main(
            [
                "--sms-outbox-path",
                str(alternate),
                "--confirm-owner-service-stopped",
            ]
        )
    assert not alternate.exists()

    assert sms_recovery_state_initializer.main(
        [
            "--sms-outbox-path",
            str(configured),
            "--confirm-owner-service-stopped",
        ]
    ) == 0
    output = capsys.readouterr().out
    result = json.loads(output)
    assert set(result) == {
        "kind",
        "initialized",
        "created",
        "stateDigest",
        "outboxItemCount",
        "unresolvedClaimCount",
        "settledClaimCount",
        "activeSettledClaimCount",
    }
    assert str(configured) not in output
    assert "claims" not in output
    assert "items" not in output


def test_strict_inspector_counts_only_unexpired_settled_claims(
    tmp_path: Path,
) -> None:
    outbox = tmp_path / "sms-outbox.json"
    initialize_automatic_sms_recovery_state(
        outbox_path=outbox,
        expected_outbox_path=outbox,
        now=_NOW,
    )
    _write_protected(
        _claim_path(outbox),
        {
            "version": 2,
            "claims": {
                "a" * 64: {
                    "claimedAt": (_NOW - timedelta(seconds=59)).isoformat(),
                    "state": "settled",
                    "groupHash": "",
                }
            },
        },
    )

    active = inspect_automatic_sms_recovery_state(
        outbox_path=outbox,
        expected_outbox_path=outbox,
        require_initialized=True,
        now=_NOW,
    )
    expired = inspect_automatic_sms_recovery_state(
        outbox_path=outbox,
        expected_outbox_path=outbox,
        require_initialized=True,
        now=_NOW + timedelta(seconds=1),
    )

    assert type(active["activeSettledClaimCount"]) is int
    assert active["activeSettledClaimCount"] == 1
    assert active["settledClaimCount"] == 1
    # runtime과 동일하게 exact 60초 경계는 재claim 가능하다.
    assert expired["activeSettledClaimCount"] == 0
    assert expired["stateDigest"] == active["stateDigest"]
