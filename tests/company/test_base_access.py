from __future__ import annotations

import json
import os
import stat
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from boxer_company.base_access import (
    SEED_ORDERING_KEY,
    BaseAccessSeedMember,
    BaseAccessSettings,
    ConfigurationError,
    ConflictError,
    LocalFileBaseAccessStore,
    StoreUnavailable,
    ValidationError,
    build_base_access_store,
    slack_ts_to_ordering_key,
)


WORKSPACE_ID = "T-WORKSPACE"
USER_ID = "U-USER"
NOW = datetime(2026, 8, 11, 3, 0, tzinfo=timezone.utc)


def _store(path: Path) -> LocalFileBaseAccessStore:
    return LocalFileBaseAccessStore(state_path=path, now=lambda: NOW)


def _members() -> list[BaseAccessSeedMember]:
    return [
        BaseAccessSeedMember(user_id="U-HYUN", display_name="Hyun"),
        BaseAccessSeedMember(user_id=USER_ID, display_name="Rosa"),
    ]


def _seed(path: Path) -> LocalFileBaseAccessStore:
    store = _store(path)
    assert store.seed_members(WORKSPACE_ID, _members(), "U-HYUN") is True
    return store


def test_factory_builds_local_store_and_rejects_empty_or_directory_path(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "access.json"
    store = build_base_access_store(BaseAccessSettings(state_path=str(state_path)))

    assert isinstance(store, LocalFileBaseAccessStore)
    assert store.state_path == state_path.resolve()
    with pytest.raises(ConfigurationError):
        build_base_access_store(BaseAccessSettings(state_path=""))
    with pytest.raises(ConfigurationError):
        build_base_access_store(BaseAccessSettings(state_path=str(tmp_path)))


def test_slack_ts_is_normalized_to_fixed_width_ordering_key() -> None:
    assert slack_ts_to_ordering_key("1720580000.1") == "00000000001720580000.100000"
    assert slack_ts_to_ordering_key("1720580001") > slack_ts_to_ordering_key(
        "1720580000.999999"
    )
    with pytest.raises(ValidationError):
        slack_ts_to_ordering_key("1720580000.1234567")


def test_seed_creates_versioned_0600_json_without_roles_or_capabilities(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "access.json"
    _seed(state_path)

    document = json.loads(state_path.read_text(encoding="utf-8"))
    member = document["workspaces"][WORKSPACE_ID]["users"][USER_ID]
    assert document["version"] == 1
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    assert member == {
        "allowed": True,
        "displayName": "Rosa",
        "orderingKey": SEED_ORDERING_KEY,
        "updatedAt": "2026-08-11T03:00:00Z",
        "updatedBy": "U-HYUN",
    }
    assert not (
        {"role", "protected", "capability", "marker", "receipt", "audit"}
        & set(member)
    )


def test_each_lookup_rereads_file_and_tombstone_is_denied(tmp_path: Path) -> None:
    state_path = tmp_path / "access.json"
    store = _seed(state_path)
    assert store.is_allowed(WORKSPACE_ID, USER_ID) is True

    # 같은 store 인스턴스도 디스크 변경을 다시 읽어 revoke를 즉시 반영해야 한다.
    document = json.loads(state_path.read_text(encoding="utf-8"))
    document["workspaces"][WORKSPACE_ID]["users"][USER_ID]["allowed"] = False
    state_path.write_text(json.dumps(document), encoding="utf-8")
    os.chmod(state_path, 0o600)

    assert store.is_allowed(WORKSPACE_ID, USER_ID) is False


@pytest.mark.parametrize("broken_content", ["{", "[]", '{"version": 2, "workspaces": {}}'])
def test_missing_or_malformed_file_fails_closed(
    tmp_path: Path,
    broken_content: str,
) -> None:
    state_path = tmp_path / "access.json"
    store = _store(state_path)
    with pytest.raises(StoreUnavailable):
        store.is_allowed(WORKSPACE_ID, USER_ID)

    state_path.write_text(broken_content, encoding="utf-8")
    os.chmod(state_path, 0o600)
    with pytest.raises(StoreUnavailable):
        store.is_allowed(WORKSPACE_ID, USER_ID)


def test_file_with_broad_permissions_fails_closed(tmp_path: Path) -> None:
    state_path = tmp_path / "access.json"
    store = _seed(state_path)
    os.chmod(state_path, 0o644)

    with pytest.raises(StoreUnavailable):
        store.is_allowed(WORKSPACE_ID, USER_ID)


def test_set_allowed_persists_newer_operation_atomically(tmp_path: Path) -> None:
    state_path = tmp_path / "access.json"
    store = _seed(state_path)
    ordering_key = slack_ts_to_ordering_key("1720580001.000001")

    result = store.set_allowed(
        WORKSPACE_ID,
        USER_ID,
        False,
        "Rosa",
        "U-HYUN",
        ordering_key,
    )

    assert (result.allowed, result.changed, result.stale) == (False, True, False)
    assert store.get_member(WORKSPACE_ID, USER_ID) == result.member
    assert list(tmp_path.glob(".access.json.*.tmp")) == []
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600


def test_replay_conflict_and_stale_operation_do_not_overwrite_current_state(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "access.json"
    store = _seed(state_path)
    current_key = slack_ts_to_ordering_key("1720580002.000001")
    store.set_allowed(
        WORKSPACE_ID,
        USER_ID,
        False,
        "Rosa",
        "U-HYUN",
        current_key,
    )

    replay = store.set_allowed(
        WORKSPACE_ID,
        USER_ID,
        False,
        "Rosa",
        "U-HYUN",
        current_key,
    )
    assert (replay.allowed, replay.changed, replay.stale) == (False, False, False)

    with pytest.raises(ConflictError):
        store.set_allowed(
            WORKSPACE_ID,
            USER_ID,
            True,
            "Rosa",
            "U-HYUN",
            current_key,
        )

    stale = store.set_allowed(
        WORKSPACE_ID,
        USER_ID,
        True,
        "Rosa",
        "U-HYUN",
        slack_ts_to_ordering_key("1720580001.000001"),
    )
    assert (stale.allowed, stale.changed, stale.stale) == (False, False, True)


def test_seed_rerun_is_noop_even_after_member_was_revoked(tmp_path: Path) -> None:
    state_path = tmp_path / "access.json"
    store = _seed(state_path)
    store.set_allowed(
        WORKSPACE_ID,
        USER_ID,
        False,
        "Rosa",
        "U-HYUN",
        slack_ts_to_ordering_key("1720580001.000001"),
    )

    assert store.seed_members(WORKSPACE_ID, _members(), "U-HYUN") is False
    assert store.is_allowed(WORKSPACE_ID, USER_ID) is False


def test_seed_existing_partial_state_is_conflict_instead_of_repair(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "access.json"
    store = _store(state_path)
    store.seed_members(
        WORKSPACE_ID,
        [BaseAccessSeedMember(user_id="U-HYUN", display_name="Hyun")],
        "U-HYUN",
    )

    with pytest.raises(ConflictError):
        store.seed_members(WORKSPACE_ID, _members(), "U-HYUN")


def test_failed_atomic_replace_keeps_previous_file_and_removes_temp(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "access.json"
    store = _seed(state_path)
    previous = state_path.read_bytes()

    with patch("boxer_company.base_access.os.replace", side_effect=OSError("raw detail")):
        with pytest.raises(StoreUnavailable) as error:
            store.set_allowed(
                WORKSPACE_ID,
                USER_ID,
                False,
                "Rosa",
                "U-HYUN",
                slack_ts_to_ordering_key("1720580001.000001"),
            )

    assert "raw detail" not in str(error.value)
    assert state_path.read_bytes() == previous
    assert list(tmp_path.glob(".access.json.*.tmp")) == []
