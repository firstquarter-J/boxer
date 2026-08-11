from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from boxer_company.base_access import ValidationError
from boxer_company.base_access_seed import (
    ENV_SEED_MEMBERS,
    FIXED_SEED_MEMBERS,
    HYUN_SLACK_USER_ID,
    load_initial_seed_members,
    main,
    seed_initial_members,
)


def _seed_env(state_path: Path) -> dict[str, str]:
    env = {
        "HYUN_USER_ID": HYUN_SLACK_USER_ID,
        "BOXER_BASE_ACCESS_STATE_PATH": str(state_path),
    }
    env.update(
        {
            env_name: f"U-TEAM-{index:02d}"
            for index, (env_name, _) in enumerate(ENV_SEED_MEMBERS, start=1)
        }
    )
    return env


def test_seed_members_are_exactly_seven_fixed_plus_eleven_env_users(
    tmp_path: Path,
) -> None:
    members = load_initial_seed_members(_seed_env(tmp_path / "access.json"))

    assert len(FIXED_SEED_MEMBERS) == 7
    assert len(ENV_SEED_MEMBERS) == 11
    assert len(members) == 18
    assert len({member.user_id for member in members}) == 18
    assert members[0].user_id == HYUN_SLACK_USER_ID
    assert {member.display_name for member in FIXED_SEED_MEMBERS} == {
        "Hyun",
        "Leon",
        "Dana",
        "Rosa",
        "Zion",
        "Jalen",
        "Justin Hyeon",
    }


def test_hyun_env_must_match_fixed_hyun_id(tmp_path: Path) -> None:
    env = _seed_env(tmp_path / "access.json")
    env["HYUN_USER_ID"] = "U-SOMEONE-ELSE"
    with pytest.raises(ValidationError):
        load_initial_seed_members(env)


def test_missing_or_duplicate_team_env_user_is_rejected(tmp_path: Path) -> None:
    missing = _seed_env(tmp_path / "access.json")
    missing.pop("MARK_USER_ID")
    with pytest.raises(ValidationError):
        load_initial_seed_members(missing)

    duplicate = _seed_env(tmp_path / "access.json")
    duplicate["MARK_USER_ID"] = HYUN_SLACK_USER_ID
    with pytest.raises(ValidationError):
        load_initial_seed_members(duplicate)


def test_seed_initial_members_creates_eighteen_members_once(tmp_path: Path) -> None:
    state_path = tmp_path / "access.json"
    env = _seed_env(state_path)

    assert seed_initial_members(
        workspace_id="T-WORKSPACE",
        requested_by=HYUN_SLACK_USER_ID,
        environ=env,
    ) is True
    assert seed_initial_members(
        workspace_id="T-WORKSPACE",
        requested_by=HYUN_SLACK_USER_ID,
        environ=env,
    ) is False

    document = json.loads(state_path.read_text(encoding="utf-8"))
    users = document["workspaces"]["T-WORKSPACE"]["users"]
    assert len(users) == 18
    assert all(member["allowed"] is True for member in users.values())


def test_cli_returns_safe_failure_without_raw_file_detail(capsys) -> None:
    with patch(
        "boxer_company.base_access_seed.seed_initial_members",
        side_effect=ValidationError("설정을 확인해"),
    ):
        exit_code = main(["--workspace-id", "T-WORKSPACE"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == "seed 실패: 설정을 확인해\n"
