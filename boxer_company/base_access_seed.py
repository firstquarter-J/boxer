from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence

from boxer_company import settings as company_settings
from boxer_company.base_access import (
    BaseAccessSeedMember,
    BaseAccessSettings,
    ConfigurationError,
    ConflictError,
    StoreUnavailable,
    ValidationError,
    build_base_access_store,
)


HYUN_SLACK_USER_ID = company_settings.BOXER_ACCESS_ADMIN_USER_ID
FIXED_SEED_MEMBERS: tuple[BaseAccessSeedMember, ...] = (
    BaseAccessSeedMember(user_id=HYUN_SLACK_USER_ID, display_name="Hyun"),
    BaseAccessSeedMember(user_id="U02KMT8TJRZ", display_name="Leon"),
    BaseAccessSeedMember(user_id="U02KMSWQN02", display_name="Dana"),
    BaseAccessSeedMember(user_id="U061M95HXMX", display_name="Rosa"),
    BaseAccessSeedMember(user_id="U037PL53L76", display_name="Zion"),
    BaseAccessSeedMember(user_id="U06D6GA28JV", display_name="Jalen"),
    BaseAccessSeedMember(user_id="U07A5FM5XPD", display_name="Justin Hyeon"),
)
ENV_SEED_MEMBERS: tuple[tuple[str, str], ...] = (
    ("MARK_USER_ID", "Mark"),
    ("DD_USER_ID", "DD"),
    ("JUNE_USER_ID", "June"),
    ("JUNO_USER_ID", "Juno"),
    ("ROY_USER_ID", "Roy"),
    ("MARU_USER_ID", "Maru"),
    ("PAUL_USER_ID", "Paul"),
    ("DANNY_USER_ID", "Danny"),
    ("LUKA_USER_ID", "Luka"),
    ("OLIVIA_USER_ID", "Olivia"),
    ("SAGE_USER_ID", "Sage"),
)


def load_initial_seed_members(
    environ: Mapping[str, str] | None = None,
) -> tuple[BaseAccessSeedMember, ...]:
    """고정 사용자와 팀 env를 합치되 누락·중복이면 seed 전에 중단한다."""

    source = os.environ if environ is None else environ
    if str(source.get("HYUN_USER_ID") or "").strip() != HYUN_SLACK_USER_ID:
        raise ValidationError("HYUN_USER_ID가 고정 Hyun 사용자와 일치하지 않아")

    members = list(FIXED_SEED_MEMBERS)
    for env_name, display_name in ENV_SEED_MEMBERS:
        user_id = str(source.get(env_name) or "").strip()
        if not user_id:
            raise ValidationError(f"{env_name}가 비어 있어")
        members.append(BaseAccessSeedMember(user_id=user_id, display_name=display_name))
    if len(members) != 18 or len({member.user_id for member in members}) != 18:
        raise ValidationError("초기 사용자는 중복 없이 정확히 18명이어야 해")
    return tuple(members)


def seed_initial_members(
    *,
    workspace_id: str,
    requested_by: str,
    environ: Mapping[str, str] | None = None,
) -> bool:
    source = os.environ if environ is None else environ
    state_path = str(
        source.get("BOXER_BASE_ACCESS_STATE_PATH")
        or company_settings.BOXER_BASE_ACCESS_STATE_PATH
    ).strip()
    store = build_base_access_store(
        BaseAccessSettings(state_path=state_path),
    )
    return store.seed_members(
        workspace_id,
        load_initial_seed_members(source),
        requested_by,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Boxer 기본 사용자 권한을 한 번 seed해")
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--requested-by", default=HYUN_SLACK_USER_ID)
    args = parser.parse_args(argv)
    try:
        changed = seed_initial_members(
            workspace_id=args.workspace_id,
            requested_by=args.requested_by,
        )
    except (ConfigurationError, ConflictError, StoreUnavailable, ValidationError) as exc:
        # 파일 경로나 운영 상세를 덧붙이지 않고 정제된 도메인 오류만 노출한다.
        print(f"seed 실패: {exc}", file=sys.stderr)
        return 1
    print("초기 사용자 18명을 저장했어" if changed else "초기 사용자가 이미 저장돼 있어")
    return 0


if __name__ == "__main__":  # pragma: no cover - console script 진입점
    raise SystemExit(main())
