from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from boxer_company import settings as company_settings
from boxer_company.sms_delivery_cycle import (
    initialize_automatic_sms_recovery_state,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Initialize the configured Boxer SMS outbox and claim state "
            "while its owner service is stopped."
        )
    )
    parser.add_argument(
        "--sms-outbox-path",
        default=company_settings.SMS_DELIVERY_OUTBOX_PATH,
    )
    parser.add_argument(
        "--confirm-owner-service-stopped",
        action="store_true",
        help="Confirm that the Slack or API process owning this path is stopped.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if not args.confirm_owner_service_stopped:
        raise SystemExit("sms_recovery_state_initializer_requires_stopped_service")
    configured_path = str(company_settings.SMS_DELIVERY_OUTBOX_PATH or "").strip()
    if not configured_path:
        raise SystemExit("configured_sms_outbox_path_is_required")
    try:
        result = initialize_automatic_sms_recovery_state(
            outbox_path=Path(args.sms_outbox_path),
            expected_outbox_path=configured_path,
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    # digest와 count만 출력하고 파일 경로·claim key·provider payload는 노출하지 않는다.
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
