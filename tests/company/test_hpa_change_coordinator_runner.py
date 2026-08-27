from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from boxer_company_api.hpa_change_coordinator_runner import (
    HpaChangeCoordinatorRunner,
    main,
)


class _Coordinator:
    def __init__(self) -> None:
        self.config = type("Config", (), {"poll_interval_sec": 17})()
        self.calls = 0

    def tick(self):
        self.calls += 1
        return (object(), object())


def test_runner_advances_api_coordinator_without_slack_dependency() -> None:
    coordinator = _Coordinator()
    runner = HpaChangeCoordinatorRunner(
        coordinator,  # type: ignore[arg-type]
        logging.getLogger("test.hpa.runner"),
    )

    assert runner.run_once() == 2
    assert coordinator.calls == 1


def test_runner_uses_configured_fixed_delay_between_cycles() -> None:
    coordinator = _Coordinator()
    runner = HpaChangeCoordinatorRunner(
        coordinator,  # type: ignore[arg-type]
        logging.getLogger("test.hpa.runner"),
    )
    waits: list[float] = []

    def stop_after_first(seconds: float) -> None:
        waits.append(seconds)
        raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        runner.run_forever(wait=stop_after_first)

    assert coordinator.calls == 1
    assert waits == [17]


def test_main_checks_runtime_security_before_loading_hpa_credentials() -> None:
    with (
        patch(
            "boxer_company_api.hpa_change_coordinator_runner."
            "validate_company_api_runtime_security",
            side_effect=RuntimeError("unsafe credential"),
        ) as security,
        patch(
            "boxer_company_api.hpa_change_coordinator_runner."
            "create_hpa_change_coordinator"
        ) as create_coordinator,
        pytest.raises(RuntimeError, match="unsafe credential"),
    ):
        main()

    security.assert_called_once_with()
    create_coordinator.assert_not_called()
