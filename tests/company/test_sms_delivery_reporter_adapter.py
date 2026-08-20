import logging
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from boxer_company_adapter_slack import sms_delivery_reporter


class SmsDeliveryReporterAdapterTests(unittest.TestCase):
    def test_once_wrapper_delegates_domain_cycle_without_local_execution(self) -> None:
        now = datetime(2026, 8, 14, 10, 0, tzinfo=timezone.utc)
        logger = logging.getLogger("test.sms_delivery_reporter_adapter")

        # Adapter가 실행 시각만 전달하고 outbox/provider/Sheet 로직을 소유하지 않는지 고정한다.
        with patch.object(
            sms_delivery_reporter,
            "run_sms_delivery_cycle_once",
            return_value=2,
        ) as cycle_mock:
            changed = sms_delivery_reporter._run_sms_delivery_reporter_once(
                logger,
                now=now,
            )

        self.assertEqual(changed, 2)
        cycle_mock.assert_called_once_with(logger, now=now)

    def test_remote_once_calls_api_without_local_provider_cycle(self) -> None:
        now = datetime(2026, 8, 14, 10, 0, tzinfo=timezone.utc)
        logger = logging.getLogger("test.sms_delivery_reporter_adapter")
        api_client = unittest.mock.Mock()
        api_client.run.return_value = SimpleNamespace(
            deliveries=(),
            metrics={"updatedCount": 4},
        )

        with patch.object(
            sms_delivery_reporter,
            "run_sms_delivery_cycle_once",
        ) as local_cycle:
            changed = sms_delivery_reporter._run_sms_delivery_reporter_once(
                logger,
                now=now,
                automation_client=api_client,
            )

        self.assertEqual(changed, 4)
        local_cycle.assert_not_called()
        api_client.run.assert_called_once()
        self.assertEqual(
            api_client.run.call_args.kwargs["cycle"],
            "sms_delivery",
        )


if __name__ == "__main__":
    unittest.main()
