import unittest

from boxer_company import sms_delivery


class SmsDeliveryTests(unittest.TestCase):
    def test_resolves_completed_success_as_delivered(self) -> None:
        result = sms_delivery._resolve_solapi_group_delivery_status(
            {
                "status": "COMPLETE",
                "count": {
                    "sentSuccess": 1,
                    "sentFailed": 0,
                    "sentPending": 0,
                    "registeredFailed": 0,
                },
            }
        )

        self.assertEqual(result, sms_delivery._SMS_DELIVERY_DELIVERED)

    def test_resolves_provider_failures_as_delivery_failed(self) -> None:
        scenarios = (
            {"status": "FAILED", "count": {}},
            {"status": "COMPLETE", "count": {"sentFailed": 1}},
            {"status": "SENDING", "count": {"registeredFailed": 1}},
        )

        for group_info in scenarios:
            with self.subTest(group_info=group_info):
                self.assertEqual(
                    sms_delivery._resolve_solapi_group_delivery_status(group_info),
                    sms_delivery._SMS_DELIVERY_FAILED,
                )

    def test_keeps_processing_group_pending(self) -> None:
        self.assertIsNone(
            sms_delivery._resolve_solapi_group_delivery_status(
                {
                    "status": "SENDING",
                    "count": {"sentPending": 1},
                }
            )
        )

    def test_marks_ambiguous_complete_group_for_confirmation(self) -> None:
        self.assertEqual(
            sms_delivery._resolve_solapi_group_delivery_status(
                {
                    "status": "COMPLETE",
                    "count": {
                        "sentSuccess": 0,
                        "sentFailed": 0,
                        "sentPending": 0,
                    },
                }
            ),
            sms_delivery._SMS_DELIVERY_CONFIRM_REQUIRED,
        )


if __name__ == "__main__":
    unittest.main()
