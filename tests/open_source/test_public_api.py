import unittest

import boxer_adapter_slack
from boxer_adapter_slack import common


class SlackPublicApiTests(unittest.TestCase):
    def test_package_exports_match_common_module(self) -> None:
        self.assertIs(boxer_adapter_slack.create_slack_app, common.create_slack_app)
        self.assertIs(boxer_adapter_slack.set_request_log_route, common.set_request_log_route)
        self.assertIs(
            boxer_adapter_slack.merge_request_log_metadata,
            common.merge_request_log_metadata,
        )

    def test_public_request_log_route_setter_updates_payload(self) -> None:
        payload = {"request_log": {}}

        boxer_adapter_slack.set_request_log_route(
            payload,
            "example_default",
            route_mode="faq",
            status="handled",
        )

        self.assertEqual(payload["request_log"]["route_name"], "example_default")
        self.assertEqual(payload["request_log"]["route_mode"], "faq")
        self.assertEqual(payload["request_log"]["status"], "handled")


if __name__ == "__main__":
    unittest.main()
