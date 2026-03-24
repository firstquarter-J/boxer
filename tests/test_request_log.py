import tempfile
import unittest
from pathlib import Path

from boxer_adapter_slack.common import _set_request_log_route
from boxer.observability.request_log import (
    _list_request_log_recent,
    _save_request_log_record,
)


class RequestLogRouteSetterTests(unittest.TestCase):
    def test_sets_handler_type_in_request_log_context(self) -> None:
        payload = {"request_log": {}}

        _set_request_log_route(
            payload,
            "llm_freeform",
            route_mode="claude",
            handler_type="llm_freeform",
        )

        self.assertEqual(payload["request_log"]["route_name"], "llm_freeform")
        self.assertEqual(payload["request_log"]["route_mode"], "claude")
        self.assertEqual(payload["request_log"]["handler_type"], "llm_freeform")


class RequestLogHandlerTypePersistenceTests(unittest.TestCase):
    def test_persists_handler_type_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "request_log.db"

            _save_request_log_record(
                {
                    "sourcePlatform": "slack",
                    "workspaceId": "T123",
                    "eventType": "app_mention",
                    "routeName": "llm_freeform",
                    "routeMode": "claude",
                    "handlerType": "llm_freeform",
                    "status": "handled",
                    "userId": "U123",
                    "channelId": "C123",
                    "threadId": "1730000000.000100",
                    "messageId": "1730000000.000100",
                    "requestText": "@Boxer 자유대화 테스트",
                    "normalizedQuestion": "자유대화 테스트",
                },
                db_path=db_path,
            )

            result = _list_request_log_recent(db_path=db_path, limit=1)
            row = result["rows"][0]

            self.assertEqual(row["routeName"], "llm_freeform")
            self.assertEqual(row["routeMode"], "claude")
            self.assertEqual(row["handlerType"], "llm_freeform")


if __name__ == "__main__":
    unittest.main()
