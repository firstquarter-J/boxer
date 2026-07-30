from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from uvicorn.config import LOGGING_CONFIG

from boxer_company_api import runtime


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class CompanyApiRuntimeTest(unittest.TestCase):
    def test_logging_config_exposes_safe_company_api_events(self) -> None:
        config = runtime._build_uvicorn_logging_config()

        self.assertNotIn("boxer.company_api", LOGGING_CONFIG["loggers"])
        self.assertEqual(
            config["loggers"]["boxer.company_api"],
            {
                "handlers": ["boxer_company_api_event"],
                "level": "INFO",
                "propagate": False,
            },
        )
        self.assertEqual(
            config["loggers"]["boxer_company_api.problems"],
            {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False,
            },
        )
        self.assertNotIn("boxer_company_api", config["loggers"])
        self.assertEqual(
            config["formatters"]["boxer_company_api_event"],
            {"format": "%(message)s"},
        )

        # 부모 프로세스의 logger를 바꾸지 않고 실제 dictConfig 출력 경계를 확인한다.
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import logging.config;"
                    "from boxer_company_api.observability import emit_api_event;"
                    "from boxer_company_api.runtime import "
                    "_build_uvicorn_logging_config;"
                    "logging.config.dictConfig("
                    "_build_uvicorn_logging_config());"
                    "emit_api_event("
                    "'company_api_turn_completed',"
                    "request_id='runtime-log-test',"
                    "status=200)"
                ),
            ],
            cwd=_PROJECT_ROOT,
            env={
                "BOXER_SKIP_DOTENV": "true",
            },
            capture_output=True,
            text=True,
            check=True,
        )
        # CloudWatch Logs Insights가 자동 추출할 수 있는 순수 JSON 한 줄이어야 한다.
        payload = json.loads(child.stderr.strip())
        self.assertEqual(payload["event"], "company_api_turn_completed")
        self.assertEqual(payload["request_id"], "runtime-log-test")
        self.assertEqual(payload["status"], 200)
        self.assertNotIn("question", payload)

    @patch("boxer_company_api.runtime.uvicorn.run")
    @patch("boxer_company_api.runtime.create_company_api_app")
    @patch("boxer_company_api.runtime.logging.config.dictConfig")
    @patch("boxer_company_api.runtime.validate_company_api_runtime_security")
    @patch("boxer_company_api.runtime.load_company_api_settings")
    def test_main_configures_logging_before_creating_app(
        self,
        load_settings,
        validate_security,
        configure_logging,
        create_app,
        uvicorn_run,
    ) -> None:
        settings = SimpleNamespace(host="127.0.0.1", port=8010)
        app = object()
        call_order: list[str] = []
        load_settings.return_value = settings
        configure_logging.side_effect = lambda _config: call_order.append("logging")

        # runtime 조립 실패도 구조화 logger에 남도록 app 생성보다 로그 설정이 앞서야 한다.
        def create_app_after_logging(*, settings):
            call_order.append("app")
            return app

        create_app.side_effect = create_app_after_logging

        runtime.main()

        validate_security.assert_called_once_with()
        configure_logging.assert_called_once()
        configured_loggers = configure_logging.call_args.args[0]["loggers"]
        self.assertEqual(
            configured_loggers["boxer.company_api"]["level"],
            "INFO",
        )
        create_app.assert_called_once_with(settings=settings)
        self.assertEqual(call_order, ["logging", "app"])
        uvicorn_run.assert_called_once()
        args, kwargs = uvicorn_run.call_args
        self.assertEqual(args, (app,))
        self.assertEqual(kwargs["host"], "127.0.0.1")
        self.assertEqual(kwargs["port"], 8010)
        self.assertIsNone(kwargs["log_config"])


if __name__ == "__main__":
    unittest.main()
