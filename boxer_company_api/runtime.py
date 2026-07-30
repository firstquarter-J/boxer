from __future__ import annotations

from copy import deepcopy
import logging.config
from typing import Any

import uvicorn
from uvicorn.config import LOGGING_CONFIG

from boxer_company_api.app import create_company_api_app
from boxer_company_api.security import validate_company_api_runtime_security
from boxer_company_api.settings import load_company_api_settings


def _build_uvicorn_logging_config() -> dict[str, Any]:
    """Uvicorn 로그에 회사 API의 안전한 구조화 event logger를 연결한다."""

    logging_config = deepcopy(LOGGING_CONFIG)
    formatters = logging_config.setdefault("formatters", {})
    handlers = logging_config.setdefault("handlers", {})
    loggers = logging_config.setdefault("loggers", {})
    # audit event는 CloudWatch Logs Insights가 필드를 바로 추출할 수 있도록
    # Uvicorn level prefix가 없는 JSON 한 줄 그대로 stderr에 쓴다.
    formatters["boxer_company_api_event"] = {
        "format": "%(message)s",
    }
    handlers["boxer_company_api_event"] = {
        "class": "logging.StreamHandler",
        "formatter": "boxer_company_api_event",
        "stream": "ext://sys.stderr",
    }
    # Uvicorn 기본 설정은 root handler를 두지 않아서 별도 설정이 없으면
    # boxer.company_api의 INFO audit event가 운영 stdout에서 사라진다.
    loggers["boxer.company_api"] = {
        "handlers": ["boxer_company_api_event"],
        "level": "INFO",
        "propagate": False,
    }
    # 안전한 필드만 기록하는 problem handler만 명시적으로 연결해 미래의
    # 다른 회사 API 하위 logger가 우발적으로 수집되는 범위를 막는다.
    loggers["boxer_company_api.problems"] = {
        "handlers": ["default"],
        "level": "INFO",
        "propagate": False,
    }
    return logging_config


def main() -> None:
    settings = load_company_api_settings()
    # 운영 프로세스는 인스턴스 역할 외 AWS credential 주입을 시작 전에 차단한다.
    validate_company_api_runtime_security()
    # app factory가 runtime을 조립하면서 내는 초기화 실패 event도 놓치지 않도록
    # Uvicorn이 app을 평가하기 전에 동일한 logging config를 먼저 적용한다.
    logging.config.dictConfig(_build_uvicorn_logging_config())
    uvicorn.run(
        create_company_api_app(settings=settings),
        host=settings.host,
        port=settings.port,
        # 이미 적용한 설정을 Uvicorn이 다시 덮어쓰거나 중복 handler로 만들지 않는다.
        log_config=None,
    )


if __name__ == "__main__":
    main()
