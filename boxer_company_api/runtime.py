from __future__ import annotations

import uvicorn

from boxer_company_api.app import create_company_api_app
from boxer_company_api.security import validate_company_api_runtime_security
from boxer_company_api.settings import load_company_api_settings


def main() -> None:
    settings = load_company_api_settings()
    # 운영 프로세스는 인스턴스 역할 외 AWS credential 주입을 시작 전에 차단한다.
    validate_company_api_runtime_security()
    uvicorn.run(
        create_company_api_app(settings=settings),
        host=settings.host,
        port=settings.port,
    )


if __name__ == "__main__":
    main()
