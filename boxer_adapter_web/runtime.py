from __future__ import annotations

import uvicorn

from boxer_adapter_web.app import create_web_app
from boxer_adapter_web.settings import get_web_settings


def main() -> None:
    settings = get_web_settings()
    uvicorn.run(
        create_web_app(),
        host=settings.host,
        port=settings.port,
    )


if __name__ == "__main__":
    main()
