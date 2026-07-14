"""Public web adapter package for Boxer.

`boxer` 코어는 지식 source와 LLM synthesis를 제공하고,
웹 런타임과 브라우저 위젯 통합은 이 패키지에서 담당한다.
"""

from boxer_adapter_web.app import create_web_app

__all__ = ["create_web_app"]
