#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="${PYTHON_FALLBACK_BIN:-python3.11}"
fi

export SLACK_BOT_TOKEN="${SLACK_BOT_TOKEN:-xoxb-sample-token}"
export SLACK_APP_TOKEN="${SLACK_APP_TOKEN:-xapp-sample-token}"
export SLACK_SIGNING_SECRET="${SLACK_SIGNING_SECRET:-sample-signing-secret}"
export ADAPTER_ENTRYPOINT="${ADAPTER_ENTRYPOINT:-boxer.adapters.sample.slack:create_app}"

if ! "${PYTHON_BIN}" -c "import slack_bolt" >/dev/null 2>&1; then
  echo "[fail] slack_bolt import 불가. .venv 생성 후 requirements 설치가 필요해" >&2
  exit 1
fi

"${PYTHON_BIN}" - <<'PY'
from slack_sdk.web.client import WebClient

WebClient.auth_test = lambda self, **kwargs: {  # type: ignore[method-assign]
    "ok": True,
    "user_id": "U_SAMPLE",
    "team_id": "T_SAMPLE",
    "bot_id": "B_SAMPLE",
}

from boxer.adapters.factory import create_app

app = create_app()
print(type(app).__name__)
PY

echo "[ok] sample adapter factory smoke test passed"
