#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PYTHON_BIN="${PYTHON_BIN:-python3.11}"

check_absent() {
  local label="$1"
  local pattern="$2"
  shift 2

  echo "[check] ${label}"
  if rg -n "${pattern}" "$@"; then
    echo "[fail] ${label}" >&2
    exit 1
  fi
}

check_absent \
  "company keywords absent from reusable core/sample/example code" \
  "마미박스|베이비매직|barcode_log_error_summary|recording_failure_analysis" \
  boxer/core boxer/context boxer/observability boxer/retrieval boxer_adapter_slack/common.py boxer_adapter_slack/context.py boxer_adapter_slack/sample.py examples

check_absent \
  "company imports absent from reusable core/sample/example code" \
  "boxer_company|boxer_company_adapter_slack|boxer\\.company|adapters\\.company|routers\\.company" \
  boxer/core boxer/context boxer/observability boxer/retrieval boxer_adapter_slack/common.py boxer_adapter_slack/context.py boxer_adapter_slack/sample.py examples

check_absent \
  "sample/example messaging stays domain-neutral" \
  "회사용 기능|company 어댑터" \
  boxer_adapter_slack/sample.py examples

check_absent \
  "company-only scripts absent from public scripts" \
  "boxer_company|boxer_company_adapter_slack|boxer\\.company|adapters\\.company|routers\\.company|마미박스|베이비매직" \
  scripts/smoke_sample_adapter.sh

check_absent \
  "company env absent from core example" \
  "MOMMYBOX|HYUN_USER_ID|MARK_USER_ID|DD_USER_ID|APP_USER_|ANTHROPIC_API_KEY_HUMANSCAPE" \
  boxer/core .env.example

check_absent \
  "korea-specific defaults absent from open core" \
  "Answer in Korean|Answer briefly in Korean|default=\"Asia/Seoul\"|or \"Asia/Seoul\"" \
  boxer/core boxer/context boxer/observability boxer/retrieval

echo "[check] compileall"
"${PYTHON_BIN}" -m compileall boxer boxer_adapter_slack >/dev/null

echo "[ok] open core boundary verified"
