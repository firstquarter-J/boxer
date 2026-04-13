#!/usr/bin/env bash
set -euo pipefail

INSTANCE_ID="${DEPLOY_INSTANCE_ID:-i-0cb53572cdab066fa}"
REGION="${DEPLOY_REGION:-ap-northeast-2}"
REMOTE_DIR="${DEPLOY_REMOTE_DIR:-/home/ec2-user/rag-bot}"
APP_USER="${DEPLOY_APP_USER:-ec2-user}"
BRANCH="${DEPLOY_BRANCH:-main}"
SERVICE_NAME="${DEPLOY_SERVICE_NAME:-boxer}"
SYNC_ENV="${DEPLOY_SYNC_ENV:-false}"
LOCAL_ENV_PATH="${DEPLOY_LOCAL_ENV_PATH:-.env}"
REMOTE_SCRIPT_PATH="${DEPLOY_REMOTE_SCRIPT_PATH:-/tmp/boxer-deploy.sh}"
AWS_SYNC_BLOCK_PATTERN='^[[:space:]]*AWS_[A-Z0-9_]+='

if ! command -v aws >/dev/null 2>&1; then
  echo "aws cli가 필요해" >&2
  exit 1
fi

LOCAL_ENV_B64=""
LOCAL_ENV_FILTERED_FILE=""
FILTERED_AWS_KEYS=""
if [[ "${SYNC_ENV}" == "true" ]]; then
  if [[ ! -f "${LOCAL_ENV_PATH}" ]]; then
    echo "Local env file not found: ${LOCAL_ENV_PATH}" >&2
    exit 1
  fi
  LOCAL_ENV_FILTERED_FILE="$(mktemp)"
  awk "!/${AWS_SYNC_BLOCK_PATTERN}/" "${LOCAL_ENV_PATH}" > "${LOCAL_ENV_FILTERED_FILE}"
  FILTERED_AWS_KEYS="$(
    grep -E "${AWS_SYNC_BLOCK_PATTERN}" "${LOCAL_ENV_PATH}" | cut -d= -f1 | sort -u | paste -sd',' - || true
  )"
  LOCAL_ENV_B64="$(base64 < "${LOCAL_ENV_FILTERED_FILE}" | tr -d '\n')"
fi

REMOTE_SCRIPT_FILE="$(mktemp)"
cleanup() {
  rm -f "${REMOTE_SCRIPT_FILE}" "${LOCAL_ENV_FILTERED_FILE}"
}
trap cleanup EXIT

cat > "${REMOTE_SCRIPT_FILE}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

if [[ "${SYNC_ENV}" == "true" ]]; then
  if [[ -f "${REMOTE_DIR}/.env" ]]; then
    cp "${REMOTE_DIR}/.env" "${REMOTE_DIR}/.env.bak-\$(date +%Y%m%d-%H%M%S)"
  fi
  printf '%s' '${LOCAL_ENV_B64}' | base64 --decode > "${REMOTE_DIR}/.env"
  chmod 600 "${REMOTE_DIR}/.env"
fi

sudo -u ${APP_USER} -H bash -lc 'cd ${REMOTE_DIR} && git pull --ff-only origin ${BRANCH} && ${REMOTE_DIR}/.venv/bin/pip install -e . && ${REMOTE_DIR}/.venv/bin/pip install -e ./boxer_adapter_slack && ${REMOTE_DIR}/.venv/bin/pip install -e ./boxer_company && ${REMOTE_DIR}/.venv/bin/pip install -e ./boxer_company_adapter_slack'
sudo systemctl restart ${SERVICE_NAME}
sudo systemctl is-active ${SERVICE_NAME}
sudo journalctl -u ${SERVICE_NAME} -n 20 --no-pager -o short-iso
EOF

REMOTE_SCRIPT_B64="$(base64 < "${REMOTE_SCRIPT_FILE}" | tr -d '\n')"
SSM_COMMAND="printf '%s' '${REMOTE_SCRIPT_B64}' | base64 --decode > '${REMOTE_SCRIPT_PATH}' && chmod +x '${REMOTE_SCRIPT_PATH}' && '${REMOTE_SCRIPT_PATH}' && rm -f '${REMOTE_SCRIPT_PATH}'"

echo "[deploy] instance=${INSTANCE_ID} region=${REGION}"
echo "[deploy] remote_dir=${REMOTE_DIR} branch=${BRANCH} service=${SERVICE_NAME}"
echo "[deploy] install=editable-company-runtime sync_env=${SYNC_ENV}"
if [[ "${SYNC_ENV}" == "true" ]]; then
  echo "[deploy] sync_env excludes AWS_* keys"
  if [[ -n "${FILTERED_AWS_KEYS}" ]]; then
    echo "[deploy] filtered_aws_keys=${FILTERED_AWS_KEYS}"
  fi
fi

COMMAND_ID="$(
  aws ssm send-command \
    --instance-ids "${INSTANCE_ID}" \
    --region "${REGION}" \
    --document-name "AWS-RunShellScript" \
    --comment "boxer deploy" \
    --parameters "commands=[\"${SSM_COMMAND}\"]" \
    --query "Command.CommandId" \
    --output text
)"

echo "[deploy] command_id=${COMMAND_ID}"

if ! aws ssm wait command-executed \
  --command-id "${COMMAND_ID}" \
  --instance-id "${INSTANCE_ID}" \
  --region "${REGION}"; then
  echo "[deploy] wait failed. invocation output를 확인할게" >&2
fi

STATUS="$(
  aws ssm get-command-invocation \
    --command-id "${COMMAND_ID}" \
    --instance-id "${INSTANCE_ID}" \
    --region "${REGION}" \
    --query "Status" \
    --output text
)"
STDOUT_CONTENT="$(
  aws ssm get-command-invocation \
    --command-id "${COMMAND_ID}" \
    --instance-id "${INSTANCE_ID}" \
    --region "${REGION}" \
    --query "StandardOutputContent" \
    --output text
)"
STDERR_CONTENT="$(
  aws ssm get-command-invocation \
    --command-id "${COMMAND_ID}" \
    --instance-id "${INSTANCE_ID}" \
    --region "${REGION}" \
    --query "StandardErrorContent" \
    --output text
)"

if [[ -n "${STDOUT_CONTENT}" && "${STDOUT_CONTENT}" != "None" ]]; then
  printf '%s\n' "${STDOUT_CONTENT}"
fi

if [[ -n "${STDERR_CONTENT}" && "${STDERR_CONTENT}" != "None" ]]; then
  printf '%s\n' "${STDERR_CONTENT}" >&2
fi

echo "[deploy] status=${STATUS}"

if [[ "${STATUS}" != "Success" ]]; then
  exit 1
fi
