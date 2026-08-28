#!/usr/bin/env bash

set -Eeuo pipefail

APP_LINK=/home/mommytalk/mommybox-v2
APP_NAME=mommybox-v2
MODE=check
MODE_REQUESTED=
CONFIRM_IDLE=0
EXPECTED_DEVICE=
MIN_FREE_KB=262144
IDLE_CONFIRM_FILE=
IDLE_CONFIRM_TOKEN=
IDLE_CONFIRM_TIMEOUT_SEC=90

BUILD_DIR=
BACKUP_DIR=
REPLACEMENT_TMP=
REPLACED=0
APP_STOPPED=0
TARGET_MODE=
PM2_COMMAND=()
LAST_PM2_STATUS=
LAST_PM2_PID=
VERIFIED_PID=
EXPECTED_PM2_PID=

log() {
  printf '[node-hid-abi-repair] %s\n' "$*"
}

fail() {
  log "ERROR: $*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
사용법:
  repair-node-hid-abi.sh --check --expected-device MB2-A00037
  repair-node-hid-abi.sh --apply --confirm-idle --expected-device MB2-A00037
  repair-node-hid-abi.sh --apply --confirm-idle --expected-device MB2-A00037 \
    --idle-confirm-file /home/mommytalk/.node-hid-abi-idle.<32자리 hex> \
    --idle-confirm-token <64자리 hex>

옵션:
  --check         현재 node-hid ABI만 진단합니다. 기본값입니다.
  --apply         호환 바이너리를 빌드해 현재 장비에만 적용합니다.
  --confirm-idle  MDA에서 녹화/업로드 없음과 NOSESS를 확인했음을 명시합니다.
  --idle-confirm-file <경로>
                  자동화가 실제 PM2 정지 직전에 생성할 일회성 승인 파일입니다.
  --idle-confirm-token <토큰>
                  승인 파일 내용과 일치해야 하는 64자리 hex 토큰입니다.
  --expected-device <장비명>
                  SSH 포트 오입력을 막기 위해 boxname.txt와 일치할 장비명을 지정합니다.
  -h, --help      도움말을 표시합니다.

종료 코드:
  0  조치 불필요 또는 복구 성공
  1  진단/복구 실패
  2  ABI 복구 필요(--check)
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --check)
      [ -z "$MODE_REQUESTED" ] || [ "$MODE_REQUESTED" = check ] || fail "--check와 --apply는 함께 사용할 수 없습니다."
      MODE=check
      MODE_REQUESTED=check
      ;;
    --apply)
      [ -z "$MODE_REQUESTED" ] || [ "$MODE_REQUESTED" = apply ] || fail "--check와 --apply는 함께 사용할 수 없습니다."
      MODE=apply
      MODE_REQUESTED=apply
      ;;
    --confirm-idle)
      CONFIRM_IDLE=1
      ;;
    --idle-confirm-file)
      [ "$#" -ge 2 ] || fail "--idle-confirm-file 뒤에 경로가 필요합니다."
      IDLE_CONFIRM_FILE=$2
      shift
      ;;
    --idle-confirm-token)
      [ "$#" -ge 2 ] || fail "--idle-confirm-token 뒤에 토큰이 필요합니다."
      IDLE_CONFIRM_TOKEN=$2
      shift
      ;;
    --expected-device)
      [ "$#" -ge 2 ] || fail "--expected-device 뒤에 장비명이 필요합니다."
      EXPECTED_DEVICE=$2
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      fail "지원하지 않는 옵션입니다: $1"
      ;;
  esac
  shift
done

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "필수 명령을 찾을 수 없습니다: $1"
}

sha256() {
  sha256sum "$1" | awk '{print $1}'
}

abi_max() {
  local strings_output
  local abi

  strings_output=$(strings "$1") || return 1
  abi=$(
    printf '%s\n' "$strings_output" |
      grep -oE 'GLIBCXX_[0-9]+\.[0-9]+(\.[0-9]+)?' |
      sort -Vu |
      tail -1 || true
  )
  printf '%s' "${abi:-none}"
}

resolve_pm2() {
  local global_root
  local candidate

  if command -v pm2 >/dev/null 2>&1; then
    PM2_COMMAND=(pm2)
    return
  fi

  for candidate in \
    /home/mommytalk/.npm-global/lib/node_modules/pm2/bin/pm2 \
    /usr/local/lib/node_modules/pm2/bin/pm2 \
    /usr/lib/node_modules/pm2/bin/pm2; do
    if [ -f "$candidate" ]; then
      PM2_COMMAND=(node "$candidate")
      return
    fi
  done

  global_root=$(timeout --kill-after=5s 20s npm root -g 2>/dev/null || true)
  candidate="$global_root/pm2/bin/pm2"
  if [ -n "$global_root" ] && [ -f "$candidate" ]; then
    PM2_COMMAND=(node "$candidate")
    return
  fi

  fail "PM2 실행 파일을 찾을 수 없습니다."
}

resolve_node_gyp() {
  local candidate
  local npm_cli
  local npm_root

  candidate=$(readlink -f "$APP_DIR/node_modules/.bin/node-gyp" 2>/dev/null || true)
  case "$candidate" in
    "$APP_DIR"/node_modules/node-gyp/*)
      if [ -f "$candidate" ]; then
        NODE_GYP=$candidate
        return
      fi
      ;;
  esac

  if command -v npm >/dev/null 2>&1; then
    npm_cli=$(readlink -f "$(command -v npm)" 2>/dev/null || true)
    npm_root=$(dirname "$(dirname "$npm_cli")")
    candidate="$npm_root/node_modules/node-gyp/bin/node-gyp.js"
    case "$candidate" in
      "$npm_root"/node_modules/node-gyp/*)
        if [ -f "$candidate" ]; then
          NODE_GYP=$candidate
          return
        fi
        ;;
    esac
  fi

  fail "현재 앱 또는 npm에 포함된 node-gyp를 찾을 수 없습니다."
}

run_pm2() {
  "${PM2_COMMAND[@]}" "$@"
}

run_pm2_with_timeout() {
  local duration=$1
  shift
  timeout --kill-after=5s "$duration" "${PM2_COMMAND[@]}" "$@"
}

pm2_value() {
  local field=$1
  local duration=${2:-10s}

  run_pm2_with_timeout "$duration" jlist | node -e '
    const fs = require("fs")
    const appName = process.argv[1]
    const field = process.argv[2]
    const list = JSON.parse(fs.readFileSync(0, "utf8"))
    const processInfo = list.find((item) => item.name === appName)
    if (!processInfo) process.exit(3)

    const values = {
      status: processInfo.pm2_env && processInfo.pm2_env.status,
      errorLog: processInfo.pm2_env && processInfo.pm2_env.pm_err_log_path,
      outputLog: processInfo.pm2_env && processInfo.pm2_env.pm_out_log_path,
      cwd: processInfo.pm2_env && processInfo.pm2_env.pm_cwd,
      scriptPath: processInfo.pm2_env && processInfo.pm2_env.pm_exec_path
    }
    const value = values[field]
    if (value === undefined || value === null) process.exit(4)
    process.stdout.write(String(value))
  ' "$APP_NAME" "$field"
}

get_pm2_pid() {
  local duration=${1:-10s}

  run_pm2_with_timeout "$duration" pid "$APP_NAME" 2>/dev/null |
    tail -1 |
    tr -d '[:space:]'
}

wait_for_pm2_online() {
  local max_seconds=$1
  local deadline=$((SECONDS + max_seconds))

  LAST_PM2_STATUS=
  LAST_PM2_PID=

  while [ "$SECONDS" -lt "$deadline" ]; do
    LAST_PM2_STATUS=$(pm2_value status 3s 2>/dev/null || true)
    LAST_PM2_PID=$(get_pm2_pid 3s || true)
    case "$LAST_PM2_PID" in
      ''|0|*[!0-9]*) ;;
      *)
        if [ "$LAST_PM2_STATUS" = online ] && [ -d "/proc/$LAST_PM2_PID" ]; then
          return 0
        fi
        ;;
    esac
    sleep 1
  done

  return 1
}

validate_hid_module() {
  local module_path=$1
  local open_scanner=$2

  timeout --kill-after=5s 20s env NODE_PATH="$APP_DIR/node_modules" \
    node - "$module_path" "$USB_LIST" "$open_scanner" <<'NODE'
const modulePath = process.argv[2]
const usbListPath = process.argv[3]
const shouldOpenScanner = process.argv[4] === 'open-scanner'

const HID = require(modulePath)
HID.setDriverType('hidraw')

const devices = HID.devices()
console.log(`HID_DEVICE_COUNT ${devices.length}`)

if (!shouldOpenScanner) process.exit(0)

const usbList = require(usbListPath)
const scannerIds = new Set(
  usbList
    .filter((item) => item.type === 'HID' && /^scanner/.test(item.name))
    .map((item) => `${Number(item.vid)}:${Number(item.pid)}`)
)
const scannerPaths = [...new Set(
  devices
    .filter((device) => scannerIds.has(`${Number(device.vendorId)}:${Number(device.productId)}`))
    .map((device) => device.path)
    .filter(Boolean)
)]

console.log(`CONFIGURED_SCANNER_PATHS ${scannerPaths.length}`)
for (const scannerPath of scannerPaths) {
  console.log(`CONFIGURED_SCANNER_PATH ${scannerPath}`)
}
if (scannerPaths.length === 0) {
  console.error('연결된 등록 스캐너 HID 경로가 없습니다.')
  process.exit(41)
}

let scanner
try {
  scanner = new HID.HID(scannerPaths[0])
} finally {
  if (scanner) scanner.close()
}
console.log('HID_OPEN_CLOSE_OK')
NODE
}

scanner_fd_count() {
  local pid=$1
  local scanner_paths=$2
  local count=0
  local fd
  local scanner_path
  local target

  for fd in "/proc/$pid/fd/"*; do
    target=$(readlink "$fd" 2>/dev/null || true)
    while IFS= read -r scanner_path; do
      if [ -n "$scanner_path" ] && [ "$target" = "$scanner_path" ]; then
        count=$((count + 1))
        break
      fi
    done <<< "$scanner_paths"
  done

  printf '%s' "$count"
}

count_new_abi_errors() {
  local log_path=$1
  local before_inode=$2
  local before_lines=$3
  local current_inode
  local current_lines
  local start_line=1

  if [ ! -f "$log_path" ]; then
    printf '0'
    return
  fi

  current_inode=$(stat -c %i "$log_path")
  current_lines=$(wc -l < "$log_path")
  if [ "$current_inode" = "$before_inode" ] && [ "$current_lines" -ge "$before_lines" ]; then
    start_line=$((before_lines + 1))
  fi

  tail -n +"$start_line" "$log_path" |
    grep -Ec 'GLIBCXX_[0-9.]+.*not found' || true
}

remove_build_dir() {
  if [ -z "$BUILD_DIR" ]; then
    return
  fi

  case "$BUILD_DIR" in
    /home/mommytalk/node-hid-abi-build.*)
      if [ -d "$BUILD_DIR" ] && [ ! -L "$BUILD_DIR" ]; then
        rm -rf -- "$BUILD_DIR"
      fi
      ;;
    *)
      log "예상하지 못한 빌드 경로라 자동 정리하지 않습니다: $BUILD_DIR" >&2
      ;;
  esac
}

remove_idle_confirmation_file() {
  [ -n "$IDLE_CONFIRM_FILE" ] || return 0

  case "$IDLE_CONFIRM_FILE" in
    /home/mommytalk/.node-hid-abi-idle.*)
      if [ -L "$IDLE_CONFIRM_FILE" ]; then
        log "WARN: 유휴 승인 경로가 심볼릭 링크라 자동 삭제하지 않습니다: $IDLE_CONFIRM_FILE" >&2
        return 0
      fi
      if [ -e "$IDLE_CONFIRM_FILE" ]; then
        if [ ! -f "$IDLE_CONFIRM_FILE" ] || [ "$(stat -c %u "$IDLE_CONFIRM_FILE")" != "$(id -u)" ]; then
          log "WARN: 소유하지 않은 유휴 승인 경로라 자동 삭제하지 않습니다: $IDLE_CONFIRM_FILE" >&2
          return 0
        fi
        rm -f -- "$IDLE_CONFIRM_FILE"
      fi
      ;;
  esac
}

wait_for_idle_confirmation() {
  local deadline
  local confirmation

  [ -n "$IDLE_CONFIRM_FILE" ] || return 0
  deadline=$((SECONDS + IDLE_CONFIRM_TIMEOUT_SEC))
  log "AWAITING_IDLE_CONFIRMATION"

  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ -e "$IDLE_CONFIRM_FILE" ] || [ -L "$IDLE_CONFIRM_FILE" ]; then
      [ -f "$IDLE_CONFIRM_FILE" ] && [ ! -L "$IDLE_CONFIRM_FILE" ] || fail "유휴 승인 파일 형식이 안전하지 않습니다."
      [ "$(stat -c %u "$IDLE_CONFIRM_FILE")" = "$(id -u)" ] || fail "유휴 승인 파일 소유자가 현재 사용자와 다릅니다."
      [ "$(stat -c %g "$IDLE_CONFIRM_FILE")" = "$(id -g)" ] || fail "유휴 승인 파일 그룹이 현재 사용자와 다릅니다."
      [ "$(stat -c %a "$IDLE_CONFIRM_FILE")" = 600 ] || fail "유휴 승인 파일 권한은 600이어야 합니다."
      confirmation=$(tr -d '\r\n' < "$IDLE_CONFIRM_FILE")
      [ "$confirmation" = "$IDLE_CONFIRM_TOKEN" ] || fail "유휴 승인 토큰이 일치하지 않습니다."
      rm -f -- "$IDLE_CONFIRM_FILE"
      IDLE_CONFIRM_FILE=
      log "IDLE_CONFIRMATION_ACCEPTED"
      return 0
    fi
    sleep 1
  done

  fail "최종 유휴 승인을 기다리는 시간이 초과됐습니다."
}

restore_original() {
  local restore_tmp

  [ "$REPLACED" -eq 1 ] || return 0
  if [ -z "$BACKUP_DIR" ] || [ ! -f "$BACKUP_DIR/HID_hidraw.node" ]; then
    log "CRITICAL: 롤백 백업을 찾을 수 없습니다." >&2
    return 1
  fi

  log "검증 실패로 원본 바이너리를 복원합니다."
  APP_STOPPED=1
  if ! run_pm2_with_timeout 30s stop "$APP_NAME" >/dev/null 2>&1; then
    log "WARN: 롤백 전 PM2 stop 결과가 비정상입니다. 원자적 복원은 계속 시도합니다." >&2
  fi

  restore_tmp=$(mktemp "$RELEASE_DIR/.HID_hidraw.node.restore.XXXXXX") || {
    log "CRITICAL: 롤백 임시 파일을 만들지 못했습니다." >&2
    return 1
  }
  if ! install -m "$TARGET_MODE" "$BACKUP_DIR/HID_hidraw.node" "$restore_tmp"; then
    rm -f -- "$restore_tmp"
    log "CRITICAL: 백업 바이너리를 롤백 임시 파일에 복사하지 못했습니다." >&2
    return 1
  fi
  if ! mv -f "$restore_tmp" "$TARGET"; then
    rm -f -- "$restore_tmp"
    log "CRITICAL: 원본 바이너리를 대상 경로에 복원하지 못했습니다." >&2
    return 1
  fi
  if [ "$(sha256 "$TARGET")" != "$CURRENT_SHA" ]; then
    log "CRITICAL: 복원된 바이너리 해시가 원본과 다릅니다." >&2
    return 1
  fi

  REPLACED=0
  if ! run_pm2_with_timeout 30s restart "$APP_NAME" >/dev/null 2>&1; then
    log "CRITICAL: 원본 복원 후 PM2 restart가 실패했습니다." >&2
    return 1
  fi
  if ! wait_for_pm2_online 20; then
    log "CRITICAL: 원본 복원 후 앱이 online으로 돌아오지 않았습니다." >&2
    return 1
  fi
  APP_STOPPED=0
  log "원본 복원 및 앱 재시작 완료: $(sha256 "$TARGET")"
  return 0
}

restart_original_process() {
  if ! run_pm2_with_timeout 30s restart "$APP_NAME" >/dev/null 2>&1; then
    return 1
  fi
  if ! wait_for_pm2_online 20; then
    return 1
  fi
  APP_STOPPED=0
  return 0
}

handle_signal() {
  local signal_name=$1

  log "신호 $signal_name 감지: 안전 정리와 필요 시 롤백을 시작합니다." >&2
  exit 1
}

cleanup() {
  local rc=$?
  local recovery_failed=0

  trap - EXIT
  trap '' HUP INT TERM
  set +e

  if [ "$rc" -ne 0 ]; then
    restore_original || true
    if [ "$APP_STOPPED" -eq 1 ]; then
      restart_original_process || true
    fi
    if [ -n "$REPLACEMENT_TMP" ] && [ -f "$REPLACEMENT_TMP" ]; then
      rm -f -- "$REPLACEMENT_TMP"
    fi
  fi

  remove_idle_confirmation_file
  remove_build_dir
  if [ "$REPLACED" -eq 1 ] || [ "$APP_STOPPED" -eq 1 ]; then
    recovery_failed=1
  fi
  if [ "$recovery_failed" -eq 1 ]; then
    log "CRITICAL: 자동 복구가 완전히 끝나지 않았습니다. 장비 상태를 즉시 수동 확인하세요." >&2
  fi
  exit "$rc"
}

trap cleanup EXIT
trap 'handle_signal HUP' HUP
trap 'handle_signal INT' INT
trap 'handle_signal TERM' TERM

require_command node
require_command readlink
require_command sha256sum
require_command strings
require_command grep
require_command sort
require_command tail
require_command timeout
require_command awk
require_command stat
require_command id
require_command env
require_command head
require_command tr

[ -n "$EXPECTED_DEVICE" ] || fail "--expected-device로 접속 대상 장비명을 지정하세요."
[[ "$EXPECTED_DEVICE" =~ ^MB2-[A-Z0-9]+$ ]] || fail "장비명 형식이 올바르지 않습니다: $EXPECTED_DEVICE"
if [ -n "$IDLE_CONFIRM_FILE" ] || [ -n "$IDLE_CONFIRM_TOKEN" ]; then
  [ -n "$IDLE_CONFIRM_FILE" ] && [ -n "$IDLE_CONFIRM_TOKEN" ] || fail "유휴 승인 파일과 토큰은 함께 지정해야 합니다."
  [[ "$IDLE_CONFIRM_FILE" =~ ^/home/mommytalk/\.node-hid-abi-idle\.[a-f0-9]{32}$ ]] || fail "유휴 승인 파일 경로 형식이 올바르지 않습니다."
  [[ "$IDLE_CONFIRM_TOKEN" =~ ^[a-f0-9]{64}$ ]] || fail "유휴 승인 토큰 형식이 올바르지 않습니다."
fi
BOXNAME_FILE=/home/mommytalk/boxname.txt
[ -f "$BOXNAME_FILE" ] || fail "장비명 파일이 없습니다: $BOXNAME_FILE"
DEVICE_NAME=$(tr -d '\r\n' < "$BOXNAME_FILE")
[ "$DEVICE_NAME" = "$EXPECTED_DEVICE" ] || fail "접속 장비가 예상과 다릅니다: expected=$EXPECTED_DEVICE actual=$DEVICE_NAME"

APP_DIR=$(readlink -f "$APP_LINK" 2>/dev/null || true)
[ -n "$APP_DIR" ] || fail "현재 앱 링크를 확인할 수 없습니다: $APP_LINK"
case "$APP_DIR" in
  /home/mommytalk/mommybox-v2-*) ;;
  *) fail "예상하지 못한 앱 경로입니다: $APP_DIR" ;;
esac

PACKAGE_JSON="$APP_DIR/package.json"
NODE_HID_DIR="$APP_DIR/node_modules/node-hid"
NODE_HID_PACKAGE="$NODE_HID_DIR/package.json"
USB_LIST="$APP_DIR/constants/usb_list.json"
RELEASE_DIR="$NODE_HID_DIR/build/Release"
TARGET="$RELEASE_DIR/HID_hidraw.node"

[ -f "$PACKAGE_JSON" ] || fail "package.json이 없습니다: $PACKAGE_JSON"
[ -f "$NODE_HID_PACKAGE" ] || fail "node-hid package.json이 없습니다: $NODE_HID_PACKAGE"
[ -f "$USB_LIST" ] || fail "USB 장치 목록이 없습니다: $USB_LIST"
[ -f "$TARGET" ] || fail "HID_hidraw.node이 없습니다: $TARGET"
[ ! -L "$TARGET" ] || fail "HID_hidraw.node이 심볼릭 링크입니다. 자동 조치를 중단합니다."
[ "$(stat -c %u "$TARGET")" = "$(id -u)" ] || fail "현재 사용자 소유가 아닌 바이너리는 교체하지 않습니다."
[ "$(stat -c %g "$TARGET")" = "$(id -g)" ] || fail "현재 사용자 기본 그룹 소유가 아닌 바이너리는 교체하지 않습니다."

APP_VERSION=$(node -e 'process.stdout.write(require(process.argv[1]).version)' "$PACKAGE_JSON")
NODE_HID_VERSION=$(node -e 'process.stdout.write(require(process.argv[1]).version)' "$NODE_HID_PACKAGE")
[[ "$NODE_HID_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "지원하지 않는 node-hid 버전 형식입니다: $NODE_HID_VERSION"
CURRENT_SHA=$(sha256 "$TARGET")
CURRENT_ABI=$(abi_max "$TARGET")

log "장비=$DEVICE_NAME 앱=$APP_VERSION node-hid=$NODE_HID_VERSION node=$(node -p 'process.version') arch=$(node -p 'process.arch')"
log "현재 바이너리 sha256=$CURRENT_SHA abi=$CURRENT_ABI"

set +e
CURRENT_LOAD_OUTPUT=$(validate_hid_module "$NODE_HID_DIR" no-open 2>&1)
CURRENT_LOAD_RC=$?
set -e

if [ "$CURRENT_LOAD_RC" -eq 0 ]; then
  log "NO_ACTION_REQUIRED: 현재 node-hid가 정상 로드됩니다."
  exit 0
fi

MISSING_ABI=$(printf '%s\n' "$CURRENT_LOAD_OUTPUT" | grep -oE 'GLIBCXX_[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1 || true)
if [ -z "$MISSING_ABI" ] || ! printf '%s\n' "$CURRENT_LOAD_OUTPUT" | grep 'not found' >/dev/null; then
  fail "GLIBCXX ABI 장애가 아닌 node-hid 오류입니다. 자동 조치하지 않습니다."
fi

log "REPAIR_REQUIRED: 시스템에 없는 ABI=$MISSING_ABI"
if [ "$MODE" = check ]; then
  trap - EXIT
  exit 2
fi

[ "$CONFIRM_IDLE" -eq 1 ] || fail "적용 전 MDA에서 녹화=false, 업로드=false, NOSESS를 확인하고 --confirm-idle을 지정하세요."

for command_name in make gcc g++ ar pkg-config ldd file mktemp install mv cp awk df wc sleep stat id tr seq rm timeout env flock mkdir dirname; do
  require_command "$command_name"
done
if command -v python3 >/dev/null 2>&1; then
  PYTHON_COMMAND=$(command -v python3)
elif command -v python >/dev/null 2>&1; then
  PYTHON_COMMAND=$(command -v python)
else
  fail "node-gyp에 사용할 python 또는 python3를 찾을 수 없습니다."
fi
pkg-config --exists libudev || fail "libudev 개발 파일이 없습니다."
pkg-config --exists libusb-1.0 || fail "libusb-1.0 개발 파일이 없습니다."

FREE_KB=$(df -Pk /home/mommytalk | awk 'NR == 2 {print $4}')
case "$FREE_KB" in
  ''|*[!0-9]*) fail "사용 가능한 디스크 공간을 확인하지 못했습니다." ;;
esac
[ "$FREE_KB" -ge "$MIN_FREE_KB" ] || fail "빌드 공간이 부족합니다: ${FREE_KB}KB"

resolve_pm2

LOCK_FILE=/home/mommytalk/.node-hid-abi-repair.lock
[ ! -L "$LOCK_FILE" ] || fail "잠금 파일이 심볼릭 링크입니다: $LOCK_FILE"
exec 9>>"$LOCK_FILE"
flock -n 9 || fail "다른 node-hid 복구 작업이 실행 중입니다."
[ "$(stat -c %u "$LOCK_FILE")" = "$(id -u)" ] || fail "현재 사용자 소유가 아닌 잠금 파일은 사용하지 않습니다."

verify_release_files() {
  local expected_sha=$1
  local link_now
  local app_version_now
  local node_hid_version_now

  link_now=$(readlink -f "$APP_LINK" 2>/dev/null || true)
  [ "$link_now" = "$APP_DIR" ] || fail "현재 앱 링크가 작업 대상에서 변경됐습니다: $link_now"
  app_version_now=$(node -e 'process.stdout.write(require(process.argv[1]).version)' "$PACKAGE_JSON")
  node_hid_version_now=$(node -e 'process.stdout.write(require(process.argv[1]).version)' "$NODE_HID_PACKAGE")
  [ "$app_version_now" = "$APP_VERSION" ] || fail "작업 중 앱 버전이 변경됐습니다: $app_version_now"
  [ "$node_hid_version_now" = "$NODE_HID_VERSION" ] || fail "작업 중 node-hid 버전이 변경됐습니다: $node_hid_version_now"
  [ "$(sha256 "$TARGET")" = "$expected_sha" ] || fail "작업 중 대상 바이너리가 변경됐습니다."
}

verify_running_app_identity() {
  local pm2_cwd
  local pm2_script
  local process_cwd

  verify_release_files "$CURRENT_SHA"
  [ "$(pm2_value status)" = online ] || fail "mommybox-v2가 online 상태가 아닙니다."

  VERIFIED_PID=$(get_pm2_pid)
  case "$VERIFIED_PID" in
    ''|0|*[!0-9]*) fail "mommybox-v2 PID를 확인할 수 없습니다." ;;
  esac
  if [ -z "$EXPECTED_PM2_PID" ]; then
    EXPECTED_PM2_PID=$VERIFIED_PID
  else
    [ "$VERIFIED_PID" = "$EXPECTED_PM2_PID" ] || fail "빌드 중 mommybox-v2 프로세스가 재시작됐습니다."
  fi
  pm2_cwd=$(readlink -f "$(pm2_value cwd)" 2>/dev/null || true)
  pm2_script=$(readlink -f "$(pm2_value scriptPath)" 2>/dev/null || true)
  process_cwd=$(readlink -f "/proc/$VERIFIED_PID/cwd" 2>/dev/null || true)
  [ "$pm2_cwd" = "$APP_DIR" ] || fail "PM2 cwd와 현재 앱 링크 대상이 다릅니다: $pm2_cwd"
  [ "$pm2_script" = "$APP_DIR/app.js" ] || fail "PM2 script path와 현재 앱 링크 대상이 다릅니다: $pm2_script"
  [ "$process_cwd" = "$APP_DIR" ] || fail "실행 중 프로세스 cwd와 현재 앱 링크 대상이 다릅니다: $process_cwd"
}

verify_running_app_identity

NODE_VERSION=$(node -p 'process.versions.node')
NODE_HEADERS_DIR="/home/mommytalk/.cache/node-gyp/$NODE_VERSION"
resolve_node_gyp
NODE_GYP_ROOT=$(dirname "$(dirname "$NODE_GYP")")
NODE_GYP_VERSION=$(node -e 'process.stdout.write(require(process.argv[1]).version)' "$NODE_GYP_ROOT/package.json")
[ -f "$NODE_HEADERS_DIR/include/node/node.h" ] || fail "현재 Node 헤더 캐시가 없습니다: $NODE_HEADERS_DIR"
log "node-gyp=$NODE_GYP_VERSION python=$PYTHON_COMMAND headers=$NODE_HEADERS_DIR"

BUILD_DIR=$(mktemp -d /home/mommytalk/node-hid-abi-build.XXXXXX)
CANDIDATE_DIR="$BUILD_DIR/node-hid"
mkdir "$CANDIDATE_DIR"
cp -a "$NODE_HID_DIR/." "$CANDIDATE_DIR/"
log "설치된 node-hid@$NODE_HID_VERSION 소스를 별도 경로에서 오프라인 재빌드합니다."
(
  cd "$CANDIDATE_DIR"
  timeout --kill-after=30s 600s env \
    NODE_PATH="$APP_DIR/node_modules" \
    PYTHON="$PYTHON_COMMAND" \
    npm_config_python="$PYTHON_COMMAND" \
    node "$NODE_GYP" rebuild --nodedir="$NODE_HEADERS_DIR"
)

CANDIDATE="$CANDIDATE_DIR/build/Release/HID_hidraw.node"
[ -f "$CANDIDATE" ] || fail "빌드 결과에 HID_hidraw.node이 없습니다."
file "$CANDIDATE" | grep 'ELF.*shared object' >/dev/null || fail "빌드 결과가 Linux ELF shared object가 아닙니다."
set +e
LDD_OUTPUT=$(ldd "$CANDIDATE" 2>&1)
LDD_RC=$?
set -e
[ "$LDD_RC" -eq 0 ] || fail "빌드 결과의 ldd 검사가 실패했습니다: $LDD_OUTPUT"
if printf '%s\n' "$LDD_OUTPUT" | grep 'not found' >/dev/null; then
  fail "빌드 결과의 동적 라이브러리 의존성을 해소하지 못했습니다."
fi

CANDIDATE_ABI=$(abi_max "$CANDIDATE")
CANDIDATE_SHA=$(sha256 "$CANDIDATE")
log "빌드 결과 sha256=$CANDIDATE_SHA abi=$CANDIDATE_ABI"
CANDIDATE_VALIDATION=$(validate_hid_module "$CANDIDATE_DIR" open-scanner)
printf '%s\n' "$CANDIDATE_VALIDATION"
SCANNER_PATHS=$(printf '%s\n' "$CANDIDATE_VALIDATION" | awk '$1 == "CONFIGURED_SCANNER_PATH" { print $2 }')
[ -n "$SCANNER_PATHS" ] || fail "후보 모듈에서 등록 스캐너 경로를 확인하지 못했습니다."
while IFS= read -r scanner_path; do
  [[ "$scanner_path" =~ ^/dev/hidraw[0-9]+$ ]] || fail "예상하지 못한 스캐너 경로입니다: $scanner_path"
done <<< "$SCANNER_PATHS"

# 빌드 중 업데이트나 다른 PM2 재시작이 있었다면 현재 릴리스를 건드리지 않습니다.
verify_running_app_identity

TARGET_MODE=$(stat -c %a "$TARGET")
BACKUP_DIR=$(mktemp -d /home/mommytalk/node-hid-abi-backup.XXXXXX)
cp -p "$TARGET" "$BACKUP_DIR/HID_hidraw.node"
{
  printf 'app_version=%s\n' "$APP_VERSION"
  printf 'node_hid_version=%s\n' "$NODE_HID_VERSION"
  printf 'original_sha256=%s\n' "$CURRENT_SHA"
  printf 'replacement_sha256=%s\n' "$CANDIDATE_SHA"
} > "$BACKUP_DIR/metadata.txt"
[ "$(sha256 "$BACKUP_DIR/HID_hidraw.node")" = "$CURRENT_SHA" ] || fail "원본 백업 해시가 일치하지 않습니다."
log "원본 백업 완료: $BACKUP_DIR"

ERROR_LOG=$(pm2_value errorLog)
OUTPUT_LOG=$(pm2_value outputLog)
ERROR_LINES=0
OUTPUT_LINES=0
ERROR_INODE=missing
OUTPUT_INODE=missing

# 승인 후 긴 PM2 조회 구간이 남지 않도록 링크·프로세스·대상
# 해시는 gate를 열기 직전에 확정한다. Boxer는 긴 빌드가 끝난
# 이 지점에서 MDA 상태를 다시 확인한 뒤에만 일회성 승인
# 파일을 생성한다. 승인 없이는 실행 중인 앱을 멈추지 않는다.
verify_running_app_identity
wait_for_idle_confirmation
log "PM2_STOPPING"

APP_STOPPED=1
run_pm2_with_timeout 30s stop "$APP_NAME" >/dev/null
STOP_CONFIRMED=0
STOP_DEADLINE=$((SECONDS + 15))
while [ "$SECONDS" -lt "$STOP_DEADLINE" ]; do
  STOP_STATUS=$(pm2_value status 3s 2>/dev/null || true)
  STOP_PID=$(get_pm2_pid 3s || true)
  if [ "$STOP_STATUS" = stopped ] && { [ -z "$STOP_PID" ] || [ "$STOP_PID" = 0 ]; }; then
    STOP_CONFIRMED=1
    break
  fi
  sleep 1
done
[ "$STOP_CONFIRMED" -eq 1 ] || fail "PM2 앱 정지 상태를 확인하지 못했습니다."

# stop 자체가 남긴 로그는 재시작 후 오류로 오인하지 않도록 여기서 기준선을 잡습니다.
if [ -f "$ERROR_LOG" ]; then
  ERROR_LINES=$(wc -l < "$ERROR_LOG")
  ERROR_INODE=$(stat -c %i "$ERROR_LOG")
fi
if [ -f "$OUTPUT_LOG" ]; then
  OUTPUT_LINES=$(wc -l < "$OUTPUT_LOG")
  OUTPUT_INODE=$(stat -c %i "$OUTPUT_LOG")
fi

# updater가 PM2 정지 구간에 릴리스를 바꿨다면 교체 직전에 중단합니다.
verify_release_files "$CURRENT_SHA"

REPLACEMENT_TMP=$(mktemp "$RELEASE_DIR/.HID_hidraw.node.repair.XXXXXX")
install -m "$TARGET_MODE" "$CANDIDATE" "$REPLACEMENT_TMP"
# mv 직후 신호가 와도 cleanup이 원본을 복원하도록 먼저 표시합니다.
REPLACED=1
mv -f "$REPLACEMENT_TMP" "$TARGET"
REPLACEMENT_TMP=
[ "$(sha256 "$TARGET")" = "$CANDIDATE_SHA" ] || fail "교체 후 바이너리 해시가 일치하지 않습니다."
verify_release_files "$CANDIDATE_SHA"

validate_hid_module "$NODE_HID_DIR" open-scanner
run_pm2_with_timeout 30s restart "$APP_NAME" >/dev/null
wait_for_pm2_online 20 || fail "재시작 후 앱이 online 상태가 아닙니다."
NEW_PID=$LAST_PM2_PID
case "$NEW_PID" in
  ''|0|*[!0-9]*) fail "재시작 후 앱 PID가 없습니다." ;;
esac
[ -d "/proc/$NEW_PID" ] || fail "재시작 후 앱 프로세스를 확인할 수 없습니다."
APP_STOPPED=0

SCANNER_FDS=0
for _ in $(seq 1 10); do
  SCANNER_FDS=$(scanner_fd_count "$NEW_PID" "$SCANNER_PATHS")
  [ "$SCANNER_FDS" -ge 1 ] && break
  sleep 1
done
[ "$SCANNER_FDS" -ge 1 ] || fail "재시작한 앱이 등록 스캐너 HID 경로를 열지 못했습니다."

NEW_ABI_ERRORS=$(count_new_abi_errors "$ERROR_LOG" "$ERROR_INODE" "$ERROR_LINES")
NEW_ABI_ERRORS=$((NEW_ABI_ERRORS + $(count_new_abi_errors "$OUTPUT_LOG" "$OUTPUT_INODE" "$OUTPUT_LINES")))
[ "$NEW_ABI_ERRORS" -eq 0 ] || fail "재시작 후 GLIBCXX 오류가 다시 발생했습니다."

FINAL_STATUS=$(pm2_value status 3s 2>/dev/null || true)
FINAL_PID=$(get_pm2_pid 3s || true)
[ "$FINAL_STATUS" = online ] || fail "최종 확인에서 앱이 online 상태가 아닙니다."
[ "$FINAL_PID" = "$NEW_PID" ] && [ -d "/proc/$FINAL_PID" ] || fail "검증 중 앱 프로세스가 다시 시작되거나 종료됐습니다."
verify_release_files "$CANDIDATE_SHA"
FINAL_PM2_CWD=$(readlink -f "$(pm2_value cwd 3s)" 2>/dev/null || true)
FINAL_PM2_SCRIPT=$(readlink -f "$(pm2_value scriptPath 3s)" 2>/dev/null || true)
FINAL_PROCESS_CWD=$(readlink -f "/proc/$FINAL_PID/cwd" 2>/dev/null || true)
[ "$FINAL_PM2_CWD" = "$APP_DIR" ] || fail "최종 PM2 cwd가 작업 대상과 다릅니다: $FINAL_PM2_CWD"
[ "$FINAL_PM2_SCRIPT" = "$APP_DIR/app.js" ] || fail "최종 PM2 script path가 작업 대상과 다릅니다: $FINAL_PM2_SCRIPT"
[ "$FINAL_PROCESS_CWD" = "$APP_DIR" ] || fail "최종 프로세스 cwd가 작업 대상과 다릅니다: $FINAL_PROCESS_CWD"

REPLACED=0
log "REPAIR_SUCCESS: pid=$NEW_PID scanner_fds=$SCANNER_FDS abi=$CANDIDATE_ABI"
log "롤백 백업: $BACKUP_DIR"
