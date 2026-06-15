#!/usr/bin/env bash
set -euo pipefail

OLD_DIR="${OLD_DIR:-/data/bkee/bkmonitorv3/unify-query-old}"
SERVICE_NAME="${SERVICE_NAME:-bk-unify-query-shadow-compare.service}"
GOR_BIN="${GOR_BIN:-/tmp/gor}"
LISTEN_PORT="${LISTEN_PORT:-10206}"
OLD_PORT="${OLD_PORT:-10216}"
NEW_MODE="${NEW_MODE:-replay}"
SAMPLE_RATE="${SAMPLE_RATE:-0.1}"
MAX_INFLIGHT="${MAX_INFLIGHT:-4}"
REPORT_DIR="${REPORT_DIR:-$OLD_DIR/shadow-reports/live}"
START_SERVICE="${START_SERVICE:-0}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST_IP="${HOST_IP:-$(hostname -I | awk '{print $1}')}"
NEW_BASE="${NEW_BASE:-http://${HOST_IP}:${LISTEN_PORT}}"
OLD_BASE="${OLD_BASE:-http://${HOST_IP}:${OLD_PORT}}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_root() {
  [[ "$(id -u)" == "0" ]] || die "run as root on the monitor unify-query host"
}

main() {
  require_root
  [[ -f "$SCRIPT_DIR/live_shadow_compare.py" ]] || die "missing live_shadow_compare.py in $SCRIPT_DIR"
  [[ -x "$GOR_BIN" ]] || die "missing executable GoReplay binary: $GOR_BIN"
  [[ -d "$OLD_DIR" ]] || die "old unify-query dir missing: $OLD_DIR"

  install -d "$OLD_DIR"
  install -m 0755 "$SCRIPT_DIR/live_shadow_compare.py" "$OLD_DIR/live_shadow_compare.py"
  install -d "$REPORT_DIR"

  cat >"/etc/systemd/system/${SERVICE_NAME}" <<EOF
[Unit]
Description="UnifyQuery live shadow compare"
After=network-online.target bk-unify-query.service bk-unify-query-old.service
Requires=bk-unify-query-old.service

[Service]
User=root
Group=root
ExecStart=/usr/bin/env python3 ${OLD_DIR}/live_shadow_compare.py \\
  --gor-bin ${GOR_BIN} \\
  --listen-port ${LISTEN_PORT} \\
  --new-mode ${NEW_MODE} \\
  --new-base ${NEW_BASE} \\
  --old-base ${OLD_BASE} \\
  --report-dir ${REPORT_DIR} \\
  --allow-path /query/ts \\
  --sample-rate ${SAMPLE_RATE} \\
  --max-inflight ${MAX_INFLIGHT} \\
  --max-pending 4096 \\
  --new-timeout 30 \\
  --old-timeout 30 \\
  --flush-interval 10 \\
  --recent-limit 50
Restart=always
RestartSec=3s
LimitNOFILE=204800

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  if [[ "$START_SERVICE" == "1" ]]; then
    systemctl restart "$SERVICE_NAME"
    systemctl is-active "$SERVICE_NAME"
  else
    echo "installed ${SERVICE_NAME}; set START_SERVICE=1 to start it"
  fi
}

main "$@"
