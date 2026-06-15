#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="${SRC_DIR:-/data/bkee/bkmonitorv3/unify-query}"
OLD_DIR="${OLD_DIR:-/data/bkee/bkmonitorv3/unify-query-old}"
OLD_BINARY_NAME="${OLD_BINARY_NAME:-unify-query.bak.20260615094936}"
OLD_PORT="${OLD_PORT:-10216}"
SERVICE_NAME="${SERVICE_NAME:-bk-unify-query-old.service}"
RUN_USER="${RUN_USER:-blueking}"
RUN_GROUP="${RUN_GROUP:-blueking}"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_root() {
  [[ "$(id -u)" == "0" ]] || die "run as root on the monitor unify-query host"
}

patch_yaml() {
  local src="$1"
  local dst="$2"
  python3 - "$src" "$dst" "$OLD_PORT" <<'PY'
import re
import sys
from pathlib import Path

src, dst, port = sys.argv[1], sys.argv[2], sys.argv[3]
text = Path(src).read_text()

lines = text.splitlines()
patched_lines = []
in_http = False
http_port_patched = False

for line in lines:
    if re.match(r"^http:\s*$", line):
        in_http = True
        patched_lines.append(line)
        continue
    if in_http and re.match(r"^\S", line):
        in_http = False
    if in_http and re.match(r"^\s*port:\s*\d+\s*$", line):
        indent = re.match(r"^(\s*)", line).group(1)
        patched_lines.append(f"{indent}port: {port}")
        http_port_patched = True
        continue
    patched_lines.append(line)

if not http_port_patched:
    raise SystemExit("http.port not found in source config")

text = "\n".join(patched_lines) + "\n"
text = re.sub(
    r"(?m)^(\s*path:\s*)/data/bkee/logs/bkmonitorv3/unify-query\.log\s*$",
    r"\g<1>/data/bkee/logs/bkmonitorv3/unify-query-old.log",
    text,
)

Path(dst).write_text(text)
PY
}

write_unit() {
  cat >"/etc/systemd/system/${SERVICE_NAME}" <<EOF
[Unit]
Description="Blueking bkmonitorv3 unify query old shadow"
After=network-online.target

[Service]
User=${RUN_USER}
Group=${RUN_GROUP}
ExecStart=${OLD_DIR}/unify-query --config ${OLD_DIR}/unify-query-old.yaml
Restart=always
RestartSec=3s
LimitNOFILE=204800

[Install]
WantedBy=multi-user.target
EOF
}

main() {
  require_root
  [[ -d "$SRC_DIR" ]] || die "source dir missing: $SRC_DIR"
  [[ -x "$SRC_DIR/$OLD_BINARY_NAME" ]] || die "old binary missing or not executable: $SRC_DIR/$OLD_BINARY_NAME"
  [[ -f "$SRC_DIR/unify-query.yaml" ]] || die "source config missing: $SRC_DIR/unify-query.yaml"

  install -d -o "$RUN_USER" -g "$RUN_GROUP" "$OLD_DIR"
  rsync -a --delete \
    --exclude "shadow-reports/" \
    "$SRC_DIR/" "$OLD_DIR/"

  cp -a "$SRC_DIR/$OLD_BINARY_NAME" "$OLD_DIR/unify-query"
  chmod 0755 "$OLD_DIR/unify-query"
  patch_yaml "$SRC_DIR/unify-query.yaml" "$OLD_DIR/unify-query-old.yaml"
  chown -R "$RUN_USER:$RUN_GROUP" "$OLD_DIR"
  install -d -o "$RUN_USER" -g "$RUN_GROUP" "$OLD_DIR/shadow-reports"

  write_unit
  systemctl daemon-reload
  systemctl restart "$SERVICE_NAME"
  systemctl is-active "$SERVICE_NAME"
  curl -sf "http://127.0.0.1:${OLD_PORT}/influxdb_print" >/dev/null || \
    curl -sf "http://$(hostname -I | awk '{print $1}'):${OLD_PORT}/influxdb_print" >/dev/null
  echo "old unify-query shadow service is ready on port ${OLD_PORT}"
}

main "$@"
