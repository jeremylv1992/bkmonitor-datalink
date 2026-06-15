# UnifyQuery Shadow Compare Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 monitor 环境复制一份 `unify-query-old`，用旧二进制 `unify-query.bak.20260615094936` 启动到独立端口，并把 10206 的线上请求采集后重放到新旧服务，生成差异汇总。

**Architecture:** 第一阶段采用非侵入式 shadow compare：新服务继续监听 10206，GoReplay 以 raw capture 方式被动采集 10206 请求，离线 replay 到新服务 10206 和旧服务 10216。旧服务独立目录、独立配置、独立 systemd unit、独立日志，不注册为线上入口。第二阶段如果必须真正代理 10206，再单独设计主动 proxy 切流方案。

**Tech Stack:** Bash, Python 3 stdlib, systemd, GoReplay, curl, Consul/read-only config.

---

## File Structure

- Create: `tools/unify-query-shadow/setup_old_unify_query.sh`
  - 在 monitor 目标机执行，复制 `/data/bkee/bkmonitorv3/unify-query` 到 `/data/bkee/bkmonitorv3/unify-query-old`，替换为旧二进制，生成独立配置和 systemd unit。
- Create: `tools/unify-query-shadow/run_capture_compare.sh`
  - 在 monitor 目标机执行，启动 GoReplay 采集 10206 流量，停止采集后调用 Python 对比工具生成报告。
- Create: `tools/unify-query-shadow/compare_goreplay.py`
  - 从 `tmp/compare_unifyquery_goreplay.py` 提升为正式工具，解析 GoReplay 文件，向新旧 endpoint 重放请求，输出 JSON 和 Markdown 报告。
- Create: `tools/unify-query-shadow/test_compare_goreplay.py`
  - 从 `tmp/test_compare_unifyquery_goreplay.py` 提升为正式测试，并补充 diff 汇总、路径过滤、响应耗时统计测试。
- Optional local-only: `.local/monitor_unifyquery_shadow_compare.sh`
  - 本地私有 wrapper，负责通过 `cwenv-ssh` 上传脚本到 monitor 环境并执行。该文件不入库。

---

## Recommended Runtime Layout

线上已有新服务：

```text
/data/bkee/bkmonitorv3/unify-query/
  unify-query
  unify-query.yaml
```

新增旧服务：

```text
/data/bkee/bkmonitorv3/unify-query-old/
  unify-query                  # copied from unify-query.bak.20260615094936
  unify-query-old.yaml         # copied and patched from new service config
  shadow-reports/
```

端口约定：

```text
new service: 10206
old service: 10216
```

systemd unit：

```text
/etc/systemd/system/bk-unify-query-old.service
```

---

## Task 1: Add Old-Service Setup Script

**Files:**
- Create: `tools/unify-query-shadow/setup_old_unify_query.sh`

- [ ] **Step 1: Create the script**

```bash
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
    if re.match(r'^http:\s*$', line):
        in_http = True
        patched_lines.append(line)
        continue
    if in_http and re.match(r'^\S', line):
        in_http = False
    if in_http and re.match(r'^\s*port:\s*\d+\s*$', line):
        indent = re.match(r'^(\s*)', line).group(1)
        patched_lines.append(f"{indent}port: {port}")
        http_port_patched = True
        continue
    patched_lines.append(line)

if not http_port_patched:
    raise SystemExit("http.port not found in source config")

text = "\n".join(patched_lines) + "\n"
text = re.sub(
    r'(?m)^(\s*path:\s*)/data/bkee/logs/bkmonitorv3/unify-query\.log\s*$',
    r'\g<1>/data/bkee/logs/bkmonitorv3/unify-query-old.log',
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
    --exclude 'shadow-reports/' \
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
```

- [ ] **Step 2: Make it executable**

Run:

```bash
chmod +x tools/unify-query-shadow/setup_old_unify_query.sh
```

Expected: command exits with code `0`.

- [ ] **Step 3: Validate shell syntax**

Run:

```bash
bash -n tools/unify-query-shadow/setup_old_unify_query.sh
```

Expected: command exits with code `0`.

---

## Task 2: Promote and Extend the Compare Tool

**Files:**
- Create: `tools/unify-query-shadow/compare_goreplay.py`
- Create: `tools/unify-query-shadow/test_compare_goreplay.py`

- [ ] **Step 1: Copy the existing parser and comparator**

Run:

```bash
mkdir -p tools/unify-query-shadow
cp tmp/compare_unifyquery_goreplay.py tools/unify-query-shadow/compare_goreplay.py
cp tmp/test_compare_unifyquery_goreplay.py tools/unify-query-shadow/test_compare_goreplay.py
```

Expected: both files exist under `tools/unify-query-shadow/`.

- [ ] **Step 2: Add request filtering options**

Modify `tools/unify-query-shadow/compare_goreplay.py`:

```python
parser.add_argument("--allow-path", action="append", default=["/query/ts"], help="only replay paths with this prefix")
parser.add_argument("--limit", type=int, default=0, help="maximum requests to replay; 0 means no limit")
parser.add_argument("--sleep-ms", type=float, default=0.0, help="sleep between replayed requests")
```

Update `run_compare` to filter before replay:

```python
def filter_requests(requests: List[ReplayRequest], allow_paths: List[str], limit: int) -> List[ReplayRequest]:
    filtered = [req for req in requests if any(req.path.startswith(prefix) for prefix in allow_paths)]
    if limit > 0:
        return filtered[:limit]
    return filtered
```

Expected behavior: `/influxdb_print` and unrelated requests are skipped unless explicitly allowed.

- [ ] **Step 3: Add summary by diff path and latency**

Add to report summary:

```python
def percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * pct))
    return round(ordered[idx], 3)

def diff_path_counts(items: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        if item["equal"]:
            continue
        key = item["diff_path"] or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))
```

Expected report fields:

```json
{
  "summary": {
    "replayed_requests": 10,
    "matched": 9,
    "mismatched": 1,
    "diff_path_counts": {"body.data.result[0].values[1][1]": 1},
    "left_latency_ms": {"p50": 12.3, "p95": 45.6},
    "right_latency_ms": {"p50": 11.9, "p95": 44.1}
  }
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
python3 tools/unify-query-shadow/test_compare_goreplay.py
```

Expected: `OK`.

---

## Task 3: Add Capture and Compare Runner

**Files:**
- Create: `tools/unify-query-shadow/run_capture_compare.sh`

- [ ] **Step 1: Create the runner**

```bash
#!/usr/bin/env bash
set -euo pipefail

CAPTURE_SECONDS="${CAPTURE_SECONDS:-60}"
NEW_BASE="${NEW_BASE:-http://127.0.0.1:10206}"
OLD_BASE="${OLD_BASE:-http://127.0.0.1:10216}"
REPORT_DIR="${REPORT_DIR:-/data/bkee/bkmonitorv3/unify-query-old/shadow-reports}"
GOR_BIN="${GOR_BIN:-gor}"
COMPARE_BIN="${COMPARE_BIN:-/data/bkee/bkmonitorv3/unify-query-old/compare_goreplay.py}"
ALLOW_PATH="${ALLOW_PATH:-/query/ts}"
LIMIT="${LIMIT:-0}"
SLEEP_MS="${SLEEP_MS:-0}"

ts="$(date +%Y%m%d%H%M%S)"
capture="${REPORT_DIR}/unify-query-${ts}.gor"
json_report="${REPORT_DIR}/unify-query-${ts}.json"
md_report="${REPORT_DIR}/unify-query-${ts}.md"

mkdir -p "$REPORT_DIR"

"$GOR_BIN" --input-raw :10206 --output-file "$capture" &
gor_pid=$!
sleep "$CAPTURE_SECONDS"
kill -INT "$gor_pid" || true
wait "$gor_pid" || true

python3 "$COMPARE_BIN" \
  --input "$capture" \
  --left "$NEW_BASE" \
  --right "$OLD_BASE" \
  --allow-path "$ALLOW_PATH" \
  --limit "$LIMIT" \
  --sleep-ms "$SLEEP_MS" \
  --output "$json_report" \
  --markdown "$md_report"

echo "capture=$capture"
echo "json_report=$json_report"
echo "markdown_report=$md_report"
```

- [ ] **Step 2: Make it executable**

Run:

```bash
chmod +x tools/unify-query-shadow/run_capture_compare.sh
```

Expected: command exits with code `0`.

- [ ] **Step 3: Validate shell syntax**

Run:

```bash
bash -n tools/unify-query-shadow/run_capture_compare.sh
```

Expected: command exits with code `0`.

---

## Task 4: Monitor Environment Execution Procedure

**Files:**
- Use: `tools/unify-query-shadow/setup_old_unify_query.sh`
- Use: `tools/unify-query-shadow/run_capture_compare.sh`
- Use: `tools/unify-query-shadow/compare_goreplay.py`

- [ ] **Step 1: Upload tools to monitor target host**

Use the existing `cwenv-ssh` pattern:

```bash
/Users/lvzeli/.codex/skills/cwenv-ssh/scripts/cwenv_ssh.sh upload-target \
  tools/unify-query-shadow/setup_old_unify_query.sh \
  /tmp/setup_old_unify_query.sh

/Users/lvzeli/.codex/skills/cwenv-ssh/scripts/cwenv_ssh.sh upload-target \
  tools/unify-query-shadow/run_capture_compare.sh \
  /tmp/run_capture_compare.sh

/Users/lvzeli/.codex/skills/cwenv-ssh/scripts/cwenv_ssh.sh upload-target \
  tools/unify-query-shadow/compare_goreplay.py \
  /tmp/compare_goreplay.py
```

Expected: each upload command exits with code `0`.

- [ ] **Step 2: Start old service**

Run on the monitor target host:

```bash
sudo bash /tmp/setup_old_unify_query.sh
```

Expected:

```text
active
old unify-query shadow service is ready on port 10216
```

- [ ] **Step 3: Verify new and old service endpoints**

Run on the monitor target host:

```bash
curl -sf 'http://127.0.0.1:10216/influxdb_print' >/tmp/old_influxdb_print.txt
curl -sf 'http://127.0.0.1:10206/influxdb_print' >/tmp/new_influxdb_print.txt || \
  curl -sf "http://$(hostname -I | awk '{print $1}'):10206/influxdb_print" >/tmp/new_influxdb_print.txt
```

Expected: both output files are non-empty.

- [ ] **Step 4: Capture and compare a small sample**

Run on the monitor target host:

```bash
GOR_BIN=/tmp/gor \
COMPARE_BIN=/tmp/compare_goreplay.py \
CAPTURE_SECONDS=60 \
NEW_BASE="http://$(hostname -I | awk '{print $1}'):10206" \
OLD_BASE="http://$(hostname -I | awk '{print $1}'):10216" \
LIMIT=50 \
SLEEP_MS=50 \
bash /tmp/run_capture_compare.sh
```

Expected:

```text
JSON report: /data/bkee/bkmonitorv3/unify-query-old/shadow-reports/...
Markdown report: /data/bkee/bkmonitorv3/unify-query-old/shadow-reports/...
```

- [ ] **Step 5: Review mismatch summary**

Run on the monitor target host:

```bash
tail -n +1 /data/bkee/bkmonitorv3/unify-query-old/shadow-reports/*.md | sed -n '1,160p'
```

Expected: summary includes replayed count, matched count, mismatched count, failed count, skipped count, and top failed request samples.

---

## Task 5: Operational Guardrails and Rollback

**Files:**
- Use: `/etc/systemd/system/bk-unify-query-old.service`
- Use: `/data/bkee/bkmonitorv3/unify-query-old`

- [ ] **Step 1: Run only during a bounded window**

Use:

```bash
CAPTURE_SECONDS=60 LIMIT=50 SLEEP_MS=50 bash /tmp/run_capture_compare.sh
```

Reason: replay doubles query traffic for sampled requests. Start with 50 requests before increasing sample size.

- [ ] **Step 2: Keep captures local and short-lived**

Run after reports are copied out:

```bash
find /data/bkee/bkmonitorv3/unify-query-old/shadow-reports -type f -name '*.gor' -mtime +1 -delete
```

Reason: GoReplay files may contain request bodies and headers.

- [ ] **Step 3: Roll back old service**

Run on monitor target host:

```bash
systemctl stop bk-unify-query-old.service || true
systemctl disable bk-unify-query-old.service || true
rm -f /etc/systemd/system/bk-unify-query-old.service
systemctl daemon-reload
```

Expected: new service on 10206 remains untouched.

- [ ] **Step 4: Remove old directory after review**

Run only after reports are no longer needed:

```bash
rm -rf /data/bkee/bkmonitorv3/unify-query-old
```

Expected: old service artifacts and reports are removed.

---

## Active Proxy Alternative

Do not use active proxy as the first implementation. It requires moving the new service away from 10206 and placing a proxy on 10206, which changes the live request path.

Use active proxy only if passive raw capture cannot observe traffic. The safe sequence would be:

```text
1. Change new unify-query port from 10206 to 10207.
2. Start old unify-query on 10216.
3. Start a proxy on 10206.
4. Proxy forwards the client response from 10207 and asynchronously mirrors to 10216.
5. Proxy records both responses and generates report.
6. Roll back by stopping proxy and moving new service back to 10206.
```

This is higher risk because client traffic depends on the proxy. Keep it out of the first validation pass.

---

## Verification Checklist

- [ ] `bash -n tools/unify-query-shadow/setup_old_unify_query.sh` passes.
- [ ] `bash -n tools/unify-query-shadow/run_capture_compare.sh` passes.
- [ ] `python3 tools/unify-query-shadow/test_compare_goreplay.py` passes.
- [ ] `systemctl is-active bk-unify-query-old.service` returns `active`.
- [ ] `curl http://<monitor-host>:10216/influxdb_print` succeeds.
- [ ] GoReplay capture file contains at least one `/query/ts` request.
- [ ] Compare report contains `replayed_requests > 0`.
- [ ] New service on 10206 is not restarted or reconfigured during passive capture.

---

## Self-Review

Spec coverage:
- Copy `unify-query` directory as `unify-query-old`: covered by Task 1.
- Run old binary `unify-query.bak.20260615094936`: covered by Task 1.
- Start old service on another port: covered by Task 1 with `OLD_PORT=10216`.
- Intercept 10206 requests: covered by Task 3 via GoReplay passive raw capture.
- Replay to new and old services: covered by Task 2 and Task 3.
- Compare and summarize: covered by Task 2 report generation.

Risk posture:
- The recommended path does not bind or replace 10206.
- Replay is bounded by `LIMIT`, `CAPTURE_SECONDS`, and `SLEEP_MS`.
- Captured request files stay on the monitor target host and have cleanup steps.
