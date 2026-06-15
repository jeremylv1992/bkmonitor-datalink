# UnifyQuery Live Shadow Compare Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建一个可持续运行的 live shadow compare 工具，实时采集 monitor 环境 10206 上的线上请求，按配置重放到新服务 10206 和旧服务 10216，持续输出不一致案例、失败请求 traceid、请求体内容与基础归因报告。

**Architecture:** 使用 GoReplay 在 10206 做被动 raw capture，Python daemon 从 stdout 流式读取请求。默认 `new-mode=replay`：同一请求同时带 `X-UnifyQuery-Shadow-Replay: 1` 重放到 new `:10206` 和 old `:10216`，用两边返回内容对比；采集侧遇到该 header 直接丢弃，避免 replay 流量再次进入 replay 链路。可选 `new-mode=capture-response`：如果 GoReplay 支持 response tracking，则 new 侧使用线上真实响应，只重放 old。

**Tech Stack:** Python 3 stdlib, GoReplay raw capture, systemd, Bash, NDJSON/Markdown/JSON reports.

---

## Design Decisions

1. **默认允许重放到 10206，但必须带 replay 标识。**  
   `new-mode=replay` 会把捕获的线上请求重放到 new `:10206` 和 old `:10216`，拿两边返回内容做对比。所有重放请求都必须带 `X-UnifyQuery-Shadow-Replay: 1`；采集侧必须先识别这个 header 并跳过，防止 replay 请求再次被 replay。代价是 new 服务也会多承受一份采样查询压力。

2. **保留 capture-response 低负载模式。**  
   如果 GoReplay 版本支持 response tracking，可以切到 `new-mode=capture-response`，new 侧直接使用真实线上响应，只 replay old `:10216`。该模式对 new 服务没有额外查询压力，但依赖 GoReplay 能捕获响应。

3. **旧服务只监听独立端口 10216。**  
   旧服务使用上一份计划里的 `bk-unify-query-old.service` 启动，监听独立端口，不注册到线上入口。

4. **实时处理，但报告异步落盘。**  
   `new-mode=replay` 下请求进入后直接放入 bounded queue，worker 同时 replay new 和 old；`new-mode=capture-response` 下先关联线上响应，再 replay old。comparator 归因，reporter 周期性 flush。

5. **不一致和请求失败案例必须可追溯。**  
   每个 mismatch 写一行 NDJSON，保留 request hash、traceid、path、status、body hash、diff path、基础归因、截断预览。任何 new/old replay 失败、超时、5xx、非预期 decode 失败，都要额外写入 failure NDJSON，记录 traceid 和请求体内容。

6. **先被动 capture，后备 active proxy。**  
   默认 `new-mode=replay` 只依赖 GoReplay 能捕获 10206 请求，不依赖 response tracking。只有被动 raw capture 无法观察请求时，才考虑 active proxy。active proxy 会改变线上请求链路，不作为第一选择。

---

## Runtime Flow

```text
client
  |
  v
new unify-query :10206
  |
  +-- normal response to client
  |
  +-- GoReplay passive capture request
        |
        v
live_shadow_compare.py
  - parse request record
  - skip replay-tagged requests
  - replay request to new :10206 with X-UnifyQuery-Shadow-Replay: 1
  - replay request to old :10216 with X-UnifyQuery-Shadow-Replay: 1
  - compare replayed new response vs replayed old response
  - write mismatch NDJSON and summary reports
```

Replay loop prevention:

```text
capture input: port 10206 only
replay output: port 10206 and 10216 in new-mode=replay
replay marker: X-UnifyQuery-Shadow-Replay: 1
drop rule: any captured request with X-UnifyQuery-Shadow-Replay is ignored
dedupe: request sha256 TTL cache prevents accidental duplicate processing bursts
```

Optional low-load flow:

```text
new-mode=capture-response
  - GoReplay captures request + real 10206 response
  - Python uses captured response as new side
  - Python only replays old :10216
  - requires GoReplay support for response tracking
```

---

## File Structure

- Create: `tools/unify-query-shadow/live_shadow_compare.py`
  - Long-running daemon-style Python tool. Starts GoReplay, reads request records, skips replay-tagged requests, replays to new/old according to `--new-mode`, classifies diffs, writes mismatch and failure reports.
- Create: `tools/unify-query-shadow/test_live_shadow_compare.py`
  - Unit tests for streaming parser, loop guard, attribution classifier, report writer.
- Create: `tools/unify-query-shadow/install_live_shadow_compare.sh`
  - Installs scripts on monitor host, writes systemd unit `bk-unify-query-shadow-compare.service`, starts continuous compare.
- Reuse: `tools/unify-query-shadow/setup_old_unify_query.sh`
  - Starts old service on 10216.
- Reuse: `tools/unify-query-shadow/compare_goreplay.py`
  - Keep for offline replay; live mode uses the same comparison primitives where possible.

---

## Report Files

Default directory:

```text
/data/bkee/bkmonitorv3/unify-query-old/shadow-reports/live/
```

Files:

```text
mismatch-YYYYMMDD.ndjson     # append-only mismatch cases
failure-YYYYMMDD.ndjson      # append-only request failure cases with traceid and request body
summary.json                 # overwritten every flush interval
summary.md                   # human-readable rolling summary
live-shadow.log              # service log
```

Mismatch NDJSON shape:

```json
{
  "time": "2026-06-15T11:20:31+08:00",
  "gor_id": "123456789",
  "trace_id": "bk-trace-xxx",
  "trace_id_source": "header:X-Bkapi-Trace-Id",
  "method": "POST",
  "path": "/query/ts",
  "request_sha256": "4a9f...",
  "request_body_encoding": "utf-8",
  "request_body": "{\"query_list\":[...]}",
  "request_body_truncated": false,
  "request_preview": "{\"query_list\":[...]}",
  "attribution": "datapoint_value_mismatch",
  "diff_path": "body.data.result[0].values[3][1]",
  "diff_message": "new='12.3' old='12.4'",
  "new": {
    "status_code": 200,
    "elapsed_ms": 31.5,
    "body_sha256": "ab12...",
    "body_preview": "{\"result\":true,...}"
  },
  "old": {
    "status_code": 200,
    "elapsed_ms": 44.8,
    "body_sha256": "cd34...",
    "body_preview": "{\"result\":true,...}"
  }
}
```

Failure NDJSON shape:

```json
{
  "time": "2026-06-15T11:20:31+08:00",
  "gor_id": "123456789",
  "trace_id": "bk-trace-xxx",
  "trace_id_source": "header:X-Bkapi-Trace-Id",
  "method": "POST",
  "path": "/query/ts",
  "request_sha256": "4a9f...",
  "request_body_encoding": "utf-8",
  "request_body": "{\"query_list\":[...]}",
  "request_body_truncated": false,
  "failure_side": "new",
  "failure_type": "transport_error",
  "failure_message": "timed out",
  "new": {
    "status_code": null,
    "elapsed_ms": 30001.1,
    "error": "timed out"
  },
  "old": {
    "status_code": 200,
    "elapsed_ms": 44.8,
    "body_sha256": "cd34..."
  }
}
```

Summary shape:

```json
{
  "started_at": "2026-06-15T11:00:00+08:00",
  "updated_at": "2026-06-15T11:20:31+08:00",
  "seen": 3800,
  "eligible": 1800,
  "skipped_replay_tag": 1800,
  "skipped_path": 200,
  "new_response_missing": 0,
  "replayed": 1797,
  "matched": 1750,
  "mismatched": 47,
  "request_failed": 2,
  "new_request_failed": 1,
  "old_request_failed": 1,
  "attribution_counts": {
    "datapoint_value_mismatch": 30,
    "series_count_mismatch": 10,
    "status_code_mismatch": 4,
    "json_decode_mismatch": 3
  },
  "latency_ms": {
    "new_p50": 30.1,
    "new_p95": 120.2,
    "old_p50": 33.3,
    "old_p95": 130.4
  },
  "recent_mismatches": [
    {
      "time": "2026-06-15T11:20:31+08:00",
      "path": "/query/ts",
      "attribution": "datapoint_value_mismatch",
      "diff_path": "body.data.result[0].values[3][1]"
    }
  ]
}
```

---

## Attribution Rules

Classifier uses the first matching rule in this order:

1. `new_response_missing`  
   GoReplay captured a request but no matching 10206 response arrived before `--new-response-timeout`.

2. `new_request_failed`  
   `new-mode=replay` replay to 10206 timed out, connection failed, or raised a transport exception.

3. `old_request_failed`  
   Replay to 10216 timed out, connection failed, or raised a transport exception.

4. `status_code_mismatch`  
   New and old HTTP status code differ.

5. `json_decode_mismatch`  
   One body is JSON and the other is not, or both should be JSON but one cannot decode.

6. `top_level_schema_mismatch`  
   JSON object top-level keys differ after volatile fields are removed.

7. `error_message_mismatch`  
   Both responses are JSON but error fields such as `error`, `message`, `errors`, or `trace` differ.

8. `series_count_mismatch`  
   Metric result containers differ in length. The classifier checks common paths: `data.result`, `data.series`, `series`, `list`.

9. `label_set_mismatch`  
   Series labels or dimensions differ after sorting by label key.

10. `datapoint_count_mismatch`  
   Same series has different number of datapoints.

11. `timestamp_mismatch`  
    Same series has datapoints at different timestamps.

12. `datapoint_value_mismatch`  
    Same timestamp has different value. Numeric strings are compared with configurable tolerance.

13. `body_mismatch`  
    Body differs but none of the structured metric rules matched.

14. `latency_regression_only`  
    Body and status match, but old latency exceeds `new_latency * --latency-ratio-threshold` and `--record-latency-only` is enabled.

Volatile fields removed before comparison:

```text
trace_id
request_id
bk_trace_id
elapsed
elapsed_ms
debug
```

Default numeric tolerance:

```text
absolute: 1e-9
relative: 1e-6
```

---

## Trace ID and Request Body Capture

Trace ID extraction priority:

```text
1. header: X-Bkapi-Trace-Id
2. header: X-Request-Id
3. header: X-Bk-Trace-Id
4. header: X-Trace-Id
5. header: Traceparent
6. JSON body field: trace_id
7. JSON body field: request_id
8. generated: request_sha256[:16]
```

Request body storage:

```text
Mismatch cases:
  - store request_body up to --mismatch-request-body-limit bytes
  - default limit: 4096

Failure cases:
  - store request_body up to --failure-request-body-limit bytes
  - default limit: 65536
  - this is required for new_request_failed, old_request_failed, and new_response_missing

Encoding:
  - if body is valid UTF-8, request_body_encoding=utf-8 and request_body is text
  - otherwise request_body_encoding=base64 and request_body is base64
  - request_body_truncated=true when original body exceeds the limit
```

Failure report privacy boundary:

```text
failure-YYYYMMDD.ndjson is local-only on the monitor host.
summary.md only includes traceid, path, attribution, diff path, body hash, and short preview.
Do not paste full request_body into chat or commit it to git.
```

---

## Task 1: Implement Live Parser and Loop Guard

**Files:**
- Create: `tools/unify-query-shadow/live_shadow_compare.py`
- Create: `tools/unify-query-shadow/test_live_shadow_compare.py`

- [ ] **Step 1: Add record parsing tests**

Add tests covering these records:

```python
def test_parse_goreplay_request_record():
    raw = (
        b"1 123 456\\n"
        b"POST /query/ts HTTP/1.1\\r\\n"
        b"Host: monitor\\r\\n"
        b"Content-Type: application/json\\r\\n"
        b"\\r\\n"
        b"{\\"a\\":1}"
    )
    record = parse_goreplay_record(raw)
    assert record.kind == "request"
    assert record.gor_id == "123"
    assert record.request.method == "POST"
    assert record.request.path == "/query/ts"

def test_parse_goreplay_response_record():
    raw = (
        b"2 123 789 32100000\\n"
        b"HTTP/1.1 200 OK\\r\\n"
        b"Content-Type: application/json\\r\\n"
        b"\\r\\n"
        b"{\\"ok\\":true}"
    )
    record = parse_goreplay_record(raw)
    assert record.kind == "response"
    assert record.gor_id == "123"
    assert record.response.status_code == 200
    assert record.response.body == b"{\\"ok\\":true}"

def test_skip_shadow_replay_header():
    req = ReplayRequest(
        index=1,
        method="POST",
        path="/query/ts",
        headers={"X-UnifyQuery-Shadow-Replay": "1"},
        body=b"{}",
    )
    assert should_skip_request(req, allow_paths=["/query/ts"]) == "replay_tag"

def test_extract_trace_id_from_header_then_body_then_hash():
    header_req = ReplayRequest(
        index=1,
        method="POST",
        path="/query/ts",
        headers={"X-Bkapi-Trace-Id": "trace-from-header"},
        body=b'{"trace_id":"trace-from-body"}',
    )
    body_req = ReplayRequest(
        index=2,
        method="POST",
        path="/query/ts",
        headers={},
        body=b'{"trace_id":"trace-from-body"}',
    )
    hash_req = ReplayRequest(index=3, method="POST", path="/query/ts", headers={}, body=b"{}")
    assert extract_trace_id(header_req).value == "trace-from-header"
    assert extract_trace_id(body_req).value == "trace-from-body"
    assert extract_trace_id(hash_req).source == "generated:request_sha256"

def test_encode_request_body_keeps_failed_request_body():
    body = b'{"query_list":[{"metric":"cpu"}]}'
    encoded = encode_request_body(body, limit=65536)
    assert encoded["request_body_encoding"] == "utf-8"
    assert encoded["request_body"] == '{"query_list":[{"metric":"cpu"}]}'
    assert encoded["request_body_truncated"] is False
```

- [ ] **Step 2: Implement parser data types**

Implement:

```python
@dataclass
class RawRecord:
    kind: str
    gor_id: str
    request: Optional[ReplayRequest] = None
    response: Optional[CapturedResponse] = None

@dataclass
class CapturedResponse:
    status_code: Optional[int]
    headers: Dict[str, str]
    body: bytes
    elapsed_ms: Optional[float]
    error: Optional[str] = None
```

- [ ] **Step 3: Implement loop guard**

Rules:

```python
def should_skip_request(req: ReplayRequest, allow_paths: List[str]) -> Optional[str]:
    if req.headers.get("X-UnifyQuery-Shadow-Replay") == "1":
        return "replay_tag"
    if not any(req.path.startswith(prefix) for prefix in allow_paths):
        return "path"
    return None
```

- [ ] **Step 4: Run parser tests**

Run:

```bash
python3 -m unittest tools/unify-query-shadow/test_live_shadow_compare.py
```

Expected: parser and loop guard tests pass.

---

## Task 2: Implement Real-Time Correlator

**Files:**
- Modify: `tools/unify-query-shadow/live_shadow_compare.py`
- Modify: `tools/unify-query-shadow/test_live_shadow_compare.py`

- [ ] **Step 1: Add correlator tests**

Test behavior:

```python
def test_correlator_emits_pair_after_request_and_response():
    corr = Correlator(new_response_timeout=1.0)
    req = RawRecord(kind="request", gor_id="abc", request=sample_request())
    resp = RawRecord(kind="response", gor_id="abc", response=sample_response())
    assert corr.add(req) == []
    pairs = corr.add(resp)
    assert len(pairs) == 1
    assert pairs[0].request.path == "/query/ts"
    assert pairs[0].new_response.status_code == 200

def test_correlator_marks_missing_new_response():
    corr = Correlator(new_response_timeout=0.01)
    corr.add(RawRecord(kind="request", gor_id="abc", request=sample_request()))
    time.sleep(0.02)
    expired = corr.expire()
    assert expired[0].new_response.error == "new_response_missing"
```

- [ ] **Step 2: Implement bounded pending map**

Behavior:

```text
pending[gor_id] = request until matching response arrives
expire() returns pairs with synthetic new_response_missing
max_pending evicts oldest records as new_response_missing
```

- [ ] **Step 3: Run correlator tests**

Run:

```bash
python3 -m unittest tools/unify-query-shadow/test_live_shadow_compare.py
```

Expected: correlator tests pass.

---

## Task 3: Implement Old-Service Replay Worker

**Files:**
- Modify: `tools/unify-query-shadow/live_shadow_compare.py`
- Modify: `tools/unify-query-shadow/test_live_shadow_compare.py`

- [ ] **Step 1: Add replay request behavior**

Replay behavior must:

```text
new-mode=replay:
  new target URL = new_base + original path
  old target URL = old_base + original path
  replay both requests concurrently when possible

new-mode=capture-response:
  new side uses captured response
  old target URL = old_base + original path

method = original method
body = original body
headers = original headers minus hop-by-hop headers
added header = X-UnifyQuery-Shadow-Replay: 1
timeout = --new-timeout for new replay
timeout = --old-timeout for old replay
```

- [ ] **Step 2: Add replay tests with mock new and old servers**

Expected assertions:

```python
assert new_server.received_path == "/query/ts"
assert old_server.received_path == "/query/ts"
assert new_server.received_headers["X-UnifyQuery-Shadow-Replay"] == "1"
assert old_server.received_headers["X-UnifyQuery-Shadow-Replay"] == "1"
assert result.new.status_code == 200
assert result.old.status_code == 200
```

- [ ] **Step 3: Run replay tests**

Run:

```bash
python3 -m unittest tools/unify-query-shadow/test_live_shadow_compare.py
```

Expected: replay worker tests pass.

---

## Task 4: Implement Diff Attribution

**Files:**
- Modify: `tools/unify-query-shadow/live_shadow_compare.py`
- Modify: `tools/unify-query-shadow/test_live_shadow_compare.py`

- [ ] **Step 1: Add attribution tests**

Cases:

```python
def test_classify_status_code_mismatch():
    case = classify(new_resp(status=200, body=b"{}"), old_resp(status=500, body=b"{}"))
    assert case.attribution == "status_code_mismatch"

def test_classify_series_count_mismatch():
    new = new_resp(body=b'{"data":{"result":[{"metric":{"a":"1"},"values":[[1,"2"]]}]}}')
    old = old_resp(body=b'{"data":{"result":[]}}')
    case = classify(new, old)
    assert case.attribution == "series_count_mismatch"

def test_classify_datapoint_value_mismatch():
    new = new_resp(body=b'{"data":{"result":[{"metric":{"a":"1"},"values":[[1,"2"]]}]}}')
    old = old_resp(body=b'{"data":{"result":[{"metric":{"a":"1"},"values":[[1,"3"]]}]}}')
    case = classify(new, old)
    assert case.attribution == "datapoint_value_mismatch"
```

- [ ] **Step 2: Implement normalization**

Normalization must:

```text
remove volatile fields recursively
sort dictionary keys
sort metric series by serialized labels
compare numeric strings with tolerance
```

- [ ] **Step 3: Implement attribution priority**

Use the priority list from the `Attribution Rules` section.

- [ ] **Step 4: Run attribution tests**

Run:

```bash
python3 -m unittest tools/unify-query-shadow/test_live_shadow_compare.py
```

Expected: attribution tests pass.

---

## Task 5: Implement Continuous Reporter

**Files:**
- Modify: `tools/unify-query-shadow/live_shadow_compare.py`
- Modify: `tools/unify-query-shadow/test_live_shadow_compare.py`

- [ ] **Step 1: Add reporter tests**

Expected behavior:

```python
def test_reporter_writes_mismatch_ndjson_and_summary(tmp_path):
    reporter = Reporter(report_dir=tmp_path, recent_limit=2)
    reporter.record(make_mismatch("status_code_mismatch"))
    reporter.flush()
    assert (tmp_path / "summary.json").exists()
    lines = list((tmp_path / today_mismatch_file()).read_text().splitlines())
    assert len(lines) == 1
    assert json.loads(lines[0])["attribution"] == "status_code_mismatch"

def test_reporter_writes_failure_with_trace_id_and_request_body(tmp_path):
    reporter = Reporter(report_dir=tmp_path, recent_limit=2)
    reporter.record(make_failure(
        attribution="old_request_failed",
        trace_id="trace-001",
        request_body=b'{"query_list":[{"metric":"cpu"}]}',
    ))
    reporter.flush()
    lines = list((tmp_path / today_failure_file()).read_text().splitlines())
    payload = json.loads(lines[0])
    assert payload["trace_id"] == "trace-001"
    assert payload["request_body"] == '{"query_list":[{"metric":"cpu"}]}'
    assert payload["failure_side"] == "old"
```

- [ ] **Step 2: Implement report files**

Writer behavior:

```text
append mismatch cases immediately to mismatch-YYYYMMDD.ndjson
append request failure cases immediately to failure-YYYYMMDD.ndjson
failure records must include trace_id, trace_id_source, request_body_encoding, request_body, request_body_truncated
write summary.json atomically through summary.json.tmp + rename
write summary.md every --flush-interval seconds
keep only --recent-limit recent mismatch summaries in summary files
keep only traceid, path, attribution, diff path, body hash, and short body preview in summary.md
```

- [ ] **Step 3: Run reporter tests**

Run:

```bash
python3 -m unittest tools/unify-query-shadow/test_live_shadow_compare.py
```

Expected: reporter tests pass.

---

## Task 6: Implement Daemon CLI

**Files:**
- Modify: `tools/unify-query-shadow/live_shadow_compare.py`

- [ ] **Step 1: Add CLI arguments**

Arguments:

```text
--gor-bin /tmp/gor
--listen-port 10206
--new-mode replay
--new-base http://127.0.0.1:10206
--old-base http://127.0.0.1:10216
--report-dir /data/bkee/bkmonitorv3/unify-query-old/shadow-reports/live
--allow-path /query/ts
--sample-rate 1.0
--max-inflight 32
--max-pending 4096
--new-timeout 30
--new-response-timeout 30
--old-timeout 30
--flush-interval 10
--recent-limit 50
--mismatch-request-body-limit 4096
--failure-request-body-limit 65536
--absolute-tolerance 1e-9
--relative-tolerance 1e-6
--latency-ratio-threshold 2.0
--record-latency-only
```

- [ ] **Step 2: Start GoReplay subprocess**

Command:

```bash
gor --input-raw :10206 --output-stdout
```

Startup validation:

```text
run `gor --help`
fail fast if output does not include `output-stdout`
if --new-mode=capture-response, also require `input-raw-track-response`
```

- [ ] **Step 3: Process stream**

Loop:

```text
read GoReplay records split by the gor separator
parse request/response records
apply skip rules to requests
if new-mode=capture-response, correlate request + captured new response
if new-mode=replay, enqueue eligible request immediately
worker replays to new and old when new-mode=replay
worker replays only to old when new-mode=capture-response
classify
write mismatch and failure records with traceid and request body fields
report
flush periodically
handle SIGTERM/SIGINT by flushing before exit
```

- [ ] **Step 4: Run full unit tests**

Run:

```bash
python3 -m unittest tools/unify-query-shadow/test_live_shadow_compare.py
```

Expected: all tests pass.

---

## Task 7: Install Continuous Service

**Files:**
- Create: `tools/unify-query-shadow/install_live_shadow_compare.sh`

- [ ] **Step 1: Create installer script**

Script behavior:

```text
copy live_shadow_compare.py to /data/bkee/bkmonitorv3/unify-query-old/live_shadow_compare.py
create /data/bkee/bkmonitorv3/unify-query-old/shadow-reports/live
write /etc/systemd/system/bk-unify-query-shadow-compare.service
run systemctl daemon-reload
start service only when START_SERVICE=1
```

Systemd unit:

```ini
[Unit]
Description="UnifyQuery live shadow compare"
After=network-online.target bk-unify-query.service bk-unify-query-old.service
Requires=bk-unify-query-old.service

[Service]
User=root
Group=root
ExecStart=/usr/bin/python3 /data/bkee/bkmonitorv3/unify-query-old/live_shadow_compare.py \
  --gor-bin /tmp/gor \
  --listen-port 10206 \
  --new-mode replay \
  --new-base http://127.0.0.1:10206 \
  --old-base http://127.0.0.1:10216 \
  --report-dir /data/bkee/bkmonitorv3/unify-query-old/shadow-reports/live \
  --allow-path /query/ts \
  --sample-rate 1.0 \
  --max-inflight 16 \
  --max-pending 4096 \
  --new-timeout 30 \
  --old-timeout 30 \
  --flush-interval 10 \
  --recent-limit 50
Restart=always
RestartSec=3s
LimitNOFILE=204800

[Install]
WantedBy=multi-user.target
```

Reason for `User=root`: GoReplay raw capture usually needs raw socket privileges. If `/tmp/gor` has `cap_net_raw,cap_net_admin+ep`, the unit can be changed to `User=blueking`.

- [ ] **Step 2: Validate shell syntax**

Run:

```bash
bash -n tools/unify-query-shadow/install_live_shadow_compare.sh
```

Expected: command exits with code `0`.

---

## Monitor Execution Plan

- [ ] **Step 1: Confirm old service is running**

Run on monitor target:

```bash
systemctl is-active bk-unify-query-old.service
curl -sf http://127.0.0.1:10216/influxdb_print >/dev/null
```

Expected:

```text
active
```

- [ ] **Step 2: Confirm GoReplay supports stdout capture**

Run on monitor target:

```bash
/tmp/gor --help | grep -E 'output-stdout'
```

Expected: `output-stdout` appears. If `input-raw-track-response` also appears, `new-mode=capture-response` can be used later to reduce new-service replay load.

- [ ] **Step 3: Run live compare in foreground for 2 minutes**

Run on monitor target:

```bash
python3 /data/bkee/bkmonitorv3/unify-query-old/live_shadow_compare.py \
  --gor-bin /tmp/gor \
  --listen-port 10206 \
  --new-mode replay \
  --new-base http://127.0.0.1:10206 \
  --old-base http://127.0.0.1:10216 \
  --report-dir /data/bkee/bkmonitorv3/unify-query-old/shadow-reports/live \
  --allow-path /query/ts \
  --sample-rate 0.1 \
  --max-inflight 4 \
  --flush-interval 5
```

Expected:

```text
seen requests increase
eligible requests increase when /query/ts traffic exists
summary.json is written
tool requests sent to 10206 include X-UnifyQuery-Shadow-Replay: 1
captured replay-tagged requests are counted as skipped_replay_tag and are not replayed again
```

- [ ] **Step 4: Start continuous service**

Run:

```bash
START_SERVICE=1 bash /tmp/install_live_shadow_compare.sh
systemctl status bk-unify-query-shadow-compare.service --no-pager
```

Expected: service is `active`.

- [ ] **Step 5: Watch reports**

Run:

```bash
tail -f /data/bkee/bkmonitorv3/unify-query-old/shadow-reports/live/mismatch-$(date +%Y%m%d).ndjson
```

And:

```bash
cat /data/bkee/bkmonitorv3/unify-query-old/shadow-reports/live/summary.md
```

Expected: mismatches appear only when responses differ; summary includes attribution counts and recent examples.

---

## Active Proxy Fallback

Use only if passive raw capture cannot observe requests at all. This mode changes the live traffic path and needs a maintenance window.

Safe fallback architecture:

```text
client -> proxy :10206
proxy -> new unify-query :10207 -> client response
proxy -> old unify-query :10216 -> async shadow response
proxy writes mismatch reports
```

Replay loop prevention in proxy mode:

```text
proxy listens 10206
new service moves to 10207
old service remains 10216
mirror requests carry X-UnifyQuery-Shadow-Replay: 1
proxy drops any incoming request carrying X-UnifyQuery-Shadow-Replay
proxy never mirrors requests to 10206
```

Rollback:

```text
stop proxy
move new unify-query back to 10206
stop old service if no longer needed
```

---

## Safety Limits

Default live limits:

```text
sample_rate: 1.0 for final run, 0.1 for initial foreground test
max_inflight: 16
max_pending: 4096
new_response_timeout: 30s
new_timeout: 30s
old_timeout: 30s
flush_interval: 10s
recent_limit: 50
body_preview_limit: 512 bytes
request_preview_limit: 512 bytes
mismatch_request_body_limit: 4096 bytes
failure_request_body_limit: 65536 bytes
```

Operational rules:

```text
If replaying to 10206, every replay request must carry X-UnifyQuery-Shadow-Replay: 1.
Captured requests carrying X-UnifyQuery-Shadow-Replay: 1 must be skipped before enqueue.
Do not run without old service health check.
Do not store full request headers containing auth tokens in markdown.
Keep full mismatch/failure NDJSON local to monitor host unless explicitly needed.
Failure NDJSON must record traceid and request body content for new_request_failed, old_request_failed, and new_response_missing.
Rotate or delete raw capture/debug files older than one day.
Start with sample_rate=0.1 before sample_rate=1.0.
```

---

## Verification Checklist

- [ ] Unit tests pass for parser, correlator, replay worker, attribution, reporter.
- [ ] Foreground run writes `summary.json`.
- [ ] In `new-mode=replay`, `skipped_replay_tag` increases when the tool replays to 10206 and those captured replay-tagged requests are not replayed again.
- [ ] Old service receives requests with `X-UnifyQuery-Shadow-Replay: 1`.
- [ ] New service replay requests, when enabled, receive `X-UnifyQuery-Shadow-Replay: 1`.
- [ ] Mismatch NDJSON contains attribution and diff path for every unequal case.
- [ ] Failure NDJSON contains traceid, request body, failure side, and failure type for every failed replay/capture case.
- [ ] `summary.md` contains attribution counts and recent mismatch examples.
- [ ] Stopping `bk-unify-query-shadow-compare.service` leaves new 10206 service unaffected.

---

## Self-Review

Spec coverage:
- 脚本持续运行：covered by `live_shadow_compare.py` daemon and systemd unit.
- 实时获取流量：covered by GoReplay stdout stream on 10206.
- 实时重放：covered by worker replay to new 10206 and old 10216 in `new-mode=replay`, or only old 10216 in `new-mode=capture-response`.
- 重放不能再次重放：covered by replay header, skip rule, and bounded dedupe cache.
- 最终报告记录不一致案例：covered by mismatch NDJSON and recent markdown examples.
- 请求失败记录 traceid 和请求体：covered by failure NDJSON shape, trace extraction, and reporter tests.
- 基本归因：covered by attribution priority rules and summary counts.

Risk posture:
- Passive mode does not replace or restart new 10206 service.
- Replay load is bounded and starts with sampling.
- Active proxy fallback is isolated and requires an explicit maintenance window.
