#!/usr/bin/env python3
# Tencent is pleased to support the open source community by making
# 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
# Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
# Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://opensource.org/licenses/MIT
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""Live shadow compare for UnifyQuery requests captured by GoReplay."""

from __future__ import print_function

import argparse
import base64
import datetime
import hashlib
import json
import os
import queue
import random
import signal
import subprocess
import sys
import threading
import time
import traceback
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

try:
    from urllib.error import HTTPError, URLError
    from urllib.parse import urljoin, urlsplit, urlunsplit
    from urllib.request import Request, urlopen
except ImportError:  # pragma: no cover - Python 2 fallback for parser-only use.
    from urllib2 import HTTPError, URLError, Request, urlopen
    from urlparse import urljoin, urlsplit, urlunsplit


GOREPLAY_SEPARATOR = "\n🐵🙈🙉\n".encode("utf-8")
SHADOW_REPLAY_HEADER = "X-UnifyQuery-Shadow-Replay"
SHADOW_REPLAY_VALUE = "1"

HOP_BY_HOP_HEADERS = {
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

REQUEST_METHODS = {
    "GET",
    "POST",
    "PUT",
    "PATCH",
    "DELETE",
    "HEAD",
    "OPTIONS",
}

VOLATILE_KEYS = {
    "trace_id",
    "request_id",
    "bk_trace_id",
    "elapsed",
    "elapsed_ms",
    "debug",
}

TRACE_HEADER_PRIORITY = [
    "x-bkapi-trace-id",
    "x-request-id",
    "x-bk-trace-id",
    "x-trace-id",
    "traceparent",
]

TRACE_CONTEXT_HEADERS = {
    "traceparent",
    "tracestate",
    "x-b3-traceid",
    "x-b3-spanid",
    "x-b3-parentspanid",
    "x-b3-sampled",
    "x-b3-flags",
    "b3",
    "x-bkapi-trace-id",
    "x-request-id",
    "x-bk-trace-id",
    "x-trace-id",
}


class ReplayRequest(object):
    def __init__(self, index, gor_id, method, path, headers, body):
        self.index = index
        self.gor_id = gor_id
        self.method = method
        self.path = path
        self.headers = headers
        self.body = body


class ResponseResult(object):
    def __init__(self, status_code, headers, body, elapsed_ms, error=None, replay_headers=None):
        self.status_code = status_code
        self.headers = headers
        self.body = body
        self.elapsed_ms = elapsed_ms
        self.error = error
        self.replay_headers = replay_headers or {}


class RawRecord(object):
    def __init__(self, kind, gor_id, request=None, response=None):
        self.kind = kind
        self.gor_id = gor_id
        self.request = request
        self.response = response


class TraceInfo(object):
    def __init__(self, value, source):
        self.value = value
        self.source = source


class CompareResult(object):
    def __init__(self, equal, attribution="", diff_path="", message=""):
        self.equal = equal
        self.attribution = attribution
        self.diff_path = diff_path
        self.message = message


class WorkItem(object):
    def __init__(self, request, new_response=None):
        self.request = request
        self.new_response = new_response


def now_iso():
    return datetime.datetime.now().astimezone().isoformat()


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def lower_headers(headers):
    return dict((k.lower(), v) for k, v in headers.items())


def parse_goreplay_record(raw):
    raw = raw.strip(b"\r\n")
    if not raw:
        raise ValueError("empty record")
    first_line, sep, rest = raw.partition(b"\n")
    if not sep:
        raise ValueError("missing goreplay metadata line")

    parts = first_line.decode("utf-8", errors="replace").split()
    if len(parts) < 2:
        raise ValueError("invalid goreplay metadata line")

    kind_id = parts[0]
    gor_id = parts[1]
    if kind_id == "1":
        return RawRecord("request", gor_id, request=parse_raw_http_request(gor_id, rest))
    if kind_id == "2":
        elapsed_ms = None
        if len(parts) >= 4:
            try:
                elapsed_ms = float(parts[3]) / 1000000.0
            except ValueError:
                elapsed_ms = None
        return RawRecord("response", gor_id, response=parse_raw_http_response(rest, elapsed_ms))
    raise ValueError("unsupported goreplay record kind: %s" % kind_id)


def parse_raw_http_request(gor_id, raw):
    header_bytes, sep, body = raw.partition(b"\r\n\r\n")
    if not sep:
        header_bytes, sep, body = raw.partition(b"\n\n")
    if not sep:
        raise ValueError("missing HTTP request header/body separator")

    lines = header_bytes.decode("latin-1").splitlines()
    if not lines:
        raise ValueError("missing request line")

    request_line = lines[0].split()
    if len(request_line) < 2:
        raise ValueError("invalid request line")
    method = request_line[0].upper()
    path = request_line[1]
    if method not in REQUEST_METHODS:
        raise ValueError("unsupported request method: %s" % method)
    if not path.startswith("/"):
        parsed = urlsplit(path)
        path = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))

    headers = parse_headers(lines[1:])
    return ReplayRequest(index=0, gor_id=gor_id, method=method, path=path, headers=headers, body=body)


def parse_raw_http_response(raw, elapsed_ms=None):
    header_bytes, sep, body = raw.partition(b"\r\n\r\n")
    if not sep:
        header_bytes, sep, body = raw.partition(b"\n\n")
    if not sep:
        raise ValueError("missing HTTP response header/body separator")

    lines = header_bytes.decode("latin-1").splitlines()
    if not lines:
        raise ValueError("missing response line")

    parts = lines[0].split(None, 2)
    if len(parts) < 2 or not parts[0].startswith("HTTP/"):
        raise ValueError("invalid response line")
    try:
        status_code = int(parts[1])
    except ValueError:
        raise ValueError("invalid response status code")

    return ResponseResult(
        status_code=status_code,
        headers=parse_headers(lines[1:]),
        body=body,
        elapsed_ms=elapsed_ms,
    )


def parse_headers(lines):
    headers = {}
    for line in lines:
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


def iter_goreplay_records(stream, separator=GOREPLAY_SEPARATOR):
    buffer = b""
    while True:
        chunk = stream.read(8192)
        if not chunk:
            if buffer.strip():
                yield buffer
            return
        buffer += chunk
        while True:
            record, sep, rest = buffer.partition(separator)
            if not sep:
                break
            if record.strip():
                yield record
            buffer = rest


def should_skip_request(req, allow_paths):
    headers = lower_headers(req.headers)
    if headers.get(SHADOW_REPLAY_HEADER.lower()) == SHADOW_REPLAY_VALUE:
        return "replay_tag"
    if not any(req.path.startswith(prefix) for prefix in allow_paths):
        return "path"
    return None


def extract_trace_id(req):
    headers = lower_headers(req.headers)
    for key in TRACE_HEADER_PRIORITY:
        value = headers.get(key)
        if value:
            return TraceInfo(value, "header:%s" % canonical_trace_header(key))

    body_json, ok = load_json(req.body)
    if ok and isinstance(body_json, dict):
        for key in ("trace_id", "request_id"):
            value = body_json.get(key)
            if value:
                return TraceInfo(str(value), "body:%s" % key)

    return TraceInfo(sha256_hex(req.body)[:16], "generated:request_sha256")


def canonical_trace_header(key):
    mapping = {
        "x-bkapi-trace-id": "X-Bkapi-Trace-Id",
        "x-request-id": "X-Request-Id",
        "x-bk-trace-id": "X-Bk-Trace-Id",
        "x-trace-id": "X-Trace-Id",
        "traceparent": "Traceparent",
    }
    return mapping.get(key, key)


def encode_request_body(body, limit):
    truncated = len(body) > limit
    data = body[:limit]
    try:
        text = data.decode("utf-8")
        return {
            "request_body_encoding": "utf-8",
            "request_body": text,
            "request_body_truncated": truncated,
        }
    except UnicodeDecodeError:
        return {
            "request_body_encoding": "base64",
            "request_body": base64.b64encode(data).decode("ascii"),
            "request_body_truncated": truncated,
        }


def preview_body(body, limit=512):
    try:
        text = body.decode("utf-8", errors="replace")
    except TypeError:  # pragma: no cover - Python 2 fallback.
        text = body.decode("utf-8", "replace")
    if len(text) <= limit:
        return text
    return text[:limit] + "...<truncated>"


def build_url(base, original_path):
    return urljoin(base.rstrip("/") + "/", original_path.lstrip("/"))


def random_hex(byte_len):
    while True:
        value = os.urandom(byte_len).hex()
        if set(value) != {"0"}:
            return value


def new_traceparent():
    return "00-%s-%s-01" % (random_hex(16), random_hex(8))


def new_request_id():
    return random_hex(16)


def build_replay_headers(headers, request_id=None, traceparent=None):
    replay_headers = {}
    for key, value in headers.items():
        lowered = key.lower()
        if lowered in HOP_BY_HOP_HEADERS or lowered in TRACE_CONTEXT_HEADERS:
            continue
        replay_headers[key] = value
    request_id = request_id or new_request_id()
    replay_headers[SHADOW_REPLAY_HEADER] = SHADOW_REPLAY_VALUE
    replay_headers["Traceparent"] = traceparent or new_traceparent()
    replay_headers["X-Request-Id"] = request_id
    replay_headers["X-Bkapi-Trace-Id"] = request_id
    return replay_headers


def send_request(replay, base_url, timeout, replay_headers=None):
    url = build_url(base_url, replay.path)
    data = replay.body if replay.method not in {"GET", "HEAD"} else None
    start = time.perf_counter() if hasattr(time, "perf_counter") else time.time()
    req = Request(url=url, data=data, method=replay.method)
    replay_headers = replay_headers or build_replay_headers(replay.headers)
    for key, value in replay_headers.items():
        req.add_header(key, value)

    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            elapsed_ms = elapsed_since_ms(start)
            return ResponseResult(resp.status, dict(resp.headers.items()), body, elapsed_ms, replay_headers=replay_headers)
    except HTTPError as err:
        body = err.read()
        elapsed_ms = elapsed_since_ms(start)
        return ResponseResult(err.code, dict(err.headers.items()), body, elapsed_ms, replay_headers=replay_headers)
    except (URLError, OSError) as err:
        return ResponseResult(None, {}, b"", elapsed_since_ms(start), error=str(err), replay_headers=replay_headers)
    except Exception as err:  # pragma: no cover - defensive reporting path.
        return ResponseResult(None, {}, b"", elapsed_since_ms(start), error="%s\n%s" % (err, traceback.format_exc()), replay_headers=replay_headers)


def elapsed_since_ms(start):
    current = time.perf_counter() if hasattr(time, "perf_counter") else time.time()
    return (current - start) * 1000


def load_json(body):
    try:
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        return json.loads(body), True
    except Exception:
        return None, False


def normalize_json(value):
    if isinstance(value, dict):
        return OrderedDict(
            (key, normalize_json(value[key]))
            for key in sorted(value.keys())
            if key not in VOLATILE_KEYS
        )
    if isinstance(value, list):
        return [normalize_json(item) for item in value]
    return value


def classify(new, old, absolute_tolerance=1e-9, relative_tolerance=1e-6):
    if new.error == "new_response_missing":
        return CompareResult(False, "new_response_missing", "new", "missing captured new response")
    if new.error:
        return CompareResult(False, "new_request_failed", "new", new.error)
    if old.error:
        return CompareResult(False, "old_request_failed", "old", old.error)
    if is_http_failure(new):
        return CompareResult(False, "new_request_failed", "new.status_code", "new status %s" % new.status_code)
    if is_http_failure(old):
        return CompareResult(False, "old_request_failed", "old.status_code", "old status %s" % old.status_code)
    if new.status_code != old.status_code:
        return CompareResult(False, "status_code_mismatch", "status_code", "%s != %s" % (new.status_code, old.status_code))

    new_json, new_ok = load_json(new.body)
    old_json, old_ok = load_json(old.body)
    if new_ok != old_ok:
        return CompareResult(False, "json_decode_mismatch", "body", "one response is not valid JSON")
    if new_ok and old_ok:
        metric_diff = metric_result_diff(new_json, old_json, absolute_tolerance, relative_tolerance)
        if metric_diff:
            return metric_diff
        presentation_diff = series_presentation_diff(new_json, old_json)
        if presentation_diff:
            return presentation_diff

        normalized_new = normalize_json(new_json)
        normalized_old = normalize_json(old_json)
        diff = first_json_diff(normalized_new, normalized_old, "body")
        if diff is None:
            return CompareResult(True)
        attribution = "body_mismatch"
        if diff[0] == "body":
            attribution = "top_level_schema_mismatch" if "keys differ" in diff[1] else "body_mismatch"
        if "error" in diff[0] or "message" in diff[0] or "errors" in diff[0] or "trace" in diff[0]:
            attribution = "error_message_mismatch"
        return CompareResult(False, attribution, diff[0], diff[1])

    if new.body != old.body:
        return CompareResult(False, "body_mismatch", "body", "non-json response bodies differ")
    return CompareResult(True)


def is_http_failure(resp):
    return resp.status_code is not None and resp.status_code >= 500


def first_json_diff(left, right, path):
    if type(left) is not type(right):
        return path, "type %s != %s" % (type(left).__name__, type(right).__name__)
    if isinstance(left, dict):
        left_keys = set(left.keys())
        right_keys = set(right.keys())
        if left_keys != right_keys:
            return path, "keys differ, missing_left=%s, missing_right=%s" % (
                sorted(right_keys - left_keys),
                sorted(left_keys - right_keys),
            )
        for key in sorted(left_keys):
            diff = first_json_diff(left[key], right[key], "%s.%s" % (path, key))
            if diff is not None:
                return diff
        return None
    if isinstance(left, list):
        if len(left) != len(right):
            return path, "length %s != %s" % (len(left), len(right))
        for idx, pair in enumerate(zip(left, right)):
            diff = first_json_diff(pair[0], pair[1], "%s[%s]" % (path, idx))
            if diff is not None:
                return diff
        return None
    if left != right:
        return path, "%r != %r" % (left, right)
    return None


def metric_result_diff(new_json, old_json, absolute_tolerance, relative_tolerance):
    new_series = find_series(new_json)
    old_series = find_series(old_json)
    if new_series is None or old_series is None:
        return None
    if len(new_series) != len(old_series):
        return CompareResult(False, "series_count_mismatch", "body.data.result", "%s != %s" % (len(new_series), len(old_series)))

    new_sorted = sorted(new_series, key=series_key)
    old_sorted = sorted(old_series, key=series_key)
    for idx, pair in enumerate(zip(new_sorted, old_sorted)):
        new_item, old_item = pair
        if series_labels(new_item) != series_labels(old_item):
            return CompareResult(False, "label_set_mismatch", "body.data.result[%s].metric" % idx, "%r != %r" % (series_labels(new_item), series_labels(old_item)))

        new_points = series_points(new_item)
        old_points = series_points(old_item)
        if new_points is None or old_points is None:
            continue
        if len(new_points) != len(old_points):
            return CompareResult(False, "datapoint_count_mismatch", "body.data.result[%s].values" % idx, "%s != %s" % (len(new_points), len(old_points)))
        for point_idx, point_pair in enumerate(zip(new_points, old_points)):
            new_point, old_point = point_pair
            if len(new_point) < 2 or len(old_point) < 2:
                continue
            if new_point[0] != old_point[0]:
                return CompareResult(False, "timestamp_mismatch", "body.data.result[%s].values[%s][0]" % (idx, point_idx), "%r != %r" % (new_point[0], old_point[0]))
            if not values_equal(new_point[1], old_point[1], absolute_tolerance, relative_tolerance):
                return CompareResult(False, "datapoint_value_mismatch", "body.data.result[%s].values[%s][1]" % (idx, point_idx), "new=%r old=%r" % (new_point[1], old_point[1]))
    return None


def series_presentation_diff(new_json, old_json):
    new_series = find_series(new_json)
    old_series = find_series(old_json)
    if new_series is None or old_series is None:
        return None
    if first_json_diff(normalize_json(new_json), normalize_json(old_json), "body") is None:
        return None
    if first_json_diff(strip_series_lists(new_json), strip_series_lists(old_json), "body") is not None:
        return None
    return CompareResult(
        True,
        "series_presentation_equal",
        "body.series",
        "series are semantically equal after normalizing series/group_keys order and float precision",
    )


def strip_series_lists(value):
    if isinstance(value, dict):
        stripped = OrderedDict()
        for key in sorted(value.keys()):
            if key in VOLATILE_KEYS:
                continue
            if key in ("series", "result", "list") and isinstance(value[key], list):
                stripped[key] = "__SERIES__"
            else:
                stripped[key] = strip_series_lists(value[key])
        return stripped
    if isinstance(value, list):
        return [strip_series_lists(item) for item in value]
    return value


def find_series(value):
    paths = [
        ("data", "result"),
        ("data", "series"),
        ("series",),
        ("list",),
    ]
    for path in paths:
        current = value
        ok = True
        for key in path:
            if not isinstance(current, dict) or key not in current:
                ok = False
                break
            current = current[key]
        if ok and isinstance(current, list):
            return current
    return None


def series_labels(item):
    if not isinstance(item, dict):
        return {}
    group_keys = item.get("group_keys")
    group_values = item.get("group_values")
    if isinstance(group_keys, list) and isinstance(group_values, list) and len(group_keys) == len(group_values):
        return OrderedDict(
            (str(key), normalize_json(group_values[idx]))
            for idx, key in sorted(enumerate(group_keys), key=lambda pair: str(pair[1]))
        )
    for key in ("metric", "dimensions", "labels", "target"):
        value = item.get(key)
        if isinstance(value, dict):
            return normalize_json(value)
    return {}


def series_key(item):
    return json.dumps(series_labels(item), ensure_ascii=False, sort_keys=True)


def series_points(item):
    if not isinstance(item, dict):
        return None
    for key in ("values", "datapoints", "points"):
        value = item.get(key)
        if isinstance(value, list):
            return value
    return None


def values_equal(left, right, absolute_tolerance, relative_tolerance):
    if left == right:
        return True
    try:
        left_f = float(left)
        right_f = float(right)
    except (TypeError, ValueError):
        return False
    diff = abs(left_f - right_f)
    if diff <= absolute_tolerance:
        return True
    return diff <= max(abs(left_f), abs(right_f)) * relative_tolerance


class Correlator(object):
    def __init__(self, new_response_timeout=30.0, max_pending=4096):
        self.new_response_timeout = new_response_timeout
        self.max_pending = max_pending
        self.pending = OrderedDict()

    def add(self, record):
        now = time.time()
        if record.kind == "request":
            self.pending[record.gor_id] = (now, record.request)
            self.pending.move_to_end(record.gor_id)
            expired = []
            while len(self.pending) > self.max_pending:
                _, item = self.pending.popitem(last=False)
                expired.append(self._missing_pair(item[1]))
            return expired
        if record.kind == "response" and record.gor_id in self.pending:
            _, request = self.pending.pop(record.gor_id)
            return [WorkItem(request, record.response)]
        return []

    def expire(self):
        now = time.time()
        expired = []
        for gor_id, item in list(self.pending.items()):
            timestamp, request = item
            if now - timestamp >= self.new_response_timeout:
                self.pending.pop(gor_id, None)
                expired.append(self._missing_pair(request))
        return expired

    def _missing_pair(self, request):
        return WorkItem(request, ResponseResult(None, {}, b"", self.new_response_timeout * 1000, error="new_response_missing"))


class Reporter(object):
    def __init__(self, report_dir, recent_limit=50, mismatch_body_limit=4096, failure_body_limit=65536):
        self.report_dir = Path(report_dir)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.recent_limit = recent_limit
        self.mismatch_body_limit = mismatch_body_limit
        self.failure_body_limit = failure_body_limit
        self.started_at = now_iso()
        self.lock = threading.Lock()
        self.stats = {
            "seen": 0,
            "eligible": 0,
            "skipped_replay_tag": 0,
            "skipped_path": 0,
            "skipped_sample": 0,
            "new_response_missing": 0,
            "replayed": 0,
            "matched": 0,
            "mismatched": 0,
            "request_failed": 0,
            "new_request_failed": 0,
            "old_request_failed": 0,
            "dropped_queue_full": 0,
        }
        self.attribution_counts = {}
        self.semantic_equal_counts = {}
        self.new_latencies = []
        self.old_latencies = []
        self.recent_mismatches = []
        self.recent_failures = []

    def increment(self, key, amount=1):
        with self.lock:
            self.stats[key] = self.stats.get(key, 0) + amount

    def record_pair(self, request, new_resp, old_resp, cmp_result):
        with self.lock:
            self.stats["replayed"] += 1
            if new_resp.elapsed_ms is not None:
                self.new_latencies.append(new_resp.elapsed_ms)
            if old_resp.elapsed_ms is not None:
                self.old_latencies.append(old_resp.elapsed_ms)

            failure = failure_info(new_resp, old_resp, cmp_result)
            if failure:
                self.stats["request_failed"] += 1
                if failure["side"] in ("new", "both"):
                    self.stats["new_request_failed"] += 1
                if failure["side"] in ("old", "both"):
                    self.stats["old_request_failed"] += 1
                if cmp_result.attribution == "new_response_missing":
                    self.stats["new_response_missing"] += 1
                self._write_failure(request, new_resp, old_resp, cmp_result, failure)
                return

            if cmp_result.equal:
                self.stats["matched"] += 1
                if cmp_result.attribution:
                    self.semantic_equal_counts[cmp_result.attribution] = self.semantic_equal_counts.get(cmp_result.attribution, 0) + 1
                return

            self.stats["mismatched"] += 1
            self.attribution_counts[cmp_result.attribution] = self.attribution_counts.get(cmp_result.attribution, 0) + 1
            self._write_mismatch(request, new_resp, old_resp, cmp_result)

    def _write_mismatch(self, request, new_resp, old_resp, cmp_result):
        payload = self._base_payload(request, self.mismatch_body_limit)
        payload.update({
            "attribution": cmp_result.attribution,
            "diff_path": cmp_result.diff_path,
            "diff_message": cmp_result.message,
            "new": response_summary(new_resp),
            "old": response_summary(old_resp),
        })
        append_ndjson(self._dated_path("mismatch"), payload)
        self.recent_mismatches.append({
            "time": payload["time"],
            "trace_id": payload["trace_id"],
            "path": payload["path"],
            "attribution": cmp_result.attribution,
            "diff_path": cmp_result.diff_path,
            "request_sha256": payload["request_sha256"],
            "request_preview": payload["request_preview"],
        })
        self.recent_mismatches = self.recent_mismatches[-self.recent_limit:]

    def _write_failure(self, request, new_resp, old_resp, cmp_result, failure):
        payload = self._base_payload(request, self.failure_body_limit)
        payload.update({
            "attribution": cmp_result.attribution,
            "failure_side": failure["side"],
            "failure_type": failure["type"],
            "failure_message": cmp_result.message,
            "new": response_summary(new_resp),
            "old": response_summary(old_resp),
        })
        append_ndjson(self._dated_path("failure"), payload)
        self.recent_failures.append({
            "time": payload["time"],
            "trace_id": payload["trace_id"],
            "path": payload["path"],
            "attribution": cmp_result.attribution,
            "failure_side": failure["side"],
            "failure_type": failure["type"],
            "request_sha256": payload["request_sha256"],
            "request_preview": payload["request_preview"],
        })
        self.recent_failures = self.recent_failures[-self.recent_limit:]

    def _base_payload(self, request, body_limit):
        trace = extract_trace_id(request)
        body = encode_request_body(request.body, body_limit)
        payload = {
            "time": now_iso(),
            "gor_id": request.gor_id,
            "trace_id": trace.value,
            "trace_id_source": trace.source,
            "method": request.method,
            "path": request.path,
            "request_sha256": sha256_hex(request.body),
            "request_preview": preview_body(request.body, 512),
        }
        payload.update(body)
        return payload

    def _dated_path(self, prefix):
        day = datetime.datetime.now().strftime("%Y%m%d")
        return self.report_dir / ("%s-%s.ndjson" % (prefix, day))

    def flush(self):
        with self.lock:
            summary = self.summary_locked()
        write_json_atomic(summary, self.report_dir / "summary.json")
        write_text_atomic(render_markdown_summary(summary), self.report_dir / "summary.md")

    def summary_locked(self):
        return {
            "started_at": self.started_at,
            "updated_at": now_iso(),
            **self.stats,
            "attribution_counts": dict(sorted(self.attribution_counts.items(), key=lambda item: (-item[1], item[0]))),
            "semantic_equal_counts": dict(sorted(self.semantic_equal_counts.items(), key=lambda item: (-item[1], item[0]))),
            "latency_ms": {
                "new_p50": percentile(self.new_latencies, 0.50),
                "new_p95": percentile(self.new_latencies, 0.95),
                "old_p50": percentile(self.old_latencies, 0.50),
                "old_p95": percentile(self.old_latencies, 0.95),
            },
            "recent_mismatches": list(self.recent_mismatches),
            "recent_failures": list(self.recent_failures),
        }


def failure_info(new_resp, old_resp, cmp_result):
    new_failed = bool(new_resp.error) or is_http_failure(new_resp)
    old_failed = bool(old_resp.error) or is_http_failure(old_resp)
    if not new_failed and not old_failed:
        return None
    if new_failed and old_failed:
        side = "both"
    elif new_failed:
        side = "new"
    else:
        side = "old"
    if (new_resp.error or old_resp.error) == "new_response_missing":
        failure_type = "new_response_missing"
    elif new_resp.error or old_resp.error:
        failure_type = "transport_error"
    else:
        failure_type = "http_5xx"
    return {"side": side, "type": failure_type}


def response_summary(resp):
    replay_headers = lower_headers(resp.replay_headers)
    return {
        "status_code": resp.status_code,
        "elapsed_ms": round(resp.elapsed_ms, 3) if resp.elapsed_ms is not None else None,
        "body_size": len(resp.body),
        "body_sha256": sha256_hex(resp.body),
        "body_preview": preview_body(resp.body, 512),
        "error": resp.error,
        "replay_request_id": replay_headers.get("x-request-id"),
        "replay_bkapi_trace_id": replay_headers.get("x-bkapi-trace-id"),
        "replay_traceparent": replay_headers.get("traceparent"),
    }


def append_ndjson(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def write_json_atomic(payload, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def write_text_atomic(text, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * pct))
    return round(ordered[idx], 3)


def render_markdown_summary(summary):
    lines = [
        "# UnifyQuery Live Shadow Summary",
        "",
        "- Started: `%s`" % summary["started_at"],
        "- Updated: `%s`" % summary["updated_at"],
        "- Seen: `%s`" % summary["seen"],
        "- Eligible: `%s`" % summary["eligible"],
        "- Replayed: `%s`" % summary["replayed"],
        "- Matched: `%s`" % summary["matched"],
        "- Mismatched: `%s`" % summary["mismatched"],
        "- Request failed: `%s`" % summary["request_failed"],
        "- Skipped replay tag: `%s`" % summary["skipped_replay_tag"],
        "- Skipped path: `%s`" % summary["skipped_path"],
        "- Skipped sample: `%s`" % summary["skipped_sample"],
        "",
        "## Attribution Counts",
        "",
    ]
    if not summary["attribution_counts"]:
        lines.append("No mismatches yet.")
    else:
        for key, count in summary["attribution_counts"].items():
            lines.append("- `%s`: `%s`" % (key, count))

    lines.extend(["", "## Semantic Equal Counts", ""])
    if not summary.get("semantic_equal_counts"):
        lines.append("No semantic-equal differences yet.")
    else:
        for key, count in summary["semantic_equal_counts"].items():
            lines.append("- `%s`: `%s`" % (key, count))

    lines.extend(["", "## Recent Mismatches", ""])
    if not summary["recent_mismatches"]:
        lines.append("No recent mismatches.")
    else:
        for item in summary["recent_mismatches"]:
            lines.append("- `%s` `%s` `%s` `%s` `%s`" % (
                item["time"],
                item["trace_id"],
                item["path"],
                item["attribution"],
                item["diff_path"],
            ))

    lines.extend(["", "## Recent Failures", ""])
    if not summary["recent_failures"]:
        lines.append("No recent failures.")
    else:
        for item in summary["recent_failures"]:
            lines.append("- `%s` `%s` `%s` `%s` `%s`" % (
                item["time"],
                item["trace_id"],
                item["path"],
                item["attribution"],
                item["failure_side"],
            ))

    return "\n".join(lines) + "\n"


def replay_work_item(item, args):
    request = item.request
    replay_headers = build_replay_headers(request.headers)
    if args.new_mode == "replay":
        with ThreadPoolExecutor(max_workers=2) as executor:
            new_future = executor.submit(send_request, request, args.new_base, args.new_timeout, replay_headers)
            old_future = executor.submit(send_request, request, args.old_base, args.old_timeout, replay_headers)
            new_resp = new_future.result()
            old_resp = old_future.result()
    else:
        new_resp = item.new_response
        old_resp = send_request(request, args.old_base, args.old_timeout, replay_headers)
    cmp_result = classify(new_resp, old_resp, args.absolute_tolerance, args.relative_tolerance)
    return request, new_resp, old_resp, cmp_result


def worker_loop(work_queue, reporter, args, stop_event):
    while not stop_event.is_set() or not work_queue.empty():
        try:
            item = work_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            request, new_resp, old_resp, cmp_result = replay_work_item(item, args)
            reporter.record_pair(request, new_resp, old_resp, cmp_result)
        finally:
            work_queue.task_done()


def enqueue_item(work_queue, reporter, item):
    try:
        work_queue.put_nowait(item)
        return True
    except queue.Full:
        reporter.increment("dropped_queue_full")
        return False


def process_record(record, args, reporter, work_queue, correlator):
    if record.kind == "request":
        reporter.increment("seen")
        reason = should_skip_request(record.request, args.allow_path)
        if reason:
            reporter.increment("skipped_%s" % reason)
            return
        if args.sample_rate < 1.0 and random.random() > args.sample_rate:
            reporter.increment("skipped_sample")
            return
        reporter.increment("eligible")
        if args.new_mode == "replay":
            enqueue_item(work_queue, reporter, WorkItem(record.request))
        else:
            for item in correlator.add(record):
                enqueue_item(work_queue, reporter, item)
        return

    if record.kind == "response" and args.new_mode == "capture-response":
        for item in correlator.add(record):
            enqueue_item(work_queue, reporter, item)


def process_stream(stream, args, reporter, work_queue, stop_event):
    correlator = Correlator(args.new_response_timeout, args.max_pending)
    last_flush = time.time()
    for raw in iter_goreplay_records(stream):
        if stop_event.is_set():
            break
        try:
            record = parse_goreplay_record(raw)
        except ValueError:
            continue
        process_record(record, args, reporter, work_queue, correlator)
        if args.new_mode == "capture-response":
            for item in correlator.expire():
                enqueue_item(work_queue, reporter, item)
        if time.time() - last_flush >= args.flush_interval:
            reporter.flush()
            last_flush = time.time()
    reporter.flush()


def validate_gor(args):
    try:
        proc = subprocess.Popen([args.gor_bin, "--help"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = proc.communicate(timeout=10)[0].decode("utf-8", errors="replace")
    except Exception as err:
        raise SystemExit("failed to run %s --help: %s" % (args.gor_bin, err))
    if "output-stdout" not in output:
        raise SystemExit("%s does not support --output-stdout" % args.gor_bin)
    if args.new_mode == "capture-response" and "input-raw-track-response" not in output:
        raise SystemExit("%s does not support --input-raw-track-response" % args.gor_bin)


def start_gor(args):
    command = [args.gor_bin, "--input-raw", ":%s" % args.listen_port, "--output-stdout"]
    if args.new_mode == "capture-response":
        command.insert(3, "--input-raw-track-response")
    return subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gor-bin", default="/tmp/gor")
    parser.add_argument("--listen-port", default="10206")
    parser.add_argument("--new-mode", choices=("replay", "capture-response"), default="replay")
    parser.add_argument("--new-base", default="http://127.0.0.1:10206")
    parser.add_argument("--old-base", default="http://127.0.0.1:10216")
    parser.add_argument("--report-dir", default="/data/bkee/bkmonitorv3/unify-query-old/shadow-reports/live")
    parser.add_argument("--allow-path", action="append", default=None)
    parser.add_argument("--sample-rate", type=float, default=1.0)
    parser.add_argument("--max-inflight", type=int, default=16)
    parser.add_argument("--max-pending", type=int, default=4096)
    parser.add_argument("--new-timeout", type=float, default=30.0)
    parser.add_argument("--new-response-timeout", type=float, default=30.0)
    parser.add_argument("--old-timeout", type=float, default=30.0)
    parser.add_argument("--flush-interval", type=float, default=10.0)
    parser.add_argument("--recent-limit", type=int, default=50)
    parser.add_argument("--mismatch-request-body-limit", type=int, default=4096)
    parser.add_argument("--failure-request-body-limit", type=int, default=65536)
    parser.add_argument("--absolute-tolerance", type=float, default=1e-9)
    parser.add_argument("--relative-tolerance", type=float, default=1e-6)
    parser.add_argument("--latency-ratio-threshold", type=float, default=2.0)
    parser.add_argument("--record-latency-only", action="store_true")
    parser.add_argument("--duration", type=float, default=0.0, help="seconds to run; 0 means forever")
    parser.add_argument("--input-file", default="", help="read GoReplay records from file instead of starting gor")
    args = parser.parse_args(argv)
    if args.allow_path is None:
        args.allow_path = ["/query/ts"]
    return args


def main(argv=None):
    args = parse_args(argv)
    reporter = Reporter(
        args.report_dir,
        recent_limit=args.recent_limit,
        mismatch_body_limit=args.mismatch_request_body_limit,
        failure_body_limit=args.failure_request_body_limit,
    )
    work_queue = queue.Queue(maxsize=max(args.max_inflight * 4, 1))
    stop_event = threading.Event()

    def stop_handler(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)

    workers = [
        threading.Thread(target=worker_loop, args=(work_queue, reporter, args, stop_event), daemon=True)
        for _ in range(max(args.max_inflight, 1))
    ]
    for worker in workers:
        worker.start()

    gor_proc = None
    timer = None

    try:
        if args.input_file:
            with open(args.input_file, "rb") as fp:
                process_stream(fp, args, reporter, work_queue, stop_event)
        else:
            validate_gor(args)
            gor_proc = start_gor(args)
            if args.duration > 0:
                def stop_gor():
                    stop_event.set()
                    if gor_proc and gor_proc.poll() is None:
                        gor_proc.terminate()

                timer = threading.Timer(args.duration, stop_gor)
                timer.daemon = True
                timer.start()
            process_stream(gor_proc.stdout, args, reporter, work_queue, stop_event)
    finally:
        stop_event.set()
        if timer:
            timer.cancel()
        if gor_proc and gor_proc.poll() is None:
            gor_proc.terminate()
            try:
                gor_proc.wait(timeout=5)
            except Exception:
                gor_proc.kill()
        work_queue.join()
        reporter.flush()

    summary = reporter.summary_locked()
    print("seen=%s eligible=%s replayed=%s matched=%s mismatched=%s request_failed=%s skipped_replay_tag=%s" % (
        summary["seen"],
        summary["eligible"],
        summary["replayed"],
        summary["matched"],
        summary["mismatched"],
        summary["request_failed"],
        summary["skipped_replay_tag"],
    ))
    print("summary=%s" % (Path(args.report_dir) / "summary.json"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
