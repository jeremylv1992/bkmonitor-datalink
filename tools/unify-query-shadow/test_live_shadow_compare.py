#!/usr/bin/env python3
# Tencent is pleased to support the open source community by making
# 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
# Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
# Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://opensource.org/licenses/MIT
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from __future__ import print_function

import importlib.util
import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


SCRIPT = Path(__file__).with_name("live_shadow_compare.py")


def load_module():
    spec = importlib.util.spec_from_file_location("live_shadow_compare", str(SCRIPT))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class LiveShadowCompareTest(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()

    def sample_request(self, headers=None, body=None):
        return self.mod.ReplayRequest(
            index=1,
            gor_id="abc",
            method="POST",
            path="/query/ts",
            headers=headers or {"Content-Type": "application/json"},
            body=body if body is not None else b'{"query_list":[{"metric":"cpu"}]}',
        )

    def sample_response(self, body=None, status=200):
        return self.mod.ResponseResult(
            status_code=status,
            headers={"Content-Type": "application/json"},
            body=body if body is not None else b'{"ok":true}',
            elapsed_ms=1.0,
        )

    def test_parse_goreplay_request_record(self):
        raw = (
            b"1 123 456\n"
            b"POST /query/ts HTTP/1.1\r\n"
            b"Host: monitor\r\n"
            b"Content-Type: application/json\r\n"
            b"\r\n"
            b'{"a":1}'
        )

        record = self.mod.parse_goreplay_record(raw)

        self.assertEqual(record.kind, "request")
        self.assertEqual(record.gor_id, "123")
        self.assertEqual(record.request.method, "POST")
        self.assertEqual(record.request.path, "/query/ts")
        self.assertEqual(record.request.body, b'{"a":1}')

    def test_parse_goreplay_response_record(self):
        raw = (
            b"2 123 789 32100000\n"
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/json\r\n"
            b"\r\n"
            b'{"ok":true}'
        )

        record = self.mod.parse_goreplay_record(raw)

        self.assertEqual(record.kind, "response")
        self.assertEqual(record.gor_id, "123")
        self.assertEqual(record.response.status_code, 200)
        self.assertEqual(record.response.body, b'{"ok":true}')
        self.assertAlmostEqual(record.response.elapsed_ms, 32.1)

    def test_skip_shadow_replay_header(self):
        req = self.sample_request(headers={"X-UnifyQuery-Shadow-Replay": "1"})

        reason = self.mod.should_skip_request(req, allow_paths=["/query/ts"])

        self.assertEqual(reason, "replay_tag")

    def test_allow_path_argument_overrides_default(self):
        args = self.mod.parse_args(["--allow-path", "/query/ts?codex=1"])

        self.assertEqual(args.allow_path, ["/query/ts?codex=1"])

    def test_extract_trace_id_from_header_then_body_then_hash(self):
        header_req = self.sample_request(
            headers={"X-Bkapi-Trace-Id": "trace-from-header"},
            body=b'{"trace_id":"trace-from-body"}',
        )
        body_req = self.sample_request(headers={}, body=b'{"trace_id":"trace-from-body"}')
        hash_req = self.sample_request(headers={}, body=b"{}")

        self.assertEqual(self.mod.extract_trace_id(header_req).value, "trace-from-header")
        self.assertEqual(self.mod.extract_trace_id(body_req).value, "trace-from-body")
        self.assertEqual(self.mod.extract_trace_id(hash_req).source, "generated:request_sha256")

    def test_encode_request_body_keeps_failed_request_body(self):
        body = b'{"query_list":[{"metric":"cpu"}]}'

        encoded = self.mod.encode_request_body(body, limit=65536)

        self.assertEqual(encoded["request_body_encoding"], "utf-8")
        self.assertEqual(encoded["request_body"], '{"query_list":[{"metric":"cpu"}]}')
        self.assertFalse(encoded["request_body_truncated"])

    def test_correlator_emits_pair_after_request_and_response(self):
        corr = self.mod.Correlator(new_response_timeout=1.0)
        req = self.mod.RawRecord(kind="request", gor_id="abc", request=self.sample_request())
        resp = self.mod.RawRecord(kind="response", gor_id="abc", response=self.sample_response())

        self.assertEqual(corr.add(req), [])
        pairs = corr.add(resp)

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0].request.path, "/query/ts")
        self.assertEqual(pairs[0].new_response.status_code, 200)

    def test_replay_to_new_and_old_adds_shadow_header(self):
        new_server = start_server({"side": "new"})
        old_server = start_server({"side": "old"})
        try:
            args = Args(
                new_mode="replay",
                new_base=new_server.url,
                old_base=old_server.url,
                new_timeout=2,
                old_timeout=2,
                absolute_tolerance=1e-9,
                relative_tolerance=1e-6,
            )

            request, new_resp, old_resp, cmp_result = self.mod.replay_work_item(
                self.mod.WorkItem(self.sample_request()),
                args,
            )

            self.assertEqual(request.path, "/query/ts")
            self.assertEqual(new_server.received_path, "/query/ts")
            self.assertEqual(old_server.received_path, "/query/ts")
            self.assertEqual(new_server.received_headers["X-Unifyquery-Shadow-Replay"], "1")
            self.assertEqual(old_server.received_headers["X-Unifyquery-Shadow-Replay"], "1")
            self.assertEqual(new_resp.status_code, 200)
            self.assertEqual(old_resp.status_code, 200)
            self.assertFalse(cmp_result.equal)
            self.assertEqual(cmp_result.attribution, "body_mismatch")
        finally:
            new_server.stop()
            old_server.stop()

    def test_replay_to_new_and_old_uses_same_generated_request_ids(self):
        new_server = start_server({"side": "new"})
        old_server = start_server({"side": "old"})
        try:
            args = Args(
                new_mode="replay",
                new_base=new_server.url,
                old_base=old_server.url,
                new_timeout=2,
                old_timeout=2,
                absolute_tolerance=1e-9,
                relative_tolerance=1e-6,
            )

            self.mod.replay_work_item(
                self.mod.WorkItem(
                    self.sample_request(
                        headers={
                            "Content-Type": "application/json",
                            "X-Request-Id": "original-request",
                            "X-Bkapi-Trace-Id": "original-bkapi",
                        }
                    )
                ),
                args,
            )

            new_req_id = new_server.received_headers["X-Request-Id"]
            old_req_id = old_server.received_headers["X-Request-Id"]
            self.assertEqual(new_req_id, old_req_id)
            self.assertNotEqual(new_req_id, "original-request")
            self.assertEqual(new_server.received_headers["X-Bkapi-Trace-Id"], new_req_id)
            self.assertEqual(old_server.received_headers["X-Bkapi-Trace-Id"], new_req_id)
        finally:
            new_server.stop()
            old_server.stop()

    def test_replay_headers_replace_original_trace_context(self):
        original = {
            "Content-Type": "application/json",
            "Traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
            "Tracestate": "vendor=value",
            "X-Bkapi-Trace-Id": "original-bkapi-trace",
            "X-Request-Id": "original-request",
        }

        first = self.mod.build_replay_headers(original)
        second = self.mod.build_replay_headers(original)

        self.assertEqual(first["Content-Type"], "application/json")
        self.assertEqual(first["X-UnifyQuery-Shadow-Replay"], "1")
        self.assertNotEqual(first["Traceparent"], original["Traceparent"])
        self.assertNotEqual(second["Traceparent"], first["Traceparent"])
        self.assertNotIn("Tracestate", first)
        self.assertIn("X-Bkapi-Trace-Id", first)
        self.assertIn("X-Request-Id", first)
        self.assertEqual(first["X-Bkapi-Trace-Id"], first["X-Request-Id"])
        self.assertNotEqual(first["X-Request-Id"], "original-request")
        self.assertNotEqual(first["X-Bkapi-Trace-Id"], "original-bkapi-trace")
        self.assertRegex(first["Traceparent"], r"^00-[0-9a-f]{32}-[0-9a-f]{16}-01$")

    def test_classify_series_presentation_only_difference_as_semantic_equal(self):
        new = self.sample_response(body=json.dumps({
            "series": [
                {
                    "group_keys": ["b", "a"],
                    "group_values": ["2", "1"],
                    "values": [[1, 1.23456789]],
                },
            ],
        }).encode("utf-8"))
        old = self.sample_response(body=json.dumps({
            "series": [
                {
                    "group_keys": ["a", "b"],
                    "group_values": ["1", "2"],
                    "values": [[1, 1.2345678900001]],
                },
            ],
        }).encode("utf-8"))

        case = self.mod.classify(new, old)

        self.assertTrue(case.equal)
        self.assertEqual(case.attribution, "series_presentation_equal")
        self.assertEqual(case.diff_path, "body.series")

    def test_classify_status_code_mismatch(self):
        case = self.mod.classify(self.sample_response(status=200), self.sample_response(status=404))

        self.assertFalse(case.equal)
        self.assertEqual(case.attribution, "status_code_mismatch")

    def test_classify_series_count_mismatch(self):
        new = self.sample_response(body=b'{"data":{"result":[{"metric":{"a":"1"},"values":[[1,"2"]]}]}}')
        old = self.sample_response(body=b'{"data":{"result":[]}}')

        case = self.mod.classify(new, old)

        self.assertFalse(case.equal)
        self.assertEqual(case.attribution, "series_count_mismatch")

    def test_classify_datapoint_value_mismatch(self):
        new = self.sample_response(body=b'{"data":{"result":[{"metric":{"a":"1"},"values":[[1,"2"]]}]}}')
        old = self.sample_response(body=b'{"data":{"result":[{"metric":{"a":"1"},"values":[[1,"3"]]}]}}')

        case = self.mod.classify(new, old)

        self.assertFalse(case.equal)
        self.assertEqual(case.attribution, "datapoint_value_mismatch")

    def test_reporter_writes_mismatch_ndjson_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            reporter = self.mod.Reporter(report_dir=tmp, recent_limit=2)
            req = self.sample_request(headers={"X-Bkapi-Trace-Id": "trace-001"})
            new = self.sample_response(body=b'{"a":1}')
            old = self.sample_response(body=b'{"a":2}')
            cmp_result = self.mod.classify(new, old)

            reporter.record_pair(req, new, old, cmp_result)
            reporter.flush()

            self.assertTrue((Path(tmp) / "summary.json").exists())
            mismatch_files = list(Path(tmp).glob("mismatch-*.ndjson"))
            self.assertEqual(len(mismatch_files), 1)
            payload = json.loads(mismatch_files[0].read_text().splitlines()[0])
            self.assertEqual(payload["trace_id"], "trace-001")
            self.assertEqual(payload["attribution"], "body_mismatch")

    def test_reporter_writes_failure_with_trace_id_and_request_body(self):
        with tempfile.TemporaryDirectory() as tmp:
            reporter = self.mod.Reporter(report_dir=tmp, recent_limit=2)
            req = self.sample_request(
                headers={"X-Bkapi-Trace-Id": "trace-001"},
                body=b'{"query_list":[{"metric":"cpu"}]}',
            )
            new = self.sample_response(body=b'{"ok":true}')
            old = self.mod.ResponseResult(None, {}, b"", 30000.0, error="timed out")
            cmp_result = self.mod.classify(new, old)

            reporter.record_pair(req, new, old, cmp_result)
            reporter.flush()

            failure_files = list(Path(tmp).glob("failure-*.ndjson"))
            self.assertEqual(len(failure_files), 1)
            payload = json.loads(failure_files[0].read_text().splitlines()[0])
            self.assertEqual(payload["trace_id"], "trace-001")
            self.assertEqual(payload["request_body"], '{"query_list":[{"metric":"cpu"}]}')
            self.assertEqual(payload["failure_side"], "old")


class Args(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class ServerHandle(object):
    def __init__(self, server, state):
        self.server = server
        self.state = state
        self.thread = threading.Thread(target=server.serve_forever)
        self.thread.daemon = True
        self.thread.start()
        self.url = "http://127.0.0.1:%s" % server.server_port

    @property
    def received_path(self):
        return self.state.get("path")

    @property
    def received_headers(self):
        return self.state.get("headers", {})

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()


def start_server(payload):
    state = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            state["path"] = self.path
            state["headers"] = dict(self.headers.items())
            _ = self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
            data = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt, *args):
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    return ServerHandle(server, state)


if __name__ == "__main__":
    unittest.main()
