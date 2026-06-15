import importlib.util
import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


SCRIPT = Path(__file__).with_name("compare_goreplay.py")


def load_module():
    spec = importlib.util.spec_from_file_location("compare_unifyquery_goreplay", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CompareUnifyQueryGoReplayTest(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()

    def write_payload(self, payload: bytes) -> Path:
        fp = tempfile.NamedTemporaryFile(delete=False)
        fp.write(payload)
        fp.close()
        return Path(fp.name)

    def test_parse_single_get_request(self):
        path = self.write_payload(
            b"1 123 456\n"
            b"GET /query/ts?x=1 HTTP/1.1\r\n"
            b"Host: old.example\r\n"
            b"X-Test: yes\r\n"
            b"\r\n"
            + self.mod.GOREPLAY_SEPARATOR
        )

        requests, skipped = self.mod.parse_goreplay_file(path)

        self.assertEqual(skipped, 0)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].method, "GET")
        self.assertEqual(requests[0].path, "/query/ts?x=1")
        self.assertEqual(requests[0].headers["X-Test"], "yes")
        self.assertEqual(requests[0].body, b"")

    def test_parse_post_json_and_skip_non_request_records(self):
        body = b'{"query":"a"}'
        path = self.write_payload(
            b"2 response 456\nHTTP/1.1 200 OK\r\n\r\nignored"
            + self.mod.GOREPLAY_SEPARATOR
            + b"1 request 789\n"
            + b"POST /query/ts HTTP/1.1\r\n"
            + b"Host: old.example\r\n"
            + b"Content-Type: application/json\r\n"
            + b"Content-Length: 13\r\n"
            + b"\r\n"
            + body
            + self.mod.GOREPLAY_SEPARATOR
            + b"bad record"
            + self.mod.GOREPLAY_SEPARATOR
        )

        requests, skipped = self.mod.parse_goreplay_file(path)

        self.assertEqual(skipped, 2)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].method, "POST")
        self.assertEqual(requests[0].path, "/query/ts")
        self.assertEqual(requests[0].body, body)
        self.assertNotIn("Content-Length", requests[0].headers)

    def test_compare_json_ignores_object_key_order_but_not_values(self):
        same = self.mod.compare_responses(
            self.mod.ResponseResult(status_code=200, body=b'{"b":2,"a":1}', elapsed_ms=1),
            self.mod.ResponseResult(status_code=200, body=b'{"a":1,"b":2}', elapsed_ms=1),
        )
        different = self.mod.compare_responses(
            self.mod.ResponseResult(status_code=200, body=b'{"a":1}', elapsed_ms=1),
            self.mod.ResponseResult(status_code=200, body=b'{"a":2}', elapsed_ms=1),
        )

        self.assertTrue(same.equal)
        self.assertFalse(different.equal)
        self.assertEqual(different.diff_path, "body.a")

    def test_compare_status_and_plain_text_strictly(self):
        status = self.mod.compare_responses(
            self.mod.ResponseResult(status_code=200, body=b"ok", elapsed_ms=1),
            self.mod.ResponseResult(status_code=500, body=b"ok", elapsed_ms=1),
        )
        text = self.mod.compare_responses(
            self.mod.ResponseResult(status_code=200, body=b"ok", elapsed_ms=1),
            self.mod.ResponseResult(status_code=200, body=b"OK", elapsed_ms=1),
        )

        self.assertFalse(status.equal)
        self.assertEqual(status.diff_path, "status_code")
        self.assertFalse(text.equal)
        self.assertEqual(text.diff_path, "body")

    def test_smoke_compare_two_mock_servers(self):
        left = start_server({"value": 1})
        right = start_server({"value": 2})
        try:
            request = self.mod.ReplayRequest(
                index=1,
                method="POST",
                path="/query/ts",
                headers={"Content-Type": "application/json", "Host": "old.example"},
                body=b'{"query":"a"}',
            )
            item = self.mod.compare_one(request, left.url, right.url, timeout=2)

            self.assertFalse(item["equal"])
            self.assertEqual(item["diff_path"], "body.value")
            self.assertEqual(item["left"]["status_code"], 200)
            self.assertEqual(item["right"]["status_code"], 200)
        finally:
            left.stop()
            right.stop()


class ServerHandle:
    def __init__(self, server):
        self.server = server
        self.thread = threading.Thread(target=server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{server.server_port}"

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()


def start_server(payload):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            _ = self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
            data = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt, *args):
            return

    return ServerHandle(HTTPServer(("127.0.0.1", 0), Handler))


if __name__ == "__main__":
    unittest.main()
