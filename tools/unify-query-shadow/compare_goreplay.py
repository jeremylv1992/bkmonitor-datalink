#!/usr/bin/env python3
"""Replay GoReplay requests against two UnifyQuery endpoints and compare responses."""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen


GOREPLAY_SEPARATOR = "\n🐵🙈🙉\n".encode("utf-8")
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


@dataclass
class ReplayRequest:
    index: int
    method: str
    path: str
    headers: Dict[str, str]
    body: bytes


@dataclass
class ResponseResult:
    status_code: Optional[int]
    body: bytes
    elapsed_ms: float
    error: Optional[str] = None


@dataclass
class CompareResult:
    equal: bool
    diff_path: str = ""
    message: str = ""


def parse_goreplay_file(path: Path) -> Tuple[List[ReplayRequest], int]:
    payload = path.read_bytes()
    records = payload.split(GOREPLAY_SEPARATOR)
    requests: List[ReplayRequest] = []
    skipped = 0

    for raw in records:
        if not raw.strip(b"\r\n"):
            continue
        request_payload = extract_request_payload(raw)
        if request_payload is None:
            skipped += 1
            continue
        try:
            requests.append(parse_raw_http_request(len(requests) + 1, request_payload))
        except ValueError:
            skipped += 1

    return requests, skipped


def extract_request_payload(record: bytes) -> Optional[bytes]:
    if looks_like_http_request(record):
        return record

    first_line, sep, rest = record.partition(b"\n")
    if not sep:
        return None
    parts = first_line.decode("utf-8", errors="replace").split()
    if not parts or parts[0] != "1":
        return None
    if not looks_like_http_request(rest):
        return None
    return rest


def looks_like_http_request(raw: bytes) -> bool:
    line = raw.splitlines()[0].decode("latin-1", errors="replace") if raw.splitlines() else ""
    parts = line.split()
    return len(parts) >= 2 and parts[0].upper() in REQUEST_METHODS


def parse_raw_http_request(index: int, raw: bytes) -> ReplayRequest:
    header_bytes, sep, body = raw.partition(b"\r\n\r\n")
    if not sep:
        header_bytes, sep, body = raw.partition(b"\n\n")
    if not sep:
        raise ValueError("missing HTTP header/body separator")

    lines = header_bytes.decode("latin-1").splitlines()
    if not lines:
        raise ValueError("missing request line")

    request_line = lines[0].split()
    if len(request_line) < 2:
        raise ValueError("invalid request line")
    method = request_line[0].upper()
    path = request_line[1]
    if method not in REQUEST_METHODS:
        raise ValueError("unsupported request method")
    if not path.startswith("/"):
        parsed = urlsplit(path)
        path = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))

    headers: Dict[str, str] = {}
    for line in lines[1:]:
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        if key.lower() in HOP_BY_HOP_HEADERS:
            continue
        headers[key] = value.strip()

    return ReplayRequest(index=index, method=method, path=path, headers=headers, body=body)


def build_url(base: str, original_path: str) -> str:
    base = base.rstrip("/") + "/"
    path = original_path.lstrip("/")
    return urljoin(base, path)


def send_request(replay: ReplayRequest, base_url: str, timeout: float) -> ResponseResult:
    url = build_url(base_url, replay.path)
    start = time.perf_counter()
    req = Request(url=url, data=replay.body if replay.method not in {"GET", "HEAD"} else None, method=replay.method)
    for key, value in replay.headers.items():
        if key.lower() in HOP_BY_HOP_HEADERS:
            continue
        req.add_header(key, value)

    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            elapsed_ms = (time.perf_counter() - start) * 1000
            return ResponseResult(status_code=resp.status, body=body, elapsed_ms=elapsed_ms)
    except HTTPError as err:
        body = err.read()
        elapsed_ms = (time.perf_counter() - start) * 1000
        return ResponseResult(status_code=err.code, body=body, elapsed_ms=elapsed_ms, error=str(err))
    except URLError as err:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return ResponseResult(status_code=None, body=b"", elapsed_ms=elapsed_ms, error=str(err))
    except Exception as err:  # pragma: no cover - defensive reporting path.
        elapsed_ms = (time.perf_counter() - start) * 1000
        return ResponseResult(
            status_code=None,
            body=b"",
            elapsed_ms=elapsed_ms,
            error=f"{err}\n{traceback.format_exc()}",
        )


def compare_responses(left: ResponseResult, right: ResponseResult) -> CompareResult:
    if left.error or right.error:
        if left.error == right.error and left.status_code == right.status_code and left.body == right.body:
            return CompareResult(equal=True)
        return CompareResult(equal=False, diff_path="error", message=f"{left.error!r} != {right.error!r}")

    if left.status_code != right.status_code:
        return CompareResult(
            equal=False,
            diff_path="status_code",
            message=f"{left.status_code} != {right.status_code}",
        )

    left_json, left_json_ok = load_json(left.body)
    right_json, right_json_ok = load_json(right.body)
    if left_json_ok and right_json_ok:
        diff = first_json_diff(left_json, right_json, "body")
        if diff is None:
            return CompareResult(equal=True)
        return CompareResult(equal=False, diff_path=diff[0], message=diff[1])

    if left.body != right.body:
        return CompareResult(equal=False, diff_path="body", message="non-json response bodies differ")
    return CompareResult(equal=True)


def load_json(body: bytes) -> Tuple[Any, bool]:
    try:
        return json.loads(body.decode("utf-8")), True
    except Exception:
        return None, False


def first_json_diff(left: Any, right: Any, path: str) -> Optional[Tuple[str, str]]:
    if type(left) is not type(right):
        return path, f"type {type(left).__name__} != {type(right).__name__}"
    if isinstance(left, dict):
        left_keys = set(left.keys())
        right_keys = set(right.keys())
        if left_keys != right_keys:
            missing_left = sorted(right_keys - left_keys)
            missing_right = sorted(left_keys - right_keys)
            return path, f"keys differ, missing_left={missing_left}, missing_right={missing_right}"
        for key in sorted(left_keys):
            diff = first_json_diff(left[key], right[key], f"{path}.{key}")
            if diff is not None:
                return diff
        return None
    if isinstance(left, list):
        if len(left) != len(right):
            return path, f"length {len(left)} != {len(right)}"
        for idx, (left_item, right_item) in enumerate(zip(left, right)):
            diff = first_json_diff(left_item, right_item, f"{path}[{idx}]")
            if diff is not None:
                return diff
        return None
    if left != right:
        return path, f"{left!r} != {right!r}"
    return None


def compare_one(replay: ReplayRequest, left_base: str, right_base: str, timeout: float) -> Dict[str, Any]:
    left = send_request(replay, left_base, timeout)
    right = send_request(replay, right_base, timeout)
    cmp = compare_responses(left, right)
    return {
        "index": replay.index,
        "method": replay.method,
        "path": replay.path,
        "equal": cmp.equal,
        "diff_path": cmp.diff_path,
        "diff_message": cmp.message,
        "left": response_summary(left),
        "right": response_summary(right),
    }


def response_summary(resp: ResponseResult) -> Dict[str, Any]:
    return {
        "status_code": resp.status_code,
        "elapsed_ms": round(resp.elapsed_ms, 3),
        "body_size": len(resp.body),
        "body_sha256": sha256_hex(resp.body),
        "body_preview": preview_body(resp.body),
        "error": resp.error,
    }


def sha256_hex(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def preview_body(body: bytes, limit: int = 240) -> str:
    text = body.decode("utf-8", errors="replace")
    if len(text) <= limit:
        return text
    return text[:limit] + "...<truncated>"


def run_compare(input_path: Path, left: str, right: str, timeout: float) -> Dict[str, Any]:
    requests, skipped = parse_goreplay_file(input_path)
    items = [compare_one(req, left, right, timeout) for req in requests]
    matched = sum(1 for item in items if item["equal"])
    mismatched = sum(1 for item in items if not item["equal"] and not (item["left"]["error"] or item["right"]["error"]))
    request_failed = sum(1 for item in items if item["left"]["error"] or item["right"]["error"])
    return {
        "input": str(input_path),
        "left": left,
        "right": right,
        "summary": {
            "total_records": len(requests) + skipped,
            "replayed_requests": len(requests),
            "matched": matched,
            "mismatched": mismatched,
            "request_failed": request_failed,
            "skipped": skipped,
            "passed": len(requests) > 0 and matched == len(requests),
        },
        "items": items,
    }


def write_json_report(report: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def write_markdown_report(report: Dict[str, Any], path: Path, top_n: int = 20) -> None:
    summary = report["summary"]
    lines = [
        "# UnifyQuery Compare Report",
        "",
        f"- Left: `{report['left']}`",
        f"- Right: `{report['right']}`",
        f"- Input: `{report['input']}`",
        f"- Replayed requests: `{summary['replayed_requests']}`",
        f"- Matched: `{summary['matched']}`",
        f"- Mismatched: `{summary['mismatched']}`",
        f"- Request failed: `{summary['request_failed']}`",
        f"- Skipped records: `{summary['skipped']}`",
        f"- Conclusion: `{'PASS' if summary['passed'] else 'FAIL'}`",
        "",
        "## Failed Requests",
        "",
    ]
    failed = [item for item in report["items"] if not item["equal"]][:top_n]
    if not failed:
        lines.append("No failed requests.")
    else:
        for item in failed:
            lines.extend(
                [
                    f"### #{item['index']} {item['method']} {item['path']}",
                    "",
                    f"- Diff path: `{item['diff_path']}`",
                    f"- Diff message: `{item['diff_message']}`",
                    f"- Left status/body: `{item['left']['status_code']}` / `{item['left']['body_size']}` bytes",
                    f"- Right status/body: `{item['right']['status_code']}` / `{item['right']['body_size']}` bytes",
                    "",
                ]
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="GoReplay --output-file raw capture")
    parser.add_argument("--left", required=True, help="left UnifyQuery base URL")
    parser.add_argument("--right", required=True, help="right UnifyQuery base URL")
    parser.add_argument("--output", default="tmp/unifyquery_compare_report.json", type=Path)
    parser.add_argument("--markdown", default="tmp/unifyquery_compare_report.md", type=Path)
    parser.add_argument("--timeout", default=30.0, type=float)
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    report = run_compare(args.input, args.left, args.right, args.timeout)
    write_json_report(report, args.output)
    write_markdown_report(report, args.markdown)

    summary = report["summary"]
    conclusion = "PASS" if summary["passed"] else "FAIL"
    print(
        f"{conclusion}: replayed={summary['replayed_requests']} "
        f"matched={summary['matched']} mismatched={summary['mismatched']} "
        f"request_failed={summary['request_failed']} skipped={summary['skipped']}"
    )
    print(f"JSON report: {args.output}")
    print(f"Markdown report: {args.markdown}")
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
