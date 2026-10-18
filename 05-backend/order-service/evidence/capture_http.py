"""Chạy từ order-service, với Main mới khởi động (RAM rỗng); chỉ gọi loopback.

python evidence/capture_http.py
Lưu stdout nguyên byte của curl.exe, không phụ thuộc encoding của PowerShell.
"""
from pathlib import Path
import json
import subprocess

BASE = "http://127.0.0.1:8084"
CASES = [
    ("create user", "POST", "/api/v1/users", "user.json", "application/json", 201),
    ("get user", "GET", "/api/v1/users/1", None, None, 200),
    ("duplicate email", "POST", "/api/v1/users", "user.json", "application/json", 409),
    ("validation", "POST", "/api/v1/users", "invalid-user.json", "application/json", 422),
    ("bad JSON", "POST", "/api/v1/users", "broken.json", "application/json", 400),
    ("wrong media type", "POST", "/api/v1/users", "user.json", "text/plain", 415),
    ("JSON with UTF-8 charset", "POST", "/api/v1/users", "user.json", "application/json; charset=UTF-8", 409),
    ("bad query type", "GET", "/api/v1/users?limit=abc", None, None, 400),
    ("bad path type", "GET", "/api/v1/users/nope", None, None, 400),
    ("query validation", "GET", "/api/v1/users?sort=password&offset=-1&limit=101", None, None, 422),
    ("list users", "GET", "/api/v1/users?q=AN&sort=-name&offset=0&limit=2", None, None, 200),
    ("missing user", "GET", "/api/v1/users/99", None, None, 404),
    ("create order", "POST", "/api/v1/users/1/orders", "order.json", "application/json", 201),
    ("missing order owner", "POST", "/api/v1/users/99/orders", "order.json", "application/json", 404),
    ("get order", "GET", "/api/v1/orders/1", None, None, 200),
    ("pay", "PATCH", "/api/v1/orders/1/status", "paid.json", "application/json", 200),
    ("retry pay", "PATCH", "/api/v1/orders/1/status", "paid.json", "application/json", 200),
    ("transition conflict", "PATCH", "/api/v1/orders/1/status", "cancelled.json", "application/json", 409),
    ("list orders", "GET", "/api/v1/users/1/orders", None, None, 200),
    ("method mismatch", "DELETE", "/api/v1/users", None, None, 405),
    ("unknown route", "GET", "/api/v2/users", None, None, 404),
    ("OpenAPI", "GET", "/docs/openapi.yaml", None, None, 200),
]

def main():
    output = []
    for label, method, path, body, content_type, expected in CASES:
        args = ["curl.exe", "-i", "-sS", "--max-time", "10", "-X", method]
        if content_type:
            args += ["-H", "Content-Type: " + content_type]
        if body:
            args += ["--data-binary", "@requests/" + body]
        args += [BASE + path]
        raw = subprocess.check_output(args)
        head, payload = raw.replace(b"\r\n", b"\n").split(b"\n\n", 1)
        status = int(head.splitlines()[0].split()[1])
        assert status == expected, (label, status, payload)
        if status >= 400:
            assert json.loads(payload)["status"] == status, (label, payload)
            assert b"application/problem+json" in head
        if status == 405:
            assert b"Allow: GET, POST" in head
        if label == "OpenAPI":
            Path("evidence/openapi.yaml").write_bytes(payload)
        output += [("\n# " + label + "\n$ " + subprocess.list2cmdline(args) + "\n").encode("utf-8"), raw, b"\n"]
        print(f"{label}: {status} OK")
    Path("evidence/curl-transcript.txt").write_bytes(b"".join(output))
    print(f"{len(CASES)} HTTP exchanges checked")

if __name__ == "__main__":
    main()
