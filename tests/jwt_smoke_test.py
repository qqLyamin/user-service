import base64
import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUILD_DIR = ROOT / "build"
EXE = BUILD_DIR / "user-service.exe"
if not EXE.exists():
    EXE = BUILD_DIR / "user-service"


def wait_for_port(timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", 18081), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("service did not start")


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def make_token(secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": "auth-service",
        "sub": "ci-smoke-user",
        "uid": "ci-smoke-user",
        "name": "CI Smoke User",
        "aud": ["internal"],
        "iat": int(time.time()),
        "exp": int(time.time()) + 300,
    }
    signing_input = f"{b64url(json.dumps(header, separators=(',', ':')).encode())}.{b64url(json.dumps(payload, separators=(',', ':')).encode())}"
    signature = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
    return f"{signing_input}.{b64url(signature)}"


def request(method: str, path: str, token: str, body=None):
    headers = {"Authorization": f"Bearer {token}"}
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    else:
        data = None
    req = urllib.request.Request(f"http://127.0.0.1:18081{path}", method=method, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode())


def main() -> int:
    if not EXE.exists():
        raise RuntimeError(f"binary not found: {EXE}")
    env = os.environ.copy()
    env["USER_SERVICE_PORT"] = "18081"
    env["JWT_SECRET"] = "test-secret"
    env["JWT_ISSUER"] = "auth-service"
    env["JWT_AUDIENCE"] = "internal"
    token = make_token(env["JWT_SECRET"])
    proc = subprocess.Popen([str(EXE)], cwd=str(ROOT), env=env)
    try:
        wait_for_port()
        before = request("GET", "/v1/users/me", token)
        patched = request("PATCH", "/v1/users/me", token, {"displayName": "CI Smoke Updated"})
        after = request("GET", "/v1/users/me", token)
        assert before["userId"]
        assert patched["displayName"] == "CI Smoke Updated"
        assert after["displayName"] == "CI Smoke Updated"
        health = urllib.request.urlopen("http://127.0.0.1:18081/health", timeout=5)
        assert health.status == 200
        print("JWT SMOKE PASSED")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
