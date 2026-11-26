"""可选的本机 HTTP/JSON 中间件入口（仅标准库）。

启动::

    python -m coordinator.server --host 127.0.0.1 --port 8765

然后::

    curl -sS -X POST http://127.0.0.1:8765/message \\
      -H 'content-type: application/json' \\
      -d '{"at":4,"kind":"scan","record":{}}'

HTTP 层与 stdin/JSONL 入口共用同一个 :class:`Coordinator`。
"""

from __future__ import annotations

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from coordinator.core import Coordinator
else:
    from .core import Coordinator


def create_handler(coordinator: Coordinator) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "JointCalibrationCoordinator/1.0"

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:  # 保持测试输出安静
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if parsed.path == "/health":
                self._send_json(200, {"ok": True, "restart_count": coordinator.state["restart_count"]})
                return
            if parsed.path == "/inspect":
                tx = (query.get("tx") or [None])[0]
                attempt_raw = (query.get("attempt") or [None])[0]
                at_raw = (query.get("at") or [None])[0]
                try:
                    attempt = int(attempt_raw) if attempt_raw is not None else None
                    at = int(at_raw) if at_raw is not None else None
                    self._send_json(200, coordinator.inspect(tx, attempt=attempt, at=at))
                except Exception as error:  # 本地中间件入口需要把协议错误返回给调用方
                    self._send_json(400, {"error": type(error).__name__, "message": str(error)})
                return
            if parsed.path == "/state":
                self._send_json(200, coordinator.state_snapshot())
                return
            self._send_json(404, {"error": "not_found", "path": parsed.path})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path not in {"/message", "/"}:
                self._send_json(404, {"error": "not_found", "path": parsed.path})
                return
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                message = json.loads(raw.decode("utf-8"))
                if not isinstance(message, dict):
                    raise ValueError("请求体必须是 JSON 对象")
                result = coordinator.handle(message)
                self._send_json(200, result)
            except Exception as error:
                self._send_json(400, {"error": type(error).__name__, "message": str(error)})

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="协调器本机 HTTP 入口")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--state", default=None)
    args = parser.parse_args(argv)
    coordinator = Coordinator(state_path=args.state)
    server = ThreadingHTTPServer((args.host, args.port), create_handler(coordinator))
    print(f"coordinator listening on http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
