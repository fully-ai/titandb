"""TitanDB Python Wire Protocol & HTTP Client."""

import json
import socket
import struct
import urllib.request
from typing import Any, Dict, List, Optional


class TitanWireClient:
    """Connects to TitanDB's length-prefixed TCP wire protocol ([u32 BE len][JSON])."""

    def __init__(self, host: str = "127.0.0.1", port: int = 7440, auth_token: Optional[str] = None, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.auth_token = auth_token
        self.sock = socket.create_connection((host, port), timeout=timeout)

    def close(self) -> None:
        self.sock.close()

    def _call(self, op: str, sql: Optional[str] = None, params: Optional[List[Any]] = None) -> Dict[str, Any]:
        req = {
            "auth_token": self.auth_token,
            "op": op,
            "sql": sql,
            "params": params or [],
        }
        payload = json.dumps(req).encode("utf-8")
        self.sock.sendall(struct.pack(">I", len(payload)) + payload)

        len_bytes = self._recv_exact(4)
        (rlen,) = struct.unpack(">I", len_bytes)
        body = self._recv_exact(rlen)
        resp = json.loads(body.decode("utf-8"))
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error") or "TitanDB wire error")
        return resp

    def _recv_exact(self, n: int) -> bytes:
        chunks = []
        remaining = n
        while remaining > 0:
            chunk = self.sock.recv(remaining)
            if not chunk:
                raise ConnectionError("unexpected EOF from TitanDB")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def ping(self) -> bool:
        return bool(self._call("ping").get("ok"))

    def query(self, sql: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
        return self._call("query", sql=sql, params=params).get("result") or {}

    def metrics(self) -> Dict[str, Any]:
        return self._call("metrics").get("metrics") or {}


class TitanHTTPClient:
    """Queries TitanDB over its HTTP Admin & Query API."""

    def __init__(self, base_url: str = "http://127.0.0.1:7441", auth_token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token

    def query(self, sql: str) -> Dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        req = urllib.request.Request(
            f"{self.base_url}/query",
            data=json.dumps({"sql": sql}).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))
