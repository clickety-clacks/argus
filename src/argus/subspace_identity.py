from __future__ import annotations

import base64
import errno
import json
import os
import socket
import ssl
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from .pipeline import PipelineError, iso_z, parse_now


class SubspaceAuthError(PipelineError):
    def __init__(self, exact_cause: str, message: str, response: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.exact_cause = exact_cause
        self.response = response or {}


def _base64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix="." + path.name + ".", dir=str(path.parent))
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w") as handle:
            json.dump(payload, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _response_payload(response: requests.Response) -> Dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        payload = {"message": response.text[:500]}
    return payload if isinstance(payload, dict) else {"response": payload}


def _error_code(payload: Dict[str, Any], fallback: str) -> str:
    error_value = payload.get("error")
    if isinstance(error_value, str) and error_value:
        return error_value
    error = error_value if isinstance(error_value, dict) else {}
    return str(payload.get("code") or error.get("code") or payload.get("reason") or error.get("reason") or fallback)


def _request_exception_cause(exc: requests.RequestException) -> str:
    current: Optional[BaseException] = exc
    seen = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, requests.exceptions.SSLError) or isinstance(current, ssl.SSLError):
            return "TLS_FAILURE"
        if isinstance(current, requests.Timeout) or isinstance(current, (TimeoutError, socket.timeout)):
            return "TIMEOUT"
        if isinstance(current, ConnectionRefusedError) or (
            isinstance(current, OSError) and current.errno == errno.ECONNREFUSED
        ):
            return "CONNECTION_REFUSED"
        if isinstance(current, ConnectionResetError) or (
            isinstance(current, OSError) and current.errno == errno.ECONNRESET
        ):
            return "CONNECTION_RESET"
        if isinstance(current, BrokenPipeError) or (
            isinstance(current, OSError) and current.errno == errno.EPIPE
        ):
            return "BROKEN_PIPE"
        if isinstance(current, socket.gaierror):
            return "DNS_RESOLUTION_FAILED"
        current = current.__cause__ or current.__context__
    if isinstance(exc, requests.ConnectionError):
        return "CONNECTION_ERROR"
    return exc.__class__.__name__


@dataclass(frozen=True)
class DurableSubspaceIdentity:
    name: str
    public_key: str
    private_key: Ed25519PrivateKey

    @classmethod
    def load(cls, path: Path) -> "DurableSubspaceIdentity":
        try:
            payload = json.loads(path.read_text())
            name = str(payload["name"])
            public_key = str(payload["public_key"])
            private_bytes = _base64url_decode(str(payload["private_key"]))
            private_key = Ed25519PrivateKey.from_private_bytes(private_bytes)
        except Exception as exc:
            raise PipelineError("invalid durable Subspace identity: {}".format(exc)) from exc
        derived_public_key = _base64url_encode(private_key.public_key().public_bytes_raw())
        if derived_public_key != public_key:
            raise PipelineError("durable Subspace identity public/private key mismatch")
        return cls(name=name, public_key=public_key, private_key=private_key)

    def sign(self, canonical_payload: str) -> str:
        return _base64url_encode(self.private_key.sign(canonical_payload.encode("utf-8")))


class DurableSubspaceSession:
    def __init__(
        self,
        endpoint: str,
        publish_target_key: str,
        identity_path: Path,
        session_path: Path,
        renew_before_seconds: int,
        timeout_seconds: float,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.publish_target_key = publish_target_key
        self.identity_path = identity_path
        self.session_path = session_path
        self.renew_before_seconds = renew_before_seconds
        self.timeout_seconds = timeout_seconds
        self.identity = DurableSubspaceIdentity.load(identity_path)
        self._lock = threading.Lock()
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        if not self.session_path.exists():
            return {
                "identity": self.identity.name,
                "subspace_endpoint": self.endpoint,
                "publish_target_key": self.publish_target_key,
                "agent_id": self.identity.public_key,
                "session_token": None,
                "session_expires_at": None,
                "token_issued_at": None,
                "reauth_generation": 0,
                "last_reauth": None,
                "last_authenticated_join_at": None,
            }
        try:
            payload = json.loads(self.session_path.read_text())
        except Exception as exc:
            raise PipelineError("invalid durable Subspace session state: {}".format(exc)) from exc
        expected = {
            "identity": self.identity.name,
            "subspace_endpoint": self.endpoint,
            "publish_target_key": self.publish_target_key,
            "agent_id": self.identity.public_key,
        }
        for key, value in expected.items():
            if payload.get(key) != value:
                raise PipelineError("durable Subspace session {} mismatch".format(key))
        return payload

    def _persist(self) -> None:
        _atomic_write_json(self.session_path, self.state)

    @property
    def agent_id(self) -> str:
        return self.identity.public_key

    @property
    def session_token(self) -> Optional[str]:
        value = self.state.get("session_token")
        return str(value) if value else None

    @property
    def session_expires_at(self) -> Optional[str]:
        value = self.state.get("session_expires_at")
        return str(value) if value else None

    def needs_reauth(self, now: datetime) -> bool:
        if not self.session_token:
            return True
        if not self.session_expires_at:
            return False
        return parse_now(self.session_expires_at) <= now + timedelta(seconds=self.renew_before_seconds)

    def ensure_session(self, now: datetime, reason: str) -> Dict[str, Any]:
        with self._lock:
            self.state = self._load_state()
            if not self.needs_reauth(now):
                return {"reauthenticated": False, "session_expires_at": self.session_expires_at}
            return self._reauth_locked(now, reason)

    def invalidate_token(self, now: datetime, exact_cause: str) -> None:
        with self._lock:
            self.state = self._load_state()
            self.state["session_token"] = None
            self.state["session_expires_at"] = None
            self.state["last_reauth"] = {
                "status": "required",
                "observed_at": iso_z(now),
                "exact_cause": exact_cause,
            }
            self._persist()

    def reauth(self, now: datetime, reason: str) -> Dict[str, Any]:
        with self._lock:
            self.state = self._load_state()
            return self._reauth_locked(now, reason)

    def _reauth_locked(self, now: datetime, reason: str) -> Dict[str, Any]:
        try:
            start = requests.post(
                self.endpoint + "/api/agents/reauth/start",
                json={"agentId": self.agent_id},
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as exc:
            cause = _request_exception_cause(exc)
            self._persist_reauth_failure(now, reason, cause, {})
            raise SubspaceAuthError(cause, "Subspace reauth/start failed: {}".format(exc)) from exc
        start_payload = _response_payload(start)
        if start.status_code < 200 or start.status_code >= 300:
            cause = _error_code(start_payload, "REAUTH_START_HTTP_{}".format(start.status_code))
            self._persist_reauth_failure(now, reason, cause, start_payload)
            raise SubspaceAuthError(cause, "Subspace reauth/start failed", start_payload)
        challenge = start_payload.get("challenge")
        challenge_id = start_payload.get("challengeId")
        if not challenge or not challenge_id:
            cause = "REAUTH_START_CONTRACT_VIOLATION"
            self._persist_reauth_failure(now, reason, cause, start_payload)
            raise SubspaceAuthError(cause, "Subspace reauth/start response is missing challenge fields", start_payload)
        canonical_payload = json.dumps(
            {"agentId": self.agent_id, "challenge": str(challenge)},
            separators=(",", ":"),
        )
        try:
            verify = requests.post(
                self.endpoint + "/api/agents/reauth/verify",
                json={
                    "challengeId": str(challenge_id),
                    "agentId": self.agent_id,
                    "signature": self.identity.sign(canonical_payload),
                },
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as exc:
            cause = _request_exception_cause(exc)
            self._persist_reauth_failure(now, reason, cause, {})
            raise SubspaceAuthError(cause, "Subspace reauth/verify failed: {}".format(exc)) from exc
        verify_payload = _response_payload(verify)
        if verify.status_code < 200 or verify.status_code >= 300:
            cause = _error_code(verify_payload, "REAUTH_VERIFY_HTTP_{}".format(verify.status_code))
            self._persist_reauth_failure(now, reason, cause, verify_payload)
            raise SubspaceAuthError(cause, "Subspace reauth/verify failed", verify_payload)
        token = verify_payload.get("sessionToken")
        if not token:
            cause = "REAUTH_VERIFY_CONTRACT_VIOLATION"
            self._persist_reauth_failure(now, reason, cause, verify_payload)
            raise SubspaceAuthError(cause, "Subspace reauth/verify response is missing sessionToken", verify_payload)
        expires_at = verify_payload.get("sessionExpiresAt")
        reauth_generation = int(self.state.get("reauth_generation") or 0) + 1
        self.state.update(
            {
                "session_token": str(token),
                "session_expires_at": str(expires_at) if expires_at else None,
                "token_issued_at": iso_z(now),
                "reauth_generation": reauth_generation,
                "last_reauth": {
                    "status": "succeeded",
                    "observed_at": iso_z(now),
                    "reason": reason,
                    "reauth_generation": reauth_generation,
                    "session_expires_at": str(expires_at) if expires_at else None,
                },
            }
        )
        self._persist()
        return {
            "reauthenticated": True,
            "reauthenticated_at": iso_z(now),
            "reason": reason,
            "reauth_generation": reauth_generation,
            "session_expires_at": self.session_expires_at,
        }

    def _persist_reauth_failure(self, now: datetime, reason: str, cause: str, payload: Dict[str, Any]) -> None:
        self.state["last_reauth"] = {
            "status": "failed",
            "observed_at": iso_z(now),
            "reason": reason,
            "exact_cause": cause,
            "response": payload,
        }
        self._persist()

    def record_authenticated_join(self, now: datetime) -> None:
        with self._lock:
            self.state = self._load_state()
            self.state["last_authenticated_join_at"] = iso_z(now)
            self._persist()

    def public_status(self) -> Dict[str, Any]:
        return {
            "identity": self.state.get("identity"),
            "agent_id": self.agent_id,
            "subspace_endpoint": self.endpoint,
            "publish_target_key": self.publish_target_key,
            "session_expires_at": self.session_expires_at,
            "token_issued_at": self.state.get("token_issued_at"),
            "reauth_generation": int(self.state.get("reauth_generation") or 0),
            "last_reauth": self.state.get("last_reauth"),
            "last_authenticated_join_at": self.state.get("last_authenticated_join_at"),
        }
