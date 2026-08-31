#!/usr/bin/env python3
"""Persist Longbridge OAuth refresh state without exposing token material.

GitHub Actions runners are ephemeral while the Longbridge SDK refreshes its
OAuth cache in place.  This helper restores/saves that cache as an encrypted
state file.  ``LONGBRIDGE_OAUTH_TOKEN_CACHE_B64`` remains an operator-owned
bootstrap source; a changed bootstrap always wins over older persisted state.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


SCHEMA_VERSION = 1
MIN_ENCRYPTION_KEY_BYTES = 32
DEFAULT_ENCRYPTED_PATH = Path("data/intraday/longbridge_oauth_cache.json.enc")


class OAuthStateError(RuntimeError):
    """Raised when encrypted OAuth state cannot be validated safely."""


def _client_id() -> str:
    explicit = os.getenv("LONGBRIDGE_OAUTH_CLIENT_ID", "").strip()
    if explicit:
        return explicit
    if not os.getenv("LONGBRIDGE_ACCESS_TOKEN", "").strip():
        return os.getenv("LONGBRIDGE_APP_KEY", "").strip()
    return ""


def _cache_path(client_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,160}", client_id):
        raise OAuthStateError("Longbridge OAuth client_id 格式不安全")
    return Path.home() / ".longbridge" / "openapi" / "tokens" / client_id


def _validate_cache(payload: bytes) -> Mapping[str, Any]:
    if not payload.strip():
        raise OAuthStateError("Longbridge OAuth token cache 为空")
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OAuthStateError("Longbridge OAuth token cache 不是有效 JSON") from exc
    if not isinstance(parsed, Mapping) or not parsed:
        raise OAuthStateError("Longbridge OAuth token cache 结构无效")
    return parsed


def _bootstrap_payload() -> Optional[bytes]:
    encoded = "".join(os.getenv("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64", "").split())
    if not encoded:
        return None
    try:
        payload = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise OAuthStateError("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64 不是有效 base64") from exc
    _validate_cache(payload)
    return payload


def _sha256(payload: Optional[bytes]) -> str:
    return hashlib.sha256(payload).hexdigest() if payload is not None else ""


def _atomic_write(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def _openssl(*, decrypt: bool, payload: bytes) -> bytes:
    encryption_key = os.getenv("LONGBRIDGE_OAUTH_CACHE_KEY", "")
    if not encryption_key:
        raise OAuthStateError("缺少 LONGBRIDGE_OAUTH_CACHE_KEY")
    if len(encryption_key.encode("utf-8")) < MIN_ENCRYPTION_KEY_BYTES:
        raise OAuthStateError("LONGBRIDGE_OAUTH_CACHE_KEY 必须至少 32 字节")
    command = [
        "openssl",
        "enc",
        "-aes-256-cbc",
        "-pbkdf2",
        "-pass",
        "env:LONGBRIDGE_OAUTH_CACHE_KEY",
    ]
    if decrypt:
        command.append("-d")
    else:
        command.append("-salt")
    try:
        completed = subprocess.run(
            command,
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        operation = "解密" if decrypt else "加密"
        raise OAuthStateError(f"Longbridge OAuth 状态{operation}失败") from exc
    return completed.stdout


def _report(path: Optional[Path], payload: Mapping[str, Any]) -> None:
    if path is None:
        return
    _atomic_write(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        mode=0o600,
    )


def restore_state(
    *,
    encrypted_path: Path = DEFAULT_ENCRYPTED_PATH,
    cache_path: Optional[Path] = None,
) -> dict[str, Any]:
    """Restore the best valid non-interactive cache and return safe metadata."""

    client_id = _client_id()
    result: dict[str, Any] = {
        "operation": "restore",
        "status": "unconfigured",
        "source": "none",
        "encrypted_state_present": encrypted_path.is_file(),
        "encryption_key_configured": bool(os.getenv("LONGBRIDGE_OAUTH_CACHE_KEY", "")),
    }
    if not client_id:
        return result

    target = cache_path or _cache_path(client_id)
    bootstrap_error: Optional[OAuthStateError] = None
    try:
        bootstrap = _bootstrap_payload()
    except OAuthStateError as exc:
        # A malformed operator seed must not hide a still-valid encrypted SDK
        # refresh state.  Prefer the persisted state and surface only the safe
        # error type in the report; fall back to invalid_bootstrap when no
        # other valid source exists.
        bootstrap = None
        bootstrap_error = exc
        result["bootstrap_error_type"] = type(exc).__name__
    bootstrap_sha = _sha256(bootstrap)

    if encrypted_path.is_file() and result["encryption_key_configured"]:
        try:
            plaintext = _openssl(
                decrypt=True,
                payload=encrypted_path.read_bytes(),
            )
            envelope = json.loads(plaintext.decode("utf-8"))
            if not isinstance(envelope, Mapping):
                raise OAuthStateError("加密 OAuth 状态结构无效")
            if int(envelope.get("schema_version") or 0) != SCHEMA_VERSION:
                raise OAuthStateError("加密 OAuth 状态版本不受支持")
            if str(envelope.get("client_id") or "") != client_id:
                raise OAuthStateError("加密 OAuth 状态 client_id 不匹配")
            persisted_bootstrap_sha = str(envelope.get("bootstrap_sha256") or "")
            # A newly supplied bootstrap is an explicit operator recovery and
            # must supersede older persisted state.
            if bootstrap is not None and persisted_bootstrap_sha != bootstrap_sha:
                raise OAuthStateError("bootstrap 已更新，忽略旧的加密 OAuth 状态")
            cache_payload = base64.b64decode(str(envelope.get("cache_b64") or ""), validate=True)
            _validate_cache(cache_payload)
            if str(envelope.get("cache_sha256") or "") != _sha256(cache_payload):
                raise OAuthStateError("加密 OAuth 状态校验和不匹配")
            _atomic_write(target, cache_payload)
            result.update(status="restored", source="encrypted_persisted")
            return result
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            binascii.Error,
            ValueError,
            OAuthStateError,
        ) as exc:
            result["persisted_error_type"] = type(exc).__name__

    if bootstrap is not None:
        _atomic_write(target, bootstrap)
        result.update(status="restored", source="bootstrap_secret")
        return result

    if target.is_file():
        try:
            _validate_cache(target.read_bytes())
            result.update(status="restored", source="existing_local")
            return result
        except (OSError, OAuthStateError):
            pass
    result["status"] = "invalid_bootstrap" if bootstrap_error else "missing"
    return result


def save_state(
    *,
    encrypted_path: Path = DEFAULT_ENCRYPTED_PATH,
    cache_path: Optional[Path] = None,
) -> dict[str, Any]:
    """Encrypt the SDK cache after a run and return safe metadata."""

    client_id = _client_id()
    result: dict[str, Any] = {
        "operation": "save",
        "status": "unconfigured",
        "source": "none",
        "encryption_key_configured": bool(os.getenv("LONGBRIDGE_OAUTH_CACHE_KEY", "")),
    }
    if not client_id:
        return result
    if not result["encryption_key_configured"]:
        result["status"] = "skipped_missing_key"
        return result

    target = cache_path or _cache_path(client_id)
    try:
        cache_payload = target.read_bytes()
        _validate_cache(cache_payload)
        try:
            bootstrap = _bootstrap_payload()
        except OAuthStateError as exc:
            # The refreshed local cache remains the authoritative state even
            # if an operator accidentally corrupts the old bootstrap secret.
            bootstrap = None
            result["bootstrap_error_type"] = type(exc).__name__
        envelope = {
            "schema_version": SCHEMA_VERSION,
            "client_id": client_id,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "bootstrap_sha256": _sha256(bootstrap),
            "cache_sha256": _sha256(cache_payload),
            "cache_b64": base64.b64encode(cache_payload).decode("ascii"),
        }
        plaintext = (json.dumps(envelope, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        encrypted = _openssl(decrypt=False, payload=plaintext)
        _atomic_write(encrypted_path, encrypted)
        result.update(status="saved", source="sdk_cache")
    except (OSError, OAuthStateError) as exc:
        result.update(status="save_failed", error_type=type(exc).__name__)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("restore", "save"))
    parser.add_argument(
        "--encrypted",
        type=Path,
        default=DEFAULT_ENCRYPTED_PATH,
        help="encrypted persisted state path",
    )
    parser.add_argument("--cache", type=Path, default=None)
    parser.add_argument("--report", type=Path, default=None)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    operation = restore_state if args.operation == "restore" else save_state
    result = operation(encrypted_path=args.encrypted, cache_path=args.cache)
    _report(args.report, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if args.operation == "save" and result.get("status") not in {
        "saved",
        "unconfigured",
    }:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
