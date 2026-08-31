from __future__ import annotations

import base64
import json
from pathlib import Path

from scripts.longbridge_oauth_state import main, restore_state, save_state


ENCRYPTION_KEY = "test-only-encryption-key-32-bytes"


def _encoded(payload: dict[str, str]) -> str:
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")


def test_refreshed_cache_round_trips_through_encrypted_state(tmp_path: Path, monkeypatch) -> None:
    bootstrap = {"access_token": "bootstrap", "refresh_token": "bootstrap-r"}
    refreshed = {"access_token": "refreshed", "refresh_token": "refreshed-r"}
    cache = tmp_path / "sdk-cache"
    encrypted = tmp_path / "oauth-state.enc"
    cache.write_text(json.dumps(refreshed), encoding="utf-8")
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CLIENT_ID", "client-1")
    monkeypatch.setenv("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64", _encoded(bootstrap))
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CACHE_KEY", ENCRYPTION_KEY)

    saved = save_state(encrypted_path=encrypted, cache_path=cache)
    cache.unlink()
    restored = restore_state(encrypted_path=encrypted, cache_path=cache)

    assert saved["status"] == "saved"
    assert restored["status"] == "restored"
    assert restored["source"] == "encrypted_persisted"
    assert json.loads(cache.read_text(encoding="utf-8")) == refreshed
    assert b"refreshed" not in encrypted.read_bytes()


def test_changed_bootstrap_supersedes_older_persisted_state(tmp_path: Path, monkeypatch) -> None:
    old_bootstrap = {"access_token": "old-bootstrap"}
    refreshed = {"access_token": "old-refreshed"}
    replacement = {"access_token": "operator-recovery"}
    cache = tmp_path / "sdk-cache"
    encrypted = tmp_path / "oauth-state.enc"
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CLIENT_ID", "client-1")
    monkeypatch.setenv("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64", _encoded(old_bootstrap))
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CACHE_KEY", ENCRYPTION_KEY)
    cache.write_text(json.dumps(refreshed), encoding="utf-8")
    assert save_state(encrypted_path=encrypted, cache_path=cache)["status"] == "saved"

    cache.unlink()
    monkeypatch.setenv("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64", _encoded(replacement))
    restored = restore_state(encrypted_path=encrypted, cache_path=cache)

    assert restored["source"] == "bootstrap_secret"
    assert restored["persisted_error_type"] == "OAuthStateError"
    assert json.loads(cache.read_text(encoding="utf-8")) == replacement


def test_missing_encryption_key_still_restores_bootstrap(tmp_path: Path, monkeypatch) -> None:
    bootstrap = {"refresh_token": "bootstrap-r"}
    cache = tmp_path / "sdk-cache"
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CLIENT_ID", "client-1")
    monkeypatch.setenv("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64", _encoded(bootstrap))
    monkeypatch.delenv("LONGBRIDGE_OAUTH_CACHE_KEY", raising=False)

    restored = restore_state(encrypted_path=tmp_path / "missing.enc", cache_path=cache)
    saved = save_state(encrypted_path=tmp_path / "missing.enc", cache_path=cache)

    assert restored["source"] == "bootstrap_secret"
    assert saved["status"] == "skipped_missing_key"


def test_corrupt_persisted_state_falls_back_to_valid_bootstrap(tmp_path: Path, monkeypatch) -> None:
    bootstrap = {"access_token": "bootstrap"}
    encrypted = tmp_path / "oauth-state.enc"
    encrypted.write_bytes(b"not-an-openssl-envelope")
    cache = tmp_path / "sdk-cache"
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CLIENT_ID", "client-1")
    monkeypatch.setenv("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64", _encoded(bootstrap))
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CACHE_KEY", ENCRYPTION_KEY)

    restored = restore_state(encrypted_path=encrypted, cache_path=cache)

    assert restored["source"] == "bootstrap_secret"
    assert restored["persisted_error_type"] == "OAuthStateError"
    assert json.loads(cache.read_text(encoding="utf-8")) == bootstrap


def test_invalid_bootstrap_does_not_hide_valid_persisted_state(tmp_path: Path, monkeypatch) -> None:
    bootstrap = {"access_token": "bootstrap"}
    refreshed = {"access_token": "refreshed"}
    encrypted = tmp_path / "oauth-state.enc"
    cache = tmp_path / "sdk-cache"
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CLIENT_ID", "client-1")
    monkeypatch.setenv("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64", _encoded(bootstrap))
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CACHE_KEY", ENCRYPTION_KEY)
    cache.write_text(json.dumps(refreshed), encoding="utf-8")
    assert save_state(encrypted_path=encrypted, cache_path=cache)["status"] == "saved"

    cache.unlink()
    monkeypatch.setenv("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64", "not-base64")
    restored = restore_state(encrypted_path=encrypted, cache_path=cache)

    assert restored["source"] == "encrypted_persisted"
    assert restored["bootstrap_error_type"] == "OAuthStateError"
    assert json.loads(cache.read_text(encoding="utf-8")) == refreshed


def test_invalid_bootstrap_does_not_block_saving_refreshed_cache(tmp_path: Path, monkeypatch) -> None:
    cache = tmp_path / "sdk-cache"
    encrypted = tmp_path / "oauth-state.enc"
    cache.write_text('{"access_token":"refreshed"}', encoding="utf-8")
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CLIENT_ID", "client-1")
    monkeypatch.setenv("LONGBRIDGE_OAUTH_TOKEN_CACHE_B64", "not-base64")
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CACHE_KEY", ENCRYPTION_KEY)

    saved = save_state(encrypted_path=encrypted, cache_path=cache)

    assert saved["status"] == "saved"
    assert saved["bootstrap_error_type"] == "OAuthStateError"
    assert encrypted.is_file()


def test_save_cli_fails_when_oauth_state_cannot_be_persisted(tmp_path: Path, monkeypatch) -> None:
    cache = tmp_path / "sdk-cache"
    cache.write_text('{"access_token":"usable"}', encoding="utf-8")
    report = tmp_path / "save-report.json"
    monkeypatch.setenv("LONGBRIDGE_OAUTH_CLIENT_ID", "client-1")
    monkeypatch.delenv("LONGBRIDGE_OAUTH_CACHE_KEY", raising=False)

    exit_code = main(
        [
            "save",
            "--encrypted",
            str(tmp_path / "oauth-state.enc"),
            "--cache",
            str(cache),
            "--report",
            str(report),
        ]
    )

    assert exit_code == 1
    assert json.loads(report.read_text(encoding="utf-8"))["status"] == ("skipped_missing_key")
