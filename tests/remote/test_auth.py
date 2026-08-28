"""Tests for independent remote application authentication (INFRA-173).

Every service is exercised against fakes only: an in-memory keychain and an
injected clock. The real ``security`` binary is never invoked from tests.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import subprocess
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import UTC, datetime, timedelta

import pytest

from hermes_orchestrator.keychain import (
    Keychain,
    KeychainItemExists,
    KeychainItemMissing,
    KeychainReadError,
    KeychainWriteError,
)
from hermes_orchestrator.remote.auth import (
    CredentialConflict,
    CredentialExists,
    CredentialStateUnreadable,
    CsrfService,
    LoginRateLimiter,
    RateLimited,
    RemoteCredentialService,
    SessionExpired,
    SessionInvalid,
    SessionRevoked,
    SessionService,
)

REMOTE_SERVICE = "hermes-orchestrator-remote"


class FakeKeychain:
    """In-memory stand-in honouring read/create fail-closed semantics.

    Read classification mirrors the real ``security`` taxonomy: an absent item
    is definitely missing, while accounts marked via ``fail_reads`` fail for an
    unknown reason (permission, interaction) and are never treated as absent.
    """

    def __init__(self) -> None:
        self._items: dict[tuple[str, str], str] = {}
        self._read_errors: set[tuple[str, str]] = set()
        self.create_attempts: list[tuple[str, str]] = []

    def fail_reads(self, service: str, account: str) -> None:
        self._read_errors.add((service, account))

    def read(self, service: str, account: str) -> str:
        if (service, account) in self._read_errors:
            raise KeychainReadError("keychain read failed")
        secret = self._items.get((service, account), "")
        if not secret:
            raise ValueError("keychain secret is empty")
        return secret

    def read_classified(self, service: str, account: str) -> str:
        if (service, account) in self._read_errors:
            raise KeychainReadError("keychain read failed")
        if (service, account) not in self._items:
            raise KeychainItemMissing("keychain item not found")
        secret = self._items[(service, account)]
        if not secret:
            raise KeychainReadError("keychain secret is empty")
        return secret

    def create(self, service: str, account: str, secret: str) -> None:
        self.create_attempts.append((service, account))
        if (service, account) in self._items:
            raise KeychainItemExists("keychain item already exists")
        self._items[(service, account)] = secret

    def items(self) -> dict[tuple[str, str], str]:
        return dict(self._items)


class FakeClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 8, 28, 12, 0, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.current

    def advance(self, *, minutes: float = 0.0, seconds: float = 0.0) -> None:
        self.current += timedelta(minutes=minutes, seconds=seconds)


@pytest.fixture
def keychain() -> FakeKeychain:
    return FakeKeychain()


@pytest.fixture
def credentials(keychain: FakeKeychain) -> RemoteCredentialService:
    return RemoteCredentialService(keychain=keychain)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def session_service(
    keychain: FakeKeychain, clock: FakeClock
) -> SessionService:
    keychain.create(REMOTE_SERVICE, "session-signing", "signing-secret-for-tests")
    return SessionService(keychain=keychain, now=clock.now)


@pytest.fixture
def csrf(keychain: FakeKeychain) -> CsrfService:
    keychain.create(REMOTE_SERVICE, "session-signing", "signing-secret-for-tests")
    return CsrfService(keychain=keychain)


# --- application token ------------------------------------------------------


def test_initialize_returns_token_once(
    credentials: RemoteCredentialService, keychain: FakeKeychain
) -> None:
    token = credentials.initialize()

    assert len(token) >= 43
    assert keychain.read(REMOTE_SERVICE, "token") == token
    with pytest.raises(CredentialExists):
        credentials.initialize()


def test_initialize_creates_a_separate_session_signing_secret(
    credentials: RemoteCredentialService, keychain: FakeKeychain
) -> None:
    token = credentials.initialize()

    signing = keychain.read(REMOTE_SERVICE, "session-signing")
    assert len(signing) >= 43
    assert signing != token
    assert set(keychain.items()) == {
        (REMOTE_SERVICE, "token"),
        (REMOTE_SERVICE, "session-signing"),
    }


class FailingOnceKeychain(FakeKeychain):
    """Fails the first create for one account, then behaves normally."""

    def __init__(self, failing_account: str) -> None:
        super().__init__()
        self._failing_account: str | None = failing_account

    def create(self, service: str, account: str, secret: str) -> None:
        if account == self._failing_account:
            self._failing_account = None
            raise KeychainWriteError("keychain add failed")
        super().create(service, account, secret)


class RacingKeychain(FakeKeychain):
    """Simulates a concurrent writer landing between probe and add."""

    def __init__(self, raced_account: str, raced_value: str) -> None:
        super().__init__()
        self._raced_account: str | None = raced_account
        self._raced_value = raced_value

    def create(self, service: str, account: str, secret: str) -> None:
        if account == self._raced_account:
            self._raced_account = None
            self._items[(service, account)] = self._raced_value
            raise KeychainItemExists("keychain item already exists")
        super().create(service, account, secret)


class RacingUnreadableKeychain(FakeKeychain):
    """A racer lands at one write but leaves an item that cannot be read."""

    def __init__(self, raced_account: str) -> None:
        super().__init__()
        self._raced_account: str | None = raced_account

    def create(self, service: str, account: str, secret: str) -> None:
        if account == self._raced_account:
            self._raced_account = None
            self.create_attempts.append((service, account))
            self._items[(service, account)] = "racer-opaque-value"
            self.fail_reads(service, account)
            raise KeychainItemExists("keychain item already exists")
        super().create(service, account, secret)


def test_initialize_rejects_an_unproven_token_only_state_without_disclosure(
    credentials: RemoteCredentialService, keychain: FakeKeychain
) -> None:
    keychain.create(REMOTE_SERVICE, "token", "preexisting-opaque-token")
    before = keychain.items()
    attempts_before = list(keychain.create_attempts)

    with pytest.raises(CredentialConflict) as error:
        credentials.initialize()

    rendered = str(error.value) + repr(error.value)
    assert "preexisting-opaque-token" not in rendered
    assert len(str(error.value)) < 200
    assert keychain.items() == before
    assert keychain.create_attempts == attempts_before


@pytest.mark.parametrize("account", ["token", "session-signing"])
def test_unreadable_keychain_state_fails_closed_before_any_create(
    account: str, credentials: RemoteCredentialService, keychain: FakeKeychain
) -> None:
    keychain.fail_reads(REMOTE_SERVICE, account)

    with pytest.raises(CredentialStateUnreadable) as error:
        credentials.initialize()

    assert keychain.create_attempts == []
    assert keychain.items() == {}
    assert len(str(error.value)) < 200


def test_account_presence_reports_three_valued_states(
    credentials: RemoteCredentialService, keychain: FakeKeychain
) -> None:
    assert credentials.account_presence() == {
        "token": "missing",
        "session-signing": "missing",
    }

    keychain.create(REMOTE_SERVICE, "token", "stored-token-value")
    keychain.fail_reads(REMOTE_SERVICE, "session-signing")

    assert credentials.account_presence() == {
        "token": "present",
        "session-signing": "unknown",
    }


def test_signing_race_with_an_unreadable_item_creates_no_token() -> None:
    keychain = RacingUnreadableKeychain("session-signing")
    credentials = RemoteCredentialService(keychain=keychain)

    with pytest.raises(CredentialStateUnreadable) as error:
        credentials.initialize()

    assert (REMOTE_SERVICE, "token") not in keychain.items()
    rendered = str(error.value) + repr(error.value)
    assert "racer-opaque-value" not in rendered
    assert len(str(error.value)) < 200


def test_signing_write_failure_leaves_no_stranded_token_and_retry_succeeds() -> None:
    keychain = FailingOnceKeychain("session-signing")
    credentials = RemoteCredentialService(keychain=keychain)

    with pytest.raises(KeychainWriteError):
        credentials.initialize()
    assert keychain.items() == {}

    token = credentials.initialize()
    assert keychain.read(REMOTE_SERVICE, "token") == token
    assert credentials.verify(token) is True


def test_initialize_preserves_a_preexisting_signing_secret(
    credentials: RemoteCredentialService, keychain: FakeKeychain
) -> None:
    keychain.create(REMOTE_SERVICE, "session-signing", "operator-signing-secret")

    token = credentials.initialize()

    assert len(token) >= 43
    assert keychain.read(REMOTE_SERVICE, "session-signing") == "operator-signing-secret"
    assert keychain.read(REMOTE_SERVICE, "token") == token
    assert set(keychain.items()) == {
        (REMOTE_SERVICE, "token"),
        (REMOTE_SERVICE, "session-signing"),
    }


def test_duplicate_race_at_the_signing_write_is_tolerated() -> None:
    keychain = RacingKeychain("session-signing", "racer-signing-secret")
    credentials = RemoteCredentialService(keychain=keychain)

    token = credentials.initialize()

    assert keychain.read(REMOTE_SERVICE, "session-signing") == "racer-signing-secret"
    assert keychain.read(REMOTE_SERVICE, "token") == token
    assert credentials.verify(token) is True


def test_duplicate_race_at_the_token_write_errors_secret_free() -> None:
    keychain = RacingKeychain("token", "racer-token-value")
    credentials = RemoteCredentialService(keychain=keychain)

    with pytest.raises(CredentialExists) as error:
        credentials.initialize()

    assert keychain.read(REMOTE_SERVICE, "token") == "racer-token-value"
    signing = keychain.read(REMOTE_SERVICE, "session-signing")
    rendered = str(error.value) + repr(error.value)
    assert "racer-token-value" not in rendered
    assert signing not in rendered
    with pytest.raises(CredentialExists):
        credentials.initialize()


def test_verify_accepts_only_the_stored_token(
    credentials: RemoteCredentialService,
) -> None:
    token = credentials.initialize()

    assert credentials.verify(token) is True
    assert credentials.verify(token + "x") is False
    assert credentials.verify("") is False


def test_wrong_token_is_constant_time_rejected(
    credentials: RemoteCredentialService, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials.initialize()
    called = False

    def compared(left: object, right: object) -> bool:
        nonlocal called
        called = True
        return False

    monkeypatch.setattr(hmac, "compare_digest", compared)

    assert credentials.verify("wrong") is False
    assert called is True


def test_verify_without_initialized_credential_rejects(
    credentials: RemoteCredentialService,
) -> None:
    assert credentials.verify("anything") is False


def test_credential_errors_never_carry_secret_material(
    credentials: RemoteCredentialService, keychain: FakeKeychain
) -> None:
    token = credentials.initialize()

    with pytest.raises(CredentialExists) as error:
        credentials.initialize()
    assert token not in str(error.value)
    assert token not in repr(error.value)
    signing = keychain.read(REMOTE_SERVICE, "session-signing")
    assert signing not in str(error.value)


# --- sessions ---------------------------------------------------------------


def test_session_is_bound_and_expires(
    session_service: SessionService, clock: FakeClock
) -> None:
    cookie = session_service.create("phone-fingerprint")

    assert session_service.verify(cookie.value, "phone-fingerprint").authenticated
    clock.advance(minutes=16)
    with pytest.raises(SessionExpired):
        session_service.verify(cookie.value, "phone-fingerprint")


def test_session_expiry_is_exactly_900_seconds(
    session_service: SessionService, clock: FakeClock
) -> None:
    cookie = session_service.create("phone-fingerprint")

    clock.advance(seconds=899)
    assert session_service.verify(cookie.value, "phone-fingerprint").authenticated
    clock.advance(seconds=1)
    with pytest.raises(SessionExpired):
        session_service.verify(cookie.value, "phone-fingerprint")


def test_session_cookie_attributes_are_hardened(
    session_service: SessionService,
) -> None:
    cookie = session_service.create("phone-fingerprint")

    assert cookie.http_only is True
    assert cookie.secure is True
    assert cookie.same_site == "Strict"
    assert cookie.path == "/"
    assert cookie.max_age == 900
    header = cookie.header_value()
    assert "HttpOnly" in header
    assert "Secure" in header
    assert "SameSite=Strict" in header
    assert "Path=/" in header


def test_session_cookie_repr_hides_the_value(
    session_service: SessionService,
) -> None:
    cookie = session_service.create("phone-fingerprint")

    assert cookie.value not in repr(cookie)


def test_session_claims_hash_the_client_fingerprint(
    session_service: SessionService,
) -> None:
    cookie = session_service.create("phone-fingerprint")

    claims = session_service.verify(cookie.value, "phone-fingerprint")
    expected = hashlib.sha256(b"phone-fingerprint").hexdigest()
    assert claims.fingerprint == expected
    assert claims.expires_at == claims.issued_at + timedelta(seconds=900)
    assert "phone-fingerprint" not in cookie.value


def test_wrong_fingerprint_is_rejected(session_service: SessionService) -> None:
    cookie = session_service.create("phone-fingerprint")

    with pytest.raises(SessionInvalid):
        session_service.verify(cookie.value, "laptop-fingerprint")


def test_tampered_cookie_is_rejected(session_service: SessionService) -> None:
    cookie = session_service.create("phone-fingerprint")
    payload_b64, signature = cookie.value.split(".")
    padded = payload_b64 + "=" * (-len(payload_b64) % 4)
    claims = json.loads(urlsafe_b64decode(padded.encode()).decode())
    claims["expires_at"] = "2999-01-01T00:00:00+00:00"
    forged_payload = (
        urlsafe_b64encode(
            json.dumps(claims, sort_keys=True, separators=(",", ":")).encode()
        )
        .decode()
        .rstrip("=")
    )
    forged = f"{forged_payload}.{signature}"

    with pytest.raises(SessionInvalid):
        session_service.verify(forged, "phone-fingerprint")


def test_resigned_cookie_with_wrong_key_is_rejected(
    session_service: SessionService,
) -> None:
    cookie = session_service.create("phone-fingerprint")
    payload_b64, _ = cookie.value.split(".")
    padded = payload_b64 + "=" * (-len(payload_b64) % 4)
    payload = urlsafe_b64decode(padded.encode())
    forged_signature = (
        urlsafe_b64encode(hmac.digest(b"attacker-key", payload, "sha256"))
        .decode()
        .rstrip("=")
    )
    forged = f"{payload_b64}.{forged_signature}"

    with pytest.raises(SessionInvalid):
        session_service.verify(forged, "phone-fingerprint")


@pytest.mark.parametrize(
    "malformed",
    ["", "no-dot", "a.b.c", "!!!.???", "onlypayload.", ".onlysignature"],
)
def test_malformed_cookies_are_rejected(
    session_service: SessionService, malformed: str
) -> None:
    with pytest.raises(SessionInvalid):
        session_service.verify(malformed, "phone-fingerprint")


def test_signature_comparison_is_constant_time(
    session_service: SessionService, monkeypatch: pytest.MonkeyPatch
) -> None:
    cookie = session_service.create("phone-fingerprint")
    called = False

    def compared(left: object, right: object) -> bool:
        nonlocal called
        called = True
        return False

    monkeypatch.setattr(hmac, "compare_digest", compared)

    with pytest.raises(SessionInvalid):
        session_service.verify(cookie.value, "phone-fingerprint")
    assert called is True


def test_replayed_cookie_from_another_client_is_rejected(
    session_service: SessionService,
) -> None:
    cookie = session_service.create("phone-fingerprint")

    with pytest.raises(SessionInvalid):
        session_service.verify(cookie.value, "stolen-by-other-device")


def test_revocation_invalidates_an_otherwise_valid_session(
    session_service: SessionService,
) -> None:
    cookie = session_service.create("phone-fingerprint")
    claims = session_service.verify(cookie.value, "phone-fingerprint")

    session_service.revoke(claims.session_id)

    with pytest.raises(SessionRevoked):
        session_service.verify(cookie.value, "phone-fingerprint")


def test_revocation_is_scoped_to_one_session(
    session_service: SessionService,
) -> None:
    revoked = session_service.create("phone-fingerprint")
    surviving = session_service.create("phone-fingerprint")
    revoked_claims = session_service.verify(revoked.value, "phone-fingerprint")

    session_service.revoke(revoked_claims.session_id)

    assert session_service.verify(surviving.value, "phone-fingerprint").authenticated


def test_session_ids_are_unique_per_session(
    session_service: SessionService,
) -> None:
    first = session_service.verify(
        session_service.create("phone-fingerprint").value, "phone-fingerprint"
    )
    second = session_service.verify(
        session_service.create("phone-fingerprint").value, "phone-fingerprint"
    )

    assert first.session_id != second.session_id


def test_session_errors_never_carry_cookie_or_key_material(
    session_service: SessionService, keychain: FakeKeychain
) -> None:
    cookie = session_service.create("phone-fingerprint")
    signing = keychain.read(REMOTE_SERVICE, "session-signing")

    with pytest.raises(SessionInvalid) as error:
        session_service.verify(cookie.value, "laptop-fingerprint")
    rendered = str(error.value) + repr(error.value)
    assert cookie.value not in rendered
    assert signing not in rendered


# --- CSRF -------------------------------------------------------------------


def test_csrf_cannot_cross_sessions(csrf: CsrfService) -> None:
    token = csrf.issue("session-a")

    assert csrf.verify("session-a", token)
    assert not csrf.verify("session-b", token)


def test_csrf_binds_method_and_path(csrf: CsrfService) -> None:
    token = csrf.issue("session-a", method="POST", path="/queue")

    assert csrf.verify("session-a", token, method="POST", path="/queue")
    assert not csrf.verify("session-a", token, method="DELETE", path="/queue")
    assert not csrf.verify("session-a", token, method="POST", path="/pause")


def test_csrf_tokens_use_a_fresh_nonce_per_form(csrf: CsrfService) -> None:
    first = csrf.issue("session-a")
    second = csrf.issue("session-a")

    assert first != second
    assert csrf.verify("session-a", first)
    assert csrf.verify("session-a", second)


@pytest.mark.parametrize("garbage", ["", "no-dot", "nonce.", ".mac", "a.b.c"])
def test_csrf_rejects_malformed_tokens(csrf: CsrfService, garbage: str) -> None:
    assert not csrf.verify("session-a", garbage)


def test_csrf_rejects_tampered_mac(csrf: CsrfService) -> None:
    nonce, mac = csrf.issue("session-a").rsplit(".", 1)
    flipped = ("0" if mac[0] != "0" else "1") + mac[1:]

    assert not csrf.verify("session-a", f"{nonce}.{flipped}")


def test_csrf_verification_is_constant_time(
    csrf: CsrfService, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = csrf.issue("session-a")
    called = False

    def compared(left: object, right: object) -> bool:
        nonlocal called
        called = True
        return False

    monkeypatch.setattr(hmac, "compare_digest", compared)

    assert not csrf.verify("session-a", token)
    assert called is True


# --- login rate limiting ----------------------------------------------------


def test_five_failures_in_window_lock_the_client(clock: FakeClock) -> None:
    limiter = LoginRateLimiter(now=clock.now)

    for _ in range(5):
        limiter.check("100.64.0.7", "phone-fingerprint")
        limiter.record_failure("100.64.0.7", "phone-fingerprint")

    with pytest.raises(RateLimited):
        limiter.check("100.64.0.7", "phone-fingerprint")


def test_lockout_expires_with_the_15_minute_window(clock: FakeClock) -> None:
    limiter = LoginRateLimiter(now=clock.now)

    for _ in range(5):
        limiter.record_failure("100.64.0.7", "phone-fingerprint")
    with pytest.raises(RateLimited):
        limiter.check("100.64.0.7", "phone-fingerprint")

    clock.advance(minutes=15, seconds=1)
    limiter.check("100.64.0.7", "phone-fingerprint")


def test_rate_limit_is_scoped_per_address_and_fingerprint(
    clock: FakeClock,
) -> None:
    limiter = LoginRateLimiter(now=clock.now)

    for _ in range(5):
        limiter.record_failure("100.64.0.7", "phone-fingerprint")

    with pytest.raises(RateLimited):
        limiter.check("100.64.0.7", "phone-fingerprint")
    limiter.check("100.64.0.8", "phone-fingerprint")
    limiter.check("100.64.0.7", "other-fingerprint")


def test_success_clears_the_failure_bucket(clock: FakeClock) -> None:
    limiter = LoginRateLimiter(now=clock.now)

    for _ in range(4):
        limiter.record_failure("100.64.0.7", "phone-fingerprint")
    limiter.record_success("100.64.0.7", "phone-fingerprint")
    for _ in range(4):
        limiter.record_failure("100.64.0.7", "phone-fingerprint")

    limiter.check("100.64.0.7", "phone-fingerprint")


def test_old_failures_slide_out_of_the_window(clock: FakeClock) -> None:
    limiter = LoginRateLimiter(now=clock.now)

    for _ in range(4):
        limiter.record_failure("100.64.0.7", "phone-fingerprint")
    clock.advance(minutes=15, seconds=1)
    limiter.record_failure("100.64.0.7", "phone-fingerprint")

    limiter.check("100.64.0.7", "phone-fingerprint")


# --- keychain write support -------------------------------------------------


class WritableRecordingRunner:
    """Fake ``security`` runner with per-call scripted exit codes."""

    def __init__(self, *, find_rc: int = 1, add_rc: int = 0) -> None:
        self.find_rc = find_rc
        self.add_rc = add_rc
        self.calls: list[list[str]] = []

    def __call__(
        self, command: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(command))
        rc = self.find_rc if command[1] == "find-generic-password" else self.add_rc
        if kwargs.get("check") and rc != 0:
            raise subprocess.CalledProcessError(rc, command)
        return subprocess.CompletedProcess(command, rc, stdout="", stderr="")


def test_keychain_create_adds_a_missing_item_without_update_flag() -> None:
    runner = WritableRecordingRunner(find_rc=1, add_rc=0)
    keychain = Keychain(runner=runner)

    keychain.create(REMOTE_SERVICE, "token", "new-secret")

    add_calls = [c for c in runner.calls if c[1] == "add-generic-password"]
    assert add_calls == [
        [
            "security",
            "add-generic-password",
            "-s",
            REMOTE_SERVICE,
            "-a",
            "token",
            "-w",
            "new-secret",
        ]
    ]
    assert "-U" not in add_calls[0]


def test_keychain_create_fails_closed_on_existing_item() -> None:
    runner = WritableRecordingRunner(find_rc=0)
    keychain = Keychain(runner=runner)

    with pytest.raises(KeychainItemExists):
        keychain.create(REMOTE_SERVICE, "token", "new-secret")
    assert all(c[1] != "add-generic-password" for c in runner.calls)


def test_keychain_write_failure_never_exposes_the_secret() -> None:
    runner = WritableRecordingRunner(find_rc=1, add_rc=45)
    keychain = Keychain(runner=runner)

    with pytest.raises(KeychainWriteError) as error:
        keychain.create(REMOTE_SERVICE, "token", "super-secret-value")
    rendered = str(error.value) + repr(error.value)
    assert "super-secret-value" not in rendered


class ScriptedReadRunner:
    """Fake ``security`` runner returning one scripted classified-read result."""

    def __init__(self, *, rc: int, stdout: str = "") -> None:
        self.rc = rc
        self.stdout = stdout
        self.calls: list[list[str]] = []

    def __call__(
        self, command: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(command))
        if kwargs.get("check") and self.rc != 0:
            raise subprocess.CalledProcessError(self.rc, command)
        return subprocess.CompletedProcess(
            command, self.rc, stdout=self.stdout, stderr="scary keychain details"
        )


def test_keychain_classified_read_returns_the_stored_secret() -> None:
    runner = ScriptedReadRunner(rc=0, stdout="stored-value\n")
    keychain = Keychain(runner=runner)

    assert keychain.read_classified(REMOTE_SERVICE, "token") == "stored-value"


def test_keychain_classified_read_reports_definite_absence_only_for_rc_44() -> None:
    runner = ScriptedReadRunner(rc=44)
    keychain = Keychain(runner=runner)

    with pytest.raises(KeychainItemMissing):
        keychain.read_classified(REMOTE_SERVICE, "token")


@pytest.mark.parametrize("rc", [1, 36, 51, 128])
def test_keychain_classified_read_fails_closed_on_other_errors(rc: int) -> None:
    runner = ScriptedReadRunner(rc=rc)
    keychain = Keychain(runner=runner)

    with pytest.raises(KeychainReadError) as error:
        keychain.read_classified(REMOTE_SERVICE, "token")
    rendered = str(error.value) + repr(error.value)
    assert "scary keychain details" not in rendered
    assert "security" not in rendered


def test_keychain_classified_read_treats_an_empty_secret_as_a_read_error() -> None:
    runner = ScriptedReadRunner(rc=0, stdout="\n")
    keychain = Keychain(runner=runner)

    with pytest.raises(KeychainReadError):
        keychain.read_classified(REMOTE_SERVICE, "token")


# --- remote-auth-init CLI ---------------------------------------------------


def test_remote_auth_init_displays_the_token_exactly_once(
    configured_repo: tuple[object, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    from hermes_orchestrator import cli as cli_module
    from tests.test_cli import base_arguments, invoke

    fake = FakeKeychain()
    monkeypatch.setattr(cli_module, "Keychain", lambda: fake)

    result = invoke([*base_arguments(configured_repo), "remote-auth-init"])

    assert result.exit_code == 0
    token = fake.read(REMOTE_SERVICE, "token")
    assert result.stdout.count(token) == 1
    assert token not in result.stderr

    repeat = invoke([*base_arguments(configured_repo), "remote-auth-init"])
    assert repeat.exit_code == 1
    assert token not in repeat.output
    assert "exists" in repeat.stderr.lower()


def test_remote_auth_init_failure_prints_bounded_secret_free_diagnostic(
    configured_repo: tuple[object, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    from hermes_orchestrator import cli as cli_module
    from tests.test_cli import base_arguments, invoke

    fake = FailingOnceKeychain("token")
    monkeypatch.setattr(cli_module, "Keychain", lambda: fake)

    result = invoke([*base_arguments(configured_repo), "remote-auth-init"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "keychain account 'token': missing" in result.stderr
    assert "keychain account 'session-signing': present" in result.stderr
    assert len(result.stderr) < 400
    signing = fake.read(REMOTE_SERVICE, "session-signing")
    assert signing not in result.output

    retry = invoke([*base_arguments(configured_repo), "remote-auth-init"])
    assert retry.exit_code == 0
    token = fake.read(REMOTE_SERVICE, "token")
    assert retry.stdout.count(token) == 1
    assert signing not in retry.output


def test_remote_auth_init_rejects_an_unproven_token_without_printing_it(
    configured_repo: tuple[object, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    from hermes_orchestrator import cli as cli_module
    from tests.test_cli import base_arguments, invoke

    fake = FakeKeychain()
    fake.create(REMOTE_SERVICE, "token", "preexisting-opaque-token")
    monkeypatch.setattr(cli_module, "Keychain", lambda: fake)

    result = invoke([*base_arguments(configured_repo), "remote-auth-init"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "preexisting-opaque-token" not in result.output
    assert "keychain account 'token': present" in result.stderr
    assert "keychain account 'session-signing': missing" in result.stderr
    assert len(result.stderr) < 400
    assert fake.items() == {(REMOTE_SERVICE, "token"): "preexisting-opaque-token"}


def test_remote_auth_init_reports_unreadable_accounts_as_unknown(
    configured_repo: tuple[object, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    from hermes_orchestrator import cli as cli_module
    from tests.test_cli import base_arguments, invoke

    fake = FakeKeychain()
    fake.fail_reads(REMOTE_SERVICE, "session-signing")
    monkeypatch.setattr(cli_module, "Keychain", lambda: fake)

    result = invoke([*base_arguments(configured_repo), "remote-auth-init"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "keychain account 'session-signing': unknown" in result.stderr
    assert "keychain account 'session-signing': missing" not in result.stderr
    assert len(result.stderr) < 400
    assert fake.create_attempts == []
