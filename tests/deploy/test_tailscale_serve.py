"""Tests for deploy/tailscale.py: serve-state verification and argv plans.

Pure tests: only literal JSON strings into verify_serve_status and returned
argv tuples. No real tailscale binary, no subprocess, no network.
"""

from __future__ import annotations

import inspect
import json

import pytest

from hermes_orchestrator.deploy import tailscale
from hermes_orchestrator.deploy.tailscale import (
    EXPECTED_BACKEND,
    UnsafeTailscaleState,
    funnel_status_argv,
    serve_enable_argv,
    serve_reset_argv,
    serve_status_argv,
    verify_serve_status,
)

CONSOLE_PORT = 8787
HOST_PORT = "machine.tailnet.ts.net:443"


def _state(
    *,
    proxy: str | None = f"http://127.0.0.1:{CONSOLE_PORT}",
    tcp: dict[str, object] | None = None,
    allow_funnel: dict[str, object] | None = None,
    extra_web: dict[str, object] | None = None,
    handler_paths: dict[str, object] | None = None,
) -> str:
    """Serialize a tailscale serve status document as literal JSON text."""
    web: dict[str, object] = {}
    if proxy is not None:
        handlers: dict[str, object] = handler_paths or {"/": {"Proxy": proxy}}
        web[HOST_PORT] = {"Handlers": handlers}
    if extra_web is not None:
        web.update(extra_web)
    document: dict[str, object] = {
        "TCP": tcp if tcp is not None else {},
        "Web": web,
        "AllowFunnel": allow_funnel if allow_funnel is not None else {HOST_PORT: False},
    }
    return json.dumps(document)


class TestSafeShapes:
    @pytest.mark.parametrize(
        "raw",
        [
            "null",
            "{}",
            '{"TCP": {}, "Web": {}, "AllowFunnel": {}}',
            '{"TCP": null, "Web": null, "AllowFunnel": null}',
            json.dumps({"AllowFunnel": {HOST_PORT: False}}),
        ],
    )
    def test_accepts_absent_state_when_allow_absent(self, raw: str) -> None:
        verify_serve_status(raw, console_port=CONSOLE_PORT, allow_absent=True)

    @pytest.mark.parametrize("allow_absent", [True, False])
    def test_accepts_exact_single_console_proxy(self, allow_absent: bool) -> None:
        verify_serve_status(
            _state(), console_port=CONSOLE_PORT, allow_absent=allow_absent
        )

    def test_accepts_console_proxy_without_allow_funnel_map(self) -> None:
        raw = json.dumps(
            {
                "TCP": {},
                "Web": {
                    HOST_PORT: {
                        "Handlers": {"/": {"Proxy": f"http://127.0.0.1:{CONSOLE_PORT}"}}
                    }
                },
            }
        )
        verify_serve_status(raw, console_port=CONSOLE_PORT, allow_absent=False)


def _code_of(raw: str | None, *, allow_absent: bool = False) -> str:
    with pytest.raises(UnsafeTailscaleState) as excinfo:
        verify_serve_status(raw, console_port=CONSOLE_PORT, allow_absent=allow_absent)
    return excinfo.value.code


class TestFailClosed:
    def test_none_input_is_unavailable(self) -> None:
        assert _code_of(None, allow_absent=True) == "serve_state_unavailable"

    @pytest.mark.parametrize("raw", ["", "not json {", "[]", '"serving"', "3"])
    def test_unparseable_or_wrong_type(self, raw: str) -> None:
        assert _code_of(raw, allow_absent=True) == "serve_state_unparseable"

    def test_funnel_true_alone(self) -> None:
        raw = json.dumps({"AllowFunnel": {HOST_PORT: True}})
        assert _code_of(raw, allow_absent=True) == "funnel_enabled"

    def test_funnel_true_alongside_valid_proxy(self) -> None:
        raw = _state(allow_funnel={HOST_PORT: True})
        assert _code_of(raw) == "funnel_enabled"

    def test_funnel_truthy_non_bool_is_funnel_enabled(self) -> None:
        raw = _state(allow_funnel={HOST_PORT: 1})
        assert _code_of(raw) == "funnel_enabled"

    def test_extra_web_handler(self) -> None:
        raw = _state(
            extra_web={
                "machine.tailnet.ts.net:8443": {
                    "Handlers": {"/": {"Proxy": f"http://127.0.0.1:{CONSOLE_PORT}"}}
                }
            }
        )
        assert _code_of(raw) == "foreign_exposure"

    def test_extra_path_in_handlers(self) -> None:
        raw = _state(
            handler_paths={
                "/": {"Proxy": f"http://127.0.0.1:{CONSOLE_PORT}"},
                "/admin": {"Proxy": f"http://127.0.0.1:{CONSOLE_PORT}"},
            }
        )
        assert _code_of(raw) == "foreign_exposure"

    def test_non_root_single_path(self) -> None:
        raw = _state(
            handler_paths={"/admin": {"Proxy": f"http://127.0.0.1:{CONSOLE_PORT}"}}
        )
        assert _code_of(raw) == "foreign_exposure"

    def test_unexpected_handler_kind(self) -> None:
        raw = _state(handler_paths={"/": {"Path": "/tmp/site"}})
        assert _code_of(raw) == "foreign_exposure"

    def test_tcp_forward_present(self) -> None:
        raw = _state(tcp={"8787": {"HTTPS": True}})
        assert _code_of(raw) == "foreign_exposure"

    def test_tcp_forward_alone(self) -> None:
        raw = json.dumps({"TCP": {"22": {"TCPForward": "127.0.0.1:22"}}})
        assert _code_of(raw, allow_absent=True) == "foreign_exposure"

    def test_unknown_top_level_key(self) -> None:
        raw = json.dumps({"Foreground": {"1234": {}}})
        assert _code_of(raw, allow_absent=True) == "foreign_exposure"

    @pytest.mark.parametrize(
        "backend",
        [
            "http://0.0.0.0:8787",
            "http://192.168.1.5:8787",
            "https://127.0.0.1:8787",
        ],
    )
    def test_non_loopback_backend(self, backend: str) -> None:
        assert _code_of(_state(proxy=backend)) == "non_loopback_backend"

    def test_loopback_backend_wrong_port(self) -> None:
        raw = _state(proxy="http://127.0.0.1:9000")
        assert _code_of(raw) == "unexpected_port"

    @pytest.mark.parametrize("raw", ["null", "{}", '{"Web": {}}'])
    def test_absent_state_refused_when_not_allowed(self, raw: str) -> None:
        assert _code_of(raw, allow_absent=False) == "serve_state_unavailable"


class TestArgvBuilders:
    def test_serve_enable_argv_exact(self) -> None:
        argv = serve_enable_argv(CONSOLE_PORT)
        assert argv == (
            "tailscale",
            "serve",
            "--bg",
            "--yes",
            "http://127.0.0.1:8787",
        )

    def test_serve_enable_argv_uses_expected_backend(self) -> None:
        assert EXPECTED_BACKEND.format(port=9001) == "http://127.0.0.1:9001"
        assert serve_enable_argv(9001)[-1] == "http://127.0.0.1:9001"

    def test_serve_status_argv_exact(self) -> None:
        assert serve_status_argv() == ("tailscale", "serve", "status", "--json")

    def test_serve_reset_argv_exact(self) -> None:
        assert serve_reset_argv() == ("tailscale", "serve", "reset")

    def test_funnel_status_argv_exact_and_read_only(self) -> None:
        assert funnel_status_argv() == ("tailscale", "funnel", "status")

    def test_no_builder_emits_funnel_except_read_only_status(self) -> None:
        non_funnel_argvs = (
            serve_enable_argv(CONSOLE_PORT),
            serve_status_argv(),
            serve_reset_argv(),
        )
        for argv in non_funnel_argvs:
            assert all("funnel" not in element for element in argv)
        funnel_argv = funnel_status_argv()
        assert funnel_argv == ("tailscale", "funnel", "status")
        assert "on" not in funnel_argv

    def test_module_source_has_no_funnel_enabling_argv(self) -> None:
        source = inspect.getsource(tailscale)
        assert '"funnel", "on"' not in source
        assert "'funnel', 'on'" not in source
        for line in source.splitlines():
            if "funnel" not in line.lower():
                continue
            assert (
                "funnel_status_argv" in line
                or "funnel_enabled" in line
                or '"funnel", "status"' in line
                or "AllowFunnel" in line
                or "allow_funnel" in line
            ), line
