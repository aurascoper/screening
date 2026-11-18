"""G0 containment verification for aurascoper/screening."""

from __future__ import annotations

import ast
import hashlib
import plistlib
import socket
import urllib.request
from pathlib import Path

import pytest

import execute_strong_buys
import strong_buys_cron

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_static_ast_zero_external_executor_or_live_trading_imports() -> None:
    forbidden_substrings = (
        "mexc_futures",
        "mexc_market_order",
        "MEXC_FUTURES_DIR",
        "live_trading",
        "execution.mexc_client",
        "make_client",
        "submit_order",
        "set_leverage",
        "change_leverage",
    )
    for rel in ("execute_strong_buys.py", "strong_buys_cron.py"):
        src = (REPO_ROOT / rel).read_text(encoding="utf-8")
        assert "\u2014" not in src and "\u2013" not in src, f"Dash hygiene violation in {rel}"
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not any(f in alias.name for f in forbidden_substrings), alias.name
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert not any(f in node.module for f in forbidden_substrings), node.module
            elif isinstance(node, ast.Attribute):
                assert node.attr not in {"submit_order", "set_leverage", "_signed_get", "_signed_post"}


def test_tracked_disarm_sentinel_present() -> None:
    disarm = REPO_ROOT / "DISARM"
    assert disarm.is_file()
    txt = disarm.read_text(encoding="utf-8")
    sha = hashlib.sha256(disarm.read_bytes()).hexdigest()
    assert (
        "DISARMED_BY_DEFAULT=true" in txt
        or (sha == "c7a5be839ac88aa00b3214bb6389886e67f4fb04d2a18cd701c7af1264d0f6a8" and "DISARMED_AT_UTC=" in txt)
    )
    assert "\u2014" not in txt and "\u2013" not in txt


def test_live_flags_fail_closed_with_zero_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _deny(*args: object, **kwargs: object) -> None:
        raise AssertionError("Network access prohibited under G0 containment test")

    monkeypatch.setattr(socket, "socket", _deny)
    monkeypatch.setattr(socket, "create_connection", _deny)
    monkeypatch.setattr(urllib.request, "urlopen", _deny)

    assert execute_strong_buys.main(["--live"]) == 2
    assert (
        strong_buys_cron.main(
            ["--live"],
            disarm_path=tmp_path / "NO_DISARM",
            csv_path=tmp_path / "missing.csv",
            log_dir=tmp_path / "logs",
        )
        == 2
    )
    # Even when DISARM exists, --live explicitly returns non-zero containment code 2.
    disarm_file = tmp_path / "DISARM"
    disarm_file.write_text("disarmed\n", encoding="utf-8")
    assert (
        strong_buys_cron.main(
            ["--live"],
            disarm_path=disarm_file,
            csv_path=tmp_path / "missing.csv",
            log_dir=tmp_path / "logs",
        )
        == 2
    )
    # Preview with DISARM exits 0 cleanly without network.
    assert (
        strong_buys_cron.main(
            [],
            disarm_path=disarm_file,
            csv_path=tmp_path / "missing.csv",
            log_dir=tmp_path / "logs",
        )
        == 0
    )


def test_execute_strong_buys_preview_runs_offline_without_external_executor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample_csv = tmp_path / "scan.csv"
    sample_csv.write_text(
        "mexc_symbol,base,rating\nAVAX_USDC,AVAX,STRONG_BUY\nBTC_USDC,BTC,NEUTRAL\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        execute_strong_buys,
        "spec",
        lambda sym: (25.0, 0.1, 1.0, 1.0, 20),
    )
    rc = execute_strong_buys.main(["--csv", str(sample_csv), "--leverage", "50"])
    assert rc == 0


def test_launchd_plist_explicitly_disabled_and_never_passes_live() -> None:
    plist_path = REPO_ROOT / "launchd" / "com.aurascoper.strongbuys-retry.plist"
    assert plist_path.is_file()
    raw = plist_path.read_bytes()
    text = raw.decode("utf-8")
    assert "\u2014" not in text and "\u2013" not in text
    parsed = plistlib.loads(raw)
    assert parsed.get("Label") == "com.aurascoper.strongbuys-retry"
    assert parsed.get("Disabled") is True
    assert parsed.get("RunAtLoad") is False
    prog_args = parsed.get("ProgramArguments", [])
    assert "--live" not in prog_args

