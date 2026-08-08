"""Test fixtures for the Rocket.Chat gateway plugin.

The adapter builds on Hermes' own ``gateway.*`` base classes, so the tests
need a Hermes Agent checkout importable. Point ``HERMES_AGENT`` at it (or rely
on the default ``~/.hermes/hermes-agent``):

    HERMES_AGENT=~/.hermes/hermes-agent \
      PYTHONPATH="$HERMES_AGENT" \
      "$HERMES_AGENT/venv/bin/python" -m pytest tests/ -q
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _ensure_hermes_on_path() -> Path:
    candidates = []
    env = os.environ.get("HERMES_AGENT")
    if env:
        candidates.append(Path(env).expanduser())
    candidates.append(Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes")) / "hermes-agent")
    for cand in candidates:
        if (cand / "gateway" / "platforms" / "base.py").is_file():
            if str(cand) not in sys.path:
                sys.path.insert(0, str(cand))
            return cand
    pytest.skip(
        "Hermes Agent not found. Set HERMES_AGENT to a hermes-agent checkout "
        "to run these tests.",
        allow_module_level=True,
    )


_HERMES_ROOT = _ensure_hermes_on_path()


def _load_adapter_module():
    """Import the repo's ``adapter.py`` under a unique module name."""
    name = "rocketchat_adapter_under_test"
    cached = sys.modules.get(name)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(name, _REPO_ROOT / "adapter.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _register_platform(module) -> None:
    """Run the plugin's ``register()`` through a real PluginContext so the
    ``rocketchat`` platform resolves via ``Platform("rocketchat")``."""
    from gateway.platform_registry import platform_registry
    if platform_registry.is_registered(module.PLATFORM_NAME):
        return
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
    manifest = PluginManifest(
        name="rocketchat-platform", kind="platform", source="user",
        path=str(_REPO_ROOT),
    )
    ctx = PluginContext(manifest, PluginManager())
    module.register(ctx)


@pytest.fixture(scope="session")
def rc_module():
    module = _load_adapter_module()
    _register_platform(module)
    return module


@pytest.fixture
def make_adapter(rc_module):
    """Factory: build a RocketChatAdapter with sensible test defaults."""
    from gateway.config import PlatformConfig

    def _make(extra=None, token="rc-token"):
        merged = {"url": "https://chat.example.com", "user_id": "botid"}
        if extra:
            merged.update(extra)
        config = PlatformConfig(enabled=True, token=token, extra=merged)
        adapter = rc_module.RocketChatAdapter(config)
        adapter._bot_user_id = "botid"
        adapter._bot_username = "hermes"
        return adapter

    return _make
