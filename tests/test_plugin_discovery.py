import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from ptilopsisbot import config


def settings_file(tmp_path: Path, extra: str = "") -> Path:
    path = tmp_path / "config.toml"
    path.write_text(
        'group_id=123\naccess_token="fixture-only"\ndata_dir="custom-data"\n' + extra,
        encoding="utf-8",
    )
    return path


def entry(name: str, provider):
    return SimpleNamespace(name=name, load=lambda: provider)


def test_installed_provider_receives_only_resolved_directory_and_protocol(tmp_path, monkeypatch):
    calls = []

    def provider(context):
        calls.append(context)
        return {"id": "example", "version": "1", "command": [sys.executable]}

    monkeypatch.setattr(config, "entry_points", lambda **kw: [entry("example", provider)])
    path = settings_file(tmp_path)
    before = path.read_bytes()
    first = config.load_settings(path)
    second = config.load_settings(path)
    assert first.plugins == second.plugins
    assert calls == [{"protocol_version": 1, "data_dir": str(tmp_path / "custom-data")}] * 2
    assert path.read_bytes() == before
    assert len(list(tmp_path.iterdir())) == 1


def test_disabled_or_explicit_plugin_never_loads_installed_provider(tmp_path, monkeypatch):
    def broken(_):
        raise AssertionError("This provider must not be loaded")

    monkeypatch.setattr(config, "entry_points", lambda **kw: [entry("example", broken)])
    path = settings_file(tmp_path, 'disabled_plugins=["example"]\n')
    assert not config.load_settings(path).plugins
    path = settings_file(
        tmp_path,
        '\n[[plugins]]\nid="example"\nversion="old"\ncommand=[' + repr(sys.executable) + "]\n",
    )
    assert config.load_settings(path).plugins[0].version == "old"


def test_missing_dependency_or_duplicate_provider_fails_visibly(tmp_path, monkeypatch):
    def broken(_):
        raise ModuleNotFoundError("missing example dependency")

    path = settings_file(tmp_path)
    monkeypatch.setattr(config, "entry_points", lambda **kw: [entry("example", broken)])
    with pytest.raises(ValueError, match="example.*missing example dependency"):
        config.load_settings(path)

    def provider(_):
        return {"id": "example", "version": "1", "command": [sys.executable]}

    monkeypatch.setattr(config, "entry_points", lambda **kw: [entry("example", provider)] * 2)
    with pytest.raises(ValueError, match="多个已安装包"):
        config.load_settings(path)


def test_plain_environment_keeps_existing_behavior(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "entry_points", lambda **kw: [])
    assert not config.load_settings(settings_file(tmp_path)).plugins
